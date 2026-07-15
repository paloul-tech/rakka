//! The effect model: intents, safety classes, and the dispatch state machine.
//!
//! Owns the `EffectIntent` record, the safety classes that decide what recovery
//! is permitted, and the generation-carrying effect status machine. Dispatch is
//! durable first: `Started` is persisted with a lease and fence before the
//! external invocation, credentials are resolved only at dispatch and never
//! persisted, and a result from a stale generation is rejected.
//!
//! Crash and timeout handling follows the safety class. An effect whose outcome
//! cannot be established becomes `Indeterminate` and moves to
//! `WaitingForReconciliation` rather than being retried; there is no generic
//! retry for an ambiguous non-idempotent effect. Cancellation fences new
//! dispatch immediately, but an ambiguous effect stays in reconciliation until
//! its outcome is resolved, so cancellation is never terminal before then.
//!
//! Specification: sections 11.1 through 11.6, with the cancellation clauses of
//! 8.7. The full state machine is filled by slice 1.7 over the
//! `rakka-agent-workflow` dispatcher and effect bridge.
//!
//! # The interim effect, landed by slice 1.5
//!
//! The durable loop cannot exist without *some* durable effect, because
//! [specification 9.5](../../../docs/plans/rakka-agent/spec.md) requires a run
//! transition to persist the next effect or wait before it returns. Slice 1.5
//! lands exactly that much and no more: [`AgentRunEffect`], the record a run
//! commits when it decides to call a model or a tool, and
//! [`AgentRunEffectSink`], which hands it to the existing agent-workflow
//! `AgentEffect` outbox for dispatch.
//!
//! ## Why the record lives in the run's own state
//!
//! The effect is a field of the run's durable state, committed by the very
//! transition that decided it — *not* a second write to the agent-workflow
//! outbox. This is the same argument the exchange journal makes in
//! [`crate::choreography`] and the history outbox makes in [`crate::task`], and
//! it matters for the same reason: the run's state and the workflow outbox are
//! two independent compare-and-sets. A run that committed `AwaitingModel` and
//! then lost its node before writing the outbox record would wait forever for an
//! effect nobody will ever dispatch. With the effect in the run's own record
//! there is no such window — recovery finds it in [`AgentRunEffect::is_pending`]
//! and re-drives the dispatch, which is idempotent on
//! [`AgentRunEffect::effect_id`].
//!
//! The agent-workflow outbox stays exactly where it belongs: the *sink* that
//! performs the bounded external I/O and returns a durable result command
//! through the inbox.
//!
//! ## What slice 1.7 adds
//!
//! [`AgentRunEffectStatus`] is deliberately the smallest status set a durable
//! wait needs, and it is *not* the effect state machine of
//! [specification 11.3](../../../docs/plans/rakka-agent/spec.md). Slice 1.7
//! retrofits that machine onto this record: the safety classes, the durable
//! `Started` with its lease and fence, the `Indeterminate` outcome and its
//! transition to `WaitingForReconciliation`, and stale-result rejection by
//! generation. [`AgentRunEffect::generation`] and
//! [`AgentRunEffect::attempts`] are here from the first commit so that machine
//! is an addition to the record rather than a rewrite of it.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentEffect, AgentEffectKind, AgentEffectStatus,
    AgentEffectTarget, AgentIdempotencyKey, AgentTelemetryContext, AgentTimestampMillis,
    StateSchemaVersion,
};
use rakka_agent_workflow::{AgentDeduplicationKey, AgentEffectId};
use serde::{Deserialize, Serialize};

use crate::definition::AgentModelProfileId;
use crate::identity::{AgentIdentityError, AgentOperationId, AgentOperationKind, AgentRunScope};
use crate::memory::AgentContextSnapshotRef;
use crate::model::{AgentModelTurn, AgentToolCallId, AgentToolCallRequest};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
};
use crate::task::{AgentTaskContent, AgentTaskError};

/// Most effects one run may have outstanding at once.
///
/// It bounds the run's durable state and its `AwaitingTools` fan-out together:
/// a model turn cannot request more tool calls than this
/// ([`crate::model::AGENT_MODEL_MAX_TOOL_CALLS`]), so a turn's effects always
/// fit.
pub const AGENT_RUN_MAX_PENDING_EFFECTS: usize = 8;

/// Largest inline tool result a run may commit, in bytes.
///
/// Anything larger arrives as an artifact reference, exactly as a task's input
/// or result does ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
pub const AGENT_TOOL_RESULT_MAX_BYTES: usize = 2 * 1024;

/// Result type for effect operations.
pub type AgentEffectResult<T> = Result<T, AgentEffectError>;

/// Boxed future returned by an [`AgentRunEffectSink`].
pub type AgentEffectFuture<'a, T> = Pin<Box<dyn Future<Output = AgentEffectResult<T>> + Send + 'a>>;

/// Monotonic dispatch generation of one effect
/// ([specification 11.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Slice 1.5 only ever mints the first generation. It is persisted from the
/// first commit because slice 1.7's reconciliation depends on it: an operator
/// who proves that an ambiguous invocation never happened causes a *new*
/// generation, and a result carrying a generation the run has passed is refused
/// rather than applied.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentEffectGeneration(u32);

impl AgentEffectGeneration {
    /// The generation of a freshly persisted effect.
    pub const FIRST: Self = Self(1);

    /// Creates a generation.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Numeric value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The next generation.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl Display for AgentEffectGeneration {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// What one effect calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEffectKind {
    /// A model provider request.
    ModelCall,
    /// A tool adapter request.
    ToolCall,
}

impl AgentRunEffectKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::ModelCall => "model-call",
            Self::ToolCall => "tool-call",
        }
    }

    /// The agent-workflow effect kind this dispatches as.
    #[must_use]
    pub const fn workflow_kind(self) -> AgentEffectKind {
        match self {
            Self::ModelCall => AgentEffectKind::ModelCall,
            Self::ToolCall => AgentEffectKind::ToolCall,
        }
    }
}

impl Display for AgentRunEffectKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Where one effect stands in the interim dispatch cycle.
///
/// It is the smallest status set a durable wait needs, not the effect state
/// machine of [specification 11.3](../../../docs/plans/rakka-agent/spec.md).
/// Notably there is no `Indeterminate` here, and there must not be: an
/// indeterminate outcome is only *meaningful* alongside a safety class, and
/// inventing one before slice 1.7 defines what may be retried would let an
/// ambiguous effect look routine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEffectStatus {
    /// Committed by a run transition and not yet handed to the sink.
    Pending,
    /// Handed to the sink; the run is waiting for its durable result command.
    Dispatched,
    /// The dispatcher returned a result.
    Completed,
    /// The dispatcher returned a failure.
    Failed,
}

impl AgentRunEffectStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Dispatched => "dispatched",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// Whether the run is still waiting on this effect.
    #[must_use]
    pub const fn is_outstanding(self) -> bool {
        matches!(self, Self::Pending | Self::Dispatched)
    }

    /// The agent-workflow status this dispatches as.
    #[must_use]
    pub const fn workflow_status(self) -> AgentEffectStatus {
        match self {
            Self::Pending => AgentEffectStatus::Scheduled,
            Self::Dispatched => AgentEffectStatus::Dispatching,
            Self::Completed => AgentEffectStatus::Completed,
            Self::Failed => AgentEffectStatus::Failed,
        }
    }
}

impl Display for AgentRunEffectStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// What one effect asks its target to do.
///
/// A request carries no resolved credential and no secret material. The model
/// profile and the tool identity are *references*: the credential a dispatch may
/// resolve is named by the agent's definition, resolved inside the bounded
/// dispatcher attempt, and never persisted
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEffectRequest {
    /// Call the model against an immutable context snapshot.
    Model {
        /// The snapshot the call is prepared against. A retry reuses it, so
        /// drift in a memory store or an index cannot change a retried input
        /// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
        context: AgentContextSnapshotRef,
        /// The model profile the settings revision selected.
        profile: Option<AgentModelProfileId>,
    },
    /// Call a tool the model requested.
    Tool {
        /// The call, exactly as the model asked for it. Whether it may be
        /// dispatched at all is decided by the tool authority of slice 1.8, not
        /// by this record.
        call: Box<AgentToolCallRequest>,
    },
}

impl AgentRunEffectRequest {
    /// The effect kind this request dispatches as.
    #[must_use]
    pub const fn kind(&self) -> AgentRunEffectKind {
        match self {
            Self::Model { .. } => AgentRunEffectKind::ModelCall,
            Self::Tool { .. } => AgentRunEffectKind::ToolCall,
        }
    }

    /// The tool call, when this is a tool request.
    #[must_use]
    pub fn tool_call(&self) -> Option<&AgentToolCallRequest> {
        match self {
            Self::Model { .. } => None,
            Self::Tool { call } => Some(call),
        }
    }

    /// The dispatch target this request names.
    #[must_use]
    pub fn target(&self) -> AgentEffectTarget {
        match self {
            Self::Model { profile, .. } => AgentEffectTarget {
                target_type: "model".to_string(),
                name: profile
                    .as_ref()
                    .map_or_else(|| "default".to_string(), ToString::to_string),
                address: None,
                attributes: BTreeMap::new(),
            },
            Self::Tool { call } => AgentEffectTarget {
                target_type: "tool".to_string(),
                name: call.tool.to_string(),
                address: None,
                attributes: BTreeMap::new(),
            },
        }
    }
}

/// One effect a run committed and is waiting on
/// ([specification 9.4](../../../docs/plans/rakka-agent/spec.md): the loop
/// state's pending effect references).
///
/// It is a component of the run's durable state, written by the transition that
/// decided it. See the module documentation for why it cannot be a second write
/// to the agent-workflow outbox instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunEffect {
    schema_version: StateSchemaVersion,
    /// Stable, *derived* effect identity: re-driving a dispatch names the same
    /// effect rather than creating a second one.
    pub effect_id: AgentEffectId,
    /// Which dispatch generation this is.
    pub generation: AgentEffectGeneration,
    /// The turn that decided the effect.
    pub turn: u64,
    /// The slot the effect takes within its turn. A turn commits one model call,
    /// or one effect per tool the model asked for.
    pub slot: usize,
    /// What the effect asks its target to do.
    pub request: AgentRunEffectRequest,
    /// Where the effect stands.
    pub status: AgentRunEffectStatus,
    /// The idempotency key handed to the target.
    pub idempotency_key: AgentIdempotencyKey,
    /// How many dispatch attempts have been made.
    pub attempts: u32,
    /// When the deciding transition committed it.
    pub created_at: AgentTimestampMillis,
    /// When it was last handed to the sink.
    pub dispatched_at: Option<AgentTimestampMillis>,
    /// Stable code of the last dispatch or execution failure.
    pub last_error_code: Option<String>,
}

impl AgentRunEffect {
    /// Commits a new effect at its first generation.
    ///
    /// The identity is derived from the run, the turn, and the slot, so a
    /// transition replayed after a crash resolves to the same effect. That is
    /// what makes handing it to the sink safe to re-drive: the sink deduplicates
    /// on exactly this value.
    pub fn new(
        scope: &AgentRunScope,
        turn: u64,
        slot: usize,
        request: AgentRunEffectRequest,
        created_at: AgentTimestampMillis,
    ) -> AgentEffectResult<Self> {
        let effect_id = effect_id_for(scope, turn, slot)?;
        let idempotency_key = AgentIdempotencyKey::new(effect_id.as_str());
        Ok(Self {
            schema_version: CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
            effect_id,
            generation: AgentEffectGeneration::FIRST,
            turn,
            slot,
            request,
            status: AgentRunEffectStatus::Pending,
            idempotency_key,
            attempts: 0,
            created_at,
            dispatched_at: None,
            last_error_code: None,
        })
    }

    /// What this effect calls.
    #[must_use]
    pub const fn kind(&self) -> AgentRunEffectKind {
        self.request.kind()
    }

    /// Whether the effect has been committed but not yet handed to the sink.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self.status, AgentRunEffectStatus::Pending)
    }

    /// Whether the run is still waiting on this effect.
    #[must_use]
    pub const fn is_outstanding(&self) -> bool {
        self.status.is_outstanding()
    }

    /// Records that the effect was handed to the sink.
    pub fn mark_dispatched(&mut self, now: AgentTimestampMillis) {
        self.status = AgentRunEffectStatus::Dispatched;
        self.attempts = self.attempts.saturating_add(1);
        self.dispatched_at = Some(now);
    }

    /// The stable operation id of the command that returns this effect's result.
    ///
    /// It is derived from the effect *and its dispatch generation*, so a
    /// dispatcher that returns the same result twice — because it retried, or
    /// because its own delivery was redelivered — is deduplicated by the run's
    /// operation log rather than advancing the loop a second time, while the
    /// result of a *later* generation is a different operation entirely. Slice
    /// 1.7's reconciliation mints a new generation when an operator establishes
    /// that an ambiguous invocation never happened; the re-dispatch's result
    /// must not be answered from the log entry a superseded attempt left
    /// behind. The run's own fence backs the log up: a result for an effect it
    /// has already resolved is refused whether or not the operation is still
    /// inside the log's bounded window
    /// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 10).
    pub fn result_operation_id(
        &self,
        scope: &AgentRunScope,
    ) -> Result<AgentOperationId, AgentIdentityError> {
        AgentOperationId::new(
            AgentOperationKind::Command,
            [
                scope.tenant().as_str(),
                scope.agent().as_str(),
                scope.run().as_str(),
                "effect-result",
                &self.turn.to_string(),
                &self.slot.to_string(),
                &self.generation.to_string(),
            ],
        )
    }

    /// Projects the effect onto the agent-workflow outbox record that dispatches
    /// it.
    ///
    /// The projection is deterministic and carries no credential: the outbox row
    /// names *what* to call and under which idempotency key, and the dispatcher
    /// resolves the credential inside its own bounded attempt
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn to_workflow_effect(&self, scope: &AgentRunScope) -> AgentEffect {
        AgentEffect {
            effect_id: self.effect_id.clone(),
            deduplication_key: AgentDeduplicationKey::new(self.effect_id.as_str()),
            kind: self.kind().workflow_kind(),
            target: self.request.target(),
            status: self.status.workflow_status(),
            payload_ref: None,
            result_ref: None,
            timeout_ms: None,
            idempotency_key: self.idempotency_key.clone(),
            expected_result_type: Some(self.kind().as_label().to_string()),
            causation_id: AgentCausationId::new(self.effect_id.as_str()),
            correlation_id: AgentCorrelationId::new(scope.key()),
            telemetry_context: AgentTelemetryContext::default(),
            attempt: self.attempts,
            created_at: self.created_at,
            due_at: None,
            last_error_code: self.last_error_code.clone(),
        }
    }
}

impl VersionedAgentRecord for AgentRunEffect {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::RunEffect;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Derives the identity of the effect one run's turn commits in one slot.
///
/// A turn may commit one model call, or up to
/// [`crate::model::AGENT_MODEL_MAX_TOOL_CALLS`] tool calls; the slot separates
/// them. The derivation is pure, so replaying the transition that decided them
/// resolves to the same effects.
pub fn effect_id_for(
    scope: &AgentRunScope,
    turn: u64,
    slot: usize,
) -> Result<AgentEffectId, AgentIdentityError> {
    let operation = AgentOperationId::new(
        AgentOperationKind::EffectDispatch,
        [
            scope.tenant().as_str(),
            scope.agent().as_str(),
            scope.run().as_str(),
            &turn.to_string(),
            &slot.to_string(),
        ],
    )?;
    Ok(AgentEffectId::new(operation.into_string()))
}

/// What a dispatcher returned for one effect.
///
/// It is the durable result command of
/// [specification 9.5](../../../docs/plans/rakka-agent/spec.md): the dispatcher
/// performs the bounded I/O and returns *this* through the inbox. The run never
/// awaits a model or a tool inside a handler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEffectOutcome {
    /// A model call produced a turn.
    Model {
        /// The Rakka-owned versioned turn. It is the only durable format for
        /// what the model produced
        /// ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
        turn: Box<AgentModelTurn>,
    },
    /// A tool call produced a bounded result.
    Tool {
        /// The call the result answers.
        call_id: AgentToolCallId,
        /// The bounded result content.
        content: AgentTaskContent,
    },
    /// The dispatcher could not complete the effect.
    Failed {
        /// Stable machine-readable code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentRunEffectOutcome {
    /// Whether the effect completed rather than failed.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    /// Stable failure code, when the effect failed.
    #[must_use]
    pub fn failure_code(&self) -> Option<&str> {
        match self {
            Self::Failed { code, .. } => Some(code),
            _ => None,
        }
    }

    /// Fails closed on a record inside the outcome that this binary cannot
    /// interpret ([specification 20](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The check runs where the outcome enters, before it is committed: a turn
    /// whose schema version the policy refuses must never become the run's
    /// pending turn, because recovery applies the same policy and would fail
    /// closed on the committed record forever after.
    pub fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        if let Self::Model { turn } = self {
            policy.check_record(turn.as_ref())?;
        }
        Ok(())
    }

    /// Rejects an outcome that cannot be bounded.
    pub fn validate(&self) -> AgentEffectResult<()> {
        match self {
            Self::Model { turn } => turn.validate().map_err(|error| AgentEffectError::Model {
                message: error.to_string(),
            }),
            Self::Tool { call_id, content } => {
                content.validate()?;
                let bytes = content.size_bytes();
                if bytes > AGENT_TOOL_RESULT_MAX_BYTES {
                    return Err(AgentEffectError::ToolResultTooLarge {
                        call_id: call_id.clone(),
                        bytes,
                        maximum: AGENT_TOOL_RESULT_MAX_BYTES,
                    });
                }
                Ok(())
            }
            Self::Failed { .. } => Ok(()),
        }
    }
}

/// One tool result the current turn has collected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolResult {
    /// The call this answers.
    pub call_id: AgentToolCallId,
    /// The bounded result content.
    pub content: AgentTaskContent,
    /// When the run recorded it.
    pub recorded_at: AgentTimestampMillis,
}

/// The durable sink that dispatches a run's effects
/// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is the existing agent-workflow `AgentEffect` outbox: the run commits the
/// effect into its *own* state, and this hands it onward for the bounded
/// external I/O that returns an [`AgentRunEffectOutcome`] through the inbox.
///
/// A dispatch is idempotent on [`AgentRunEffect::effect_id`], which is what
/// makes a re-driven flush — after a crash between the run's transition and the
/// sink's write — safe. It is also why shard movement cannot make an effect
/// dispatchable twice
/// ([specification 15](../../../docs/plans/rakka-agent/spec.md)): both owners
/// derive the same id.
pub trait AgentRunEffectSink: Clone + Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Hands one effect to the outbox, idempotently on its effect id.
    fn dispatch<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        effect: &'a AgentEffect,
    ) -> AgentEffectFuture<'a, ()>;
}

/// An in-memory effect sink, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentRunEffectSink {
    dispatched: Arc<Mutex<BTreeMap<String, BTreeMap<String, AgentEffect>>>>,
}

impl InMemoryAgentRunEffectSink {
    /// Creates an empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every effect one run has dispatched, in effect-id order.
    #[must_use]
    pub fn dispatched(&self, scope: &AgentRunScope) -> Vec<AgentEffect> {
        self.dispatched
            .lock()
            .expect("the effect sink should not be poisoned")
            .get(&scope.key())
            .map(|effects| effects.values().cloned().collect())
            .unwrap_or_default()
    }

    /// How many distinct effects one run has dispatched.
    ///
    /// A re-driven flush must not raise this: the sink deduplicates on the
    /// effect id, which is what proves shard movement cannot dispatch one effect
    /// twice.
    #[must_use]
    pub fn len(&self, scope: &AgentRunScope) -> usize {
        self.dispatched
            .lock()
            .expect("the effect sink should not be poisoned")
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one run has dispatched nothing.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentRunScope) -> bool {
        self.len(scope) == 0
    }
}

impl AgentRunEffectSink for InMemoryAgentRunEffectSink {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn dispatch<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        effect: &'a AgentEffect,
    ) -> AgentEffectFuture<'a, ()> {
        Box::pin(async move {
            let mut dispatched = self
                .dispatched
                .lock()
                .expect("the effect sink should not be poisoned");
            let run = dispatched.entry(scope.key()).or_default();
            // Idempotent on the effect id: re-driving an interrupted flush
            // rewrites the same row rather than dispatching a second effect.
            run.insert(effect.effect_id.as_str().to_string(), effect.clone());
            Ok(())
        })
    }
}

/// Rejection of an effect operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentEffectError {
    /// An identifier could not be derived.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// A model turn could not be bounded.
    Model {
        /// What was out of bounds.
        message: String,
    },
    /// A tool result exceeded its inline bound.
    ToolResultTooLarge {
        /// The call whose result was refused.
        call_id: AgentToolCallId,
        /// Size of the rejected result, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// Inline content exceeded its bound.
    ContentTooLarge {
        /// Size of the rejected content, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// The run already holds as many outstanding effects as it may.
    PendingOverflow {
        /// The maximum number of outstanding effects.
        maximum: usize,
    },
    /// The effect sink rejected a dispatch.
    Sink {
        /// Stable machine-readable code.
        code: String,
        /// The failure detail.
        message: String,
    },
}

impl AgentEffectError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Model { .. } => "effect-model-turn-invalid",
            Self::ToolResultTooLarge { .. } => "effect-tool-result-too-large",
            Self::ContentTooLarge { .. } => "effect-content-too-large",
            Self::PendingOverflow { .. } => "effect-pending-overflow",
            Self::Sink { .. } => "effect-sink-failed",
        }
    }
}

impl Display for AgentEffectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Model { message } => write!(f, "the model turn is not bounded: {message}"),
            Self::ToolResultTooLarge {
                call_id,
                bytes,
                maximum,
            } => write!(
                f,
                "the result of tool call {call_id} is {bytes} bytes, which exceeds the {maximum} byte limit; it belongs behind an artifact reference"
            ),
            Self::ContentTooLarge { bytes, maximum } => write!(
                f,
                "inline effect content is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::PendingOverflow { maximum } => write!(
                f,
                "a run may not hold more than {maximum} outstanding effects"
            ),
            Self::Sink { code, message } => {
                write!(f, "the effect sink rejected a dispatch ({code}): {message}")
            }
        }
    }
}

impl Error for AgentEffectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentEffectError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentEffectError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentTaskError> for AgentEffectError {
    fn from(error: AgentTaskError) -> Self {
        match error {
            AgentTaskError::Identity(error) => Self::Identity(error),
            AgentTaskError::Schema(error) => Self::Schema(error),
            AgentTaskError::ContentTooLarge { bytes, maximum } => {
                Self::ContentTooLarge { bytes, maximum }
            }
            other => Self::Model {
                message: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::memory::AgentContextSnapshotRef;

    #[test]
    fn the_result_operation_id_folds_in_the_dispatch_generation() {
        let scope = AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("the agent id is valid"),
            AgentRunId::new("t-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid");
        let context = AgentContextSnapshotRef::for_turn(&scope, 1).expect("the reference derives");
        let mut effect = AgentRunEffect::new(
            &scope,
            1,
            0,
            AgentRunEffectRequest::Model {
                context,
                profile: None,
            },
            AgentTimestampMillis::new(1),
        )
        .expect("the effect derives");

        // Within one generation the derivation is pure: a redelivered result is
        // the same operation, and the run's log answers it once.
        let first = effect
            .result_operation_id(&scope)
            .expect("the operation id derives");
        assert_eq!(
            first,
            effect
                .result_operation_id(&scope)
                .expect("the operation id derives")
        );

        // A later generation is a different operation entirely: slice 1.7's
        // reconciliation re-dispatch must not be answered from the log entry a
        // superseded attempt left behind.
        effect.generation = effect.generation.next();
        let second = effect
            .result_operation_id(&scope)
            .expect("the operation id derives");
        assert_ne!(first, second);
    }
}
