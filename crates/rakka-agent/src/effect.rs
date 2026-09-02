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
//! 8.7. Slice 1.5 landed the interim record; slice 1.7 filled the machine and
//! integrated it with the `rakka-agent-workflow` dispatcher through
//! [`crate::dispatch`].
//!
//! # Why the record lives in the run's own state
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
//! [`AgentRunEffect::dispatch_ticket_id`].
//!
//! The agent-workflow outbox stays exactly where it belongs: the *sink* that
//! carries the dispatch ticket for the bounded external I/O, whose result
//! returns as a durable command.
//!
//! # The machine, as landed by slice 1.7
//!
//! [`AgentRunEffect`] is the durable effect intent of
//! [specification 11.1](../../../docs/plans/rakka-agent/spec.md): identity and
//! generation, safety record with external idempotency key or reconciliation
//! protocol, canonical argument digest, settings revision, timeout, credential
//! *binding* (never a resolved value), and the request itself.
//! [`AgentRunEffectStatus`] is the run-side half of the
//! [specification 11.3](../../../docs/plans/rakka-agent/spec.md) status
//! machine — see its documentation for the exact split with the dispatch
//! layer, which owns durable `Started` (outbox `Dispatching` plus the fleet
//! lease and fencing token) and `RetryScheduled`. Crash and timeout recovery
//! per safety class, and the reconciliation that resolves an
//! [`AgentRunEffectStatus::Indeterminate`] generation, live in
//! [`crate::dispatch`] and [`crate::run`].

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{
    require_agent_trace_context, AgentAttributes, AgentCausationId, AgentCorrelationId,
    AgentEffect, AgentEffectKind, AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey,
    AgentTelemetryContext, AgentTimestampMillis, PrincipalRef, StateSchemaVersion,
    AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE,
};
use rakka_agent_workflow::{AgentDeduplicationKey, AgentEffectId};
use serde::{Deserialize, Serialize};

use crate::definition::{
    AgentCredentialBindingRef, AgentEffectSafetyClass, AgentExecutionPolicyRef,
    AgentModelProfileId, AgentRevisionNumber, AgentToolId,
};
use crate::identity::{
    AgentDelegationId, AgentGoalId, AgentId, AgentIdentityError, AgentOperationId,
    AgentOperationKind, AgentRunId, AgentRunScope, AgentTaskId, KnowledgeSpaceId,
    AGENT_IDENTITY_MAX_LENGTH,
};
use crate::memory::{
    AgentContextSnapshotRef, AgentPrivateMemoryId, AgentPrivateMemoryKind, AgentPromotedMemoryRef,
    MemoryClassification, MemorySequence, AGENT_SESSION_WINDOW_MAX_ENTRIES,
};
use crate::model::{AgentModelTurn, AgentToolCallId, AgentToolCallRequest};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
};
use crate::task::{AgentContentDigest, AgentTaskContent, AgentTaskError};

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
/// A generation is one authorization to invoke: retries stay within it, and
/// only a reconciliation decision that proves the previous invocation never
/// happened mints the next one ([`AgentEffectResolution::ConfirmedNotExecuted`]).
/// A result carrying a generation the run has passed is refused rather than
/// applied, and each generation is its own dispatch ticket, attempt budget,
/// and — for an idempotent effect — external idempotency key.
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

/// Largest external idempotency key, in bytes.
pub const AGENT_EXTERNAL_IDEMPOTENCY_KEY_MAX_LENGTH: usize = 512;

/// The idempotency key handed to an external target so a repeated invocation is
/// safe ([specification 11.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is derived from the effect's identity *and its generation*: every retry
/// within one generation reuses it — which is the whole point of the
/// `Idempotent` safety class ([specification 11.4](../../../docs/plans/rakka-agent/spec.md))
/// — while a new generation, minted only when an operator proves the previous
/// invocation never happened, presents a fresh key for what is genuinely a new
/// invocation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AgentExternalIdempotencyKey(String);

impl AgentExternalIdempotencyKey {
    /// Creates a key, rejecting an empty or oversized value.
    pub fn new(value: impl Into<String>) -> AgentEffectResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(AgentEffectError::InvalidPolicy {
                message: "an external idempotency key must not be empty".to_string(),
            });
        }
        if value.len() > AGENT_EXTERNAL_IDEMPOTENCY_KEY_MAX_LENGTH {
            return Err(AgentEffectError::InvalidPolicy {
                message: format!(
                    "an external idempotency key may hold at most \
                     {AGENT_EXTERNAL_IDEMPOTENCY_KEY_MAX_LENGTH} bytes"
                ),
            });
        }
        Ok(Self(value))
    }

    /// The key value handed to the target.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for AgentExternalIdempotencyKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AgentExternalIdempotencyKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

crate::identity::validated_id! {
    /// Opaque reference to the application-owned protocol that can establish
    /// the authoritative outcome of an ambiguous `Reconcileable` attempt
    /// ([specification 11.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Rakka persists and routes the reference; the application owns the
    /// protocol behind it. The dispatch pipeline resolves it through an
    /// [`crate::dispatch::AgentEffectReconciler`].
    pub AgentReconciliationProtocolRef, "agent_reconciliation_protocol_ref"
}

/// The full effect-safety record of one effect intent
/// ([specification 11.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// [`crate::definition::AgentEffectSafetyClass`] is the bare discriminant a
/// tool *declares*; this record is what one committed effect *carries*: the
/// `Idempotent` class is only meaningful with the external key a retry must
/// reuse, and the `Reconcileable` class only with the protocol that can
/// establish an ambiguous attempt's outcome. The declaration is trusted
/// definition/setup/deployment data — model output can never choose or
/// downgrade it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEffectSafety {
    /// The effect does not change external state.
    ReadOnly,
    /// Re-invocation with the same external idempotency key is safe.
    Idempotent {
        /// The key every retry of this generation must reuse.
        external_key: AgentExternalIdempotencyKey,
    },
    /// The outcome of an ambiguous attempt can be established by an
    /// application-owned reconciliation protocol.
    Reconcileable {
        /// The protocol that establishes the authoritative outcome.
        protocol: AgentReconciliationProtocolRef,
    },
    /// An ambiguous attempt can be neither safely retried nor mechanically
    /// reconciled. Its recovery is a human decision.
    NonIdempotent,
}

impl AgentEffectSafety {
    /// The bare safety-class discriminant.
    #[must_use]
    pub const fn class(&self) -> AgentEffectSafetyClass {
        match self {
            Self::ReadOnly => AgentEffectSafetyClass::ReadOnly,
            Self::Idempotent { .. } => AgentEffectSafetyClass::Idempotent,
            Self::Reconcileable { .. } => AgentEffectSafetyClass::Reconcileable,
            Self::NonIdempotent => AgentEffectSafetyClass::NonIdempotent,
        }
    }

    /// The external idempotency key, when the class carries one.
    #[must_use]
    pub const fn external_key(&self) -> Option<&AgentExternalIdempotencyKey> {
        match self {
            Self::Idempotent { external_key } => Some(external_key),
            _ => None,
        }
    }

    /// The reconciliation protocol, when the class carries one.
    #[must_use]
    pub const fn reconciliation_protocol(&self) -> Option<&AgentReconciliationProtocolRef> {
        match self {
            Self::Reconcileable { protocol } => Some(protocol),
            _ => None,
        }
    }
}

/// What one committed effect is permitted to do about failure and ambiguity:
/// its safety class, its reconciliation protocol, its credential binding, its
/// timeout, and its attempt bound
/// ([specification 11.1](../../../docs/plans/rakka-agent/spec.md): "safety
/// class and retry/reconciliation policy").
///
/// The spec is *class-level*: the constructor derives the per-generation
/// external idempotency key when the class is `Idempotent`, because the key is
/// a function of the effect's identity and generation, which the spec cannot
/// know. [`Self::validate`] runs where a spec enters — construction,
/// deserialization, and the effect constructor — so an unenforceable
/// combination never reaches a durable record: a `NonIdempotent` spec
/// permitting a retry would override the ambiguity rule of
/// [specification 11.4](../../../docs/plans/rakka-agent/spec.md), and a
/// `Reconcileable` spec without a protocol could never be reconciled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentEffectSpec {
    /// The declared safety class.
    pub safety_class: AgentEffectSafetyClass,
    /// Most dispatch attempts one generation may make, its first included.
    pub max_attempts: u32,
    /// The reconciliation protocol; required exactly when the class is
    /// `Reconcileable`.
    pub reconciliation_protocol: Option<AgentReconciliationProtocolRef>,
    /// The logical credential binding a dispatch attempt may resolve. Never a
    /// resolved value ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// Timeout for one dispatch attempt, in milliseconds.
    pub timeout_ms: Option<u64>,
    /// The application-owned execution policy or trust class the effect's
    /// dispatch is routed through
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)). Rakka
    /// persists and routes the reference; the application owns what stands
    /// behind it.
    pub execution_policy: Option<AgentExecutionPolicyRef>,
    /// The guardrail chain revision the effect is committed under
    /// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The pin is what makes a guardrail transform deterministic *across*
    /// attempts of one generation: the dispatch pipeline refuses a retry —
    /// and any transformed execution — whose current chain revision no longer
    /// matches it, so one external idempotency key can never carry two
    /// different payloads. Stamp it with
    /// [`crate::tools::AgentToolAuthority::effect_policies`], which projects
    /// the registry and the configured chain together.
    pub guardrail_revision: Option<AgentRevisionNumber>,
    /// Whether the effect may dispatch only under a durable checkpoint grant
    /// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Projected from the tool binding's
    /// [`crate::tools::AgentToolBinding::checkpoint_required`], so the run knows
    /// at commit time to open an approval checkpoint and park rather than
    /// dispatch. A model call never sets it; a guardrail stage may still require
    /// a checkpoint dynamically at dispatch, which is a separate, dispatch-time
    /// discovery.
    #[serde(default)]
    pub checkpoint_required: bool,
    /// Whether the effect may dispatch only under a grant issued by a
    /// [`crate::checkpoints::AgentCheckpointKind::SecurityAuthorization`]
    /// checkpoint ([specification 12.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Projected from the tool binding's
    /// [`crate::tools::AgentToolBinding::authorization_required`], so the run
    /// knows at commit time to open a security-authorization checkpoint and
    /// park rather than dispatch. An approval grant does not satisfy it.
    #[serde(default)]
    pub authorization_required: bool,
}

impl AgentEffectSpec {
    /// A single read-only attempt: the conservative default for a model call.
    #[must_use]
    pub const fn read_only() -> Self {
        Self {
            safety_class: AgentEffectSafetyClass::ReadOnly,
            max_attempts: 1,
            reconciliation_protocol: None,
            credential_binding: None,
            timeout_ms: None,
            execution_policy: None,
            guardrail_revision: None,
            checkpoint_required: false,
            authorization_required: false,
        }
    }

    /// A single non-idempotent attempt: the fail-closed default for a tool the
    /// deployment has not classified.
    #[must_use]
    pub const fn non_idempotent() -> Self {
        Self {
            safety_class: AgentEffectSafetyClass::NonIdempotent,
            max_attempts: 1,
            reconciliation_protocol: None,
            credential_binding: None,
            timeout_ms: None,
            execution_policy: None,
            guardrail_revision: None,
            checkpoint_required: false,
            authorization_required: false,
        }
    }

    /// The spec a model retry policy declares
    /// ([`crate::model::AgentModelRetryPolicy`]; open decision 4).
    ///
    /// A `Reconcileable` model policy needs the protocol that reconciles a
    /// model call, so it must be supplied; the other classes carry nothing.
    pub fn for_model_policy(
        policy: crate::model::AgentModelRetryPolicy,
        reconciliation_protocol: Option<AgentReconciliationProtocolRef>,
    ) -> AgentEffectResult<Self> {
        let spec = Self {
            safety_class: policy.safety_class,
            max_attempts: policy.max_attempts,
            reconciliation_protocol,
            credential_binding: None,
            timeout_ms: None,
            execution_policy: None,
            guardrail_revision: None,
            checkpoint_required: false,
            authorization_required: false,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Sets the attempt bound.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> AgentEffectResult<Self> {
        self.max_attempts = max_attempts;
        self.validate()?;
        Ok(self)
    }

    /// Binds the effect's dispatch to a logical credential reference.
    #[must_use]
    pub fn with_credential_binding(mut self, binding: AgentCredentialBindingRef) -> Self {
        self.credential_binding = Some(binding);
        self
    }

    /// Sets the per-attempt timeout.
    #[must_use]
    pub const fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Routes the effect's dispatch through an execution policy or trust class.
    #[must_use]
    pub fn with_execution_policy(mut self, policy: AgentExecutionPolicyRef) -> Self {
        self.execution_policy = Some(policy);
        self
    }

    /// Pins the guardrail chain revision the effect commits under.
    #[must_use]
    pub const fn with_guardrail_revision(mut self, revision: AgentRevisionNumber) -> Self {
        self.guardrail_revision = Some(revision);
        self
    }

    /// Requires a durable checkpoint grant before the effect may dispatch.
    #[must_use]
    pub const fn with_checkpoint_required(mut self) -> Self {
        self.checkpoint_required = true;
        self
    }

    /// Requires a security-authorization grant before the effect may dispatch.
    #[must_use]
    pub const fn with_authorization_required(mut self) -> Self {
        self.authorization_required = true;
        self
    }

    /// Declares a reconcileable spec with its protocol.
    pub fn reconcileable(
        protocol: AgentReconciliationProtocolRef,
        max_attempts: u32,
    ) -> AgentEffectResult<Self> {
        let spec = Self {
            safety_class: AgentEffectSafetyClass::Reconcileable,
            max_attempts,
            reconciliation_protocol: Some(protocol),
            credential_binding: None,
            timeout_ms: None,
            execution_policy: None,
            guardrail_revision: None,
            checkpoint_required: false,
            authorization_required: false,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Declares an idempotent spec; the external key is derived per effect
    /// generation by the effect constructor.
    pub fn idempotent(max_attempts: u32) -> AgentEffectResult<Self> {
        let spec = Self {
            safety_class: AgentEffectSafetyClass::Idempotent,
            max_attempts,
            reconciliation_protocol: None,
            credential_binding: None,
            timeout_ms: None,
            execution_policy: None,
            guardrail_revision: None,
            checkpoint_required: false,
            authorization_required: false,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Rejects a spec the crash-and-timeout rules could not honor
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md),
    /// [11.5](../../../docs/plans/rakka-agent/spec.md)).
    pub fn validate(&self) -> AgentEffectResult<()> {
        if self.max_attempts == 0 {
            return Err(AgentEffectError::InvalidPolicy {
                message: "an effect spec must permit at least one attempt".to_string(),
            });
        }
        if self.safety_class == AgentEffectSafetyClass::NonIdempotent && self.max_attempts > 1 {
            return Err(AgentEffectError::InvalidPolicy {
                message: "a non-idempotent effect may not be auto-retried after ambiguity"
                    .to_string(),
            });
        }
        match (self.safety_class, &self.reconciliation_protocol) {
            (AgentEffectSafetyClass::Reconcileable, None) => {
                return Err(AgentEffectError::InvalidPolicy {
                    message: "a reconcileable effect must name its reconciliation protocol"
                        .to_string(),
                })
            }
            (AgentEffectSafetyClass::Reconcileable, Some(_)) => {}
            (_, Some(_)) => {
                return Err(AgentEffectError::InvalidPolicy {
                    message: "only a reconcileable effect may name a reconciliation protocol"
                        .to_string(),
                })
            }
            (_, None) => {}
        }
        Ok(())
    }

    /// Builds the full safety record for one effect generation, deriving the
    /// external idempotency key where the class calls for one.
    fn safety_for(
        &self,
        scope: &AgentRunScope,
        turn: u64,
        slot: usize,
        generation: AgentEffectGeneration,
    ) -> AgentEffectResult<AgentEffectSafety> {
        Ok(match self.safety_class {
            AgentEffectSafetyClass::ReadOnly => AgentEffectSafety::ReadOnly,
            AgentEffectSafetyClass::Idempotent => AgentEffectSafety::Idempotent {
                external_key: external_idempotency_key_for(scope, turn, slot, generation)?,
            },
            AgentEffectSafetyClass::Reconcileable => AgentEffectSafety::Reconcileable {
                protocol: self.reconciliation_protocol.clone().ok_or_else(|| {
                    AgentEffectError::InvalidPolicy {
                        message: "a reconcileable effect must name its reconciliation protocol"
                            .to_string(),
                    }
                })?,
            },
            AgentEffectSafetyClass::NonIdempotent => AgentEffectSafety::NonIdempotent,
        })
    }
}

/// The wire shape of [`AgentEffectSpec`], validated on load.
#[derive(Deserialize)]
struct AgentEffectSpecRecord {
    safety_class: AgentEffectSafetyClass,
    max_attempts: u32,
    #[serde(default)]
    reconciliation_protocol: Option<AgentReconciliationProtocolRef>,
    #[serde(default)]
    credential_binding: Option<AgentCredentialBindingRef>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    execution_policy: Option<AgentExecutionPolicyRef>,
    #[serde(default)]
    guardrail_revision: Option<AgentRevisionNumber>,
    #[serde(default)]
    checkpoint_required: bool,
    #[serde(default)]
    authorization_required: bool,
}

impl<'de> Deserialize<'de> for AgentEffectSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let record = AgentEffectSpecRecord::deserialize(deserializer)?;
        let spec = Self {
            safety_class: record.safety_class,
            max_attempts: record.max_attempts,
            reconciliation_protocol: record.reconciliation_protocol,
            credential_binding: record.credential_binding,
            timeout_ms: record.timeout_ms,
            execution_policy: record.execution_policy,
            guardrail_revision: record.guardrail_revision,
            checkpoint_required: record.checkpoint_required,
            authorization_required: record.authorization_required,
        };
        spec.validate().map_err(serde::de::Error::custom)?;
        Ok(spec)
    }
}

/// The effect specs a run stamps onto the effects its transitions commit.
///
/// This is the commit-time projection of
/// [specification 11.2](../../../docs/plans/rakka-agent/spec.md) ("the
/// registered tool or adapter supplies the permitted safety declaration"):
/// deployment configuration on the run entity, keyed by tool. Since slice 1.8
/// the source of the tool entries is the registry —
/// [`crate::tools::AgentToolRegistry::effect_policies`] projects the
/// registered bindings onto this map, and the dispatch pipeline revalidates
/// every intent against the same registry before durable `Started`. The
/// defaults fail safe: a model call is one read-only attempt, and a tool the
/// deployment has not classified is non-idempotent — an ambiguous loss parks
/// it for reconciliation rather than guessing that a retry is harmless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentEffectPolicies {
    model: AgentEffectSpec,
    tools: BTreeMap<AgentToolId, AgentEffectSpec>,
    default_tool: AgentEffectSpec,
    compensation: AgentEffectSpec,
    memory_promotion: AgentEffectSpec,
    goal_evaluation: AgentEffectSpec,
    a2a_send: AgentEffectSpec,
    workflow_start: AgentEffectSpec,
    workflow_cancel: AgentEffectSpec,
    claim_append: AgentEffectSpec,
    checkpoint_sla: crate::checkpoints::AgentCheckpointSla,
}

impl AgentEffectPolicies {
    /// The fail-safe defaults.
    #[must_use]
    pub fn new() -> Self {
        Self {
            model: AgentEffectSpec::read_only(),
            tools: BTreeMap::new(),
            default_tool: AgentEffectSpec::non_idempotent(),
            compensation: AgentEffectSpec::non_idempotent(),
            // A promotion upserts under purely derived operation ids, so a
            // repeat of any attempt converges on the same records: idempotent
            // by construction, with a small retry budget.
            memory_promotion: AgentEffectSpec {
                safety_class: AgentEffectSafetyClass::Idempotent,
                max_attempts: AGENT_MEMORY_PROMOTION_DEFAULT_MAX_ATTEMPTS,
                reconciliation_protocol: None,
                credential_binding: None,
                timeout_ms: None,
                execution_policy: None,
                guardrail_revision: None,
                checkpoint_required: false,
                authorization_required: false,
            },
            // An evaluation judges evidence and never mutates the world:
            // read-only is what makes a crash-retry safe and keeps an
            // ambiguous loss off the reconciliation path.
            goal_evaluation: AgentEffectSpec {
                safety_class: AgentEffectSafetyClass::ReadOnly,
                max_attempts: crate::evaluation::AGENT_GOAL_EVALUATION_DEFAULT_MAX_ATTEMPTS,
                reconciliation_protocol: None,
                credential_binding: None,
                timeout_ms: None,
                execution_policy: None,
                guardrail_revision: None,
                checkpoint_required: false,
                authorization_required: false,
            },
            // The send's deduplication key is a pure derivation of the
            // delegation identity, so every retry converges on the same
            // logical child: idempotent by construction, and an ambiguous
            // loss retries safely under the same key instead of parking for
            // reconciliation.
            a2a_send: AgentEffectSpec {
                safety_class: AgentEffectSafetyClass::Idempotent,
                max_attempts: crate::delegation::AGENT_A2A_SEND_DEFAULT_MAX_ATTEMPTS,
                reconciliation_protocol: None,
                credential_binding: None,
                timeout_ms: None,
                execution_policy: None,
                guardrail_revision: None,
                checkpoint_required: false,
                authorization_required: false,
            },
            // The start's command id and deduplication key are pure,
            // generation-free derivations of the invocation identity, so
            // every retry converges on the same child run: idempotent by
            // construction — the workflow *behind* the start keeps its own
            // declared safety, which the descriptor overlays at commit.
            workflow_start: AgentEffectSpec {
                safety_class: AgentEffectSafetyClass::Idempotent,
                max_attempts: crate::workflow_tool::AGENT_WORKFLOW_START_DEFAULT_MAX_ATTEMPTS,
                reconciliation_protocol: None,
                credential_binding: None,
                timeout_ms: None,
                execution_policy: None,
                guardrail_revision: None,
                checkpoint_required: false,
                authorization_required: false,
            },
            // The cancel's command id and deduplication key are pure,
            // generation-free derivations of the invocation identity, so
            // every retry converges on one logical request: idempotent by
            // construction, and the request authorizes nothing — an
            // ambiguous loss retries safely instead of parking a wind-down
            // for reconciliation.
            workflow_cancel: AgentEffectSpec {
                safety_class: AgentEffectSafetyClass::Idempotent,
                max_attempts: crate::workflow_tool::AGENT_WORKFLOW_CANCEL_DEFAULT_MAX_ATTEMPTS,
                reconciliation_protocol: None,
                credential_binding: None,
                timeout_ms: None,
                execution_policy: None,
                guardrail_revision: None,
                checkpoint_required: false,
                authorization_required: false,
            },
            // The append's operation id derives from the intent's external
            // idempotency key, and the store's ledger answers every replay
            // with the original claim: idempotent by construction.
            claim_append: AgentEffectSpec {
                safety_class: AgentEffectSafetyClass::Idempotent,
                max_attempts: AGENT_CLAIM_APPEND_DEFAULT_MAX_ATTEMPTS,
                reconciliation_protocol: None,
                credential_binding: None,
                timeout_ms: None,
                execution_policy: None,
                guardrail_revision: None,
                checkpoint_required: false,
                authorization_required: false,
            },
            checkpoint_sla: crate::checkpoints::AgentCheckpointSla::default(),
        }
    }

    /// Sets the SLA and expiration deadlines a run stamps onto every approval
    /// checkpoint it opens ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn with_checkpoint_sla(mut self, sla: crate::checkpoints::AgentCheckpointSla) -> Self {
        self.checkpoint_sla = sla;
        self
    }

    /// The checkpoint SLA the deployment configured.
    #[must_use]
    pub const fn checkpoint_sla(&self) -> &crate::checkpoints::AgentCheckpointSla {
        &self.checkpoint_sla
    }

    /// Sets the spec model calls dispatch under.
    pub fn with_model_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.model = spec;
        Ok(self)
    }

    /// Registers the spec one tool's calls dispatch under.
    pub fn with_tool_spec(
        mut self,
        tool: AgentToolId,
        spec: AgentEffectSpec,
    ) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.tools.insert(tool, spec);
        Ok(self)
    }

    /// Sets the spec unclassified tools dispatch under.
    pub fn with_default_tool_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.default_tool = spec;
        Ok(self)
    }

    /// Sets the spec compensation effects dispatch under
    /// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is one non-idempotent attempt.
    pub fn with_compensation_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.compensation = spec;
        Ok(self)
    }

    /// Sets the spec memory promotions dispatch under
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is [`AGENT_MEMORY_PROMOTION_DEFAULT_MAX_ATTEMPTS`] idempotent
    /// attempts.
    pub fn with_memory_promotion_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.memory_promotion = spec;
        Ok(self)
    }

    /// Sets the spec goal evaluations dispatch under
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is
    /// [`crate::evaluation::AGENT_GOAL_EVALUATION_DEFAULT_MAX_ATTEMPTS`]
    /// read-only attempts.
    pub fn with_goal_evaluation_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.goal_evaluation = spec;
        Ok(self)
    }

    /// Sets the spec outbound A2A sends dispatch under
    /// ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is
    /// [`crate::delegation::AGENT_A2A_SEND_DEFAULT_MAX_ATTEMPTS`] idempotent
    /// attempts.
    pub fn with_a2a_send_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.a2a_send = spec;
        Ok(self)
    }

    /// Sets the spec workflow starts dispatch under
    /// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is
    /// [`crate::workflow_tool::AGENT_WORKFLOW_START_DEFAULT_MAX_ATTEMPTS`]
    /// idempotent attempts.
    pub fn with_workflow_start_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.workflow_start = spec;
        Ok(self)
    }

    /// Sets the spec workflow cancels dispatch under
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is
    /// [`crate::workflow_tool::AGENT_WORKFLOW_CANCEL_DEFAULT_MAX_ATTEMPTS`]
    /// idempotent attempts.
    pub fn with_workflow_cancel_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.workflow_cancel = spec;
        Ok(self)
    }

    /// Sets the spec communal claim appends dispatch under
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)). The
    /// default is [`AGENT_CLAIM_APPEND_DEFAULT_MAX_ATTEMPTS`] idempotent
    /// attempts.
    pub fn with_claim_append_spec(mut self, spec: AgentEffectSpec) -> AgentEffectResult<Self> {
        spec.validate()?;
        self.claim_append = spec;
        Ok(self)
    }

    /// Pins every spec — model, registered tools, and the unclassified
    /// default — to the guardrail chain revision the deployment evaluates at
    /// dispatch, so each committed intent records the policy its transforms
    /// are deterministic under. Prefer deriving policies through
    /// [`crate::tools::AgentToolAuthority::effect_policies`], which applies
    /// this stamp from the chain it actually holds.
    #[must_use]
    pub fn with_guardrail_revision(mut self, revision: AgentRevisionNumber) -> Self {
        self.model.guardrail_revision = Some(revision);
        self.default_tool.guardrail_revision = Some(revision);
        self.memory_promotion.guardrail_revision = Some(revision);
        self.goal_evaluation.guardrail_revision = Some(revision);
        self.a2a_send.guardrail_revision = Some(revision);
        self.workflow_start.guardrail_revision = Some(revision);
        self.workflow_cancel.guardrail_revision = Some(revision);
        self.claim_append.guardrail_revision = Some(revision);
        for spec in self.tools.values_mut() {
            spec.guardrail_revision = Some(revision);
        }
        self
    }

    /// Stamps the trust class every effect *Rakka itself* commits runs under:
    /// the model call, a compensation, a memory promotion, a goal evaluation,
    /// an A2A send, a workflow start or cancel, and a claim append
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// These are the effects no application declaration names, and without
    /// this they can carry no class at all — which is what makes
    /// [`crate::tools::AgentToolAuthority::with_required_execution_policy`]
    /// enforceable rather than a switch that refuses every run's first model
    /// call.
    ///
    /// Registered tools are deliberately untouched: a tool's class comes from
    /// its [`crate::tools::AgentToolBinding`] declaration, and the dispatch
    /// gate refuses an intent whose class disagrees with that binding. So is
    /// the unclassified-tool default, which must stay refusable — a tool the
    /// deployment never classified is exactly what a required execution
    /// policy exists to catch.
    ///
    /// Prefer deriving policies through
    /// [`crate::tools::AgentToolAuthority::effect_policies`], which applies
    /// this stamp from the class the authority requires, so the gate and the
    /// specs that satisfy it cannot drift.
    #[must_use]
    pub fn with_substrate_execution_policy(mut self, policy: AgentExecutionPolicyRef) -> Self {
        for spec in [
            &mut self.model,
            &mut self.compensation,
            &mut self.memory_promotion,
            &mut self.goal_evaluation,
            &mut self.a2a_send,
            &mut self.workflow_start,
            &mut self.workflow_cancel,
            &mut self.claim_append,
        ] {
            spec.execution_policy = Some(policy.clone());
        }
        self
    }

    /// The spec one request dispatches under.
    #[must_use]
    pub fn spec_for(&self, request: &AgentRunEffectRequest) -> &AgentEffectSpec {
        match request {
            AgentRunEffectRequest::Model { .. } => &self.model,
            AgentRunEffectRequest::Tool { call } => {
                self.tools.get(&call.tool).unwrap_or(&self.default_tool)
            }
            AgentRunEffectRequest::Compensation { .. } => &self.compensation,
            AgentRunEffectRequest::MemoryPromotion { .. } => &self.memory_promotion,
            AgentRunEffectRequest::Evaluation { .. } => &self.goal_evaluation,
            AgentRunEffectRequest::A2aSend { .. } | AgentRunEffectRequest::A2aHandoff { .. } => {
                &self.a2a_send
            }
            AgentRunEffectRequest::WorkflowStart { .. } => &self.workflow_start,
            AgentRunEffectRequest::WorkflowCancel { .. } => &self.workflow_cancel,
            AgentRunEffectRequest::ClaimAppend { .. } => &self.claim_append,
        }
    }
}

impl Default for AgentEffectPolicies {
    fn default() -> Self {
        Self::new()
    }
}

/// Derives the external idempotency key of one effect generation.
///
/// Within a generation the derivation is pure, so every retry hands the target
/// the same key ([specification 11.4](../../../docs/plans/rakka-agent/spec.md));
/// a new generation — minted only after an operator proves the previous
/// invocation never happened — derives a fresh one.
pub fn external_idempotency_key_for(
    scope: &AgentRunScope,
    turn: u64,
    slot: usize,
    generation: AgentEffectGeneration,
) -> AgentEffectResult<AgentExternalIdempotencyKey> {
    let effect_id = effect_id_for(scope, turn, slot)?;
    AgentExternalIdempotencyKey::new(format!("{}#g{generation}", effect_id.as_str()))
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
    /// An application-defined compensation request, scheduled by a
    /// [`crate::checkpoints::AgentReconciliationDecision::Compensate`] decision
    /// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
    CompensationCall,
    /// A promotion of session-memory entries into the agent's private
    /// long-term store ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    MemoryPromotionCall,
    /// An evaluation of the goal's success criteria against durable evidence
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    GoalEvaluationCall,
    /// An outbound agent-to-agent send carrying one durable delegation
    /// ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)).
    A2aSendCall,
    /// The start-or-adopt command of one durable workflow-tool invocation
    /// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)). The
    /// effect is the start, never the workflow: the child run's internal
    /// effects keep their own durable boundaries.
    WorkflowStartCall,
    /// The cancel command of one durable workflow-tool invocation
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)). The
    /// effect is the request, never the stop: delivering it to the child's
    /// durable inbox is all it ever claims, and the child's terminal outcome
    /// still returns through its own result relay.
    WorkflowCancelCall,
    /// An append of one provenance-bearing claim into a communal knowledge
    /// space ([specification 8.5 and 13.4](../../../docs/plans/rakka-agent/spec.md)):
    /// idempotent by construction — the store's append ledger converges every
    /// replay of the derived operation on the original claim.
    ClaimAppendCall,
}

impl AgentRunEffectKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::ModelCall => "model-call",
            Self::ToolCall => "tool-call",
            Self::CompensationCall => "compensation-call",
            Self::MemoryPromotionCall => "memory-promotion-call",
            Self::GoalEvaluationCall => "goal-evaluation-call",
            Self::A2aSendCall => "a2a-send-call",
            Self::WorkflowStartCall => "workflow-start-call",
            Self::WorkflowCancelCall => "workflow-cancel-call",
            Self::ClaimAppendCall => "claim-append-call",
        }
    }

    /// The agent-workflow effect kind this dispatches as.
    #[must_use]
    pub const fn workflow_kind(self) -> AgentEffectKind {
        match self {
            Self::ModelCall => AgentEffectKind::ModelCall,
            // A compensation, memory promotion, goal evaluation, A2A send,
            // workflow start, or workflow cancel dispatches through the same
            // adapter surface as a tool call: the outbox ticket's target type
            // (`compensation`, `memory-promotion`, `goal-evaluation`,
            // `a2a-peer`, `workflow-tool`, `workflow-cancel`) is what routes
            // it to its executor.
            Self::ToolCall
            | Self::CompensationCall
            | Self::MemoryPromotionCall
            | Self::GoalEvaluationCall
            | Self::A2aSendCall
            | Self::WorkflowStartCall
            | Self::WorkflowCancelCall
            | Self::ClaimAppendCall => AgentEffectKind::ToolCall,
        }
    }

    /// Whether an effect of this kind may still be handed to the outbox and
    /// dispatched while its run winds down.
    ///
    /// The wind-down fence forbids new dispatch
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)), with
    /// exactly two exemptions, each a piece of work the wind-down itself
    /// authorizes: the compensation an operator's `Compensate` decision
    /// schedules after the fence
    /// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)), and
    /// the workflow-cancel the wind-down owes its started child workflows.
    #[must_use]
    pub const fn exempt_from_wind_down_fence(self) -> bool {
        matches!(self, Self::CompensationCall | Self::WorkflowCancelCall)
    }
}

impl Display for AgentRunEffectKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Where one effect generation stands, as the run's own durable record holds
/// it ([specification 11.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The specification's effect state model spans two durable layers, and this
/// enum is the run-side half. The run's record owns the states the *run*
/// transitions through; the dispatch layer's durable records — the
/// agent-workflow outbox entry and the dispatcher-fleet entry with its lease
/// and fencing token — own the attempt-level states between `Ready` and a
/// terminal outcome:
///
/// | Specification 11.3 | Durable record |
/// | --- | --- |
/// | `Pending` | here: committed, provably not yet handed to the sink |
/// | `Ready` | here: handed to the durable outbox, dispatchable |
/// | `Started` | outbox `Dispatching` + fleet lease/fence, before invocation |
/// | `RetryScheduled` | outbox/fleet `RetryScheduled` |
/// | `Succeeded` … `Cancelled` | here: the generation's terminal outcome |
///
/// `Pending` is load-bearing: an effect is handed to the sink only *after* the
/// transition that marked it [`Self::Ready`] committed, so a `Pending` effect
/// has provably never reached the outbox — which is what lets a cancellation
/// fence it in place without risking the abandonment of an invocation that
/// might already be running ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// `Succeeded`, `Failed`, `Exhausted`, `Indeterminate`, and `Cancelled` are
/// terminal **for one generation**. `Indeterminate` alone does not release the
/// run: the outcome is unknown, so the effect still blocks the run's
/// settlement until an explicit reconciliation decision resolves it — and a
/// decision that the invocation never happened mints a *new* generation rather
/// than mutating this one back into a routine retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEffectStatus {
    /// Committed by a run transition and provably not yet handed to the sink.
    Pending,
    /// Handed to the durable outbox; the dispatch layer owns the attempt.
    Ready,
    /// The generation produced its bounded result.
    Succeeded,
    /// The generation failed definitively.
    Failed,
    /// The generation's retry budget was spent without a result.
    Exhausted,
    /// The generation's outcome is unknowable mechanically; an explicit
    /// reconciliation decision is owed
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
    Indeterminate,
    /// The generation's ambiguity was closed by an explicitly scheduled
    /// compensation effect ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The outcome itself was never established — what resolves the generation
    /// is the operator's decision that the scheduled compensation, not further
    /// evidence, settles the ambiguity.
    Compensated,
    /// The generation was fenced before any invocation could have happened.
    Cancelled,
}

impl AgentRunEffectStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Exhausted => "exhausted",
            Self::Indeterminate => "indeterminate",
            Self::Compensated => "compensated",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the run is still waiting on this effect's result.
    #[must_use]
    pub const fn is_outstanding(self) -> bool {
        matches!(self, Self::Pending | Self::Ready)
    }

    /// Whether the generation has a terminal outcome.
    #[must_use]
    pub const fn is_resolved(self) -> bool {
        !self.is_outstanding()
    }

    /// Whether the effect blocks the run from becoming terminal.
    ///
    /// An outstanding effect does, and so does an [`Self::Indeterminate`] one:
    /// its outcome is unknown, and a run that went terminal over it would
    /// abandon work whose consequences may be real
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md),
    /// [11.5](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn blocks_settlement(self) -> bool {
        self.is_outstanding() || matches!(self, Self::Indeterminate)
    }

    /// The agent-workflow status this projects as.
    #[must_use]
    pub const fn workflow_status(self) -> AgentEffectStatus {
        match self {
            Self::Pending | Self::Ready => AgentEffectStatus::Scheduled,
            Self::Succeeded => AgentEffectStatus::Completed,
            // The workflow substrate has no indeterminate or compensated
            // status; the run's record is authoritative, and neither
            // generation is ever projected for dispatch.
            Self::Failed | Self::Indeterminate | Self::Compensated => AgentEffectStatus::Failed,
            Self::Exhausted => AgentEffectStatus::Exhausted,
            Self::Cancelled => AgentEffectStatus::Cancelled,
        }
    }
}

impl Display for AgentRunEffectStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Most session entries one memory promotion may select.
///
/// The same bound as the snapshot window: a promotion selects from what a
/// turn's working set could have seen, and the executor reads the selection in
/// one bounded page.
pub const AGENT_MEMORY_PROMOTION_MAX_ENTRIES: usize = AGENT_SESSION_WINDOW_MAX_ENTRIES;

/// The default attempt bound of a memory-promotion effect.
pub const AGENT_MEMORY_PROMOTION_DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// What one memory promotion selects and where it writes
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The payload is selection *references* only — sequences, a kind, an optional
/// consolidation target — never content: the intent stays bounded, and the
/// executor's session reads happen inside the bounded dispatch attempt, not in
/// a run transition ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AgentMemoryPromotionRequest {
    /// The first selected session sequence, inclusive.
    pub from_sequence: MemorySequence,
    /// The last selected session sequence, inclusive.
    pub to_sequence: MemorySequence,
    /// The private-memory kind the promoted records take.
    pub kind: AgentPrivateMemoryKind,
    /// A consolidation target: update exactly one existing memory under
    /// compare-and-set instead of deriving fresh identities.
    #[serde(default)]
    pub target: Option<AgentMemoryConsolidationTarget>,
    /// The confidence stamped on the promoted records, in basis points.
    pub confidence_bps: u16,
    /// Who asked for the promotion. Provenance and audit, never authority.
    pub requested_by: PrincipalRef,
}

impl AgentMemoryPromotionRequest {
    /// How many session entries the selection spans, when it is well-formed.
    #[must_use]
    pub const fn selected_entries(&self) -> Option<u64> {
        if self.from_sequence.get() == 0 || self.to_sequence.get() < self.from_sequence.get() {
            return None;
        }
        Some(self.to_sequence.get() - self.from_sequence.get() + 1)
    }
}

/// Most evidence artifacts one claim append may carry — the graph record's
/// own cap, enforced here so a refused request never reaches the store.
pub const AGENT_CLAIM_APPEND_MAX_EVIDENCE: usize = 16;

/// Longest inline claim-object value, in bytes — the graph record's own
/// bound, enforced here so a refused request never reaches the store.
pub const AGENT_CLAIM_APPEND_OBJECT_MAX_BYTES: usize = 4096;

/// The default attempt bound of a claim-append effect.
pub const AGENT_CLAIM_APPEND_DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Highest confidence one claim append may assert, in basis points — the
/// graph record's own range, enforced here so a refused request never reaches
/// the store.
pub const AGENT_CLAIM_APPEND_MAX_CONFIDENCE_BPS: u16 = 10_000;

/// The object half of a requested communal claim
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)), mirroring
/// the graph's own statement shape: an edge to another node, or a bounded
/// literal value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentClaimObjectRequest {
    /// An edge to another node, by its raw node id.
    Node(String),
    /// A bounded literal or artifact-referenced value.
    Value(AgentTaskContent),
}

/// What one communal claim append asserts and where
/// ([specification 8.5 and 13.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// The statement is caller input; the provenance never is — it rides beside
/// this request as [`AgentClaimAppendProvenance`], stamped from durable run
/// identity by the committing transition, so no initiator can assert work in
/// another scope's name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AgentClaimAppendRequest {
    /// The communal knowledge space the claim lands in, validated against
    /// the run's delegated grant and the definition envelope at the door.
    pub space: KnowledgeSpaceId,
    /// The subject node's raw id.
    pub subject: String,
    /// The predicate's raw label.
    pub predicate: String,
    /// The object half of the statement.
    pub object: AgentClaimObjectRequest,
    /// The asserted confidence, in basis points.
    pub confidence_bps: u16,
    /// The claim's classification.
    pub classification: MemoryClassification,
    /// Evidence artifact references, at most
    /// [`AGENT_CLAIM_APPEND_MAX_EVIDENCE`].
    #[serde(default)]
    pub evidence: Vec<rakka_agent_workflow::ArtifactRef>,
    /// Who asked for the append. Provenance and audit, never authority.
    pub requested_by: PrincipalRef,
}

impl AgentClaimAppendRequest {
    /// Rejects a request that exceeds its structural bounds.
    pub fn validate(&self) -> AgentEffectResult<()> {
        let bounded_segment = |label: &str, value: &str| {
            if value.is_empty() || value.len() > AGENT_IDENTITY_MAX_LENGTH {
                return Err(AgentEffectError::InvalidPolicy {
                    message: format!(
                        "the claim {label} is {} bytes; it must be non-empty and at most {}",
                        value.len(),
                        AGENT_IDENTITY_MAX_LENGTH
                    ),
                });
            }
            Ok(())
        };
        bounded_segment("subject", &self.subject)?;
        bounded_segment("predicate", &self.predicate)?;
        match &self.object {
            AgentClaimObjectRequest::Node(node) => bounded_segment("object node", node)?,
            AgentClaimObjectRequest::Value(content) => {
                content.validate()?;
                let bytes = content.size_bytes();
                if bytes > AGENT_CLAIM_APPEND_OBJECT_MAX_BYTES {
                    return Err(AgentEffectError::InvalidPolicy {
                        message: format!(
                            "the claim object value is {bytes} bytes, which exceeds the \
                             {AGENT_CLAIM_APPEND_OBJECT_MAX_BYTES} byte bound"
                        ),
                    });
                }
            }
        }
        if self.evidence.len() > AGENT_CLAIM_APPEND_MAX_EVIDENCE {
            return Err(AgentEffectError::InvalidPolicy {
                message: format!(
                    "the claim names {} evidence artifacts, which exceeds the {} bound",
                    self.evidence.len(),
                    AGENT_CLAIM_APPEND_MAX_EVIDENCE
                ),
            });
        }
        // Basis points, so the store refuses anything past ten thousand. The
        // check belongs here for the same reason the bounds above do: a
        // request refused at the door costs the caller an error, while one
        // refused at dispatch has already reserved the run's attempts and
        // spent an effect slot on a claim that can never land.
        if self.confidence_bps > AGENT_CLAIM_APPEND_MAX_CONFIDENCE_BPS {
            return Err(AgentEffectError::InvalidPolicy {
                message: format!(
                    "the claim asserts {} basis points of confidence, which exceeds the {} bound",
                    self.confidence_bps, AGENT_CLAIM_APPEND_MAX_CONFIDENCE_BPS
                ),
            });
        }
        Ok(())
    }
}

/// The provenance one claim append carries
/// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md): a
/// collaboration claim includes goal, task, source agent/run, and delegation
/// identity).
///
/// Stamped only by the run transition, from durable state — never command
/// input — which is what makes forged provenance unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AgentClaimAppendProvenance {
    /// The agent asserting the claim.
    pub agent: AgentId,
    /// The goal the assertion serves, when the run serves one.
    #[serde(default)]
    pub goal: Option<AgentGoalId>,
    /// The task the assertion serves.
    pub task: AgentTaskId,
    /// The run that produced the assertion.
    pub run: AgentRunId,
    /// The delegation this run works under, when it works under one.
    #[serde(default)]
    pub delegation: Option<AgentDelegationId>,
}

/// The existing memory one consolidation updates, at the exact revision the
/// initiator read ([specification 13.3](../../../docs/plans/rakka-agent/spec.md);
/// open decision 1's compare-and-set rule).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AgentMemoryConsolidationTarget {
    /// The memory to update.
    pub memory_id: AgentPrivateMemoryId,
    /// The revision the update expects; a stale expectation is refused.
    pub expected_revision: AgentRevisionNumber,
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
    /// Invoke the explicitly defined compensation an operator scheduled for an
    /// ambiguous effect ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Rakka persists and routes the reference; the application owns the
    /// compensation behind it. The record names the effect generation being
    /// compensated, so the handler and every audit trail can tie the two
    /// together.
    Compensation {
        /// The application-defined compensation to invoke.
        compensation: crate::checkpoints::AgentCompensationRef,
        /// The effect whose ambiguous generation is being compensated.
        compensated: AgentEffectId,
        /// The generation being compensated.
        compensated_generation: AgentEffectGeneration,
    },
    /// Promote selected session-memory entries into the agent's private
    /// long-term store ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)):
    /// an idempotent durable effect, committed by a deduplicated run command
    /// and executed by the dispatcher's promotion executor.
    MemoryPromotion {
        /// What to promote and where.
        promotion: Box<AgentMemoryPromotionRequest>,
    },
    /// Evaluate the goal's current success-criteria revision against durable
    /// evidence ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)):
    /// a read-only durable effect, committed by a deduplicated run command and
    /// executed by the dispatcher's evaluation executor. Its completed record
    /// is what a criteria decision rests on.
    Evaluation {
        /// What to evaluate, as which evaluator, by which method.
        evaluation: Box<crate::evaluation::AgentGoalEvaluationRequest>,
    },
    /// Send one durable delegation to a specialist agent over `rakka-a2a`
    /// ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)): an
    /// idempotent durable effect whose payload *is* the delegation record
    /// persisted alongside it — what was persisted is exactly what is sent,
    /// and a replay re-sends it verbatim.
    ///
    /// This variant is constructible only by the loop's delegation
    /// interception; model output can never produce it, which is one half of
    /// why a generic tool cannot reach a peer.
    A2aSend {
        /// The delegation record the send carries.
        delegation: Box<crate::delegation::AgentDelegationRecord>,
    },
    /// Send one durable handoff to the task's surface over `rakka-a2a`
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)): an
    /// idempotent durable effect whose payload *is* the handoff record
    /// persisted alongside it — what was persisted is exactly what is sent,
    /// and a replay re-sends it verbatim, converging on the recorded
    /// transfer through the derived deduplication key.
    ///
    /// This variant is constructible only by the loop's handoff
    /// interception; model output can never produce it.
    A2aHandoff {
        /// The handoff record the send carries.
        handoff: Box<crate::coordination::AgentHandoffRecord>,
    },
    /// Start — or adopt — the one durable child workflow run of a
    /// workflow-tool invocation
    /// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)): an
    /// idempotent durable effect whose payload *is* the invocation record
    /// persisted alongside it, and whose derived, generation-free `StartRun`
    /// identities make every replay converge on the same child run.
    ///
    /// This variant is constructible only by the loop's workflow-tool
    /// interception; model output can never produce it.
    WorkflowStart {
        /// The invocation record the start carries.
        invocation: Box<crate::workflow_tool::AgentWorkflowInvocationRecord>,
    },
    /// Deliver the durable cancellation request of one workflow-tool
    /// invocation to its child workflow run
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)): an
    /// idempotent durable effect whose derived, generation-free `CancelRun`
    /// identities make every replay converge on one logical request. The
    /// effect is the request, never the stop — the child's terminal outcome
    /// still returns through its own result relay, and delivery is never
    /// proof its started internal effects stopped.
    ///
    /// This variant is constructible only by the run's wind-down commit;
    /// model output can never produce it.
    WorkflowCancel {
        /// The invocation record whose child is asked to cancel.
        invocation: Box<crate::workflow_tool::AgentWorkflowInvocationRecord>,
        /// The bounded reason carried to the child.
        reason: String,
    },
    /// Append one provenance-bearing claim into a communal knowledge space
    /// ([specification 8.5 and 13.4](../../../docs/plans/rakka-agent/spec.md)):
    /// an idempotent durable effect, committed by a deduplicated run command
    /// and executed by the dispatcher's claim-append executor. The statement
    /// is the command's; the provenance is stamped from durable run identity
    /// by the committing transition and can never be forged by an initiator.
    ClaimAppend {
        /// The statement to append and the space it lands in.
        append: Box<AgentClaimAppendRequest>,
        /// The transition-stamped provenance the claim records.
        provenance: Box<AgentClaimAppendProvenance>,
    },
}

impl AgentRunEffectRequest {
    /// The effect kind this request dispatches as.
    #[must_use]
    pub const fn kind(&self) -> AgentRunEffectKind {
        match self {
            Self::Model { .. } => AgentRunEffectKind::ModelCall,
            Self::Tool { .. } => AgentRunEffectKind::ToolCall,
            Self::Compensation { .. } => AgentRunEffectKind::CompensationCall,
            Self::MemoryPromotion { .. } => AgentRunEffectKind::MemoryPromotionCall,
            Self::Evaluation { .. } => AgentRunEffectKind::GoalEvaluationCall,
            Self::A2aSend { .. } | Self::A2aHandoff { .. } => AgentRunEffectKind::A2aSendCall,
            Self::WorkflowStart { .. } => AgentRunEffectKind::WorkflowStartCall,
            Self::WorkflowCancel { .. } => AgentRunEffectKind::WorkflowCancelCall,
            Self::ClaimAppend { .. } => AgentRunEffectKind::ClaimAppendCall,
        }
    }

    /// The tool call, when this is a tool request.
    #[must_use]
    pub fn tool_call(&self) -> Option<&AgentToolCallRequest> {
        match self {
            Self::Model { .. }
            | Self::Compensation { .. }
            | Self::MemoryPromotion { .. }
            | Self::Evaluation { .. }
            | Self::A2aSend { .. }
            | Self::A2aHandoff { .. }
            | Self::WorkflowStart { .. }
            | Self::WorkflowCancel { .. }
            | Self::ClaimAppend { .. } => None,
            Self::Tool { call } => Some(call),
        }
    }

    /// Canonical fingerprint of the request
    /// ([specification 11.1](../../../docs/plans/rakka-agent/spec.md): the
    /// intent's canonical argument digest).
    ///
    /// The digest is computed over the request's canonical JSON encoding, so
    /// two structurally equal requests always fingerprint alike. It is a
    /// content fingerprint, not a security boundary; slice 1.10's digest-bound
    /// grants add a cryptographic algorithm.
    pub fn argument_digest(&self) -> AgentEffectResult<AgentContentDigest> {
        let value = serde_json::to_value(self).map_err(|error| AgentEffectError::Model {
            message: format!("the effect request could not be encoded: {error}"),
        })?;
        Ok(AgentContentDigest::of_json(&value))
    }

    /// Cryptographic digest of the request, for a digest-bound authorization
    /// grant ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A checkpoint grant binds this SHA-256 digest, not the FNV fingerprint of
    /// [`Self::argument_digest`]: only a collision-resistant digest can decide
    /// whether a human's approval still binds the exact arguments a dispatch is
    /// about to send. It is computed over the same canonical encoding, so an
    /// argument that changed after approval necessarily changes the digest and
    /// invalidates the grant.
    pub fn cryptographic_argument_digest(&self) -> AgentEffectResult<AgentContentDigest> {
        let value = serde_json::to_value(self).map_err(|error| AgentEffectError::Model {
            message: format!("the effect request could not be encoded: {error}"),
        })?;
        Ok(AgentContentDigest::sha256_of_json(&value))
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
            Self::Compensation { compensation, .. } => AgentEffectTarget {
                target_type: "compensation".to_string(),
                name: compensation.as_str().to_string(),
                address: None,
                attributes: BTreeMap::new(),
            },
            Self::MemoryPromotion { promotion } => AgentEffectTarget {
                target_type: "memory-promotion".to_string(),
                name: promotion.kind.as_label().to_string(),
                address: None,
                attributes: BTreeMap::new(),
            },
            Self::Evaluation { evaluation } => AgentEffectTarget {
                target_type: "goal-evaluation".to_string(),
                name: evaluation.evaluator.to_string(),
                address: None,
                attributes: BTreeMap::new(),
            },
            Self::A2aSend { delegation } => AgentEffectTarget {
                target_type: "a2a-peer".to_string(),
                name: delegation.resolved.agent.to_string(),
                address: delegation.resolved.endpoint.clone(),
                attributes: BTreeMap::from([
                    ("skill".to_string(), delegation.requested_skill.to_string()),
                    ("delegation".to_string(), delegation.delegation.to_string()),
                ]),
            },
            Self::A2aHandoff { handoff } => AgentEffectTarget {
                target_type: "a2a-peer".to_string(),
                name: handoff.resolved.agent.to_string(),
                address: handoff.resolved.endpoint.clone(),
                attributes: BTreeMap::from([
                    ("skill".to_string(), handoff.requested_skill.to_string()),
                    ("handoff".to_string(), handoff.handoff.to_string()),
                ]),
            },
            Self::WorkflowStart { invocation } => AgentEffectTarget {
                target_type: "workflow-tool".to_string(),
                name: invocation.workflow_tool.to_string(),
                address: None,
                attributes: BTreeMap::from([
                    (
                        "workflow-type".to_string(),
                        invocation.workflow_type.clone(),
                    ),
                    (
                        "definition-version".to_string(),
                        invocation.definition_version.as_str().to_string(),
                    ),
                    ("invocation".to_string(), invocation.invocation.to_string()),
                    ("child-run".to_string(), invocation.child_run.to_string()),
                ]),
            },
            Self::WorkflowCancel { invocation, .. } => AgentEffectTarget {
                target_type: "workflow-cancel".to_string(),
                name: invocation.workflow_tool.to_string(),
                address: None,
                attributes: BTreeMap::from([
                    (
                        "workflow-type".to_string(),
                        invocation.workflow_type.clone(),
                    ),
                    ("invocation".to_string(), invocation.invocation.to_string()),
                    ("child-run".to_string(), invocation.child_run.to_string()),
                ]),
            },
            Self::ClaimAppend { append, .. } => AgentEffectTarget {
                target_type: "claim-append".to_string(),
                name: append.space.to_string(),
                address: None,
                attributes: BTreeMap::new(),
            },
        }
    }
}

/// One effect a run committed: its durable effect intent and where its current
/// generation stands
/// ([specification 11.1](../../../docs/plans/rakka-agent/spec.md),
/// [9.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is a component of the run's durable state, written by the transition that
/// decided it. See the module documentation for why it cannot be a second write
/// to the agent-workflow outbox instead. The identity fields specification 11.1
/// requires — tenant, goal, task, agent, run — are the surrounding record's:
/// the effect lives inside [`crate::run::AgentRunState`], whose scope and loop
/// state carry them, and [`Self::to_workflow_effect`] stamps them onto the
/// self-contained dispatch ticket. A resolved credential appears nowhere: the
/// record carries only the logical binding reference, resolved inside the
/// dispatcher's bounded attempt
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
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
    /// The full safety record of the current generation
    /// ([specification 11.2](../../../docs/plans/rakka-agent/spec.md)).
    pub safety: AgentEffectSafety,
    /// Most dispatch attempts the current generation may make.
    pub max_attempts: u32,
    /// Canonical fingerprint of the request, so a grant or an operator can tell
    /// that the arguments a decision was made about are the arguments being
    /// dispatched ([specification 11.1](../../../docs/plans/rakka-agent/spec.md)).
    pub argument_digest: AgentContentDigest,
    /// The settings revision the effect was committed under.
    pub settings_revision: AgentRevisionNumber,
    /// The logical credential binding a dispatch attempt may resolve.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// The application-owned execution policy or trust class the dispatch is
    /// routed through
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)).
    #[serde(default)]
    pub execution_policy: Option<AgentExecutionPolicyRef>,
    /// The guardrail chain revision the effect was committed under
    /// ([specification 16](../../../docs/plans/rakka-agent/spec.md)). The
    /// dispatch pipeline refuses an attempt whose current chain no longer
    /// matches this pin whenever payload identity across attempts is at stake
    /// — a transformed call, or any attempt after the first.
    #[serde(default)]
    pub guardrail_revision: Option<AgentRevisionNumber>,
    /// Timeout for one dispatch attempt, in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Deadline after which the effect must not be dispatched.
    pub deadline_at: Option<AgentTimestampMillis>,
    /// Where the current generation stands.
    pub status: AgentRunEffectStatus,
    /// The idempotency key handed to the target: the external key when the
    /// safety class carries one, the derived internal key otherwise.
    pub idempotency_key: AgentIdempotencyKey,
    /// Dispatch attempts of the current generation, as last reported by the
    /// dispatch layer.
    pub attempts: u32,
    /// The lease fencing token of the last reported attempt.
    pub last_fence: Option<u64>,
    /// When the deciding transition committed it.
    pub created_at: AgentTimestampMillis,
    /// When the current generation was handed to the sink.
    pub dispatched_at: Option<AgentTimestampMillis>,
    /// Stable code of the last dispatch or execution failure.
    pub last_error_code: Option<String>,
    /// Whether the effect may dispatch only under a durable checkpoint grant
    /// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)). Projected
    /// from the tool binding at commit time, so the run parks on an approval
    /// checkpoint before dispatch rather than being refused at the authority.
    #[serde(default)]
    pub checkpoint_required: bool,
    /// Whether the effect may dispatch only under a grant issued by a
    /// security-authorization checkpoint
    /// ([specification 12.4](../../../docs/plans/rakka-agent/spec.md)). Projected
    /// from the tool binding at commit time, so the run parks on a
    /// security-authorization checkpoint before dispatch.
    #[serde(default)]
    pub authorization_required: bool,
    /// Trace context of the segment that committed the current generation —
    /// the scheduling side of the schedule -> dispatch boundary, forwarded to
    /// the dispatch ticket so the dispatcher's consumer span can link to it
    /// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)). A new
    /// generation carries a span link to the superseded one instead of its
    /// parent. Observability only, never correctness: an effect persisted
    /// before this field decodes to the empty context, and no dispatch
    /// decision reads it.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
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
        spec: &AgentEffectSpec,
        settings_revision: AgentRevisionNumber,
        created_at: AgentTimestampMillis,
    ) -> AgentEffectResult<Self> {
        spec.validate()?;
        let effect_id = effect_id_for(scope, turn, slot)?;
        let generation = AgentEffectGeneration::FIRST;
        let safety = spec.safety_for(scope, turn, slot, generation)?;
        let idempotency_key = idempotency_key_for(&effect_id, &safety);
        let argument_digest = request.argument_digest()?;
        Ok(Self {
            schema_version: CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
            effect_id,
            generation,
            turn,
            slot,
            request,
            safety,
            max_attempts: spec.max_attempts,
            argument_digest,
            settings_revision,
            credential_binding: spec.credential_binding.clone(),
            execution_policy: spec.execution_policy.clone(),
            guardrail_revision: spec.guardrail_revision,
            timeout_ms: spec.timeout_ms,
            deadline_at: None,
            status: AgentRunEffectStatus::Pending,
            idempotency_key,
            attempts: 0,
            last_fence: None,
            created_at,
            dispatched_at: None,
            last_error_code: None,
            checkpoint_required: spec.checkpoint_required,
            authorization_required: spec.authorization_required,
            telemetry: AgentTelemetryContext::default(),
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

    /// Whether this effect blocks the run from becoming terminal.
    #[must_use]
    pub const fn blocks_settlement(&self) -> bool {
        self.status.blocks_settlement()
    }

    /// Marks the effect dispatchable, *before* any sink write for it may start.
    ///
    /// The ordering is the invariant: the sink is written only after the
    /// transition that committed `Ready`, so a `Pending` effect has provably
    /// never reached the outbox and can be fenced in place by a cancellation.
    pub fn mark_ready(&mut self, now: AgentTimestampMillis) {
        self.status = AgentRunEffectStatus::Ready;
        self.dispatched_at = Some(now);
    }

    /// Records the attempt and fence a durable result command carried
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)).
    pub fn record_attempt(&mut self, attempt: u32, fence: u64) {
        self.attempts = self.attempts.max(attempt);
        self.last_fence = Some(fence);
    }

    /// The durable span identity of one dispatch attempt of the current
    /// generation: the id the dispatcher's `effect-dispatch` segment for that
    /// attempt exports under, derived from this record so that the run — which
    /// never sees the dispatcher's segment — can link an indeterminate
    /// transition to the attempt whose outcome it could not establish
    /// ([specification 17.9](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// `None` when the effect carries no trace context.
    #[must_use]
    pub fn attempt_span_identity(
        &self,
        attempt: u32,
    ) -> Option<rakka_agent_workflow::AgentTraceContext> {
        crate::observability::agent_durable_span_identity(
            &self.telemetry,
            &[
                "effect-dispatch",
                self.effect_id.as_str(),
                &self.generation.get().to_string(),
                &attempt.to_string(),
            ],
        )
    }

    /// Begins the next generation after an operator proved the previous
    /// invocation never happened
    /// ([specification 11.3](../../../docs/plans/rakka-agent/spec.md): "if a
    /// new invocation is authorized, it uses a new effect generation").
    ///
    /// The generation is a fresh dispatchable intent: attempts reset, the
    /// external idempotency key is re-derived, and the superseded generation's
    /// result operation can never answer for it.
    pub fn begin_next_generation(
        &mut self,
        scope: &AgentRunScope,
        now: AgentTimestampMillis,
    ) -> AgentEffectResult<()> {
        self.generation = self.generation.next();
        self.safety = match &self.safety {
            AgentEffectSafety::Idempotent { .. } => AgentEffectSafety::Idempotent {
                external_key: external_idempotency_key_for(
                    scope,
                    self.turn,
                    self.slot,
                    self.generation,
                )?,
            },
            other => other.clone(),
        };
        self.idempotency_key = idempotency_key_for(&self.effect_id, &self.safety);
        self.status = AgentRunEffectStatus::Pending;
        self.attempts = 0;
        self.last_fence = None;
        self.dispatched_at = None;
        self.last_error_code = None;
        self.created_at = now;
        self.telemetry = superseded_generation_telemetry(&self.telemetry);
        Ok(())
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

    /// The identity of the outbox row that dispatches the current generation.
    ///
    /// It folds the generation in, so a re-authorized invocation is a *new*
    /// dispatch ticket with its own attempt lifecycle, and the superseded
    /// generation's terminal row can never be redispatched on its behalf.
    #[must_use]
    pub fn dispatch_ticket_id(&self) -> AgentEffectId {
        AgentEffectId::new(format!("{}#g{}", self.effect_id.as_str(), self.generation))
    }

    /// Projects the effect onto the agent-workflow outbox record that dispatches
    /// it: the self-contained dispatch ticket.
    ///
    /// The projection is deterministic and carries no credential: the ticket
    /// names *what* to call, under which idempotency key, and under which
    /// logical credential binding, and the dispatcher resolves the binding
    /// inside its own bounded attempt
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)). The
    /// intent metadata the run's record holds — the run-side effect id, the
    /// generation, the safety class, the settings revision, the argument
    /// digest — rides in the target attributes, so the dispatch layer can
    /// reject a stale ticket without reaching into the run's state format.
    #[must_use]
    pub fn to_workflow_effect(&self, scope: &AgentRunScope) -> AgentEffect {
        let ticket_id = self.dispatch_ticket_id();
        let mut target = self.request.target();
        target.attributes.insert(
            ATTR_AGENT_EFFECT_ID.to_string(),
            self.effect_id.as_str().to_string(),
        );
        target.attributes.insert(
            ATTR_AGENT_EFFECT_GENERATION.to_string(),
            self.generation.to_string(),
        );
        target.attributes.insert(
            ATTR_AGENT_EFFECT_SAFETY_CLASS.to_string(),
            self.safety.class().as_label().to_string(),
        );
        target.attributes.insert(
            ATTR_AGENT_EFFECT_SETTINGS_REVISION.to_string(),
            self.settings_revision.to_string(),
        );
        target.attributes.insert(
            ATTR_AGENT_EFFECT_MAX_ATTEMPTS.to_string(),
            self.max_attempts.to_string(),
        );
        target.attributes.insert(
            ATTR_AGENT_EFFECT_ARGUMENT_DIGEST.to_string(),
            self.argument_digest.to_string(),
        );
        if let Some(protocol) = self.safety.reconciliation_protocol() {
            target.attributes.insert(
                ATTR_AGENT_EFFECT_RECONCILIATION_PROTOCOL.to_string(),
                protocol.as_str().to_string(),
            );
        }
        if let Some(binding) = &self.credential_binding {
            target.attributes.insert(
                AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE.to_string(),
                binding.as_str().to_string(),
            );
        }
        if let Some(policy) = &self.execution_policy {
            target.attributes.insert(
                ATTR_AGENT_EFFECT_EXECUTION_POLICY.to_string(),
                policy.as_str().to_string(),
            );
        }
        AgentEffect {
            effect_id: ticket_id.clone(),
            deduplication_key: AgentDeduplicationKey::new(ticket_id.as_str()),
            kind: self.kind().workflow_kind(),
            target,
            status: self.status.workflow_status(),
            payload_ref: None,
            result_ref: None,
            timeout_ms: self.timeout_ms,
            idempotency_key: self.idempotency_key.clone(),
            expected_result_type: Some(self.kind().as_label().to_string()),
            causation_id: AgentCausationId::new(ticket_id.as_str()),
            correlation_id: AgentCorrelationId::new(scope.key()),
            telemetry_context: self.telemetry.clone(),
            attempt: self.attempts,
            created_at: self.created_at,
            due_at: None,
            last_error_code: self.last_error_code.clone(),
        }
    }
}

/// Span-link attribute naming what a persisted link points at.
pub const ATTR_AGENT_TELEMETRY_LINK_KIND: &str = "rakka.agent.link.kind";
/// Link-kind value: the scheduling segment of a superseded effect generation.
pub const LINK_KIND_SUPERSEDED_GENERATION: &str = "superseded-generation";

/// The telemetry context a regenerated effect starts with: no parent — the
/// superseded generation's segment must not parent the reconciled re-dispatch —
/// and a span link back to it, so the causal chain across generations stays
/// walkable ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// The new generation keeps the run's trace and its propagated parent, so its
/// spans are *siblings* of the superseded attempt's rather than children of
/// them: the re-dispatch is caused by a reconciliation decision, not by the
/// segment that scheduled the attempt an operator just proved never executed.
/// This context used to be built from `default()` to express that, which
/// dropped the `traceparent` along with the parentage — and once a context's
/// span id became a span's *parent* rather than its identity, a context with
/// no `traceparent` belongs to no trace at all. Every span of the
/// re-invocation was then refused by the mapper and counted `unmappable`:
/// `tool-authorize`, `effect-dispatch`, `model-inference`, and `execute-tool`
/// all vanished for exactly the re-invocation an incident is about, and
/// silently, because `unmappable` labels no `rakka.agent.*` instrument.
fn superseded_generation_telemetry(prior: &AgentTelemetryContext) -> AgentTelemetryContext {
    let mut next = AgentTelemetryContext {
        trace_parent: prior.trace_parent.clone(),
        trace_state: prior.trace_state.clone(),
        span_links: prior.span_links.clone(),
        ..AgentTelemetryContext::default()
    };
    if let Ok(superseded) = require_agent_trace_context(prior) {
        let mut attributes = AgentAttributes::new();
        attributes.insert(
            ATTR_AGENT_TELEMETRY_LINK_KIND.to_string(),
            LINK_KIND_SUPERSEDED_GENERATION.to_string(),
        );
        next.span_links.push(superseded.to_span_link(attributes));
    }
    crate::observability::sanitize_agent_telemetry_context(next)
}

/// Dispatch-ticket attribute naming the run-side effect id.
pub const ATTR_AGENT_EFFECT_ID: &str = "agent_effect_id";
/// Dispatch-ticket attribute naming the effect generation.
pub const ATTR_AGENT_EFFECT_GENERATION: &str = "agent_effect_generation";
/// Dispatch-ticket attribute naming the safety class.
pub const ATTR_AGENT_EFFECT_SAFETY_CLASS: &str = "agent_effect_safety_class";
/// Dispatch-ticket attribute naming the settings revision.
pub const ATTR_AGENT_EFFECT_SETTINGS_REVISION: &str = "agent_effect_settings_revision";
/// Dispatch-ticket attribute naming the generation's attempt bound.
pub const ATTR_AGENT_EFFECT_MAX_ATTEMPTS: &str = "agent_effect_max_attempts";
/// Dispatch-ticket attribute carrying the canonical argument digest.
pub const ATTR_AGENT_EFFECT_ARGUMENT_DIGEST: &str = "agent_effect_argument_digest";
/// Dispatch-ticket attribute naming the reconciliation protocol.
pub const ATTR_AGENT_EFFECT_RECONCILIATION_PROTOCOL: &str = "agent_effect_reconciliation_protocol";
/// Dispatch-ticket attribute naming the execution policy the dispatch is
/// routed through.
pub const ATTR_AGENT_EFFECT_EXECUTION_POLICY: &str = "agent_effect_execution_policy";

/// The idempotency key the dispatch ticket hands to the target: the external
/// key when the safety class carries one, the derived internal key otherwise.
fn idempotency_key_for(
    effect_id: &AgentEffectId,
    safety: &AgentEffectSafety,
) -> AgentIdempotencyKey {
    match safety.external_key() {
        Some(external) => AgentIdempotencyKey::new(external.as_str()),
        None => AgentIdempotencyKey::new(effect_id.as_str()),
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

/// What a dispatcher returned for one effect generation — always final for
/// that generation.
///
/// It is the durable result command of
/// [specification 9.5](../../../docs/plans/rakka-agent/spec.md): the dispatcher
/// performs the bounded I/O and returns *this* through the inbox. The run never
/// awaits a model or a tool inside a handler. Attempt-level retries never reach
/// the run: the dispatch layer retries under the effect's own policy and
/// reports only the generation's terminal outcome
/// ([specification 11.3](../../../docs/plans/rakka-agent/spec.md)).
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
    /// A memory promotion durably upserted its selected entries
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    MemoryPromotion {
        /// The bounded receipt: identities and revisions only, never content.
        promoted: Vec<AgentPromotedMemoryRef>,
    },
    /// A goal evaluation completed with a verdict
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    Evaluation {
        /// The durable record: outcome, evidence references, and the criteria
        /// revision it assessed.
        record: Box<crate::evaluation::AgentGoalEvaluationRecord>,
    },
    /// An outbound A2A send durably created — or replayed to — its one
    /// logical child ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)).
    A2aSend {
        /// The bounded receipt: identities and a status label only.
        receipt: crate::delegation::AgentA2aSendReceipt,
    },
    /// An outbound handoff send durably recorded — or replayed onto — its
    /// one logical transfer at the task
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)). Never
    /// proof the target accepted: acceptance returns later through the
    /// handoff-result exchange.
    A2aHandoff {
        /// The bounded receipt: identities and a status label only.
        receipt: crate::coordination::AgentA2aHandoffReceipt,
    },
    /// A workflow start durably created — or adopted — its one logical child
    /// run ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)).
    WorkflowStart {
        /// The bounded receipt: identities and the adoption flag only.
        receipt: crate::workflow_tool::AgentWorkflowStartReceipt,
    },
    /// A workflow-cancel request durably reached its child's inbox, or found
    /// the child already finished
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)). Never
    /// proof the child's started internal effects stopped — its terminal
    /// outcome still returns through the result relay.
    WorkflowCancel {
        /// Whether the child had already finished when the request arrived.
        already_finished: bool,
    },
    /// A claim append durably landed — or replayed onto — its one logical
    /// claim ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)).
    ClaimAppend {
        /// The appended claim's stable id, as the store recorded it.
        claim: crate::identity::AgentCommunalClaimId,
    },
    /// The generation failed definitively.
    Failed {
        /// Stable machine-readable code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
    /// The generation's retry budget was spent without a result.
    Exhausted {
        /// Stable machine-readable code of the last failure.
        code: String,
        /// Human-readable detail.
        message: String,
    },
    /// An attempt may have invoked the target and its outcome cannot be
    /// established mechanically. The run must park for reconciliation
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
    Indeterminate {
        /// Stable machine-readable code describing the ambiguity.
        code: String,
        /// Human-readable detail.
        message: String,
    },
    /// The generation was fenced and settled without invocation.
    Cancelled {
        /// A bounded reason.
        reason: String,
    },
}

impl AgentRunEffectOutcome {
    /// Whether the effect produced its bounded result.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(
            self,
            Self::Model { .. }
                | Self::Tool { .. }
                | Self::MemoryPromotion { .. }
                | Self::Evaluation { .. }
                | Self::A2aSend { .. }
                | Self::A2aHandoff { .. }
                | Self::WorkflowStart { .. }
                | Self::WorkflowCancel { .. }
                | Self::ClaimAppend { .. }
        )
    }

    /// The run-side status this outcome resolves the generation to.
    #[must_use]
    pub const fn resolved_status(&self) -> AgentRunEffectStatus {
        match self {
            Self::Model { .. }
            | Self::Tool { .. }
            | Self::MemoryPromotion { .. }
            | Self::Evaluation { .. }
            | Self::A2aSend { .. }
            | Self::A2aHandoff { .. }
            | Self::WorkflowStart { .. }
            | Self::WorkflowCancel { .. }
            | Self::ClaimAppend { .. } => AgentRunEffectStatus::Succeeded,
            Self::Failed { .. } => AgentRunEffectStatus::Failed,
            Self::Exhausted { .. } => AgentRunEffectStatus::Exhausted,
            Self::Indeterminate { .. } => AgentRunEffectStatus::Indeterminate,
            Self::Cancelled { .. } => AgentRunEffectStatus::Cancelled,
        }
    }

    /// Stable failure code, when the effect failed, exhausted, or became
    /// indeterminate.
    #[must_use]
    pub fn failure_code(&self) -> Option<&str> {
        match self {
            Self::Failed { code, .. }
            | Self::Exhausted { code, .. }
            | Self::Indeterminate { code, .. } => Some(code),
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
        if let Self::Evaluation { record } = self {
            policy.check_record(record.as_ref())?;
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
            Self::MemoryPromotion { promoted } => {
                if promoted.len() > AGENT_MEMORY_PROMOTION_MAX_ENTRIES {
                    return Err(AgentEffectError::InvalidPolicy {
                        message: format!(
                            "the promotion receipt names {} memories, which exceeds the {} \
                             selection bound",
                            promoted.len(),
                            AGENT_MEMORY_PROMOTION_MAX_ENTRIES
                        ),
                    });
                }
                Ok(())
            }
            Self::Evaluation { record } => {
                record
                    .validate()
                    .map_err(|error| AgentEffectError::InvalidPolicy {
                        message: error.to_string(),
                    })
            }
            Self::A2aSend { receipt } => {
                receipt
                    .validate()
                    .map_err(|error| AgentEffectError::InvalidPolicy {
                        message: error.to_string(),
                    })
            }
            Self::A2aHandoff { receipt } => {
                receipt
                    .validate()
                    .map_err(|error| AgentEffectError::InvalidPolicy {
                        message: error.to_string(),
                    })
            }
            // The start receipt is identities and a flag, the cancel outcome
            // one flag, and the append receipt one identity: bounded by
            // construction.
            Self::WorkflowStart { .. } | Self::WorkflowCancel { .. } | Self::ClaimAppend { .. } => {
                Ok(())
            }
            Self::Failed { .. }
            | Self::Exhausted { .. }
            | Self::Indeterminate { .. }
            | Self::Cancelled { .. } => Ok(()),
        }
    }
}

/// An explicit decision on an [`AgentRunEffectStatus::Indeterminate`] effect
/// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md),
/// [12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// There is deliberately no generic `Retry`: an ambiguous effect is never
/// mutated back into a routine attempt. The decision either records the
/// outcome that was established, or proves the invocation never happened — and
/// only the latter authorizes a new effect generation. Slice 1.10 wraps this
/// decision in the reconciliation checkpoint record; the run-side semantics
/// land here because the effect layer must already refuse to guess.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEffectResolution {
    /// The invocation happened, and this is its established outcome.
    ConfirmedExecuted {
        /// The authoritative outcome: a completed result or a definitive
        /// failure. An indeterminate or cancelled outcome is refused — it
        /// would not resolve anything.
        outcome: Box<AgentRunEffectOutcome>,
    },
    /// The invocation provably never happened. A new effect generation is
    /// authorized where the run still wants the work.
    ConfirmedNotExecuted,
}

impl AgentEffectResolution {
    /// Rejects a resolution that resolves nothing.
    pub fn validate(&self) -> AgentEffectResult<()> {
        match self {
            Self::ConfirmedExecuted { outcome } => {
                outcome.validate()?;
                if matches!(
                    outcome.as_ref(),
                    AgentRunEffectOutcome::Indeterminate { .. }
                        | AgentRunEffectOutcome::Cancelled { .. }
                ) {
                    return Err(AgentEffectError::InvalidPolicy {
                        message: "a reconciliation decision must establish an outcome; an \
                                  indeterminate or cancelled outcome resolves nothing"
                            .to_string(),
                    });
                }
                Ok(())
            }
            Self::ConfirmedNotExecuted => Ok(()),
        }
    }
}

/// The stable call id a compensation effect's result settles under
/// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// A compensation has no model-issued tool call, so the id is derived from the
/// effect's own identity — pure per generation slot, like every other derived
/// identity a re-driven dispatch must resolve to.
#[must_use]
pub fn compensation_call_id(effect: &AgentRunEffect) -> AgentToolCallId {
    AgentToolCallId::new(format!("compensation-t{}-s{}", effect.turn, effect.slot))
        .expect("the derived compensation call id is well formed")
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
    /// An effect spec, safety record, or reconciliation decision could not be
    /// honored by the crash-and-timeout rules.
    InvalidPolicy {
        /// What made it unenforceable.
        message: String,
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
            Self::InvalidPolicy { .. } => "effect-policy-invalid",
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
            Self::InvalidPolicy { message } => {
                write!(f, "the effect policy cannot be honored: {message}")
            }
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
    /// Every structural bound a claim append can violate is refused at the
    /// door, so a request that reaches dispatch is one the store can accept.
    /// A bound checked only store-side would have already reserved the run's
    /// attempts and spent an effect slot before failing.
    #[test]
    fn a_claim_append_request_refuses_every_bound_at_the_door() {
        let base = || AgentClaimAppendRequest {
            space: crate::identity::KnowledgeSpaceId::new("space").expect("the space id is valid"),
            subject: "finding".to_string(),
            predicate: "links".to_string(),
            object: AgentClaimObjectRequest::Node("evidence".to_string()),
            confidence_bps: 5_000,
            classification: MemoryClassification::Unclassified,
            evidence: Vec::new(),
            requested_by: PrincipalRef {
                principal_type: "service".to_string(),
                principal_id: "researcher".to_string(),
                display_name: None,
            },
        };
        base().validate().expect("the base request is bounded");

        let mut empty_subject = base();
        empty_subject.subject = String::new();
        empty_subject
            .validate()
            .expect_err("an empty subject is refused");

        let mut long_predicate = base();
        long_predicate.predicate = "p".repeat(AGENT_IDENTITY_MAX_LENGTH + 1);
        long_predicate
            .validate()
            .expect_err("an oversized predicate is refused");

        let mut over_evidence = base();
        over_evidence.evidence = (0..=AGENT_CLAIM_APPEND_MAX_EVIDENCE)
            .map(|index| rakka_agent_workflow::ArtifactRef {
                artifact_id: format!("artifact-{index}"),
                kind: rakka_agent_workflow::ArtifactKind::File,
                uri: format!("s3://evidence/artifact-{index}"),
                checksum: None,
                content_type: None,
                byte_len: None,
                retention_class: None,
                encryption: None,
                redaction: rakka_agent_workflow::RedactionStatus::Unredacted,
                created_at: AgentTimestampMillis::new(1),
                metadata: rakka_agent_workflow::AgentAttributes::default(),
            })
            .collect();
        over_evidence
            .validate()
            .expect_err("evidence past the bound is refused");

        // Confidence is basis points: the store refuses anything past ten
        // thousand, and `u16` admits six times that.
        let mut over_confidence = base();
        over_confidence.confidence_bps = AGENT_CLAIM_APPEND_MAX_CONFIDENCE_BPS + 1;
        over_confidence
            .validate()
            .expect_err("confidence past ten thousand basis points is refused");
        let mut at_bound = base();
        at_bound.confidence_bps = AGENT_CLAIM_APPEND_MAX_CONFIDENCE_BPS;
        at_bound.validate().expect("the bound itself is admissible");
    }

    use super::*;
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::memory::AgentContextSnapshotRef;

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("the agent id is valid"),
            AgentRunId::new("t-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid")
    }

    fn model_effect(spec: &AgentEffectSpec) -> AgentRunEffect {
        let scope = scope();
        let context = AgentContextSnapshotRef::for_turn(&scope, 1).expect("the reference derives");
        AgentRunEffect::new(
            &scope,
            1,
            0,
            AgentRunEffectRequest::Model {
                context,
                profile: None,
            },
            spec,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(1),
        )
        .expect("the effect derives")
    }

    #[test]
    fn the_result_operation_id_folds_in_the_dispatch_generation() {
        let scope = scope();
        let mut effect = model_effect(&AgentEffectSpec::read_only());

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

        // A later generation is a different operation entirely: the
        // reconciliation re-dispatch must not be answered from the log entry a
        // superseded attempt left behind.
        effect.generation = effect.generation.next();
        let second = effect
            .result_operation_id(&scope)
            .expect("the operation id derives");
        assert_ne!(first, second);
    }

    #[test]
    fn an_unenforceable_effect_spec_is_refused_where_it_enters() {
        // Zero attempts.
        assert_eq!(
            AgentEffectSpec::idempotent(0)
                .expect_err("zero attempts is refused")
                .code(),
            "effect-policy-invalid"
        );
        // A retry bound never overrides the non-idempotent ambiguity rule.
        assert_eq!(
            AgentEffectSpec::non_idempotent()
                .with_max_attempts(2)
                .expect_err("a non-idempotent retry is refused")
                .code(),
            "effect-policy-invalid"
        );
        // A reconcileable spec without its protocol could never be reconciled.
        let missing = AgentEffectSpec {
            safety_class: AgentEffectSafetyClass::Reconcileable,
            max_attempts: 1,
            reconciliation_protocol: None,
            credential_binding: None,
            timeout_ms: None,
            execution_policy: None,
            guardrail_revision: None,
            checkpoint_required: false,
            authorization_required: false,
        };
        assert_eq!(
            missing
                .validate()
                .expect_err("a protocol-less reconcileable spec is refused")
                .code(),
            "effect-policy-invalid"
        );
        // Deserialization applies the same gate, so an unenforceable spec can
        // neither cross the wire nor load from a durable record.
        let encoded = serde_json::to_value(&missing).expect("the spec serializes");
        serde_json::from_value::<AgentEffectSpec>(encoded)
            .expect_err("an unenforceable spec is refused on load");
    }

    #[test]
    fn a_new_generation_is_a_fresh_dispatch_ticket_with_a_fresh_external_key() {
        let scope = scope();
        let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
        let mut effect = model_effect(&spec);
        let first_ticket = effect.dispatch_ticket_id();
        let first_key = effect
            .safety
            .external_key()
            .expect("an idempotent effect carries its external key")
            .clone();

        effect.status = AgentRunEffectStatus::Indeterminate;
        effect
            .begin_next_generation(&scope, AgentTimestampMillis::new(2))
            .expect("the next generation begins");

        // The identity is stable; the ticket, key, and attempt budget are new.
        assert_eq!(effect.generation, AgentEffectGeneration::new(2));
        assert_ne!(effect.dispatch_ticket_id(), first_ticket);
        assert_ne!(
            effect
                .safety
                .external_key()
                .expect("the new generation carries a key"),
            &first_key
        );
        assert_eq!(effect.status, AgentRunEffectStatus::Pending);
        assert_eq!(effect.attempts, 0);
    }

    #[test]
    fn the_dispatch_ticket_carries_the_intent_metadata_and_no_credential() {
        let scope = scope();
        let spec = AgentEffectSpec::read_only().with_credential_binding(
            AgentCredentialBindingRef::new("model-provider").expect("the binding ref is valid"),
        );
        let effect = model_effect(&spec);
        let ticket = effect.to_workflow_effect(&scope);

        let attributes = &ticket.target.attributes;
        assert_eq!(
            attributes.get(ATTR_AGENT_EFFECT_ID).map(String::as_str),
            Some(effect.effect_id.as_str())
        );
        assert_eq!(
            attributes
                .get(ATTR_AGENT_EFFECT_GENERATION)
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            attributes
                .get(ATTR_AGENT_EFFECT_SAFETY_CLASS)
                .map(String::as_str),
            Some("read-only")
        );
        assert_eq!(
            attributes
                .get(AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE)
                .map(String::as_str),
            Some("model-provider")
        );
        // The ticket names the binding, never a resolved value: nothing in its
        // encoding may look like secret material, and the intent record itself
        // has no field to hold one.
        let encoded = serde_json::to_string(&ticket).expect("the ticket serializes");
        assert!(!encoded.contains("secret"));
    }
}
