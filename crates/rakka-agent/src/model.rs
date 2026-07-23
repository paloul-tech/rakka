//! The provider-neutral model adapter trait and the durable turn it produces.
//!
//! Owns the Rakka model contract (working name `AgentModelAdapter`): it turns
//! an immutable context snapshot and a settings revision into a bounded model
//! request, and turns the provider response into a bounded Rakka result or
//! artifact. The durable loop, the effect model, and the testkit depend on this
//! trait and on nothing else — no provider client, stream, open request, or
//! credential value is ever durable state.
//!
//! Model calls are effects with an explicit retry policy, so a provider that
//! stalls or fails is handled by the effect machine rather than by the adapter.
//!
//! Specification: sections 10.1, 10.2, and 10.3. The adapter *trait* is filled
//! by slice 1.6; its Rig-backed implementation lives behind the `rig` feature in
//! [`crate::rig`] and its deterministic implementation in [`crate::testkit`].
//!
//! # The durable half, landed by slice 1.5
//!
//! [`AgentModelTurn`] is the Rakka-owned, versioned representation of one model
//! turn, and it is the **only durable format**
//! ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)): no provider
//! type and no adapter's internal runner state ever reaches durable state, so
//! Rakka can upgrade or replace an adapter through an explicit migration rather
//! than through whatever a provider library happened to serialize.
//!
//! The loop of [`crate::loop_runtime`] therefore acts on this record and nothing
//! else. It arrives the way every effect result arrives — as a durable command
//! returned through the inbox by the dispatcher that performed the bounded I/O
//! ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)) — which is why
//! the loop can be driven end to end, and its recovery proven, before any
//! adapter exists to produce one.
//!
//! A turn carries no resolved credential and no secret material, and it is
//! bounded in every dimension, because it is committed into the run's own
//! durable state.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use rakka_agent_workflow::StateSchemaVersion;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::definition::{
    AgentEffectSafetyClass, AgentModelProfileId, AgentRevisionNumber, AgentSamplingSettings,
    AgentToolId,
};
use crate::identity::validated_id;
use crate::memory::AgentContextSnapshotRef;
use crate::schema::{
    AgentRecordKind, AgentSchemaError, VersionedAgentRecord,
    CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION,
};
use crate::task::AgentTaskContent;

/// Largest serialized [`AgentModelTurn`], in bytes.
///
/// A turn is committed into the run's own durable state, so it is bounded there
/// rather than trusted to be small. Anything larger belongs behind an artifact
/// reference.
pub const AGENT_MODEL_TURN_MAX_BYTES: usize = 16 * 1024;

/// Largest assistant text one model turn may carry, in bytes.
pub const AGENT_MODEL_TEXT_MAX_LENGTH: usize = 4 * 1024;

/// Most tool calls one model turn may request.
///
/// The bound is what keeps the loop's `AwaitingTools` phase finite: a turn
/// cannot ask for more concurrent effects than the run may hold.
pub const AGENT_MODEL_MAX_TOOL_CALLS: usize = 8;

/// Largest serialized argument value one tool call may carry, in bytes.
pub const AGENT_TOOL_ARGUMENTS_MAX_BYTES: usize = 2 * 1024;

/// Result type for model-turn construction and validation.
pub type AgentModelResult<T> = Result<T, AgentModelError>;

validated_id! {
    /// Identity of one tool call inside a model turn.
    ///
    /// It is the model's own handle for the call, and the loop uses it to match
    /// a tool result back to the request that asked for it.
    pub AgentToolCallId, "agent_tool_call_id"
}

/// What one model turn consumed.
///
/// The dimensions are the ones the run's ledger charges
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). A provider
/// that reports nothing leaves them zero; a budget is still charged for the call
/// itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentModelUsage {
    /// Prompt tokens the provider billed.
    pub input_tokens: u64,
    /// Completion tokens the provider billed.
    pub output_tokens: u64,
    /// Provider cost, in micro-units of currency.
    pub cost_micros: u64,
}

impl AgentModelUsage {
    /// Total tokens the turn billed.
    #[must_use]
    pub const fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

/// One tool the model asked to call.
///
/// It is a *request*, not an authorization. Model output can never widen what a
/// run may do: the tool binding, the dispatch grant, and the credential class
/// are trusted definition and setup data, and slice 1.8 revalidates every one of
/// them before the call can be dispatched
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolCallRequest {
    /// The model's handle for this call.
    pub call_id: AgentToolCallId,
    /// The tool it asked for.
    pub tool: AgentToolId,
    /// The bounded arguments it passed.
    pub arguments: Value,
}

impl AgentToolCallRequest {
    /// Creates a tool-call request, rejecting arguments that exceed the bound.
    pub fn new(
        call_id: AgentToolCallId,
        tool: AgentToolId,
        arguments: Value,
    ) -> AgentModelResult<Self> {
        let request = Self {
            call_id,
            tool,
            arguments,
        };
        request.validate()?;
        Ok(request)
    }

    /// Rejects a request whose arguments exceed [`AGENT_TOOL_ARGUMENTS_MAX_BYTES`].
    pub fn validate(&self) -> AgentModelResult<()> {
        let bytes = serde_json::to_vec(&self.arguments)
            .map_err(|error| AgentModelError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_TOOL_ARGUMENTS_MAX_BYTES {
            return Err(AgentModelError::ToolArgumentsTooLarge {
                call_id: self.call_id.clone(),
                bytes,
                maximum: AGENT_TOOL_ARGUMENTS_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// The Rakka-owned, versioned record of one model turn
/// ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is the only durable format for what a model produced, and the loop of
/// [`crate::loop_runtime`] acts on it alone. Three things a turn may ask for,
/// and the loop's continuation follows from which:
///
/// - **tool calls** — the loop persists one effect per call and waits;
/// - **a result proposal** — the loop persists the proposal and proposes it to
///   the task entity, which alone decides whether the public task completes
///   ([specification 9.5](../../../docs/plans/rakka-agent/spec.md));
/// - **neither** — the loop takes another bounded iteration, if its budget still
///   permits one.
///
/// The fields are public so an adapter can assemble a turn directly, which means
/// construction alone cannot guarantee the bounded invariants.
/// [`AgentModelTurn::validate`] therefore runs inside [`AgentModelTurn::new`], on
/// deserialization — an out-of-bounds turn can neither cross the wire nor load
/// from a durable record — and again before the loop commits one.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentModelTurn {
    schema_version: StateSchemaVersion,
    /// Version of the adapter that produced the turn.
    ///
    /// It is persisted with the loop state
    /// ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)) so an
    /// adapter upgrade is an explicit migration rather than a silent
    /// reinterpretation of records the previous adapter wrote.
    pub adapter_version: AgentRevisionNumber,
    /// The model profile that produced the turn, when the adapter reports one.
    pub model_profile: Option<AgentModelProfileId>,
    /// Bounded assistant text.
    pub text: Option<String>,
    /// Tools the model asked to call.
    pub tool_calls: Vec<AgentToolCallRequest>,
    /// A typed task-result proposal, when the model proposed one.
    pub proposal: Option<AgentTaskContent>,
    /// What the turn consumed.
    pub usage: AgentModelUsage,
}

impl AgentModelTurn {
    /// Creates an empty turn at the current schema version.
    #[must_use]
    pub fn new(adapter_version: AgentRevisionNumber) -> Self {
        Self {
            schema_version: CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION,
            adapter_version,
            model_profile: None,
            text: None,
            tool_calls: Vec::new(),
            proposal: None,
            usage: AgentModelUsage::default(),
        }
    }

    /// Sets the assistant text.
    #[must_use]
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    /// Adds a tool call the model asked for.
    #[must_use]
    pub fn with_tool_call(mut self, call: AgentToolCallRequest) -> Self {
        self.tool_calls.push(call);
        self
    }

    /// Sets the typed task-result proposal.
    #[must_use]
    pub fn with_proposal(mut self, proposal: AgentTaskContent) -> Self {
        self.proposal = Some(proposal);
        self
    }

    /// Sets the model profile that produced the turn.
    #[must_use]
    pub fn with_model_profile(mut self, profile: AgentModelProfileId) -> Self {
        self.model_profile = Some(profile);
        self
    }

    /// Sets what the turn consumed.
    #[must_use]
    pub const fn with_usage(mut self, usage: AgentModelUsage) -> Self {
        self.usage = usage;
        self
    }

    /// Whether the turn asked for at least one tool call.
    #[must_use]
    pub fn requests_tools(&self) -> bool {
        !self.tool_calls.is_empty()
    }

    /// Serialized size of the turn, in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects a turn that cannot be bounded.
    pub fn validate(&self) -> AgentModelResult<()> {
        if let Some(text) = &self.text {
            if text.len() > AGENT_MODEL_TEXT_MAX_LENGTH {
                return Err(AgentModelError::TextTooLong {
                    bytes: text.len(),
                    maximum: AGENT_MODEL_TEXT_MAX_LENGTH,
                });
            }
        }
        if self.tool_calls.len() > AGENT_MODEL_MAX_TOOL_CALLS {
            return Err(AgentModelError::TooManyToolCalls {
                calls: self.tool_calls.len(),
                maximum: AGENT_MODEL_MAX_TOOL_CALLS,
            });
        }
        for call in &self.tool_calls {
            call.validate()?;
        }
        if let Some(proposal) = &self.proposal {
            proposal
                .validate()
                .map_err(|error| AgentModelError::Proposal {
                    message: error.to_string(),
                })?;
        }

        let bytes = self.size_bytes();
        if bytes > AGENT_MODEL_TURN_MAX_BYTES {
            return Err(AgentModelError::TurnTooLarge {
                bytes,
                maximum: AGENT_MODEL_TURN_MAX_BYTES,
            });
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentModelTurn {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::ModelTurn;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The wire and durable shape of [`AgentModelTurn`], validated on load.
///
/// A turn arrives from a dispatcher as a durable command, so it crosses a trust
/// boundary. Deserializing through this shadow record means an out-of-bounds
/// turn is refused where it enters, not after it has been committed to a run's
/// state.
#[derive(Deserialize)]
struct AgentModelTurnRecord {
    schema_version: StateSchemaVersion,
    adapter_version: AgentRevisionNumber,
    #[serde(default)]
    model_profile: Option<AgentModelProfileId>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    tool_calls: Vec<AgentToolCallRequest>,
    #[serde(default)]
    proposal: Option<AgentTaskContent>,
    #[serde(default)]
    usage: AgentModelUsage,
}

impl<'de> Deserialize<'de> for AgentModelTurn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = AgentModelTurnRecord::deserialize(deserializer)?;
        let turn = Self {
            schema_version: record.schema_version,
            adapter_version: record.adapter_version,
            model_profile: record.model_profile,
            text: record.text,
            tool_calls: record.tool_calls,
            proposal: record.proposal,
            usage: record.usage,
        };
        turn.validate().map_err(serde::de::Error::custom)?;
        Ok(turn)
    }
}

/// The bounded, provider-neutral request one model turn is computed from
/// ([specification 10.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is what [`AgentModelAdapter::call`] receives: the immutable context the
/// call is prepared against, the model profile and sampling the settings
/// revision selected, and the turn it serves. It is the "bounded model request"
/// of the model contract — an input the adapter maps onto whatever a provider
/// expects, never a provider type itself.
///
/// It carries no resolved credential and no secret material. The credential a
/// model call may need is named by the agent's definition and resolved inside
/// the dispatcher's bounded attempt, never here
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentModelRequest {
    /// The immutable context the call is prepared against.
    ///
    /// It is opaque and versioned for now; slice 1.11 gives it the
    /// `MemoryContextSnapshot` content of
    /// [specification 13.5](../../../docs/plans/rakka-agent/spec.md) without
    /// moving this reference. A retry of the model effect reuses the same
    /// reference, so drift in a memory store or a retrieval index cannot change
    /// a retried input.
    pub context: AgentContextSnapshotRef,
    /// The model profile the settings revision selected, when one is selected.
    pub profile: Option<AgentModelProfileId>,
    /// The bounded sampling parameters resolved for this turn.
    pub sampling: AgentSamplingSettings,
    /// The settings revision this request resolved against.
    ///
    /// Turn-bound settings resolve against the agent's current revision at each
    /// turn ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)). The
    /// field exists so that, once slice 1.8 resolves settings at dispatch, an
    /// in-flight call can be reasoned about against the exact settings it was
    /// prepared under; until then the interim loop stamps
    /// [`AgentRevisionNumber::INITIAL`] and nothing reads the value back.
    pub settings_revision: AgentRevisionNumber,
    /// The turn this request serves, counting from one.
    pub turn: u64,
}

impl AgentModelRequest {
    /// A request for one turn, prepared against a context snapshot.
    ///
    /// The profile and sampling default to unset; the interim loop prepares a
    /// model effect with neither, and slice 1.8 fills them from the settings
    /// resolved at dispatch.
    #[must_use]
    pub fn new(context: AgentContextSnapshotRef, turn: u64) -> Self {
        Self {
            context,
            profile: None,
            sampling: AgentSamplingSettings::default(),
            settings_revision: AgentRevisionNumber::INITIAL,
            turn,
        }
    }

    /// Selects the model profile the request resolves against.
    #[must_use]
    pub fn with_profile(mut self, profile: AgentModelProfileId) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Sets the sampling parameters resolved for the turn.
    #[must_use]
    pub const fn with_sampling(mut self, sampling: AgentSamplingSettings) -> Self {
        self.sampling = sampling;
        self
    }

    /// Records the settings revision the request resolved against.
    #[must_use]
    pub const fn with_settings_revision(mut self, revision: AgentRevisionNumber) -> Self {
        self.settings_revision = revision;
        self
    }
}

/// The explicit policy that governs whether and how a model call is retried
/// after an ambiguous worker loss
/// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md);
/// [open decision 4](../../../docs/plans/rakka-agent/spec.md)).
///
/// There is no implicit retry: a model call is billed on every attempt, so the
/// bound is a deployment decision based on provider idempotency, cost, and
/// replay tolerance — not a default the framework picks silently. An
/// [`AgentModelAdapter`] declares its policy, and the effect machine of slice
/// 1.7 enforces it; recording it from the first commit is what lets that
/// enforcement be an addition rather than a change to the adapter contract.
///
/// [`Self::validate`] runs where a policy enters: the adapters' fallible
/// `with_retry_policy` builders, [`Self::read_only`], and deserialization all
/// refuse a policy the crash-and-timeout rules could not honor. The fields stay
/// public for literal construction, so the effect machine revalidates the policy
/// it reads rather than trusting construction alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AgentModelRetryPolicy {
    /// The safety class the model call dispatches under.
    ///
    /// A completion does not change external correctness state, so
    /// [`AgentEffectSafetyClass::ReadOnly`] is the default and an ambiguous
    /// attempt may be retried up to [`Self::max_attempts`]. A deployment whose
    /// provider offers an idempotency key may raise it to
    /// [`AgentEffectSafetyClass::Idempotent`]. A retry count never overrides the
    /// non-idempotent ambiguity rule
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)):
    /// [`Self::validate`] refuses a [`AgentEffectSafetyClass::NonIdempotent`]
    /// policy that permits more than one attempt.
    pub safety_class: AgentEffectSafetyClass,
    /// The most dispatch attempts one model call may make, its first included.
    pub max_attempts: u32,
}

impl AgentModelRetryPolicy {
    /// One read-only attempt, no automatic retry.
    ///
    /// The conservative default: an ambiguous model call is not silently retried
    /// and re-billed. A deployment raises the bound explicitly.
    pub const DEFAULT: Self = Self {
        safety_class: AgentEffectSafetyClass::ReadOnly,
        max_attempts: 1,
    };

    /// A read-only policy that retries an ambiguous call up to `max_attempts`
    /// times, refusing a bound of zero.
    pub fn read_only(max_attempts: u32) -> AgentModelResult<Self> {
        let policy = Self {
            safety_class: AgentEffectSafetyClass::ReadOnly,
            max_attempts,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Rejects a policy the crash-and-timeout rules could not honor.
    ///
    /// A policy must permit at least one attempt, and a non-idempotent call must
    /// permit exactly one: an ambiguous non-idempotent attempt is never
    /// auto-retried ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)).
    pub fn validate(&self) -> AgentModelResult<()> {
        if self.max_attempts == 0 {
            return Err(AgentModelError::InvalidRetryPolicy {
                message: "a model retry policy must permit at least one attempt".to_string(),
            });
        }
        if self.safety_class == AgentEffectSafetyClass::NonIdempotent && self.max_attempts > 1 {
            return Err(AgentModelError::InvalidRetryPolicy {
                message: "a non-idempotent model call may not be auto-retried after ambiguity"
                    .to_string(),
            });
        }
        Ok(())
    }
}

impl Default for AgentModelRetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The wire shape of [`AgentModelRetryPolicy`], validated on load.
///
/// A policy may be persisted with the effect intent it governs, so it crosses a
/// trust boundary the same way a turn does: deserializing through this shadow
/// record means a policy the crash-and-timeout rules could not honor is refused
/// where it enters, never handed to the effect machine.
#[derive(Deserialize)]
struct AgentModelRetryPolicyRecord {
    safety_class: AgentEffectSafetyClass,
    max_attempts: u32,
}

impl<'de> Deserialize<'de> for AgentModelRetryPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = AgentModelRetryPolicyRecord::deserialize(deserializer)?;
        let policy = Self {
            safety_class: record.safety_class,
            max_attempts: record.max_attempts,
        };
        policy.validate().map_err(serde::de::Error::custom)?;
        Ok(policy)
    }
}

/// The future an [`AgentModelAdapter::call`] returns.
pub type AgentModelFuture<'a> =
    Pin<Box<dyn Future<Output = AgentModelResult<AgentModelTurn>> + Send + 'a>>;

/// The Rakka-owned, provider-neutral model adapter
/// ([specification 10.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is the whole of the model contract: it converts an immutable context
/// snapshot and a settings revision (an [`AgentModelRequest`]) into a bounded
/// Rakka [`AgentModelTurn`], and it converts a provider's response into that
/// same bounded turn. What a provider request or response looks like is the
/// adapter's private concern; it never crosses this boundary, and it is never
/// durable state — the turn is the only durable format
/// ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The durable loop, the effect model, and the testkit depend on this trait and
/// on nothing else. A model call is an effect, so the adapter is invoked by the
/// dispatcher performing the bounded I/O — never inside a run transition — and
/// its turn returns to the loop as a durable result command
/// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)). Its
/// [`retry_policy`](Self::retry_policy) is the *declared ceiling* the dispatch
/// pipeline of [`crate::dispatch`] re-enforces before every invocation: a
/// committed effect whose configured policy is weaker — a laxer safety class,
/// or more attempts — fails closed rather than letting recovery retry what the
/// adapter declared unsafe.
///
/// The Rig-backed implementation lives behind the `rig` feature in
/// [`crate::rig`]; the deterministic implementation in [`crate::testkit`] scripts
/// turns without any provider and exercises the same durable effect path.
pub trait AgentModelAdapter: Send + Sync {
    /// The version stamped onto every turn this adapter produces.
    ///
    /// It is persisted with the loop state, so an adapter upgrade is an explicit
    /// migration rather than a silent reinterpretation of the turns a previous
    /// adapter wrote ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
    fn adapter_version(&self) -> AgentRevisionNumber;

    /// The retry policy model calls made through this adapter dispatch under.
    ///
    /// The default is [`AgentModelRetryPolicy::DEFAULT`]: one attempt, no
    /// automatic retry.
    fn retry_policy(&self) -> AgentModelRetryPolicy {
        AgentModelRetryPolicy::DEFAULT
    }

    /// Performs one bounded model call, producing a durable turn.
    ///
    /// The returned turn is validated by the effect path before it is committed,
    /// so an adapter that assembles an out-of-bounds turn is refused where the
    /// outcome enters rather than after the run has recorded it.
    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a>;
}

/// A model call that could not produce a bounded, interpretable turn.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentModelError {
    /// The turn carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The assistant text exceeded its bound.
    TextTooLong {
        /// Length of the rejected text, in bytes.
        bytes: usize,
        /// Maximum accepted length, in bytes.
        maximum: usize,
    },
    /// The turn asked for more tool calls than a run may hold.
    TooManyToolCalls {
        /// Number of calls the turn requested.
        calls: usize,
        /// Maximum accepted number of calls.
        maximum: usize,
    },
    /// One tool call's arguments exceeded their bound.
    ToolArgumentsTooLarge {
        /// The call whose arguments were refused.
        call_id: AgentToolCallId,
        /// Size of the rejected arguments, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// The whole turn exceeded its bound.
    TurnTooLarge {
        /// Size of the rejected turn, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// The proposed task result could not be bounded.
    Proposal {
        /// What was out of bounds.
        message: String,
    },
    /// A value could not be encoded.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
    /// A model retry policy could not be honored by the crash-and-timeout rules.
    InvalidRetryPolicy {
        /// What made the policy unenforceable.
        message: String,
    },
    /// The provider, or the adapter mapping its response, failed.
    ///
    /// A provider transport failure or a response the adapter could not map onto
    /// a bounded turn surfaces here. The dispatch pipeline records it as a
    /// failed attempt and retries it under the effect's bounded policy; a spent
    /// budget reaches the run as the generation's `Exhausted` word.
    Provider {
        /// The provider or mapping failure detail.
        message: String,
    },
}

impl AgentModelError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Schema(error) => error.code(),
            Self::TextTooLong { .. } => "model-text-too-long",
            Self::TooManyToolCalls { .. } => "model-too-many-tool-calls",
            Self::ToolArgumentsTooLarge { .. } => "model-tool-arguments-too-large",
            Self::TurnTooLarge { .. } => "model-turn-too-large",
            Self::Proposal { .. } => "model-proposal-not-bounded",
            Self::Encoding { .. } => "model-encoding-failed",
            Self::InvalidRetryPolicy { .. } => "model-retry-policy-invalid",
            Self::Provider { .. } => "model-provider-failed",
        }
    }
}

impl Display for AgentModelError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(error) => Display::fmt(error, f),
            Self::TextTooLong { bytes, maximum } => write!(
                f,
                "the model turn's text is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::TooManyToolCalls { calls, maximum } => write!(
                f,
                "the model turn requested {calls} tool calls, and a run may hold at most {maximum}"
            ),
            Self::ToolArgumentsTooLarge {
                call_id,
                bytes,
                maximum,
            } => write!(
                f,
                "the arguments of tool call {call_id} are {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::TurnTooLarge { bytes, maximum } => write!(
                f,
                "the model turn is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::Proposal { message } => {
                write!(f, "the proposed task result is not bounded: {message}")
            }
            Self::Encoding { message } => {
                write!(f, "a model value could not be encoded: {message}")
            }
            Self::InvalidRetryPolicy { message } => {
                write!(f, "the model retry policy cannot be honored: {message}")
            }
            Self::Provider { message } => {
                write!(f, "the model provider call failed: {message}")
            }
        }
    }
}

impl Error for AgentModelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentSchemaError> for AgentModelError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_retry_policy_permits_one_read_only_attempt() {
        let policy = AgentModelRetryPolicy::default();
        assert_eq!(policy.safety_class, AgentEffectSafetyClass::ReadOnly);
        assert_eq!(policy.max_attempts, 1);
        policy.validate().expect("the default policy is valid");
    }

    #[test]
    fn a_retry_policy_must_permit_at_least_one_attempt() {
        let policy = AgentModelRetryPolicy {
            safety_class: AgentEffectSafetyClass::ReadOnly,
            max_attempts: 0,
        };
        assert_eq!(
            policy
                .validate()
                .expect_err("zero attempts is refused")
                .code(),
            "model-retry-policy-invalid"
        );
    }

    #[test]
    fn a_non_idempotent_model_call_may_not_be_auto_retried_after_ambiguity() {
        // The retry count never overrides the non-idempotent ambiguity rule
        // ([specification 11.4]).
        let auto_retry = AgentModelRetryPolicy {
            safety_class: AgentEffectSafetyClass::NonIdempotent,
            max_attempts: 2,
        };
        assert_eq!(
            auto_retry
                .validate()
                .expect_err("auto-retry of a non-idempotent call is refused")
                .code(),
            "model-retry-policy-invalid"
        );

        // Exactly one attempt is honored: no retry, no ambiguity.
        AgentModelRetryPolicy {
            safety_class: AgentEffectSafetyClass::NonIdempotent,
            max_attempts: 1,
        }
        .validate()
        .expect("a single-attempt non-idempotent policy is valid");
    }

    #[test]
    fn an_invalid_retry_policy_is_refused_where_it_enters() {
        // The fallible constructor refuses a bound of zero.
        assert_eq!(
            AgentModelRetryPolicy::read_only(0)
                .expect_err("zero attempts is refused at construction")
                .code(),
            "model-retry-policy-invalid"
        );
        AgentModelRetryPolicy::read_only(2).expect("a bounded read-only retry is valid");

        // Deserialization refuses a policy validate() rejects, so an invalid
        // policy can neither cross the wire nor load from a durable record.
        let valid =
            serde_json::to_value(AgentModelRetryPolicy::DEFAULT).expect("the policy serializes");
        serde_json::from_value::<AgentModelRetryPolicy>(valid)
            .expect("a valid policy deserializes");
        let auto_retry = serde_json::to_value(AgentModelRetryPolicy {
            safety_class: AgentEffectSafetyClass::NonIdempotent,
            max_attempts: 2,
        })
        .expect("the policy serializes");
        serde_json::from_value::<AgentModelRetryPolicy>(auto_retry)
            .expect_err("a non-idempotent auto-retry policy is refused on load");
    }
}
