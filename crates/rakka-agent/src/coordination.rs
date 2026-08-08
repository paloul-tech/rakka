//! Coordination capabilities: handoff, teams, and moderation.
//!
//! Owns `AgentCoordinationCapability` descriptors, which are trusted definition
//! and setup data. The runtime may expose a capability to the model as a tool,
//! but model output can never create the capability, its target, its budget, or
//! its scope.
//!
//! Handoff keeps the same `AgentTaskId`: the source run is fenced, a target run
//! is created, context and artifacts are projected explicitly rather than
//! inherited, and `HandedOff` is recorded only after the target durably
//! accepts. The handoff reuses the delegation idioms wholesale: the
//! [`AgentHandoffRecord`] persists in the same compare-and-set that commits
//! the outbound send effect, strictly before any dispatch; its identity is a
//! pure derivation of the source run's `(turn, slot)` coordinate that doubles
//! as the A2A message id and deduplication key; and the target resolves once,
//! inside that compare-and-set, through the application-owned
//! [`crate::delegation::AgentDelegationCatalog`] — replays reuse the recorded
//! resolution verbatim (open decision 6's disposition). Unlike delegation, a
//! handoff never creates a child task: ingress drives a new assignment
//! generation on the *same* task, so the transfer debits no descendant and
//! opens no fan-in membership.
//!
//! Team coordination owns `AgentTeamId`, bounded membership, and a
//! durable shared task board whose claims, releases, and transfers are atomic
//! under revision and lease fencing. Moderation owns `AgentConversationId`,
//! the participant set, durable turn and round state, and transcript artifacts,
//! where only the current participant may submit and duplicates are rejected.
//! Their policy payloads are revision-only shells here; their fields arrive
//! with their own slices.
//!
//! Every one of these exchanges travels the outbox, inbox, and `rakka-a2a` path
//! even when the participants are colocated, and idle teams, boards, and
//! participants passivate — the board is data, not a resident process.
//!
//! Specification: sections 8.8 through 8.11.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentEffectId, AgentTelemetryContext, AgentTimestampMillis};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::definition::{
    AgentCapabilityId, AgentCoordinationCapabilityKind, AgentRevisionNumber, AgentToolId,
};
use crate::delegation::AgentDelegationTarget;
use crate::identity::{
    validate_tenant, AgentGoalId, AgentHandoffId, AgentIdentityError, AgentOperationId,
    AgentOperationKind, AgentRunId, AgentRunScope, AgentTaskId, TenantId,
};
use crate::model::AgentToolCallId;
use crate::task::{AgentAssignmentGeneration, AgentContentDigest};

/// Result type for coordination construction and validation.
pub type AgentCoordinationResult<T> = Result<T, AgentCoordinationError>;

/// Prefix of every derived [`AgentHandoffId`].
///
/// The suffix is a fixed-length digest, so the id always satisfies the
/// identity bounds whatever the source scope contains, and an id without this
/// prefix was not derived by [`handoff_id_for`].
pub const AGENT_HANDOFF_ID_PREFIX: &str = "handoff-";

/// Maximum serialized bytes of one [`AgentHandoffRecord`].
///
/// The record rides the source run's bounded durable state, so context that
/// does not fit as references belongs behind an artifact — the same rule the
/// delegation record applies to its inline input.
pub const AGENT_HANDOFF_RECORD_MAX_BYTES: usize = 8 * 1024;

/// Maximum explicit context references one handoff projects.
pub const AGENT_HANDOFF_MAX_CONTEXT_REFS: usize = 8;

/// Maximum bytes of one projected context reference.
pub const AGENT_HANDOFF_CONTEXT_REF_MAX_BYTES: usize = 256;

/// Maximum bytes of the model-supplied handoff reason.
pub const AGENT_HANDOFF_REASON_MAX_BYTES: usize = 256;

/// Derives the identity of the handoff one run's turn commits in one slot.
///
/// The derivation is pure — the same length-prefixed digest construction as
/// [`crate::delegation::delegation_id_for`], with a leading domain segment so
/// a handoff and a delegation committed at the same `(turn, slot)` coordinate
/// can never collide — and it doubles verbatim as the A2A message id and
/// deduplication key of the send, so replaying the transition that decided
/// the handoff resolves to the same recorded transfer.
pub fn handoff_id_for(
    scope: &AgentRunScope,
    turn: u64,
    slot: usize,
) -> AgentCoordinationResult<AgentHandoffId> {
    validate_tenant(scope.tenant())?;
    let digest = AgentContentDigest::sha256_of_segments([
        "handoff",
        scope.tenant().as_str(),
        scope.agent().as_str(),
        scope.run().as_str(),
        &turn.to_string(),
        &slot.to_string(),
    ]);
    Ok(AgentHandoffId::new(format!(
        "{AGENT_HANDOFF_ID_PREFIX}{}",
        digest.value
    ))?)
}

/// Derives the stable operation id of the one handoff-result exchange a task
/// ever owes one handoff's source run.
///
/// Pure over `(tenant, handoff)`: the handoff's resolution is absorbing —
/// accepted or refused, first writer wins — so one logical result exists per
/// handoff, ever, and every re-drive after any loss owes the identical
/// operation ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
pub fn handoff_result_operation_id(
    tenant: &TenantId,
    handoff: &AgentHandoffId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::Handoff,
        [tenant.as_str(), handoff.as_str(), "result"],
    )
}

/// One coordination capability descriptor: the policy payload behind one
/// [`AgentCoordinationCapabilityKind`]
/// ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// Descriptors are trusted definition/setup data. They never join the
/// serialized agent definition: admission enforces the *kind* set as the
/// [`crate::definition::AgentEnvelopeDimension::CoordinationCapability`]
/// envelope dimension, and each runtime wiring validates its descriptor
/// against that set at construction — a deployment cannot wire a
/// coordination tool while forgetting the capability that authorizes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCoordinationCapability {
    /// Same-task transfer of responsibility to another agent
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
    Handoff(AgentHandoffPolicy),
    /// Child task/run creation with bounded fan-out and result fan-in
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
    Delegation(AgentDelegationPolicy),
    /// Team leadership over a durable shared task board
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    TeamLeadership(AgentTeamPolicy),
    /// Moderation of a turn-taking conversation
    /// ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)).
    Moderation(AgentModerationPolicy),
}

impl AgentCoordinationCapability {
    /// The capability kind this descriptor realizes.
    #[must_use]
    pub const fn kind(&self) -> AgentCoordinationCapabilityKind {
        match self {
            Self::Handoff(_) => AgentCoordinationCapabilityKind::Handoff,
            Self::Delegation(_) => AgentCoordinationCapabilityKind::Delegation,
            Self::TeamLeadership(_) => AgentCoordinationCapabilityKind::Team,
            Self::Moderation(_) => AgentCoordinationCapabilityKind::Moderation,
        }
    }

    /// Stable kebab-case label: the kind's label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        self.kind().as_label()
    }

    /// The policy revision this descriptor carries.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        match self {
            Self::Handoff(policy) => policy.revision,
            Self::Delegation(policy) => policy.revision,
            Self::TeamLeadership(policy) => policy.revision,
            Self::Moderation(policy) => policy.revision,
        }
    }
}

/// The handoff capability's policy payload
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// The tool id is the one declared coordination tool the loop intercepts
/// into a handoff; the revision is persisted on every handoff record this
/// policy authorizes. The capability revision of specification 8.9 is the
/// definition revision under which the `Handoff` kind was admitted — the
/// record carries both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentHandoffPolicy {
    /// The declared coordination tool the loop intercepts.
    pub tool: AgentToolId,
    /// The policy revision handoff records persist.
    pub revision: AgentRevisionNumber,
}

impl AgentHandoffPolicy {
    /// Creates the policy.
    #[must_use]
    pub const fn new(tool: AgentToolId, revision: AgentRevisionNumber) -> Self {
        Self { tool, revision }
    }
}

/// The delegation capability's policy payload
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// A descriptor mirror of the declared surface of
/// [`crate::delegation::AgentRunDelegationConfig`], which realizes it — the
/// config's catalog stays runtime wiring and is never descriptor data.
/// Derive one with [`crate::delegation::AgentRunDelegationConfig::descriptor`]
/// rather than constructing a parallel copy of trusted config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentDelegationPolicy {
    /// The declared coordination tool the loop intercepts.
    pub tool: AgentToolId,
    /// The declared await verb the loop intercepts into a fan-in close,
    /// when the deployment wires one.
    #[serde(default)]
    pub fan_in_tool: Option<AgentToolId>,
    /// The policy revision.
    pub revision: AgentRevisionNumber,
}

impl AgentDelegationPolicy {
    /// Creates the policy.
    #[must_use]
    pub const fn new(tool: AgentToolId, revision: AgentRevisionNumber) -> Self {
        Self {
            tool,
            fan_in_tool: None,
            revision,
        }
    }

    /// Declares the await verb.
    #[must_use]
    pub fn with_fan_in_tool(mut self, tool: AgentToolId) -> Self {
        self.fan_in_tool = Some(tool);
        self
    }
}

/// The team-leadership capability's policy payload
/// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// A revision-only shell: membership bounds, capability scopes, and
/// creation/expiry policy arrive with the team slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentTeamPolicy {
    /// The policy revision.
    pub revision: AgentRevisionNumber,
}

impl AgentTeamPolicy {
    /// Creates the policy.
    #[must_use]
    pub const fn new(revision: AgentRevisionNumber) -> Self {
        Self { revision }
    }
}

/// The moderation capability's policy payload
/// ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)).
///
/// A revision-only shell: participant, mode, and budget fields arrive with
/// the moderation slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentModerationPolicy {
    /// The policy revision.
    pub revision: AgentRevisionNumber,
}

impl AgentModerationPolicy {
    /// Creates the policy.
    #[must_use]
    pub const fn new(revision: AgentRevisionNumber) -> Self {
        Self { revision }
    }
}

/// The bounded request the model may make through the declared handoff tool.
///
/// This is the *entire* vocabulary model output has over handoff: a skill, a
/// bounded reason, and explicit context references. Unknown fields fail the
/// parse, so an agent id, endpoint, budget, or scope in model output is
/// refused rather than ignored — the catalog and the task's own durable state
/// decide those. Context entries are artifact or content *references* only,
/// never inline payloads: source session and private memory are structurally
/// out of reach, and this vocabulary keeps them out of the projection too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHandoffToolCall {
    /// The skill the model requests a transfer to.
    pub skill: AgentCapabilityId,
    /// The bounded reason for the transfer.
    pub reason: String,
    /// Explicit context/artifact references projected to the target.
    #[serde(default)]
    pub context: Vec<String>,
}

impl AgentHandoffToolCall {
    /// Parses the handoff tool's arguments, failing closed on anything
    /// beyond the declared vocabulary.
    pub fn parse(arguments: &Value) -> AgentCoordinationResult<Self> {
        serde_json::from_value(arguments.clone()).map_err(|error| {
            AgentCoordinationError::InvalidArguments {
                message: error.to_string(),
            }
        })
    }
}

/// Rejects a context projection that exceeds its structural bounds.
fn check_context_refs(context: &[String]) -> AgentCoordinationResult<()> {
    if context.len() > AGENT_HANDOFF_MAX_CONTEXT_REFS {
        return Err(AgentCoordinationError::ContextRefsTooMany {
            count: context.len(),
            maximum: AGENT_HANDOFF_MAX_CONTEXT_REFS,
        });
    }
    for reference in context {
        if reference.is_empty() || reference.len() > AGENT_HANDOFF_CONTEXT_REF_MAX_BYTES {
            return Err(AgentCoordinationError::ContextRefInvalid {
                bytes: reference.len(),
                maximum: AGENT_HANDOFF_CONTEXT_REF_MAX_BYTES,
            });
        }
    }
    Ok(())
}

/// One durable transfer of a task's responsibility from a source run to a
/// target agent ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Persisted in the same compare-and-set that commits the send effect,
/// strictly before any dispatch. Every identity below is either a pure
/// derivation of the source's `(turn, slot)` coordinate or the recorded
/// output of the one catalog resolution this handoff ever performs. The
/// specification's capability revision is [`Self::definition_revision`] —
/// the revision under which the `Handoff` kind was admitted — and its policy
/// revision is [`Self::policy_revision`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentHandoffRecord {
    /// The handoff identity, derived by [`handoff_id_for`].
    pub handoff: AgentHandoffId,
    /// The collaborative goal the source serves, when it serves one.
    #[serde(default)]
    pub goal: Option<AgentGoalId>,
    /// The task whose responsibility transfers. Preserved verbatim: a
    /// handoff never mints a task identity.
    pub task: AgentTaskId,
    /// The source run initiating the transfer.
    pub source_run: AgentRunScope,
    /// The assignment generation the source run serves.
    pub source_generation: AgentAssignmentGeneration,
    /// The skill the model requested.
    pub requested_skill: AgentCapabilityId,
    /// The target the catalog resolved, recorded so a replay never
    /// re-resolves.
    pub resolved: AgentDelegationTarget,
    /// The bounded reason the model supplied.
    pub reason: String,
    /// The handoff policy revision that authorized the transfer.
    pub policy_revision: AgentRevisionNumber,
    /// The agent definition revision the source decided under: the revision
    /// that admitted the handoff capability.
    pub definition_revision: AgentRevisionNumber,
    /// The agent settings revision the source decided under.
    pub settings_revision: AgentRevisionNumber,
    /// Explicit context/artifact references projected to the target — the
    /// only context that crosses; never session or private memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<String>,
    /// The A2A message id of the send: the handoff id verbatim.
    pub a2a_message_id: String,
    /// The deduplication key of the send: the handoff id verbatim.
    pub deduplication_key: String,
    /// The source turn that committed the handoff.
    pub turn: u64,
    /// The effect slot within the turn.
    pub slot: usize,
    /// The send effect's derived identity.
    pub effect: AgentEffectId,
    /// The model tool call this handoff answers — its causation. The
    /// settlement transition records the resolution as this call's tool
    /// result, which is how the turn completes.
    pub call_id: AgentToolCallId,
    /// Trace propagation for the send.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
    /// When the record was committed.
    pub created_at: AgentTimestampMillis,
}

impl AgentHandoffRecord {
    /// Rejects a record that exceeds its structural bounds.
    ///
    /// The whole record refuses rather than truncating: a reason or context
    /// projection that does not fit belongs behind an artifact reference.
    pub fn validate(&self) -> AgentCoordinationResult<()> {
        self.resolved
            .validate()
            .map_err(|error| AgentCoordinationError::TargetInvalid {
                message: error.to_string(),
            })?;
        if self.reason.len() > AGENT_HANDOFF_REASON_MAX_BYTES {
            return Err(AgentCoordinationError::ReasonTooLong {
                bytes: self.reason.len(),
                maximum: AGENT_HANDOFF_REASON_MAX_BYTES,
            });
        }
        check_context_refs(&self.context)?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| AgentCoordinationError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_HANDOFF_RECORD_MAX_BYTES {
            return Err(AgentCoordinationError::RecordTooLarge {
                bytes,
                maximum: AGENT_HANDOFF_RECORD_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// Where one handoff stands.
///
/// `Pending` and `Sent` are the unsettled states: the record is durable and
/// the transfer is committed or recorded at the task, but the target's
/// assignment has not resolved. Every other variant is absorbing for this
/// handoff identity — recovery after ambiguity resolves through the task's
/// recorded provenance, never a resurrected send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentHandoffStatus {
    /// The record is persisted and the send effect is committed or in
    /// flight.
    Pending,
    /// The task durably recorded the transfer and offered the target its
    /// assignment generation.
    Sent {
        /// The assignment generation the task minted toward the target,
        /// when the receipt reported one.
        #[serde(default)]
        target_generation: Option<AgentAssignmentGeneration>,
    },
    /// The target durably accepted its assignment: responsibility has
    /// transferred, and the source terminates `HandedOff`.
    Accepted {
        /// The target run now serving the task.
        target_run: AgentRunId,
        /// The accepted assignment generation.
        generation: AgentAssignmentGeneration,
    },
    /// The task or target refused definitively; the source assignment was
    /// restored and the source run resumes.
    Refused {
        /// Stable machine-readable refusal code.
        code: String,
    },
    /// The send failed definitively without the task recording a transfer.
    Failed {
        /// Stable machine-readable failure code.
        code: String,
    },
}

impl AgentHandoffStatus {
    /// Whether the handoff reached a resolution.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self, Self::Pending | Self::Sent { .. })
    }

    /// Whether the source run is still fenced by this handoff.
    ///
    /// The fence is derived from this status, never from a separate marker:
    /// an unresolved transfer and an accepted one both fence — acceptance
    /// terminates the source in the same compare-and-set that records it —
    /// while a refusal or failure releases the fence and the run resumes.
    #[must_use]
    pub const fn holds_fence(&self) -> bool {
        !matches!(self, Self::Refused { .. } | Self::Failed { .. })
    }

    /// Whether the transfer still awaits the target's durable resolution.
    #[must_use]
    pub const fn awaits_target(&self) -> bool {
        matches!(self, Self::Pending | Self::Sent { .. })
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sent { .. } => "sent",
            Self::Accepted { .. } => "accepted",
            Self::Refused { .. } => "refused",
            Self::Failed { .. } => "failed",
        }
    }
}

/// One handoff's durable home on the source run's loop state.
///
/// The cell commits with the send effect and settles in the same
/// compare-and-set that applies the effect's outcome or the task's
/// handoff-result exchange, so the record, the effect, and the status can
/// never disagree about what happened. A run holds at most one handoff cell,
/// ever: a settled refusal or failure may be replaced by a later attempt
/// under a new `(turn, slot)` identity, and an accepted transfer terminates
/// the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentHandoffCell {
    /// The durable record, persisted before the send.
    pub record: Box<AgentHandoffRecord>,
    /// Where the handoff stands.
    pub status: AgentHandoffStatus,
    /// When the status settled, when it has.
    #[serde(default)]
    pub settled_at: Option<AgentTimestampMillis>,
}

impl AgentHandoffCell {
    /// Creates the pending cell committed alongside the send effect.
    #[must_use]
    pub fn pending(record: Box<AgentHandoffRecord>) -> Self {
        Self {
            record,
            status: AgentHandoffStatus::Pending,
            settled_at: None,
        }
    }

    /// Records the task's durable acknowledgement of the transfer.
    ///
    /// Only a pending cell moves: a settled cell keeps its resolution, and
    /// a duplicate receipt cannot rewrite it.
    pub fn mark_sent(&mut self, target_generation: Option<AgentAssignmentGeneration>) {
        if matches!(self.status, AgentHandoffStatus::Pending) {
            self.status = AgentHandoffStatus::Sent { target_generation };
        }
    }

    /// Settles the cell with the target's durably accepted assignment,
    /// first-writer-wins.
    pub fn settle_accepted(
        &mut self,
        target_run: AgentRunId,
        generation: AgentAssignmentGeneration,
        now: AgentTimestampMillis,
    ) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentHandoffStatus::Accepted {
            target_run,
            generation,
        };
        self.settled_at = Some(now);
    }

    /// Settles the cell with a definitive refusal, first-writer-wins.
    pub fn settle_refused(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentHandoffStatus::Refused { code: code.into() };
        self.settled_at = Some(now);
    }

    /// Settles the cell with a definitive failure, first-writer-wins.
    pub fn settle_failed(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentHandoffStatus::Failed { code: code.into() };
        self.settled_at = Some(now);
    }
}

/// The bounded receipt one completed handoff send returns
/// ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identities and a status label only. A receipt proves the task durably
/// recorded the transfer — never that the target accepted: acceptance
/// returns later through the handoff-result exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentA2aHandoffReceipt {
    /// The handoff the send carried.
    pub handoff: AgentHandoffId,
    /// The assignment generation the task minted toward the target, when
    /// the receiving surface reported one.
    #[serde(default)]
    pub target_generation: Option<AgentAssignmentGeneration>,
    /// The peer's bounded task-state label at the time of the send.
    pub peer_status: String,
}

impl AgentA2aHandoffReceipt {
    /// Rejects a receipt whose status label exceeds its bound.
    pub fn validate(&self) -> AgentCoordinationResult<()> {
        if self.peer_status.len() > crate::delegation::AGENT_A2A_SEND_STATUS_MAX_BYTES {
            return Err(AgentCoordinationError::ReceiptInvalid {
                message: format!(
                    "the peer status label is {} bytes, which exceeds the {} byte bound",
                    self.peer_status.len(),
                    crate::delegation::AGENT_A2A_SEND_STATUS_MAX_BYTES
                ),
            });
        }
        Ok(())
    }
}

/// Errors of coordination construction and validation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentCoordinationError {
    /// An identity failed validation.
    Identity(AgentIdentityError),
    /// The tool call's arguments do not parse as the declared vocabulary.
    InvalidArguments {
        /// The parse failure detail.
        message: String,
    },
    /// The reason exceeds its byte bound.
    ReasonTooLong {
        /// Serialized size.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The context projection carries too many references.
    ContextRefsTooMany {
        /// The reference count.
        count: usize,
        /// The bound.
        maximum: usize,
    },
    /// A context reference is empty or exceeds its byte bound.
    ContextRefInvalid {
        /// The reference's size.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The record exceeds its serialized byte bound.
    RecordTooLarge {
        /// Serialized size.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The resolved target failed validation.
    TargetInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// A receipt failed validation.
    ReceiptInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// The record or a related value failed to encode.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
}

impl AgentCoordinationError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Identity(_) => "handoff-identity-invalid",
            Self::InvalidArguments { .. } => "handoff-invalid-arguments",
            Self::ReasonTooLong { .. } => "handoff-reason-too-long",
            Self::ContextRefsTooMany { .. } | Self::ContextRefInvalid { .. } => {
                "handoff-context-invalid"
            }
            Self::RecordTooLarge { .. } => "handoff-record-too-large",
            Self::TargetInvalid { .. } => "handoff-target-invalid",
            Self::ReceiptInvalid { .. } => "handoff-receipt-invalid",
            Self::Encoding { .. } => "handoff-record-unencodable",
        }
    }
}

impl Display for AgentCoordinationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::InvalidArguments { message } => {
                write!(f, "the handoff arguments do not parse: {message}")
            }
            Self::ReasonTooLong { bytes, maximum } => write!(
                f,
                "the handoff reason is {bytes} bytes, which exceeds the {maximum} byte bound"
            ),
            Self::ContextRefsTooMany { count, maximum } => write!(
                f,
                "the handoff projects {count} context references, which exceeds the bound of \
                 {maximum}"
            ),
            Self::ContextRefInvalid { bytes, maximum } => write!(
                f,
                "a handoff context reference is {bytes} bytes; it must be non-empty and at most \
                 {maximum} bytes"
            ),
            Self::RecordTooLarge { bytes, maximum } => write!(
                f,
                "the handoff record is {bytes} bytes, which exceeds the {maximum} byte bound"
            ),
            Self::TargetInvalid { message } => {
                write!(f, "the resolved handoff target is invalid: {message}")
            }
            Self::ReceiptInvalid { message } => {
                write!(f, "the handoff send receipt is invalid: {message}")
            }
            Self::Encoding { message } => {
                write!(f, "the handoff record failed to encode: {message}")
            }
        }
    }
}

impl Error for AgentCoordinationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentCoordinationError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentId, AgentRunId, TenantId};

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("tenant-a"),
            AgentId::new("agent-source").expect("agent"),
            AgentRunId::new("run-1").expect("run"),
        )
        .expect("scope")
    }

    #[test]
    fn handoff_ids_are_pure_and_domain_separated() {
        let first = handoff_id_for(&scope(), 3, 1).expect("id");
        let replay = handoff_id_for(&scope(), 3, 1).expect("id");
        assert_eq!(first, replay);
        assert!(first.as_str().starts_with(AGENT_HANDOFF_ID_PREFIX));
        let other_slot = handoff_id_for(&scope(), 3, 2).expect("id");
        assert_ne!(first, other_slot);
        let delegation =
            crate::delegation::delegation_id_for(&scope(), 3, 1).expect("delegation id");
        assert_ne!(
            first.as_str().trim_start_matches(AGENT_HANDOFF_ID_PREFIX),
            delegation
                .as_str()
                .trim_start_matches(crate::delegation::AGENT_DELEGATION_ID_PREFIX),
            "a handoff and a delegation at the same coordinate must never share a digest"
        );
    }

    #[test]
    fn handoff_result_operation_ids_are_pure() {
        let tenant = TenantId::new("tenant-a");
        let handoff = handoff_id_for(&scope(), 1, 0).expect("id");
        let first = handoff_result_operation_id(&tenant, &handoff).expect("operation");
        let replay = handoff_result_operation_id(&tenant, &handoff).expect("operation");
        assert_eq!(first, replay);
    }

    #[test]
    fn descriptor_kinds_align() {
        let handoff = AgentCoordinationCapability::Handoff(AgentHandoffPolicy::new(
            AgentToolId::new("transfer").expect("tool"),
            AgentRevisionNumber::new(1),
        ));
        assert_eq!(handoff.kind(), AgentCoordinationCapabilityKind::Handoff);
        assert_eq!(handoff.as_label(), "handoff");
        assert_eq!(handoff.revision(), AgentRevisionNumber::new(1));
    }

    #[test]
    fn tool_call_parse_fails_closed_on_unknown_fields() {
        let arguments = serde_json::json!({
            "skill": "billing-transfer",
            "reason": "needs billing authority",
            "target_agent": "agent-b",
        });
        let error = AgentHandoffToolCall::parse(&arguments).expect_err("must refuse");
        assert!(matches!(
            error,
            AgentCoordinationError::InvalidArguments { .. }
        ));
    }

    #[test]
    fn settlement_is_first_writer_wins() {
        let record = AgentHandoffRecord {
            handoff: handoff_id_for(&scope(), 1, 0).expect("id"),
            goal: None,
            task: AgentTaskId::new("task-1").expect("task"),
            source_run: scope(),
            source_generation: AgentAssignmentGeneration::new(1),
            requested_skill: AgentCapabilityId::new("billing").expect("skill"),
            resolved: AgentDelegationTarget::new(
                AgentId::new("agent-target").expect("agent"),
                crate::definition::AgentTaskDefinitionId::new("refund").expect("definition"),
            ),
            reason: "needs billing authority".into(),
            policy_revision: AgentRevisionNumber::new(1),
            definition_revision: AgentRevisionNumber::new(1),
            settings_revision: AgentRevisionNumber::new(1),
            context: Vec::new(),
            a2a_message_id: "handoff-x".into(),
            deduplication_key: "handoff-x".into(),
            turn: 1,
            slot: 0,
            effect: AgentEffectId::new("effect-1"),
            call_id: AgentToolCallId::new("call-1").expect("call"),
            telemetry: AgentTelemetryContext::default(),
            created_at: AgentTimestampMillis::new(1),
        };
        record.validate().expect("valid");
        let mut cell = AgentHandoffCell::pending(Box::new(record));
        assert!(cell.status.holds_fence());
        assert!(cell.status.awaits_target());
        cell.mark_sent(Some(AgentAssignmentGeneration::new(2)));
        assert!(matches!(cell.status, AgentHandoffStatus::Sent { .. }));
        cell.settle_refused("handoff-target-refused", AgentTimestampMillis::new(2));
        assert!(!cell.status.holds_fence());
        cell.settle_accepted(
            AgentRunId::new("task-1-gen-2").expect("run"),
            AgentAssignmentGeneration::new(2),
            AgentTimestampMillis::new(3),
        );
        assert!(
            matches!(cell.status, AgentHandoffStatus::Refused { .. }),
            "a settled cell must keep its resolution"
        );
    }
}
