//! The typed task entity, its definition, and its lifecycle.
//!
//! Owns [`AgentTaskEntity`], keyed by `(TenantId, AgentTaskId)`, with a
//! serializable command protocol; the versioned [`AgentTaskDefinition`] carrying
//! typed input and result schema references, deterministic result rules,
//! rejection limits, dependency policy, and per-task budgets; the task lifecycle
//! and its durable, bounded, acyclic dependencies; and the bounded materialized
//! state that keeps history and content behind cursors and artifact references.
//!
//! Result proposals are validated in-entity by deterministic rules only. Model-
//! assisted evaluation is a durable effect, never in-entity I/O. Tasks
//! deliberately left unassigned to an agent and completed by an authenticated
//! human or service travel the same typed validation path.
//!
//! Specification: sections 6.4, 9.1, 9.2, 9.6, and 9.8; human-owned tasks in
//! section 8.12. Filled by slice 1.4; human-owned completion by slice 5.4.
//!
//! # The entity is a choreography participant
//!
//! The task's durable state carries the [`AgentExchangeJournal`], so an exchange
//! it owes is persisted in the *same* compare-and-set as the domain transition
//! that owed it, and a decision it returns is persisted in the same
//! compare-and-set as the transition that produced it. Every cross-entity step
//! travels the substrate of [`crate::choreography`]; there is no colocated
//! shortcut.
//!
//! ```text
//! ingress (durable, deduplicated)
//!     -> Create command (operation id from the ingress)      [1 CAS]
//!     -> assignment decision, read from the agent's durable
//!        definition and admission state — never a synchronous
//!        round trip through AgentEntity                      [1 CAS + run-creation owed]
//!     -> run-creation exchange -> AgentRunEntity
//!     -> run acceptance reply -> task InProgress             [1 CAS]
//!     ...
//!     -> result-proposal exchange from the run
//!     -> deterministic validation, durable accept/reject     [1 CAS]
//!     -> the decision travels home as the exchange's reply
//! ```
//!
//! A delegating run (M4) creates a child task through the
//! [`AgentExchangeKind::Creation`] exchange instead of an ingress command; both
//! reach the same bounded transition, so the two paths cannot diverge.
//!
//! # The assignment decision is a separate transition
//!
//! Creating a task and deciding its assignment are two transitions, because the
//! decision needs the agent's durable definition and admission state and reading
//! it is I/O. The entity performs that bounded durable read *outside* the
//! transition — [`load_agent_entity_state`], never a command round trip through
//! [`crate::agent::AgentEntity`] ([specification 9.8](../../../docs/plans/rakka-agent/spec.md))
//! — and then runs a pure, bounded transition over the facts it read. The
//! transition is idempotent: it is fenced on the task's own status and current
//! assignment, and the run-creation operation id is derived from the task and
//! the assignment generation, so a replay resolves to the same run rather than a
//! second one.
//!
//! # Bounded state
//!
//! The materialized record ([`AgentTask`]) holds only what the next legal
//! transition needs: identity, status, revisions, a bounded dependency summary,
//! the *current* assignment, pending references, the accepted result reference,
//! and the terminal reason. It never accumulates: superseded assignments, result
//! proposals, and rejection decisions leave the record and become
//! [`AgentTaskHistoryEntry`] rows, readable only through the bounded, authorized
//! cursor of an [`AgentTaskHistoryStore`]
//! ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)). Content beyond
//! [`AGENT_TASK_INLINE_CONTENT_MAX_BYTES`] must arrive as an artifact reference;
//! a creation whose materialized record would exceed
//! [`AGENT_TASK_MATERIALIZED_MAX_BYTES`] is rejected rather than persisted.
//!
//! History is written through a bounded outbox in the entity's own state, for
//! the same reason the exchange journal is: an entry is committed by the
//! transition that produced it, and
//! [`AgentTaskEntityStore::settle_side_effects`] appends it to the sink
//! afterwards. An append is idempotent on `(scope, sequence)`, so a flush
//! interrupted anywhere is safe to re-drive, and no transition can commit while
//! forgetting the history it owes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentTelemetryContext, AgentTimestampMillis, ArtifactRef,
    StateSchemaVersion,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, MetricsRecorder,
    NoopMetricsRecorder, ReplyTo,
};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeResult, ClusterSharding, ClusterShardingResult, Entity,
    EntityContext, EntityId, EntityTypeKey, EntityTypeRegistration, ShardBufferConfig,
    ShardedEntityRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::admission::AgentAdmissionRefusal;
use crate::agent::{load_agent_entity_state, AgentEntityState, AgentLifecycleStatus};
use crate::budget::{
    AgentBudgetAllocation, AgentBudgetConsumption, AgentBudgetDimension, AgentBudgetExhaustion,
    AgentBudgetGrant, AgentEscrowChildId, AgentEscrowError, AgentEscrowLedger,
    AGENT_ESCROW_CHILD_CAPACITY, AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN,
};
use crate::choreography::{
    drive_pending_exchanges, AgentChoreographyError, AgentEntityAddress, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter,
    AgentExchangeState, AgentExchangeTransition, AGENT_EXCHANGE_PENDING_CAPACITY,
};
use crate::definition::{
    AgentBudgetCeilings, AgentCapabilityId, AgentOperationClass, AgentPolicyRefs,
    AgentRevisionNumber, AgentRevisionProvenance, AgentTaskDefinitionId,
};
use crate::evaluation::{
    AgentGoalEvaluationRecord, AgentGoalStagnationAction, AgentGoalStagnationPolicy,
    AgentStagnationTrigger,
};
use crate::goal::{
    AgentGoalCriteriaSource, AgentGoalDecision, AgentGoalDelegationBudget, AgentGoalError,
    AgentGoalExhaustionAction, AgentGoalMode, AgentGoalOutcome, AgentGoalSpecDraft,
    AgentGoalSpecRevision, AgentGoalState, AgentGoalStatus, AgentGoalStatusView,
    AgentGoalTerminalReason, AgentGoalWaitReason,
};
use crate::identity::{
    validated_id, AgentDelegationId, AgentGoalId, AgentId, AgentIdentityError, AgentOperationId,
    AgentOperationKind, AgentRunId, AgentRunScope, AgentScope, AgentTaskId, AgentTaskScope,
    AgentWakeId, TenantId, AGENT_IDENTITY_MAX_LENGTH,
};
use crate::observability::{
    record_agent_domain_counter, METRIC_AGENT_DEPENDENCY_OUTCOMES, METRIC_AGENT_EPOCHS,
    METRIC_AGENT_GOAL_LIFECYCLE, METRIC_AGENT_GOAL_STAGNATION, METRIC_AGENT_GOAL_STATUS,
    METRIC_AGENT_HUMAN_RESULTS, METRIC_AGENT_WAKE_DISPOSITIONS,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION, CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION,
    CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
};
use crate::wake::{
    epoch_admission_operation_id, epoch_result_operation_id, epoch_task_id_for_wake,
    AgentEpochOutcomeClass, AgentEpochRef, AgentGoalLifecycleStatus, AgentWakeBinding,
    AgentWakeControllerState, AgentWakeError, AgentWakeOutcome, AgentWakePolicyRevision,
    AgentWakeStatusView, ScheduleRevision,
};
use crate::wake_timers::{AgentWakeRewakeParker, AgentWakeTimerError};

/// Default sharded entity type of the typed-task entity.
pub const DEFAULT_AGENT_TASK_ENTITY_TYPE: &str = "RakkaAgentTask";

/// Maximum length, in bytes, of a task definition's outcome-oriented
/// description.
pub const AGENT_TASK_DESCRIPTION_MAX_LENGTH: usize = 1024;

/// Maximum length, in bytes, of any bounded free-text detail persisted by the
/// task: a rejection detail, a refusal detail, a cancellation reason, or the
/// sanitized feedback returned to a run.
pub const AGENT_TASK_DETAIL_MAX_LENGTH: usize = 512;

/// Largest inline task input or result content, in bytes, once serialized.
///
/// Anything larger must arrive as an [`ArtifactRef`]. The bound is what keeps a
/// task's materialized state, its mailbox, and the exchange payload that carries
/// its assignment all within their own limits
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
pub const AGENT_TASK_INLINE_CONTENT_MAX_BYTES: usize = 8 * 1024;

/// Largest materialized task record, in bytes, once serialized.
///
/// Checked before a creation is persisted, so an oversized definition or input
/// is refused at admission rather than discovered when durable state has already
/// grown unbounded. It bounds [`AgentTask`], the task's own domain record; the
/// exchange journal alongside it is bounded by the substrate's own constants.
pub const AGENT_TASK_MATERIALIZED_MAX_BYTES: usize = 32 * 1024;

/// Bytes of growth headroom an admitted task record keeps below
/// [`AGENT_TASK_MATERIALIZED_MAX_BYTES`].
///
/// After admission, the materialized record may still grow by at most one live
/// assignment, one assignment refusal, one rejection decision, one terminal
/// reason, and one resolution outcome per declared dependency — each of them
/// individually bounded, and together under this reserve. Creation and
/// post-creation dependency declarations therefore enforce the materialized
/// bound *minus* this reserve: a task the entity admits can never later be
/// refused its own assignment, rejection, or terminal reason because the
/// record it was admitted with left no room. It is the same reservation
/// [`AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH`] makes for derived run ids.
pub const AGENT_TASK_STATE_GROWTH_RESERVE_BYTES: usize = 6 * 1024;

/// Default horizon, in milliseconds, a board-governed task may wait unclaimed
/// before the settle pass expires it: one day.
pub const AGENT_TASK_DEFAULT_MAX_UNCLAIMED_MILLIS: u64 = 86_400_000;

/// Maximum number of deterministic result rules one task definition may carry.
pub const AGENT_TASK_MAX_RESULT_RULES: usize = 32;

/// Maximum length, in bytes, of the JSON pointer one result rule inspects.
pub const AGENT_TASK_RULE_POINTER_MAX_LENGTH: usize = 256;

/// Maximum number of permitted values one one-of result rule may carry.
pub const AGENT_TASK_RULE_ONE_OF_MAX_VALUES: usize = 32;

/// Maximum length, in bytes, of one permitted one-of value.
pub const AGENT_TASK_RULE_VALUE_MAX_LENGTH: usize = 256;

/// Maximum number of dependencies one task may declare.
pub const AGENT_TASK_MAX_DEPENDENCIES: usize = 32;

/// Maximum declared ancestor depth carried by a dependency declaration.
///
/// The ancestors are what make the dependency graph acyclic without a global
/// read: a task refuses a dependency that already lists the task itself as an
/// ancestor. The chain is bounded because durable state is.
pub const AGENT_TASK_MAX_DEPENDENCY_DEPTH: usize = 32;

/// Maximum number of dependents one task may durably register.
///
/// Symmetric with [`AGENT_TASK_MAX_DEPENDENCIES`]: the reverse edges are
/// bounded exactly like the forward edges, so neither side of the dependency
/// graph can grow a task's materialized record without bound. Entries are
/// never pruned, so the ceiling counts every dependent that ever registered
/// while the task was nonterminal, not the ones still live.
///
/// The thirty-third registration is refused definitively — and the refusal is
/// an answer, not silence: the refused dependent resolves that forward edge
/// *failed* and applies the policy the edge declared, because an upstream that
/// holds no registry entry for it can never send it an outcome.
pub const AGENT_TASK_MAX_DEPENDENTS: usize = 32;

/// How many rejected human-submission operation ids the task remembers past
/// the operation log's bounded window.
///
/// The ring is what keeps a replayed rejected submission from spending the
/// rejection budget twice: the most recent rejection echoes from the task's
/// materialized `last_rejection`, and an older replay found in the ring is
/// refused without a second validation decision. The rejection *limit* is
/// definition-declared with no upper bound, so the echo needs its own cap.
pub const AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY: usize = 32;

/// Maximum number of evidence artifact references one result proposal may carry.
pub const AGENT_TASK_MAX_EVIDENCE_ARTIFACTS: usize = 8;

/// How many resolved operation ids the task entity remembers for deduplication.
///
/// The window is the fast path; every command transition is additionally fenced
/// by the task's own durable state, so a replay older than the window is still
/// refused rather than applied twice.
pub const AGENT_TASK_OPERATION_LOG_CAPACITY: usize = 64;

/// How many history entries the task may owe its sink at once.
///
/// An owed entry is never dropped — that would silently lose audit history — so
/// this is a bound the entity enforces at its door: a task whose sink has been
/// unreachable long enough to fill the outbox refuses further transitions rather
/// than growing an unbounded durable record.
pub const AGENT_TASK_PENDING_HISTORY_CAPACITY: usize = 64;

/// The most history entries any one transition can record.
///
/// A creation is the worst case: one row for the task, one for each dependency
/// it declares, one for the goal contract it may institute active, and one for
/// the assignment its own eligibility may decide. The entity requires this much
/// headroom before it runs a transition, which is what lets recording an entry
/// be infallible — an owed entry is never dropped, and a backed-up sink is
/// refused at the entity's door instead.
pub const AGENT_TASK_MAX_HISTORY_PER_TRANSITION: usize = AGENT_TASK_MAX_DEPENDENCIES + 3;

/// Largest page one history cursor may request.
pub const AGENT_TASK_HISTORY_MAX_PAGE_SIZE: usize = 64;

/// Default page size of a history cursor.
pub const AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE: usize = 16;

/// Payload type of an [`AgentTaskCreation`] exchange command.
pub const AGENT_TASK_CREATION_PAYLOAD_TYPE: &str = "rakka.agent.TaskCreation";

/// Payload type of an [`AgentExchangeKind::EpochResult`] exchange.
pub const AGENT_EPOCH_RESULT_PAYLOAD_TYPE: &str = "rakka.agent.EpochResult";

/// Payload type of the controller's reply to an epoch result.
pub const AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.EpochResultOutcome";

/// Payload type of an [`AgentExchangeKind::DelegationResult`] exchange.
pub const AGENT_DELEGATION_RESULT_PAYLOAD_TYPE: &str = "rakka.agent.DelegationResult";

/// Payload type of the parent run's reply to a delegation result.
pub const AGENT_DELEGATION_RESULT_OUTCOME_PAYLOAD_TYPE: &str =
    "rakka.agent.DelegationResultOutcome";

/// Payload type of an [`AgentExchangeKind::GoalEvaluation`] exchange: the
/// full [`crate::evaluation::AgentGoalEvaluationRecord`].
pub const AGENT_GOAL_EVALUATION_PAYLOAD_TYPE: &str = "rakka.agent.GoalEvaluation";

/// Payload type of the coordinating task's reply to a goal evaluation.
pub const AGENT_GOAL_EVALUATION_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.GoalEvaluationOutcome";

/// Payload type of an [`AgentExchangeKind::RunCancel`] exchange command.
pub const AGENT_RUN_CANCEL_PAYLOAD_TYPE: &str = "rakka.agent.RunCancel";

/// Payload type of the run's receipt replying to a [`AgentExchangeKind::RunCancel`].
pub const AGENT_RUN_CANCEL_RECEIPT_PAYLOAD_TYPE: &str = "rakka.agent.RunCancelReceipt";

/// Payload type of an [`AgentExchangeKind::DelegationCancel`] exchange command.
pub const AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE: &str = "rakka.agent.DelegationCancel";

/// Payload type of the child task's receipt replying to a
/// [`AgentExchangeKind::DelegationCancel`].
pub const AGENT_DELEGATION_CANCEL_RECEIPT_PAYLOAD_TYPE: &str =
    "rakka.agent.DelegationCancelReceipt";

/// Payload type of an [`AgentExchangeKind::HandoffResult`] exchange notice.
pub const AGENT_HANDOFF_RESULT_PAYLOAD_TYPE: &str = "rakka.agent.HandoffResult";

/// Payload type of the source run's receipt replying to an
/// [`AgentExchangeKind::HandoffResult`].
pub const AGENT_HANDOFF_RESULT_RECEIPT_PAYLOAD_TYPE: &str = "rakka.agent.HandoffResultReceipt";

/// Payload type of an [`AgentExchangeKind::DependencyRegistration`] exchange
/// command.
pub const AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE: &str = "rakka.agent.DependencyRegistration";

/// Payload type of the upstream task's receipt replying to a
/// [`AgentExchangeKind::DependencyRegistration`].
pub const AGENT_DEPENDENCY_REGISTRATION_RECEIPT_PAYLOAD_TYPE: &str =
    "rakka.agent.DependencyRegistrationReceipt";

/// Payload type of an [`AgentExchangeKind::DependencyOutcome`] exchange
/// notice.
pub const AGENT_DEPENDENCY_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.DependencyOutcome";

/// Payload type of the dependent task's receipt replying to a
/// [`AgentExchangeKind::DependencyOutcome`].
pub const AGENT_DEPENDENCY_OUTCOME_RECEIPT_PAYLOAD_TYPE: &str =
    "rakka.agent.DependencyOutcomeReceipt";

/// Payload type of an [`AgentRunAssignment`] exchange command.
pub const AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE: &str = "rakka.agent.RunAssignment";

/// Payload type of an [`AgentRunAcceptance`] exchange result.
pub const AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE: &str = "rakka.agent.RunAcceptance";

/// Payload type of an [`AgentTaskResultProposal`] exchange command.
pub const AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE: &str = "rakka.agent.TaskResultProposal";

/// Payload type of an [`AgentTaskDecision`] exchange result.
pub const AGENT_TASK_DECISION_PAYLOAD_TYPE: &str = "rakka.agent.TaskDecision";

/// Refusal code of an [`AgentTaskDecision::Refused`] fenced by a newer
/// assignment generation.
///
/// It is the one refusal the run maps to a distinct terminal status
/// ([`crate::run::AgentRunStatus::Superseded`]), so both sides name it through
/// this constant rather than each holding its own copy of the literal. The
/// string is wire and durable surface — it never changes.
pub const AGENT_TASK_REFUSAL_STALE_GENERATION: &str = "stale-assignment-generation";

/// Refusal code of a proposal the task would not validate because its
/// cancellation is propagating
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// Like [`AGENT_TASK_REFUSAL_STALE_GENERATION`], it is the one refusal the run
/// maps to a distinct terminal disposition — here the cancellation wind-down,
/// not a failure — so both sides name it through this constant rather than
/// each holding its own copy of the literal. Under the deferred-terminal task
/// the refusal carries a *nonterminal* status, so the run cannot infer the
/// cancellation from the status alone; this code is what it reads. The string
/// is wire and durable surface — it never changes.
pub const AGENT_TASK_REFUSAL_CANCEL_REQUESTED: &str = "task-cancel-requested";

/// Payload type of the [`AgentTaskOutcome`] an accepted
/// [`AgentExchangeKind::Creation`] reply carries.
///
/// A refused creation carries an [`AgentTaskDecision::Refused`] under
/// [`AGENT_TASK_DECISION_PAYLOAD_TYPE`] instead; an initiator settling a
/// creation branches on the reply's payload type, and each type names exactly
/// one wire shape.
pub const AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.TaskCreationOutcome";

/// The longest `-gen-{generation}` suffix a derived run id can carry: the
/// separator plus a full-width `u64` generation.
const AGENT_TASK_RUN_SUFFIX_MAX_LENGTH: usize = "-gen-".len() + 20;

/// Longest id an agent-owned task may use, in bytes.
///
/// The run serving one assignment generation derives its id as
/// `{task}-gen-{generation}` ([`run_id_for_assignment`]), and the derived id
/// must itself satisfy [`AGENT_IDENTITY_MAX_LENGTH`]. Creation reserves room
/// for the longest possible suffix, so a task admitted here can never become
/// unassignable when its assignment is decided.
pub const AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH: usize =
    AGENT_IDENTITY_MAX_LENGTH - AGENT_TASK_RUN_SUFFIX_MAX_LENGTH;

const DEFAULT_AGENT_TASK_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

/// The source of the durable timestamps a task's transitions are stamped with.
///
/// It is injectable because a durable timestamp is part of the record a
/// transition writes: a test that drives recovery has to be able to control it,
/// exactly as the choreography tests control theirs.
pub type AgentTaskClock = Arc<dyn Fn() -> AgentTimestampMillis + Send + Sync>;

/// A clock reading the system's wall clock.
#[must_use]
pub fn system_task_clock() -> AgentTaskClock {
    Arc::new(|| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        AgentTimestampMillis::new(millis)
    })
}

/// Result type for typed-task operations.
pub type AgentTaskResult<T> = Result<T, AgentTaskError>;

validated_id! {
    /// Stable identity of one schema a task's input or result is validated
    /// against ([specification 9.1](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The schema itself is application-owned. The task persists only the
    /// reference, so a durable result can always be interpreted against the
    /// exact schema revision that accepted it.
    pub AgentSchemaId, "agent_schema_id"
}

validated_id! {
    /// Stable identity of one deterministic result rule.
    ///
    /// It is persisted on every rejection, so an operator can always name the
    /// rule that refused a result
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    pub AgentTaskRuleId, "agent_task_rule_id"
}

/// A versioned reference to the schema a typed value must satisfy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentSchemaRef {
    /// Stable schema identity.
    pub schema_id: AgentSchemaId,
    /// Monotonic schema revision.
    pub version: AgentRevisionNumber,
}

impl AgentSchemaRef {
    /// Creates a schema reference.
    #[must_use]
    pub const fn new(schema_id: AgentSchemaId, version: AgentRevisionNumber) -> Self {
        Self { schema_id, version }
    }
}

impl Display for AgentSchemaRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.schema_id, self.version)
    }
}

/// Monotonic assignment generation of one task
/// ([specification 9.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every assignment decision advances it, and it fences the run it created: a
/// proposal or acceptance carrying a generation the task has passed is refused,
/// so a superseded run can neither schedule work nor complete the public task.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentAssignmentGeneration(u64);

impl AgentAssignmentGeneration {
    /// The generation of a task that has never been assigned.
    pub const UNASSIGNED: Self = Self(0);

    /// Creates a generation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next generation.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for AgentAssignmentGeneration {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Monotonic sequence of one task history entry.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentTaskHistorySequence(u64);

impl AgentTaskHistorySequence {
    /// The first sequence a task's history uses.
    pub const FIRST: Self = Self(1);

    /// Creates a sequence.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next sequence.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for AgentTaskHistorySequence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Algorithm that produced an [`AgentContentDigest`].
///
/// The algorithm travels with the digest so it can be strengthened without
/// rewriting the records that already carry one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDigestAlgorithm {
    /// FNV-1a over canonical JSON: a stable content fingerprint, and nothing
    /// more.
    Fnv1a128,
    /// SHA-256 over canonical JSON: a collision-resistant digest suitable for a
    /// security decision. This is the algorithm a digest-bound authorization
    /// grant binds ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)):
    /// only a second-preimage-resistant digest can decide whether an approval
    /// still binds the exact arguments a dispatch is about to send.
    Sha256,
}

impl AgentDigestAlgorithm {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Fnv1a128 => "fnv1a-128",
            Self::Sha256 => "sha2-256",
        }
    }

    /// Whether the algorithm is a cryptographic digest a security decision may
    /// rely on. FNV-1a is a fingerprint and must never gate authorization.
    #[must_use]
    pub const fn is_cryptographic(self) -> bool {
        match self {
            Self::Fnv1a128 => false,
            Self::Sha256 => true,
        }
    }
}

impl Display for AgentDigestAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A stable fingerprint of bounded content.
///
/// It identifies *which* value a durable decision was made about — the proposal
/// a rejection refused, the input an assignment carried — so an operator reading
/// history can tell one proposal from another and detect a value that changed
/// under a stable identifier
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The default [`AgentContentDigest::of_json`] fingerprint is deliberately
/// **not** a security boundary. The digest-bound authorization grants of
/// [specification 12.3](../../../docs/plans/rakka-agent/spec.md) need a
/// cryptographic digest, so [`AgentContentDigest::sha256_of_json`] produces a
/// [`AgentDigestAlgorithm::Sha256`] digest for exactly that use; the FNV
/// fingerprint must not be used to decide whether an approval still binds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentContentDigest {
    /// Algorithm that produced the value.
    pub algorithm: AgentDigestAlgorithm,
    /// Lowercase hexadecimal digest value.
    pub value: String,
}

impl AgentContentDigest {
    /// Fingerprints a JSON value over its canonical encoding.
    ///
    /// Canonical means object keys in sorted order, so two structurally equal
    /// values always fingerprint alike regardless of how they were built or
    /// which binary serialized them.
    #[must_use]
    pub fn of_json(value: &Value) -> Self {
        let mut canonical = String::new();
        write_canonical_json(value, &mut canonical);
        Self::of_bytes(canonical.as_bytes())
    }

    /// Fingerprints raw bytes.
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
        const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

        let mut hash = OFFSET;
        for byte in bytes {
            hash ^= u128::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        Self {
            algorithm: AgentDigestAlgorithm::Fnv1a128,
            value: format!("{hash:032x}"),
        }
    }

    /// Computes the cryptographic [`AgentDigestAlgorithm::Sha256`] digest of a
    /// JSON value over its canonical encoding.
    ///
    /// This is the constructor a digest-bound authorization grant uses
    /// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)): the
    /// canonicalization matches [`Self::of_json`], so the same structural value
    /// always produces the same digest, and SHA-256 makes a changed argument
    /// computationally impossible to disguise under an unchanged digest.
    #[must_use]
    pub fn sha256_of_json(value: &Value) -> Self {
        let mut canonical = String::new();
        write_canonical_json(value, &mut canonical);
        Self::sha256_of_bytes(canonical.as_bytes())
    }

    /// Computes the cryptographic [`AgentDigestAlgorithm::Sha256`] digest of raw
    /// bytes.
    #[must_use]
    pub fn sha256_of_bytes(bytes: &[u8]) -> Self {
        Self {
            algorithm: AgentDigestAlgorithm::Sha256,
            value: sha256_hex(bytes),
        }
    }

    /// Computes the [`AgentDigestAlgorithm::Sha256`] digest of a segment
    /// sequence under the injective length-prefixed canonical encoding.
    ///
    /// This is the one encoding behind every derived durable identity — wake
    /// ids, delegation ids, workflow-invocation ids: each segment is written
    /// as `<decimal byte length>:<bytes>`, so no boundary can be forged by
    /// segment content. The format is a persisted compatibility surface (the
    /// golden vectors in `tests/wake_identity.rs` pin it); changing it
    /// requires a migration.
    #[must_use]
    pub fn sha256_of_segments<'a>(segments: impl IntoIterator<Item = &'a str>) -> Self {
        let mut canonical = Vec::new();
        for segment in segments {
            canonical.extend_from_slice(segment.len().to_string().as_bytes());
            canonical.push(b':');
            canonical.extend_from_slice(segment.as_bytes());
        }
        Self::sha256_of_bytes(&canonical)
    }
}

impl Display for AgentContentDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.value)
    }
}

/// Writes a JSON value in a canonical form: object keys sorted, no insignificant
/// whitespace.
///
/// `serde_json` already orders map keys, but only while its `preserve_order`
/// feature is off — a feature any crate in a build may turn on. The canonical
/// form is written explicitly so a durable fingerprint can never depend on which
/// features a binary happened to unify.
fn write_canonical_json(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let ordered: BTreeMap<&String, &Value> = map.iter().collect();
            out.push('{');
            for (index, (key, entry)) in ordered.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical_json(entry, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Computes the SHA-256 digest of `bytes` as a lowercase hexadecimal string.
///
/// SHA-256 is implemented inline in safe Rust — exactly as the FNV-1a
/// fingerprint above is — so a security-relevant digest depends on no external
/// crate and stays fully reviewable under this crate's `forbid(unsafe_code)`.
/// The algorithm is FIPS 180-4; the round constants and initial hash values are
/// the standard ones, and the unit tests pin the empty-string and `"abc"`
/// vectors.
#[must_use]
fn sha256_hex(bytes: &[u8]) -> String {
    // First 32 bits of the fractional parts of the square roots of the first
    // eight primes.
    let mut h: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    // First 32 bits of the fractional parts of the cube roots of the first
    // sixty-four primes.
    const K: [u32; 64] = [
        0x428a_2f98,
        0x7137_4491,
        0xb5c0_fbcf,
        0xe9b5_dba5,
        0x3956_c25b,
        0x59f1_11f1,
        0x923f_82a4,
        0xab1c_5ed5,
        0xd807_aa98,
        0x1283_5b01,
        0x2431_85be,
        0x550c_7dc3,
        0x72be_5d74,
        0x80de_b1fe,
        0x9bdc_06a7,
        0xc19b_f174,
        0xe49b_69c1,
        0xefbe_4786,
        0x0fc1_9dc6,
        0x240c_a1cc,
        0x2de9_2c6f,
        0x4a74_84aa,
        0x5cb0_a9dc,
        0x76f9_88da,
        0x983e_5152,
        0xa831_c66d,
        0xb003_27c8,
        0xbf59_7fc7,
        0xc6e0_0bf3,
        0xd5a7_9147,
        0x06ca_6351,
        0x1429_2967,
        0x27b7_0a85,
        0x2e1b_2138,
        0x4d2c_6dfc,
        0x5338_0d13,
        0x650a_7354,
        0x766a_0abb,
        0x81c2_c92e,
        0x9272_2c85,
        0xa2bf_e8a1,
        0xa81a_664b,
        0xc24b_8b70,
        0xc76c_51a3,
        0xd192_e819,
        0xd699_0624,
        0xf40e_3585,
        0x106a_a070,
        0x19a4_c116,
        0x1e37_6c08,
        0x2748_774c,
        0x34b0_bcb5,
        0x391c_0cb3,
        0x4ed8_aa4a,
        0x5b9c_ca4f,
        0x682e_6ff3,
        0x748f_82ee,
        0x78a5_636f,
        0x84c8_7814,
        0x8cc7_0208,
        0x90be_fffa,
        0xa450_6ceb,
        0xbef9_a3f7,
        0xc671_78f2,
    ];

    // Padding: 0x80, then zeros to 56 mod 64, then the 64-bit big-endian bit
    // length.
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut message = bytes.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in message.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let base = i * 4;
            *word = u32::from_be_bytes([
                chunk[base],
                chunk[base + 1],
                chunk[base + 2],
                chunk[base + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut hex = String::with_capacity(64);
    for word in h {
        hex.push_str(&format!("{word:08x}"));
    }
    hex
}

/// Bounded task content: an inline value, or a reference to one.
///
/// Attachments and results are immutable: an artifact reference carries its own
/// digest, media type, size, classification, and provenance, and content loading
/// happens through bounded adapters, never inside a task transition. A resolved
/// credential never appears in either variant
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskContent {
    /// A bounded inline value, at most [`AGENT_TASK_INLINE_CONTENT_MAX_BYTES`]
    /// serialized bytes.
    Inline(Value),
    /// An immutable reference to application-owned content.
    Artifact(Box<ArtifactRef>),
}

impl AgentTaskContent {
    /// Creates inline content, rejecting a value that exceeds the inline bound.
    pub fn inline(value: Value) -> AgentTaskResult<Self> {
        let content = Self::Inline(value);
        content.validate()?;
        Ok(content)
    }

    /// Creates content held behind an artifact reference.
    #[must_use]
    pub fn artifact(artifact: ArtifactRef) -> Self {
        Self::Artifact(Box::new(artifact))
    }

    /// The inline value, when the content is inline.
    #[must_use]
    pub const fn inline_value(&self) -> Option<&Value> {
        match self {
            Self::Inline(value) => Some(value),
            Self::Artifact(_) => None,
        }
    }

    /// The artifact reference, when the content is held behind one.
    #[must_use]
    pub fn artifact_ref(&self) -> Option<&ArtifactRef> {
        match self {
            Self::Inline(_) => None,
            Self::Artifact(artifact) => Some(artifact),
        }
    }

    /// Stable fingerprint of the content.
    ///
    /// Inline content is fingerprinted over its canonical encoding; artifact
    /// content is fingerprinted over its immutable reference, because the task
    /// never loads the bytes.
    #[must_use]
    pub fn digest(&self) -> AgentContentDigest {
        match self {
            Self::Inline(value) => AgentContentDigest::of_json(value),
            Self::Artifact(artifact) => {
                let identity = format!(
                    "{}|{}|{}",
                    artifact.artifact_id,
                    artifact.uri,
                    artifact.checksum.as_deref().unwrap_or_default()
                );
                AgentContentDigest::of_bytes(identity.as_bytes())
            }
        }
    }

    /// Serialized size of the content, in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects inline content that exceeds the inline bound.
    pub fn validate(&self) -> AgentTaskResult<()> {
        let Self::Inline(value) = self else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(value)
            .map_err(|error| AgentTaskError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_TASK_INLINE_CONTENT_MAX_BYTES {
            return Err(AgentTaskError::ContentTooLarge {
                bytes,
                maximum: AGENT_TASK_INLINE_CONTENT_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// One deterministic check a proposed result must pass
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every variant is a pure function of the proposed value. Nothing here can call
/// a model, load an artifact, or reach a service: a model-assisted or external
/// evaluator is a durable effect that returns evidence to the task, never I/O
/// inside the task's transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskResultCheck {
    /// The value at the JSON pointer must be present and not null.
    Required {
        /// JSON pointer into the proposed result.
        pointer: String,
    },
    /// The value at the JSON pointer must be absent or null.
    Forbidden {
        /// JSON pointer into the proposed result.
        pointer: String,
    },
    /// The value at the JSON pointer must be a non-empty string.
    NonEmptyString {
        /// JSON pointer into the proposed result.
        pointer: String,
    },
    /// The value at the JSON pointer must be an integer within an inclusive
    /// range.
    IntegerRange {
        /// JSON pointer into the proposed result.
        pointer: String,
        /// Inclusive lower bound, when bounded.
        minimum: Option<i64>,
        /// Inclusive upper bound, when bounded.
        maximum: Option<i64>,
    },
    /// The string at the JSON pointer must be one of a bounded set.
    OneOf {
        /// JSON pointer into the proposed result.
        pointer: String,
        /// Permitted values.
        values: BTreeSet<String>,
    },
    /// The proposal must carry at least one evidence artifact.
    EvidenceRequired,
}

impl AgentTaskResultCheck {
    /// Rejects a check whose content cannot be bounded.
    ///
    /// A rule travels inside the definition, and the definition is part of the
    /// materialized record, so an unbounded pointer or value set would let a
    /// definition grow the record without limit.
    fn validate(&self) -> AgentTaskResult<()> {
        let pointer = match self {
            Self::Required { pointer }
            | Self::Forbidden { pointer }
            | Self::NonEmptyString { pointer }
            | Self::IntegerRange { pointer, .. }
            | Self::OneOf { pointer, .. } => Some(pointer),
            Self::EvidenceRequired => None,
        };
        if let Some(pointer) = pointer {
            if pointer.len() > AGENT_TASK_RULE_POINTER_MAX_LENGTH {
                return Err(AgentTaskError::InvalidDefinition {
                    detail: format!(
                        "a result rule's JSON pointer is {} bytes, which exceeds the \
                         {AGENT_TASK_RULE_POINTER_MAX_LENGTH} byte limit",
                        pointer.len()
                    ),
                });
            }
        }
        if let Self::OneOf { values, .. } = self {
            if values.len() > AGENT_TASK_RULE_ONE_OF_MAX_VALUES {
                return Err(AgentTaskError::InvalidDefinition {
                    detail: format!(
                        "a one-of result rule may permit at most \
                         {AGENT_TASK_RULE_ONE_OF_MAX_VALUES} values"
                    ),
                });
            }
            if let Some(value) = values
                .iter()
                .find(|value| value.len() > AGENT_TASK_RULE_VALUE_MAX_LENGTH)
            {
                return Err(AgentTaskError::InvalidDefinition {
                    detail: format!(
                        "a permitted one-of value is {} bytes, which exceeds the \
                         {AGENT_TASK_RULE_VALUE_MAX_LENGTH} byte limit",
                        value.len()
                    ),
                });
            }
        }
        Ok(())
    }

    /// Stable kebab-case label of the check, used as the rejection reason code.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Required { .. } => "required-field-missing",
            Self::Forbidden { .. } => "forbidden-field-present",
            Self::NonEmptyString { .. } => "empty-string-field",
            Self::IntegerRange { .. } => "integer-out-of-range",
            Self::OneOf { .. } => "value-not-permitted",
            Self::EvidenceRequired => "evidence-missing",
        }
    }
}

/// One versioned, deterministic result rule.
///
/// The rule's identity and version are persisted on every rejection, so a
/// rejection can always be traced to the exact rule revision that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskResultRule {
    /// Stable rule identity.
    pub rule_id: AgentTaskRuleId,
    /// Monotonic rule revision.
    pub version: AgentRevisionNumber,
    /// The deterministic check.
    pub check: AgentTaskResultCheck,
}

impl AgentTaskResultRule {
    /// Creates a result rule at its initial revision.
    #[must_use]
    pub const fn new(rule_id: AgentTaskRuleId, check: AgentTaskResultCheck) -> Self {
        Self {
            rule_id,
            version: AgentRevisionNumber::INITIAL,
            check,
        }
    }

    /// Sets the rule revision.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// Evaluates the rule against a proposed result.
    ///
    /// Artifact-backed content carries no inspectable value, so a rule that
    /// needs one fails closed: the task cannot claim a result satisfied a rule
    /// it was never able to evaluate.
    fn evaluate(&self, content: &AgentTaskContent, evidence: &[ArtifactRef]) -> Option<String> {
        if matches!(self.check, AgentTaskResultCheck::EvidenceRequired) {
            return evidence
                .is_empty()
                .then(|| "the proposal carries no evidence artifact".to_string());
        }

        let Some(value) = content.inline_value() else {
            return Some(
                "the rule needs an inspectable result, and the proposal is held behind an artifact \
                 reference"
                    .to_string(),
            );
        };

        match &self.check {
            AgentTaskResultCheck::EvidenceRequired => None,
            AgentTaskResultCheck::Required { pointer } => match value.pointer(pointer) {
                None | Some(Value::Null) => Some(format!("{pointer} is required")),
                Some(_) => None,
            },
            AgentTaskResultCheck::Forbidden { pointer } => match value.pointer(pointer) {
                None | Some(Value::Null) => None,
                Some(_) => Some(format!("{pointer} is forbidden")),
            },
            AgentTaskResultCheck::NonEmptyString { pointer } => match value.pointer(pointer) {
                Some(Value::String(text)) if !text.trim().is_empty() => None,
                _ => Some(format!("{pointer} must be a non-empty string")),
            },
            AgentTaskResultCheck::IntegerRange {
                pointer,
                minimum,
                maximum,
            } => match value.pointer(pointer).and_then(Value::as_i64) {
                None => Some(format!("{pointer} must be an integer")),
                Some(number) => {
                    if minimum.is_some_and(|minimum| number < minimum) {
                        Some(format!(
                            "{pointer} is {number}, below the minimum {}",
                            minimum.unwrap_or_default()
                        ))
                    } else if maximum.is_some_and(|maximum| number > maximum) {
                        Some(format!(
                            "{pointer} is {number}, above the maximum {}",
                            maximum.unwrap_or_default()
                        ))
                    } else {
                        None
                    }
                }
            },
            AgentTaskResultCheck::OneOf { pointer, values } => {
                match value.pointer(pointer).and_then(Value::as_str) {
                    Some(text) if values.contains(text) => None,
                    _ => Some(format!("{pointer} is not one of the permitted values")),
                }
            }
        }
    }
}

/// What happens to a task when one of its dependencies does not complete
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDependencyFailurePolicy {
    /// The default: a failed or cancelled dependency cancels its dependents.
    #[default]
    CancelDependents,
    /// The dependent proceeds, and the dependency's outcome becomes evidence
    /// its run must account for. It must be chosen explicitly.
    ContinueWithEvidence,
}

impl AgentDependencyFailurePolicy {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::CancelDependents => "cancel-dependents",
            Self::ContinueWithEvidence => "continue-with-evidence",
        }
    }
}

impl Display for AgentDependencyFailurePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Who may complete a task
/// ([specification 9.1](../../../docs/plans/rakka-agent/spec.md)).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskOwnership {
    /// An agent run, created by the task's assignment decision.
    #[default]
    Agent,
    /// An authenticated human or service, through the same typed validation
    /// path. The task is deliberately never assigned to an agent
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)); it
    /// waits `WaitingForInput` for an authenticated
    /// [`AgentHumanResultSubmission`].
    ///
    /// A human-owned task is a work product with a typed result, never a
    /// substitute for an effect-bound checkpoint: a decision that approves,
    /// authorizes, or reconciles a *specific effect* must ride
    /// [`crate::checkpoints::AgentCheckpoint`], bound to the exact effect
    /// intent — a human task's result binds to nothing it could gate.
    Human,
}

impl AgentTaskOwnership {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Human => "human",
        }
    }
}

impl Display for AgentTaskOwnership {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The bounds a task definition places on its own durable state
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Each field may only tighten the crate-level maximum it corresponds to; a
/// definition cannot raise a bound that keeps durable state finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskLimits {
    /// How many result rejections the task tolerates before it fails.
    pub max_result_rejections: u32,
    /// How many assignment generations the task may consume, counting the
    /// first: a run that refuses its assignment costs one.
    pub max_assignments: u32,
    /// How many dependencies the task may declare.
    pub max_dependencies: usize,
    /// How many handoffs the task may record over its lifetime
    /// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md):
    /// handoffs/reassignments are bounded; this is also what bounds an
    /// A→B→A oscillation deterministically). Definitions persisted before
    /// this field load with the default bound.
    #[serde(default = "default_max_handoffs")]
    pub max_handoffs: u32,
    /// How many team board claims the task may record over its lifetime
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)): the
    /// deterministic bound on claim/refuse cycles, the handoff bound's
    /// precedent. Definitions persisted before this field load with the
    /// default bound.
    #[serde(default = "default_max_team_claims")]
    pub max_team_claims: u32,
    /// How long, in milliseconds, a board-governed task may wait unclaimed
    /// and unassigned before the settle pass expires it
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)): the
    /// bounded replacement for the assignee fail-fast a team creation
    /// forgoes — a team id that never produces a claim surfaces as a
    /// cancelled task instead of parking silently, escrow locked, forever.
    /// `None` waits unbounded, explicitly. Definitions persisted before this
    /// field load with the default horizon.
    #[serde(default = "default_max_unclaimed_millis")]
    pub max_unclaimed_millis: Option<u64>,
}

/// The default handoff bound of [`AgentTaskLimits`].
const fn default_max_handoffs() -> u32 {
    4
}

/// The default team-claim bound of [`AgentTaskLimits`].
const fn default_max_team_claims() -> u32 {
    4
}

/// The default unclaimed-wait horizon of [`AgentTaskLimits`].
const fn default_max_unclaimed_millis() -> Option<u64> {
    Some(AGENT_TASK_DEFAULT_MAX_UNCLAIMED_MILLIS)
}

impl AgentTaskLimits {
    /// The default bounds: three rejections, three assignments, four
    /// handoffs, and the crate-level dependency maximum.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_result_rejections: 3,
            max_assignments: 3,
            max_dependencies: AGENT_TASK_MAX_DEPENDENCIES,
            max_handoffs: default_max_handoffs(),
            max_team_claims: default_max_team_claims(),
            max_unclaimed_millis: default_max_unclaimed_millis(),
        }
    }

    /// Sets how many result rejections the task tolerates.
    #[must_use]
    pub const fn with_max_result_rejections(mut self, maximum: u32) -> Self {
        self.max_result_rejections = maximum;
        self
    }

    /// Sets how many assignment generations the task may consume.
    #[must_use]
    pub const fn with_max_assignments(mut self, maximum: u32) -> Self {
        self.max_assignments = maximum;
        self
    }

    /// Sets how many dependencies the task may declare.
    #[must_use]
    pub const fn with_max_dependencies(mut self, maximum: usize) -> Self {
        self.max_dependencies = maximum;
        self
    }

    /// Sets how many handoffs the task may record over its lifetime.
    #[must_use]
    pub const fn with_max_handoffs(mut self, maximum: u32) -> Self {
        self.max_handoffs = maximum;
        self
    }

    /// Sets how long the task may wait unclaimed on a team board, `None` for
    /// an explicitly unbounded wait.
    #[must_use]
    pub const fn with_max_unclaimed_millis(mut self, horizon: Option<u64>) -> Self {
        self.max_unclaimed_millis = horizon;
        self
    }

    fn validate(&self) -> AgentTaskResult<()> {
        if self.max_result_rejections == 0 {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task must tolerate at least one result rejection".to_string(),
            });
        }
        if self.max_assignments == 0 {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task must permit at least one assignment".to_string(),
            });
        }
        if self.max_dependencies > AGENT_TASK_MAX_DEPENDENCIES {
            return Err(AgentTaskError::InvalidDefinition {
                detail: format!(
                    "a task may not raise the dependency bound above {AGENT_TASK_MAX_DEPENDENCIES}"
                ),
            });
        }
        if self.max_unclaimed_millis == Some(0) {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task's unclaimed horizon must be positive when set".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for AgentTaskLimits {
    fn default() -> Self {
        Self::new()
    }
}

/// One versioned typed task definition
/// ([specification 9.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The record is deliberately not generic: durable state and the A2A projection
/// carry stable versioned schema references and bounded serialized values, never
/// a Rust type. [`TypedTask`] is where generics live, as compile-time ergonomics
/// over this same record.
///
/// The fields are public so a definition can be assembled directly, so
/// construction alone cannot guarantee the bounded invariants.
/// [`AgentTaskDefinition::validate`] therefore runs inside
/// [`AgentTaskDefinition::new`], on deserialization — an out-of-bounds definition
/// can neither cross the wire nor load from a durable record — and again at the
/// entity's creation path before anything is persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentTaskDefinition {
    schema_version: StateSchemaVersion,
    /// Stable definition identity.
    pub definition_id: AgentTaskDefinitionId,
    /// Monotonic definition revision. A result proposed under a different
    /// revision fails closed.
    pub version: AgentRevisionNumber,
    /// Bounded, outcome-oriented description.
    pub description: String,
    /// Schema the task input is expressed in.
    pub input_schema: AgentSchemaRef,
    /// Schema a proposed result must be expressed in.
    pub result_schema: AgentSchemaRef,
    /// The deterministic rules every proposed result must pass.
    pub result_rules: Vec<AgentTaskResultRule>,
    /// Bounds on the task's own durable state.
    pub limits: AgentTaskLimits,
    /// Per-task budget ceilings, which a run allocation may only narrow.
    ///
    /// This is the escrow the task holds: every run it assigns is debited from
    /// it, and a run's settlement and return are credited back to it
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    pub budgets: AgentBudgetCeilings,
    /// What each run is escrowed when the task assigns it, or `None` to escrow
    /// everything the task can still afford.
    ///
    /// Declaring a portion is what makes a top-up meaningful: a task that hands
    /// its whole escrow to one run has nothing left to grant when that run
    /// exhausts, so the run can only stop. Reserving the rest lets the task
    /// answer a top-up request under the same ceilings, which is exactly the
    /// parent-local allocation decision
    /// [specification 9.7](../../../docs/plans/rakka-agent/spec.md) describes.
    pub run_allocation: Option<AgentBudgetAllocation>,
    /// The definition's own delegation ceilings, which every delegation
    /// authority a task of this definition receives is min-narrowed to
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): parent
    /// *and definition* ceilings enforce at allocation and admission time).
    ///
    /// This is the one cap a forged or inflated provenance cannot escape: a
    /// peer minting a "root child" under this definition still delegates only
    /// what the definition permits. `None` bounds nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<AgentGoalDelegationBudget>,
    /// What happens to this task when a dependency does not complete.
    pub dependency_policy: AgentDependencyFailurePolicy,
    /// Who may complete the task.
    pub ownership: AgentTaskOwnership,
    /// The operating behavior the task's runs execute under
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a property of the work, not of the agent: the same agent may serve
    /// an interactive task with a human in the loop and an unattended one that
    /// must be admitted first. The agent's envelope declares which classes it
    /// may be given, and its admission decision declares which it may currently
    /// run.
    pub operation_class: AgentOperationClass,
    /// Skills an assignee agent must declare. An empty set requires none.
    pub required_skills: BTreeSet<AgentCapabilityId>,
    /// Application-owned retention, audit, guardrail, and escalation policies.
    pub policies: AgentPolicyRefs,
}

impl AgentTaskDefinition {
    /// Creates a task definition at its initial revision, rejecting one that
    /// cannot be bounded.
    pub fn new(
        definition_id: AgentTaskDefinitionId,
        description: impl Into<String>,
        input_schema: AgentSchemaRef,
        result_schema: AgentSchemaRef,
    ) -> AgentTaskResult<Self> {
        let definition = Self {
            schema_version: CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION,
            definition_id,
            version: AgentRevisionNumber::INITIAL,
            description: description.into(),
            input_schema,
            result_schema,
            result_rules: Vec::new(),
            limits: AgentTaskLimits::new(),
            budgets: AgentBudgetCeilings::unbounded(),
            run_allocation: None,
            delegation: None,
            dependency_policy: AgentDependencyFailurePolicy::default(),
            ownership: AgentTaskOwnership::default(),
            // Attended is the safe default: an unattended class is the one that
            // must be admitted, so defaulting to it would make the fail-closed
            // check something a caller opts *out* of.
            operation_class: AgentOperationClass::Interactive,
            required_skills: BTreeSet::new(),
            policies: AgentPolicyRefs::default(),
        };
        definition.validate()?;
        Ok(definition)
    }

    /// Adds a deterministic result rule.
    #[must_use]
    pub fn with_result_rule(mut self, rule: AgentTaskResultRule) -> Self {
        self.result_rules.push(rule);
        self
    }

    /// Sets the task's durable-state bounds.
    #[must_use]
    pub const fn with_limits(mut self, limits: AgentTaskLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Sets the per-task budget ceilings.
    #[must_use]
    pub const fn with_budgets(mut self, budgets: AgentBudgetCeilings) -> Self {
        self.budgets = budgets;
        self
    }

    /// Sets what each run is escrowed when the task assigns it.
    #[must_use]
    pub const fn with_run_allocation(mut self, allocation: AgentBudgetAllocation) -> Self {
        self.run_allocation = Some(allocation);
        self
    }

    /// Sets the definition's own delegation ceilings.
    #[must_use]
    pub const fn with_delegation(mut self, delegation: AgentGoalDelegationBudget) -> Self {
        self.delegation = Some(delegation);
        self
    }

    /// What one run should be escrowed, before the task's own headroom narrows
    /// it.
    ///
    /// An undeclared per-run allocation asks for everything: the task then
    /// escrows whatever it can still afford, which is the right default for the
    /// one-run-at-a-time task of this phase and wastes nothing.
    #[must_use]
    pub fn run_allocation_request(&self) -> AgentBudgetAllocation {
        self.run_allocation
            .unwrap_or_else(AgentBudgetAllocation::unbounded)
    }

    /// Sets the failed-dependency policy.
    #[must_use]
    pub const fn with_dependency_policy(mut self, policy: AgentDependencyFailurePolicy) -> Self {
        self.dependency_policy = policy;
        self
    }

    /// Sets who may complete the task.
    #[must_use]
    pub const fn with_ownership(mut self, ownership: AgentTaskOwnership) -> Self {
        self.ownership = ownership;
        self
    }

    /// Sets the operating behavior the task's runs execute under.
    #[must_use]
    pub const fn with_operation_class(mut self, class: AgentOperationClass) -> Self {
        self.operation_class = class;
        self
    }

    /// Sets the definition revision.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// Requires an assignee agent to declare one skill.
    #[must_use]
    pub fn with_required_skill(mut self, skill: AgentCapabilityId) -> Self {
        self.required_skills.insert(skill);
        self
    }

    /// Rejects a definition that cannot be bounded.
    pub fn validate(&self) -> AgentTaskResult<()> {
        if self.description.is_empty() {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task definition must carry an outcome-oriented description".to_string(),
            });
        }
        if self.description.len() > AGENT_TASK_DESCRIPTION_MAX_LENGTH {
            return Err(AgentTaskError::InvalidDefinition {
                detail: format!(
                    "the description is {} bytes, which exceeds the {AGENT_TASK_DESCRIPTION_MAX_LENGTH} byte limit",
                    self.description.len()
                ),
            });
        }
        if self.result_rules.len() > AGENT_TASK_MAX_RESULT_RULES {
            return Err(AgentTaskError::InvalidDefinition {
                detail: format!(
                    "a task definition may declare at most {AGENT_TASK_MAX_RESULT_RULES} result rules"
                ),
            });
        }
        for rule in &self.result_rules {
            rule.check.validate()?;
        }
        self.limits.validate()
    }

    /// Whether the task is completed by an agent run rather than a human.
    #[must_use]
    pub const fn is_agent_owned(&self) -> bool {
        matches!(self.ownership, AgentTaskOwnership::Agent)
    }

    /// Validates a claimed result against the definition's schema reference,
    /// revision, and every deterministic rule, returning the first rule that
    /// refused it.
    ///
    /// It is a pure function: the same claim always produces the same
    /// decision, on any node, after any restart — which is what lets a run's
    /// proposal and a human's submission share it verbatim
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md): the
    /// same typed validation path).
    fn validate_result(&self, claim: &AgentResultClaim<'_>) -> Result<(), AgentTaskRejectionCause> {
        if claim.definition_id != &self.definition_id || claim.definition_version != self.version {
            return Err(AgentTaskRejectionCause::definition_mismatch(format!(
                "the result was proposed under task definition {}@{} but the task runs {}@{}",
                claim.definition_id, claim.definition_version, self.definition_id, self.version
            )));
        }
        if claim.result_schema != &self.result_schema {
            return Err(AgentTaskRejectionCause::schema_mismatch(format!(
                "the result was proposed under schema {} but the task requires {}",
                claim.result_schema, self.result_schema
            )));
        }
        if let Err(error) = claim.content.validate() {
            return Err(AgentTaskRejectionCause::malformed(error.to_string()));
        }
        if claim.evidence.len() > AGENT_TASK_MAX_EVIDENCE_ARTIFACTS {
            return Err(AgentTaskRejectionCause::malformed(format!(
                "a proposal may carry at most {AGENT_TASK_MAX_EVIDENCE_ARTIFACTS} evidence artifacts"
            )));
        }

        for rule in &self.result_rules {
            if let Some(detail) = rule.evaluate(claim.content, claim.evidence) {
                return Err(AgentTaskRejectionCause::rule(rule, detail));
            }
        }
        Ok(())
    }
}

/// The origin-neutral borrow of one claimed result, shared by the run's
/// proposal and the human's submission so both travel the identical
/// deterministic validation.
struct AgentResultClaim<'a> {
    definition_id: &'a AgentTaskDefinitionId,
    definition_version: AgentRevisionNumber,
    result_schema: &'a AgentSchemaRef,
    content: &'a AgentTaskContent,
    evidence: &'a [ArtifactRef],
}

impl<'a> AgentResultClaim<'a> {
    fn from_proposal(proposal: &'a AgentTaskResultProposal) -> Self {
        Self {
            definition_id: &proposal.definition_id,
            definition_version: proposal.definition_version,
            result_schema: &proposal.result_schema,
            content: &proposal.content,
            evidence: &proposal.evidence,
        }
    }

    fn from_submission(submission: &'a AgentHumanResultSubmission) -> Self {
        Self {
            definition_id: &submission.definition_id,
            definition_version: submission.definition_version,
            result_schema: &submission.result_schema,
            content: &submission.content,
            evidence: &submission.evidence,
        }
    }
}

impl<'de> Deserialize<'de> for AgentTaskDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            schema_version: StateSchemaVersion,
            definition_id: AgentTaskDefinitionId,
            version: AgentRevisionNumber,
            description: String,
            input_schema: AgentSchemaRef,
            result_schema: AgentSchemaRef,
            result_rules: Vec<AgentTaskResultRule>,
            limits: AgentTaskLimits,
            budgets: AgentBudgetCeilings,
            run_allocation: Option<AgentBudgetAllocation>,
            #[serde(default)]
            delegation: Option<AgentGoalDelegationBudget>,
            dependency_policy: AgentDependencyFailurePolicy,
            ownership: AgentTaskOwnership,
            operation_class: AgentOperationClass,
            required_skills: BTreeSet<AgentCapabilityId>,
            policies: AgentPolicyRefs,
        }

        let wire = Wire::deserialize(deserializer)?;
        let definition = Self {
            schema_version: wire.schema_version,
            definition_id: wire.definition_id,
            version: wire.version,
            description: wire.description,
            input_schema: wire.input_schema,
            result_schema: wire.result_schema,
            result_rules: wire.result_rules,
            limits: wire.limits,
            budgets: wire.budgets,
            run_allocation: wire.run_allocation,
            delegation: wire.delegation,
            dependency_policy: wire.dependency_policy,
            ownership: wire.ownership,
            operation_class: wire.operation_class,
            required_skills: wire.required_skills,
            policies: wire.policies,
        };
        definition.validate().map_err(serde::de::Error::custom)?;
        Ok(definition)
    }
}

impl VersionedAgentRecord for AgentTaskDefinition {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TaskDefinition;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A task definition bound to the Rust types of its input and result.
///
/// This is the whole of the "generics as compile-time ergonomics" clause of
/// [specification 9.1](../../../docs/plans/rakka-agent/spec.md): the handle
/// encodes and decodes application types against the definition's schema
/// references, and everything it produces is the same bounded, versioned,
/// non-generic record the durable state already holds. A result decoded under a
/// mismatched definition or schema revision fails closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedTask<I, R> {
    definition: AgentTaskDefinition,
    marker: std::marker::PhantomData<fn(I) -> R>,
}

impl<I, R> TypedTask<I, R>
where
    I: Serialize,
    R: Serialize + serde::de::DeserializeOwned,
{
    /// Binds Rust input and result types to a task definition.
    #[must_use]
    pub const fn new(definition: AgentTaskDefinition) -> Self {
        Self {
            definition,
            marker: std::marker::PhantomData,
        }
    }

    /// The bounded, versioned definition this handle wraps.
    #[must_use]
    pub const fn definition(&self) -> &AgentTaskDefinition {
        &self.definition
    }

    /// Encodes a typed input as bounded task content.
    pub fn input(&self, input: &I) -> AgentTaskResult<AgentTaskContent> {
        let value = serde_json::to_value(input).map_err(|error| AgentTaskError::Encoding {
            message: error.to_string(),
        })?;
        AgentTaskContent::inline(value)
    }

    /// Encodes a typed result as bounded task content.
    pub fn result(&self, result: &R) -> AgentTaskResult<AgentTaskContent> {
        let value = serde_json::to_value(result).map_err(|error| AgentTaskError::Encoding {
            message: error.to_string(),
        })?;
        AgentTaskContent::inline(value)
    }

    /// Decodes the task's accepted result, failing closed when it was accepted
    /// under a different definition or schema revision than this handle's.
    pub fn decode_accepted(&self, accepted: &AgentAcceptedResult) -> AgentTaskResult<R> {
        if accepted.definition_id != self.definition.definition_id
            || accepted.definition_version != self.definition.version
        {
            return Err(AgentTaskError::DefinitionMismatch {
                expected: format!(
                    "{}@{}",
                    self.definition.definition_id, self.definition.version
                ),
                actual: format!("{}@{}", accepted.definition_id, accepted.definition_version),
            });
        }
        if accepted.result_schema != self.definition.result_schema {
            return Err(AgentTaskError::SchemaMismatch {
                expected: self.definition.result_schema.to_string(),
                actual: accepted.result_schema.to_string(),
            });
        }
        let Some(value) = accepted.content.inline_value() else {
            return Err(AgentTaskError::ResultBehindArtifact);
        };
        serde_json::from_value(value.clone()).map_err(|error| AgentTaskError::Decoding {
            message: error.to_string(),
        })
    }
}

/// Public lifecycle of one typed task
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The status is independent of whether any actor or run is resident: a task may
/// be `Blocked`, `Assigned`, or `WaitingForInput` with no live execution
/// resource anywhere in the cluster
/// ([specification 6.11](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskStatus {
    /// Created, eligible, and not yet assigned.
    Created,
    /// Waiting on a dependency.
    Blocked,
    /// Assigned to an agent; its run has not yet durably accepted.
    Assigned,
    /// The assigned run has durably accepted and owns the work.
    InProgress,
    /// Deliberately unassigned to an agent, waiting for an authenticated human
    /// or service ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    WaitingForInput,
    /// Terminal: a proposed result passed every deterministic rule.
    Completed,
    /// Terminal: the task cannot produce an accepted result.
    Failed,
    /// Terminal: the task was cancelled.
    Cancelled,
}

impl AgentTaskStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Blocked => "blocked",
            Self::Assigned => "assigned",
            Self::InProgress => "in-progress",
            Self::WaitingForInput => "waiting-for-input",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the status is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Whether the task is waiting to be assigned to an agent.
    #[must_use]
    pub const fn is_assignable(self) -> bool {
        matches!(self, Self::Created)
    }
}

impl Display for AgentTaskStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Why a task reached a terminal status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskTerminalReason {
    /// A proposed result passed every deterministic rule.
    ResultAccepted,
    /// Every tolerated result rejection was consumed without an accepted result.
    ResultRejectionsExhausted {
        /// How many rejections the task recorded.
        rejections: u32,
    },
    /// Every tolerated assignment generation was consumed.
    AssignmentsExhausted {
        /// How many assignment generations the task consumed.
        assignments: u32,
    },
    /// A dependency did not complete, and the task's policy cancels dependents.
    DependencyNotSatisfied {
        /// The dependency that did not complete.
        dependency: AgentTaskId,
        /// Its outcome.
        outcome: AgentTaskDependencyOutcome,
    },
    /// The task was cancelled by an authorized command.
    CancellationRequested {
        /// Bounded, stable reason.
        reason: String,
    },
    /// The goal this task coordinates exhausted its budget under a
    /// `Terminate` exhaustion policy
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    GoalBudgetExhausted {
        /// The ceiling that was reached.
        exhaustion: AgentBudgetExhaustion,
    },
    /// The goal this task coordinates tripped a stagnation threshold under a
    /// `Terminate` stagnation action
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    GoalStagnant {
        /// The condition that tripped.
        trigger: crate::evaluation::AgentStagnationTrigger,
        /// The streak length at the trip.
        epochs: u32,
    },
}

impl AgentTaskTerminalReason {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ResultAccepted => "result-accepted",
            Self::ResultRejectionsExhausted { .. } => "result-rejections-exhausted",
            Self::AssignmentsExhausted { .. } => "assignments-exhausted",
            Self::DependencyNotSatisfied { .. } => "dependency-not-satisfied",
            Self::CancellationRequested { .. } => "cancellation-requested",
            Self::GoalBudgetExhausted { .. } => "goal-budget-exhausted",
            Self::GoalStagnant { .. } => "goal-stagnant",
        }
    }

    /// The task status this reason terminates the task in.
    ///
    /// A dependency that did not complete *cancels* its dependents rather than
    /// failing them: the dependent never got to try
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn status(&self) -> AgentTaskStatus {
        match self {
            Self::ResultAccepted => AgentTaskStatus::Completed,
            Self::ResultRejectionsExhausted { .. }
            | Self::AssignmentsExhausted { .. }
            | Self::GoalBudgetExhausted { .. }
            | Self::GoalStagnant { .. } => AgentTaskStatus::Failed,
            Self::DependencyNotSatisfied { .. } | Self::CancellationRequested { .. } => {
                AgentTaskStatus::Cancelled
            }
        }
    }
}

/// How one dependency resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskDependencyOutcome {
    /// The dependency produced an accepted result.
    Completed,
    /// The dependency failed.
    Failed,
    /// The dependency was cancelled.
    Cancelled,
}

impl AgentTaskDependencyOutcome {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the outcome satisfies a dependency rule.
    #[must_use]
    pub const fn is_satisfied(self) -> bool {
        matches!(self, Self::Completed)
    }

    /// The outcome a terminal task status reports to its dependents, `None`
    /// for a nonterminal status.
    ///
    /// Mapping from *status* — not from the terminal reason — keeps
    /// transitive propagation exact: a mid-chain dependent cancelled by its
    /// own policy reports `Cancelled` onward, and each downstream edge
    /// applies its own declared [`AgentDependencyFailurePolicy`].
    #[must_use]
    pub const fn from_terminal_status(status: AgentTaskStatus) -> Option<Self> {
        match status {
            AgentTaskStatus::Completed => Some(Self::Completed),
            AgentTaskStatus::Failed => Some(Self::Failed),
            AgentTaskStatus::Cancelled => Some(Self::Cancelled),
            _ => None,
        }
    }
}

impl Display for AgentTaskDependencyOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A declared dependency of one task on another.
///
/// The declared ancestors are what keep the graph acyclic without a global read:
/// a task refuses a dependency whose ancestry already contains the task itself.
/// The chain is bounded by [`AGENT_TASK_MAX_DEPENDENCY_DEPTH`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskDependencyDeclaration {
    /// The task that must resolve first.
    pub dependency: AgentTaskId,
    /// The dependency's own declared ancestry, nearest first.
    pub ancestors: Vec<AgentTaskId>,
    /// What happens to the dependent when this dependency does not complete.
    pub policy: AgentDependencyFailurePolicy,
}

impl AgentTaskDependencyDeclaration {
    /// Declares a dependency under the default failed-dependency policy.
    #[must_use]
    pub fn new(dependency: AgentTaskId) -> Self {
        Self {
            dependency,
            ancestors: Vec::new(),
            policy: AgentDependencyFailurePolicy::default(),
        }
    }

    /// Records the dependency's declared ancestry.
    #[must_use]
    pub fn with_ancestors(mut self, ancestors: Vec<AgentTaskId>) -> Self {
        self.ancestors = ancestors;
        self
    }

    /// Sets the failed-dependency policy for this edge.
    #[must_use]
    pub const fn with_policy(mut self, policy: AgentDependencyFailurePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Rejects a declaration that would create a cycle or exceed the bounds.
    fn validate(&self, task: &AgentTaskId) -> AgentTaskResult<()> {
        if &self.dependency == task {
            return Err(AgentTaskError::DependencyCycle {
                dependency: self.dependency.clone(),
            });
        }
        if self.ancestors.contains(task) {
            return Err(AgentTaskError::DependencyCycle {
                dependency: self.dependency.clone(),
            });
        }
        if self.ancestors.len() > AGENT_TASK_MAX_DEPENDENCY_DEPTH {
            return Err(AgentTaskError::DependencyDepthExceeded {
                depth: self.ancestors.len(),
                maximum: AGENT_TASK_MAX_DEPENDENCY_DEPTH,
            });
        }
        Ok(())
    }
}

/// The bounded durable summary of one dependency edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskDependency {
    /// The task that must resolve first.
    pub dependency: AgentTaskId,
    /// What happens to this task when the dependency does not complete.
    pub policy: AgentDependencyFailurePolicy,
    /// How the dependency resolved, once it has.
    pub outcome: Option<AgentTaskDependencyOutcome>,
    /// The operation that declared the edge.
    pub declared_by: AgentOperationId,
    /// When the edge was declared.
    pub declared_at: AgentTimestampMillis,
    /// Whether the registration exchange toward the upstream has settled:
    /// the durable once-guard past the exchange journal's bounded window.
    /// Edges persisted before the dependents registry load unsettled, so an
    /// unresolved pre-registry edge registers itself on the next settle pass
    /// — and an already-terminal upstream answers the late registration with
    /// its outcome directly.
    #[serde(default, skip_serializing_if = "is_false")]
    pub registration_settled: bool,
}

impl AgentTaskDependency {
    /// Whether the edge permits the dependent to become eligible.
    ///
    /// A `ContinueWithEvidence` edge is satisfied by any resolution: the
    /// dependency's outcome becomes evidence the dependent's run must account
    /// for, rather than a block.
    #[must_use]
    pub fn is_satisfied(&self) -> bool {
        match self.outcome {
            None => false,
            Some(outcome) => {
                outcome.is_satisfied()
                    || matches!(
                        self.policy,
                        AgentDependencyFailurePolicy::ContinueWithEvidence
                    )
            }
        }
    }

    /// Whether the edge cancels its dependent.
    #[must_use]
    pub fn cancels_dependent(&self) -> bool {
        matches!(self.policy, AgentDependencyFailurePolicy::CancelDependents)
            && self.outcome.is_some_and(|outcome| !outcome.is_satisfied())
    }
}

/// One registered reverse dependency edge: a task that depends on this one
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The registry is what the upstream's terminal transition walks to owe each
/// dependent its [`AgentExchangeKind::DependencyOutcome`] notification. It is
/// bounded by [`AGENT_TASK_MAX_DEPENDENTS`] and populated only by the
/// dependent's own [`AgentExchangeKind::DependencyRegistration`] — the
/// upstream never guesses who depends on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskDependentRecord {
    /// The dependent task.
    pub dependent: AgentTaskId,
    /// The registration that recorded it.
    pub registered_by: AgentOperationId,
    /// When it was recorded.
    pub registered_at: AgentTimestampMillis,
    /// Whether the outcome notification toward this dependent has settled:
    /// the durable once-guard past the exchange journal's bounded window.
    #[serde(default, skip_serializing_if = "is_false")]
    pub outcome_settled: bool,
}

/// Whether the run an assignment created has durably accepted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAssignmentStatus {
    /// The run-creation exchange is owed or in flight; the run has not replied.
    Offered,
    /// The run durably accepted its assignment.
    Accepted,
}

impl AgentAssignmentStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Offered => "offered",
            Self::Accepted => "accepted",
        }
    }
}

impl Display for AgentAssignmentStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One task's durable, nonterminal cancellation request
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// Set by a direct cancel command, a parent run's delegation-cancel exchange,
/// or the settle pass observing a terminal goal decision in the cancel or
/// expiry family. Absorbing: the first request fixes the reason finalization
/// terminates under, and every later request answers from it. The marker —
/// never a terminal status — is what keeps the task nonterminal while its
/// assigned run and delegated children quiesce, so terminal `Cancelled` is
/// never projected while a started consequential effect's outcome is unknown.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskCancellation {
    /// The terminal reason finalization will record.
    pub reason: AgentTaskTerminalReason,
    /// When the request was durably recorded.
    pub requested_at: AgentTimestampMillis,
}

impl AgentTaskCancellation {
    /// The bounded human-readable detail the request carried, for the records
    /// propagation writes downstream.
    #[must_use]
    pub fn detail(&self) -> String {
        match &self.reason {
            AgentTaskTerminalReason::CancellationRequested { reason } => reason.clone(),
            other => other.code().to_string(),
        }
    }
}

/// The task's *current* assignment.
///
/// Only the current one is materialized. A superseded assignment leaves the
/// record and becomes a history entry
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskAssignment {
    /// The generation this assignment owns. It fences every earlier run.
    pub generation: AgentAssignmentGeneration,
    /// The assigned agent.
    pub agent: AgentId,
    /// The run created to serve this generation.
    pub run: AgentRunId,
    /// Whether the run has durably accepted.
    pub status: AgentAssignmentStatus,
    /// The agent definition revision the decision was made against.
    pub agent_definition_revision: AgentRevisionNumber,
    /// The agent settings revision the decision was made against.
    pub agent_settings_revision: AgentRevisionNumber,
    /// The run-creation operation this assignment owes or owed.
    pub operation_id: AgentOperationId,
    /// When the decision was recorded.
    pub assigned_at: AgentTimestampMillis,
}

/// The task's *latest* handoff: the bounded materialized provenance of one
/// same-task transfer ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Only the latest hop is materialized — the chain is history, exactly as
/// superseded assignments are ([specification 9.6](../../../docs/plans/rakka-agent/spec.md))
/// — and this record is load-bearing three ways:
///
/// - it is the deduplication **echo past the journal's bounded window**: a
///   replayed handoff command matching [`Self::handoff`] answers with the
///   recorded target rather than minting a second transfer;
/// - it is the **source address** every owed derivation reads — the current
///   assignment names the target after the transfer, so without this record
///   nothing could reach the source run again; and
/// - it is the goal view's **source-run join**, the one thing that keeps a
///   handed-off generation out of the earlier-generations gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskHandoff {
    /// The handoff identity, derived by the source run.
    pub handoff: crate::identity::AgentHandoffId,
    /// The source assignment, stashed whole at the transfer so a refused
    /// handoff restores it exactly — the source never stopped being its
    /// generation's accepted owner, and its escrow child was never touched.
    pub source_assignment: Box<AgentTaskAssignment>,
    /// The agent the transfer targets.
    pub target: AgentId,
    /// The assignment generation minted toward the target, once the decision
    /// ran.
    #[serde(default)]
    pub target_generation: Option<AgentAssignmentGeneration>,
    /// The bounded reason the source's model supplied.
    pub reason: String,
    /// The handoff policy revision that authorized the transfer.
    pub policy_revision: AgentRevisionNumber,
    /// Explicit context/artifact references projected to the target — never
    /// content, never memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<String>,
    /// The communal knowledge spaces the catalog explicitly delegates to the
    /// target ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)).
    /// The envelope derivation intersects the task's own grant with this
    /// statement, so a handoff can never widen communal access.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub knowledge_spaces: BTreeSet<crate::identity::KnowledgeSpaceId>,
    /// Where the transfer stands.
    pub status: AgentTaskHandoffStatus,
    /// Whether the handoff-result exchange to the source has settled: the
    /// durable once-guard past the journal's bounded deduplication window,
    /// the delegation cell's `cancel` precedent.
    #[serde(default)]
    pub result_settled: bool,
    /// When the transfer was recorded.
    pub recorded_at: AgentTimestampMillis,
    /// When the transfer settled, when it has.
    #[serde(default)]
    pub settled_at: Option<AgentTimestampMillis>,
}

impl AgentTaskHandoff {
    /// Whether the transfer has resolved, accepted or refused.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self.status, AgentTaskHandoffStatus::Initiated)
    }
}

/// Where one recorded handoff stands on the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskHandoffStatus {
    /// Recorded: the source assignment is stashed and the target's
    /// generation is offered or about to be.
    Initiated,
    /// The target's assignment was durably accepted: responsibility has
    /// transferred.
    Accepted,
    /// The transfer resolved without an accepted target; the source
    /// assignment was restored.
    Refused {
        /// Stable machine-readable refusal code.
        code: String,
    },
}

impl AgentTaskHandoffStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Initiated => "initiated",
            Self::Accepted => "accepted",
            Self::Refused { .. } => "refused",
        }
    }
}

/// The request an [`AgentTaskEntityCommand::RecordHandoff`] command carries
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Built by the A2A ingress from the collaboration metadata's handoff
/// cluster. Every field is a *claim* the transition re-validates against the
/// task's durable state — the source must be the current accepted
/// assignment, the target contract must match — so forged metadata fails
/// closed at the transition, never by trusting the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskHandoffRequest {
    /// The handoff identity the source run derived.
    pub handoff: crate::identity::AgentHandoffId,
    /// The agent the source run claims to be.
    pub source_agent: AgentId,
    /// The source run claiming the transfer.
    pub source_run: AgentRunId,
    /// The assignment generation the source claims to serve.
    pub source_generation: AgentAssignmentGeneration,
    /// The agent the transfer targets.
    pub target: AgentId,
    /// The task definition the resolved target serves — the contract half of
    /// specification 8.9's target-acceptance validation.
    pub target_task_definition: crate::definition::AgentTaskDefinitionId,
    /// The result schema the resolved target expects, when its catalog entry
    /// declares one.
    #[serde(default)]
    pub result_schema: Option<AgentSchemaRef>,
    /// The bounded reason the source's model supplied.
    pub reason: String,
    /// The handoff policy revision that authorized the transfer.
    pub policy_revision: AgentRevisionNumber,
    /// Explicit context/artifact references projected to the target.
    #[serde(default)]
    pub context: Vec<String>,
    /// The communal knowledge spaces the catalog explicitly delegates to the
    /// target.
    #[serde(default)]
    pub knowledge_spaces: BTreeSet<crate::identity::KnowledgeSpaceId>,
}

/// The task's *latest* team board claim: the bounded materialized provenance
/// of one board-driven assignment
/// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Only the latest claim is materialized — the chain is history, exactly as
/// superseded assignments and handoffs are — and this record is load-bearing
/// the same three ways [`AgentTaskHandoff`] is: it is the deduplication echo
/// past the journal's bounded window, the address the owed claim-result
/// derivation reads, and the durable claim fence's provenance. The board
/// never holds ownership: the assignment-generation fence stays the
/// one-normal-owner guarantee, and this record only mirrors the claim that
/// drove it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskTeamClaim {
    /// The claim identity the team's board decision derived.
    pub claim: crate::identity::AgentTeamClaimId,
    /// The team whose board drove it.
    pub team: crate::identity::AgentTeamScope,
    /// The claiming member.
    pub member: AgentId,
    /// The board entry's claim epoch at the decision — the value the task's
    /// claim fence records.
    pub epoch: u64,
    /// The assignment generation minted toward the claimant, once the
    /// decision ran.
    #[serde(default)]
    pub target_generation: Option<AgentAssignmentGeneration>,
    /// Where the claim stands.
    pub status: AgentTaskTeamClaimStatus,
    /// Whether the claim-result exchange to the team has settled: the
    /// durable once-guard past the journal's bounded deduplication window.
    #[serde(default)]
    pub result_settled: bool,
    /// When the claim was recorded.
    pub recorded_at: AgentTimestampMillis,
    /// When the claim settled, when it has.
    #[serde(default)]
    pub settled_at: Option<AgentTimestampMillis>,
}

impl AgentTaskTeamClaim {
    /// Whether the claim has resolved, accepted or refused.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self.status, AgentTaskTeamClaimStatus::Initiated)
    }
}

/// Where one recorded team claim stands on the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskTeamClaimStatus {
    /// Recorded: the claimant is the assignee and its generation is offered
    /// or about to be.
    Initiated,
    /// The claimant's assignment was durably accepted.
    Accepted,
    /// The claim resolved without an accepted assignment; the assignee was
    /// cleared back to the board-pending posture.
    Refused {
        /// Stable machine-readable refusal code.
        code: String,
    },
}

impl AgentTaskTeamClaimStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Initiated => "initiated",
            Self::Accepted => "accepted",
            Self::Refused { .. } => "refused",
        }
    }
}

/// Why an assignment decision refused an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAssignmentRefusalReason {
    /// The agent has no durable state: it was never instantiated.
    AgentNotInstantiated,
    /// The agent is suspended or terminated, so it may not dispatch.
    AgentNotActive,
    /// The agent's definition envelope does not declare this task definition.
    TaskDefinitionNotPermitted,
    /// The agent's definition envelope does not declare the unattended
    /// operation class the task runs under
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a distinct reason from [`Self::TaskDefinitionNotPermitted`]
    /// because the remedies differ: the task definition may be fully declared
    /// while the *class* of autonomy it asks for is not.
    OperationClassNotDeclared,
    /// The agent's definition envelope does not declare a skill the task
    /// requires.
    SkillNotDeclared,
    /// The agent has no autonomy admission decision that admits this work, or
    /// the one it has no longer admits it
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// This is the fail-closed default for an unattended class: an agent is
    /// unadmitted until something says otherwise, and a widening update makes it
    /// unadmitted again.
    NotAdmitted,
    /// The task cannot escrow a run from what its own ledger still holds
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    BudgetUnavailable,
    /// The run refused the assignment.
    RunRefusedAssignment,
}

impl AgentAssignmentRefusalReason {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::AgentNotInstantiated => "agent-not-instantiated",
            Self::AgentNotActive => "agent-not-active",
            Self::TaskDefinitionNotPermitted => "task-definition-not-permitted",
            Self::OperationClassNotDeclared => "operation-class-not-declared",
            Self::SkillNotDeclared => "skill-not-declared",
            Self::NotAdmitted => "agent-not-admitted",
            Self::BudgetUnavailable => "task-budget-unavailable",
            Self::RunRefusedAssignment => "run-refused-assignment",
        }
    }
}

impl Display for AgentAssignmentRefusalReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// The bounded record of the most recent refused assignment.
///
/// A refusal is not terminal: the task stays logically available and assignable,
/// so resuming a suspended agent or widening its envelope lets the next
/// assignment attempt succeed without re-creating the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAssignmentRefusal {
    /// The agent that was refused.
    pub agent: AgentId,
    /// Why it was refused.
    pub reason: AgentAssignmentRefusalReason,
    /// Bounded detail.
    pub detail: String,
    /// When the refusal was recorded.
    pub refused_at: AgentTimestampMillis,
}

/// The stable cause of one result rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskRejectionCause {
    /// Stable machine-readable reason code.
    pub reason: String,
    /// The rule that refused the result, when a rule did.
    pub rule_id: Option<AgentTaskRuleId>,
    /// The revision of that rule.
    pub rule_version: Option<AgentRevisionNumber>,
    /// Bounded detail.
    pub detail: String,
}

impl AgentTaskRejectionCause {
    fn rule(rule: &AgentTaskResultRule, detail: String) -> Self {
        Self {
            reason: rule.check.as_label().to_string(),
            rule_id: Some(rule.rule_id.clone()),
            rule_version: Some(rule.version),
            detail: bounded_detail(detail),
        }
    }

    fn definition_mismatch(detail: String) -> Self {
        Self::without_rule("definition-version-mismatch", detail)
    }

    fn schema_mismatch(detail: String) -> Self {
        Self::without_rule("result-schema-mismatch", detail)
    }

    fn malformed(detail: String) -> Self {
        Self::without_rule("malformed-result", detail)
    }

    fn without_rule(reason: &str, detail: String) -> Self {
        Self {
            reason: reason.to_string(),
            rule_id: None,
            rule_version: None,
            detail: bounded_detail(detail),
        }
    }
}

/// One durable rejection decision
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Only the most recent rejection stays materialized. Every rejection is also a
/// history entry, so the full sequence is readable through the bounded cursor
/// without the task's state growing with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskRejection {
    /// The proposal that was refused.
    pub proposal_id: AgentOperationId,
    /// The fingerprint of the refused content.
    pub digest: AgentContentDigest,
    /// Why it was refused.
    pub cause: AgentTaskRejectionCause,
    /// How many rejections the task had recorded once this one was persisted.
    pub rejection_count: u32,
    /// What caused the proposal.
    pub causation_id: AgentCausationId,
    /// When the decision was recorded.
    pub rejected_at: AgentTimestampMillis,
}

/// The task's accepted typed result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAcceptedResult {
    /// The proposal that produced it.
    pub proposal_id: AgentOperationId,
    /// The run that proposed it, when a run did. A human-owned task's
    /// accepted result carries a principal instead; every record persisted
    /// before human submissions existed carries the run.
    pub run: Option<AgentRunId>,
    /// The authenticated principal that submitted it, when a human or
    /// external service did ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    /// Records persisted before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// The task definition it was validated under.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition.
    pub definition_version: AgentRevisionNumber,
    /// The schema it is expressed in.
    pub result_schema: AgentSchemaRef,
    /// The bounded content, inline or behind an artifact reference.
    pub content: AgentTaskContent,
    /// Its fingerprint.
    pub digest: AgentContentDigest,
    /// Evidence artifacts the proposal carried.
    pub evidence: Vec<ArtifactRef>,
    /// When the task accepted it.
    pub accepted_at: AgentTimestampMillis,
}

fn bounded_detail(detail: impl Into<String>) -> String {
    let mut detail = detail.into();
    if detail.len() > AGENT_TASK_DETAIL_MAX_LENGTH {
        detail.truncate(
            (0..=AGENT_TASK_DETAIL_MAX_LENGTH)
                .rev()
                .find(|index| detail.is_char_boundary(*index))
                .unwrap_or(0),
        );
    }
    detail
}

/// What one history entry records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskHistoryKind {
    /// The task was created.
    Created,
    /// A dependency edge was declared.
    DependencyDeclared,
    /// A dependency resolved.
    DependencyResolved,
    /// A dependent task registered itself for outcome notification
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)); the
    /// detail carries the dependent's id.
    DependentRegistered,
    /// This task's registration toward an upstream it depends on was
    /// definitively refused; the detail carries the refusal code, and the
    /// edge stays resolvable only through the application relay.
    DependentRegistrationRefused,
    /// An assignment generation was decided.
    AssignmentDecided,
    /// An assignment was refused, and no generation was consumed.
    AssignmentRefused,
    /// The assigned run durably accepted.
    AssignmentAccepted,
    /// The assigned run refused its assignment, retiring the generation.
    AssignmentReleased,
    /// A handoff was recorded: the source assignment was stashed and a
    /// generation offered toward the target
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)). The
    /// entry carries the source assignment; the detail carries the handoff
    /// id and target agent — this is where handoff lineage lands in
    /// authorized task history ([specification 14.2](../../../docs/plans/rakka-agent/spec.md)).
    HandoffInitiated,
    /// The handoff's target assignment was durably accepted: responsibility
    /// transferred.
    HandoffAccepted,
    /// The handoff resolved without an accepted target; the source
    /// assignment was restored. The detail carries the refusal code.
    HandoffRefused,
    /// A team board claim was recorded: the claimant became the assignee and
    /// a generation is about to be offered
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)). The
    /// detail carries the claim id and member.
    TeamClaimRecorded,
    /// The claim's assignment was durably accepted: the claimant owns the
    /// task under the assignment fence.
    TeamClaimAccepted,
    /// The claim resolved without an accepted assignment; the detail carries
    /// the refusal code and the assignee cleared back to the board-pending
    /// posture.
    TeamClaimRefused,
    /// A run proposed a typed result.
    ResultProposed,
    /// A proposal passed every deterministic rule.
    ResultAccepted,
    /// A proposal was refused by a deterministic rule.
    ResultRejected,
    /// A durable cancellation request was recorded, leaving the task
    /// nonterminal while its wind-down propagates
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)); the
    /// detail carries the terminal-reason code finalization will record.
    CancellationRequested,
    /// The task reached a terminal status.
    Terminated,
    /// A delivered wake occurrence was dispositioned by the controller.
    WakeDispositioned,
    /// An epoch was admitted — directly, coalesced, or by promotion.
    EpochAdmitted,
    /// An admitted epoch's terminal result settled on the controller.
    EpochSettled,
    /// The continuous goal was suspended, by an operator or by failure
    /// escalation.
    GoalSuspended,
    /// The continuous goal was resumed.
    GoalResumed,
    /// The continuous goal's expiry was renewed.
    GoalRenewed,
    /// The continuous goal expired, observed by a recorded transition.
    GoalExpired,
    /// The continuous goal was retired, by an operator or by reaching its
    /// retirement policy.
    GoalRetired,
    /// A schedule update took force.
    ScheduleUpdated,
    /// The goal contract was activated: `Proposed` became `Active`, in the
    /// creating transition or by command. The `Goal*` kinds above are the
    /// continuous admission gate's; these four are the goal-contract status
    /// of [specification 8.1](../../../docs/plans/rakka-agent/spec.md).
    GoalActivated,
    /// The goal contract was parked `Waiting` under a persisted policy
    /// decision; the detail carries the structured reason code.
    GoalParked,
    /// The goal contract was reactivated from `Waiting`.
    GoalReactivated,
    /// The goal contract reached a terminal decision; the detail carries the
    /// terminal status and reason code.
    GoalDecided,
    /// The stagnation detector tripped a threshold; the detail carries the
    /// trigger, the streak, and the action taken, and the digest carries the
    /// repeated result fingerprint for a repetition trip.
    GoalStagnationDetected,
    /// The goal's success criteria were revised; the detail carries the
    /// criteria revision now in force.
    GoalCriteriaRevised,
}

impl AgentTaskHistoryKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::DependencyDeclared => "dependency-declared",
            Self::DependencyResolved => "dependency-resolved",
            Self::DependentRegistered => "dependent-registered",
            Self::DependentRegistrationRefused => "dependent-registration-refused",
            Self::AssignmentDecided => "assignment-decided",
            Self::AssignmentRefused => "assignment-refused",
            Self::AssignmentAccepted => "assignment-accepted",
            Self::AssignmentReleased => "assignment-released",
            Self::HandoffInitiated => "handoff-initiated",
            Self::HandoffAccepted => "handoff-accepted",
            Self::HandoffRefused => "handoff-refused",
            Self::TeamClaimRecorded => "team-claim-recorded",
            Self::TeamClaimAccepted => "team-claim-accepted",
            Self::TeamClaimRefused => "team-claim-refused",
            Self::ResultProposed => "result-proposed",
            Self::ResultAccepted => "result-accepted",
            Self::ResultRejected => "result-rejected",
            Self::CancellationRequested => "cancellation-requested",
            Self::Terminated => "terminated",
            Self::WakeDispositioned => "wake-dispositioned",
            Self::EpochAdmitted => "epoch-admitted",
            Self::EpochSettled => "epoch-settled",
            Self::GoalSuspended => "goal-suspended",
            Self::GoalResumed => "goal-resumed",
            Self::GoalRenewed => "goal-renewed",
            Self::GoalExpired => "goal-expired",
            Self::GoalRetired => "goal-retired",
            Self::ScheduleUpdated => "schedule-updated",
            Self::GoalActivated => "goal-activated",
            Self::GoalParked => "goal-parked",
            Self::GoalReactivated => "goal-reactivated",
            Self::GoalDecided => "goal-decided",
            Self::GoalStagnationDetected => "goal-stagnation-detected",
            Self::GoalCriteriaRevised => "goal-criteria-revised",
        }
    }
}

impl Display for AgentTaskHistoryKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One append-only entry in a task's durable domain history
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is a bounded record of *what happened*: identities, digests, and artifact
/// references. It never carries messages, observations, tool payloads, prompts,
/// memory records, or resolved credentials — those live behind the artifact
/// references it points at, under the application's own retention policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskHistoryEntry {
    schema_version: StateSchemaVersion,
    /// Monotonic sequence within the task, and the append's idempotency key.
    pub sequence: AgentTaskHistorySequence,
    /// What the entry records.
    pub kind: AgentTaskHistoryKind,
    /// The operation that produced it.
    pub operation_id: AgentOperationId,
    /// The task's status once the transition committed.
    pub status: AgentTaskStatus,
    /// The assignment generation in force, when one was.
    pub generation: Option<AgentAssignmentGeneration>,
    /// The agent involved, when one was.
    pub agent: Option<AgentId>,
    /// The run involved, when one was.
    pub run: Option<AgentRunId>,
    /// The fingerprint of the content involved, when there was any.
    pub digest: Option<AgentContentDigest>,
    /// The authenticated principal involved, when one was: a human-owned
    /// task's submission rows carry the submitter
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    /// Entries persisted before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// Bounded detail: the rejection reason, the refusal code, the terminal
    /// reason.
    pub detail: String,
    /// When the transition committed.
    pub at: AgentTimestampMillis,
}

impl AgentTaskHistoryEntry {
    fn new(
        sequence: AgentTaskHistorySequence,
        kind: AgentTaskHistoryKind,
        operation_id: AgentOperationId,
        status: AgentTaskStatus,
        at: AgentTimestampMillis,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION,
            sequence,
            kind,
            operation_id,
            status,
            generation: None,
            agent: None,
            run: None,
            digest: None,
            principal: None,
            detail: String::new(),
            at,
        }
    }

    fn with_assignment(mut self, assignment: &AgentTaskAssignment) -> Self {
        self.generation = Some(assignment.generation);
        self.agent = Some(assignment.agent.clone());
        self.run = Some(assignment.run.clone());
        self
    }

    fn with_digest(mut self, digest: AgentContentDigest) -> Self {
        self.digest = Some(digest);
        self
    }

    fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = bounded_detail(detail);
        self
    }
}

impl VersionedAgentRecord for AgentTaskHistoryEntry {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TaskHistoryEntry;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, authorized read over a task's history.
///
/// The cursor is the only way to reach history a task's materialized state no
/// longer holds. Its page size is clamped to
/// [`AGENT_TASK_HISTORY_MAX_PAGE_SIZE`], so no reader can ask a store for an
/// unbounded page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentTaskHistoryCursor {
    after: Option<AgentTaskHistorySequence>,
    limit: usize,
}

impl AgentTaskHistoryCursor {
    /// A cursor over the whole history, from the beginning.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// A cursor resuming after one sequence.
    #[must_use]
    pub const fn after(sequence: AgentTaskHistorySequence) -> Self {
        Self {
            after: Some(sequence),
            limit: AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// Sets the page size, clamped to [`AGENT_TASK_HISTORY_MAX_PAGE_SIZE`].
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = if limit == 0 {
            1
        } else if limit > AGENT_TASK_HISTORY_MAX_PAGE_SIZE {
            AGENT_TASK_HISTORY_MAX_PAGE_SIZE
        } else {
            limit
        };
        self
    }

    /// The sequence this page resumes after.
    #[must_use]
    pub const fn position(&self) -> Option<AgentTaskHistorySequence> {
        self.after
    }

    /// The clamped page size.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for AgentTaskHistoryCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of task history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTaskHistoryPage {
    /// The entries, oldest first.
    pub entries: Vec<AgentTaskHistoryEntry>,
    /// The cursor that resumes after this page, when more history exists.
    pub next: Option<AgentTaskHistoryCursor>,
}

impl AgentTaskHistoryPage {
    /// Whether more history follows this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.next.is_some()
    }
}

/// Boxed future returned by an [`AgentTaskHistoryStore`].
pub type AgentTaskHistoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentTaskResult<T>> + Send + 'a>>;

/// The append-only durable history of every task, separate from the bounded
/// materialized state that drives transitions
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// An append is idempotent on `(scope, sequence)`: the entity assigns the
/// sequence inside the transition that produced the entry, so re-driving an
/// interrupted flush writes the same entry to the same slot rather than
/// duplicating it. A store that finds a different entry already at a sequence
/// must fail closed rather than overwrite it — that would mean two different
/// transitions claimed one slot.
///
/// Reads are tenant-scoped by the [`AgentTaskScope`] they address, so a caller
/// cannot learn whether another tenant's task exists.
pub trait AgentTaskHistoryStore: Clone + Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends one entry, idempotently.
    fn append<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        entry: &'a AgentTaskHistoryEntry,
    ) -> AgentTaskHistoryFuture<'a, ()>;

    /// Reads one bounded page.
    fn read<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        cursor: AgentTaskHistoryCursor,
    ) -> AgentTaskHistoryFuture<'a, AgentTaskHistoryPage>;
}

/// An in-memory task history, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentTaskHistoryStore {
    entries: Arc<Mutex<BTreeMap<String, BTreeMap<u64, AgentTaskHistoryEntry>>>>,
}

impl InMemoryAgentTaskHistoryStore {
    /// Creates an empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many entries one task has.
    #[must_use]
    pub fn len(&self, scope: &AgentTaskScope) -> usize {
        self.entries
            .lock()
            .expect("the task history should not be poisoned")
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one task has no history.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentTaskScope) -> bool {
        self.len(scope) == 0
    }
}

impl AgentTaskHistoryStore for InMemoryAgentTaskHistoryStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        entry: &'a AgentTaskHistoryEntry,
    ) -> AgentTaskHistoryFuture<'a, ()> {
        Box::pin(async move {
            let mut entries = self
                .entries
                .lock()
                .expect("the task history should not be poisoned");
            let task = entries.entry(scope.key()).or_default();
            match task.get(&entry.sequence.get()) {
                // A re-driven flush writes the same entry to the same slot.
                Some(existing) if existing == entry => Ok(()),
                Some(_) => Err(AgentTaskError::HistoryConflict {
                    sequence: entry.sequence,
                }),
                None => {
                    task.insert(entry.sequence.get(), entry.clone());
                    Ok(())
                }
            }
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        cursor: AgentTaskHistoryCursor,
    ) -> AgentTaskHistoryFuture<'a, AgentTaskHistoryPage> {
        Box::pin(async move {
            let entries = self
                .entries
                .lock()
                .expect("the task history should not be poisoned");
            let Some(task) = entries.get(&scope.key()) else {
                return Ok(AgentTaskHistoryPage {
                    entries: Vec::new(),
                    next: None,
                });
            };

            let start = cursor.position().map_or(0, |after| after.get() + 1);
            let mut page: Vec<AgentTaskHistoryEntry> = task
                .range(start..)
                .map(|(_, entry)| entry.clone())
                .take(cursor.limit() + 1)
                .collect();

            let next = (page.len() > cursor.limit())
                .then(|| {
                    page.pop();
                    page.last().map(|entry| {
                        AgentTaskHistoryCursor::after(entry.sequence).with_limit(cursor.limit())
                    })
                })
                .flatten();

            Ok(AgentTaskHistoryPage {
                entries: page,
                next,
            })
        })
    }
}

/// The command an [`AgentExchangeKind::Creation`] exchange carries.
///
/// This is the delegating-run path of
/// [specification 9.8](../../../docs/plans/rakka-agent/spec.md): a parent run
/// creates a child task through the durable substrate. An ingress creates a task
/// through the equivalent [`AgentTaskEntityCommand::Create`], and both reach the
/// same bounded transition, so the two paths cannot diverge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskCreation {
    /// The typed contract of the created task.
    pub definition: AgentTaskDefinition,
    /// Its bounded input.
    pub input: AgentTaskContent,
    /// The agent the task should be assigned to, when it is agent-owned.
    pub assignee: Option<AgentId>,
    /// The team whose shared board will govern the assignment
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)). An
    /// agent-owned creation carrying a team may omit the assignee: the task
    /// waits on the board until a claim names one. Records persisted before
    /// this field load without one.
    #[serde(default)]
    pub team: Option<crate::identity::AgentTeamId>,
    /// The collaborative goal it contributes to.
    pub goal: Option<AgentGoalId>,
    /// Whether the task coordinates a finite or continuous goal
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Continuous mode makes this the root control task the wake controller of
    /// slice 3.2 drives, and it requires a goal binding. Records persisted
    /// before this field load as finite.
    #[serde(default)]
    pub goal_mode: AgentGoalMode,
    /// The goal contract this creation institutes, making the created task the
    /// goal's root coordinator
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)). Absent,
    /// the task is a child contributor — or, before slice 4.1, a root without
    /// a goal record — and carries only the `goal` binding. When present
    /// without an explicit `goal` binding, the goal id defaults to the created
    /// task's own id. Records persisted before this field load without one.
    #[serde(default)]
    pub goal_spec: Option<Box<AgentGoalSpecDraft>>,
    /// The task that created it.
    pub parent: Option<AgentTaskId>,
    /// Dependencies declared with the creation.
    pub dependencies: Vec<AgentTaskDependencyDeclaration>,
    /// The escrow grant the creating parent debited from its own ledger for
    /// this task, carried on the creation command exactly as
    /// [specification 9.7](../../../docs/plans/rakka-agent/spec.md) requires.
    /// Absent — every root creation — the task's ledger is built from its
    /// definition ceilings. Records persisted before this field load without
    /// one.
    #[serde(default)]
    pub escrow: Option<AgentBudgetGrant>,
    /// The continuous-goal wake this task executes as an epoch of
    /// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)), when it
    /// is one. An epoch task completing owes its result back to the parent
    /// controller under this wake. Records persisted before this field load
    /// without one.
    #[serde(default)]
    pub wake: Option<AgentWakeId>,
    /// The delegation provenance the collaboration metadata carried, when a
    /// parent run's delegation created this task
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)). Records
    /// persisted before this field load without one.
    #[serde(default)]
    pub delegation: Option<Box<crate::delegation::AgentTaskDelegationProvenance>>,
    /// Trace context of the ingress that created the task — what the A2A
    /// surface extracted before durable acceptance
    /// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
    /// Context flows with commands: the creating transition records it, and
    /// the assignment the task later owes carries it onward. Observability
    /// only, never correctness; a creation without one starts a root.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
}

/// The command an [`AgentExchangeKind::Assignment`] exchange carries to the run
/// entity: create the run that serves one assignment generation.
///
/// The run entity of slice 1.5 is its receiver. It is deduplicated by the task
/// and the generation, so replaying it resolves to the same run rather than a
/// second one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunAssignment {
    /// The task the run will serve for its entire lifetime.
    pub task: AgentTaskScope,
    /// The run to create.
    pub run: AgentRunScope,
    /// The generation this run owns.
    pub generation: AgentAssignmentGeneration,
    /// The typed contract the run must satisfy.
    pub definition: AgentTaskDefinition,
    /// The task's bounded input.
    pub input: AgentTaskContent,
    /// The collaborative goal the run contributes to.
    pub goal: Option<AgentGoalId>,
    /// The escrow the task debited from its own ledger for this run, carried on
    /// the creation command exactly as
    /// [specification 9.7](../../../docs/plans/rakka-agent/spec.md) requires.
    ///
    /// The grant travels *with* the command rather than being fetched by the
    /// run, because the debit and the command commit in one transition: a run
    /// that exists holding this grant is a run whose parent has already paid
    /// for it.
    pub budget: AgentBudgetGrant,
    /// The agent definition revision the assignment was decided against.
    pub agent_definition_revision: AgentRevisionNumber,
    /// The agent settings revision the assignment was decided against.
    pub agent_settings_revision: AgentRevisionNumber,
    /// The delegation authority the run enforces
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)): the
    /// goal's skill and tool narrowing, its advisory ceilings, and the
    /// lineage/depth of the task this run serves. Copied from the task's own
    /// durable state by the assignment decision, because the run never reads
    /// the goal spec. Commands persisted before this field load without one,
    /// which means no goal narrowing.
    #[serde(default)]
    pub delegation: Option<Box<crate::delegation::AgentRunDelegationEnvelope>>,
    /// When the decision was recorded.
    pub assigned_at: AgentTimestampMillis,
}

/// The run's durable acceptance, returned as the assignment exchange's reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunAcceptance {
    /// The run that accepted.
    pub run: AgentRunScope,
    /// The generation it accepted.
    pub generation: AgentAssignmentGeneration,
    /// When it durably accepted.
    pub accepted_at: AgentTimestampMillis,
}

/// The command an [`AgentExchangeKind::RunCancel`] exchange carries to the run
/// entity: wind down the run serving one assignment generation
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The request is owed by the task whose nonterminal cancellation marker is
/// set, and only for a generation whose run durably accepted — a cancellation
/// can therefore never outrun an in-flight assignment and refuse definitively
/// against a run that does not exist yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunCancelRequest {
    /// The task requesting the wind-down.
    pub task: AgentTaskScope,
    /// The assignment generation the request fences against: a run serving a
    /// different generation refuses rather than winding down on a stale push.
    pub generation: AgentAssignmentGeneration,
    /// The bounded reason recorded on the run's terminal record.
    pub reason: String,
}

/// The run's durable receipt replying to an [`AgentExchangeKind::RunCancel`].
///
/// The receipt reports the status the wind-down reached — an accepted receipt
/// is the observable outcome of the propagation request, never proof that a
/// started effect stopped ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunCancelReceipt {
    /// The run that recorded the request.
    pub run: AgentRunScope,
    /// The status the run held after recording it: `Cancelling` while work
    /// quiesces, `WaitingForReconciliation` while an ambiguous effect blocks
    /// terminalization, or a terminal status when the request found the run
    /// already settled.
    pub status: crate::run::AgentRunStatus,
}

/// The notice an [`AgentExchangeKind::HandoffResult`] exchange carries to the
/// source run: how its handoff resolved
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Owed by the task whose handoff provenance settled — accepted when the
/// target's assignment was durably accepted, refused when the transfer
/// resolved without one — and re-derived by every settle pass until the
/// exchange settles, so a lost notice is re-owed rather than gone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHandoffResultNotice {
    /// The task reporting the resolution.
    pub task: AgentTaskScope,
    /// The handoff this notice resolves. The source run's arm matches it
    /// against its own cell's record: a mismatch is a forgery, whatever the
    /// sender claims.
    pub handoff: crate::identity::AgentHandoffId,
    /// How the handoff resolved.
    pub resolution: AgentHandoffResolutionNotice,
}

/// How a handoff resolved, as its result notice reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentHandoffResolutionNotice {
    /// The target's assignment was durably accepted: responsibility has
    /// transferred, and the source terminalizes `HandedOff`.
    Accepted {
        /// The target run now serving the task.
        target_run: AgentRunId,
        /// The accepted assignment generation.
        generation: AgentAssignmentGeneration,
    },
    /// The transfer resolved without an accepted target: the source
    /// assignment was restored and the source resumes with the failed tool
    /// result this code reaches the model as.
    Refused {
        /// Stable machine-readable refusal code.
        code: String,
    },
}

/// The command an [`AgentExchangeKind::DependencyRegistration`] exchange
/// carries to the upstream task: record the sender as a dependent so the
/// upstream's terminal transition can notify it
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Owed by the dependent in the same compare-and-set that records the
/// forward edge. The policy travels for the record only: the *dependent*
/// applies its own declared policy when the outcome arrives — an upstream
/// enforcing another task's policy would be deciding with facts it does not
/// own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDependencyRegistration {
    /// The dependent registering itself. The upstream's arm matches it
    /// against the envelope's initiator: a mismatch is a forgery, whatever
    /// the sender claims.
    pub dependent: AgentTaskScope,
    /// The upstream task the sender depends on.
    pub upstream: AgentTaskId,
    /// The failure policy the dependent declared for this edge.
    pub policy: AgentDependencyFailurePolicy,
}

/// The upstream task's durable receipt replying to an
/// [`AgentExchangeKind::DependencyRegistration`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDependencyRegistrationReceipt {
    /// The upstream that answered.
    pub upstream: AgentTaskScope,
    /// The upstream's terminal outcome, when it was already terminal at
    /// registration: the dependent applies it from the receipt, because the
    /// upstream owes no notification for an edge it never recorded.
    pub outcome: Option<AgentTaskDependencyOutcome>,
    /// The terminal-reason code behind that outcome, when one was recorded.
    pub terminal_reason: Option<String>,
    /// The accepted result's fingerprint, when the upstream completed.
    pub result_digest: Option<AgentContentDigest>,
}

/// The notice an [`AgentExchangeKind::DependencyOutcome`] exchange carries to
/// one registered dependent: how the upstream terminalized
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Owed immediately by the terminal transition — status, terminal reason,
/// and result digest are all absorbing the moment the terminal commits, so
/// unlike a delegation result nothing here waits for the escrow ledger to
/// close.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDependencyOutcomeNotice {
    /// The upstream reporting its terminal outcome. The dependent's arm
    /// matches it against its own forward edge: an unknown or mismatched
    /// upstream is refused, whatever the sender claims.
    pub upstream: AgentTaskScope,
    /// How the upstream resolved.
    pub outcome: AgentTaskDependencyOutcome,
    /// The terminal-reason code, for the dependent's records.
    pub terminal_reason: Option<String>,
    /// The accepted result's fingerprint, when the upstream completed.
    pub result_digest: Option<AgentContentDigest>,
}

/// The dependent task's durable receipt replying to an
/// [`AgentExchangeKind::DependencyOutcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDependencyOutcomeReceipt {
    /// The dependent that applied the outcome.
    pub dependent: AgentTaskScope,
    /// The dependent's status after applying it: unblocked, cancelling under
    /// its declared policy, or unchanged for an echoed replay.
    pub status: AgentTaskStatus,
}

/// A run's report of what it finally consumed, carried by an
/// [`AgentExchangeKind::BudgetSettlement`] exchange
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It travels only after a known terminal run outcome, and it is what makes the
/// parent's own consumption — and, in a deeper hierarchy, its parent's — the
/// truth about what the work cost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetSettlement {
    /// The run that settled.
    pub run: AgentRunScope,
    /// The generation it served.
    pub generation: AgentAssignmentGeneration,
    /// What it consumed, per conserved dimension.
    pub consumed: AgentBudgetConsumption,
}

/// A run's release of the escrow it held and will not use, carried by an
/// [`AgentExchangeKind::BudgetReturn`] exchange
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It carries no amount. The parent recorded what it escrowed and has already
/// applied the settlement, so it — not the child — computes the remainder. A
/// child that named its own refund would be asking the parent to trust the
/// child's arithmetic about the parent's ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetReturn {
    /// The run releasing its escrow.
    pub run: AgentRunScope,
    /// The generation it served.
    pub generation: AgentAssignmentGeneration,
}

/// A run's request for more escrow, carried by an
/// [`AgentExchangeKind::BudgetAllocation`] exchange
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run *asks*; the grant is an ordinary parent-local allocation decision
/// under the same ceilings, and a grant of nothing is a legitimate answer. This
/// is the one allocation that is its own exchange: a run's first allocation
/// rides its assignment, because the debit and the command that carries it
/// commit in the parent's one transition, and only a later top-up needs a
/// command of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetTopUpRequest {
    /// The run asking.
    pub run: AgentRunScope,
    /// The generation it serves.
    pub generation: AgentAssignmentGeneration,
    /// Which request this is, counting from one.
    ///
    /// It fences the replay window the exchange journal's bounded ring cannot:
    /// the parent's escrow records the sequence it last granted, so a re-driven
    /// request returns the original grant and an older one is refused.
    pub sequence: u64,
    /// The ceiling the run reached, so the parent decides on facts.
    pub exhaustion: AgentBudgetExhaustion,
}

/// What a ledger exchange returns to the run that initiated it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetLedgerOutcome {
    /// The escrow the parent granted, for a top-up request. It is
    /// [`AgentBudgetAllocation::nothing`] when the parent has nothing left, and
    /// absent for a settlement or a return, which grant nothing.
    pub granted: Option<AgentBudgetAllocation>,
}

/// Payload type of an [`AgentBudgetSettlement`] exchange command.
pub const AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE: &str = "rakka.agent.BudgetSettlement";

/// Payload type of an [`AgentBudgetReturn`] exchange command.
pub const AGENT_BUDGET_RETURN_PAYLOAD_TYPE: &str = "rakka.agent.BudgetReturn";

/// Payload type of an [`AgentBudgetTopUpRequest`] exchange command.
pub const AGENT_BUDGET_TOP_UP_PAYLOAD_TYPE: &str = "rakka.agent.BudgetTopUpRequest";

/// Payload type of an [`AgentBudgetLedgerOutcome`] exchange result.
pub const AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.BudgetLedgerOutcome";

/// A run's proposal of a typed task result
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run persists the proposal before it sends, and the task's decision — not
/// the run's state — is what makes the public task terminal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskResultProposal {
    /// The stable proposal identity. Replaying it returns the original decision.
    pub proposal_id: AgentOperationId,
    /// The agent that proposed it.
    pub agent: AgentId,
    /// The run that proposed it.
    pub run: AgentRunId,
    /// The generation the run owns. A proposal from a superseded generation is
    /// fenced.
    pub generation: AgentAssignmentGeneration,
    /// The task definition the run validated against.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition. A mismatch fails closed.
    pub definition_version: AgentRevisionNumber,
    /// The schema the result is expressed in.
    pub result_schema: AgentSchemaRef,
    /// The bounded proposed content.
    pub content: AgentTaskContent,
    /// Evidence artifacts supporting the result.
    pub evidence: Vec<ArtifactRef>,
    /// What caused the proposal. At most [`AGENT_IDENTITY_MAX_LENGTH`] bytes:
    /// a rejection persists it, and a longer id is refused without a
    /// validation decision.
    pub causation_id: AgentCausationId,
    /// When the run proposed it.
    pub proposed_at: AgentTimestampMillis,
}

/// An authenticated human or external service's typed-result submission to a
/// human-owned task ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
///
/// It travels the same deterministic validation path as a run's
/// [`AgentTaskResultProposal`] — definition binding, schema binding, content
/// bounds, and every applicable result rule — differing only in provenance: a
/// principal instead of an assignment. A human task is a work product with a
/// typed result, never a substitute for an effect-bound checkpoint: a
/// decision that approves, authorizes, or reconciles a *specific effect*
/// stays bound to the exact effect intent through
/// [`crate::checkpoints::AgentCheckpoint`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentHumanResultSubmission {
    /// The authenticated principal submitting the result, as the
    /// deployment's stable `type:id` reference. Attribution, never
    /// authorization: the caller authenticated at the public boundary before
    /// the command was built. At most [`AGENT_IDENTITY_MAX_LENGTH`] bytes.
    pub principal: String,
    /// The task definition the submission claims to fulfill. A mismatch
    /// fails closed.
    pub definition_id: AgentTaskDefinitionId,
    /// The claimed revision of that definition.
    pub definition_version: AgentRevisionNumber,
    /// The schema the result is expressed in.
    pub result_schema: AgentSchemaRef,
    /// The bounded submitted content.
    pub content: AgentTaskContent,
    /// Evidence artifacts supporting the result.
    pub evidence: Vec<ArtifactRef>,
    /// What caused the submission. At most [`AGENT_IDENTITY_MAX_LENGTH`]
    /// bytes: a rejection persists it, and a longer id is refused without a
    /// validation decision.
    pub causation_id: AgentCausationId,
    /// When the submission entered the accepting boundary, by that
    /// boundary's clock.
    pub submitted_at: AgentTimestampMillis,
}

/// The task's durable decision on one result proposal, returned as the
/// proposal exchange's reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskDecision {
    /// The proposal passed every deterministic rule; the task is `Completed`.
    Accepted {
        /// The accepted typed result.
        result: Box<AgentAcceptedResult>,
    },
    /// A deterministic rule refused the proposal.
    Rejected {
        /// The durable rejection decision.
        rejection: Box<AgentTaskRejection>,
        /// Sanitized feedback the run may use for another bounded iteration.
        feedback: String,
        /// How many further proposals the task will still consider. Zero means
        /// the rejection budget is spent and the task has failed.
        remaining_iterations: u32,
        /// The task's status after the decision.
        status: AgentTaskStatus,
    },
    /// The proposal was refused without a validation decision: it was fenced by
    /// a newer assignment generation, or the task is already terminal. It costs
    /// the live run nothing, and it consumes none of the rejection budget.
    Refused {
        /// Stable machine-readable code.
        code: String,
        /// The task's current status.
        status: AgentTaskStatus,
    },
}

/// The agent facts one assignment decision is made against.
///
/// It is read from the agent's *durable state* — never a command round trip
/// through [`crate::agent::AgentEntity`], which would serialize every run of a
/// popular agent through one mailbox
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)). It is bounded
/// and carries no credential material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAssignmentReadiness {
    /// The agent the decision is about.
    pub agent: AgentId,
    /// Its durable lifecycle status, when it is instantiated.
    pub status: Option<AgentLifecycleStatus>,
    /// The definition revision in force.
    pub definition_revision: Option<AgentRevisionNumber>,
    /// The settings revision in force.
    pub settings_revision: Option<AgentRevisionNumber>,
    /// Whether the agent's definition envelope declares this task definition.
    pub permits_task_definition: bool,
    /// Whether the agent's definition envelope declares the operation class the
    /// task runs under.
    pub permits_operation_class: bool,
    /// Whether the agent's definition envelope declares every skill the task
    /// requires.
    pub declares_required_skills: bool,
    /// Why the agent's autonomy admission does not admit this work, when it
    /// does not ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is `Some` for an agent with no admission at all, which is what makes
    /// the check fail closed: unattended work needs a decision that says it may
    /// run, and the absence of one is not permission.
    pub admission_refusal: Option<AgentAdmissionRefusal>,
}

impl AgentAssignmentReadiness {
    /// Reads the decision-relevant facts out of an agent's durable state.
    ///
    /// An agent whose envelope declares no task definitions is authorized for
    /// none: the check fails closed rather than treating an empty declaration as
    /// permission for everything.
    ///
    /// The admission answer is *derived* here rather than read from a flag: the
    /// decision on record is asked whether it admits this task's operation class
    /// under the agent's definition envelope as it stands now
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)). A
    /// definition published since the admission that widened anything therefore
    /// stops assignment without anything having had to notice the update.
    #[must_use]
    pub fn from_agent_state(
        state: &AgentEntityState,
        definition: &AgentTaskDefinition,
        now: AgentTimestampMillis,
    ) -> Self {
        let envelope = state.definition().envelope();
        Self {
            agent: state.scope().agent().clone(),
            status: Some(state.status()),
            definition_revision: Some(state.definition().revision()),
            settings_revision: Some(state.settings().revision()),
            permits_task_definition: envelope
                .task_definitions
                .contains(&definition.definition_id),
            permits_operation_class: !definition.operation_class.is_unattended()
                || envelope
                    .operation_classes
                    .contains(&definition.operation_class),
            declares_required_skills: definition.required_skills.iter().all(|skill| {
                envelope
                    .tools
                    .values()
                    .any(|tool| tool.capabilities.contains(skill))
            }),
            admission_refusal: definition
                .operation_class
                .is_unattended()
                .then(|| {
                    state
                        .admission()
                        .map_or(Some(AgentAdmissionRefusal::Missing), |decision| {
                            // The full enforcement point: the decision is
                            // re-derived against the agent definition now in
                            // force, so a republish that dropped a verified
                            // requirement (a policy is not in the envelope) stops
                            // assignment even when the envelope did not widen.
                            decision
                                .admits_definition(
                                    definition.operation_class,
                                    state.definition().definition(),
                                    now,
                                )
                                .err()
                        })
                })
                .flatten(),
        }
    }

    /// The facts of an agent that has no durable state at all.
    ///
    /// It is refused for having no state before any of the rest is consulted,
    /// so every other fact is the fail-closed one.
    #[must_use]
    pub fn not_instantiated(agent: AgentId) -> Self {
        Self {
            agent,
            status: None,
            definition_revision: None,
            settings_revision: None,
            permits_task_definition: false,
            permits_operation_class: false,
            declares_required_skills: false,
            admission_refusal: Some(AgentAdmissionRefusal::Missing),
        }
    }

    /// Why the decision must refuse this agent, when it must.
    #[must_use]
    pub fn refusal(&self) -> Option<(AgentAssignmentRefusalReason, String)> {
        let Some(status) = self.status else {
            return Some((
                AgentAssignmentRefusalReason::AgentNotInstantiated,
                format!("agent {} has no durable state", self.agent),
            ));
        };
        if !status.permits_dispatch() {
            return Some((
                AgentAssignmentRefusalReason::AgentNotActive,
                format!("agent {} is {status}", self.agent),
            ));
        }
        if !self.permits_task_definition {
            return Some((
                AgentAssignmentRefusalReason::TaskDefinitionNotPermitted,
                format!(
                    "agent {} does not declare this task definition in its authority envelope",
                    self.agent
                ),
            ));
        }
        if !self.permits_operation_class {
            return Some((
                AgentAssignmentRefusalReason::OperationClassNotDeclared,
                format!(
                    "agent {} does not declare this task's operation class in its authority envelope",
                    self.agent
                ),
            ));
        }
        if !self.declares_required_skills {
            return Some((
                AgentAssignmentRefusalReason::SkillNotDeclared,
                format!(
                    "agent {} does not declare every skill this task requires",
                    self.agent
                ),
            ));
        }
        if let Some(refusal) = &self.admission_refusal {
            // The fail-closed rule of specification 7.4. It is checked last of
            // the envelope facts and first of nothing: an agent may be
            // instantiated, active, and fully declared, and still not admitted
            // to run unattended.
            return Some((
                AgentAssignmentRefusalReason::NotAdmitted,
                refusal.to_string(),
            ));
        }
        None
    }
}

/// The bounded materialized record of one created task
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// It holds what the next legal transition needs and nothing else: no messages,
/// no observations, no tool payloads, no superseded assignments, no result
/// proposals, no audit events, no memory records. Those are history entries and
/// artifact references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTask {
    /// The typed contract.
    pub definition: AgentTaskDefinition,
    /// The bounded input.
    pub input: AgentTaskContent,
    /// The public lifecycle status.
    pub status: AgentTaskStatus,
    /// The collaborative goal this task contributes to.
    pub goal: Option<AgentGoalId>,
    /// Whether it coordinates a finite or continuous goal. Records persisted
    /// before this field load as finite.
    #[serde(default)]
    pub goal_mode: AgentGoalMode,
    /// The goal contract this task coordinates as the goal's root
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md),
    /// [6.3](../../../docs/plans/rakka-agent/spec.md)): the versioned spec and
    /// its status lifecycle, held as a component of the root task's own record
    /// so every goal transition commits in the task's compare-and-set. A child
    /// task carries only the `goal` binding; records persisted before this
    /// field load without a goal record.
    #[serde(default)]
    pub goal_state: Option<Box<AgentGoalState>>,
    /// The wake controller's durable state, once the task coordinates a
    /// continuous goal. Records persisted before this field load with no
    /// controller activity, and a finite task never carries one.
    #[serde(default)]
    pub wake_controller: Option<AgentWakeControllerState>,
    /// The continuous-goal wake this task executes as an epoch of, when it is
    /// one. Its terminal transition owes the epoch result back to the parent
    /// controller under this wake. Records persisted before this field load
    /// without one.
    #[serde(default)]
    pub wake: Option<AgentWakeId>,
    /// The task that created it.
    pub parent: Option<AgentTaskId>,
    /// The delegation provenance recorded at creation, when a parent run's
    /// delegation created this task
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)). Records
    /// persisted before this field load without one.
    #[serde(default)]
    pub delegation: Option<Box<crate::delegation::AgentTaskDelegationProvenance>>,
    /// The agent the task is meant for, when it is agent-owned.
    pub assignee: Option<AgentId>,
    /// The bounded dependency summary.
    pub dependencies: BTreeMap<AgentTaskId, AgentTaskDependency>,
    /// The bounded dependents registry: tasks that registered a dependency
    /// on this one, walked at terminalization to owe each its outcome
    /// notification ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    /// Records persisted before this field load with none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dependents: BTreeMap<AgentTaskId, AgentTaskDependentRecord>,
    /// The escrow this task holds and debits every run it assigns from
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a component of the task's own record, so an allocation is debited
    /// in the same compare-and-set as the assignment that carries it. There is
    /// no window in which a run exists holding budget its parent did not debit,
    /// and no second writer that could debit it twice.
    pub escrow: AgentEscrowLedger,
    /// The current assignment, if any.
    pub assignment: Option<AgentTaskAssignment>,
    /// The highest assignment generation the task has decided.
    pub assignment_generation: AgentAssignmentGeneration,
    /// How many assignment generations the task has consumed.
    pub assignments: u32,
    /// The latest handoff, when one was recorded
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)); the
    /// chain is history. Records persisted before this field load without
    /// one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<Box<AgentTaskHandoff>>,
    /// How many handoffs the task has recorded over its lifetime.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub handoffs: u32,
    /// The team whose shared board governs this task's assignment, when one
    /// does ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    /// Creation-time provenance: a task created for a board waits unassigned
    /// until a claim names its assignee. Records persisted before this field
    /// load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<crate::identity::AgentTeamId>,
    /// The latest board claim, when a team drove one; the chain is history.
    /// Records persisted before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_claim: Option<Box<AgentTaskTeamClaim>>,
    /// How many board claims the task has recorded over its lifetime.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub team_claims: u32,
    /// The claim-epoch fence: a team-claim exchange whose epoch is not above
    /// this refuses, so a courier-reordered stale board decision can never
    /// revive a superseded claim. Records persisted before this field load
    /// with the fence at zero.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub team_claim_fence: u64,
    /// The most recent assignment refusal.
    pub last_refusal: Option<AgentAssignmentRefusal>,
    /// The accepted typed result.
    pub accepted_result: Option<Box<AgentAcceptedResult>>,
    /// How many result proposals deterministic rules have refused.
    pub rejection_count: u32,
    /// The most recent rejection decision. Earlier ones are history.
    pub last_rejection: Option<Box<AgentTaskRejection>>,
    /// Fingerprints ([`AgentContentDigest::of_bytes`] values) of rejected
    /// human-submission operation ids, oldest first, bounded by
    /// [`AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY`]: the durable echo
    /// past the operation log's window, so a replayed rejected submission is
    /// refused without spending the rejection budget twice. Fingerprints —
    /// not full operation ids — because the ring lives inside the bounded
    /// materialized record; a collision merely refuses a fresh submission,
    /// which retries under a new deduplication key. Records persisted before
    /// this field load with none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected_submissions: Vec<String>,
    /// Why the task reached its terminal status.
    pub terminal_reason: Option<AgentTaskTerminalReason>,
    /// The nonterminal cancellation request the task carries while its
    /// wind-down propagates ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    /// Records persisted before this field load without one.
    #[serde(default)]
    pub cancellation: Option<Box<AgentTaskCancellation>>,
    /// When the creation committed, stamped by the owner that wrote it — never
    /// by the initiator's clock, exactly as
    /// [`crate::choreography::AgentExchangeParticipant::apply`] requires of
    /// every durable timestamp.
    pub created_at: AgentTimestampMillis,
    /// Trace context of the ingress that created the task, held so the
    /// assignment decided in a *later* transition can carry the causal chain
    /// onward ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
    /// Observability only, never correctness: a task persisted before this
    /// field decodes to the empty context, and no transition reads it to
    /// decide anything.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
}

impl AgentTask {
    /// Whether every dependency permits the task to be worked on.
    #[must_use]
    pub fn dependencies_satisfied(&self) -> bool {
        self.dependencies
            .values()
            .all(AgentTaskDependency::is_satisfied)
    }

    /// The dependency that cancels this task, when one does.
    #[must_use]
    pub fn cancelling_dependency(&self) -> Option<&AgentTaskDependency> {
        self.dependencies
            .values()
            .find(|dependency| dependency.cancels_dependent())
    }

    /// Whether the task is waiting for an agent assignment decision.
    ///
    /// A goal-bearing root only assigns while its goal permits work: a
    /// `Proposed` goal spends nothing until activated, and a parked goal
    /// spends nothing until resumed
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
    /// A requested cancellation fences the next generation the same way
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)):
    /// acceptance of the request immediately stops new dispatch.
    #[must_use]
    pub fn awaits_assignment(&self) -> bool {
        self.definition.is_agent_owned()
            && self.status.is_assignable()
            && self.assignment.is_none()
            && self.cancellation.is_none()
            && self.dependencies_satisfied()
            && self
                .goal_state
                .as_ref()
                .is_none_or(|goal| goal.status().permits_work())
    }

    /// Serialized size of the materialized record, in bytes.
    ///
    /// This is what [`AGENT_TASK_MATERIALIZED_MAX_BYTES`] bounds, and what
    /// scenario 55 asserts stays inside its limit however long the task runs.
    #[must_use]
    pub fn materialized_size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects a record that exceeds its bounds, keeping `reserve` bytes below
    /// the materialized maximum.
    ///
    /// Admission-time transitions — creation and dependency declaration — pass
    /// [`AGENT_TASK_STATE_GROWTH_RESERVE_BYTES`], so every record they admit
    /// still has room for the assignment, refusal, rejection, and terminal
    /// reason its lifecycle may add; the transitions that add exactly that
    /// reserved growth pass zero.
    fn check_bounds(&self, reserve: usize) -> AgentTaskResult<()> {
        if self.dependencies.len() > self.definition.limits.max_dependencies {
            return Err(AgentTaskError::DependencyLimitExceeded {
                maximum: self.definition.limits.max_dependencies,
            });
        }
        let bytes = self.materialized_size_bytes();
        let maximum = AGENT_TASK_MATERIALIZED_MAX_BYTES.saturating_sub(reserve);
        if bytes > maximum {
            return Err(AgentTaskError::MaterializedStateTooLarge { bytes, maximum });
        }
        Ok(())
    }
}

/// The `skip_serializing_if` predicate of the task's handoff counter: a task
/// that never handed off serializes byte-identically to one persisted before
/// the field existed.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

/// The `skip_serializing_if` predicate of the task's claim fence: a task no
/// board ever touched serializes byte-identically to one persisted before
/// the field existed.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// The `skip_serializing_if` predicate of the dependency settled markers: an
/// unsettled record serializes byte-identically to one persisted before the
/// field existed.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

/// How a human submission's validation decided
/// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskSubmissionDisposition {
    /// The submission passed every deterministic rule; the task completed.
    Accepted,
    /// A deterministic rule refused the submission; the rejection is a
    /// durable decision.
    Rejected,
}

impl AgentTaskSubmissionDisposition {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }
}

/// The bounded summary of one human submission's validation decision,
/// carried on the [`AgentTaskOutcome`] of the transition that decided it.
///
/// A summary, never the content: the outcome rides the operation log inside
/// the bounded materialized record, so it carries codes, counts, and the
/// digest — the accepted content itself lives on the task's accepted-result
/// cell, and rejected content is never retained at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskSubmissionDecision {
    /// Whether the submission was accepted or rejected.
    pub disposition: AgentTaskSubmissionDisposition,
    /// The rejection cause code, when rejected.
    pub code: Option<String>,
    /// Bounded sanitized feedback, when rejected.
    pub feedback: String,
    /// How many further submissions the task will still consider. On a
    /// rejected decision, zero means this rejection exhausted the budget and
    /// the task failed.
    pub remaining_attempts: u32,
    /// The fingerprint of the decided content.
    pub digest: AgentContentDigest,
}

/// What a command transition attaches to its recorded [`AgentTaskOutcome`]
/// beyond the state-derived fields.
///
/// The attachment rides the outcome into the operation log, so a duplicate
/// reply inside the window answers with the very decision the original
/// transition carried.
struct AgentTaskOutcomeExtras {
    wake: Option<AgentWakeOutcome>,
    submission: Option<Box<AgentTaskSubmissionDecision>>,
}

impl AgentTaskOutcomeExtras {
    const NONE: Self = Self {
        wake: None,
        submission: None,
    };

    fn wake(wake: AgentWakeOutcome) -> Self {
        Self {
            wake: Some(wake),
            submission: None,
        }
    }

    fn submission(decision: AgentTaskSubmissionDecision) -> Self {
        Self {
            wake: None,
            submission: Some(Box::new(decision)),
        }
    }
}

/// The compact result of one accepted task transition.
///
/// A replayed operation returns this again rather than transitioning twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskOutcome {
    /// The task's status after the transition.
    pub status: AgentTaskStatus,
    /// The assignment generation in force.
    pub assignment_generation: AgentAssignmentGeneration,
    /// The assigned agent, when the task has one.
    pub agent: Option<AgentId>,
    /// The run serving the current assignment, when the task has one.
    pub run: Option<AgentRunId>,
    /// How many result rejections the task has recorded.
    pub rejection_count: u32,
    /// Whether every dependency permits the task to be worked on.
    pub dependencies_satisfied: bool,
    /// What a wake transition recorded, when this outcome answers one.
    /// Outcomes persisted before this field load with no wake record.
    #[serde(default)]
    pub wake: Option<AgentWakeOutcome>,
    /// The goal contract's status, when this task coordinates one. Outcomes
    /// persisted before this field load without it.
    #[serde(default)]
    pub goal: Option<AgentGoalOutcome>,
    /// The human submission decision this outcome answers, when it answers
    /// one ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    /// Outcomes persisted before this field load without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission: Option<Box<AgentTaskSubmissionDecision>>,
}

/// Bounded log of resolved operation ids and the outcome each produced.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentTaskOperationLog {
    entries: VecDeque<AgentTaskOperationLogEntry>,
}

impl AgentTaskOperationLog {
    /// The outcome a previously applied operation produced, if it is still in
    /// the window.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentTaskOutcome> {
        self.entries
            .iter()
            .find(|entry| &entry.operation_id == operation_id)
            .map(|entry| &entry.outcome)
    }

    /// How many operations are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no operation is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn record(&mut self, operation_id: AgentOperationId, outcome: AgentTaskOutcome) {
        self.entries.push_back(AgentTaskOperationLogEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_TASK_OPERATION_LOG_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentTaskOperationLogEntry {
    operation_id: AgentOperationId,
    outcome: AgentTaskOutcome,
}

/// The durable state of one typed-task entity.
///
/// It is the task's materialized record, the history it owes its sink, the
/// operations it has resolved, and the exchange journal the choreography
/// substrate writes — all in one compare-and-set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskState {
    schema_version: StateSchemaVersion,
    scope: AgentTaskScope,
    task: Option<AgentTask>,
    applied_operations: AgentTaskOperationLog,
    pending_history: Vec<AgentTaskHistoryEntry>,
    next_history_sequence: AgentTaskHistorySequence,
    journal: AgentExchangeJournal,
    updated_at: AgentTimestampMillis,
}

impl AgentTaskState {
    /// The state of a task that has never been created.
    #[must_use]
    pub fn uncreated(scope: AgentTaskScope, now: AgentTimestampMillis) -> Self {
        Self {
            schema_version: CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
            scope,
            task: None,
            applied_operations: AgentTaskOperationLog::default(),
            pending_history: Vec::new(),
            next_history_sequence: AgentTaskHistorySequence::FIRST,
            journal: AgentExchangeJournal::new(),
            updated_at: now,
        }
    }

    /// The scope this state belongs to.
    #[must_use]
    pub const fn scope(&self) -> &AgentTaskScope {
        &self.scope
    }

    /// The tenant boundary of this task.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        self.scope.tenant()
    }

    /// The materialized task, once it has been created.
    #[must_use]
    pub const fn task(&self) -> Option<&AgentTask> {
        self.task.as_ref()
    }

    /// Whether the task has been created.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.task.is_some()
    }

    /// The task's public status, once it has been created.
    #[must_use]
    pub fn status(&self) -> Option<AgentTaskStatus> {
        self.task.as_ref().map(|task| task.status)
    }

    /// The bounded log of resolved operations.
    #[must_use]
    pub const fn applied_operations(&self) -> &AgentTaskOperationLog {
        &self.applied_operations
    }

    /// The history entries the task owes its sink.
    #[must_use]
    pub fn pending_history(&self) -> &[AgentTaskHistoryEntry] {
        &self.pending_history
    }

    /// The time of the last accepted transition.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// The compact outcome describing the current state.
    #[must_use]
    pub fn outcome(&self) -> AgentTaskOutcome {
        let Some(task) = &self.task else {
            return AgentTaskOutcome {
                status: AgentTaskStatus::Created,
                assignment_generation: AgentAssignmentGeneration::UNASSIGNED,
                agent: None,
                run: None,
                rejection_count: 0,
                dependencies_satisfied: true,
                wake: None,
                goal: None,
                submission: None,
            };
        };
        AgentTaskOutcome {
            status: task.status,
            assignment_generation: task.assignment_generation,
            agent: task
                .assignment
                .as_ref()
                .map(|assignment| assignment.agent.clone()),
            run: task
                .assignment
                .as_ref()
                .map(|assignment| assignment.run.clone()),
            rejection_count: task.rejection_count,
            dependencies_satisfied: task.dependencies_satisfied(),
            wake: None,
            goal: task.goal_state.as_deref().map(|goal| AgentGoalOutcome {
                status: goal.status(),
                status_revision: goal.status_revision(),
            }),
            submission: None,
        }
    }

    /// A bounded, credential-free projection of this state.
    #[must_use]
    pub fn snapshot(&self) -> Option<AgentTaskSnapshot> {
        let task = self.task.as_ref()?;
        Some(AgentTaskSnapshot {
            scope: self.scope.clone(),
            definition_id: task.definition.definition_id.clone(),
            definition_version: task.definition.version,
            status: task.status,
            goal: task.goal.clone(),
            parent: task.parent.clone(),
            delegation: task.delegation.clone(),
            handoff: task.handoff.clone(),
            handoffs: task.handoffs,
            team: task.team.clone(),
            team_claim: task.team_claim.clone(),
            team_claims: task.team_claims,
            assignment: task.assignment.clone(),
            assignment_generation: task.assignment_generation,
            dependencies: task.dependencies.values().cloned().collect(),
            dependencies_satisfied: task.dependencies_satisfied(),
            rejection_count: task.rejection_count,
            last_rejection: task.last_rejection.clone(),
            last_refusal: task.last_refusal.clone(),
            accepted_result: task.accepted_result.clone(),
            terminal_reason: task.terminal_reason.clone(),
            cancellation: task.cancellation.clone(),
            outstanding_escrow: task.escrow.outstanding().count(),
            history_entries: self.next_history_sequence.get().saturating_sub(1),
            updated_at: self.updated_at,
            wake: task.goal_mode.continuous().map(|spec| {
                let controller = task.wake_controller.as_ref();
                AgentWakeStatusView {
                    schedule_revision: spec.schedule_revision,
                    policy_revision: spec.wake_policy.revision(),
                    active: controller
                        .map(|state| {
                            state
                                .active()
                                .iter()
                                .map(|active| active.binding().wake_id().clone())
                                .collect()
                        })
                        .unwrap_or_default(),
                    pending: controller
                        .map(|state| {
                            state
                                .pending()
                                .iter()
                                .map(|binding| binding.wake_id().clone())
                                .collect()
                        })
                        .unwrap_or_default(),
                    last_admitted: controller.and_then(|state| state.last_admitted().cloned()),
                    last_admitted_at: controller
                        .and_then(AgentWakeControllerState::last_admitted_at),
                    counters: controller
                        .map(|state| *state.counters())
                        .unwrap_or_default(),
                    window: controller.and_then(|state| state.window().copied()),
                    lifecycle: controller.map(|state| state.lifecycle().clone()),
                    epochs: controller
                        .map(|state| {
                            state
                                .active()
                                .iter()
                                .filter_map(|active| active.epoch().cloned())
                                .collect()
                        })
                        .unwrap_or_default(),
                }
            }),
            goal_state: task.goal_state.as_ref().map(|goal| AgentGoalStatusView {
                status: goal.status(),
                status_revision: goal.status_revision(),
                spec_revision: goal.spec().revision(),
                criteria_revision: goal.spec().spec().criteria.revision,
                evaluator: goal.spec().spec().evaluator.clone(),
                wait: goal.wait().cloned(),
                terminal: goal.terminal().map(|decision| decision.reason.clone()),
                coordinator: task
                    .assignment
                    .as_ref()
                    .map(|assignment| assignment.run.clone()),
                activated_at: goal.activated_at(),
                decided_at: goal.decided_at(),
            }),
        })
    }

    /// How many further history entries the task may record before its outbox is
    /// full.
    #[must_use]
    pub fn history_headroom(&self) -> usize {
        AGENT_TASK_PENDING_HISTORY_CAPACITY.saturating_sub(self.pending_history.len())
    }

    /// Records one history entry the task now owes its sink.
    ///
    /// It cannot drop an entry, because dropping one would silently lose audit
    /// history. It cannot fail either, and it does not have to: the entity
    /// guarantees [`AGENT_TASK_MAX_HISTORY_PER_TRANSITION`] entries of headroom
    /// before it runs any transition, and no transition records more than that.
    /// A backlog is refused at the entity's door — where the exchange can still be
    /// re-driven — rather than in here, where the only ways out would be losing an
    /// entry or turning a transient sink failure into a durable decision.
    fn record_history(
        &mut self,
        build: impl FnOnce(AgentTaskHistorySequence) -> AgentTaskHistoryEntry,
    ) {
        let sequence = self.next_history_sequence;
        self.next_history_sequence = sequence.next();
        self.pending_history.push(build(sequence));
    }

    /// Drops the entries a flush has durably appended.
    fn clear_flushed_history(&mut self, flushed: &[AgentTaskHistorySequence]) {
        self.pending_history
            .retain(|entry| !flushed.contains(&entry.sequence));
    }

    fn task_mut(&mut self) -> AgentTaskResult<&mut AgentTask> {
        self.task
            .as_mut()
            .ok_or_else(|| AgentTaskError::NotCreated {
                scope: self.scope.clone(),
            })
    }
}

impl AgentExchangeState for AgentTaskState {
    fn exchange_journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }

    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal {
        &mut self.journal
    }

    fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        policy.check_record(self)?;
        if let Some(task) = &self.task {
            policy.check_record(&task.definition)?;
            if let Some(spec) = task.goal_mode.continuous() {
                policy.check_record(&spec.wake_policy)?;
            }
            if let Some(goal) = &task.goal_state {
                policy.check_record(goal.spec())?;
            }
        }
        for entry in &self.pending_history {
            policy.check_record(entry)?;
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentTaskState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TaskState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, credential-free projection of one task's durable state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskSnapshot {
    /// The task's scope.
    pub scope: AgentTaskScope,
    /// Its definition identity.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition.
    pub definition_version: AgentRevisionNumber,
    /// Its public lifecycle status.
    pub status: AgentTaskStatus,
    /// The goal it contributes to.
    pub goal: Option<AgentGoalId>,
    /// The task that created it.
    pub parent: Option<AgentTaskId>,
    /// The delegation provenance recorded at creation, when a delegation
    /// created it. Snapshots persisted before this field load without one.
    #[serde(default)]
    pub delegation: Option<Box<crate::delegation::AgentTaskDelegationProvenance>>,
    /// The latest handoff, when one was recorded
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
    /// Snapshots persisted before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<Box<AgentTaskHandoff>>,
    /// How many handoffs the task has recorded over its lifetime.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub handoffs: u32,
    /// The team whose shared board governs the assignment, when one does
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    /// Snapshots persisted before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<crate::identity::AgentTeamId>,
    /// The latest board claim, when a team drove one. Snapshots persisted
    /// before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_claim: Option<Box<AgentTaskTeamClaim>>,
    /// How many board claims the task has recorded over its lifetime.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub team_claims: u32,
    /// Its current assignment.
    pub assignment: Option<AgentTaskAssignment>,
    /// The highest assignment generation it has decided.
    pub assignment_generation: AgentAssignmentGeneration,
    /// Its bounded dependency summary.
    pub dependencies: Vec<AgentTaskDependency>,
    /// Whether every dependency permits the task to be worked on.
    pub dependencies_satisfied: bool,
    /// How many result proposals its rules have refused.
    pub rejection_count: u32,
    /// The most recent rejection decision.
    pub last_rejection: Option<Box<AgentTaskRejection>>,
    /// The most recent assignment refusal.
    pub last_refusal: Option<AgentAssignmentRefusal>,
    /// Its accepted typed result.
    pub accepted_result: Option<Box<AgentAcceptedResult>>,
    /// Why it reached its terminal status.
    pub terminal_reason: Option<AgentTaskTerminalReason>,
    /// The nonterminal cancellation request it carries while its wind-down
    /// propagates ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    /// Snapshots persisted before this field load without one.
    #[serde(default)]
    pub cancellation: Option<Box<AgentTaskCancellation>>,
    /// How many escrow children the task still holds open: every live
    /// generation and admitted epoch opens one at the transition that decided
    /// it, and it closes only when that child's run settles and returns.
    ///
    /// This is the finalization gate a requested cancellation waits on
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)) — and it
    /// is the reason the count is projected rather than inferred from
    /// [`Self::assignment`]: a continuous root between epoch assignments holds
    /// no assignment while its admitted epochs are still executing.
    /// Snapshots persisted before this field load as zero.
    #[serde(default)]
    pub outstanding_escrow: usize,
    /// How many history entries it has produced. The entries themselves are
    /// read through [`AgentTaskHistoryStore::read`].
    pub history_entries: u64,
    /// The time of its last accepted transition.
    pub updated_at: AgentTimestampMillis,
    /// The continuous goal's wake state, when the task coordinates one.
    /// Snapshots persisted before this field load without it.
    #[serde(default)]
    pub wake: Option<AgentWakeStatusView>,
    /// The goal contract's status, when this task is the goal's root
    /// coordinator. Snapshots persisted before this field load without it.
    #[serde(default)]
    pub goal_state: Option<AgentGoalStatusView>,
}

/// Loads one task's durable state without waking its entity.
///
/// This is the authoritative point read of
/// [specification 17.18](../../../docs/plans/rakka-agent/spec.md): it is correct
/// while the task is passivated and while telemetry is unavailable, because it
/// reads the same record the entity transitions. The schema check applies here
/// too, so a stale reader fails closed rather than projecting a record it cannot
/// interpret.
pub async fn load_agent_task_state<Store>(
    store: &Store,
    scope: &AgentTaskScope,
    policy: &AgentSchemaPolicy,
) -> AgentTaskResult<Option<AgentTaskState>>
where
    Store: DurableStateStore<AgentTaskState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(record.state))
}

/// Derives the run that serves one assignment generation.
///
/// The identity is *derived*, not generated, so replaying an assignment resolves
/// to the same run rather than creating a second one — which is what
/// "deduplicated by task id + assignment generation"
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)) requires of
/// the run-creation command. The derivation cannot exceed the identity bound
/// for a task the entity admitted: creation caps an agent-owned task's id at
/// [`AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH`], which reserves room for the
/// longest possible suffix.
pub fn run_id_for_assignment(
    task: &AgentTaskId,
    generation: AgentAssignmentGeneration,
) -> Result<AgentRunId, AgentIdentityError> {
    AgentRunId::new(format!("{task}-gen-{generation}"))
}

/// Derives the stable operation id of one assignment's run-creation command.
pub fn assignment_operation_id(
    scope: &AgentTaskScope,
    generation: AgentAssignmentGeneration,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::RunCreation,
        [
            scope.tenant().as_str(),
            scope.task().as_str(),
            &generation.to_string(),
        ],
    )
}

/// Derives the stable operation id of the one run-cancel exchange a task ever
/// owes one assignment generation.
///
/// Pure over `(scope, generation)`: the cancellation marker is absorbing and
/// a generation is assigned at most one run, so one logical request exists
/// per generation, ever, and every re-drive after any loss owes the identical
/// operation ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
pub fn run_cancel_operation_id(
    scope: &AgentTaskScope,
    generation: AgentAssignmentGeneration,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::Cancellation,
        [
            scope.tenant().as_str(),
            scope.task().as_str(),
            "run-cancel",
            &generation.to_string(),
        ],
    )
}

/// Derives the stable operation id of one authenticated human-result
/// submission ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
///
/// Pure over `(tenant, task, discriminator)`: a retried submission re-derives
/// the identical operation and converges on the original decision — a
/// recorded rejection included — while a corrected resubmission after a
/// rejection carries a new discriminator and is a new logical operation.
pub fn human_result_operation_id(
    tenant: &TenantId,
    task: &AgentTaskId,
    discriminator: &str,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::ResultSubmission,
        [tenant.as_str(), task.as_str(), discriminator],
    )
}

/// Derives the stable operation id of the one dependency registration a
/// dependent ever owes one upstream.
///
/// Pure over `(tenant, upstream, dependent)`: an edge's policy is immutable
/// once declared — a conflicting redeclaration fails closed — so one logical
/// registration exists per edge, ever, and every re-drive after any loss
/// owes the identical operation.
pub fn dependency_registration_operation_id(
    tenant: &TenantId,
    upstream: &AgentTaskId,
    dependent: &AgentTaskId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::DependencyRegistration,
        [tenant.as_str(), upstream.as_str(), dependent.as_str()],
    )
}

/// Derives the stable operation id of the one dependency-outcome
/// notification an upstream ever owes one registered dependent.
///
/// Pure over `(tenant, upstream, dependent)`: a dependency resolves exactly
/// once — a conflicting second outcome fails closed as a conflict, never a
/// correction — so one logical notification exists per edge, ever.
pub fn dependency_outcome_operation_id(
    tenant: &TenantId,
    upstream: &AgentTaskId,
    dependent: &AgentTaskId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::DependencyOutcome,
        [tenant.as_str(), upstream.as_str(), dependent.as_str()],
    )
}

/// The granted delegation ceilings min-narrowed to the definition's own
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): parent and
/// definition ceilings enforce at allocation and admission time). `None` on
/// either side is the unbounded identity.
fn narrowed_delegation_budget(
    granted: Option<AgentGoalDelegationBudget>,
    definition: Option<AgentGoalDelegationBudget>,
) -> Option<AgentGoalDelegationBudget> {
    match (granted, definition) {
        (Some(granted), Some(definition)) => Some(granted.narrowed_to(&definition)),
        (granted, definition) => granted.or(definition),
    }
}

/// The delegation authority one assignment carries to its run.
///
/// A goal-bearing root projects its spec's skill and tool narrowing, its
/// delegation ceilings, and its deadline; a delegated child projects its own
/// creation provenance — the full ancestor chain including the delegation
/// that created it, at the depth the parent recorded, with the ancestor-agent
/// chain extended by the delegating parent's own agent so the run's cycle
/// check sees the whole chain. Either way the ceilings are min-narrowed to
/// the task definition's own, which is the cap no provenance can escape. A
/// task that is neither — an epoch task, or a plain agent-owned task —
/// carries no goal narrowing and no chain, but still carries its
/// definition's own ceilings when the definition declares any: without an
/// envelope the delegation door would enforce nothing for these runs.
fn delegation_envelope_for(
    task: &AgentTask,
) -> Option<Box<crate::delegation::AgentRunDelegationEnvelope>> {
    let mut envelope = delegation_envelope_base(task);
    // The handoff post-step ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)):
    // a transfer preserves the task's own lineage, depth, and narrowing —
    // the same task is the same node in the tree, so no fourth branch — but
    // the target's communal grant is the task's own statement intersected
    // with what the catalog explicitly delegates to that target, so a
    // handoff can never widen communal access. Ancestors deliberately do not
    // gain the source agent: the chains stay parallel to the lineage, and
    // returning a task to a previous agent is bounded by the handoff limit,
    // not refused structurally.
    if let Some(handoff) = task
        .handoff
        .as_deref()
        .filter(|handoff| !matches!(handoff.status, AgentTaskHandoffStatus::Refused { .. }))
    {
        if let Some(env) = envelope.as_deref_mut() {
            env.knowledge_spaces = Some(match env.knowledge_spaces.take() {
                Some(granted) => handoff
                    .knowledge_spaces
                    .intersection(&granted)
                    .cloned()
                    .collect(),
                // A chain without its own grant statement cannot prove what
                // it may pass on: deny-when-unknown, the ancestry-gap
                // posture the delegation door takes.
                None if !env.lineage.is_empty() => BTreeSet::new(),
                // At the root the definition envelope still bounds every
                // append, and the catalog's explicit statement becomes the
                // grant — the delegation door's root posture.
                None => handoff.knowledge_spaces.clone(),
            });
        }
    }
    envelope
}

/// The base envelope of [`delegation_envelope_for`]: the three creation-shape
/// arms, before the handoff post-step.
fn delegation_envelope_base(
    task: &AgentTask,
) -> Option<Box<crate::delegation::AgentRunDelegationEnvelope>> {
    if let Some(goal_state) = task.goal_state.as_deref() {
        let spec = goal_state.spec().spec();
        return Some(Box::new(crate::delegation::AgentRunDelegationEnvelope {
            allowed_skills: spec.allowed_skills.clone(),
            allowed_tools: spec.allowed_tools.clone(),
            allowed_workflows: spec.allowed_workflows.clone(),
            budget: narrowed_delegation_budget(spec.delegation, task.definition.delegation),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 0,
            deadline: spec.deadline,
            fan_in: spec.fan_in,
            environments: spec.environments.clone(),
            // The goal spec's set has no declaredness of its own — empty
            // means no goal narrowing, so the root's grant statement is the
            // set when one exists and no statement otherwise.
            knowledge_spaces: (!spec.knowledge_spaces.is_empty())
                .then(|| spec.knowledge_spaces.clone()),
        }));
    }
    if let Some(provenance) = task.delegation.as_deref() {
        let mut lineage = provenance.lineage.clone();
        lineage.push(provenance.delegation.clone());
        // The ancestry stays parallel to the lineage: the delegating parent
        // committed `provenance.delegation`, so its agent closes the chain.
        // A provenance whose own ancestry predates the field carries lineage
        // without agents; the envelope then carries the same gap, and the
        // run's cycle check refuses to extend it rather than trusting it.
        let mut ancestors = provenance.ancestors.clone();
        if !ancestors.is_empty() || provenance.lineage.is_empty() {
            ancestors.push(provenance.parent_run.agent().clone());
        }
        return Some(Box::new(crate::delegation::AgentRunDelegationEnvelope {
            allowed_skills: BTreeSet::new(),
            allowed_tools: BTreeSet::new(),
            allowed_workflows: BTreeSet::new(),
            budget: narrowed_delegation_budget(provenance.budget, task.definition.delegation),
            lineage,
            ancestors,
            depth: provenance.depth,
            deadline: provenance.deadline,
            // A delegated child's own fan-out groups open under its wiring's
            // default: the parent's policy governs the parent's group, not
            // the child's.
            fan_in: None,
            environments: provenance.environments.clone(),
            // A delegated child's grant statement is always explicit: the
            // provenance's set, which decodes empty — no communal access —
            // for a chain recorded before the field existed.
            knowledge_spaces: Some(provenance.knowledge_spaces.clone()),
        }));
    }
    // The definition is the cap no creation shape escapes: a run with no
    // envelope at all would enforce no depth, fan-out, or concurrency
    // ceiling at its delegation door. Root position — no chain, no
    // narrowing, no deadline — and its fan-out groups open under the
    // wiring's default.
    task.definition.delegation.map(|ceilings| {
        Box::new(crate::delegation::AgentRunDelegationEnvelope {
            allowed_skills: BTreeSet::new(),
            allowed_tools: BTreeSet::new(),
            allowed_workflows: BTreeSet::new(),
            budget: Some(ceilings),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 0,
            deadline: None,
            fan_in: None,
            environments: BTreeSet::new(),
            // A plain or epoch task carries no goal scope and no chain: no
            // grant statement, and the definition envelope governs.
            knowledge_spaces: None,
        })
    })
}

/// Creates the task, or fails closed.
///
/// It is the one transition both creation paths reach: the ingress
/// [`AgentTaskEntityCommand::Create`] and the delegating run's
/// [`AgentExchangeKind::Creation`] exchange.
fn create_task(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    creation: AgentTaskCreation,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTaskOutcome> {
    if state.task.is_some() {
        // The domain fence. A replay inside the deduplication window never
        // reaches here, and one that has aged out of it is still refused.
        return Err(AgentTaskError::AlreadyCreated {
            scope: state.scope.clone(),
        });
    }

    // The definition's fields are public, so its bounded invariants cannot be
    // assumed from construction; they are re-checked before anything is
    // persisted.
    creation.definition.validate()?;
    creation.input.validate()?;
    if let Some(provenance) = creation.delegation.as_deref() {
        provenance
            .validate()
            .map_err(|error| AgentTaskError::DelegationProvenanceInvalid {
                message: error.to_string(),
            })?;
        // A delegated child lives in its parent's tenant: the delegating
        // executor always creates the child under the parent run's tenant,
        // so a provenance naming a foreign one is a forgery or a
        // misrouting — refused, never recorded for the enforcement slices
        // to trust.
        if provenance.parent_run.tenant() != state.scope.tenant() {
            return Err(AgentTaskError::DelegationProvenanceInvalid {
                message: format!(
                    "the provenance names a parent run in tenant {}, but the task is created in \
                     tenant {}",
                    provenance.parent_run.tenant(),
                    state.scope.tenant()
                ),
            });
        }
        // A delegated child cannot institute a goal of its own: a goal record
        // would win the run's delegation envelope and re-root the chain —
        // empty lineage, empty ancestors, depth zero — so the ceilings and
        // the cycle set the parent committed would vanish from every door
        // check. One creation shape carries one authority chain.
        if creation.goal_spec.is_some() {
            return Err(AgentTaskError::DelegationProvenanceInvalid {
                message: "a creation carrying delegation provenance cannot also carry a goal \
                          spec: a delegated child contributes to its parent's goal and may not \
                          re-root the delegation chain"
                    .to_string(),
            });
        }
    }

    let mut dependencies = BTreeMap::new();
    for declaration in &creation.dependencies {
        declaration.validate(state.scope.task())?;
        if let Some(existing) = dependencies.insert(
            declaration.dependency.clone(),
            AgentTaskDependency {
                dependency: declaration.dependency.clone(),
                policy: declaration.policy,
                outcome: None,
                declared_by: operation_id.clone(),
                declared_at: now,
                registration_settled: false,
            },
        ) {
            // The same rule as a post-creation redeclaration: repeating an edge
            // is idempotent, and it may not silently change the failure policy
            // the dependent committed to.
            if existing.policy != declaration.policy {
                return Err(AgentTaskError::DependencyConflict {
                    dependency: declaration.dependency.clone(),
                });
            }
        }
    }

    // The limit counts declared edges, not declarations, exactly as
    // `declare_dependency` does after creation.
    if dependencies.len() > creation.definition.limits.max_dependencies {
        return Err(AgentTaskError::DependencyLimitExceeded {
            maximum: creation.definition.limits.max_dependencies,
        });
    }

    // A creation carrying a goal spec institutes the goal on this task, and
    // the binding defaults to the task's own id — open decision 14's resolved
    // default ([specification 6.3](../../../docs/plans/rakka-agent/spec.md)).
    // An explicit binding simply names the goal; the types stay distinct.
    let goal = creation.goal.or_else(|| {
        creation
            .goal_spec
            .is_some()
            .then(|| AgentGoalId::for_root_task(state.scope.task()))
    });

    if creation.goal_mode.is_continuous() && goal.is_none() {
        // A continuous root control task exists to admit epochs for a goal;
        // without the goal binding there is nothing for the wake controller
        // to fence, budget, or retire against.
        return Err(AgentTaskError::ContinuousWithoutGoal);
    }

    let agent_owned = creation.definition.is_agent_owned();
    // A board-governed creation may defer its assignee: the task waits
    // unassigned — no generation ever mints without an assignee — until a
    // team claim names one ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    // The wait is bounded by the definition's unclaimed horizon, observed by
    // the settle pass: a team that never claims expires the task instead of
    // parking it silently forever.
    if agent_owned && creation.assignee.is_none() && creation.team.is_none() {
        return Err(AgentTaskError::MissingAssignee);
    }
    if agent_owned && state.scope.task().as_str().len() > AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH {
        // Every assignment derives a run id from this id plus a generation
        // suffix; an id the suffix would push past the identity bound is
        // refused here, where the caller can still choose another, rather than
        // at decision time, where the task would be created and unassignable.
        return Err(AgentTaskError::TaskIdTooLong {
            length: state.scope.task().as_str().len(),
            maximum: AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH,
        });
    }

    let status = if !dependencies.is_empty() {
        AgentTaskStatus::Blocked
    } else if agent_owned {
        AgentTaskStatus::Created
    } else {
        // A human-owned task is never assigned to an agent; it waits for an
        // authenticated completion through the same typed validation path.
        AgentTaskStatus::WaitingForInput
    };

    // A creation that rides a parent's transition carries the escrow that
    // parent already debited — the epoch creation the wake controller owes,
    // and later the delegated creations of phase 4. A root creation with no
    // parent scope to debit is escrowed exactly its ceilings. A goal-bearing
    // root holds the goal's own allocation instead, narrowed to that base —
    // the definition-ceiling → goal-allocation → task rung of
    // ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)): the
    // goal may hold less than the definition permits, never more.
    let base_grant = match creation.escrow {
        Some(grant) => grant,
        None => AgentBudgetGrant::from_ceilings(&creation.definition.budgets),
    };
    let mut seed = match &creation.goal_spec {
        Some(draft) => {
            AgentBudgetGrant::new(draft.spec.allocation, draft.spec.limits).narrowed_to(&base_grant)
        }
        None => base_grant,
    };
    // The conserved descendants seed: the escrow this task holds in the
    // descendants dimension is min-narrowed to every delegation ceiling in
    // scope — the definition's own, the goal spec's at a root, and the
    // parent-granted provenance budget at a delegated child. The provenance
    // is a validated cap, never a conserved grant riding A2A: the ledger
    // builds from the task's own ceilings, and a peer can only shrink it.
    let descendants_ceiling = [
        creation
            .definition
            .delegation
            .and_then(|d| d.max_descendants),
        creation
            .goal_spec
            .as_ref()
            .and_then(|draft| draft.spec.delegation.and_then(|d| d.max_descendants)),
        creation
            .delegation
            .as_deref()
            .and_then(|p| p.budget.and_then(|b| b.max_descendants)),
    ]
    .into_iter()
    .flatten()
    .map(u64::from)
    .min();
    if let Some(ceiling) = descendants_ceiling {
        let bounded = seed
            .allocation
            .descendants
            .map_or(ceiling, |held| held.min(ceiling));
        seed.allocation.descendants = Some(bounded);
    }
    let escrow = AgentEscrowLedger::new(seed);

    if creation.wake.is_some() && creation.parent.is_none() {
        // An epoch task exists to return its result to the controller that
        // admitted its wake; without the parent binding there is no
        // controller to return it to.
        return Err(AgentTaskError::EpochWithoutParent);
    }

    // The goal record is instituted in the creating transition, so the goal
    // exists from its first commit and there is no window in which the root
    // coordinates a contract it does not hold. Validation runs inside
    // `AgentGoalSpecRevision::initial`, so a spec that violates a bound
    // refuses the whole creation.
    let goal_state = match creation.goal_spec {
        Some(draft) => {
            let revision = AgentGoalSpecRevision::initial(draft.spec, draft.provenance)?;
            Some(Box::new(AgentGoalState::new(
                revision,
                draft.activate_on_creation,
                now,
            )))
        }
        None => None,
    };
    let goal_active = goal_state
        .as_ref()
        .is_some_and(|goal| goal.status().permits_work());

    let wake_controller = creation
        .goal_mode
        .is_continuous()
        .then(AgentWakeControllerState::new);
    let mut task = AgentTask {
        definition: creation.definition,
        input: creation.input,
        status,
        goal,
        goal_mode: creation.goal_mode,
        goal_state,
        wake_controller,
        wake: creation.wake,
        parent: creation.parent,
        delegation: creation.delegation,
        assignee: creation.assignee,
        dependencies,
        dependents: BTreeMap::new(),
        escrow,
        assignment: None,
        assignment_generation: AgentAssignmentGeneration::UNASSIGNED,
        assignments: 0,
        handoff: None,
        handoffs: 0,
        team: creation.team,
        team_claim: None,
        team_claims: 0,
        team_claim_fence: 0,
        last_refusal: None,
        accepted_result: None,
        rejection_count: 0,
        last_rejection: None,
        rejected_submissions: Vec::new(),
        terminal_reason: None,
        cancellation: None,
        created_at: now,
        telemetry: crate::observability::sanitize_agent_telemetry_context(creation.telemetry),
    };
    // A continuous root instituted with a `Proposed` goal parks admission
    // until the goal is activated: a proposed goal spends nothing, and the
    // gate's own coalescing is what holds triggers that arrive meanwhile.
    if let (Some(goal_record), Some(controller)) =
        (task.goal_state.as_deref(), task.wake_controller.as_mut())
    {
        if goal_record.status() == AgentGoalStatus::Proposed {
            controller.suspend_by_policy(GOAL_PROPOSED_GATE_REASON);
        }
    }
    // Admission reserves growth headroom: a record accepted here must still be
    // able to hold everything its own lifecycle may add.
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;

    state.task = Some(task);
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::Created,
            operation_id.clone(),
            status,
            now,
        )
    });
    if goal_active {
        // A goal activated in the creating transition records its activation
        // exactly as a commanded activation would, so history reads the same
        // whichever door activated it.
        state.record_history(|sequence| {
            AgentTaskHistoryEntry::new(
                sequence,
                AgentTaskHistoryKind::GoalActivated,
                operation_id.clone(),
                status,
                now,
            )
        });
    }
    // Each declared edge is its own history row, so scenario 37's "one
    // dependency edge" is observable in history as well as in state.
    let declared: Vec<AgentTaskId> = state
        .task
        .as_ref()
        .expect("the task was just created")
        .dependencies
        .keys()
        .cloned()
        .collect();
    for dependency in declared {
        state.record_history(|sequence| {
            AgentTaskHistoryEntry::new(
                sequence,
                AgentTaskHistoryKind::DependencyDeclared,
                operation_id.clone(),
                status,
                now,
            )
            .with_detail(dependency.to_string())
        });
    }

    Ok(state.outcome())
}

/// Declares one dependency edge after creation.
fn declare_dependency(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    declaration: &AgentTaskDependencyDeclaration,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTaskOutcome> {
    let scope_task = state.scope.task().clone();
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    declaration.validate(&scope_task)?;

    if let Some(existing) = task.dependencies.get(&declaration.dependency) {
        // The edge exists. Redeclaring it is idempotent, and it may not silently
        // change the policy a dependent already committed to.
        if existing.policy != declaration.policy {
            return Err(AgentTaskError::DependencyConflict {
                dependency: declaration.dependency.clone(),
            });
        }
        return Ok(state.outcome());
    }

    if task.dependencies.len() >= task.definition.limits.max_dependencies {
        return Err(AgentTaskError::DependencyLimitExceeded {
            maximum: task.definition.limits.max_dependencies,
        });
    }

    task.dependencies.insert(
        declaration.dependency.clone(),
        AgentTaskDependency {
            dependency: declaration.dependency.clone(),
            policy: declaration.policy,
            outcome: None,
            declared_by: operation_id.clone(),
            declared_at: now,
            registration_settled: false,
        },
    );
    // A task that has become dependent again is no longer eligible.
    if matches!(task.status, AgentTaskStatus::Created) {
        task.status = AgentTaskStatus::Blocked;
    }
    // A late edge grows the admitted record, so it keeps the same growth
    // headroom the creation reserved.
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;

    let status = task.status;
    let detail = declaration.dependency.to_string();
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::DependencyDeclared,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(detail)
    });
    Ok(state.outcome())
}

/// Records how one dependency resolved, and propagates its policy.
///
/// The default failed-dependency policy cancels the dependent
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)). The command
/// is the receiving half of the propagation; the sending half — an upstream task
/// notifying its dependents when it goes terminal — needs a durable dependents
/// registry and lands with slice 5.4's human-owned tasks, the first flow that
/// builds cross-task dependency graphs. Slice 4.6's propagation covers the
/// delegation and workflow trees, whose parent-child edges the run's cells
/// already record.
///
/// A cancelling dependency takes the *request* path, never a direct
/// terminalization ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)):
/// a dependency can resolve while this task's run is mid-flight, and a task
/// that terminalized here would project terminal `Cancelled` over a started
/// consequential effect whose outcome is unknown, strand its escrow, and
/// leave its run to discover the cancellation only if it ever proposes.
/// The request trades fail-fast for honesty: a dependent with in-flight work
/// stays `Cancelling` until its subtree quiesces, and that wait's liveness
/// rests on the doors every cancellation rests on — effect reconciliation
/// for a stuck consequential effect, and the hosting application's
/// workflow-tool result relay. A dependent with a closed ledger still
/// terminalizes in this same compare-and-set, through the request's own
/// finalization. Returns whatever the transition owes.
fn record_dependency_outcome(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    dependency: &AgentTaskId,
    outcome: AgentTaskDependencyOutcome,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }

    let edge =
        task.dependencies
            .get_mut(dependency)
            .ok_or_else(|| AgentTaskError::UnknownDependency {
                dependency: dependency.clone(),
            })?;

    match edge.outcome {
        // A dependency resolves once. A replayed outcome is idempotent; a
        // *different* outcome for a resolved dependency is a conflict, not a
        // correction, and it fails closed.
        Some(existing) if existing == outcome => return Ok(Vec::new()),
        Some(_) => {
            return Err(AgentTaskError::DependencyConflict {
                dependency: dependency.clone(),
            })
        }
        None => edge.outcome = Some(outcome),
    }

    let cancelling = task
        .cancelling_dependency()
        .map(|edge| (edge.dependency.clone(), edge.outcome));

    if let Some((dependency, Some(outcome))) = cancelling {
        return request_task_cancellation(
            state,
            operation_id,
            AgentTaskTerminalReason::DependencyNotSatisfied {
                dependency,
                outcome,
            },
            now,
        );
    }

    let task = state.task_mut()?;
    if task.dependencies_satisfied() && matches!(task.status, AgentTaskStatus::Blocked) {
        // Eligible at last. The entity's next pass decides the assignment; it
        // cannot happen here, because the decision needs the agent's durable
        // state and reading it is I/O.
        task.status = if task.definition.is_agent_owned() {
            AgentTaskStatus::Created
        } else {
            AgentTaskStatus::WaitingForInput
        };
    }

    let status = task.status;
    let detail = format!("{dependency} {outcome}");
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::DependencyResolved,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(detail)
    });
    Ok(Vec::new())
}

/// Decides the goal a root task holds under the projection its terminal
/// reason implies, and closes admission with it, when the goal is still
/// undecided. Returns the history detail of the decision it made.
///
/// A terminal root coordinator ends the goal it holds: nothing can drive
/// the contract further. A completion deliberately does not — completion
/// is evidence, and only the configured evaluator makes a goal `Satisfied`
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)); the
/// goal's own budget termination already decided itself. A cancellation
/// request runs this projection at *request* time
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)), so the
/// later finalizing [`terminate`] finds the goal terminal and skips it.
fn project_goal_decision(
    task: &mut AgentTask,
    reason: &AgentTaskTerminalReason,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<String>> {
    let goal_reason = match reason {
        AgentTaskTerminalReason::ResultAccepted => None,
        AgentTaskTerminalReason::CancellationRequested { .. }
        | AgentTaskTerminalReason::DependencyNotSatisfied { .. } => {
            Some(AgentGoalTerminalReason::RootTaskCancelled)
        }
        AgentTaskTerminalReason::GoalBudgetExhausted { exhaustion } => {
            Some(AgentGoalTerminalReason::BudgetExhausted {
                exhaustion: *exhaustion,
            })
        }
        // Symmetry with the budget arm; in practice the stagnation executor
        // decided the goal before terminating the task, so the non-terminal
        // guard below skips this projection.
        AgentTaskTerminalReason::GoalStagnant { trigger, epochs } => {
            Some(AgentGoalTerminalReason::Stagnant {
                trigger: *trigger,
                epochs: *epochs,
            })
        }
        other => Some(AgentGoalTerminalReason::ExecutionFailed {
            code: other.code().to_string(),
        }),
    };
    let row = match (goal_reason, task.goal_state.as_deref_mut()) {
        (Some(goal_reason), Some(goal)) if !goal.status().is_terminal() => {
            let detail = format!("{} {}", goal_reason.status().as_label(), goal_reason.code());
            goal.decide(
                AgentGoalDecision {
                    reason: goal_reason,
                    evaluation: None,
                    provenance: None,
                    expected_status_revision: goal.status_revision(),
                },
                now,
            )?;
            // Admission closes with the contract.
            if let Some(controller) = task.wake_controller.as_mut() {
                controller.retire_by_policy();
            }
            Some(detail)
        }
        _ => None,
    };
    Ok(row)
}

/// Records a durable, nonterminal cancellation request
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)) and returns
/// the exchanges it owes now.
///
/// In one compare-and-set: the absorbing marker is set with the terminal
/// reason finalization will record, the goal the task holds is decided and
/// its admission closed, and the propagation the current state permits is
/// owed — the run-cancel exchange when a run has durably accepted, or the
/// immediate finalization when no generation is live. An offered assignment
/// owes its run-cancel at the acceptance settle instead, so a cancellation
/// can never definitively refuse ahead of an in-flight assignment.
///
/// Absorbing: a request that finds the marker set is answered without a
/// second transition. A request that finds the task terminal is refused
/// exactly as the terminal transition it replays would be.
fn request_task_cancellation(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    reason: AgentTaskTerminalReason,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    if task.cancellation.is_some() {
        return Ok(Vec::new());
    }
    task.cancellation = Some(Box::new(AgentTaskCancellation {
        reason: reason.clone(),
        requested_at: now,
    }));
    // The goal decision and admission close at request time: acceptance of a
    // cancellation request immediately fences new dispatch for the scope.
    let goal_row = project_goal_decision(task, &reason, now)?;

    let status = task.status;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::CancellationRequested,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(reason.code())
    });
    if let Some(detail) = goal_row {
        record_wake_history(
            state,
            AgentTaskHistoryKind::GoalDecided,
            operation_id,
            detail,
            now,
        );
    }

    // A pending handoff whose target generation was never minted resolves
    // refused here, in the same compare-and-set as the marker: the restored
    // source assignment is the accepted one the run-cancel below reaches,
    // and the source's fence releases into its wind-down through the owed
    // handoff result. A minted, still-offered generation is left to its
    // Assignment settle — acceptance routes the cancel to the target and the
    // result to the source; refusal restores the source exactly as here
    // ([specification 8.7 and 8.9](../../../docs/plans/rakka-agent/spec.md)).
    let unminted_handoff = state.task.as_ref().is_some_and(|task| {
        task.handoff
            .as_deref()
            .is_some_and(|handoff| !handoff.is_settled() && handoff.target_generation.is_none())
    });
    let mut owed: Vec<AgentExchangeEnvelope> = Vec::new();
    if unminted_handoff {
        owed.extend(resolve_handoff_refusal(
            state,
            AGENT_TASK_REFUSAL_CANCEL_REQUESTED,
            now,
        )?);
    }
    // A pending board claim whose generation was never minted resolves
    // refused in the same compare-and-set as the marker, exactly as the
    // handoff does: the owed claim result reopens the board entry under the
    // cancellation code. A minted, still-offered generation is left to its
    // Assignment settle.
    let unminted_claim = state.task.as_ref().is_some_and(|task| {
        task.team_claim
            .as_deref()
            .is_some_and(|claim| !claim.is_settled() && claim.target_generation.is_none())
    });
    if unminted_claim {
        owed.extend(resolve_team_claim_refusal(
            state,
            "team-claim-task-cancelling",
            now,
        )?);
    }
    owed.extend(owed_run_cancel(state, now)?);
    owed.extend(finalize_task_cancellation(state, operation_id, now)?);
    Ok(owed)
}

/// The run-cancel exchange the task owes its accepted assignment, when it
/// owes one now.
///
/// Owed exactly once per generation, and only once the run durably accepted:
/// an offered assignment's run may not exist yet, and a definitive
/// `run-cancel-unassigned` refusal against it would end the propagation the
/// acceptance still owes. The journal's initiation record is the once-guard.
fn owed_run_cancel(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(None);
    };
    let Some(cancellation) = task.cancellation.as_deref() else {
        return Ok(None);
    };
    if task.status.is_terminal() {
        return Ok(None);
    }
    let Some(assignment) = task.assignment.as_ref() else {
        return Ok(None);
    };
    if assignment.status != AgentAssignmentStatus::Accepted {
        return Ok(None);
    }
    let operation_id = run_cancel_operation_id(&state.scope, assignment.generation)?;
    if state.journal.has_initiated(&operation_id) {
        return Ok(None);
    }
    let run_scope = AgentRunScope::new(
        state.scope.tenant().clone(),
        assignment.agent.clone(),
        assignment.run.clone(),
    )?;
    let request = AgentRunCancelRequest {
        task: state.scope.clone(),
        generation: assignment.generation,
        reason: cancellation.detail(),
    };
    let payload = AgentExchangePayload::encode(AGENT_RUN_CANCEL_PAYLOAD_TYPE, &request)?;
    Ok(Some(
        AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::RunCancel,
            AgentEntityAddress::Task(state.scope.clone()),
            AgentEntityAddress::Run(run_scope),
            payload,
            AgentCorrelationId::new(operation_id.as_str()),
            now,
        )?
        .with_telemetry(task.telemetry.clone()),
    ))
}

/// Finalizes a requested cancellation once the task's ledger proves its work
/// quiescent, and returns the terminal reports the finalization owes.
///
/// The gate is escrow closure: every live generation and admitted epoch holds
/// an open escrow child from the transition that decided it, a refused
/// generation releases at its settle, and a terminal run's settlement and
/// return close its child — budget settlement travels only after a known
/// terminal outcome, so a closed ledger is durable proof no assigned work is
/// still running or ambiguous
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md): terminal
/// `Cancelled` only after quiescence, with every started effect's outcome
/// known or explicitly reconciled — an unresolved run rests in its own
/// `WaitingForReconciliation`, holding its escrow open and this gate closed).
fn finalize_task_cancellation(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(cancellation) = task.cancellation.as_deref() else {
        return Ok(Vec::new());
    };
    if task.status.is_terminal() {
        return Ok(Vec::new());
    }
    if task.escrow.outstanding().count() > 0 {
        return Ok(Vec::new());
    }
    let reason = cancellation.reason.clone();
    terminate(state, operation_id, reason, now)?;
    owed_child_reports(state, now, 0)
}

/// Moves the task to a terminal status and records why.
///
/// The direct terminalization: it is what the *finalizing* transitions call
/// once nothing is owed — an accepted result, an exhausted assignment budget,
/// a closed-ledger cancellation. An ingress that can fire while a run is
/// mid-flight takes [`request_task_cancellation`] instead, so the task never
/// projects a terminal status over work whose outcome is unknown
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
fn terminate(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    reason: AgentTaskTerminalReason,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    task.status = reason.status();
    task.terminal_reason = Some(reason.clone());
    // A terminal task fences its run: the assignment is retired, so a late
    // proposal from it can no longer be validated.
    task.assignment = None;
    let goal_row = project_goal_decision(task, &reason, now)?;

    let status = task.status;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::Terminated,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(reason.code())
    });
    if let Some(detail) = goal_row {
        record_wake_history(
            state,
            AgentTaskHistoryKind::GoalDecided,
            operation_id,
            detail,
            now,
        );
    }
    // No upstream outcome can move a terminal task, so the registrations its
    // forward edges still owe are moot. They are withdrawn in this same
    // compare-and-set, which is what leaves the journal room for the outcome
    // notifications this terminal now owes its own dependents.
    withdraw_moot_registrations(state);
    Ok(())
}

/// Whether the task carries an unresolved handoff
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
fn task_handoff_pending(task: &AgentTask) -> bool {
    task.handoff
        .as_deref()
        .is_some_and(|handoff| !handoff.is_settled())
}

/// Records one handoff: validates the claim against durable state, stashes
/// the source assignment whole, and points the task at the target — the
/// same-task transfer of [specification 8.9](../../../docs/plans/rakka-agent/spec.md).
///
/// Atomically validate-then-mutate: a refusal leaves the task exactly as it
/// found it, which is what lets the source treat a definitive refusal as
/// proof no transfer was recorded. A replay matching the recorded handoff id
/// accepts idempotently — the materialized provenance is the deduplication
/// echo past the journal's bounded window.
fn record_handoff(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    request: &AgentTaskHandoffRequest,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    if let Some(existing) = task.handoff.as_deref() {
        if existing.handoff == request.handoff {
            // First-writer-wins echo: the transfer is already recorded, and
            // the reply carries the recorded outcome rather than minting a
            // second one. Checked before every guard — including the
            // terminal one — because this is the deduplication echo past the
            // journal's bounded window: a re-dispatched send replaying after
            // the target completed the task must converge on the recorded
            // transfer, not be refused as if none was recorded.
            return Ok(());
        }
    }
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    if let Some(existing) = task.handoff.as_deref() {
        if !existing.is_settled() {
            return Err(AgentTaskError::HandoffRefused {
                code: "handoff-conflict",
                message: format!(
                    "the task already carries the unresolved handoff {}",
                    existing.handoff
                ),
            });
        }
    }
    // The structural re-validation the request's contract promises ("every
    // field is a claim the transition re-validates"): the wire's context
    // projection re-passes the sender-side reference bounds, and the whole
    // claim re-passes the record's serialized ceiling, so an oversized
    // cluster can never consume the task's bounded materialized headroom and
    // wedge later transitions that need it.
    if let Err(error) = crate::coordination::check_context_refs(&request.context) {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-context-invalid",
            message: error.to_string(),
        });
    }
    let request_bytes = serde_json::to_vec(request)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if request_bytes > crate::coordination::AGENT_HANDOFF_RECORD_MAX_BYTES {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-request-too-large",
            message: format!(
                "the handoff cluster serializes to {request_bytes} bytes; at most {} are accepted",
                crate::coordination::AGENT_HANDOFF_RECORD_MAX_BYTES
            ),
        });
    }
    if task.cancellation.is_some() {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-task-cancelling",
            message: "the task's cancellation is propagating; no transfer can be recorded"
                .to_string(),
        });
    }
    if !task.definition.is_agent_owned() {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-not-agent-owned",
            message: "only an agent-owned task can be handed off".to_string(),
        });
    }
    let Some(assignment) = task.assignment.as_ref() else {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-source-not-current",
            message: "the task has no current assignment to transfer from".to_string(),
        });
    };
    if assignment.status != AgentAssignmentStatus::Accepted
        || assignment.agent != request.source_agent
        || assignment.run != request.source_run
        || assignment.generation != request.source_generation
    {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-source-not-current",
            message: format!(
                "the claimed source {}/{} generation {} is not the current accepted assignment",
                request.source_agent, request.source_run, request.source_generation
            ),
        });
    }
    if task.handoffs >= task.definition.limits.max_handoffs {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-limit-exceeded",
            message: format!(
                "the task already recorded its maximum of {} handoffs",
                task.definition.limits.max_handoffs
            ),
        });
    }
    // The target-acceptance validation of specification 8.9: the resolved
    // target must serve this task's typed contract. The definition identity
    // must match, and a target that declares an expected result schema must
    // declare *this* task's.
    if request.target_task_definition != task.definition.definition_id {
        return Err(AgentTaskError::HandoffRefused {
            code: "handoff-contract-mismatch",
            message: format!(
                "the target serves task definition {}, not {}",
                request.target_task_definition, task.definition.definition_id
            ),
        });
    }
    if let Some(schema) = request.result_schema.as_ref() {
        if *schema != task.definition.result_schema {
            return Err(AgentTaskError::HandoffRefused {
                code: "handoff-contract-mismatch",
                message: "the target's expected result schema is not this task's".to_string(),
            });
        }
    }

    let source_assignment = Box::new(assignment.clone());
    let entry_assignment = assignment.clone();
    task.handoff = Some(Box::new(AgentTaskHandoff {
        handoff: request.handoff.clone(),
        source_assignment,
        target: request.target.clone(),
        target_generation: None,
        reason: bounded_detail(request.reason.clone()),
        policy_revision: request.policy_revision,
        context: request.context.clone(),
        knowledge_spaces: request.knowledge_spaces.clone(),
        status: AgentTaskHandoffStatus::Initiated,
        result_settled: false,
        recorded_at: now,
        settled_at: None,
    }));
    task.handoffs += 1;
    // The source assignment leaves the record — stashed, never released: the
    // source run is still live, its escrow child stays open, and its late
    // proposals are fenced by the stale-generation fence once the target's
    // generation mints. The task dips to its assignable posture so the very
    // next decision offers the target.
    task.assignment = None;
    task.assignee = Some(request.target.clone());
    task.status = if task.dependencies_satisfied() {
        AgentTaskStatus::Created
    } else {
        AgentTaskStatus::Blocked
    };
    let status = task.status;
    let handoff_id = request.handoff.clone();
    let target = request.target.clone();
    task.check_bounds(0)?;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::HandoffInitiated,
            operation_id.clone(),
            status,
            now,
        )
        .with_assignment(&entry_assignment)
        .with_detail(format!("{handoff_id} -> {target}"))
    });
    Ok(())
}

/// Resolves a pending handoff as refused: restores the stashed source
/// assignment — the source never stopped being its generation's accepted
/// owner, so the ledger is already consistent — and returns the owed
/// handoff-result exchange that releases the source's fence
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md): "or resolve
/// through explicit recovery").
///
/// The handoff offer gets exactly one assignment-generation attempt; every
/// definitive refusal of that attempt — the target's own refusal, a
/// readiness or affordability refusal, an exhausted assignment budget, a
/// cancellation arriving before the generation minted — resolves through
/// this one helper. Escrow is deliberately untouched: the caller that
/// released a refused generation already released it, and no other caller
/// minted one.
fn resolve_handoff_refusal(
    state: &mut AgentTaskState,
    code: &str,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let task = state.task_mut()?;
    let Some(handoff) = task
        .handoff
        .as_deref_mut()
        .filter(|handoff| !handoff.is_settled())
    else {
        return Ok(Vec::new());
    };
    let detail = bounded_detail(code);
    handoff.status = AgentTaskHandoffStatus::Refused {
        code: detail.clone(),
    };
    handoff.settled_at = Some(now);
    let source = handoff.source_assignment.as_ref().clone();
    let operation_id =
        crate::coordination::handoff_result_operation_id(scope.tenant(), &handoff.handoff)?;
    // The restore. The generation counter is deliberately NOT rolled back:
    // the refused generation was durably offered, and a later decision must
    // mint a fresh one rather than reuse an identity a run entity may have
    // already seen.
    task.assignee = Some(source.agent.clone());
    task.assignment = Some(source.clone());
    task.status = AgentTaskStatus::InProgress;
    let status = task.status;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::HandoffRefused,
            operation_id.clone(),
            status,
            now,
        )
        .with_assignment(&source)
        .with_detail(detail)
    });
    Ok(owed_handoff_result(state, now)?.into_iter().collect())
}

/// The handoff-result exchange the task owes its source run, when it owes
/// one now ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Owed exactly once per handoff — the resolution is absorbing, first writer
/// wins — and re-derived by every settle pass until the exchange settles:
/// the journal's initiation record is the once-guard inside its bounded
/// window, and the provenance's `result_settled` marker is the durable
/// once-guard past it.
fn owed_handoff_result(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(None);
    };
    let Some(handoff) = task.handoff.as_deref() else {
        return Ok(None);
    };
    if !handoff.is_settled() || handoff.result_settled {
        return Ok(None);
    }
    let operation_id =
        crate::coordination::handoff_result_operation_id(state.scope.tenant(), &handoff.handoff)?;
    if state.journal.has_initiated(&operation_id) {
        return Ok(None);
    }
    let resolution = match &handoff.status {
        AgentTaskHandoffStatus::Initiated => return Ok(None),
        AgentTaskHandoffStatus::Accepted => {
            let Some(generation) = handoff.target_generation else {
                // An accepted transfer always recorded its minted generation;
                // absent one there is nothing coherent to report.
                return Ok(None);
            };
            AgentHandoffResolutionNotice::Accepted {
                target_run: run_id_for_assignment(state.scope.task(), generation)?,
                generation,
            }
        }
        AgentTaskHandoffStatus::Refused { code } => {
            AgentHandoffResolutionNotice::Refused { code: code.clone() }
        }
    };
    let source = handoff.source_assignment.as_ref();
    let source_scope = AgentRunScope::new(
        state.scope.tenant().clone(),
        source.agent.clone(),
        source.run.clone(),
    )?;
    let notice = AgentHandoffResultNotice {
        task: state.scope.clone(),
        handoff: handoff.handoff.clone(),
        resolution,
    };
    let payload = AgentExchangePayload::encode(AGENT_HANDOFF_RESULT_PAYLOAD_TYPE, &notice)?;
    Ok(Some(
        AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::HandoffResult,
            AgentEntityAddress::Task(state.scope.clone()),
            AgentEntityAddress::Run(source_scope),
            payload,
            AgentCorrelationId::new(operation_id.as_str()),
            now,
        )?
        .with_telemetry(task.telemetry.clone()),
    ))
}

/// Marks the handoff-result exchange settled on the provenance: the durable
/// once-guard past the journal's bounded deduplication window.
///
/// The marker settles only when the settled envelope's operation id is the
/// one the *currently materialized* handoff derives: a settled provenance
/// can be replaced by a successor hop, and a late settlement of the previous
/// hop's exchange must not quiesce the successor's owed result — that would
/// strand its fenced source run forever.
fn settle_handoff_result_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) {
    let owed = state
        .task()
        .and_then(|task| task.handoff.as_deref())
        .and_then(|handoff| {
            crate::coordination::handoff_result_operation_id(state.scope.tenant(), &handoff.handoff)
                .ok()
        });
    if owed.as_ref() != Some(envelope.operation_id()) {
        return;
    }
    let mut settled = false;
    if let Ok(task) = state.task_mut() {
        if let Some(handoff) = task.handoff.as_deref_mut() {
            if handoff.is_settled() && !handoff.result_settled {
                handoff.result_settled = true;
                settled = true;
            }
        }
    }
    if settled {
        state.updated_at = now;
    }
}

fn task_team_claim_pending(task: &AgentTask) -> bool {
    task.team_claim
        .as_deref()
        .is_some_and(|claim| !claim.is_settled())
}

/// Refuses a board decision over a task whose assignment record still
/// stands: an accepted generation is owned, and an offered one is in
/// flight. Claim and release arbitrate the same board entry through this
/// one guard so the two can never diverge; only the in-flight code differs
/// per action — a refused claim reopens the entry, a refused release
/// restores it pending — so the caller names it.
fn check_assignment_free(task: &AgentTask, inflight_code: &'static str) -> AgentTaskResult<()> {
    let Some(assignment) = task.assignment.as_ref() else {
        return Ok(());
    };
    if assignment.status == AgentAssignmentStatus::Accepted {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-already-owned",
            message: format!(
                "generation {} is accepted by {}",
                assignment.generation, assignment.agent
            ),
        });
    }
    Err(AgentTaskError::TeamClaimRefused {
        code: inflight_code,
        message: format!(
            "generation {} is offered and undecided",
            assignment.generation
        ),
    })
}

/// Whether a board-governed task has waited unclaimed past its horizon
/// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the bounded-wait replacement for the assignee fail-fast a team
/// creation forgoes: a wrong team id, a task never posted, or a board that
/// expired before any claim would otherwise park the task — and lock its
/// delegated escrow — silently forever. The wait re-arms from the task's
/// last transition, so a refused claim's reopened window starts fresh.
fn task_unclaimed_expired(
    task: &AgentTask,
    updated_at: AgentTimestampMillis,
    now: AgentTimestampMillis,
) -> bool {
    if task.status != AgentTaskStatus::Created {
        return false;
    }
    if task.team.is_none()
        || task.assignee.is_some()
        || task.assignment.is_some()
        || task.cancellation.is_some()
    {
        return false;
    }
    if task_team_claim_pending(task) || task_handoff_pending(task) {
        return false;
    }
    let Some(horizon) = task.definition.limits.max_unclaimed_millis else {
        return false;
    };
    now.as_millis() >= updated_at.as_millis().saturating_add(horizon)
}

/// The bounded outcome echo one applied team-claim exchange returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamClaimApplyOutcome {
    /// The task's status once the transition committed.
    pub status: AgentTaskStatus,
    /// The claim the arbitration recorded or echoed.
    pub claim: crate::identity::AgentTeamClaimId,
    /// The task's claim fence after the transition.
    pub epoch: u64,
}

/// Payload type of the team-claim outcome echo.
pub const AGENT_TEAM_CLAIM_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.TeamClaimOutcome";

/// Applies one delivered team-claim exchange: the task-side arbitration of a
/// board decision ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// The reply means "claim recorded", never "assignment made" — the receiving
/// entity's local-progress pass decides the assignment right after this, and
/// the outcome returns by the claim-result exchange.
fn apply_team_claim(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let command: crate::coordination::AgentTeamClaimCommand = match envelope
        .payload()
        .decode(crate::coordination::AGENT_TEAM_CLAIM_PAYLOAD_TYPE)
    {
        Ok(command) => command,
        Err(error) => return refuse(state, error.code(), error.to_string()),
    };
    // The initiator must be the team the command names: a board decision
    // about team T sent by anything but T's entity is forged.
    match envelope.initiator() {
        AgentEntityAddress::Team(scope) if scope == &command.team => {}
        _ => {
            return refuse(
                state,
                "team-claim-forged",
                "a team claim must be initiated by the team it names".to_string(),
            )
        }
    }
    let applied = match &command.action {
        crate::coordination::AgentTeamClaimAction::Claim { member } => {
            record_team_claim(state, envelope.operation_id(), &command, member, now)
        }
        crate::coordination::AgentTeamClaimAction::Release => {
            release_team_claim(state, envelope.operation_id(), &command, now)
        }
    };
    match applied {
        Ok(outcome) => AgentExchangeResult::accepted(
            AgentExchangePayload::encode(AGENT_TEAM_CLAIM_OUTCOME_PAYLOAD_TYPE, &outcome)
                .unwrap_or_else(|_| {
                    AgentExchangePayload::empty(AGENT_TEAM_CLAIM_OUTCOME_PAYLOAD_TYPE)
                }),
        ),
        Err(error) => refuse(state, error.code(), error.to_string()),
    }
}

/// Records one board claim: validates the decision against durable state and
/// points the task's assignee at the claimant
/// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Atomically validate-then-mutate: a refusal leaves the task exactly as it
/// found it, which is what lets the board treat a definitive refusal as
/// proof no claim was recorded. A replay matching the recorded claim id
/// accepts idempotently — the materialized claim is the deduplication echo
/// past the journal's bounded window, checked before every guard including
/// the terminal one. The epoch fence closes courier reordering: a stale
/// board decision refuses however well formed it is.
fn record_team_claim(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    command: &crate::coordination::AgentTeamClaimCommand,
    member: &AgentId,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTeamClaimApplyOutcome> {
    let task = state
        .task
        .as_mut()
        .ok_or(AgentTaskError::TeamClaimRefused {
            code: "team-claim-task-unknown",
            message: "no task exists under this scope".to_string(),
        })?;
    if let Some(existing) = task.team_claim.as_deref() {
        if existing.claim == command.claim {
            return Ok(AgentTeamClaimApplyOutcome {
                status: task.status,
                claim: existing.claim.clone(),
                epoch: task.team_claim_fence,
            });
        }
    }
    if task.team.as_ref() != Some(command.team.team()) {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-wrong-team",
            message: "the task is not governed by this team's board".to_string(),
        });
    }
    if command.epoch <= task.team_claim_fence {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-stale-epoch",
            message: format!(
                "the decision's epoch {} is not above the recorded fence {}",
                command.epoch, task.team_claim_fence
            ),
        });
    }
    if task.status.is_terminal() {
        let detail = task.terminal_reason.as_ref().map_or_else(
            || task.status.to_string(),
            |reason| reason.code().to_string(),
        );
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-task-terminal",
            message: format!("the task is terminal: {detail}"),
        });
    }
    if task.cancellation.is_some() {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-task-cancelling",
            message: "the task's cancellation is propagating; no claim can be recorded".to_string(),
        });
    }
    if task_handoff_pending(task) {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-handoff-pending",
            message: "the task carries an unresolved handoff".to_string(),
        });
    }
    // An offered generation belongs to the decision the board is
    // superseding; refusing keeps exactly one generation in flight
    // ever, and the board reopens for a retry after the offer resolves.
    check_assignment_free(task, "team-claim-assignment-inflight")?;
    if task.team_claims >= task.definition.limits.max_team_claims {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-limit-exceeded",
            message: format!(
                "the task already recorded its maximum of {} board claims",
                task.definition.limits.max_team_claims
            ),
        });
    }

    // A superseding claim replaces a pending predecessor whose generation
    // never minted — a transfer's prior claimant, or an expired-lease steal.
    // The board recorded the supersession in the same compare-and-set that
    // committed this decision, so the predecessor has no board slot left to
    // report into and its materialized record is simply replaced; the chain
    // is history.
    //
    // Bounds are checked over the fully recorded claim, and a failure
    // restores every touched field before the refusal leaves: the accept
    // path persists state even under a refusal, and the board treats a
    // definitive refusal as proof no claim was recorded.
    let previous_claim = task.team_claim.take();
    let previous_claims = task.team_claims;
    let previous_fence = task.team_claim_fence;
    let previous_assignee = task.assignee.take();
    let previous_refusal = task.last_refusal.take();
    task.team_claim = Some(Box::new(AgentTaskTeamClaim {
        claim: command.claim.clone(),
        team: command.team.clone(),
        member: member.clone(),
        epoch: command.epoch,
        target_generation: None,
        status: AgentTaskTeamClaimStatus::Initiated,
        result_settled: false,
        recorded_at: now,
        settled_at: None,
    }));
    task.team_claims += 1;
    task.team_claim_fence = command.epoch;
    task.assignee = Some(member.clone());
    let status = task.status;
    if let Err(error) = task.check_bounds(0) {
        task.team_claim = previous_claim;
        task.team_claims = previous_claims;
        task.team_claim_fence = previous_fence;
        task.assignee = previous_assignee;
        task.last_refusal = previous_refusal;
        return Err(error);
    }
    state.updated_at = now;
    let claim_id = command.claim.clone();
    let member = member.clone();
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::TeamClaimRecorded,
            operation,
            status,
            now,
        )
        .with_detail(format!("{claim_id} -> {member}"))
    });
    Ok(AgentTeamClaimApplyOutcome {
        status,
        claim: command.claim.clone(),
        epoch: command.epoch,
    })
}

/// Releases one pending board claim before its assignment accepted.
///
/// Valid only pre-mint: an offered generation refuses in-flight (the entry
/// restores and the release may be retried once the offer resolves), and an
/// accepted assignment refuses owned — ownership leaves the board only
/// through task-side outcomes. The release's outcome rides the reply home;
/// no claim-result exchange is owed for it.
fn release_team_claim(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    command: &crate::coordination::AgentTeamClaimCommand,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTeamClaimApplyOutcome> {
    let task = state
        .task
        .as_mut()
        .ok_or(AgentTaskError::TeamClaimRefused {
            code: "team-claim-task-unknown",
            message: "no task exists under this scope".to_string(),
        })?;
    let Some(existing) = task
        .team_claim
        .as_deref()
        .filter(|claim| claim.claim == command.claim)
    else {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-release-unknown",
            message: "the task holds no such claim to release".to_string(),
        });
    };
    if existing.status == AgentTaskTeamClaimStatus::Accepted {
        // Ownership leaves the board only through task-side outcomes: a
        // release that raced the acceptance refuses owned even though the
        // claim is settled, so the board marks the entry owned instead of
        // reopening an entry whose task is durably in progress.
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-already-owned",
            message: format!(
                "claim {} is accepted by {}",
                existing.claim, existing.member
            ),
        });
    }
    if existing.is_settled() {
        // The release replayed past the journal window; the recorded
        // resolution stands.
        return Ok(AgentTeamClaimApplyOutcome {
            status: task.status,
            claim: existing.claim.clone(),
            epoch: task.team_claim_fence,
        });
    }
    if command.epoch <= task.team_claim_fence {
        return Err(AgentTaskError::TeamClaimRefused {
            code: "team-claim-stale-epoch",
            message: format!(
                "the release's epoch {} is not above the recorded fence {}",
                command.epoch, task.team_claim_fence
            ),
        });
    }
    check_assignment_free(task, "team-release-assignment-inflight")?;

    let claim = task
        .team_claim
        .as_deref_mut()
        .expect("the claim was matched above");
    claim.status = AgentTaskTeamClaimStatus::Refused {
        code: "team-claim-released".to_string(),
    };
    claim.settled_at = Some(now);
    claim.result_settled = true;
    let claim_id = claim.claim.clone();
    let member = claim.member.clone();
    task.team_claim_fence = command.epoch;
    task.assignee = None;
    let status = task.status;
    state.updated_at = now;
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::TeamClaimRefused,
            operation,
            status,
            now,
        )
        .with_detail(format!("{claim_id} released by {member}"))
    });
    Ok(AgentTeamClaimApplyOutcome {
        status,
        claim: command.claim.clone(),
        epoch: command.epoch,
    })
}

/// Resolves the pending team claim as refused: clears the assignee back to
/// the board-pending posture and settles the claim under a stable code.
///
/// The claim gets exactly one assignment-generation attempt — the handoff
/// single-attempt precedent: every definitive refusal of that attempt (the
/// run's own refusal, a readiness or affordability refusal, an exhausted
/// assignment budget, a cancellation arriving before the generation minted)
/// resolves through this one helper, and the owed claim-result exchange
/// carries the code back to the board, which reopens the entry. Escrow is
/// deliberately untouched: the caller that released a refused generation
/// already released it, and no other caller minted one.
fn resolve_team_claim_refusal(
    state: &mut AgentTaskState,
    code: &str,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let task = state.task_mut()?;
    let Some(claim) = task
        .team_claim
        .as_deref_mut()
        .filter(|claim| !claim.is_settled())
    else {
        return Ok(Vec::new());
    };
    let detail = bounded_detail(code);
    claim.status = AgentTaskTeamClaimStatus::Refused {
        code: detail.clone(),
    };
    claim.settled_at = Some(now);
    let claim_id = claim.claim.clone();
    let member = claim.member.clone();
    let operation_id =
        crate::coordination::team_claim_result_operation_id(scope.tenant(), &claim_id)?;
    task.assignee = None;
    let status = task.status;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::TeamClaimRefused,
            operation_id,
            status,
            now,
        )
        .with_detail(format!("{claim_id} {member}: {detail}"))
    });
    Ok(owed_team_claim_result(state, now)?.into_iter().collect())
}

/// The claim-result exchange the task owes its claim's team, when it owes
/// one now ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Owed exactly once per claim — the resolution is absorbing, first writer
/// wins — and re-derived by every settle pass until the exchange settles:
/// the journal's initiation record is the once-guard inside its bounded
/// window, and the claim's `result_settled` marker is the durable
/// once-guard past it.
fn owed_team_claim_result(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(None);
    };
    let Some(claim) = task.team_claim.as_deref() else {
        return Ok(None);
    };
    if !claim.is_settled() || claim.result_settled {
        return Ok(None);
    }
    let operation_id =
        crate::coordination::team_claim_result_operation_id(state.scope.tenant(), &claim.claim)?;
    if state.journal.has_initiated(&operation_id) {
        return Ok(None);
    }
    let outcome = match &claim.status {
        AgentTaskTeamClaimStatus::Initiated => return Ok(None),
        AgentTaskTeamClaimStatus::Accepted => {
            let Some(generation) = claim.target_generation else {
                // An accepted claim always recorded its minted generation;
                // absent one there is nothing coherent to report.
                return Ok(None);
            };
            crate::coordination::AgentTeamClaimOutcome::Activated {
                generation,
                run: run_id_for_assignment(state.scope.task(), generation)?,
                member: claim.member.clone(),
            }
        }
        AgentTaskTeamClaimStatus::Refused { code } => {
            crate::coordination::AgentTeamClaimOutcome::Refused { code: code.clone() }
        }
    };
    let notice = crate::coordination::AgentTeamClaimResultNotice {
        task: state.scope.clone(),
        claim: claim.claim.clone(),
        epoch: claim.epoch,
        outcome,
    };
    let payload = AgentExchangePayload::encode(
        crate::coordination::AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
        &notice,
    )?;
    Ok(Some(
        AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::TeamClaimResult,
            AgentEntityAddress::Task(state.scope.clone()),
            AgentEntityAddress::Team(claim.team.clone()),
            payload,
            AgentCorrelationId::new(operation_id.as_str()),
            now,
        )?
        .with_telemetry(task.telemetry.clone()),
    ))
}

/// Marks the claim-result exchange settled on the claim provenance: the
/// durable once-guard past the journal's bounded deduplication window.
///
/// The marker settles only when the settled envelope's operation id is the
/// one the *currently materialized* claim derives, exactly as the handoff
/// marker does: a settled claim can be replaced by a successor, and a late
/// settlement of the predecessor's exchange must not quiesce the
/// successor's owed result.
fn settle_team_claim_result_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) {
    let owed = state
        .task()
        .and_then(|task| task.team_claim.as_deref())
        .and_then(|claim| {
            crate::coordination::team_claim_result_operation_id(state.scope.tenant(), &claim.claim)
                .ok()
        });
    if owed.as_ref() != Some(envelope.operation_id()) {
        return;
    }
    let mut settled = false;
    if let Ok(task) = state.task_mut() {
        if let Some(claim) = task.team_claim.as_deref_mut() {
            if claim.is_settled() && !claim.result_settled {
                claim.result_settled = true;
                settled = true;
            }
        }
    }
    if settled {
        state.updated_at = now;
    }
}

/// The continuous root control task a wake command must address, or the
/// refusal it answers instead.
///
/// Every wake transition shares these fences: the task must exist, must not be
/// terminal, and must coordinate a continuous goal.
fn continuous_task_mut(state: &mut AgentTaskState) -> AgentTaskResult<&mut AgentTask> {
    let scope = state.scope.clone();
    let task = state
        .task
        .as_mut()
        .ok_or(AgentTaskError::NotCreated { scope })?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    if !task.goal_mode.is_continuous() {
        return Err(AgentTaskError::WakeNotContinuous);
    }
    Ok(task)
}

/// The command an [`AgentExchangeKind::EpochResult`] exchange carries to the
/// continuous root control task: one completed epoch's terminal outcome,
/// consumption, and evidence reference
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Epoch completion returns evidence to the controller and never by itself
/// terminates the continuous goal; the controller releases the wake, settles
/// the epoch's escrow, and promotes whatever the release makes admittable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEpochResult {
    /// The wake whose epoch completed.
    pub wake: AgentWakeId,
    /// The epoch's derived child task.
    pub task: AgentTaskId,
    /// The epoch task's terminal status.
    pub status: AgentTaskStatus,
    /// What the epoch consumed, settled up from its own ledger.
    pub consumed: AgentBudgetConsumption,
    /// The accepted result's fingerprint, when the epoch produced one.
    pub result_digest: Option<AgentContentDigest>,
}

/// The epoch-result exchange one completed epoch task owes its controller,
/// when it owes one now.
///
/// An epoch owes its result exactly once, after its own ledger closed — the
/// run's settlement and return have both applied, so the consumption it
/// reports is what its parent settles, never an early under-count. The
/// journal's initiation record is the once-guard: whichever transition
/// observes the closed ledger first owes the exchange, and every later
/// observer finds it initiated.
fn owed_epoch_result(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(None);
    };
    if !task.status.is_terminal() {
        return Ok(None);
    }
    let (Some(wake), Some(parent), Some(goal)) = (&task.wake, &task.parent, &task.goal) else {
        return Ok(None);
    };
    if task.escrow.outstanding().count() > 0 {
        // The run has not settled and returned its escrow yet; reporting now
        // would under-count what the parent settles.
        return Ok(None);
    }
    // A construction failure from here on is loud: the transition that owed
    // nothing when it should have would silently orphan the controller's
    // active occurrence. Refusing lets the exchange or command re-drive.
    let operation_id = epoch_result_operation_id(state.scope.tenant(), goal, wake)?;
    if state.journal.has_initiated(&operation_id) {
        return Ok(None);
    }
    let parent_scope = AgentTaskScope::new(state.scope.tenant().clone(), parent.clone())?;
    let result = AgentEpochResult {
        wake: wake.clone(),
        task: state.scope.task().clone(),
        status: task.status,
        consumed: *task.escrow.consumed(),
        result_digest: task
            .accepted_result
            .as_ref()
            .map(|accepted| accepted.digest.clone()),
    };
    let payload = AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)?;
    Ok(Some(
        AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::EpochResult,
            AgentEntityAddress::Task(state.scope.clone()),
            AgentEntityAddress::Task(parent_scope),
            payload,
            AgentCorrelationId::new(operation_id.as_str()),
            now,
        )?
        .with_telemetry(task.telemetry.clone()),
    ))
}

/// The payload an [`AgentExchangeKind::DelegationResult`] exchange carries to
/// the parent run: one delegated child task's terminal outcome, as bounded
/// references only ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// A child's terminal state is evidence returned to the parent — never a goal
/// decision, and never the child's content: the digest fingerprints the
/// accepted result, and the child task id is the authorized-query handle for
/// anything more ([`crate::query::authorized_agent_goal_view`] assembles the
/// goal-wide view those handles key into).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDelegationReport {
    /// The delegation whose child reports.
    pub delegation: AgentDelegationId,
    /// The reporting child task.
    pub child_task: AgentTaskId,
    /// The run that served the child's terminal assignment, when it was ever
    /// assigned one.
    #[serde(default)]
    pub child_run: Option<AgentRunId>,
    /// The child task's terminal status.
    pub status: AgentTaskStatus,
    /// The child's stable terminal-reason code, when it recorded one.
    #[serde(default)]
    pub terminal_reason: Option<String>,
    /// The accepted result's fingerprint, when the child produced one.
    #[serde(default)]
    pub result_digest: Option<AgentContentDigest>,
    /// Descendant tasks the child's own subtree created, from its settled
    /// ledger. Recorded on the parent's cell for a later slice to credit
    /// unused sub-quota; slice 4.4 never credits.
    #[serde(default)]
    pub descendants_created: u64,
}

/// The command an [`AgentExchangeKind::DelegationCancel`] exchange carries to
/// a delegated child task: the parent run's durable cancellation request
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The request names the delegation the parent's cell recorded, so the child
/// authenticates it against its own delegation provenance — a sender that is
/// not the recorded parent run, or a delegation the child does not carry, is
/// a forged request refused definitively.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDelegationCancelRequest {
    /// The delegation whose child is asked to cancel.
    pub delegation: AgentDelegationId,
    /// The child task the parent's cell created.
    pub child_task: AgentTaskId,
    /// The bounded reason recorded on the child's cancellation marker.
    pub reason: String,
}

/// The child task's durable receipt replying to an
/// [`AgentExchangeKind::DelegationCancel`].
///
/// Acceptance means the child durably recorded the request — its own
/// cancellation marker is set, or it was already terminal. The child's
/// terminal outcome still arrives separately as its
/// [`AgentExchangeKind::DelegationResult`]; an accepted receipt is never
/// proof the child's started effects stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDelegationCancelReceipt {
    /// The child task that recorded the request.
    pub child_task: AgentTaskId,
    /// The status the child held after recording it: nonterminal while its
    /// own wind-down propagates, or the terminal status the request found.
    pub status: AgentTaskStatus,
}

/// The delegation-result exchange one terminal delegated child owes its
/// parent run, when it owes one now.
///
/// Owed exactly once, after the child's own ledger closed — the run's
/// settlement and return have both applied, so the consumption-derived
/// fields are final. The journal's initiation record is the once-guard,
/// exactly as the epoch result's.
fn owed_delegation_result(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(None);
    };
    if !task.status.is_terminal() {
        return Ok(None);
    }
    let Some(provenance) = task.delegation.as_deref() else {
        return Ok(None);
    };
    if task.escrow.outstanding().count() > 0 {
        // The child's run has not settled and returned its escrow yet;
        // reporting now would carry a non-final consumption.
        return Ok(None);
    }
    let operation_id = crate::delegation::delegation_result_operation_id(
        state.scope.tenant(),
        &provenance.delegation,
    )?;
    if state.journal.has_initiated(&operation_id) {
        return Ok(None);
    }
    let child_run = (task.assignment_generation != AgentAssignmentGeneration::UNASSIGNED)
        .then(|| run_id_for_assignment(state.scope.task(), task.assignment_generation))
        .transpose()?;
    let report = AgentDelegationReport {
        delegation: provenance.delegation.clone(),
        child_task: state.scope.task().clone(),
        child_run,
        status: task.status,
        terminal_reason: task
            .terminal_reason
            .as_ref()
            .map(|reason| reason.code().to_string()),
        result_digest: task
            .accepted_result
            .as_ref()
            .map(|accepted| accepted.digest.clone()),
        descendants_created: task.escrow.consumed().descendants,
    };
    let payload = AgentExchangePayload::encode(AGENT_DELEGATION_RESULT_PAYLOAD_TYPE, &report)?;
    Ok(Some(
        AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::DelegationResult,
            AgentEntityAddress::Task(state.scope.clone()),
            AgentEntityAddress::Run(provenance.parent_run.clone()),
            payload,
            AgentCorrelationId::new(operation_id.as_str()),
            now,
        )?
        .with_telemetry(task.telemetry.clone()),
    ))
}

/// The dependency-registration exchanges this task owes its upstreams right
/// now: one per unresolved forward edge whose registration has not settled
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Pure over durable state: the journal's initiation record guards the
/// bounded window, and the edge's `registration_settled` marker quiesces the
/// derivation past it. A terminal or cancelling task registers nothing — an
/// outcome could no longer move it. Edges persisted before the registry
/// existed carry an unsettled marker, so an unresolved pre-registry edge
/// registers itself on the next settle pass.
fn owed_dependency_registrations(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(Vec::new());
    };
    if task.status.is_terminal() || task.cancellation.is_some() {
        return Ok(Vec::new());
    }
    let mut owed = Vec::new();
    for edge in task.dependencies.values() {
        if edge.outcome.is_some() || edge.registration_settled {
            continue;
        }
        let operation_id = dependency_registration_operation_id(
            state.scope.tenant(),
            &edge.dependency,
            state.scope.task(),
        )?;
        if state.journal.has_initiated(&operation_id) {
            continue;
        }
        let upstream = AgentTaskScope::new(state.scope.tenant().clone(), edge.dependency.clone())?;
        let registration = AgentDependencyRegistration {
            dependent: state.scope.clone(),
            upstream: edge.dependency.clone(),
            policy: edge.policy,
        };
        let payload = AgentExchangePayload::encode(
            AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE,
            &registration,
        )?;
        owed.push(
            AgentExchangeEnvelope::new(
                operation_id.clone(),
                AgentExchangeKind::DependencyRegistration,
                AgentEntityAddress::Task(state.scope.clone()),
                AgentEntityAddress::Task(upstream),
                payload,
                AgentCorrelationId::new(operation_id.as_str()),
                now,
            )?
            .with_telemetry(task.telemetry.clone()),
        );
    }
    Ok(owed)
}

/// The dependency-outcome notifications a terminal task owes its registered
/// dependents right now
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)) — owed
/// immediately at the terminal commit, with no escrow gate: the status, the
/// terminal reason, and the result digest are all absorbing the moment
/// [`terminate`] commits, unlike a delegation report's consumption-derived
/// fields. The journal's initiation record guards the bounded window; the
/// registry entry's `outcome_settled` marker quiesces the derivation past
/// it.
///
/// At most `budget` notifications are derived per call, so a registry larger
/// than the journal's free pending slots defers the remainder to a later pass
/// rather than failing the terminal transition outright
/// ([`owed_exchange_budget`]).
fn owed_dependent_outcomes(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
    budget: usize,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let Some(task) = state.task.as_ref() else {
        return Ok(Vec::new());
    };
    if !task.status.is_terminal() {
        return Ok(Vec::new());
    }
    let Some(outcome) = AgentTaskDependencyOutcome::from_terminal_status(task.status) else {
        return Ok(Vec::new());
    };
    let mut owed = Vec::new();
    for record in task.dependents.values() {
        if owed.len() >= budget {
            // Out of journal headroom, not out of dependents: the rest are
            // owed by a later pass. See [`owed_exchange_budget`].
            break;
        }
        if record.outcome_settled {
            continue;
        }
        let operation_id = dependency_outcome_operation_id(
            state.scope.tenant(),
            state.scope.task(),
            &record.dependent,
        )?;
        if state.journal.has_initiated(&operation_id) {
            continue;
        }
        let dependent =
            AgentTaskScope::new(state.scope.tenant().clone(), record.dependent.clone())?;
        let notice = AgentDependencyOutcomeNotice {
            upstream: state.scope.clone(),
            outcome,
            terminal_reason: task
                .terminal_reason
                .as_ref()
                .map(|reason| reason.code().to_string()),
            result_digest: task
                .accepted_result
                .as_ref()
                .map(|accepted| accepted.digest.clone()),
        };
        let payload = AgentExchangePayload::encode(AGENT_DEPENDENCY_OUTCOME_PAYLOAD_TYPE, &notice)?;
        owed.push(
            AgentExchangeEnvelope::new(
                operation_id.clone(),
                AgentExchangeKind::DependencyOutcome,
                AgentEntityAddress::Task(state.scope.clone()),
                AgentEntityAddress::Task(dependent),
                payload,
                AgentCorrelationId::new(operation_id.as_str()),
                now,
            )?
            .with_telemetry(task.telemetry.clone()),
        );
    }
    Ok(owed)
}

/// How many further exchanges this task may owe in one transition without
/// overflowing the journal's bounded pending list, given the `reserved`
/// envelopes the calling transition already owes.
///
/// Recording an owed exchange fails the *whole* transition on overflow rather
/// than dropping the exchange, so a derivation that produces a batch — the
/// outcome notifications a terminal task owes its dependents — has to fit
/// itself to what is free, or a task with a full dependency graph could not
/// terminalize at all. The dependent registry and the forward edges are each
/// bounded by their own ceiling and share one pending list, and a terminal
/// task's registrations are withdrawn before this is consulted
/// ([`withdraw_moot_registrations`]), so in practice the budget binds only a
/// task that owes an unusual amount of other work at the same moment.
///
/// Fitting is safe because the derivation is pure over durable state: the
/// registry entry's `outcome_settled` marker survives the transition, so a
/// notification that does not fit here is owed by the next settle pass
/// instead. Deferral, never loss.
fn owed_exchange_budget(state: &AgentTaskState, reserved: usize) -> usize {
    AGENT_EXCHANGE_PENDING_CAPACITY
        .saturating_sub(state.journal.outstanding_count())
        .saturating_sub(reserved)
}

/// Withdraws the dependency registrations a task can no longer act on, and
/// reports how many it dropped.
///
/// A registration exists to make an upstream owe this task an outcome
/// notification. A terminal task consumes no outcome — [`owed_dependency_registrations`]
/// already derives nothing for one — so an outstanding registration is dead
/// weight holding a pending slot that this task's *own* notifications to its
/// dependents need. Both sides of the graph are bounded by the same ceiling
/// and the pending list is smaller than their sum, which is exactly the case
/// this reclaims.
fn withdraw_moot_registrations(state: &mut AgentTaskState) -> usize {
    let Some(task) = state.task.as_ref() else {
        return 0;
    };
    if !task.status.is_terminal() {
        return 0;
    }
    let operations: Vec<AgentOperationId> = task
        .dependencies
        .values()
        .filter_map(|edge| {
            dependency_registration_operation_id(
                state.scope.tenant(),
                &edge.dependency,
                state.scope.task(),
            )
            .ok()
        })
        .collect();
    operations
        .iter()
        .filter(|operation| state.journal.withdraw(operation))
        .count()
}

/// Every child report a terminal task owes right now: the epoch result to a
/// wake controller, the delegation result to a delegating parent run, and
/// the dependency outcomes its registered dependents wait on. One consult
/// point, so every transition that can terminalize a task or close its
/// ledger owes all three from the same compare-and-set.
///
/// `reserved` is how many envelopes the calling transition already owes; the
/// dependent notifications fit themselves into what is left of the journal's
/// pending list ([`owed_exchange_budget`]).
fn owed_child_reports(
    state: &AgentTaskState,
    now: AgentTimestampMillis,
    reserved: usize,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let mut owed = Vec::new();
    owed.extend(owed_epoch_result(state, now)?);
    owed.extend(owed_delegation_result(state, now)?);
    let budget = owed_exchange_budget(state, reserved.saturating_add(owed.len()));
    owed.extend(owed_dependent_outcomes(state, now, budget)?);
    Ok(owed)
}

/// Applies one epoch's result to the controller that admitted its wake:
/// settles and returns the epoch's escrow, releases the wake, and owes the
/// promoted occurrence's epoch creation — all in this one accepted
/// transition.
fn apply_epoch_result(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeTransition {
    let result: AgentEpochResult = match envelope.payload().decode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE)
    {
        Ok(result) => result,
        Err(error) => {
            return AgentExchangeTransition::new(refuse(state, error.code(), error.to_string()))
        }
    };

    // The sender must be the very epoch the wake derives: a result from any
    // other address is a forgery, whatever it claims.
    let derived = match epoch_task_id_for_wake(&result.wake) {
        Ok(derived) => derived,
        Err(error) => {
            return AgentExchangeTransition::new(refuse(state, error.code(), error.to_string()))
        }
    };
    let sender = match envelope.initiator() {
        AgentEntityAddress::Task(scope) => scope.task().clone(),
        other => {
            return AgentExchangeTransition::new(refuse(
                state,
                "task-epoch-forged",
                format!("an epoch result cannot originate from {other}"),
            ))
        }
    };
    if derived != result.task || sender != derived {
        return AgentExchangeTransition::new(refuse(
            state,
            "task-epoch-forged",
            format!(
                "the epoch result claims task {}, sent by {sender}, but the wake derives {derived}",
                result.task
            ),
        ));
    }

    let scope = state.scope.clone();
    // The sequence below mutates before it can still fail: the settlement and
    // the release land ahead of the owed creation and the bounds check, either
    // of which may yet refuse. It therefore runs on a scratch clone, committed
    // only whole — a refusal persists the recorded result alone, never a
    // half-applied release that promoted an occurrence whose epoch nobody
    // owes.
    let mut scratch = state.clone();
    let applied = (|state: &mut AgentTaskState| -> AgentTaskResult<(
        Option<crate::wake::AgentWakeRelease>,
        Vec<AgentExchangeEnvelope>,
    )> {
        let task = continuous_task_mut(state)?;

        // Settle and return the epoch's escrow, idempotently: a child the
        // ledger no longer knows was settled and returned by an earlier
        // delivery.
        let generation = AgentAssignmentGeneration::new(1);
        let run = run_id_for_assignment(&result.task, generation)?;
        let child = AgentEscrowChildId::for_run(&run)?;
        match task.escrow.settle_child(&child, &result.consumed) {
            Ok(_) => {
                task.escrow.return_child(&child)?;
            }
            Err(error) if error.code() == AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN => {}
            Err(error) => return Err(error.into()),
        }

        // Account the epoch's outcome *before* the release attempt, so the
        // raced-CompleteWakeOccurrence path — where the release answers
        // NotActive — still counts a failure exactly as its settlement still
        // counts. An escalation suspends here, and the release's own
        // promotion is gated by it in the same breath.
        let policy = task
            .goal_mode
            .continuous()
            .expect("continuous_task_mut proved the mode")
            .wake_policy
            .policy()
            .clone();
        let outcome_class = match result.status {
            AgentTaskStatus::Completed => AgentEpochOutcomeClass::Completed,
            AgentTaskStatus::Failed => AgentEpochOutcomeClass::Failed,
            // Cancellation — and, defensively, anything that is not a
            // terminal outcome — neither resets nor grows the streak.
            _ => AgentEpochOutcomeClass::Cancelled,
        };
        // The stagnation facts, read before the controller borrow: the
        // detector always accounts, but an action fires only against a goal
        // that is authorized or parked — the exhaustion executor's guards.
        let (stagnation_policy, stagnation_allowed) = match task.goal_state.as_deref() {
            Some(goal) => (
                goal.spec().spec().stagnation_policy.clone(),
                matches!(
                    goal.status(),
                    AgentGoalStatus::Active | AgentGoalStatus::Waiting
                ),
            ),
            None => (AgentGoalStagnationPolicy::default(), false),
        };
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        let lifecycle_before = controller.lifecycle().status();
        controller.observe_lifecycle(&policy, now);
        controller.record_epoch_outcome(&policy, outcome_class, now);
        // The outcome accounting itself may have flipped the lifecycle — an
        // escalated failure streak auto-suspends — so the flip window spans
        // both the observation and the accounting. It deliberately excludes
        // the stagnation action below: a stagnation park is the contract
        // door's own wait, and projecting the gate's view of it would hand
        // the park to the wrong resume door.
        let lifecycle_flip =
            observed_lifecycle_history(lifecycle_before, controller.lifecycle().status());
        // Account the epoch's progress evidence. Only a completed epoch moves
        // the detector, so a stagnation trip and a failure flip can never
        // fire from one settlement.
        let tripped = controller.record_epoch_progress(
            &stagnation_policy,
            outcome_class,
            result.result_digest.as_ref(),
        );
        let stagnation = tripped.filter(|_| stagnation_allowed).map(|trigger| {
            let epochs = match trigger {
                AgentStagnationTrigger::RepeatedResult => {
                    controller.lifecycle().repeated_result_epochs()
                }
                AgentStagnationTrigger::NoProgress => controller.lifecycle().no_progress_epochs(),
            };
            (trigger, epochs)
        });
        // A gate-closing action lands *before* the release, the failure
        // escalation's precedent: the release's promotion is gated by the
        // lifecycle in the same breath, so a closed gate releases the active
        // wake without promoting a coalesced occurrence nobody should admit.
        if let Some((trigger, _)) = stagnation {
            match stagnation_policy.action_for(trigger) {
                AgentGoalStagnationAction::Wait | AgentGoalStagnationAction::Escalate => {
                    controller.suspend_by_policy(AgentGoalWaitReason::Stagnant { trigger }.code());
                }
                AgentGoalStagnationAction::Terminate => {
                    controller.retire_by_policy();
                }
                AgentGoalStagnationAction::Continue | AgentGoalStagnationAction::Replan => {}
            }
        }

        // Release the wake. A wake that is no longer active was released by
        // an explicit CompleteWakeOccurrence; the settlement above still
        // counted.
        let release = match controller.release(&policy, &result.wake, now) {
            Ok(release) => Some(release),
            Err(AgentWakeError::NotActive { .. }) => None,
            Err(error) => return Err(error.into()),
        };
        controller.ensure_rewakes(&policy, now);

        let mut owed = Vec::new();
        if let Some(next) = release
            .as_ref()
            .and_then(|release| release.admitted_next.clone())
        {
            owed.push(owe_epoch_creation(&scope, task, &next, now)?);
        }
        task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
        state.updated_at = now;
        let operation_id = envelope.operation_id();
        if let Some(kind) = lifecycle_flip {
            record_wake_history(state, kind, operation_id, "observed".to_string(), now);
            project_gate_onto_goal(state, operation_id, None, now)?;
        }
        if let Some((trigger, epochs)) = stagnation {
            let digest = match trigger {
                AgentStagnationTrigger::RepeatedResult => result.result_digest.clone(),
                AgentStagnationTrigger::NoProgress => None,
            };
            apply_goal_stagnation(state, operation_id, trigger, epochs, digest, now)?;
        }
        record_wake_history_with_digest(
            state,
            AgentTaskHistoryKind::EpochSettled,
            operation_id,
            format!("{} {}", outcome_class.as_label(), result.wake),
            result.result_digest.clone(),
            now,
        );
        if let Some(next) = release
            .as_ref()
            .and_then(|release| release.admitted_next.as_ref())
        {
            record_wake_history(
                state,
                AgentTaskHistoryKind::EpochAdmitted,
                operation_id,
                next.to_string(),
                now,
            );
        }
        Ok((release, owed))
    })(&mut scratch);

    match applied {
        Ok((release, owed)) => {
            *state = scratch;
            let outcome = release.map(AgentWakeOutcome::Release);
            let payload =
                AgentExchangePayload::encode(AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE, &outcome)
                    .unwrap_or_else(|_| {
                        AgentExchangePayload::empty(AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE)
                    });
            let mut transition =
                AgentExchangeTransition::new(AgentExchangeResult::accepted(payload));
            for envelope in owed {
                transition = transition.owing(envelope);
            }
            transition
        }
        Err(error) => AgentExchangeTransition::new(refuse(state, error.code(), error.to_string())),
    }
}

/// Applies one completed goal evaluation to the coordinating root task
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)): the
/// exchange ingress of the decision door, and — under a configured evaluator —
/// the only one that can make a criteria decision.
///
/// The sender fence is the attestation: the initiator must be the run the
/// task's current assignment binds, so a forged or superseded report never
/// reaches the door. Every contract fence — terminality, status revision read
/// in this same transition, criteria revision, evaluator identity, required
/// evidence — runs in the shared decision core; a contract refusal becomes
/// the exchange's refused reply, which settles the run's evaluation cell and
/// tells the caller to re-evaluate.
fn apply_goal_evaluation(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeTransition {
    let record: AgentGoalEvaluationRecord = match envelope
        .payload()
        .decode(AGENT_GOAL_EVALUATION_PAYLOAD_TYPE)
    {
        Ok(record) => record,
        Err(error) => {
            return AgentExchangeTransition::new(refuse(state, error.code(), error.to_string()))
        }
    };

    let sender = match envelope.initiator() {
        AgentEntityAddress::Run(scope) => scope.run().clone(),
        other => {
            return AgentExchangeTransition::new(refuse(
                state,
                "task-goal-evaluation-forged",
                format!("a goal evaluation cannot originate from {other}"),
            ))
        }
    };
    let (assigned, goal_id, expected) = match state.task.as_ref() {
        Some(task) => (
            task.assignment
                .as_ref()
                .map(|assignment| assignment.run.clone()),
            task.goal.clone(),
            task.goal_state
                .as_deref()
                .map(AgentGoalState::status_revision),
        ),
        None => (None, None, None),
    };
    if assigned.as_ref() != Some(&sender) {
        return AgentExchangeTransition::new(refuse(
            state,
            "task-goal-evaluation-forged",
            format!("the evaluation was sent by run {sender}, which is not the current assignment"),
        ));
    }
    if goal_id.as_ref() != Some(&record.goal) {
        return AgentExchangeTransition::new(refuse(
            state,
            "task-goal-evaluation-forged",
            format!(
                "the evaluation judges goal {}, which this task does not coordinate",
                record.goal
            ),
        ));
    }

    // The status revision is read inside this same transition — local state,
    // no window — so the load-bearing fences are the ones the record must
    // prove: terminality, criteria revision, evaluator, evidence. Replays are
    // already deduplicated by the journal before this arm runs.
    let reason = match record.outcome {
        crate::evaluation::AgentGoalEvaluationOutcome::Satisfied => {
            AgentGoalTerminalReason::CriteriaSatisfied
        }
        crate::evaluation::AgentGoalEvaluationOutcome::NotSatisfied => {
            AgentGoalTerminalReason::CriteriaNotMet
        }
    };
    // The attestation digest is what binds this decision to this record, so a
    // reference that cannot be built refuses rather than deciding unbound.
    let evaluation = match record.to_evaluation_ref() {
        Ok(evaluation) => evaluation,
        Err(error) => {
            return AgentExchangeTransition::new(refuse(state, error.code(), error.to_string()))
        }
    };
    let decision = AgentGoalDecision {
        reason,
        evaluation: Some(Box::new(evaluation)),
        provenance: None,
        expected_status_revision: expected.unwrap_or(AgentRevisionNumber::INITIAL),
    };
    match apply_goal_decision(state, envelope.operation_id(), decision, now) {
        Ok(()) => {
            let outcome = state
                .task
                .as_ref()
                .and_then(|task| task.goal_state.as_deref())
                .map(|goal| AgentGoalOutcome {
                    status: goal.status(),
                    status_revision: goal.status_revision(),
                });
            let payload =
                AgentExchangePayload::encode(AGENT_GOAL_EVALUATION_OUTCOME_PAYLOAD_TYPE, &outcome)
                    .unwrap_or_else(|_| {
                        AgentExchangePayload::empty(AGENT_GOAL_EVALUATION_OUTCOME_PAYLOAD_TYPE)
                    });
            AgentExchangeTransition::new(AgentExchangeResult::accepted(payload))
        }
        Err(error) => AgentExchangeTransition::new(refuse(state, error.code(), error.to_string())),
    }
}

/// Owes the creation of one admitted wake's epoch
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)): derives the
/// epoch's task and run identities from the wake, debits its escrow from the
/// root controller's own ledger, attaches the epoch to its active occurrence,
/// and returns the creation exchange the courier delivers.
///
/// Everything here commits in the admitting transition's one compare-and-set:
/// the controller can never durably hold an admitted occurrence while having
/// forgotten the epoch it owes, and a replay resolves to the same derived
/// identities and the same already-debited escrow rather than a second epoch.
fn owe_epoch_creation(
    scope: &AgentTaskScope,
    task: &mut AgentTask,
    wake: &AgentWakeId,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentExchangeEnvelope> {
    let spec = task
        .goal_mode
        .continuous()
        .expect("the caller proved the mode");
    let Some(epoch_spec) = spec.epoch.clone() else {
        // A pre-3.3 record, or a goal that never declared its epoch contract:
        // there is no definition to run, so admission fails closed rather
        // than guessing one.
        return Err(AgentTaskError::EpochUndefined);
    };
    let epoch_budget = spec.wake_policy.policy().epoch_budget;
    let epoch_deadline = spec.wake_policy.policy().epoch_deadline_millis;
    let goal = task
        .goal
        .clone()
        .expect("a continuous task binds its goal at creation");

    let epoch_task = epoch_task_id_for_wake(wake)?;
    let generation = AgentAssignmentGeneration::new(1);
    let run = run_id_for_assignment(&epoch_task, generation)?;
    let operation_id = epoch_admission_operation_id(scope.tenant(), &goal, wake)?;
    let epoch_scope = AgentTaskScope::new(scope.tenant().clone(), epoch_task.clone())?;

    // The down-front escrow of specification 9.7: debited from the root's
    // ledger here, idempotent on the derived run id, carried on the creation.
    let allocation = task
        .escrow
        .open_child(AgentEscrowChildId::for_run(&run)?, &epoch_budget)?;
    let mut limits = *task.escrow.limits();
    limits.max_wall_clock_millis = match (limits.max_wall_clock_millis, epoch_deadline) {
        (Some(root), Some(epoch)) => Some(root.min(epoch)),
        (root, epoch) => epoch.or(root),
    };
    let budget = AgentBudgetGrant::new(allocation, limits);

    let controller = task
        .wake_controller
        .as_mut()
        .expect("an admission just ran on this controller");
    let binding = controller
        .active()
        .iter()
        .find(|active| active.binding().wake_id() == wake)
        .map(|active| active.binding().clone())
        .ok_or_else(|| AgentTaskError::Wake(AgentWakeError::NotActive { wake: wake.clone() }))?;
    controller.attach_epoch(
        wake,
        AgentEpochRef {
            task: epoch_task,
            run,
        },
    )?;

    // The epoch's input: the occurrence it observes and the authorized
    // observation scope — bounded, credential-free, and derived, so a replay
    // encodes the identical payload.
    let input = AgentTaskContent::inline(serde_json::json!({
        "wake": wake.as_str(),
        "occurrence": binding.occurrence(),
        "schedule_revision": binding.schedule_revision().get(),
        "observation_scope": epoch_spec.observation_scope,
    }))?;

    let creation = AgentTaskCreation {
        definition: epoch_spec.definition.clone(),
        input,
        assignee: Some(epoch_spec.assignee.clone()),
        // An epoch is admitted by its own controller, never board-claimed.
        team: None,
        goal: Some(goal),
        goal_mode: AgentGoalMode::Finite,
        // An epoch task contributes to the goal; it never coordinates it, so
        // it carries the binding and no goal record of its own.
        goal_spec: None,
        parent: Some(scope.task().clone()),
        dependencies: Vec::new(),
        escrow: Some(budget),
        wake: Some(wake.clone()),
        // An epoch is admitted by its own controller, never delegated.
        delegation: None,
        telemetry: task.telemetry.clone(),
    };
    let payload = AgentExchangePayload::encode(AGENT_TASK_CREATION_PAYLOAD_TYPE, &creation)?;
    Ok(AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::Creation,
        AgentEntityAddress::Task(scope.clone()),
        AgentEntityAddress::Task(epoch_scope),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        now,
    )?
    .with_telemetry(task.telemetry.clone()))
}

/// Dispositions one delivered wake occurrence, or fails closed.
///
/// The operation id must be the one the binding itself derives — every trigger
/// path reconstructs it, so a delivery whose id disagrees with its own binding
/// is not a redelivery, it is a forgery, and it is refused before any state is
/// read. The disposition — including a fence or a skip — is a recorded
/// transition, which is what makes the wake counters exact and a replayed
/// delivery a [`AgentTaskEntityReply::Duplicate`] instead of a second epoch.
/// An admission additionally owes the epoch's creation exchange, committed in
/// the same compare-and-set.
/// The audit entry an observed lifecycle flip earns, when a recorded
/// transition's own observation — an expiry crossed, a retirement count
/// reached, a failure streak escalated — moved the goal rather than an
/// operator command.
fn observed_lifecycle_history(
    before: AgentGoalLifecycleStatus,
    after: AgentGoalLifecycleStatus,
) -> Option<AgentTaskHistoryKind> {
    if before == after {
        return None;
    }
    match after {
        AgentGoalLifecycleStatus::Suspended => Some(AgentTaskHistoryKind::GoalSuspended),
        AgentGoalLifecycleStatus::Expired => Some(AgentTaskHistoryKind::GoalExpired),
        AgentGoalLifecycleStatus::Retired => Some(AgentTaskHistoryKind::GoalRetired),
        AgentGoalLifecycleStatus::Active => None,
    }
}

/// Records one wake-audit history entry against the task's current status.
///
/// Audit is history ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)):
/// the entry rides the same recorded transition as the change it describes,
/// so it is emitted exactly once per committed transition and replays never
/// duplicate it.
fn record_wake_history(
    state: &mut AgentTaskState,
    kind: AgentTaskHistoryKind,
    operation_id: &AgentOperationId,
    detail: String,
    now: AgentTimestampMillis,
) {
    let status = state
        .task
        .as_ref()
        .expect("a wake transition proved the task exists")
        .status;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(sequence, kind, operation_id.clone(), status, now)
            .with_detail(detail)
    });
}

/// Records one wake-scoped history row carrying a content fingerprint in the
/// entry's digest slot — the epoch's result fingerprint on its settlement row,
/// the repeated fingerprint on a stagnation detection. History is bounded
/// observability; the controller's durable counters remain the correctness
/// record.
fn record_wake_history_with_digest(
    state: &mut AgentTaskState,
    kind: AgentTaskHistoryKind,
    operation_id: &AgentOperationId,
    detail: String,
    digest: Option<AgentContentDigest>,
    now: AgentTimestampMillis,
) {
    let status = state
        .task
        .as_ref()
        .expect("a wake transition proved the task exists")
        .status;
    state.record_history(|sequence| {
        let entry = AgentTaskHistoryEntry::new(sequence, kind, operation_id.clone(), status, now)
            .with_detail(detail.clone());
        match &digest {
            Some(digest) => entry.with_digest(digest.clone()),
            None => entry,
        }
    });
}

fn admit_wake(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    binding: AgentWakeBinding,
    now: AgentTimestampMillis,
) -> AgentTaskResult<(AgentWakeOutcome, Vec<AgentExchangeEnvelope>)> {
    let expected = binding.admission_operation_id()?;
    if *operation_id != expected {
        return Err(AgentTaskError::WakeOperationMismatch);
    }
    let scope = state.scope.clone();
    let task = continuous_task_mut(state)?;
    if task.goal.as_ref() != Some(binding.goal()) {
        return Err(AgentTaskError::WakeGoalMismatch {
            offered: binding.goal().clone(),
        });
    }
    let spec = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode");
    let current_revision = spec.schedule_revision;
    let policy = spec.wake_policy.policy().clone();
    let (promoted, disposition, lifecycle_flip) = {
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        // Logical time first: an expiry or retirement this delivery's clock
        // has made true is observed by this same recorded transition.
        let lifecycle_before = controller.lifecycle().status();
        controller.observe_lifecycle(&policy, now);
        let lifecycle_flip =
            observed_lifecycle_history(lifecycle_before, controller.lifecycle().status());
        // Oldest parked first: a deferred occurrence takes the free slot
        // ahead of the fresh delivery once the window can pay, so fresher
        // occurrences never leapfrog it.
        let promoted = controller.promote_admittable(&policy, now);
        let disposition = controller.admit(&policy, current_revision, binding, now)?;
        controller.ensure_rewakes(&policy, now);
        (promoted, disposition, lifecycle_flip)
    };
    let mut admitted_wakes = Vec::new();
    if let Some(wake) = promoted {
        admitted_wakes.push(wake);
    }
    if disposition.is_admission() {
        admitted_wakes.push(disposition.wake_id().clone());
    }
    let mut owed = Vec::new();
    for wake in &admitted_wakes {
        owed.push(owe_epoch_creation(&scope, task, wake, now)?);
    }
    // Admission stores at most the bounded slots, but the record must still
    // keep its lifecycle growth reserve free.
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    if let Some(kind) = lifecycle_flip {
        record_wake_history(state, kind, operation_id, "observed".to_string(), now);
        project_gate_onto_goal(state, operation_id, None, now)?;
    }
    record_wake_history(
        state,
        AgentTaskHistoryKind::WakeDispositioned,
        operation_id,
        format!("{} {}", disposition.as_label(), disposition.wake_id()),
        now,
    );
    for wake in &admitted_wakes {
        record_wake_history(
            state,
            AgentTaskHistoryKind::EpochAdmitted,
            operation_id,
            wake.to_string(),
            now,
        );
    }
    Ok((AgentWakeOutcome::Disposition(disposition), owed))
}

/// Releases the active occurrence a completed execution owned, promoting the
/// oldest parked occurrence in the same transition — and owing the promoted
/// occurrence's epoch creation, committed in the same compare-and-set.
///
/// The epoch-result exchange drives this same transition; the command exists
/// so the release is a durable, deduplicated act rather than an implicit
/// consequence of anything resident.
fn complete_wake_occurrence(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    wake: &AgentWakeId,
    now: AgentTimestampMillis,
) -> AgentTaskResult<(AgentWakeOutcome, Vec<AgentExchangeEnvelope>)> {
    let scope = state.scope.clone();
    let task = continuous_task_mut(state)?;
    let policy = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode")
        .wake_policy
        .policy()
        .clone();
    let (release, lifecycle_flip) = {
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        let lifecycle_before = controller.lifecycle().status();
        controller.observe_lifecycle(&policy, now);
        let lifecycle_flip =
            observed_lifecycle_history(lifecycle_before, controller.lifecycle().status());
        let release = controller.release(&policy, wake, now)?;
        controller.ensure_rewakes(&policy, now);
        (release, lifecycle_flip)
    };
    let owed = if let Some(next) = release.admitted_next.clone() {
        vec![owe_epoch_creation(&scope, task, &next, now)?]
    } else {
        Vec::new()
    };
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    if let Some(kind) = lifecycle_flip {
        record_wake_history(state, kind, operation_id, "observed".to_string(), now);
        project_gate_onto_goal(state, operation_id, None, now)?;
    }
    if let Some(next) = &release.admitted_next {
        record_wake_history(
            state,
            AgentTaskHistoryKind::EpochAdmitted,
            operation_id,
            next.to_string(),
            now,
        );
    }
    Ok((AgentWakeOutcome::Release(release), owed))
}

/// Takes a schedule update into force, fencing every parked occurrence the
/// old schedule created.
///
/// The revision must move strictly forward — a replayed update inside the
/// deduplication window answers as a duplicate, and one outside it is refused
/// here, so a restart can never reset the revision
/// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)). A policy
/// update rides the same transition and must also move strictly forward.
fn update_continuous_schedule(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    schedule_revision: ScheduleRevision,
    wake_policy: Option<AgentWakePolicyRevision>,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let task = continuous_task_mut(state)?;
    let AgentGoalMode::Continuous(spec) = &mut task.goal_mode else {
        unreachable!("continuous_task_mut proved the mode");
    };
    if schedule_revision <= spec.schedule_revision {
        return Err(AgentTaskError::ScheduleNotMonotonic {
            offered: schedule_revision,
            current: spec.schedule_revision,
        });
    }
    if let Some(policy) = wake_policy {
        if policy.revision() <= spec.wake_policy.revision() {
            return Err(AgentTaskError::WakePolicyNotNewer {
                offered: policy.revision(),
                current: spec.wake_policy.revision(),
            });
        }
        policy.policy().validate()?;
        spec.wake_policy = policy;
    }
    spec.schedule_revision = schedule_revision;
    let policy_revision = spec.wake_policy.revision();
    // The update may have replaced the policy: observation and the re-wake
    // recomputation both run under the one now in force.
    let policy = spec.wake_policy.policy().clone();
    let (fenced, lifecycle_flip) = {
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        let fenced = controller.apply_schedule_update(schedule_revision);
        let lifecycle_before = controller.lifecycle().status();
        controller.observe_lifecycle(&policy, now);
        let lifecycle_flip =
            observed_lifecycle_history(lifecycle_before, controller.lifecycle().status());
        controller.ensure_rewakes(&policy, now);
        (fenced, lifecycle_flip)
    };
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    if let Some(kind) = lifecycle_flip {
        record_wake_history(state, kind, operation_id, "observed".to_string(), now);
        project_gate_onto_goal(state, operation_id, None, now)?;
    }
    record_wake_history(
        state,
        AgentTaskHistoryKind::ScheduleUpdated,
        operation_id,
        format!(
            "schedule-revision {} policy-revision {} fenced {fenced}",
            schedule_revision.get(),
            policy_revision.get(),
        ),
        now,
    );
    Ok(AgentWakeOutcome::ScheduleUpdated {
        schedule_revision,
        policy_revision,
        fenced,
    })
}

/// Suspends the continuous goal under an operator's authority.
fn suspend_continuous_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected: AgentRevisionNumber,
    reason: Option<String>,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let task = continuous_task_mut(state)?;
    let policy = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode")
        .wake_policy
        .policy()
        .clone();
    let controller = task
        .wake_controller
        .get_or_insert_with(AgentWakeControllerState::new);
    controller.observe_lifecycle(&policy, now);
    let detail = reason.clone().unwrap_or_default();
    let lifecycle_revision = controller.suspend(expected, reason, provenance)?;
    controller.ensure_rewakes(&policy, now);
    let status = controller.lifecycle().status();
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalSuspended,
        operation_id,
        detail,
        now,
    );
    project_gate_onto_goal(state, operation_id, None, now)?;
    Ok(AgentWakeOutcome::Lifecycle {
        status,
        lifecycle_revision,
    })
}

/// Resumes the continuous goal, promoting whatever the suspension parked and
/// owing its epoch creation in this same transition.
fn resume_continuous_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected: AgentRevisionNumber,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<(AgentWakeOutcome, Vec<AgentExchangeEnvelope>)> {
    let scope = state.scope.clone();
    let task = continuous_task_mut(state)?;
    // Any wait the gate door does not own fences this resume: a budget park
    // or a stagnation park is the goal door's wait, and resuming the gate
    // under it would re-admit spending the contract says is parked. The goal
    // door's own resume — `ResumeGoal` — lifts both records together.
    if let Some(wait) = task.goal_state.as_deref().and_then(AgentGoalState::wait) {
        if !matches!(wait, AgentGoalWaitReason::AdmissionSuspended) {
            return Err(AgentTaskError::GoalWaitOwnedElsewhere { code: wait.code() });
        }
    }
    let policy = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode")
        .wake_policy
        .policy()
        .clone();
    let (promoted, status, lifecycle_revision) = {
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        controller.observe_lifecycle(&policy, now);
        let lifecycle_revision = controller.resume(expected, provenance.clone())?;
        let promoted = controller.promote_admittable(&policy, now);
        controller.ensure_rewakes(&policy, now);
        (
            promoted,
            controller.lifecycle().status(),
            lifecycle_revision,
        )
    };
    let owed = match &promoted {
        Some(wake) => vec![owe_epoch_creation(&scope, task, wake, now)?],
        None => Vec::new(),
    };
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalResumed,
        operation_id,
        String::new(),
        now,
    );
    if let Some(wake) = &promoted {
        record_wake_history(
            state,
            AgentTaskHistoryKind::EpochAdmitted,
            operation_id,
            wake.to_string(),
            now,
        );
    }
    project_gate_onto_goal(state, operation_id, Some(&provenance), now)?;
    Ok((
        AgentWakeOutcome::Lifecycle {
            status,
            lifecycle_revision,
        },
        owed,
    ))
}

/// Extends the continuous goal's effective expiry.
fn renew_continuous_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected: AgentRevisionNumber,
    new_expires_at: AgentTimestampMillis,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let task = continuous_task_mut(state)?;
    let policy = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode")
        .wake_policy
        .policy()
        .clone();
    let controller = task
        .wake_controller
        .get_or_insert_with(AgentWakeControllerState::new);
    controller.observe_lifecycle(&policy, now);
    let lifecycle_revision =
        controller.renew(expected, &policy, new_expires_at, provenance, now)?;
    controller.ensure_rewakes(&policy, now);
    let status = controller.lifecycle().status();
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalRenewed,
        operation_id,
        format!("expires-at {}", new_expires_at.as_millis()),
        now,
    );
    Ok(AgentWakeOutcome::Lifecycle {
        status,
        lifecycle_revision,
    })
}

/// Retires the continuous goal under an operator's authority.
fn retire_continuous_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected: AgentRevisionNumber,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let task = continuous_task_mut(state)?;
    let policy = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode")
        .wake_policy
        .policy()
        .clone();
    let controller = task
        .wake_controller
        .get_or_insert_with(AgentWakeControllerState::new);
    controller.observe_lifecycle(&policy, now);
    let lifecycle_revision = controller.retire(expected, provenance.clone())?;
    controller.ensure_rewakes(&policy, now);
    let status = controller.lifecycle().status();
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalRetired,
        operation_id,
        String::new(),
        now,
    );
    project_gate_onto_goal(state, operation_id, Some(&provenance), now)?;
    Ok(AgentWakeOutcome::Lifecycle {
        status,
        lifecycle_revision,
    })
}

/// The gate-suspension reason a goal instituted `Proposed` parks continuous
/// admission under, lifted by [`AgentTaskEntityCommand::ActivateGoal`].
const GOAL_PROPOSED_GATE_REASON: &str = "goal-proposed";

/// Converges the goal contract with the continuous admission gate, one-way —
/// gate to contract — inside the same transition that moved the gate.
///
/// The wake-side [`AgentGoalLifecycleStatus`] is the admission gate; the goal
/// record carries the contract status of
/// [specification 8.1](../../../docs/plans/rakka-agent/spec.md). Where the two
/// overlap, the gate drives: a retirement cancels the goal with the `Retired`
/// reason, an observed expiry expires it, a suspension parks it, and a resume
/// reactivates a goal that was waiting on exactly that suspension — and
/// nothing else, so a goal parked for budget exhaustion is never silently
/// reactivated by a gate resume that granted nothing.
///
/// Every arm must stay infallible by construction: this runs inside exchange
/// transitions — the epoch-result apply among them — where an error becomes a
/// durably recorded refusal that is replayed forever. The guards are what
/// guarantee it today (`Retired`/`Expired` map to reasons every non-terminal
/// status accepts, the park and reactivate arms are status-guarded, and the
/// fence uses the current revision); a future arm inherits the obligation.
fn project_gate_onto_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    provenance: Option<&AgentRevisionProvenance>,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let Some(task) = state.task.as_mut() else {
        return Ok(());
    };
    let Some(gate) = task
        .wake_controller
        .as_ref()
        .map(|controller| controller.lifecycle().status())
    else {
        return Ok(());
    };
    let Some(goal) = task.goal_state.as_deref_mut() else {
        return Ok(());
    };
    if goal.status().is_terminal() {
        return Ok(());
    }
    let row = match gate {
        AgentGoalLifecycleStatus::Retired | AgentGoalLifecycleStatus::Expired => {
            let reason = if gate == AgentGoalLifecycleStatus::Retired {
                AgentGoalTerminalReason::Retired
            } else {
                AgentGoalTerminalReason::ScheduleExpired
            };
            let detail = format!("{} {}", reason.status().as_label(), reason.code());
            goal.decide(
                AgentGoalDecision {
                    reason,
                    evaluation: None,
                    provenance: provenance.cloned().map(Box::new),
                    expected_status_revision: goal.status_revision(),
                },
                now,
            )?;
            Some((AgentTaskHistoryKind::GoalDecided, detail))
        }
        AgentGoalLifecycleStatus::Suspended if goal.status() == AgentGoalStatus::Active => {
            goal.park(AgentGoalWaitReason::AdmissionSuspended, now)?;
            Some((
                AgentTaskHistoryKind::GoalParked,
                AgentGoalWaitReason::AdmissionSuspended.code().to_string(),
            ))
        }
        AgentGoalLifecycleStatus::Active
            if matches!(goal.wait(), Some(AgentGoalWaitReason::AdmissionSuspended)) =>
        {
            let Some(provenance) = provenance else {
                return Ok(());
            };
            goal.reactivate(goal.status_revision(), provenance.clone(), now)?;
            Some((AgentTaskHistoryKind::GoalReactivated, String::new()))
        }
        _ => None,
    };
    if let Some((kind, detail)) = row {
        state.updated_at = now;
        record_wake_history(state, kind, operation_id, detail, now);
    }
    Ok(())
}

/// The goal-scope exhaustion the root task's own ledger reports right now:
/// what is spoken for — consumption and still-outstanding child escrow
/// together — against the goal's allocation.
fn goal_scope_exhaustion(
    task: &AgentTask,
    dimension: AgentBudgetDimension,
) -> AgentBudgetExhaustion {
    let limit = task.escrow.allocation().get(dimension).unwrap_or(0);
    let available = task.escrow.available(dimension).unwrap_or(0);
    AgentBudgetExhaustion::new(dimension, limit, limit.saturating_sub(available))
}

/// The conserved dimension whose assignment refusal no settlement can relieve,
/// when there is one: zero available headroom and no outstanding child escrow
/// in that dimension whose return could restore any.
fn permanent_budget_exhaustion(task: &AgentTask) -> Option<AgentBudgetDimension> {
    if task.escrow.outstanding().count() >= AGENT_ESCROW_CHILD_CAPACITY {
        return None;
    }
    let request = task.definition.run_allocation_request();
    let affordable = request.narrowed_to(&task.escrow.available_allocation());
    let dimension = affordable.first_empty_for(&request)?;
    let outstanding_holds = task
        .escrow
        .outstanding()
        .any(|(_, escrow)| escrow.allocated().get(dimension).unwrap_or(0) > 0);
    (!outstanding_holds).then_some(dimension)
}

/// Applies the goal's persisted budget-exhaustion policy
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): hard
/// ceilings deterministically park, escalate, or terminate per persisted
/// policy), inside the transition that observed the exhaustion.
///
/// Idempotent: a goal already parked on the same reason, or already terminal,
/// moves nothing — the consult runs inside command transitions and settle
/// passes alike, and an unchanged exhaustion is not a new fact.
fn apply_goal_exhaustion(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    dimension: AgentBudgetDimension,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    let Some(goal) = task.goal_state.as_deref() else {
        return Ok(());
    };
    if goal.status().is_terminal() {
        return Ok(());
    }
    let action = goal.spec().spec().exhaustion.action_for(dimension);
    let exhaustion = goal_scope_exhaustion(task, dimension);
    match action {
        AgentGoalExhaustionAction::Park | AgentGoalExhaustionAction::Escalate => {
            let reason = match action {
                AgentGoalExhaustionAction::Park => {
                    AgentGoalWaitReason::BudgetExhausted { exhaustion }
                }
                _ => AgentGoalWaitReason::Escalated { exhaustion },
            };
            let escalation = task
                .goal_state
                .as_deref()
                .and_then(|goal| goal.spec().spec().escalation.clone());
            let goal = task
                .goal_state
                .as_deref_mut()
                .expect("the goal record was read above");
            // A Proposed goal spends nothing already, so there is nothing to
            // park; only an authorized goal parks.
            if !matches!(
                goal.status(),
                AgentGoalStatus::Active | AgentGoalStatus::Waiting
            ) {
                return Ok(());
            }
            let before = goal.status_revision();
            let code = reason.code();
            let moved = goal.park(reason, now)? != before;
            if !moved {
                return Ok(());
            }
            // For a continuous goal the park closes admission in the same
            // compare-and-set, so triggers coalesce and nothing spends — the
            // same durable suspension a failure escalation makes.
            if task.goal_mode.is_continuous() {
                task.wake_controller
                    .get_or_insert_with(AgentWakeControllerState::new)
                    .suspend_by_policy(code);
            }
            let detail = match (action, escalation) {
                (AgentGoalExhaustionAction::Escalate, Some(policy)) => {
                    format!("{code} {exhaustion} escalation {policy}")
                }
                _ => format!("{code} {exhaustion}"),
            };
            state.updated_at = now;
            record_wake_history(
                state,
                AgentTaskHistoryKind::GoalParked,
                operation_id,
                detail,
                now,
            );
        }
        AgentGoalExhaustionAction::Terminate => {
            let goal = task
                .goal_state
                .as_deref_mut()
                .expect("the goal record was read above");
            let reason = AgentGoalTerminalReason::BudgetExhausted { exhaustion };
            let detail = format!(
                "{} {} {exhaustion}",
                reason.status().as_label(),
                reason.code()
            );
            goal.decide(
                AgentGoalDecision {
                    reason,
                    evaluation: None,
                    provenance: None,
                    expected_status_revision: goal.status_revision(),
                },
                now,
            )?;
            // Admission closes with the contract, and the root task ends with
            // the goal: durability does not authorize unbounded compute, and a
            // policy that says terminate means the whole scope stops
            // ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
            if task.goal_mode.is_continuous() {
                if let Some(controller) = task.wake_controller.as_mut() {
                    controller.retire_by_policy();
                }
            }
            state.updated_at = now;
            record_wake_history(
                state,
                AgentTaskHistoryKind::GoalDecided,
                operation_id,
                detail,
                now,
            );
            terminate(
                state,
                operation_id,
                AgentTaskTerminalReason::GoalBudgetExhausted { exhaustion },
                now,
            )?;
        }
    }
    Ok(())
}

/// Applies the goal's persisted stagnation policy
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md): bounded
/// repetition and no-progress epochs continue, wait, escalate, or terminate
/// under deterministic policy), inside the epoch-settle transition that
/// observed the trip.
///
/// Every arm is infallible against a record this binary wrote — the exhaustion
/// executor's obligation, inherited because this runs inside an exchange
/// transition where an error becomes a durably replayed refusal. The
/// detection row is always recorded; a `Continue` records nothing else. The
/// gate-side suspension or retirement already landed beside the release; the
/// idempotent re-application here keeps the executor total on every path.
fn apply_goal_stagnation(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    trigger: AgentStagnationTrigger,
    epochs: u32,
    digest: Option<AgentContentDigest>,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    let Some(goal) = task.goal_state.as_deref() else {
        return Ok(());
    };
    if goal.status().is_terminal() {
        return Ok(());
    }
    // A Proposed goal spends nothing already; only an authorized or parked
    // goal is acted on. The streak was still accounted durably — it is a fact
    // about the epochs — but a suppressed trip is deliberately *not* counted,
    // because it leaves no detection row for the count to correspond to.
    if !matches!(
        goal.status(),
        AgentGoalStatus::Active | AgentGoalStatus::Waiting
    ) {
        return Ok(());
    }
    let action = goal.spec().spec().stagnation_policy.action_for(trigger);
    let escalation = goal.spec().spec().escalation.clone();
    let stagnation_ref = goal.spec().spec().stagnation.clone();
    let detection = format!("{} {epochs} {}", trigger.code(), action.as_label());
    // Past every guard, so the durable counter moves exactly with the
    // detection row recorded below and the metric counts what happened.
    if let Some(controller) = task.wake_controller.as_mut() {
        controller.record_stagnation_trip(trigger);
    }

    let follow_up = match action {
        // Observe-only; `Replan` is refused at spec validation, so its arm is
        // defensively identical rather than silently a park.
        AgentGoalStagnationAction::Continue | AgentGoalStagnationAction::Replan => None,
        AgentGoalStagnationAction::Wait | AgentGoalStagnationAction::Escalate => {
            let goal = task
                .goal_state
                .as_deref_mut()
                .expect("the goal record was read above");
            let before = goal.status_revision();
            let reason = AgentGoalWaitReason::Stagnant { trigger };
            let code = reason.code();
            let moved = goal.park(reason, now)? != before;
            if moved {
                if task.goal_mode.is_continuous() {
                    task.wake_controller
                        .get_or_insert_with(AgentWakeControllerState::new)
                        .suspend_by_policy(code);
                }
                let detail = match (action, escalation, stagnation_ref) {
                    (AgentGoalStagnationAction::Escalate, Some(policy), Some(stagnation)) => {
                        format!("{code} {trigger} {epochs} escalation {policy} policy {stagnation}")
                    }
                    (AgentGoalStagnationAction::Escalate, Some(policy), None) => {
                        format!("{code} {trigger} {epochs} escalation {policy}")
                    }
                    _ => format!("{code} {trigger} {epochs}"),
                };
                Some((AgentTaskHistoryKind::GoalParked, detail, false))
            } else {
                None
            }
        }
        AgentGoalStagnationAction::Terminate => {
            let goal = task
                .goal_state
                .as_deref_mut()
                .expect("the goal record was read above");
            let reason = AgentGoalTerminalReason::Stagnant { trigger, epochs };
            let detail = format!(
                "{} {} {trigger} {epochs}",
                reason.status().as_label(),
                reason.code()
            );
            goal.decide(
                AgentGoalDecision {
                    reason,
                    evaluation: None,
                    provenance: None,
                    expected_status_revision: goal.status_revision(),
                },
                now,
            )?;
            // Admission closes with the contract, and the root task ends with
            // the goal — the exhaustion terminate's posture: durability does
            // not authorize unbounded compute.
            if task.goal_mode.is_continuous() {
                if let Some(controller) = task.wake_controller.as_mut() {
                    controller.retire_by_policy();
                }
            }
            Some((AgentTaskHistoryKind::GoalDecided, detail, true))
        }
    };

    state.updated_at = now;
    record_wake_history_with_digest(
        state,
        AgentTaskHistoryKind::GoalStagnationDetected,
        operation_id,
        detection,
        digest,
        now,
    );
    if let Some((kind, detail, terminates)) = follow_up {
        record_wake_history(state, kind, operation_id, detail, now);
        if terminates {
            terminate(
                state,
                operation_id,
                AgentTaskTerminalReason::GoalStagnant { trigger, epochs },
                now,
            )?;
        }
    }
    Ok(())
}

/// Activates a `Proposed` goal under an authorized command, lifting the
/// proposed-goal admission park when this is a continuous root.
fn activate_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected: AgentRevisionNumber,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let task = state.task_mut()?;
    let goal = task
        .goal_state
        .as_deref_mut()
        .ok_or(AgentTaskError::GoalNotCoordinated)?;
    // Every goal entry point observes the deadline: a passed one expires the
    // goal, and the activation below then refuses it as terminal.
    goal.observe_deadline(now);
    goal.activate(expected, provenance.clone(), now)?;
    let mut promoted = None;
    let mut owed = Vec::new();
    if task.goal_mode.is_continuous() {
        let policy = task
            .goal_mode
            .continuous()
            .expect("the mode was just matched")
            .wake_policy
            .policy()
            .clone();
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        // Lift only the park the proposed goal itself made: an operator's own
        // suspension stays until the operator resumes it.
        if controller.lifecycle().status() == AgentGoalLifecycleStatus::Suspended
            && controller.lifecycle().suspended_reason() == Some(GOAL_PROPOSED_GATE_REASON)
        {
            let revision = controller.lifecycle().lifecycle_revision();
            controller.resume(revision, provenance)?;
            promoted = controller.promote_admittable(&policy, now);
            controller.ensure_rewakes(&policy, now);
            if let Some(wake) = &promoted {
                owed.push(owe_epoch_creation(&scope, task, wake, now)?);
            }
        }
    }
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalActivated,
        operation_id,
        String::new(),
        now,
    );
    if let Some(wake) = &promoted {
        record_wake_history(
            state,
            AgentTaskHistoryKind::EpochAdmitted,
            operation_id,
            wake.to_string(),
            now,
        );
    }
    // The gate may still be suspended by an operator, in which case the goal
    // parks right back — the projection converges the two records either way.
    project_gate_onto_goal(state, operation_id, None, now)?;
    Ok(owed)
}

/// The one decision core both ingresses share — the open command and the
/// goal-evaluation exchange: observe the deadline, decide, close a continuous
/// gate with the contract, enforce the bound whole, and record the decision
/// row. Every fence the contract owns — status revision, criteria revision,
/// evaluator identity, required evidence — runs inside
/// [`AgentGoalState::decide`], so the two ingresses can never diverge on what
/// a decision must prove.
fn apply_goal_decision(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    decision: AgentGoalDecision,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    let goal = task
        .goal_state
        .as_deref_mut()
        .ok_or(AgentTaskError::GoalNotCoordinated)?;
    goal.observe_deadline(now);
    let outcome = decision.reason.status();
    let code = decision.reason.code();
    goal.decide(decision, now)?;
    if task.goal_mode.is_continuous() {
        if let Some(controller) = task.wake_controller.as_mut() {
            controller.retire_by_policy();
        }
    }
    // The decision spends growth headroom admission reserved, so it checks
    // the full bound: `decide` truncated the reason strings, but the
    // evaluation reference carries caller-sized artifact fields the record
    // must still be able to hold.
    task.check_bounds(0)?;
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalDecided,
        operation_id,
        format!("{} {code}", outcome.as_label()),
        now,
    );
    Ok(())
}

/// Records a terminal goal decision under an authorized command; a terminal
/// continuous goal closes admission with the contract.
///
/// Under a configured evaluator, a criteria decision may not enter here at
/// all: the goal-evaluation exchange — whose sender fence and digest-bearing
/// record are the attestation — is the only ingress
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)). Without a
/// configured evaluator the 4.1 contract stands: any authorized commander may
/// record a criteria decision, still revision- and evidence-fenced.
fn record_goal_decision(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    decision: AgentGoalDecision,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    {
        let task = state.task_mut()?;
        let goal = task
            .goal_state
            .as_deref_mut()
            .ok_or(AgentTaskError::GoalNotCoordinated)?;
        // Every goal entry point observes the deadline, and this one observes
        // it *before* the attestation fence: an expired goal is terminal, and
        // refusing it `task-goal-decision-unattested` would answer the wrong
        // question — the decision is not unattested, the goal is over. The
        // now-terminal goal falls through to `decide`, which refuses it
        // `goal-terminal`. Durability of the expiry itself stays the settle
        // pass's job, as it is for every other refused decision.
        goal.observe_deadline(now);
        if !goal.status().is_terminal()
            && decision.reason.requires_evaluation()
            && goal.spec().spec().evaluator.is_some()
        {
            return Err(AgentTaskError::GoalDecisionUnattested);
        }
    }
    apply_goal_decision(state, operation_id, decision, now)
}

/// Accepts a revised success criteria under an authorized command
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md): if the goal
/// changes, evaluations against the old revision are invalid — which the
/// decision door's existing staleness fence enforces, so this command carries
/// no cancellation machinery at all).
fn revise_goal_criteria(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected_criteria_revision: AgentRevisionNumber,
    source: AgentGoalCriteriaSource,
    digest: Option<AgentContentDigest>,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    let goal = task
        .goal_state
        .as_deref_mut()
        .ok_or(AgentTaskError::GoalNotCoordinated)?;
    // Every goal entry point observes the deadline: a passed one expires the
    // goal, and the revision below then refuses it as terminal.
    goal.observe_deadline(now);
    let revision = goal.revise_criteria(expected_criteria_revision, source, digest, provenance)?;
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalCriteriaRevised,
        operation_id,
        revision.to_string(),
        now,
    );
    Ok(())
}

/// Reactivates a `Waiting` goal under an authorized command — the un-park door
/// of the goal-scope exhaustion policy. One command widens the ledger under
/// the definition ceilings, reactivates the contract, and lifts the
/// goal-driven admission park, in one fenced compare-and-set.
fn resume_goal(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    expected: AgentRevisionNumber,
    top_up: Option<AgentBudgetAllocation>,
    provenance: AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let task = state.task_mut()?;
    let goal = task
        .goal_state
        .as_deref_mut()
        .ok_or(AgentTaskError::GoalNotCoordinated)?;
    goal.observe_deadline(now);
    // What the park recorded, before reactivation clears it. An admission
    // suspension is the gate door's wait: resuming it here would lift a
    // suspension this command never examined.
    let wait = goal.wait().cloned();
    let wait_code = wait.as_ref().map(AgentGoalWaitReason::code);
    if matches!(wait, Some(AgentGoalWaitReason::AdmissionSuspended)) {
        return Err(AgentTaskError::GoalWaitOwnedElsewhere {
            code: AgentGoalWaitReason::AdmissionSuspended.code(),
        });
    }
    let parked = goal
        .wait()
        .and_then(AgentGoalWaitReason::exhaustion)
        .copied();
    goal.reactivate(expected, provenance.clone(), now)?;
    if let Some(additional) = &top_up {
        // The widening is the owner's parent-scope allocation decision; the
        // definition ceiling still bounds it
        // ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
        let ceiling = AgentBudgetAllocation::from_ceilings(&task.definition.budgets);
        task.escrow.widen(additional, &ceiling);
    }
    if let Some(exhaustion) = parked {
        // The resume must actually relieve what parked the goal; resuming
        // into the same exhaustion would only re-park on the next decision.
        if task.escrow.available(exhaustion.dimension) == Some(0) {
            return Err(AgentTaskError::GoalResumeUnrelieved {
                exhaustion: goal_scope_exhaustion(task, exhaustion.dimension),
            });
        }
    }
    // The structured reason, not its label: the detector reset below is a
    // durable state change, and a code two reasons could share must not decide
    // it. `wait_code` stays for the gate's own suspension-reason comparison,
    // which is string-keyed on the controller side.
    let stagnant = matches!(wait, Some(AgentGoalWaitReason::Stagnant { .. }));
    let mut promoted = None;
    let mut owed = Vec::new();
    if task.goal_mode.is_continuous() {
        let policy = task
            .goal_mode
            .continuous()
            .expect("the mode was just matched")
            .wake_policy
            .policy()
            .clone();
        let controller = task
            .wake_controller
            .get_or_insert_with(AgentWakeControllerState::new);
        // Lift only the park the goal's own policy made — exhaustion or
        // stagnation alike.
        let goal_driven = controller.lifecycle().status() == AgentGoalLifecycleStatus::Suspended
            && controller.lifecycle().suspended_reason() == wait_code;
        if goal_driven {
            let revision = controller.lifecycle().lifecycle_revision();
            controller.resume(revision, provenance)?;
            promoted = controller.promote_admittable(&policy, now);
            controller.ensure_rewakes(&policy, now);
            if let Some(wake) = &promoted {
                owed.push(owe_epoch_creation(&scope, task, wake, now)?);
            }
        }
        if stagnant {
            // The one deliberate non-progress reset
            // ([specification 8.3](../../../docs/plans/rakka-agent/spec.md):
            // never silently reset): the authorized resume clears the
            // detector, with provenance on the contract and the reset in
            // history — otherwise the very next identical epoch would
            // instantly re-park, and the operator who knows the repetition is
            // benign would have no relief valve.
            task.wake_controller
                .get_or_insert_with(AgentWakeControllerState::new)
                .reset_stagnation();
        }
    }
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    let mut detail_parts = Vec::new();
    if top_up.is_some() {
        detail_parts.push("top-up");
    }
    if stagnant {
        detail_parts.push("stagnation-reset");
    }
    record_wake_history(
        state,
        AgentTaskHistoryKind::GoalReactivated,
        operation_id,
        detail_parts.join(" "),
        now,
    );
    if let Some(wake) = &promoted {
        record_wake_history(
            state,
            AgentTaskHistoryKind::EpochAdmitted,
            operation_id,
            wake.to_string(),
            now,
        );
    }
    // The gate may still be suspended by an operator, in which case the goal
    // parks right back — the projection converges the two records either way.
    project_gate_onto_goal(state, operation_id, None, now)?;
    Ok(owed)
}

/// Why the task cannot escrow a run right now, when it cannot.
///
/// The affordability answer is derived from the ledger without touching it, so
/// both the decision and the settle pass that predicts the decision ask the same
/// question of the same record. It predicts *every* way `open_child` can refuse
/// — an exhausted dimension and a full child set alike — so the decision path
/// records a stable refusal and stays assignable rather than failing the
/// transition on an error the prediction never saw.
fn task_budget_refusal(task: &AgentTask) -> Option<(AgentAssignmentRefusalReason, String)> {
    if task.escrow.outstanding().count() >= AGENT_ESCROW_CHILD_CAPACITY {
        return Some((
            AgentAssignmentRefusalReason::BudgetUnavailable,
            format!(
                "the task cannot escrow a run: its ledger already holds \
                 {AGENT_ESCROW_CHILD_CAPACITY} outstanding children"
            ),
        ));
    }
    let request = task.definition.run_allocation_request();
    let affordable = request.narrowed_to(&task.escrow.available_allocation());
    let dimension = affordable.first_empty_for(&request)?;
    // The exhaustion reports what is *spoken for* — consumption and
    // still-outstanding child escrow together — because that is what the
    // headroom is measured against. Reporting consumption alone would tell an
    // operator "consumed 0 of 10" about a ledger whose 10 are all escrowed to
    // a child that has not settled yet.
    let limit = task.escrow.allocation().get(dimension).unwrap_or(0);
    let available = task.escrow.available(dimension).unwrap_or(0);
    let exhaustion = AgentBudgetExhaustion::new(dimension, limit, limit.saturating_sub(available));
    Some((
        AgentAssignmentRefusalReason::BudgetUnavailable,
        format!("the task cannot escrow a run: {exhaustion}"),
    ))
}

/// The refusal one assignment decision would record now, if it would record
/// one.
///
/// It mirrors [`decide_assignment`]'s order, so the settle pass can predict the
/// decision without running it. It deliberately reports nothing when the
/// decision would *terminate* the task: an exhausted assignment limit is a
/// change worth a transition, not a refusal to deduplicate against.
fn pending_assignment_refusal(
    task: &AgentTask,
    readiness: &AgentAssignmentReadiness,
) -> Option<(AgentAssignmentRefusalReason, String)> {
    if let Some(refusal) = readiness.refusal() {
        return Some(refusal);
    }
    if task.assignments >= task.definition.limits.max_assignments {
        return None;
    }
    task_budget_refusal(task)
}

/// Whether the refusal on record already states this fact, so recording it
/// again would add nothing.
fn assignment_refusal_is_current(
    task: &AgentTask,
    agent: &AgentId,
    reason: AgentAssignmentRefusalReason,
    detail: &str,
) -> bool {
    task.last_refusal
        .as_ref()
        .is_some_and(|last| last.agent == *agent && last.reason == reason && last.detail == detail)
}

/// Records one assignment refusal and stays assignable.
///
/// Failing closed here is not failing the task: resuming the agent, widening
/// its envelope, re-admitting it, or an earlier generation settling its escrow
/// all let the next decision succeed without re-creating anything. The refusal
/// deduplicates against the one on record, because the decision runs inside the
/// command transition and again on every settle pass, and the same agent
/// refused for the same reason is not a new fact.
fn refuse_assignment(
    state: &mut AgentTaskState,
    readiness: &AgentAssignmentReadiness,
    reason: AgentAssignmentRefusalReason,
    detail: impl Into<String>,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let detail = bounded_detail(detail);
    let task = state.task_mut()?;
    if assignment_refusal_is_current(task, &readiness.agent, reason, &detail) {
        // Re-recording it — on the settle pass of the same command, or on every
        // recovery sweep while the agent stays refused — would grow the
        // append-only history without a new fact.
        return Ok(None);
    }

    task.last_refusal = Some(AgentAssignmentRefusal {
        agent: readiness.agent.clone(),
        reason,
        detail: detail.clone(),
        refused_at: now,
    });
    let status = task.status;
    let operation_id = assignment_operation_id(&scope, task.assignment_generation.next())?;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::AssignmentRefused,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(format!("{}: {detail}", reason.code()))
    });
    Ok(None)
}

/// Decides one assignment against the agent facts the entity read from durable
/// state, and returns the exchanges the decision owes: the run-creation
/// command of a decided assignment, or — when the decision exhausts the
/// task's assignments instead — the terminal reports the newly terminal task
/// owes upward, from this same compare-and-set.
///
/// The transition is idempotent on the task's own state: a task that already has
/// an assignment is not assignable, so a replay produces no second generation.
fn decide_assignment(
    state: &mut AgentTaskState,
    readiness: &AgentAssignmentReadiness,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let task = state.task_mut()?;
    if !task.awaits_assignment() {
        return Ok(Vec::new());
    }

    // The handoff single-attempt rule ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)):
    // the transfer's target gets exactly one assignment-generation attempt,
    // and every definitive refusal of it resolves the handoff — restoring the
    // stashed source — rather than parking the task in a refusal loop or
    // terminalizing it over a source run that is fenced-alive awaiting the
    // resolution.
    let handoff_pending = task_handoff_pending(task);
    // The claim single-attempt rule mirrors the handoff's: the claimant gets
    // exactly one assignment-generation attempt, and every definitive
    // refusal of it resolves the claim — reopening the board entry through
    // the owed claim result — rather than parking the task in a refusal
    // loop.
    let team_claim_pending = task_team_claim_pending(task);
    if let Some((reason, detail)) = readiness.refusal() {
        if handoff_pending {
            return resolve_handoff_refusal(state, reason.code(), now);
        }
        if team_claim_pending {
            return resolve_team_claim_refusal(state, reason.code(), now);
        }
        return Ok(refuse_assignment(state, readiness, reason, detail, now)?
            .into_iter()
            .collect());
    }

    if task.assignments >= task.definition.limits.max_assignments {
        if handoff_pending {
            return resolve_handoff_refusal(state, "handoff-assignments-exhausted", now);
        }
        if team_claim_pending {
            // The claim resolves rather than the task terminating: the
            // board's members decide what an unassignable entry means, and
            // the claim ceiling bounds how often they may ask.
            return resolve_team_claim_refusal(state, "team-claim-assignments-exhausted", now);
        }
        let assignments = task.assignments;
        let operation_id = assignment_operation_id(&scope, task.assignment_generation)?;
        terminate(
            state,
            &operation_id,
            AgentTaskTerminalReason::AssignmentsExhausted { assignments },
            now,
        )?;
        // The exhaustion closed this task's ledger — every refused
        // generation's escrow was released at its settle, so nothing is
        // outstanding — and a delegated child's parent is parked awaiting
        // exactly this outcome: the reports the terminal task owes upward
        // are accurate now, and owed from this same compare-and-set.
        return owed_child_reports(state, now, 0);
    }

    let (agent_definition_revision, agent_settings_revision) =
        match (readiness.definition_revision, readiness.settings_revision) {
            (Some(definition), Some(settings)) => (definition, settings),
            _ => return Err(AgentTaskError::NotReady),
        };

    let generation = task.assignment_generation.next();
    let run = run_id_for_assignment(scope.task(), generation)?;
    let run_scope =
        AgentRunScope::new(scope.tenant().clone(), readiness.agent.clone(), run.clone())?;
    let operation_id = assignment_operation_id(&scope, generation)?;

    // The escrow debit, inside the creating transition
    // ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). It is
    // decided against the same record it is written to, which is what makes
    // oversubscription impossible without any distributed transaction: the task
    // is the ledger's only writer, and the assignment that carries the grant
    // commits with the debit.
    //
    // Affordability is settled before anything is debited, so the refusal path
    // leaves the ledger exactly as it found it. Refusing is not failing: the
    // task stays assignable, and an earlier generation's settlement may restore
    // the headroom, so this takes the same path a suspended agent's refusal
    // takes.
    if let Some((reason, detail)) = task_budget_refusal(task) {
        if handoff_pending {
            // The source's escrow child is still open — settlement travels
            // only post-terminal, and the source is fenced non-terminal — so
            // an exact-fit budget cannot afford the target's generation. Fail
            // closed: the handoff resolves refused and the source resumes
            // with the failed tool result, rather than the task parking over
            // a fenced-alive source (the user-approved posture; a reserved
            // handoff allowance is a recorded policy hook, not this slice).
            return resolve_handoff_refusal(state, "handoff-budget-unaffordable", now);
        }
        if team_claim_pending {
            // Fail closed exactly as the handoff does: the claim resolves
            // refused and the board reopens, rather than the task parking a
            // claim over headroom that may never return.
            return resolve_team_claim_refusal(state, "team-claim-budget-unaffordable", now);
        }
        // A refusal no settlement can relieve — zero headroom and nothing
        // outstanding to return — is the goal scope's own exhaustion, and the
        // goal's persisted policy decides what it does about it.
        let permanent = permanent_budget_exhaustion(task);
        let refusal = refuse_assignment(state, readiness, reason, detail, now)?;
        if let Some(dimension) = permanent {
            apply_goal_exhaustion(state, &operation_id, dimension, now)?;
        }
        return Ok(refusal.into_iter().collect());
    }
    let request = task.definition.run_allocation_request();
    let allocation = task
        .escrow
        .open_child(AgentEscrowChildId::for_run(&run)?, &request)?;
    let budget = AgentBudgetGrant::new(allocation, *task.escrow.limits());

    let assignment = AgentTaskAssignment {
        generation,
        agent: readiness.agent.clone(),
        run,
        status: AgentAssignmentStatus::Offered,
        agent_definition_revision,
        agent_settings_revision,
        operation_id: operation_id.clone(),
        assigned_at: now,
    };

    let command = AgentRunAssignment {
        task: scope.clone(),
        run: run_scope.clone(),
        generation,
        definition: task.definition.clone(),
        input: task.input.clone(),
        goal: task.goal.clone(),
        budget,
        agent_definition_revision,
        agent_settings_revision,
        delegation: delegation_envelope_for(task),
        assigned_at: now,
    };
    // The payload is bounded by the substrate. A definition and input that do
    // not fit belong behind an artifact reference, and the decision fails closed
    // rather than persisting an assignment whose command can never be delivered.
    let payload = AgentExchangePayload::encode(AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE, &command)?;
    // The assignment carries the ingress's causal chain onward: the segment
    // that created the task is the cause of the assignment it decided
    // ([specification 17.5]). Stamping the empty context is stamping nothing.
    let envelope = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::Assignment,
        AgentEntityAddress::Task(scope),
        AgentEntityAddress::Run(run_scope),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        now,
    )?
    .with_telemetry(task.telemetry.clone());

    task.assignment_generation = generation;
    task.assignments += 1;
    task.status = AgentTaskStatus::Assigned;
    task.last_refusal = None;
    task.assignment = Some(assignment.clone());
    // A pending handoff records the generation it minted, in this same
    // compare-and-set: the provenance's target generation is what the
    // acceptance flip and the result notice key on.
    if let Some(handoff) = task.handoff.as_deref_mut() {
        if !handoff.is_settled()
            && handoff.target == assignment.agent
            && handoff.target_generation.is_none()
        {
            handoff.target_generation = Some(generation);
        }
    }
    // A pending board claim records the generation it minted the same way:
    // the claim's target generation is what the acceptance flip and the
    // claim-result notice key on.
    if let Some(claim) = task.team_claim.as_deref_mut() {
        if !claim.is_settled()
            && claim.member == assignment.agent
            && claim.target_generation.is_none()
        {
            claim.target_generation = Some(generation);
        }
    }
    // The assignment spends growth headroom admission reserved, so it checks
    // the full bound: it cannot fail for a record admission accepted.
    task.check_bounds(0)?;

    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::AssignmentDecided,
            operation_id.clone(),
            AgentTaskStatus::Assigned,
            now,
        )
        .with_assignment(&assignment)
    });
    Ok(vec![envelope])
}

/// Validates one proposed result by deterministic rules alone, and records the
/// durable decision.
///
/// Nothing here performs I/O: a model-assisted or external evaluator is a
/// durable effect that returns evidence to the task, never a call from inside
/// this transition ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
fn apply_result_proposal(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let proposal: AgentTaskResultProposal = match envelope
        .payload()
        .decode(AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE)
    {
        Ok(proposal) => proposal,
        Err(error) => return refuse(state, error.code(), error.to_string()),
    };

    if proposal.causation_id.as_str().len() > AGENT_IDENTITY_MAX_LENGTH {
        // The causation id is the one externally supplied field a rejection
        // persists whose type does not bound itself, and the admission-time
        // growth reserve is sized against bounded fields only. Refusing costs
        // the run nothing; a corrected proposal is a new decision.
        return refuse(
            state,
            "proposal-causation-too-long",
            format!(
                "the proposal's causation id is {} bytes, and the task persists at most \
                 {AGENT_IDENTITY_MAX_LENGTH}",
                proposal.causation_id.as_str().len()
            ),
        );
    }

    let Some(task) = state.task.as_ref() else {
        return refuse(
            state,
            "task-not-created",
            "the task does not exist".to_string(),
        );
    };

    if task.status.is_terminal() {
        return refuse(
            state,
            "task-terminal",
            format!("the task is already {}", task.status),
        );
    }

    // The assignment fence. A run of a superseded generation may not complete the
    // public task, and its proposal must not consume the live run's rejection
    // budget ([specification 9.3](../../../docs/plans/rakka-agent/spec.md)).
    let Some(assignment) = task.assignment.as_ref() else {
        return refuse(
            state,
            "task-not-assigned",
            "the task has no current assignment".to_string(),
        );
    };
    if assignment.generation != proposal.generation || assignment.run != proposal.run {
        return refuse(
            state,
            AGENT_TASK_REFUSAL_STALE_GENERATION,
            format!(
                "generation {} is not the current generation {}",
                proposal.generation, assignment.generation
            ),
        );
    }

    // The cancellation fence ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)):
    // a requested cancellation immediately stops new validation, so a run
    // racing its proposal against the run-cancel exchange converges on the
    // wind-down whichever message arrives first. Definitive — the run's
    // settle rule winds it down exactly as a terminal task's refusal would.
    if task.cancellation.is_some() {
        return refuse(
            state,
            AGENT_TASK_REFUSAL_CANCEL_REQUESTED,
            "the task's cancellation is propagating; no proposal can be validated".to_string(),
        );
    }

    let digest = proposal.content.digest();
    let proposal_id = proposal.proposal_id.clone();
    let status_before = task.status;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultProposed,
            proposal_id.clone(),
            status_before,
            now,
        )
        .with_digest(digest.clone())
        .with_detail(proposal.run.to_string())
    });

    let task = state.task.as_ref().expect("the task exists on this path");
    match task
        .definition
        .validate_result(&AgentResultClaim::from_proposal(&proposal))
    {
        Ok(()) => accept_result(state, &proposal, digest, now),
        Err(cause) => reject_result(state, &proposal, digest, cause, now),
    }
}

/// How the origin-neutral acceptance core resolved.
enum ResultAcceptanceCore {
    /// The result committed and the task terminalized `Completed`.
    Accepted,
    /// The accepted result would push the materialized record past its
    /// bound; the write was rolled back and nothing committed.
    Bounds(AgentTaskError),
    /// The task turned terminal under the acceptance — a defensive shield
    /// the fence ladders make unreachable.
    AlreadyTerminal,
}

/// Commits one validated result: the accepted-result cell, the `Completed`
/// terminal, and the `ResultAccepted` history row.
///
/// Origin-neutral — the run's proposal wrapper passes no principal and the
/// human submission passes its authenticated one — so both commit the
/// identical transition. The caller ensures the task exists.
fn accept_result_core(
    state: &mut AgentTaskState,
    accepted: &AgentAcceptedResult,
    principal: Option<&str>,
    now: AgentTimestampMillis,
) -> ResultAcceptanceCore {
    let bounded = {
        let task = state.task.as_mut().expect("the task exists on this path");
        task.accepted_result = Some(Box::new(accepted.clone()));
        // An accepted result is not covered by the admission reserve: unlike
        // an assignment or a rejection, an oversized one has a graceful retry
        // — the submitter resubmits it behind an artifact reference.
        task.check_bounds(0)
    };
    if let Err(error) = bounded {
        // The accepted result would push the materialized record past its bound.
        // Refusing is the only safe answer: a task must never persist a record it
        // cannot bound, and the submitter must resubmit the result behind an
        // artifact reference.
        state
            .task
            .as_mut()
            .expect("the task exists on this path")
            .accepted_result = None;
        return ResultAcceptanceCore::Bounds(error);
    }

    let proposal_id = accepted.proposal_id.clone();
    if terminate(
        state,
        &proposal_id,
        AgentTaskTerminalReason::ResultAccepted,
        now,
    )
    .is_err()
    {
        return ResultAcceptanceCore::AlreadyTerminal;
    }

    let digest = accepted.digest.clone();
    let principal = principal.map(str::to_string);
    state.record_history(|sequence| {
        let entry = AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultAccepted,
            proposal_id.clone(),
            AgentTaskStatus::Completed,
            now,
        )
        .with_digest(digest.clone());
        match &principal {
            Some(principal) => entry.with_principal(principal.clone()),
            None => entry,
        }
    });
    state.updated_at = now;
    ResultAcceptanceCore::Accepted
}

fn accept_result(
    state: &mut AgentTaskState,
    proposal: &AgentTaskResultProposal,
    digest: AgentContentDigest,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let accepted = AgentAcceptedResult {
        proposal_id: proposal.proposal_id.clone(),
        run: Some(proposal.run.clone()),
        principal: None,
        definition_id: proposal.definition_id.clone(),
        definition_version: proposal.definition_version,
        result_schema: proposal.result_schema.clone(),
        content: proposal.content.clone(),
        digest,
        evidence: proposal.evidence.clone(),
        accepted_at: now,
    };

    if state.task.is_none() {
        return refuse(
            state,
            "task-not-created",
            "the task does not exist".to_string(),
        );
    }

    match accept_result_core(state, &accepted, None, now) {
        ResultAcceptanceCore::Accepted => decision(AgentTaskDecision::Accepted {
            result: Box::new(accepted),
        }),
        ResultAcceptanceCore::Bounds(error) => refuse(state, error.code(), error.to_string()),
        ResultAcceptanceCore::AlreadyTerminal => refuse(
            state,
            "task-terminal",
            "the task is already terminal".to_string(),
        ),
    }
}

/// What the origin-neutral rejection core committed.
struct ResultRejectionCore {
    /// The durable rejection decision.
    rejection: AgentTaskRejection,
    /// How many further proposals the task will still consider.
    remaining: u32,
    /// The task's status after the decision: unchanged, or `Failed` when
    /// this rejection exhausted the budget.
    status_after: AgentTaskStatus,
}

/// Commits one validation rejection: the counter, the materialized
/// `last_rejection`, the exhaustion terminal when the budget is spent, and
/// the `ResultRejected` history row.
///
/// Origin-neutral exactly like [`accept_result_core`]. The caller ensures
/// the task exists.
fn reject_result_core(
    state: &mut AgentTaskState,
    proposal_id: &AgentOperationId,
    causation_id: &AgentCausationId,
    digest: AgentContentDigest,
    cause: &AgentTaskRejectionCause,
    principal: Option<&str>,
    now: AgentTimestampMillis,
) -> ResultRejectionCore {
    let task = state.task.as_mut().expect("the task exists on this path");

    task.rejection_count += 1;
    let rejection = AgentTaskRejection {
        proposal_id: proposal_id.clone(),
        digest: digest.clone(),
        cause: cause.clone(),
        rejection_count: task.rejection_count,
        causation_id: causation_id.clone(),
        rejected_at: now,
    };
    task.last_rejection = Some(Box::new(rejection.clone()));

    let exhausted = task.rejection_count >= task.definition.limits.max_result_rejections;
    let remaining = task
        .definition
        .limits
        .max_result_rejections
        .saturating_sub(task.rejection_count);
    let rejections = task.rejection_count;

    let status_after = if exhausted {
        // The rejection budget is spent. The task fails; it never silently
        // accepts the proposal it just refused
        // ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
        let _ = terminate(
            state,
            proposal_id,
            AgentTaskTerminalReason::ResultRejectionsExhausted { rejections },
            now,
        );
        AgentTaskStatus::Failed
    } else {
        state
            .task
            .as_ref()
            .map_or(AgentTaskStatus::Failed, |task| task.status)
    };

    let cause_reason = cause.reason.clone();
    let cause_detail = cause.detail.clone();
    let proposal_id = proposal_id.clone();
    let principal = principal.map(str::to_string);
    state.record_history(|sequence| {
        let entry = AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultRejected,
            proposal_id.clone(),
            status_after,
            now,
        )
        .with_digest(digest.clone())
        .with_detail(format!("{cause_reason}: {cause_detail}"));
        match &principal {
            Some(principal) => entry.with_principal(principal.clone()),
            None => entry,
        }
    });
    state.updated_at = now;

    ResultRejectionCore {
        rejection,
        remaining,
        status_after,
    }
}

fn reject_result(
    state: &mut AgentTaskState,
    proposal: &AgentTaskResultProposal,
    digest: AgentContentDigest,
    cause: AgentTaskRejectionCause,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    if state.task.is_none() {
        return refuse(
            state,
            "task-not-created",
            "the task does not exist".to_string(),
        );
    }

    let committed = reject_result_core(
        state,
        &proposal.proposal_id,
        &proposal.causation_id,
        digest,
        &cause,
        None,
        now,
    );

    let feedback = bounded_detail(format!("{}: {}", cause.reason, cause.detail));
    let code = cause.reason.clone();
    let payload = decision_payload(&AgentTaskDecision::Rejected {
        rejection: Box::new(committed.rejection),
        feedback,
        remaining_iterations: committed.remaining,
        status: committed.status_after,
    });
    // A rule rejection is a durable *decision*, not a failure: it travels home as
    // the exchange's result, is returned unchanged on replay, and the run settles
    // on it.
    AgentExchangeResult::rejected(code, "the proposed result was refused", payload)
}

/// The bounded fingerprint under which a rejected submission's operation id
/// enters the task's echo ring.
fn submission_fingerprint(operation_id: &AgentOperationId) -> String {
    AgentContentDigest::of_bytes(operation_id.as_str().as_bytes()).value
}

/// Applies one authenticated human-result submission
/// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
///
/// The ladder answers every durable echo *before* any guard, the terminal
/// one included — the accepted result and the latest rejection replay
/// idempotently past the operation log's bounded window — and every refusal
/// is non-committing: a rejected transition never reaches the store, so a
/// corrected retry under the same operation id is still accepted.
fn submit_human_result(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    submission: &AgentHumanResultSubmission,
    now: AgentTimestampMillis,
) -> AgentTaskResult<(AgentTaskOutcomeExtras, Vec<AgentExchangeEnvelope>)> {
    if submission.principal.is_empty() {
        return Err(AgentTaskError::SubmissionRefused {
            code: "submission-principal-missing",
            message: "a human-result submission requires an authenticated principal".to_string(),
        });
    }
    if submission.principal.len() > AGENT_IDENTITY_MAX_LENGTH {
        return Err(AgentTaskError::SubmissionRefused {
            code: "submission-principal-too-long",
            message: format!(
                "the submission's principal is {} bytes, and the task persists at most \
                 {AGENT_IDENTITY_MAX_LENGTH}",
                submission.principal.len()
            ),
        });
    }
    if submission.causation_id.as_str().len() > AGENT_IDENTITY_MAX_LENGTH {
        // The same rationale as the run proposal's causation guard: a
        // rejection persists the id, and the growth reserve is sized against
        // bounded fields only.
        return Err(AgentTaskError::SubmissionRefused {
            code: "submission-causation-too-long",
            message: format!(
                "the submission's causation id is {} bytes, and the task persists at most \
                 {AGENT_IDENTITY_MAX_LENGTH}",
                submission.causation_id.as_str().len()
            ),
        });
    }

    // The durable echoes, before every guard including the terminal one (the
    // handoff-provenance ordering): a past-window replay of a decided
    // submission converges on its recorded decision instead of refusing.
    // First-writer-wins — the replay's content is never re-read.
    if let Some(task) = state.task.as_ref() {
        let remaining = task
            .definition
            .limits
            .max_result_rejections
            .saturating_sub(task.rejection_count);
        if let Some(accepted) = task.accepted_result.as_deref() {
            if accepted.proposal_id == *operation_id {
                let decision = AgentTaskSubmissionDecision {
                    disposition: AgentTaskSubmissionDisposition::Accepted,
                    code: None,
                    feedback: String::new(),
                    remaining_attempts: remaining,
                    digest: accepted.digest.clone(),
                };
                return Ok((AgentTaskOutcomeExtras::submission(decision), Vec::new()));
            }
        }
        if let Some(last) = task.last_rejection.as_deref() {
            if last.proposal_id == *operation_id {
                let decision = AgentTaskSubmissionDecision {
                    disposition: AgentTaskSubmissionDisposition::Rejected,
                    code: Some(last.cause.reason.clone()),
                    feedback: bounded_detail(format!(
                        "{}: {}",
                        last.cause.reason, last.cause.detail
                    )),
                    remaining_attempts: remaining,
                    digest: last.digest.clone(),
                };
                return Ok((AgentTaskOutcomeExtras::submission(decision), Vec::new()));
            }
        }
        if task
            .rejected_submissions
            .contains(&submission_fingerprint(operation_id))
        {
            // An older rejection evicted from the materialized record: the
            // decision stands, the budget is not re-spent, and the caller
            // resubmits corrected content under a new discriminator.
            return Err(AgentTaskError::SubmissionRefused {
                code: "submission-already-rejected",
                message: "this submission was already rejected; a corrected resubmission \
                          carries a new deduplication key"
                    .to_string(),
            });
        }
    }

    let Some(task) = state.task.as_ref() else {
        return Err(AgentTaskError::NotCreated {
            scope: state.scope.clone(),
        });
    };
    if task.definition.is_agent_owned() {
        return Err(AgentTaskError::SubmissionRefused {
            code: "task-not-human-owned",
            message: "the task is agent-owned; its result arrives from its assigned run"
                .to_string(),
        });
    }
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    if !matches!(task.status, AgentTaskStatus::WaitingForInput) {
        // A human-owned task still blocked on dependencies cannot be
        // completed early: the dependency graph is deterministic
        // ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
        return Err(AgentTaskError::SubmissionRefused {
            code: "task-not-awaiting-input",
            message: format!("the task is {}, not waiting for input", task.status),
        });
    }
    if task.cancellation.is_some() {
        return Err(AgentTaskError::SubmissionRefused {
            code: AGENT_TASK_REFUSAL_CANCEL_REQUESTED,
            message: "the task's cancellation is propagating; no submission can be validated"
                .to_string(),
        });
    }

    let digest = submission.content.digest();
    let status_before = task.status;
    let principal = submission.principal.clone();
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultProposed,
            operation_id.clone(),
            status_before,
            now,
        )
        .with_digest(digest.clone())
        .with_principal(principal.clone())
    });

    let task = state.task.as_ref().expect("the task exists on this path");
    match task
        .definition
        .validate_result(&AgentResultClaim::from_submission(submission))
    {
        Ok(()) => {
            let accepted = AgentAcceptedResult {
                proposal_id: operation_id.clone(),
                run: None,
                principal: Some(submission.principal.clone()),
                definition_id: submission.definition_id.clone(),
                definition_version: submission.definition_version,
                result_schema: submission.result_schema.clone(),
                content: submission.content.clone(),
                digest: digest.clone(),
                evidence: submission.evidence.clone(),
                accepted_at: now,
            };
            match accept_result_core(state, &accepted, Some(&submission.principal), now) {
                ResultAcceptanceCore::Accepted => {
                    let remaining = state.task.as_ref().map_or(0, |task| {
                        task.definition
                            .limits
                            .max_result_rejections
                            .saturating_sub(task.rejection_count)
                    });
                    let decision = AgentTaskSubmissionDecision {
                        disposition: AgentTaskSubmissionDisposition::Accepted,
                        code: None,
                        feedback: String::new(),
                        remaining_attempts: remaining,
                        digest,
                    };
                    let owed = owed_child_reports(state, now, 0)?;
                    Ok((AgentTaskOutcomeExtras::submission(decision), owed))
                }
                ResultAcceptanceCore::Bounds(error) => Err(error),
                ResultAcceptanceCore::AlreadyTerminal => {
                    let status = state
                        .task
                        .as_ref()
                        .map_or(AgentTaskStatus::Completed, |task| task.status);
                    Err(AgentTaskError::Terminal { status })
                }
            }
        }
        Err(cause) => {
            let committed = reject_result_core(
                state,
                operation_id,
                &submission.causation_id,
                digest.clone(),
                &cause,
                Some(&submission.principal),
                now,
            );
            // The rejection's fingerprint enters the bounded echo ring, so a
            // replay that outlives both the operation log and the
            // materialized `last_rejection` still cannot re-spend the budget.
            let task = state.task.as_mut().expect("the task exists on this path");
            let fingerprint = submission_fingerprint(operation_id);
            if !task.rejected_submissions.contains(&fingerprint) {
                task.rejected_submissions.push(fingerprint);
                if task.rejected_submissions.len() > AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY {
                    task.rejected_submissions.remove(0);
                }
            }
            let decision = AgentTaskSubmissionDecision {
                disposition: AgentTaskSubmissionDisposition::Rejected,
                code: Some(committed.rejection.cause.reason.clone()),
                feedback: bounded_detail(format!("{}: {}", cause.reason, cause.detail)),
                remaining_attempts: committed.remaining,
                digest,
            };
            let owed = owed_child_reports(state, now, 0)?;
            Ok((AgentTaskOutcomeExtras::submission(decision), owed))
        }
    }
}

/// The task's half of the three ledger exchanges
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// Each is idempotent on the task's own escrow record rather than on the
/// journal's bounded ring, which is what makes a replay that has outlived the
/// window still safe: the ledger answers from what it already holds
/// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 61).
fn apply_ledger_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let kind = envelope.kind();
    let outcome = match kind {
        AgentExchangeKind::BudgetSettlement => {
            match envelope
                .payload()
                .decode::<AgentBudgetSettlement>(AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE)
            {
                Ok(settlement) => apply_settlement(state, &settlement),
                Err(error) => return refuse(state, error.code(), error.to_string()),
            }
        }
        AgentExchangeKind::BudgetReturn => {
            match envelope
                .payload()
                .decode::<AgentBudgetReturn>(AGENT_BUDGET_RETURN_PAYLOAD_TYPE)
            {
                Ok(release) => apply_return(state, &release),
                Err(error) => return refuse(state, error.code(), error.to_string()),
            }
        }
        AgentExchangeKind::BudgetAllocation => {
            match envelope
                .payload()
                .decode::<AgentBudgetTopUpRequest>(AGENT_BUDGET_TOP_UP_PAYLOAD_TYPE)
            {
                Ok(request) => apply_top_up(state, envelope.operation_id(), &request, now),
                Err(error) => return refuse(state, error.code(), error.to_string()),
            }
        }
        other => Err(AgentTaskError::InvalidDefinition {
            detail: format!("{other} is not a ledger exchange"),
        }),
    };

    match outcome {
        Ok(granted) => {
            state.updated_at = now;
            ledger_outcome(granted)
        }
        Err(error) => refuse(state, error.code(), error.to_string()),
    }
}

fn apply_settlement(
    state: &mut AgentTaskState,
    settlement: &AgentBudgetSettlement,
) -> AgentTaskResult<Option<AgentBudgetAllocation>> {
    let child = AgentEscrowChildId::for_run(settlement.run.run())?;
    let task = state.task_mut()?;
    task.escrow.settle_child(&child, &settlement.consumed)?;
    Ok(None)
}

fn apply_return(
    state: &mut AgentTaskState,
    release: &AgentBudgetReturn,
) -> AgentTaskResult<Option<AgentBudgetAllocation>> {
    let child = AgentEscrowChildId::for_run(release.run.run())?;
    let task = state.task_mut()?;
    // The ledger refuses a return the settlement has not preceded, so a lost or
    // reordered settlement can never release headroom that was already spent.
    task.escrow.return_child(&child)?;
    Ok(None)
}

fn apply_top_up(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    request: &AgentBudgetTopUpRequest,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentBudgetAllocation>> {
    let child = AgentEscrowChildId::for_run(request.run.run())?;
    let task = state.task_mut()?;
    // The grant is the parent-local decision of specification 9.7: what the run
    // asks for, narrowed to what this task can still afford under its own
    // ceilings. Nothing here reads another scope's ledger.
    let wanted = task.definition.run_allocation_request();
    let granted = task
        .escrow
        .top_up_child(&child, request.sequence, &wanted)?;
    // The run still receives the honest zero grant and stops with its original
    // exhaustion; what is new in slice 4.1 is that the *goal* now consults its
    // exhaustion policy instead of silently idling: the empty grant in the
    // dimension the run ran out of is the goal scope observing its own ceiling.
    let dimension = request.exhaustion.dimension;
    if dimension.is_conserved() && granted.get(dimension) == Some(0) {
        apply_goal_exhaustion(state, operation_id, dimension, now)?;
    }
    Ok(Some(granted))
}

fn ledger_outcome(granted: Option<AgentBudgetAllocation>) -> AgentExchangeResult {
    let outcome = AgentBudgetLedgerOutcome { granted };
    let payload = AgentExchangePayload::encode(AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE, &outcome)
        .unwrap_or_else(|_| AgentExchangePayload::empty(AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE));
    AgentExchangeResult::accepted(payload)
}

/// Applies a parent run's [`AgentExchangeKind::DelegationCancel`] request
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)): the
/// receiving half of the parent → child propagation leg, and the recursion
/// point — the accepted request owes the child's own run-cancel onward in
/// the same compare-and-set, so the machinery re-enters unchanged at every
/// depth.
///
/// The sender must be the very parent run this task's delegation provenance
/// records, naming the very delegation that created it; anything else is a
/// forgery refused definitively. A terminal task, or one whose marker is
/// already set, answers idempotently: its own durable record is the fence
/// past the journal's bounded deduplication window.
fn apply_delegation_cancel(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeTransition {
    let request: AgentDelegationCancelRequest = match envelope
        .payload()
        .decode(AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE)
    {
        Ok(request) => request,
        // Version skew, not the receiver answering: the parent's settle rule
        // leaves the exchange outstanding, and it converges after upgrade.
        Err(error) => {
            return AgentExchangeTransition::new(refuse(
                state,
                "delegation-cancel-undecodable",
                error.to_string(),
            ))
        }
    };
    let Some(task) = state.task.as_ref() else {
        return AgentExchangeTransition::new(refuse(
            state,
            "delegation-cancel-not-delegated",
            "the addressed task does not exist".to_string(),
        ));
    };
    let Some(provenance) = task.delegation.as_deref() else {
        return AgentExchangeTransition::new(refuse(
            state,
            "delegation-cancel-not-delegated",
            "the task carries no delegation provenance".to_string(),
        ));
    };
    if provenance.delegation != request.delegation {
        return AgentExchangeTransition::new(refuse(
            state,
            "delegation-cancel-forged",
            format!(
                "the task was created by delegation {}, not {}",
                provenance.delegation, request.delegation
            ),
        ));
    }
    let sender = match envelope.initiator() {
        AgentEntityAddress::Run(scope) => scope,
        other => {
            return AgentExchangeTransition::new(refuse(
                state,
                "delegation-cancel-forged",
                format!("a delegation-cancel cannot originate from {other}"),
            ))
        }
    };
    if *sender != provenance.parent_run {
        return AgentExchangeTransition::new(refuse(
            state,
            "delegation-cancel-forged",
            format!(
                "the task's delegating run is {}, not {}",
                provenance.parent_run.run(),
                sender.run()
            ),
        ));
    }
    let receipt = |state: &AgentTaskState| {
        let status = state.status().unwrap_or(AgentTaskStatus::Created);
        let receipt = AgentDelegationCancelReceipt {
            child_task: state.scope.task().clone(),
            status,
        };
        AgentExchangeResult::accepted(
            AgentExchangePayload::encode(AGENT_DELEGATION_CANCEL_RECEIPT_PAYLOAD_TYPE, &receipt)
                .unwrap_or_else(|_| {
                    AgentExchangePayload::empty(AGENT_DELEGATION_CANCEL_RECEIPT_PAYLOAD_TYPE)
                }),
        )
    };
    if task.status.is_terminal() || task.cancellation.is_some() {
        return AgentExchangeTransition::new(receipt(state));
    }
    let reason = AgentTaskTerminalReason::CancellationRequested {
        reason: bounded_detail(request.reason),
    };
    match request_task_cancellation(state, envelope.operation_id(), reason, now) {
        Ok(owed) => {
            let mut transition = AgentExchangeTransition::new(receipt(state));
            for envelope in owed {
                transition = transition.owing(envelope);
            }
            transition
        }
        // The guards above exclude every refusal `request_task_cancellation`
        // can make; a failure here is a construction bug answered as a
        // definitive refusal rather than a silent drop.
        Err(error) => AgentExchangeTransition::new(refuse(
            state,
            "delegation-cancel-forged",
            error.to_string(),
        )),
    }
}

/// Records one dependent's registration in the upstream task's bounded
/// registry ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The sender fence is the envelope's initiator matched against the claimed
/// dependent; the receiver's own registry entry is the durable echo past the
/// journal's bounded window. An already-terminal upstream accepts without
/// recording — the receipt carries the outcome directly, so a late
/// registration never waits for a notification the upstream will not owe.
fn apply_dependency_registration(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeTransition {
    let registration: AgentDependencyRegistration = match envelope
        .payload()
        .decode(AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE)
    {
        Ok(registration) => registration,
        Err(error) => {
            // Non-settling, so a newer binary's payload converges after a
            // rolling upgrade instead of being refused for good.
            return AgentExchangeTransition::new(refuse(
                state,
                "dependency-registration-undecodable",
                error.to_string(),
            ));
        }
    };
    let sender = match envelope.initiator() {
        AgentEntityAddress::Task(scope) => scope,
        other => {
            return AgentExchangeTransition::new(refuse(
                state,
                "dependency-registration-forged",
                format!("a dependency registration cannot originate from {other}"),
            ))
        }
    };
    if *sender != registration.dependent
        || sender.tenant() != state.scope.tenant()
        || sender.task() == state.scope.task()
    {
        return AgentExchangeTransition::new(refuse(
            state,
            "dependency-registration-forged",
            format!(
                "the registration claims dependent {}, sent by {}",
                registration.dependent.task(),
                sender.task()
            ),
        ));
    }
    if registration.upstream != *state.scope.task() {
        return AgentExchangeTransition::new(refuse(
            state,
            "dependency-registration-forged",
            format!(
                "the registration names upstream {}, but this task is {}",
                registration.upstream,
                state.scope.task()
            ),
        ));
    }
    let Some(task) = state.task.as_ref() else {
        // Retryable by classification, and unmemoized because of it: the host
        // does not record an unsettleable refusal in the applied log, so the
        // next re-drive re-runs this arm rather than replaying the answer, and
        // a racing create converges. A never-created upstream leaves the
        // dependent durably `Blocked` — the documented stuck-dependency
        // posture.
        return AgentExchangeTransition::new(refuse(
            state,
            "task-not-created",
            "the upstream task does not exist yet".to_string(),
        ));
    };
    let receipt = |state: &AgentTaskState| {
        let (outcome, terminal_reason, result_digest) =
            state.task().map_or((None, None, None), |task| {
                (
                    AgentTaskDependencyOutcome::from_terminal_status(task.status),
                    task.terminal_reason
                        .as_ref()
                        .map(|reason| reason.code().to_string()),
                    task.accepted_result
                        .as_ref()
                        .map(|accepted| accepted.digest.clone()),
                )
            });
        let receipt = AgentDependencyRegistrationReceipt {
            upstream: state.scope.clone(),
            outcome,
            terminal_reason,
            result_digest,
        };
        AgentExchangeResult::accepted(
            AgentExchangePayload::encode(
                AGENT_DEPENDENCY_REGISTRATION_RECEIPT_PAYLOAD_TYPE,
                &receipt,
            )
            .unwrap_or_else(|_| {
                AgentExchangePayload::empty(AGENT_DEPENDENCY_REGISTRATION_RECEIPT_PAYLOAD_TYPE)
            }),
        )
    };
    if task.dependents.contains_key(sender.task()) {
        // The durable echo past the journal window: a replay finds the entry
        // recorded and accepts idempotently — carrying the outcome when the
        // task has since terminalized, which the dependent applies exactly
        // as it would the notification.
        return AgentExchangeTransition::new(receipt(state));
    }
    if task.status.is_terminal() {
        // A moot registration grows nothing: the receipt carries the outcome
        // directly, and no registry entry — hence no owed notification —
        // exists for it.
        return AgentExchangeTransition::new(receipt(state));
    }
    if task.dependents.len() >= AGENT_TASK_MAX_DEPENDENTS {
        return AgentExchangeTransition::new(refuse(
            state,
            "task-dependents-exhausted",
            format!("the task already registers {AGENT_TASK_MAX_DEPENDENTS} dependents"),
        ));
    }
    let dependent = sender.task().clone();
    let record = AgentTaskDependentRecord {
        dependent: dependent.clone(),
        registered_by: envelope.operation_id().clone(),
        registered_at: now,
        outcome_settled: false,
    };
    let bounded = {
        let task = state.task.as_mut().expect("the task exists on this path");
        task.dependents.insert(dependent.clone(), record);
        // A registration grows the admitted record, so it keeps the same
        // growth headroom a late dependency declaration does.
        task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)
    };
    if let Err(error) = bounded {
        state
            .task
            .as_mut()
            .expect("the task exists on this path")
            .dependents
            .remove(&dependent);
        return AgentExchangeTransition::new(refuse(state, error.code(), error.to_string()));
    }
    let status = state
        .task
        .as_ref()
        .map_or(AgentTaskStatus::Created, |task| task.status);
    let operation_id = envelope.operation_id().clone();
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::DependentRegistered,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(dependent.to_string())
    });
    AgentExchangeTransition::new(receipt(state))
}

/// Applies an upstream's terminal outcome to this dependent's forward edge
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)), through
/// the same core the `RecordDependencyOutcome` relay command uses — which is
/// what guarantees a failing edge takes the cancellation *request* path over
/// a live run, never a direct terminalization.
fn apply_dependency_outcome(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeTransition {
    let notice: AgentDependencyOutcomeNotice = match envelope
        .payload()
        .decode(AGENT_DEPENDENCY_OUTCOME_PAYLOAD_TYPE)
    {
        Ok(notice) => notice,
        Err(error) => {
            // Non-settling: the rolling-upgrade posture.
            return AgentExchangeTransition::new(refuse(
                state,
                "dependency-outcome-undecodable",
                error.to_string(),
            ));
        }
    };
    let sender = match envelope.initiator() {
        AgentEntityAddress::Task(scope) => scope,
        other => {
            return AgentExchangeTransition::new(refuse(
                state,
                "dependency-outcome-forged",
                format!("a dependency outcome cannot originate from {other}"),
            ))
        }
    };
    if *sender != notice.upstream || sender.tenant() != state.scope.tenant() {
        return AgentExchangeTransition::new(refuse(
            state,
            "dependency-outcome-forged",
            format!(
                "the notice claims upstream {}, sent by {}",
                notice.upstream.task(),
                sender.task()
            ),
        ));
    }
    let receipt = |state: &AgentTaskState| {
        let status = state.status().unwrap_or(AgentTaskStatus::Created);
        let receipt = AgentDependencyOutcomeReceipt {
            dependent: state.scope.clone(),
            status,
        };
        AgentExchangeResult::accepted(
            AgentExchangePayload::encode(AGENT_DEPENDENCY_OUTCOME_RECEIPT_PAYLOAD_TYPE, &receipt)
                .unwrap_or_else(|_| {
                    AgentExchangePayload::empty(AGENT_DEPENDENCY_OUTCOME_RECEIPT_PAYLOAD_TYPE)
                }),
        )
    };
    let Some(task) = state.task.as_ref() else {
        // Definitive for this kind: a dependent that registered exists, so
        // absence is misroute-shaped and re-driving cannot repair it.
        return AgentExchangeTransition::new(refuse(
            state,
            "task-not-created",
            "the addressed dependent does not exist".to_string(),
        ));
    };
    // The forward edge is the durable record the sender is fenced against
    // *and* the echo past the journal window.
    let Some(edge) = task.dependencies.get(sender.task()) else {
        return AgentExchangeTransition::new(refuse(
            state,
            "task-unknown-dependency",
            format!("the task declares no dependency on {}", sender.task()),
        ));
    };
    match edge.outcome {
        Some(existing) if existing == notice.outcome => {
            return AgentExchangeTransition::new(receipt(state));
        }
        Some(_) => {
            return AgentExchangeTransition::new(refuse(
                state,
                "task-dependency-conflict",
                "the dependency already resolved with a different outcome; a conflict, not a \
                 correction"
                    .to_string(),
            ));
        }
        None => {}
    }
    if task.status.is_terminal() {
        // The dependent settled first — nothing left for the outcome to
        // move; the upstream just needs its settlement.
        return AgentExchangeTransition::new(receipt(state));
    }
    match record_dependency_outcome(
        state,
        envelope.operation_id(),
        sender.task(),
        notice.outcome,
        now,
    ) {
        Ok(mut owed) => {
            // The terminalization a cancelling edge may have finalized owes
            // its own child reports in this same compare-and-set — the
            // transitive-chain recursion point.
            match owed_child_reports(state, now, owed.len()) {
                Ok(reports) => owed.extend(reports),
                Err(error) => {
                    debug_assert!(false, "child-report construction failed: {error}");
                }
            }
            let mut transition = AgentExchangeTransition::new(receipt(state));
            for envelope in owed {
                transition = transition.owing(envelope);
            }
            transition
        }
        Err(error) => AgentExchangeTransition::new(refuse(state, error.code(), error.to_string())),
    }
}

/// Settles the upstream's answer to a dependency registration at the
/// dependent: the edge's marker flips, and the answer resolves the edge when
/// it decides it — a receipt carrying an already-terminal upstream's outcome,
/// or a definitive refusal, which means no notification can ever arrive. Both
/// resolve through the same core the notification would.
fn settle_dependency_registration_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    result: &AgentExchangeResult,
    now: AgentTimestampMillis,
) -> Vec<AgentExchangeEnvelope> {
    let upstream = match envelope.target() {
        AgentEntityAddress::Task(scope) => scope.task().clone(),
        _ => return Vec::new(),
    };
    let expected =
        dependency_registration_operation_id(state.scope.tenant(), &upstream, state.scope.task())
            .ok();
    if expected.as_ref() != Some(envelope.operation_id()) {
        return Vec::new();
    }
    {
        let Ok(task) = state.task_mut() else {
            return Vec::new();
        };
        let Some(edge) = task.dependencies.get_mut(&upstream) else {
            return Vec::new();
        };
        if edge.registration_settled {
            return Vec::new();
        }
        edge.registration_settled = true;
    }
    state.updated_at = now;
    if result.is_accepted() {
        let receipt: Result<AgentDependencyRegistrationReceipt, _> = result
            .payload()
            .decode(AGENT_DEPENDENCY_REGISTRATION_RECEIPT_PAYLOAD_TYPE);
        if let Ok(receipt) = receipt {
            if let Some(outcome) = receipt.outcome {
                return apply_registration_outcome(state, &upstream, outcome, now);
            }
        }
        return Vec::new();
    }
    // A refusal `check_settle` classified definitive: the upstream will never
    // hold a registry entry for this dependent, so the notification the
    // forward edge waits on can never be sent. The refusal is recorded, and
    // then the edge resolves *failed* — through the same core an upstream
    // failure takes, so the policy the edge declared decides what that means:
    // cancellation under the default, evidence under `ContinueWithEvidence`.
    // Leaving the edge unresolved instead would hold the dependent `Blocked`
    // forever with no terminal status, no policy applied, and no signal beyond
    // the history row below.
    let code = result
        .status()
        .rejection_code()
        .unwrap_or("dependency-registration-refused")
        .to_string();
    let status = state.status().unwrap_or(AgentTaskStatus::Created);
    let operation_id = envelope.operation_id().clone();
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::DependentRegistrationRefused,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(code.clone())
    });
    apply_registration_outcome(state, &upstream, AgentTaskDependencyOutcome::Failed, now)
}

/// Resolves one forward edge from what its registration exchange returned —
/// an already-terminal upstream's carried outcome, or the failure a
/// definitively refused registration implies — and returns whatever the
/// resolution now owes.
///
/// It runs the relay command's own core, so a registration-derived resolution
/// and an application-relayed one are the same transition: the same conflict
/// fence, the same cancellation *request* over a live run, the same history
/// row, and the same durable counters the outcome metric is measured from.
fn apply_registration_outcome(
    state: &mut AgentTaskState,
    upstream: &AgentTaskId,
    outcome: AgentTaskDependencyOutcome,
    now: AgentTimestampMillis,
) -> Vec<AgentExchangeEnvelope> {
    let Ok(operation_id) =
        dependency_outcome_operation_id(state.scope.tenant(), upstream, state.scope.task())
    else {
        return Vec::new();
    };
    match record_dependency_outcome(state, &operation_id, upstream, outcome, now) {
        Ok(mut owed) => {
            match owed_child_reports(state, now, owed.len()) {
                Ok(reports) => owed.extend(reports),
                Err(error) => {
                    debug_assert!(false, "child-report construction failed: {error}");
                }
            }
            owed
        }
        // The dependent turned terminal meanwhile, or the relay resolved the
        // edge first — either way nothing is owed here, and the record that
        // already stands is the one that counts.
        Err(_) => Vec::new(),
    }
}

/// Settles the dependent's answer to an outcome notification at the
/// upstream: the registry entry's marker flips — the durable once-guard that
/// quiesces the owed derivation past the journal's bounded window.
fn settle_dependency_outcome_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) {
    let dependent = match envelope.target() {
        AgentEntityAddress::Task(scope) => scope.task().clone(),
        _ => return,
    };
    let expected =
        dependency_outcome_operation_id(state.scope.tenant(), state.scope.task(), &dependent).ok();
    if expected.as_ref() != Some(envelope.operation_id()) {
        return;
    }
    let mut settled = false;
    if let Ok(task) = state.task_mut() {
        if let Some(record) = task.dependents.get_mut(&dependent) {
            if !record.outcome_settled {
                record.outcome_settled = true;
                settled = true;
            }
        }
    }
    if settled {
        state.updated_at = now;
    }
}

/// Refuses an exchange without making a validation decision.
fn refuse(state: &AgentTaskState, code: &str, message: String) -> AgentExchangeResult {
    let status = state.status().unwrap_or(AgentTaskStatus::Created);
    let payload = decision_payload(&AgentTaskDecision::Refused {
        code: code.to_string(),
        status,
    });
    AgentExchangeResult::rejected(code, message, payload)
}

fn decision(decision: AgentTaskDecision) -> AgentExchangeResult {
    AgentExchangeResult::accepted(decision_payload(&decision))
}

fn decision_payload(decision: &AgentTaskDecision) -> AgentExchangePayload {
    AgentExchangePayload::encode(AGENT_TASK_DECISION_PAYLOAD_TYPE, decision)
        .unwrap_or_else(|_| AgentExchangePayload::empty(AGENT_TASK_DECISION_PAYLOAD_TYPE))
}

/// The domain half of the typed-task entity.
///
/// It supplies bounded, pure transitions and nothing else; the choreography
/// substrate owns durability, deduplication, re-drive, and routing.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentTaskParticipant;

impl AgentExchangeParticipant for AgentTaskParticipant {
    type State = AgentTaskState;

    fn initialize(&self, address: &AgentEntityAddress, now: AgentTimestampMillis) -> Self::State {
        let scope = match address {
            AgentEntityAddress::Task(scope) => scope.clone(),
            // The host builds a participant for the address it serves, and the
            // entity refuses an id that does not parse into a task scope, so this
            // is unreachable in practice. Panicking would take down a shard owner
            // over a routing bug; an uncreated task under an address that can
            // never receive a creation is inert instead.
            other => AgentTaskScope::new(other.tenant().clone(), unroutable_task_id())
                .expect("the unroutable task scope is well formed"),
        };
        AgentTaskState::uncreated(scope, now)
    }

    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        let result = match envelope.kind() {
            AgentExchangeKind::Creation => {
                let result = apply_creation_exchange(state, envelope, now);
                // The created task's forward edges register with their
                // upstreams in this same compare-and-set, exactly as the
                // command-path creation owes them.
                let mut transition = AgentExchangeTransition::new(result);
                match owed_dependency_registrations(state, now) {
                    Ok(owed) => {
                        for envelope in owed {
                            transition = transition.owing(envelope);
                        }
                    }
                    Err(error) => {
                        debug_assert!(false, "registration construction failed: {error}");
                    }
                }
                return transition;
            }
            AgentExchangeKind::ResultProposal => {
                let result = apply_result_proposal(state, envelope, now);
                // The decision may have terminalized the task — an accepted
                // result, or an exhausting rejection — and the reports the
                // terminal owes commit in this same compare-and-set. The
                // dependents' outcome notifications wait for no escrow;
                // the epoch and delegation reports self-gate on ledger
                // closure, so a surviving rejection owes nothing.
                let mut transition = AgentExchangeTransition::new(result);
                match owed_child_reports(state, now, 0) {
                    Ok(owed) => {
                        for envelope in owed {
                            transition = transition.owing(envelope);
                        }
                    }
                    Err(error) => {
                        debug_assert!(false, "child-report construction failed: {error}");
                    }
                }
                return transition;
            }
            AgentExchangeKind::BudgetAllocation
            | AgentExchangeKind::BudgetSettlement
            | AgentExchangeKind::BudgetReturn => {
                let result = apply_ledger_exchange(state, envelope, now);
                // A ledger exchange may have closed a terminal child's own
                // ledger: the run's settlement and return both applied, so
                // the reports this child owes upward — the epoch result to
                // its controller, the delegation result to its parent run —
                // are now accurate, and owed in this same compare-and-set.
                // The same closure is the gate a requested cancellation
                // finalizes behind ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)),
                // so the finalization commits here too; its own child
                // reports deduplicate against the consult below.
                let mut transition = AgentExchangeTransition::new(result);
                let mut reserved = 0;
                match finalize_task_cancellation(state, envelope.operation_id(), now) {
                    Ok(owed) => {
                        reserved = owed.len();
                        for envelope in owed {
                            transition = transition.owing(envelope);
                        }
                    }
                    Err(error) => {
                        debug_assert!(false, "cancellation finalization failed: {error}");
                    }
                }
                match owed_child_reports(state, now, reserved) {
                    Ok(owed) => {
                        for envelope in owed {
                            transition = transition.owing(envelope);
                        }
                    }
                    Err(error) => {
                        // Owing cannot fail for records this binary wrote; a
                        // failure is a bug. The settlement itself is applied
                        // and must commit — refusing it would record a
                        // durable refusal the run could never settle — so
                        // the owe is skipped loudly instead. The strict
                        // propagation on the cancellation paths catches
                        // systematic construction bugs in tests.
                        debug_assert!(false, "child-report construction failed: {error}");
                    }
                }
                return transition;
            }
            AgentExchangeKind::EpochResult => return apply_epoch_result(state, envelope, now),
            AgentExchangeKind::GoalEvaluation => {
                return apply_goal_evaluation(state, envelope, now)
            }
            AgentExchangeKind::DelegationCancel => {
                return apply_delegation_cancel(state, envelope, now)
            }
            AgentExchangeKind::DependencyRegistration => {
                return apply_dependency_registration(state, envelope, now)
            }
            AgentExchangeKind::DependencyOutcome => {
                return apply_dependency_outcome(state, envelope, now)
            }
            AgentExchangeKind::TeamClaim => apply_team_claim(state, envelope, now),
            kind => refuse(
                state,
                "unsupported-exchange",
                format!("a task entity does not receive a {kind} exchange"),
            ),
        };
        AgentExchangeTransition::new(result)
    }

    fn check_settle(
        &self,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
    ) -> Result<(), crate::choreography::AgentChoreographyError> {
        match envelope.kind() {
            AgentExchangeKind::DelegationResult if !result.is_accepted() => {
                // A refused delegation result settles only under the parent's
                // definitive answers: the delegation is unknown, forged, not
                // owned, or the run never existed — undeliverable however
                // often it is re-driven. Every other refusal is the
                // receiver's inability — a `delegation-result-early` receipt
                // race, an `unsupported-exchange` from an owner that predates
                // the kind, a payload it could not decode — and the exchange
                // stays outstanding for re-drive until an owner that can
                // answer it does.
                match result.status().rejection_code() {
                    Some(
                        "delegation-result-unknown-run"
                        | "delegation-result-unknown-delegation"
                        | "delegation-result-forged"
                        | "delegation-result-not-owned",
                    ) => Ok(()),
                    code => Err(
                        crate::choreography::AgentChoreographyError::UnsettleableRefusal {
                            kind: AgentExchangeKind::DelegationResult,
                            code: code.unwrap_or_default().to_string(),
                        },
                    ),
                }
            }
            AgentExchangeKind::RunCancel if !result.is_accepted() => {
                // A refused run-cancel settles only under the run's definitive
                // answers: the sender was not its task, the run never received
                // the generation, or the generation is not the one it serves.
                // Every other refusal — an `unsupported-exchange` from an
                // owner that predates the kind, a payload it could not decode
                // — leaves the exchange outstanding for re-drive until an
                // owner that can answer it does (the rolling-upgrade rule).
                match result.status().rejection_code() {
                    Some(
                        "run-cancel-forged"
                        | "run-cancel-unassigned"
                        | "run-cancel-stale-generation",
                    ) => Ok(()),
                    code => Err(
                        crate::choreography::AgentChoreographyError::UnsettleableRefusal {
                            kind: AgentExchangeKind::RunCancel,
                            code: code.unwrap_or_default().to_string(),
                        },
                    ),
                }
            }
            AgentExchangeKind::HandoffResult if !result.is_accepted() => {
                // A refused handoff result settles only under the source
                // run's definitive answers: it holds no such handoff, or the
                // notice was forged — undeliverable however often it is
                // re-driven. Every other refusal — an `unsupported-exchange`
                // from an owner that predates the kind, a payload it could
                // not decode — leaves the exchange outstanding for re-drive
                // until an owner that can answer it does (the
                // rolling-upgrade rule).
                match result.status().rejection_code() {
                    Some("handoff-forged" | "handoff-not-held") => Ok(()),
                    code => Err(
                        crate::choreography::AgentChoreographyError::UnsettleableRefusal {
                            kind: AgentExchangeKind::HandoffResult,
                            code: code.unwrap_or_default().to_string(),
                        },
                    ),
                }
            }
            AgentExchangeKind::TeamClaimResult if !result.is_accepted() => {
                // A refused claim result settles only under the team's
                // definitive answers: no team exists, the board holds no
                // such entry, or the notice was forged — undeliverable
                // however often it is re-driven. Every other refusal leaves
                // the exchange outstanding for re-drive (the rolling-upgrade
                // rule).
                match result.status().rejection_code() {
                    Some("team-not-found" | "team-claim-unknown" | "team-claim-forged") => Ok(()),
                    code => Err(
                        crate::choreography::AgentChoreographyError::UnsettleableRefusal {
                            kind: AgentExchangeKind::TeamClaimResult,
                            code: code.unwrap_or_default().to_string(),
                        },
                    ),
                }
            }
            AgentExchangeKind::DependencyRegistration if !result.is_accepted() => {
                // A refused registration settles only under the upstream's
                // definitive answers: forged, ceiling reached, or unable to
                // bound the record. Each means the upstream holds no registry
                // entry and never will, so the dependent resolves the edge
                // itself on settlement. `task-not-created` deliberately stays
                // outstanding instead — the upstream may yet be created, and
                // the receiver does not memoize this class of refusal, so a
                // racing create converges on the next re-drive. A
                // never-created upstream leaves the dependent durably
                // `Blocked`, the documented stuck-dependency struggle signal.
                match result.status().rejection_code() {
                    Some(
                        "dependency-registration-forged"
                        | "task-dependents-exhausted"
                        | "task-state-too-large",
                    ) => Ok(()),
                    code => Err(
                        crate::choreography::AgentChoreographyError::UnsettleableRefusal {
                            kind: AgentExchangeKind::DependencyRegistration,
                            code: code.unwrap_or_default().to_string(),
                        },
                    ),
                }
            }
            AgentExchangeKind::DependencyOutcome if !result.is_accepted() => {
                // A refused outcome settles only under the dependent's
                // definitive answers: forged, no such task, no such edge, or
                // a conflicting resolution — undeliverable however often it
                // is re-driven. An undecodable payload or a pre-slice
                // receiver stays outstanding (the rolling-upgrade rule).
                match result.status().rejection_code() {
                    Some(
                        "dependency-outcome-forged"
                        | "task-not-created"
                        | "task-unknown-dependency"
                        | "task-dependency-conflict",
                    ) => Ok(()),
                    code => Err(
                        crate::choreography::AgentChoreographyError::UnsettleableRefusal {
                            kind: AgentExchangeKind::DependencyOutcome,
                            code: code.unwrap_or_default().to_string(),
                        },
                    ),
                }
            }
            _ => Ok(()),
        }
    }

    fn settle(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
        now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope> {
        if envelope.kind() == AgentExchangeKind::Assignment {
            settle_assignment(state, envelope, result, now);
        }
        if envelope.kind() == AgentExchangeKind::HandoffResult {
            // The source run answered — accepted, or refused under a code
            // `check_settle` classified definitive. Either way the marker
            // settles on the provenance: the durable once-guard that
            // quiesces the owed derivation past the journal's bounded window.
            settle_handoff_result_exchange(state, envelope, now);
        }
        if envelope.kind() == AgentExchangeKind::TeamClaimResult {
            // The team answered — the board settlement applied, or refused
            // under a definitive code. The marker settles on the claim
            // provenance exactly as the handoff marker does.
            settle_team_claim_result_exchange(state, envelope, now);
        }
        if envelope.kind() == AgentExchangeKind::DependencyRegistration {
            // The upstream answered — recorded, answered with its terminal
            // outcome, or refused under a definitive code. The edge's marker
            // settles either way, and an outcome carried on the receipt
            // applies here, owing whatever the application owes.
            return settle_dependency_registration_exchange(state, envelope, result, now);
        }
        if envelope.kind() == AgentExchangeKind::DependencyOutcome {
            // The dependent answered — applied, echoed, or refused under a
            // definitive code. The registry entry's marker settles, so the
            // derivation quiesces past the journal's bounded window.
            settle_dependency_outcome_exchange(state, envelope, now);
            return Vec::new();
        }
        if !matches!(
            envelope.kind(),
            AgentExchangeKind::Assignment
                | AgentExchangeKind::RunCancel
                | AgentExchangeKind::HandoffResult
                | AgentExchangeKind::TeamClaimResult
        ) {
            return Vec::new();
        }
        // A settled assignment or run-cancel may have moved a requested
        // cancellation forward: an acceptance owes the run-cancel the request
        // deferred, a refusal released the generation's escrow, and a receipt
        // may have found the run already settled — so the propagation and the
        // finalization it permits are owed from this same compare-and-set.
        // A settled assignment may equally have resolved a pending handoff,
        // owing the source its handoff result. Settling may not fail; a
        // construction failure is a bug surfaced loudly in tests and skipped
        // here.
        let mut owed = Vec::new();
        match owed_run_cancel(state, now) {
            Ok(envelopes) => owed.extend(envelopes),
            Err(error) => {
                debug_assert!(false, "run-cancel construction failed: {error}");
            }
        }
        match owed_handoff_result(state, now) {
            Ok(envelopes) => owed.extend(envelopes),
            Err(error) => {
                debug_assert!(false, "handoff-result construction failed: {error}");
            }
        }
        match owed_team_claim_result(state, now) {
            Ok(envelopes) => owed.extend(envelopes),
            Err(error) => {
                debug_assert!(false, "team-claim-result construction failed: {error}");
            }
        }
        match finalize_task_cancellation(state, envelope.operation_id(), now) {
            Ok(envelopes) => owed.extend(envelopes),
            Err(error) => {
                debug_assert!(false, "cancellation finalization failed: {error}");
            }
        }
        owed
    }
}

fn unroutable_task_id() -> AgentTaskId {
    AgentTaskId::new("unroutable").expect("the literal is a valid task id")
}

/// Creates a task from a delegating run's [`AgentExchangeKind::Creation`]
/// exchange.
fn apply_creation_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let creation: AgentTaskCreation =
        match envelope.payload().decode(AGENT_TASK_CREATION_PAYLOAD_TYPE) {
            Ok(creation) => creation,
            Err(error) => return refuse(state, error.code(), error.to_string()),
        };

    match create_task(state, envelope.operation_id(), creation, now) {
        Ok(outcome) => AgentExchangeResult::accepted(
            AgentExchangePayload::encode(AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE, &outcome)
                .unwrap_or_else(|_| {
                    AgentExchangePayload::empty(AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE)
                }),
        ),
        Err(error) => refuse(state, error.code(), error.to_string()),
    }
}

/// Settles the run's answer to an assignment.
///
/// An acceptance moves the task to `InProgress`. A refusal retires the
/// generation, releases its escrow — the run never accepted, so nothing was
/// consumed — and leaves the task assignable, so the next decision creates a
/// new run rather than reusing a run that refused.
fn settle_assignment(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    result: &AgentExchangeResult,
    now: AgentTimestampMillis,
) {
    let operation_id = envelope.operation_id().clone();
    let Some(task) = state.task.as_mut() else {
        return;
    };
    let Some(assignment) = task.assignment.as_mut() else {
        return;
    };
    if assignment.operation_id != operation_id {
        // The reply belongs to a generation this task has already moved past.
        return;
    }

    if result.is_accepted() {
        assignment.status = AgentAssignmentStatus::Accepted;
        let assignment = assignment.clone();
        task.status = AgentTaskStatus::InProgress;
        // The handoff acceptance flip ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)):
        // when the accepted generation is the one a pending handoff minted,
        // responsibility has durably transferred — the provenance settles
        // `Accepted` in this same compare-and-set, and the settle pass owes
        // the source its handoff result.
        let mut handoff_accepted = None;
        if let Some(handoff) = task.handoff.as_deref_mut() {
            if !handoff.is_settled() && handoff.target_generation == Some(assignment.generation) {
                handoff.status = AgentTaskHandoffStatus::Accepted;
                handoff.settled_at = Some(now);
                handoff_accepted = Some(handoff.handoff.clone());
            }
        }
        // The claim acceptance flip ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)):
        // when the accepted generation is the one a pending board claim
        // minted, the claimant owns the task under the assignment fence —
        // the claim settles `Accepted` in this same compare-and-set, and the
        // settle pass owes the board its claim result.
        let mut claim_accepted = None;
        if let Some(claim) = task.team_claim.as_deref_mut() {
            if !claim.is_settled() && claim.target_generation == Some(assignment.generation) {
                claim.status = AgentTaskTeamClaimStatus::Accepted;
                claim.settled_at = Some(now);
                claim_accepted = Some(claim.claim.clone());
            }
        }
        state.updated_at = now;
        state.record_history(|sequence| {
            AgentTaskHistoryEntry::new(
                sequence,
                AgentTaskHistoryKind::AssignmentAccepted,
                operation_id.clone(),
                AgentTaskStatus::InProgress,
                now,
            )
            .with_assignment(&assignment)
        });
        if let Some(handoff_id) = handoff_accepted {
            state.record_history(|sequence| {
                AgentTaskHistoryEntry::new(
                    sequence,
                    AgentTaskHistoryKind::HandoffAccepted,
                    operation_id.clone(),
                    AgentTaskStatus::InProgress,
                    now,
                )
                .with_assignment(&assignment)
                .with_detail(handoff_id.to_string())
            });
        }
        if let Some(claim_id) = claim_accepted {
            state.record_history(|sequence| {
                AgentTaskHistoryEntry::new(
                    sequence,
                    AgentTaskHistoryKind::TeamClaimAccepted,
                    operation_id.clone(),
                    AgentTaskStatus::InProgress,
                    now,
                )
                .with_assignment(&assignment)
                .with_detail(claim_id.to_string())
            });
        }
        return;
    }

    let code = result
        .status()
        .rejection_code()
        .unwrap_or("run-refused-assignment")
        .to_string();
    let assignment = assignment.clone();
    task.assignment = None;
    // A dependency may have been declared while the assignment was
    // outstanding, so the retired task is only assignable again if the
    // dependency summary still permits it.
    task.status = if task.dependencies_satisfied() {
        AgentTaskStatus::Created
    } else {
        AgentTaskStatus::Blocked
    };
    let status = task.status;
    task.last_refusal = Some(AgentAssignmentRefusal {
        agent: assignment.agent.clone(),
        reason: AgentAssignmentRefusalReason::RunRefusedAssignment,
        detail: bounded_detail(code.clone()),
        refused_at: now,
    });
    // The refused generation's escrow debit is released in this same settle:
    // the run never accepted, so it consumed nothing, and a child left
    // outstanding would both shrink the headroom every later generation is
    // decided against and hold the ledger gate closed over the terminal
    // reports an exhausted child owes upward. Idempotent: a re-driven settle
    // finds the assignment already retired and never reaches here.
    if let Ok(child) = AgentEscrowChildId::for_run(&assignment.run) {
        if task
            .escrow
            .settle_child(&child, &AgentBudgetConsumption::default())
            .is_ok()
        {
            // Returning a child this transition just settled cannot fail for
            // records this binary wrote; a failure is a construction bug.
            let returned = task.escrow.return_child(&child);
            debug_assert!(
                returned.is_ok(),
                "refused-generation escrow return failed: {returned:?}"
            );
        }
    }
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::AssignmentReleased,
            operation_id.clone(),
            status,
            now,
        )
        .with_assignment(&assignment)
        .with_detail(code.clone())
    });
    // The handoff single-attempt rule: a refused handoff generation resolves
    // the transfer — restoring the stashed source — rather than re-offering
    // toward a target that just refused. The refused generation's escrow was
    // released above; the restored source's child was never touched. The
    // owed handoff-result exchange is collected by the participant's settle
    // pass, which runs right after this.
    let refused_handoff_generation = state.task.as_ref().is_some_and(|task| {
        task.handoff.as_deref().is_some_and(|handoff| {
            !handoff.is_settled() && handoff.target_generation == Some(assignment.generation)
        })
    });
    if refused_handoff_generation {
        let resolved = resolve_handoff_refusal(state, &code, now);
        debug_assert!(
            resolved.is_ok(),
            "handoff refusal resolution failed: {resolved:?}"
        );
    }
    // The claim single-attempt rule, the handoff precedent: a refused claim
    // generation resolves the claim — clearing the assignee back to the
    // board-pending posture — rather than re-offering toward a member whose
    // run just refused. The owed claim-result exchange is collected by the
    // participant's settle pass, which runs right after this.
    let refused_claim_generation = state.task.as_ref().is_some_and(|task| {
        task.team_claim.as_deref().is_some_and(|claim| {
            !claim.is_settled() && claim.target_generation == Some(assignment.generation)
        })
    });
    if refused_claim_generation {
        let resolved = resolve_team_claim_refusal(state, &code, now);
        debug_assert!(
            resolved.is_ok(),
            "team-claim refusal resolution failed: {resolved:?}"
        );
    }
}

/// The durable facade over one typed-task entity.
///
/// It owns the three things a bounded transition cannot do for itself: the
/// durable read of the agent's admission state that an assignment decision needs,
/// the flush of the history the task owes its sink, and the courier that drives
/// the exchanges it owes. Every decision is still a pure transition, and the
/// actor is a thin shell over this type.
pub struct AgentTaskEntityStore<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    scope: AgentTaskScope,
    host: AgentExchangeHost<AgentTaskParticipant, Store>,
    agents: Agents,
    history: History,
    policy: AgentSchemaPolicy,
    rewake_parker: Option<Arc<dyn AgentWakeRewakeParker>>,
    metrics: Arc<dyn MetricsRecorder>,
    recovered: bool,
}

impl<Store, Agents, History> Debug for AgentTaskEntityStore<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTaskEntityStore")
            .field("scope", &self.scope)
            .field("history", &self.history.backend_name())
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}

impl<Store, Agents, History> AgentTaskEntityStore<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    /// Creates a durable facade for one task scope.
    #[must_use]
    pub fn new(scope: AgentTaskScope, store: Store, agents: Agents, history: History) -> Self {
        let host = AgentExchangeHost::new(
            AgentEntityAddress::Task(scope.clone()),
            AgentTaskParticipant,
            store,
        );
        Self {
            scope,
            host,
            agents,
            history,
            policy: AgentSchemaPolicy::default(),
            rewake_parker: None,
            metrics: Arc::new(NoopMetricsRecorder),
            recovered: false,
        }
    }

    /// Wires a metrics recorder for the bounded wake, epoch, and lifecycle
    /// counters this entity emits after its durable transitions commit.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Uses an explicit schema-compatibility policy.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self.host = self.host.with_schema_policy(policy);
        self
    }

    /// Wires the durable wake-timer parker the settle pass parks
    /// controller-originated re-wakes through.
    ///
    /// Without one, owed re-wakes stay durably owed on the controller state —
    /// visible through the operational query — and are parked by whichever
    /// wiring later supplies the parker: an honest degraded mode, never a
    /// silent loss.
    #[must_use]
    pub fn with_wake_timers(mut self, parker: Arc<dyn AgentWakeRewakeParker>) -> Self {
        self.rewake_parker = Some(parker);
        self
    }

    /// The scope this facade addresses.
    #[must_use]
    pub const fn scope(&self) -> &AgentTaskScope {
        &self.scope
    }

    /// The durable persistence id of this task's state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        self.scope.persistence_id()
    }

    /// Loads the task's durable state, failing closed on an unsupported schema
    /// version.
    pub async fn recover(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<&AgentTaskState> {
        let state = self.host.recover(now).await?;
        self.recovered = true;
        Ok(state)
    }

    /// The currently recovered state.
    pub fn state(&self) -> AgentTaskResult<&AgentTaskState> {
        Ok(self.host.state()?)
    }

    /// The bounded projection of the task, once it has been created.
    pub fn snapshot(&self) -> AgentTaskResult<Option<AgentTaskSnapshot>> {
        Ok(self.state()?.snapshot())
    }

    /// Applies one command, then settles whatever it made possible: the
    /// assignment decision, the history the transition owes, and the exchanges it
    /// owes.
    ///
    /// # Errors
    ///
    /// An error does not prove the command was not applied. The transition
    /// commits before the settle pass, so a settlement failure — a history-sink
    /// outage, a delivery fault — surfaces here after the command has durably
    /// applied. Retrying with the same operation id is always safe: a command
    /// that committed answers [`AgentTaskEntityReply::Duplicate`] with its
    /// original outcome rather than transitioning twice.
    pub async fn apply(
        &mut self,
        command: AgentTaskEntityCommand,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentTaskEntityReply> {
        self.ensure_recovered(now).await?;

        if let Some(operation_id) = command.operation_id() {
            if let Some(outcome) = self
                .state()?
                .applied_operations
                .outcome(operation_id)
                .cloned()
            {
                // The command already ran. Its original outcome is returned, and
                // no second transition happens — which is what makes a replayed
                // creation, dependency, or cancellation converge on one task, one
                // edge, and one decision.
                return Ok(AgentTaskEntityReply::Duplicate { outcome });
            }
        }

        if matches!(command, AgentTaskEntityCommand::Describe) {
            return Ok(AgentTaskEntityReply::Snapshot(
                self.snapshot()?.map(Box::new),
            ));
        }
        self.require_history_headroom(now).await?;

        // The agent's durable admission state is read *before* the transition, so
        // a command that makes the task eligible can create it, decide its
        // assignment, and record the run-creation command it owes in one
        // compare-and-set. Reading it is I/O, and a transition may not do I/O
        // ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)); deciding
        // on what was read is pure.
        let readiness = self.resolve_readiness(&command, now).await?;

        // Bounded metric labels captured before the command moves. Renewal is
        // the one lifecycle transition the status difference across the
        // committed transition cannot see — it extends the goal without
        // changing the status — so it alone is counted from its command;
        // every status-changing transition, commanded or observed, is counted
        // by the difference. The disposition does not carry the trigger that
        // delivered it, so that too is captured here.
        let lifecycle_transition = match &command {
            AgentTaskEntityCommand::RenewContinuousGoal { .. } => Some("renewed"),
            _ => None,
        };
        let wake_trigger = match &command {
            AgentTaskEntityCommand::AdmitWake { binding, .. } => Some(binding.trigger().as_label()),
            _ => None,
        };

        let reply = match command {
            AgentTaskEntityCommand::Describe => unreachable!("handled above"),
            AgentTaskEntityCommand::Create {
                operation_id,
                creation,
            } => {
                self.transition(now, readiness, move |state| {
                    create_task(state, &operation_id, *creation, now)?;
                    // The declared forward edges register with their
                    // upstreams in this same compare-and-set.
                    let owed = owed_dependency_registrations(state, now)?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, owed))
                })
                .await?
            }
            AgentTaskEntityCommand::DeclareDependency {
                operation_id,
                declaration,
            } => {
                self.transition(now, readiness, move |state| {
                    declare_dependency(state, &operation_id, &declaration, now)?;
                    // The late edge registers with its upstream in this same
                    // compare-and-set, exactly as a creation-time edge does.
                    let owed = owed_dependency_registrations(state, now)?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, owed))
                })
                .await?
            }
            AgentTaskEntityCommand::RecordDependencyOutcome {
                operation_id,
                dependency,
                outcome,
            } => {
                let resolved_before = self.dependency_outcome_totals();
                let reply = self
                    .transition(now, readiness, move |state| {
                        // A failed dependency requests the dependent's
                        // cancellation rather than terminalizing it: with no live
                        // generation that finalizes here, owing the terminal
                        // reports upward in this same transition; with an
                        // accepted run it owes the run-cancel exchange and stays
                        // nonterminal until its ledger closes.
                        let mut owed = record_dependency_outcome(
                            state,
                            &operation_id,
                            &dependency,
                            outcome,
                            now,
                        )?;
                        owed.extend(owed_child_reports(state, now, owed.len())?);
                        Ok((operation_id, AgentTaskOutcomeExtras::NONE, owed))
                    })
                    .await?;
                self.record_dependency_outcomes(resolved_before);
                reply
            }
            AgentTaskEntityCommand::Cancel {
                operation_id,
                reason,
            } => {
                self.transition(now, None, move |state| {
                    // The request is absorbing and nonterminal: a task with no
                    // live generation finalizes in this same transition —
                    // owing its terminal reports upward — while a task whose
                    // run has durably accepted owes the run-cancel exchange
                    // instead and stays nonterminal until its ledger closes
                    // ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
                    let owed = request_task_cancellation(
                        state,
                        &operation_id,
                        AgentTaskTerminalReason::CancellationRequested {
                            reason: bounded_detail(reason),
                        },
                        now,
                    )?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, owed))
                })
                .await?
            }
            AgentTaskEntityCommand::RecordHandoff {
                operation_id,
                request,
            } => {
                self.transition(now, readiness, move |state| {
                    // The transfer records, and — with the target's readiness
                    // read before the transition — the wrapper's inline
                    // assignment decision mints the target's generation in
                    // this same compare-and-set. A refused readiness resolves
                    // the handoff refused through the same inline decision
                    // ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
                    record_handoff(state, &operation_id, &request, now)?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, Vec::new()))
                })
                .await?
            }
            AgentTaskEntityCommand::SubmitHumanResult {
                operation_id,
                submission,
            } => {
                let decided_before = self.submission_decision_snapshot();
                let reply = self
                    .transition(now, None, move |state| {
                        // A terminal decision — acceptance, or an exhausting
                        // rejection — owes its child reports in this same
                        // compare-and-set; the derivations self-gate, so a
                        // surviving rejection owes nothing.
                        let (extras, owed) =
                            submit_human_result(state, &operation_id, &submission, now)?;
                        Ok((operation_id, extras, owed))
                    })
                    .await?;
                self.record_submission_decision(decided_before);
                reply
            }
            AgentTaskEntityCommand::AdmitWake {
                operation_id,
                binding,
            } => {
                self.transition(now, None, move |state| {
                    let (wake, owed) = admit_wake(state, &operation_id, *binding, now)?;
                    Ok((operation_id, AgentTaskOutcomeExtras::wake(wake), owed))
                })
                .await?
            }
            AgentTaskEntityCommand::CompleteWakeOccurrence { operation_id, wake } => {
                self.transition(now, None, move |state| {
                    let (outcome, owed) =
                        complete_wake_occurrence(state, &operation_id, &wake, now)?;
                    Ok((operation_id, AgentTaskOutcomeExtras::wake(outcome), owed))
                })
                .await?
            }
            AgentTaskEntityCommand::UpdateContinuousSchedule {
                operation_id,
                schedule_revision,
                wake_policy,
            } => {
                self.transition(now, None, move |state| {
                    let outcome = update_continuous_schedule(
                        state,
                        &operation_id,
                        schedule_revision,
                        wake_policy.map(|policy| *policy),
                        now,
                    )?;
                    Ok((
                        operation_id,
                        AgentTaskOutcomeExtras::wake(outcome),
                        Vec::new(),
                    ))
                })
                .await?
            }
            AgentTaskEntityCommand::SuspendContinuousGoal {
                operation_id,
                expected_lifecycle_revision,
                reason,
                provenance,
            } => {
                self.transition(now, None, move |state| {
                    let outcome = suspend_continuous_goal(
                        state,
                        &operation_id,
                        expected_lifecycle_revision,
                        reason,
                        *provenance,
                        now,
                    )?;
                    Ok((
                        operation_id,
                        AgentTaskOutcomeExtras::wake(outcome),
                        Vec::new(),
                    ))
                })
                .await?
            }
            AgentTaskEntityCommand::ResumeContinuousGoal {
                operation_id,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition(now, None, move |state| {
                    let (outcome, owed) = resume_continuous_goal(
                        state,
                        &operation_id,
                        expected_lifecycle_revision,
                        *provenance,
                        now,
                    )?;
                    Ok((operation_id, AgentTaskOutcomeExtras::wake(outcome), owed))
                })
                .await?
            }
            AgentTaskEntityCommand::RenewContinuousGoal {
                operation_id,
                expected_lifecycle_revision,
                new_expires_at,
                provenance,
            } => {
                self.transition(now, None, move |state| {
                    let outcome = renew_continuous_goal(
                        state,
                        &operation_id,
                        expected_lifecycle_revision,
                        new_expires_at,
                        *provenance,
                        now,
                    )?;
                    Ok((
                        operation_id,
                        AgentTaskOutcomeExtras::wake(outcome),
                        Vec::new(),
                    ))
                })
                .await?
            }
            AgentTaskEntityCommand::RetireContinuousGoal {
                operation_id,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition(now, None, move |state| {
                    let outcome = retire_continuous_goal(
                        state,
                        &operation_id,
                        expected_lifecycle_revision,
                        *provenance,
                        now,
                    )?;
                    Ok((
                        operation_id,
                        AgentTaskOutcomeExtras::wake(outcome),
                        Vec::new(),
                    ))
                })
                .await?
            }
            AgentTaskEntityCommand::ActivateGoal {
                operation_id,
                expected_status_revision,
                provenance,
            } => {
                // Activation may make the task assignable, so the readiness
                // read rides along and the assignment commits with it.
                self.transition(now, readiness, move |state| {
                    let owed = activate_goal(
                        state,
                        &operation_id,
                        expected_status_revision,
                        *provenance,
                        now,
                    )?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, owed))
                })
                .await?
            }
            AgentTaskEntityCommand::RecordGoalDecision {
                operation_id,
                decision,
            } => {
                self.transition(now, None, move |state| {
                    record_goal_decision(state, &operation_id, *decision, now)?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, Vec::new()))
                })
                .await?
            }
            AgentTaskEntityCommand::ReviseGoalCriteria {
                operation_id,
                expected_criteria_revision,
                source,
                digest,
                provenance,
            } => {
                self.transition(now, None, move |state| {
                    revise_goal_criteria(
                        state,
                        &operation_id,
                        expected_criteria_revision,
                        source,
                        digest,
                        *provenance,
                        now,
                    )?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, Vec::new()))
                })
                .await?
            }
            AgentTaskEntityCommand::ResumeGoal {
                operation_id,
                expected_status_revision,
                top_up,
                provenance,
            } => {
                // Reactivation may make the task assignable again, so the
                // readiness read rides along exactly as activation's does.
                self.transition(now, readiness, move |state| {
                    let owed = resume_goal(
                        state,
                        &operation_id,
                        expected_status_revision,
                        top_up.map(|allocation| *allocation),
                        *provenance,
                        now,
                    )?;
                    Ok((operation_id, AgentTaskOutcomeExtras::NONE, owed))
                })
                .await?
            }
        };

        self.record_command_metrics(&reply, lifecycle_transition, wake_trigger);
        self.settle_side_effects(router, now).await?;
        Ok(reply)
    }

    /// Emits the bounded wake-disposition and lifecycle counters for a command
    /// reply. Only `Applied` replies count — a `Duplicate` answers from the
    /// dedup record without a new transition, so counting it would break the
    /// once-per-transition rule ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)).
    fn record_command_metrics(
        &self,
        reply: &AgentTaskEntityReply,
        lifecycle_transition: Option<&'static str>,
        wake_trigger: Option<&'static str>,
    ) {
        let AgentTaskEntityReply::Applied { outcome } = reply else {
            return;
        };
        match &outcome.wake {
            Some(AgentWakeOutcome::Disposition(disposition)) => {
                let trigger = wake_trigger.unwrap_or("controller");
                record_agent_domain_counter(
                    self.metrics.as_ref(),
                    METRIC_AGENT_WAKE_DISPOSITIONS,
                    1,
                    &[("outcome", disposition.as_label()), ("trigger", trigger)],
                )
                .ok();
            }
            Some(AgentWakeOutcome::Lifecycle { .. }) => {
                if let Some(transition) = lifecycle_transition {
                    record_agent_domain_counter(
                        self.metrics.as_ref(),
                        METRIC_AGENT_GOAL_LIFECYCLE,
                        1,
                        &[("transition", transition)],
                    )
                    .ok();
                }
            }
            _ => {}
        }
    }

    /// Emits the settled-epoch counter for a freshly accepted epoch-result
    /// exchange. A replayed delivery answers from the journal without a new
    /// transition, so it emits nothing.
    fn record_epoch_settlement(
        &self,
        envelope: &AgentExchangeEnvelope,
        reply: &AgentExchangeReply,
    ) {
        if envelope.kind() != AgentExchangeKind::EpochResult
            || reply.is_replayed()
            || !reply.result().is_accepted()
        {
            return;
        }
        let Ok(result) = envelope
            .payload()
            .decode::<AgentEpochResult>(AGENT_EPOCH_RESULT_PAYLOAD_TYPE)
        else {
            return;
        };
        let class = match result.status {
            AgentTaskStatus::Completed => AgentEpochOutcomeClass::Completed,
            AgentTaskStatus::Failed => AgentEpochOutcomeClass::Failed,
            _ => AgentEpochOutcomeClass::Cancelled,
        };
        record_agent_domain_counter(
            self.metrics.as_ref(),
            METRIC_AGENT_EPOCHS,
            1,
            &[("outcome", class.as_label())],
        )
        .ok();
    }

    /// The controller's monotone admission count, read from the durable state.
    ///
    /// Epoch-admission metrics are a difference of this count across a
    /// committed transition — the one source that sees a promotion made in
    /// the same breath as a fresh delivery, a release, or a resume.
    fn wake_admitted_total(&self) -> u64 {
        self.state()
            .ok()
            .and_then(|state| state.task())
            .and_then(|task| task.wake_controller.as_ref())
            .map_or(0, |controller| controller.counters().admitted)
    }

    /// The goal's lifecycle status, read from the durable state.
    ///
    /// Lifecycle metrics are a difference of this status across a committed
    /// transition — the one source that sees an observed flip (expiry,
    /// retirement by policy, escalation) made inside whatever transition
    /// first saw it true, alongside the commanded ones.
    fn goal_lifecycle_status(&self) -> Option<AgentGoalLifecycleStatus> {
        self.state()
            .ok()
            .and_then(|state| state.task())
            .and_then(|task| task.wake_controller.as_ref())
            .map(|controller| controller.lifecycle().status())
    }

    /// Reads the agent facts an assignment decision would need, when this command
    /// could make the task assignable.
    ///
    /// A task that is human-owned, already assigned, or terminal can never be
    /// assigned by this command, so its agent is not read at all.
    async fn resolve_readiness(
        &self,
        command: &AgentTaskEntityCommand,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<Option<AgentAssignmentReadiness>> {
        let (assignee, definition) = match command {
            AgentTaskEntityCommand::Create { creation, .. } => (
                creation.assignee.clone(),
                creation
                    .definition
                    .is_agent_owned()
                    .then(|| creation.definition.clone()),
            ),
            // A handoff reads the *target's* readiness — the request names
            // it — so the transfer and the generation it offers the target
            // commit in one compare-and-set. The current assignment (the
            // source's) does not block the read: the transition clears it.
            AgentTaskEntityCommand::RecordHandoff { request, .. } => {
                let Some(task) = self.state()?.task() else {
                    return Ok(None);
                };
                if task.status.is_terminal() {
                    return Ok(None);
                }
                (
                    Some(request.target.clone()),
                    task.definition
                        .is_agent_owned()
                        .then(|| task.definition.clone()),
                )
            }
            // Wake transitions never change assignability, so they read no
            // agent state at all: a scanner delivering to a passivated
            // controller costs one entity transition, not an extra durable
            // read.
            // A human-owned task never assigns, so its submission reads no
            // agent state either; the ownership refusal for an agent-owned
            // target needs no readiness to answer.
            AgentTaskEntityCommand::Describe
            | AgentTaskEntityCommand::Cancel { .. }
            | AgentTaskEntityCommand::SubmitHumanResult { .. }
            | AgentTaskEntityCommand::AdmitWake { .. }
            | AgentTaskEntityCommand::CompleteWakeOccurrence { .. }
            | AgentTaskEntityCommand::UpdateContinuousSchedule { .. }
            | AgentTaskEntityCommand::SuspendContinuousGoal { .. }
            | AgentTaskEntityCommand::ResumeContinuousGoal { .. }
            | AgentTaskEntityCommand::RenewContinuousGoal { .. }
            | AgentTaskEntityCommand::RetireContinuousGoal { .. }
            | AgentTaskEntityCommand::RecordGoalDecision { .. }
            | AgentTaskEntityCommand::ReviseGoalCriteria { .. } => return Ok(None),
            _ => {
                let Some(task) = self.state()?.task() else {
                    return Ok(None);
                };
                if task.assignment.is_some() || task.status.is_terminal() {
                    return Ok(None);
                }
                (
                    task.assignee.clone(),
                    task.definition
                        .is_agent_owned()
                        .then(|| task.definition.clone()),
                )
            }
        };

        let (Some(assignee), Some(definition)) = (assignee, definition) else {
            return Ok(None);
        };
        self.read_readiness(&assignee, &definition, now)
            .await
            .map(Some)
    }

    /// The bounded durable read of [specification 9.8](../../../docs/plans/rakka-agent/spec.md):
    /// the assignment decision reads the agent's definition and admission state,
    /// and never round-trips through the agent entity's mailbox.
    async fn read_readiness(
        &self,
        assignee: &AgentId,
        definition: &AgentTaskDefinition,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentAssignmentReadiness> {
        let agent_scope = AgentScope::new(self.scope.tenant().clone(), assignee.clone())?;
        let agent_state = load_agent_entity_state(&self.agents, &agent_scope, &self.policy)
            .await
            .map_err(|error| AgentTaskError::AgentRead {
                agent: assignee.clone(),
                message: error.to_string(),
            })?;

        Ok(agent_state.as_ref().map_or_else(
            || AgentAssignmentReadiness::not_instantiated(assignee.clone()),
            |state| AgentAssignmentReadiness::from_agent_state(state, definition, now),
        ))
    }

    /// Accepts one delivered exchange, then settles what it made possible.
    pub async fn accept(
        &mut self,
        envelope: &AgentExchangeEnvelope,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentExchangeReply> {
        self.ensure_recovered(now).await?;
        // A transition the task cannot record the history of must not happen at
        // all — and it must not happen *as a rejection* either. Whatever a
        // participant returns for an exchange becomes the task's durable decision
        // and is replayed forever, so a transient sink outage answered with
        // "rejected" would refuse that proposal for good. Failing before the
        // transition leaves the exchange outstanding on its initiator instead,
        // which re-drives it once the sink is back.
        self.require_history_headroom(now).await?;
        let admitted_before = self.wake_admitted_total();
        let stagnation_before = self.wake_stagnation_totals();
        let lifecycle_before = self.goal_lifecycle_status();
        let goal_before = self.goal_contract_status();
        let resolved_before = self.dependency_outcome_totals();
        let reply = self.host.accept(envelope, now).await?;
        self.record_admitted_epochs(admitted_before);
        self.record_stagnation_trips(stagnation_before);
        self.record_lifecycle_transition(lifecycle_before);
        self.record_goal_status_transition(goal_before);
        self.record_epoch_settlement(envelope, &reply);
        self.record_dependency_outcomes(resolved_before);
        // Accepting a delivered exchange makes *local* progress only: it may
        // decide an assignment freed escrow now permits and flush the history it
        // owes, both of which touch only the task's own state and its history
        // sink. It does **not** deliver the cross-entity exchanges that decision
        // committed (the assignment to the run). Those are drained by the
        // courier — a command's settle pass, a recovery sweep, `pump` — never
        // synchronously from inside a delivery.
        //
        // The initiator of `envelope` is mid-delivery to this task right now, so
        // driving an owed exchange back to it here would re-enter its `accept`
        // before this reply settles. A run that owes this task a settlement, and
        // a task that re-drives that run's still-outstanding assignment, would
        // otherwise recurse without bound (see [`crate::run`]'s `accept`).
        let _ = router;
        self.make_local_progress(now).await?;
        Ok(reply)
    }

    /// Decides an assignment the task now permits and flushes owed history,
    /// without delivering any owed cross-entity exchange.
    ///
    /// This is the half of [`Self::settle_side_effects`] that touches only the
    /// task's own state and its history sink. It is what a delivered exchange is
    /// allowed to trigger; see [`Self::accept`] for why the drive half is not.
    async fn make_local_progress(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        self.require_history_headroom(now).await?;
        self.observe_goal_deadline(now).await?;
        self.observe_unclaimed_expiry(now).await?;
        self.settle_requested_cancellation(now).await?;
        self.settle_handoff_resolution(now).await?;
        self.settle_team_claim_resolution(now).await?;
        self.settle_dependency_registrations(now).await?;
        self.settle_dependent_notifications(now).await?;
        self.decide_assignment(now).await?;
        self.flush_history(now).await?;
        self.park_owed_rewakes(now).await?;
        Ok(())
    }

    /// Re-owes the handoff-result exchange a settled handoff still owes its
    /// source run ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// This is the courier half of the resolution machine: the transition
    /// that settled the provenance owed the exchange in its own
    /// compare-and-set, but a crash between that commit and the initiation —
    /// or a lost initiation — must not strand the fenced source forever. The
    /// derivation is pure over durable state, the journal's initiation
    /// record guards the bounded window, and the provenance's
    /// `result_settled` marker quiesces it past that window.
    async fn settle_handoff_resolution(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<()> {
        let would_advance = {
            let state = self.state()?;
            match state.task() {
                None => false,
                Some(task) => task.handoff.as_deref().is_some_and(|handoff| {
                    handoff.is_settled()
                        && !handoff.result_settled
                        && crate::coordination::handoff_result_operation_id(
                            state.scope.tenant(),
                            &handoff.handoff,
                        )
                        .is_ok_and(|operation| !state.journal.has_initiated(&operation))
                }),
            }
        };
        if !would_advance {
            return Ok(());
        }
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| match owed_handoff_result(state, now) {
                Ok(owed) => Ok(owed.into_iter().collect()),
                Err(error) => {
                    let carried = AgentChoreographyError::from(error.clone());
                    rejection = Some(error);
                    Err(carried)
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(())
    }

    /// Re-owes the claim-result exchange a settled board claim still owes
    /// its team ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The `settle_handoff_resolution` twin: the transition that settled the
    /// claim owed the exchange in its own compare-and-set, but a crash
    /// between that commit and the initiation must not strand the board's
    /// pending entry forever. The derivation is pure over durable state, the
    /// journal's initiation record guards the bounded window, and the
    /// claim's `result_settled` marker quiesces it past that window.
    async fn settle_team_claim_resolution(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<()> {
        let would_advance = {
            let state = self.state()?;
            match state.task() {
                None => false,
                Some(task) => task.team_claim.as_deref().is_some_and(|claim| {
                    claim.is_settled()
                        && !claim.result_settled
                        && crate::coordination::team_claim_result_operation_id(
                            state.scope.tenant(),
                            &claim.claim,
                        )
                        .is_ok_and(|operation| !state.journal.has_initiated(&operation))
                }),
            }
        };
        if !would_advance {
            return Ok(());
        }
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| match owed_team_claim_result(state, now) {
                Ok(owed) => Ok(owed.into_iter().collect()),
                Err(error) => {
                    let carried = AgentChoreographyError::from(error.clone());
                    rejection = Some(error);
                    Err(carried)
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(())
    }

    /// Re-owes the dependency registrations this task's unresolved edges
    /// still owe their upstreams
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The courier half of the registry: the declaring transition owed the
    /// exchange in its own compare-and-set, but a crash between that commit
    /// and the initiation — or an edge persisted before the registry
    /// existed — must not strand the dependent. The derivation is pure over
    /// durable state, the journal guards the bounded window, and the edge's
    /// `registration_settled` marker quiesces it past that window.
    async fn settle_dependency_registrations(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<()> {
        let would_advance = {
            let state = self.state()?;
            match state.task() {
                None => false,
                Some(task) => {
                    !task.status.is_terminal()
                        && task.cancellation.is_none()
                        && task.dependencies.values().any(|edge| {
                            edge.outcome.is_none()
                                && !edge.registration_settled
                                && dependency_registration_operation_id(
                                    state.scope.tenant(),
                                    &edge.dependency,
                                    state.scope.task(),
                                )
                                .is_ok_and(|operation| !state.journal.has_initiated(&operation))
                        })
                }
            }
        };
        if !would_advance {
            return Ok(());
        }
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                match owed_dependency_registrations(state, now) {
                    Ok(owed) => Ok(owed),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(())
    }

    /// Re-owes the outcome notifications a terminal task still owes its
    /// registered dependents
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The courier half of the notification: every terminal transition owes
    /// the exchanges through [`owed_child_reports`] in its own
    /// compare-and-set, but the goal-budget and stagnation terminals commit
    /// inside transitions that predate the registry's consult — and any
    /// crash between a terminal commit and the initiation lands here. The
    /// registry entry's `outcome_settled` marker quiesces the derivation
    /// past the journal's bounded window.
    ///
    /// It is also where a notification the terminal transition had no journal
    /// headroom for is finally owed: the pass first withdraws the forward-edge
    /// registrations the terminal task can no longer act on, which is what
    /// frees the slots for it.
    async fn settle_dependent_notifications(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<()> {
        let would_advance = {
            let state = self.state()?;
            match state.task() {
                None => false,
                Some(task) => {
                    task.status.is_terminal()
                        && task.dependents.values().any(|record| {
                            !record.outcome_settled
                                && dependency_outcome_operation_id(
                                    state.scope.tenant(),
                                    state.scope.task(),
                                    &record.dependent,
                                )
                                .is_ok_and(|operation| !state.journal.has_initiated(&operation))
                        })
                }
            }
        };
        if !would_advance {
            return Ok(());
        }
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                // A task that terminalized under a binary predating the
                // withdrawal — or one whose registrations were still owed when
                // it terminalized — reclaims their slots here, before the
                // budget is measured.
                withdraw_moot_registrations(state);
                match owed_dependent_outcomes(state, now, owed_exchange_budget(state, 0)) {
                    Ok(owed) => Ok(owed),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(())
    }

    /// Expires the goal contract when its own deadline has passed
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)): the
    /// settle pass is the goal entry point that always commits, so a passed
    /// deadline lands durably here even when no command arrives — a command's
    /// own observation would be discarded with the command's refusal.
    ///
    /// The write is skipped entirely while nothing would flip, so a sweep over
    /// a healthy goal burns no revision.
    async fn observe_goal_deadline(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        let would_expire = self
            .state()?
            .task()
            .and_then(|task| task.goal_state.as_deref())
            .is_some_and(|goal| {
                !goal.status().is_terminal()
                    && goal
                        .spec()
                        .spec()
                        .deadline
                        .is_some_and(|deadline| now.as_millis() >= deadline.as_millis())
            });
        if !would_expire {
            return Ok(());
        }
        let operation_id = AgentOperationId::new(
            AgentOperationKind::Command,
            [
                self.scope.tenant().as_str(),
                self.scope.task().as_str(),
                "goal-deadline",
            ],
        )?;
        let goal_before = self.goal_contract_status();
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let observe = |state: &mut AgentTaskState| -> AgentTaskResult<()> {
                    let task = state.task_mut()?;
                    let Some(goal) = task.goal_state.as_deref_mut() else {
                        return Ok(());
                    };
                    if goal.observe_deadline(now).is_none() {
                        return Ok(());
                    }
                    if let Some(controller) = task.wake_controller.as_mut() {
                        controller.retire_by_policy();
                    }
                    state.updated_at = now;
                    record_wake_history(
                        state,
                        AgentTaskHistoryKind::GoalDecided,
                        &operation_id,
                        "expired deadline-expired".to_string(),
                        now,
                    );
                    Ok(())
                };
                match observe(state) {
                    Ok(()) => Ok(Vec::new()),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        self.record_goal_status_transition(goal_before);
        Ok(())
    }

    /// Expires a board-governed task that has waited unclaimed past its
    /// definition's horizon ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)),
    /// observed lazily like every board expiry — no timer ever fires. The
    /// request rides the cancellation machinery whole: with no claim, no
    /// assignment, and no children, the marker finalizes in the same
    /// transition, the terminal report reaches a delegating parent, and the
    /// locked escrow settles home.
    ///
    /// The write is skipped entirely while nothing would expire, so a sweep
    /// over a healthy task burns no revision.
    async fn observe_unclaimed_expiry(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        let would_expire = {
            let state = self.state()?;
            let updated_at = state.updated_at;
            state
                .task()
                .is_some_and(|task| task_unclaimed_expired(task, updated_at, now))
        };
        if !would_expire {
            return Ok(());
        }
        let operation_id = AgentOperationId::new(
            AgentOperationKind::Command,
            [
                self.scope.tenant().as_str(),
                self.scope.task().as_str(),
                "unclaimed-expiry",
            ],
        )?;
        let goal_before = self.goal_contract_status();
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let step =
                    |state: &mut AgentTaskState| -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
                        let updated_at = state.updated_at;
                        let Some(task) = state.task.as_ref() else {
                            return Ok(Vec::new());
                        };
                        if !task_unclaimed_expired(task, updated_at, now) {
                            return Ok(Vec::new());
                        }
                        request_task_cancellation(
                            state,
                            &operation_id,
                            AgentTaskTerminalReason::CancellationRequested {
                                reason: "unclaimed-expired".to_string(),
                            },
                            now,
                        )
                    };
                match step(state) {
                    Ok(owed) => Ok(owed),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        self.record_goal_status_transition(goal_before);
        Ok(())
    }

    /// Advances a requested cancellation from the settle pass
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)): the
    /// settle pass is the goal entry point that always commits, so a terminal
    /// goal decision in the cancel or expiry family lands on the root task
    /// here even when no command arrives, the run-cancel a crash may have
    /// raced is re-owed under its derived operation id, and a closed ledger
    /// finalizes the marker. One chokepoint, so every goal-decision ingress —
    /// the operator command, the evaluation door, the deadline observation
    /// above — propagates identically without owning any of it.
    ///
    /// The write is skipped entirely while nothing would advance, so a sweep
    /// over a healthy task burns no revision.
    async fn settle_requested_cancellation(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<()> {
        let would_advance = {
            let state = self.state()?;
            match state.task() {
                None => false,
                Some(task) if task.status.is_terminal() => false,
                Some(task) => match task.cancellation.as_deref() {
                    None => task.goal_state.as_deref().is_some_and(|goal| {
                        goal.terminal()
                            .is_some_and(|decision| decision.reason.requests_root_cancellation())
                    }),
                    Some(_) => {
                        let owes_run_cancel = task.assignment.as_ref().is_some_and(|assignment| {
                            assignment.status == AgentAssignmentStatus::Accepted
                                && run_cancel_operation_id(&state.scope, assignment.generation)
                                    .is_ok_and(|operation| !state.journal.has_initiated(&operation))
                        });
                        owes_run_cancel || task.escrow.outstanding().count() == 0
                    }
                },
            }
        };
        if !would_advance {
            return Ok(());
        }
        let operation_id = AgentOperationId::new(
            AgentOperationKind::Command,
            [
                self.scope.tenant().as_str(),
                self.scope.task().as_str(),
                "goal-cancellation",
            ],
        )?;
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let step =
                    |state: &mut AgentTaskState| -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
                        let Some(task) = state.task.as_ref() else {
                            return Ok(Vec::new());
                        };
                        if task.status.is_terminal() {
                            return Ok(Vec::new());
                        }
                        if task.cancellation.is_none() {
                            let Some(code) = task
                                .goal_state
                                .as_deref()
                                .and_then(AgentGoalState::terminal)
                                .filter(|decision| decision.reason.requests_root_cancellation())
                                .map(|decision| decision.reason.code())
                            else {
                                return Ok(Vec::new());
                            };
                            return request_task_cancellation(
                                state,
                                &operation_id,
                                AgentTaskTerminalReason::CancellationRequested {
                                    reason: format!("goal-{code}"),
                                },
                                now,
                            );
                        }
                        let mut owed: Vec<AgentExchangeEnvelope> =
                            owed_run_cancel(state, now)?.into_iter().collect();
                        owed.extend(finalize_task_cancellation(state, &operation_id, now)?);
                        Ok(owed)
                    };
                match step(state) {
                    Ok(owed) => Ok(owed),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(())
    }

    /// Flushes whatever history the task owes, and fails closed if the outbox
    /// still cannot hold what the next transition may record.
    async fn require_history_headroom(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        if self.state()?.history_headroom() >= AGENT_TASK_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }

        // The outbox is backed up, so try to drain it before refusing.
        self.flush_history(now).await?;

        let state = self.state()?;
        if state.history_headroom() >= AGENT_TASK_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }
        Err(AgentTaskError::HistoryBacklog {
            pending: state.pending_history().len(),
            maximum: AGENT_TASK_PENDING_HISTORY_CAPACITY,
        })
    }

    /// Decides the assignment when the task awaits one, flushes the history the
    /// task owes, and drives the exchanges it owes.
    ///
    /// It is safe to call at any time and from any node: every step reads what it
    /// needs from durable state, so calling it after a transition, after recovery,
    /// or on a timer are the same operation.
    pub async fn settle_side_effects(
        &mut self,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentTaskProgress> {
        self.ensure_recovered(now).await?;
        // The settle pass commits transitions of its own — an assignment
        // decision here, a settlement inside the drive — so it stands behind
        // the same headroom fence as every command and exchange: a backlog is
        // refused (after an attempt to drain it) rather than pushed past the
        // pending-history bound.
        self.require_history_headroom(now).await?;
        self.observe_goal_deadline(now).await?;
        self.observe_unclaimed_expiry(now).await?;
        self.settle_requested_cancellation(now).await?;
        self.settle_handoff_resolution(now).await?;
        self.settle_team_claim_resolution(now).await?;
        self.settle_dependency_registrations(now).await?;
        self.settle_dependent_notifications(now).await?;
        let assigned = self.decide_assignment(now).await?;
        let flushed = self.flush_history(now).await?;
        let rewakes_parked = self.park_owed_rewakes(now).await?;
        let resolved_before = self.dependency_outcome_totals();
        let report = drive_pending_exchanges(&mut self.host, router, now).await?;
        self.record_dependency_outcomes(resolved_before);
        Ok(AgentTaskProgress {
            assigned,
            history_flushed: flushed,
            settled: report.settled,
            failed: report.failed,
            outstanding: self.host.outstanding()?.len(),
            rewakes_parked,
        })
    }

    /// Parks the controller-originated re-wakes the controller owes, when a
    /// wake-timer parker is wired.
    ///
    /// The transition that owed a re-wake recorded it unparked; this pass
    /// parks the derived retry occurrence idempotently and then marks the
    /// slot parked in its own small transition. A crash between the two
    /// re-parks the identical entry — answered as a duplicate — and marks;
    /// a schedule update in between leaves one fenced orphan entry, which a
    /// later delivery answers as fenced and the store marks terminal.
    async fn park_owed_rewakes(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<usize> {
        let Some(parker) = self.rewake_parker.clone() else {
            return Ok(0);
        };
        let owed: Vec<(crate::wake::AgentWakeRewakeCause, AgentTimestampMillis, u64)> = {
            let state = self.state()?;
            let Some(task) = state.task() else {
                return Ok(0);
            };
            let Some(controller) = task.wake_controller.as_ref() else {
                return Ok(0);
            };
            if !controller.lifecycle().rewakes().owes_parking() {
                return Ok(0);
            }
            let rewakes = controller.lifecycle().rewakes();
            [
                (crate::wake::AgentWakeRewakeCause::Backoff, rewakes.backoff),
                (
                    crate::wake::AgentWakeRewakeCause::WindowTurn,
                    rewakes.window_turn,
                ),
            ]
            .into_iter()
            .filter_map(|(cause, slot)| {
                slot.filter(|slot| !slot.parked)
                    .map(|slot| (cause, slot.due_at, slot.attempt))
            })
            .collect()
        };

        let (goal, schedule_revision, policy_revision) = {
            let state = self.state()?;
            let task = state.task().expect("checked above");
            let spec = task
                .goal_mode
                .continuous()
                .expect("a controller exists only on a continuous task");
            (
                task.goal.clone().expect("a continuous task binds its goal"),
                spec.schedule_revision,
                spec.wake_policy.revision(),
            )
        };

        let mut parked = 0;
        for (cause, due_at, attempt) in owed {
            let binding = AgentWakeBinding::new(
                self.scope.tenant().clone(),
                goal.clone(),
                schedule_revision,
                crate::wake::AgentWakeOccurrence::Retry {
                    due_at,
                    cause,
                    attempt,
                },
                crate::wake::AgentWakeTriggerKind::Controller,
                now,
                policy_revision,
            )?;
            let entry = crate::wake_timers::AgentWakeTimerEntry::new(
                binding,
                self.scope.task().clone(),
                now,
            );
            parker
                .park(entry)
                .await
                .map_err(AgentTaskError::WakeTimer)?;
            self.host
                .initiate(now, |state| {
                    if let Some(controller) = state
                        .task
                        .as_mut()
                        .and_then(|task| task.wake_controller.as_mut())
                    {
                        controller.mark_rewake_parked(cause, due_at, attempt);
                    }
                    state.updated_at = now;
                    Ok(Vec::new())
                })
                .await?;
            parked += 1;
        }
        Ok(parked)
    }

    /// Reads the agent's durable admission state and, if the task awaits one,
    /// decides its assignment.
    ///
    /// This is the recovery path: a task whose earlier decision was refused —
    /// because its agent was suspended, or did not exist yet — is decided here
    /// when that changes, with no new command and no lost work.
    async fn decide_assignment(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<bool> {
        let Some((assignee, definition)) = self.pending_assignment()? else {
            return Ok(false);
        };
        let readiness = self.read_readiness(&assignee, &definition, now).await?;
        if self.state()?.task().is_some_and(|task| {
            pending_assignment_refusal(task, &readiness).is_some_and(|(reason, detail)| {
                assignment_refusal_is_current(
                    task,
                    &readiness.agent,
                    reason,
                    &bounded_detail(detail),
                )
            })
        }) {
            // Deciding would record nothing new, so the pass skips the write:
            // a settle sweep over a still-refused task must not burn a
            // revision per pass.
            return Ok(false);
        }

        let mut assigned = false;
        let mut rejection = None;
        // A settle-pass refusal may consult the goal's exhaustion policy, so
        // this commit path counts goal-status flips exactly as the command
        // path does.
        let goal_before = self.goal_contract_status();
        let committed = self
            .host
            .initiate(now, |state| {
                match decide_assignment(state, &readiness, now) {
                    Ok(envelopes) => {
                        // The decision may instead have exhausted the task's
                        // assignments and owed its terminal reports; only a
                        // run-creation command counts as an assignment made.
                        assigned = envelopes
                            .iter()
                            .any(|envelope| envelope.kind() == AgentExchangeKind::Assignment);
                        Ok(envelopes)
                    }
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;

        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        self.record_goal_status_transition(goal_before);
        Ok(assigned)
    }

    /// Appends the history the task owes its sink, then drops the entries the
    /// sink durably accepted.
    ///
    /// A crash between the append and the clearing re-drives the append, which is
    /// idempotent on the entry's sequence; a crash before the append leaves the
    /// entry owed in durable state. Neither loses an entry, and neither writes one
    /// twice.
    async fn flush_history(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<usize> {
        let pending = self.state()?.pending_history().to_vec();
        if pending.is_empty() {
            return Ok(0);
        }

        let mut flushed = Vec::with_capacity(pending.len());
        for entry in &pending {
            self.history.append(&self.scope, entry).await?;
            flushed.push(entry.sequence);
        }

        self.host
            .initiate(now, |state| {
                state.clear_flushed_history(&flushed);
                Ok(Vec::new())
            })
            .await?;
        Ok(flushed.len())
    }

    fn pending_assignment(&self) -> AgentTaskResult<Option<(AgentId, AgentTaskDefinition)>> {
        let Some(task) = self.state()?.task() else {
            return Ok(None);
        };
        if !task.awaits_assignment() {
            return Ok(None);
        }
        Ok(task
            .assignee
            .clone()
            .map(|assignee| (assignee, task.definition.clone())))
    }

    /// Runs one bounded command transition and records its resolved operation id
    /// in the same compare-and-set.
    ///
    /// A rejected transition never reaches the store, so it leaves no trace in
    /// the operation log and a corrected retry under the same operation id is
    /// still accepted. The domain rejection is carried out of the substrate's
    /// closure unchanged, so a caller sees the task's own stable code rather than
    /// a choreography error that happened to transport it.
    async fn transition<F>(
        &mut self,
        now: AgentTimestampMillis,
        readiness: Option<AgentAssignmentReadiness>,
        transition: F,
    ) -> AgentTaskResult<AgentTaskEntityReply>
    where
        F: FnOnce(
            &mut AgentTaskState,
        ) -> AgentTaskResult<(
            AgentOperationId,
            AgentTaskOutcomeExtras,
            Vec<AgentExchangeEnvelope>,
        )>,
    {
        let mut outcome = None;
        let mut rejection = None;
        let admitted_before = self.wake_admitted_total();
        let lifecycle_before = self.goal_lifecycle_status();
        let goal_before = self.goal_contract_status();
        let committed = self
            .host
            .initiate(now, |state| {
                let assign =
                    |state: &mut AgentTaskState| -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
                        let (operation_id, extras, mut owed) = transition(state)?;
                        // The command's own transition may have made the task
                        // eligible. Deciding here means the assignment, the run-creation
                        // command it owes, and the transition that caused it all commit
                        // together: the task can never be durably assigned and have
                        // forgotten to tell the run. A wake admission owes its epoch's
                        // creation exchange the same way.
                        if let Some(readiness) = &readiness {
                            owed.extend(decide_assignment(state, readiness, now)?);
                        }
                        let mut result = state.outcome();
                        result.wake = extras.wake;
                        result.submission = extras.submission;
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
                        state.updated_at = now;
                        outcome = Some(result);
                        Ok(owed)
                    };

                match assign(state) {
                    Ok(owed) => Ok(owed),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;

        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        self.record_admitted_epochs(admitted_before);
        self.record_lifecycle_transition(lifecycle_before);
        self.record_goal_status_transition(goal_before);
        Ok(AgentTaskEntityReply::Applied {
            outcome: outcome.expect("an accepted transition produces an outcome"),
        })
    }

    /// Emits one `admitted` epoch count per admission the just-committed
    /// transition made, measured as the difference of the controller's
    /// monotone admission counter. A transition that did not commit never
    /// reaches this, so a replay or a lost compare-and-set emits nothing.
    fn record_admitted_epochs(&self, admitted_before: u64) {
        let admitted = self.wake_admitted_total().saturating_sub(admitted_before);
        if admitted > 0 {
            record_agent_domain_counter(
                self.metrics.as_ref(),
                METRIC_AGENT_EPOCHS,
                admitted,
                &[("outcome", "admitted")],
            )
            .ok();
        }
    }

    /// The task's durable result cells a human submission can move: whether a
    /// result is accepted, the rejection count, and whether the task failed.
    fn submission_decision_snapshot(&self) -> (bool, u32, bool) {
        self.state()
            .ok()
            .and_then(|state| state.task())
            .map_or((false, 0, false), |task| {
                (
                    task.accepted_result.is_some(),
                    task.rejection_count,
                    matches!(task.status, AgentTaskStatus::Failed),
                )
            })
    }

    /// Emits one human-result decision count as the difference of the task's
    /// durable result cells across the committed transition — so duplicates,
    /// durable echoes, and non-committing refusals emit nothing.
    fn record_submission_decision(&self, before: (bool, u32, bool)) {
        let (had_result, rejections_before, failed_before) = before;
        let (has_result, rejections, failed) = self.submission_decision_snapshot();
        let outcome = if !had_result && has_result {
            Some("accepted")
        } else if rejections > rejections_before {
            Some(if failed && !failed_before {
                "exhausted"
            } else {
                "rejected"
            })
        } else {
            None
        };
        if let Some(outcome) = outcome {
            record_agent_domain_counter(
                self.metrics.as_ref(),
                METRIC_AGENT_HUMAN_RESULTS,
                1,
                &[("outcome", outcome)],
            )
            .ok();
        }
    }

    /// The task's per-outcome resolved-dependency totals: completed, failed,
    /// cancelled.
    fn dependency_outcome_totals(&self) -> (usize, usize, usize) {
        self.state()
            .ok()
            .and_then(|state| state.task())
            .map_or((0, 0, 0), |task| {
                let mut totals = (0, 0, 0);
                for edge in task.dependencies.values() {
                    match edge.outcome {
                        Some(AgentTaskDependencyOutcome::Completed) => totals.0 += 1,
                        Some(AgentTaskDependencyOutcome::Failed) => totals.1 += 1,
                        Some(AgentTaskDependencyOutcome::Cancelled) => totals.2 += 1,
                        None => {}
                    }
                }
                totals
            })
    }

    /// Emits one dependency-outcome count per edge the just-committed work
    /// durably resolved, measured as the difference of the per-outcome
    /// totals — so a replayed or conflicting delivery emits nothing.
    fn record_dependency_outcomes(&self, before: (usize, usize, usize)) {
        let after = self.dependency_outcome_totals();
        for (count, outcome) in [
            (after.0.saturating_sub(before.0), "completed"),
            (after.1.saturating_sub(before.1), "failed"),
            (after.2.saturating_sub(before.2), "cancelled"),
        ] {
            if count > 0 {
                record_agent_domain_counter(
                    self.metrics.as_ref(),
                    METRIC_AGENT_DEPENDENCY_OUTCOMES,
                    count as u64,
                    &[("outcome", outcome)],
                )
                .ok();
            }
        }
    }

    /// The controller's durable stagnation-trip totals: repeated-result, then
    /// no-progress.
    fn wake_stagnation_totals(&self) -> (u64, u64) {
        self.state()
            .ok()
            .and_then(|state| state.task())
            .and_then(|task| task.wake_controller.as_ref())
            .map_or((0, 0), |controller| {
                let counters = controller.counters();
                (
                    counters.stagnation_repeated,
                    counters.stagnation_no_progress,
                )
            })
    }

    /// Emits stagnation-trip counts as the difference of the controller's
    /// durable counters across the committed transition, so a replayed
    /// settlement emits nothing — and a `Continue` trip, which flips no
    /// status, still counts.
    fn record_stagnation_trips(&self, before: (u64, u64)) {
        let (repeated_before, no_progress_before) = before;
        let (repeated, no_progress) = self.wake_stagnation_totals();
        for (count, trigger) in [
            (
                repeated.saturating_sub(repeated_before),
                AgentStagnationTrigger::RepeatedResult,
            ),
            (
                no_progress.saturating_sub(no_progress_before),
                AgentStagnationTrigger::NoProgress,
            ),
        ] {
            if count > 0 {
                record_agent_domain_counter(
                    self.metrics.as_ref(),
                    METRIC_AGENT_GOAL_STAGNATION,
                    count,
                    &[("trigger", trigger.code())],
                )
                .ok();
            }
        }
    }

    /// Emits one lifecycle-transition count when the just-committed
    /// transition changed the goal's lifecycle status — a command or an
    /// observed flip (expiry, retirement by policy, escalation) alike. The
    /// label is the status arrived at; a controller that first appears in
    /// the transition emits nothing, because creation is not a transition
    /// out of any prior status. Renewal leaves the status unchanged and is
    /// counted from its command instead.
    /// The goal contract's status, read from the durable state.
    ///
    /// Goal-status metrics are a difference of this status across a committed
    /// transition, exactly as the admission gate's are: the one source that
    /// sees a projected or policy-driven move — a budget park, a terminal
    /// projection, an observed expiry — alongside the commanded ones.
    fn goal_contract_status(&self) -> Option<AgentGoalStatus> {
        self.state()
            .ok()
            .and_then(|state| state.task())
            .and_then(|task| task.goal_state.as_deref())
            .map(AgentGoalState::status)
    }

    fn record_goal_status_transition(&self, before: Option<AgentGoalStatus>) {
        let after = self.goal_contract_status();
        if before == after {
            return;
        }
        let Some(after) = after else {
            return;
        };
        record_agent_domain_counter(
            self.metrics.as_ref(),
            METRIC_AGENT_GOAL_STATUS,
            1,
            &[("transition", after.as_label())],
        )
        .ok();
    }

    fn record_lifecycle_transition(&self, before: Option<AgentGoalLifecycleStatus>) {
        let (Some(before), Some(after)) = (before, self.goal_lifecycle_status()) else {
            return;
        };
        if before == after {
            return;
        }
        let transition = match after {
            AgentGoalLifecycleStatus::Active => "resumed",
            AgentGoalLifecycleStatus::Suspended => "suspended",
            AgentGoalLifecycleStatus::Expired => "expired",
            AgentGoalLifecycleStatus::Retired => "retired",
        };
        record_agent_domain_counter(
            self.metrics.as_ref(),
            METRIC_AGENT_GOAL_LIFECYCLE,
            1,
            &[("transition", transition)],
        )
        .ok();
    }

    async fn ensure_recovered(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        // Recovery is lazy and idempotent: the first message after activation
        // loads the authoritative state, which is exactly what an entity
        // re-materialized on a new shard owner must do before it transitions.
        if !self.recovered || self.host.state().is_err() {
            self.recover(now).await?;
        }
        Ok(())
    }
}

/// What one pass of the task entity's settlement did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskProgress {
    /// Whether the pass decided an assignment.
    pub assigned: bool,
    /// How many history entries it appended to the sink.
    pub history_flushed: usize,
    /// How many exchanges it settled.
    pub settled: usize,
    /// How many delivery attempts failed, leaving their exchange outstanding.
    pub failed: usize,
    /// How many exchanges the task still owes.
    pub outstanding: usize,
    /// How many controller-originated re-wakes it parked durably.
    #[serde(default)]
    pub rewakes_parked: usize,
}

/// The serializable command protocol of the typed-task entity.
///
/// Large payloads are boxed so the enum stays small enough to move cheaply
/// through mailboxes and remote envelopes, and nothing in it is an `Arc` or an
/// in-process reply channel: the protocol is serializable from this first commit,
/// so no later slice has to retrofit remoting into an entity whose commands
/// cannot cross a node boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskEntityCommand {
    /// Create the task. The operation id comes from the durable, deduplicated
    /// ingress that accepted the work
    /// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)); a
    /// delegating run creates a child task through the equivalent
    /// [`AgentExchangeKind::Creation`] exchange instead.
    Create {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// What to create.
        creation: Box<AgentTaskCreation>,
    },
    /// Declare one dependency edge after creation.
    DeclareDependency {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The edge to declare.
        declaration: Box<AgentTaskDependencyDeclaration>,
    },
    /// Record how one dependency resolved, applying its failure policy.
    RecordDependencyOutcome {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The dependency that resolved.
        dependency: AgentTaskId,
        /// How it resolved.
        outcome: AgentTaskDependencyOutcome,
    },
    /// Cancel the task.
    Cancel {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// A bounded, stable reason.
        reason: String,
    },
    /// Record one handoff: transfer responsibility for this same task from
    /// its current accepted assignment to a target agent
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)). The
    /// operation id comes from the A2A ingress, derived under
    /// [`crate::identity::AgentOperationKind::Handoff`] from the handoff's
    /// deduplication key.
    RecordHandoff {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The transfer to record, validated against durable state.
        request: Box<AgentTaskHandoffRequest>,
    },
    /// Submit an authenticated human or external service's typed result to a
    /// human-owned task, through the same validation core as a run's
    /// proposal ([specification 8.12](../../../docs/plans/rakka-agent/spec.md);
    /// scenario 41) — the ingress door slice 1.12 deferred. The operation id
    /// comes from the authenticated, deduplicated ingress, derived by
    /// [`human_result_operation_id`]: pure over
    /// `(tenant, task, discriminator)`, so a retried send converges on the
    /// original decision and a corrected resubmission after a rejection
    /// carries a new discriminator.
    ///
    /// A human task never substitutes for an effect-bound checkpoint
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)):
    /// approving, authorizing, or reconciling a specific effect stays bound
    /// to the exact effect intent through
    /// [`crate::checkpoints::AgentCheckpoint`].
    SubmitHumanResult {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The submission to validate.
        submission: Box<AgentHumanResultSubmission>,
    },
    /// Deliver one wake occurrence to the continuous goal's controller.
    ///
    /// Every trigger path — the shared scanner, an external event, an
    /// authenticated A2A command, a callback — injects this same command with
    /// the operation id the binding itself derives, so duplicate delivery is
    /// deduplicated by construction
    /// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
    AdmitWake {
        /// The stable operation id this command deduplicates on. It must be
        /// the binding's own derived admission operation id.
        operation_id: AgentOperationId,
        /// The wake to disposition.
        binding: Box<AgentWakeBinding>,
    },
    /// Release the active occurrence a completed execution owned.
    CompleteWakeOccurrence {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The active wake to release.
        wake: AgentWakeId,
    },
    /// Take a schedule update into force, fencing parked occurrences the old
    /// schedule created.
    UpdateContinuousSchedule {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The strictly newer schedule revision.
        schedule_revision: ScheduleRevision,
        /// A strictly newer wake-policy revision riding the same update, when
        /// the policy changes too.
        wake_policy: Option<Box<AgentWakePolicyRevision>>,
    },
    /// Suspend the continuous goal: triggers coalesce or drop per the
    /// suspension policy until an authorized resume. Fenced on the monotonic
    /// lifecycle revision, so a stale replay cannot reorder over a later
    /// transition.
    SuspendContinuousGoal {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The lifecycle revision this command expects to advance.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// A bounded operator reason, when one is given.
        reason: Option<String>,
        /// Who commanded the transition.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Resume a suspended continuous goal, clearing its failure backoff and
    /// promoting whatever the suspension parked.
    ResumeContinuousGoal {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The lifecycle revision this command expects to advance.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who commanded the transition.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Extend the continuous goal's effective expiry, inside the renewal
    /// window its policy requires.
    RenewContinuousGoal {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The lifecycle revision this command expects to advance.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// The strictly later effective expiry.
        new_expires_at: AgentTimestampMillis,
        /// Who commanded the transition.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Retire the continuous goal. Absorbing: no further admission, ever.
    RetireContinuousGoal {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The lifecycle revision this command expects to advance.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who commanded the transition.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Activate a `Proposed` goal contract
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)). Fenced
    /// on the goal's monotonic status revision.
    ActivateGoal {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The status revision this command expects to advance.
        expected_status_revision: AgentRevisionNumber,
        /// Who commanded the transition.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Record a terminal goal decision — the entry point slice 4.2's
    /// evaluator, an administrative cancellation, and policy terminations
    /// drive. A criteria decision must carry the evaluation it rests on; the
    /// decision's own `expected_status_revision` fences it.
    RecordGoalDecision {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The decision to record.
        decision: Box<AgentGoalDecision>,
    },
    /// Reactivate a `Waiting` goal, optionally widening its ledger under the
    /// definition ceilings — the un-park door of the goal-scope
    /// budget-exhaustion policy
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// This door lifts only the admission park the goal's own policy made.
    /// When the gate is also suspended under a reason this command does not
    /// own — an operator's suspension, or the first reason of a mixed-reason
    /// park — the goal reactivates and immediately re-parks as
    /// `AdmissionSuspended`, and the reply honestly reads `Waiting`: resume
    /// converges in two commands, this one and the gate's own
    /// [`Self::ResumeContinuousGoal`].
    ResumeGoal {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The status revision this command expects to advance.
        expected_status_revision: AgentRevisionNumber,
        /// The allocation increase the owner grants, when one does.
        top_up: Option<Box<AgentBudgetAllocation>>,
        /// Who commanded the transition.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Revise the goal's success criteria
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)):
    /// advances the criteria revision — and with it the spec revision — so
    /// evaluations against the old revision are refused stale at the decision
    /// door. Fenced on the criteria revision, not the status revision: the
    /// status does not move, and a concurrent park or resume must not refuse
    /// a criteria revision.
    ReviseGoalCriteria {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The criteria revision this command expects to advance.
        expected_criteria_revision: AgentRevisionNumber,
        /// Where the revised criteria live.
        source: AgentGoalCriteriaSource,
        /// Content digest of the revised criteria, when fingerprinted.
        digest: Option<AgentContentDigest>,
        /// Who commanded the revision.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Read the task's bounded durable projection.
    Describe,
}

impl AgentTaskEntityCommand {
    /// The operation id this command deduplicates on, when it mutates state.
    #[must_use]
    pub const fn operation_id(&self) -> Option<&AgentOperationId> {
        match self {
            Self::Create { operation_id, .. }
            | Self::DeclareDependency { operation_id, .. }
            | Self::RecordDependencyOutcome { operation_id, .. }
            | Self::Cancel { operation_id, .. }
            | Self::RecordHandoff { operation_id, .. }
            | Self::SubmitHumanResult { operation_id, .. }
            | Self::AdmitWake { operation_id, .. }
            | Self::CompleteWakeOccurrence { operation_id, .. }
            | Self::UpdateContinuousSchedule { operation_id, .. }
            | Self::SuspendContinuousGoal { operation_id, .. }
            | Self::ResumeContinuousGoal { operation_id, .. }
            | Self::RenewContinuousGoal { operation_id, .. }
            | Self::RetireContinuousGoal { operation_id, .. }
            | Self::ActivateGoal { operation_id, .. }
            | Self::RecordGoalDecision { operation_id, .. }
            | Self::ResumeGoal { operation_id, .. }
            | Self::ReviseGoalCriteria { operation_id, .. } => Some(operation_id),
            Self::Describe => None,
        }
    }
}

/// The serializable reply protocol of the typed-task entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskEntityReply {
    /// The command transitioned the task.
    Applied {
        /// The outcome of the transition.
        outcome: AgentTaskOutcome,
    },
    /// The operation id was already applied; this is the original outcome, and
    /// no second transition happened.
    Duplicate {
        /// The outcome the original application produced.
        outcome: AgentTaskOutcome,
    },
    /// The task's bounded durable projection, absent if it was never created.
    Snapshot(Option<Box<AgentTaskSnapshot>>),
    /// What one settlement pass did.
    Progressed {
        /// The pass's report.
        progress: AgentTaskProgress,
    },
    /// The command was rejected.
    ///
    /// A rejection is not proof the command did not apply: a settlement
    /// failure after the transition committed — a history-sink outage, a
    /// delivery fault — reaches the caller as this reply too. Retrying with
    /// the same operation id is always safe; a command that committed answers
    /// [`Self::Duplicate`] with its original outcome.
    Rejected {
        /// Stable machine-readable error code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentTaskEntityReply {
    fn rejected(error: &AgentTaskError) -> Self {
        Self::Rejected {
            code: error.code().to_string(),
            message: error.to_string(),
        }
    }
}

/// The process-local message the typed-task entity accepts.
///
/// The reply channels never cross a node boundary:
/// [`init_agent_task_entity_remote_sharding`] reconstructs the exchange arm on
/// the owning node from the [`AgentExchangeEnvelope`] that arrived over
/// `rakka-remote`, which is the surface every cross-entity command travels.
pub enum AgentTaskEntityMessage {
    /// An ingress or administrative command.
    Command {
        /// The command to apply.
        command: Box<AgentTaskEntityCommand>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentTaskEntityReply>,
    },
    /// A cross-entity exchange: a delegating run's creation, or a run's result
    /// proposal.
    Exchange {
        /// The exchange to apply.
        envelope: Box<AgentExchangeEnvelope>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentExchangeReply>,
    },
    /// Decide a pending assignment, flush owed history, and drive owed
    /// exchanges.
    ///
    /// The entity does this itself after every transition. The command exists so
    /// that a recovery sweep or a test can drive a task that was lost between
    /// persisting what it owed and delivering it.
    Settle {
        /// Where the reply goes.
        reply_to: ReplyTo<AgentTaskEntityReply>,
    },
}

impl Debug for AgentTaskEntityMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Command { command, .. } => f
                .debug_struct("AgentTaskEntityMessage::Command")
                .field("command", command)
                .finish_non_exhaustive(),
            Self::Exchange { envelope, .. } => f
                .debug_struct("AgentTaskEntityMessage::Exchange")
                .field("envelope", envelope)
                .finish_non_exhaustive(),
            Self::Settle { .. } => f
                .debug_struct("AgentTaskEntityMessage::Settle")
                .finish_non_exhaustive(),
        }
    }
}

/// The actor-backed host of one sharded typed-task entity.
///
/// The actor is a routing and recovery shell: every decision lives in
/// [`AgentTaskEntityStore`] and every durable fact lives in the state store, so
/// the entity can passivate after any message and recover on another pod
/// ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
///
/// An entity id that does not parse into an [`AgentTaskScope`] cannot address a
/// durable record, so such an entity rejects every message instead of guessing a
/// scope.
pub struct AgentTaskEntity<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    entity: Result<AgentTaskEntityStore<Store, Agents, History>, AgentIdentityError>,
    router: AgentExchangeRouter,
    clock: AgentTaskClock,
}

impl<Store, Agents, History> AgentTaskEntity<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    /// Creates an entity for one sharded entity id.
    #[must_use]
    pub fn new(
        entity_id: &EntityId,
        store: Store,
        agents: Agents,
        history: History,
        router: AgentExchangeRouter,
        clock: AgentTaskClock,
        policy: AgentSchemaPolicy,
    ) -> Self {
        let entity = AgentTaskScope::from_entity_id(entity_id).map(|scope| {
            AgentTaskEntityStore::new(scope, store, agents, history).with_schema_policy(policy)
        });
        Self {
            entity,
            router,
            clock,
        }
    }

    /// Wires the durable wake-timer parker the hosted entity's settle passes
    /// park controller-originated re-wakes through.
    #[must_use]
    pub fn with_wake_timers(mut self, parker: Arc<dyn AgentWakeRewakeParker>) -> Self {
        self.entity = self.entity.map(|store| store.with_wake_timers(parker));
        self
    }

    /// Wires a metrics recorder for the hosted entity's bounded wake, epoch,
    /// and lifecycle counters.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.entity = self.entity.map(|store| store.with_metrics(metrics));
        self
    }

    fn store(
        &mut self,
    ) -> Result<&mut AgentTaskEntityStore<Store, Agents, History>, AgentTaskError> {
        self.entity
            .as_mut()
            .map_err(|error| AgentTaskError::Identity(error.clone()))
    }
}

impl<Store, Agents, History> Actor for AgentTaskEntity<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    type Msg = AgentTaskEntityMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            // A transition is stamped where it commits, on the owner that wrote
            // it.
            let now = (self.clock)();
            let router = self.router.clone();

            match msg {
                AgentTaskEntityMessage::Command { command, reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentTaskEntityReply::rejected(&error),
                        Ok(entity) => match entity.apply(*command, &router, now).await {
                            Ok(reply) => reply,
                            Err(error) => AgentTaskEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
                AgentTaskEntityMessage::Exchange { envelope, reply_to } => {
                    let Ok(entity) = self.store() else {
                        // A misrouted entity cannot answer an exchange. Dropping
                        // the reply leaves the exchange outstanding on its
                        // initiator, which re-drives it — which is exactly what a
                        // lost delivery does, and what the substrate is built to
                        // converge from.
                        return Ok(ActorAction::Continue);
                    };
                    if let Ok(reply) = entity.accept(&envelope, &router, now).await {
                        let _reply_dropped = reply_to.reply(reply);
                    }
                }
                AgentTaskEntityMessage::Settle { reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentTaskEntityReply::rejected(&error),
                        Ok(entity) => match entity.settle_side_effects(&router, now).await {
                            Ok(progress) => AgentTaskEntityReply::Progressed { progress },
                            Err(error) => AgentTaskEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// The entity type key of the typed-task entity.
pub type AgentTaskEntityTypeKey = EntityTypeKey<AgentTaskEntityMessage>;

/// The registration returned after initializing sharded task entities.
pub type AgentTaskEntityRegistration = EntityTypeRegistration<AgentTaskEntityMessage>;

/// A sharded reference to one typed-task entity.
pub type AgentTaskEntityRef = ShardedEntityRef<AgentTaskEntityMessage>;

/// The sharding settings of typed-task entities.
#[derive(Clone)]
pub struct AgentTaskEntityShardingSettings {
    key: AgentTaskEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    schema_policy: AgentSchemaPolicy,
    clock: AgentTaskClock,
    rewake_parker: Option<Arc<dyn AgentWakeRewakeParker>>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl Debug for AgentTaskEntityShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTaskEntityShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("schema_policy", &self.schema_policy)
            .finish_non_exhaustive()
    }
}

impl AgentTaskEntityShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentTaskEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_TASK_PASSIVATION_BUFFER_DURATION,
            schema_policy: AgentSchemaPolicy::default(),
            clock: system_task_clock(),
            rewake_parker: None,
            metrics: Arc::new(NoopMetricsRecorder),
        }
    }

    /// Wires the durable wake-timer parker every hosted entity's settle
    /// passes park controller-originated re-wakes through.
    #[must_use]
    pub fn with_wake_timers(mut self, parker: Arc<dyn AgentWakeRewakeParker>) -> Self {
        self.rewake_parker = Some(parker);
        self
    }

    /// Wires a metrics recorder for every hosted entity's bounded wake,
    /// epoch, and lifecycle counters.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Uses an explicit clock for the timestamps hosted entities persist.
    #[must_use]
    pub fn with_clock(mut self, clock: AgentTaskClock) -> Self {
        self.clock = clock;
        self
    }

    /// The entity type key used for task entities.
    #[must_use]
    pub const fn key(&self) -> &AgentTaskEntityTypeKey {
        &self.key
    }

    /// Sets the options used when each task entity actor is spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for quiescent task entities.
    #[must_use]
    pub const fn with_idle_passivation(mut self, timeout: Duration) -> Self {
        self.idle_passivation_timeout = Some(timeout);
        self
    }

    /// Disables idle passivation.
    #[must_use]
    pub const fn without_idle_passivation(mut self) -> Self {
        self.idle_passivation_timeout = None;
        self
    }

    /// Configures bounded buffering during shard handoff and passivation.
    #[must_use]
    pub fn with_buffering(mut self, config: ShardBufferConfig) -> Self {
        self.buffer_config = Some(config);
        self
    }

    /// Disables shard-level buffering.
    #[must_use]
    pub const fn without_buffering(mut self) -> Self {
        self.buffer_config = None;
        self
    }

    /// Sets how long explicit passivation buffers incoming messages.
    #[must_use]
    pub const fn with_passivation_buffer_duration(mut self, duration: Duration) -> Self {
        self.passivation_buffer_duration = duration;
        self
    }

    /// Uses an explicit schema-compatibility policy for hosted entities.
    #[must_use]
    pub const fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }
}

impl Default for AgentTaskEntityShardingSettings {
    fn default() -> Self {
        Self::new(agent_task_entity_type_key())
    }
}

/// Creates the default sharded entity type key for task entities.
#[must_use]
pub fn agent_task_entity_type_key() -> AgentTaskEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_TASK_ENTITY_TYPE)
}

/// Maps a task scope to its sharded entity id.
#[must_use]
pub fn agent_task_entity_id(scope: &AgentTaskScope) -> EntityId {
    scope.entity_id()
}

/// The durable persistence id of one task entity's state.
#[must_use]
pub fn agent_task_entity_persistence_id(scope: &AgentTaskScope) -> PersistenceId {
    scope.persistence_id()
}

/// Initializes node-local sharded typed-task entities.
pub fn init_agent_task_entity_sharding<Store, Agents, History>(
    sharding: &ClusterSharding,
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentTaskEntityShardingSettings,
) -> ClusterShardingResult<AgentTaskEntityRegistration>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    sharding.init(agent_task_entity(store, agents, history, router, &settings))
}

/// Initializes sharded typed-task entities that a non-owning node can reach over
/// `rakka-remote`.
///
/// The remote ask surface is the [`AgentExchangeEnvelope`], because that is what
/// every cross-entity command is: a run's result proposal and a delegating run's
/// task creation both arrive as exchanges, and
/// [`crate::choreography::ShardedExchangeRoute`] delivers them to the owning node
/// unchanged. The application registers the exchange codecs with the node
/// runtime's serialization registry through
/// [`crate::choreography::register_agent_exchange_codecs`].
pub fn init_agent_task_entity_remote_sharding<Store, Agents, History>(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentTaskEntityShardingSettings,
) -> ClusterNodeRuntimeResult<AgentTaskEntityRegistration>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    let entity = agent_task_entity(store, agents, history, router, &settings);
    sharding.init_remote_with_ask(
        runtime,
        entity,
        |envelope: AgentExchangeEnvelope, reply_to: ReplyTo<AgentExchangeReply>| {
            AgentTaskEntityMessage::Exchange {
                envelope: Box::new(envelope),
                reply_to,
            }
        },
    )
}

// The task entity is generic over its three stores — its own state, the agent
// state it reads admission facts from, and its history sink — so the entity type
// it builds is unavoidably wide.
#[allow(clippy::type_complexity)]
fn agent_task_entity<Store, Agents, History>(
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    settings: &AgentTaskEntityShardingSettings,
) -> Entity<
    AgentTaskEntityMessage,
    AgentTaskEntity<Store, Agents, History>,
    impl Fn(EntityContext<AgentTaskEntityMessage>) -> AgentTaskEntity<Store, Agents, History>
        + Send
        + Sync
        + 'static,
>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    let schema_policy = settings.schema_policy;
    let clock = settings.clock.clone();
    let rewake_parker = settings.rewake_parker.clone();
    let metrics = settings.metrics.clone();
    let mut entity = Entity::of(settings.key.clone(), move |context: EntityContext<_>| {
        let mut entity = AgentTaskEntity::new(
            context.entity_id(),
            store.clone(),
            agents.clone(),
            history.clone(),
            router.clone(),
            clock.clone(),
            schema_policy,
        )
        .with_metrics(metrics.clone());
        if let Some(parker) = rewake_parker.clone() {
            entity = entity.with_wake_timers(parker);
        }
        entity
    })
    .with_actor_options(settings.actor_options.clone())
    .with_passivation_buffer_duration(settings.passivation_buffer_duration);

    if let Some(timeout) = settings.idle_passivation_timeout {
        entity = entity.with_idle_passivation(timeout);
    }
    if let Some(buffer_config) = settings.buffer_config.clone() {
        entity = entity.with_buffering(buffer_config);
    } else {
        entity = entity.without_buffering();
    }
    entity
}

/// Returns a sharded reference to one typed-task entity.
pub fn agent_task_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentTaskEntityTypeKey,
    scope: &AgentTaskScope,
) -> ClusterShardingResult<AgentTaskEntityRef> {
    sharding.entity_ref_for(key, scope.key())
}

/// Returns a sharded reference to one typed-task entity from a registration.
#[must_use]
pub fn registered_agent_task_entity_ref(
    registration: &AgentTaskEntityRegistration,
    scope: &AgentTaskScope,
) -> AgentTaskEntityRef {
    registration.entity_ref_for(scope.key())
}

/// Explicitly passivates one local typed-task entity.
pub fn passivate_agent_task_entity(
    sharding: &ClusterSharding,
    key: &AgentTaskEntityTypeKey,
    scope: &AgentTaskScope,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &scope.entity_id())
}

/// The rejection of a typed-task operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentTaskError {
    /// An identifier or scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The choreography substrate rejected an exchange.
    Choreography(Box<AgentChoreographyError>),
    /// The task's escrow ledger rejected an allocation, settlement, or return.
    Escrow(AgentEscrowError),
    /// The durable store rejected a load or write.
    Persistence(DurableError),
    /// A task definition could not be bounded.
    InvalidDefinition {
        /// What was out of bounds.
        detail: String,
    },
    /// The task does not exist.
    NotCreated {
        /// The scope of the task.
        scope: AgentTaskScope,
    },
    /// The task already exists.
    AlreadyCreated {
        /// The scope of the task.
        scope: AgentTaskScope,
    },
    /// The task is terminal and accepts no further transition.
    Terminal {
        /// Its terminal status.
        status: AgentTaskStatus,
    },
    /// An agent-owned task was created without an assignee.
    MissingAssignee,
    /// A handoff command was refused without recording a transfer
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Non-committing: the transition validates before it mutates, so this
    /// refusal proves the task is exactly as the command found it — which is
    /// what lets the source run settle its cell failed and resume.
    HandoffRefused {
        /// The stable machine-readable refusal code.
        code: &'static str,
        /// Bounded human-readable detail.
        message: String,
    },
    /// A team board claim was refused by the task's arbitration
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    TeamClaimRefused {
        /// The stable machine-readable refusal code.
        code: &'static str,
        /// Bounded human-readable detail.
        message: String,
    },
    /// A human-result submission was refused without a validation decision
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Non-committing: the transition validates before it mutates, so this
    /// refusal proves the task is exactly as the command found it, no
    /// rejection budget was spent, and a corrected retry under the same
    /// operation id is still accepted.
    SubmissionRefused {
        /// The stable machine-readable refusal code.
        code: &'static str,
        /// Bounded human-readable detail.
        message: String,
    },
    /// A creation carried delegation provenance that violates its structural
    /// bounds.
    DelegationProvenanceInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// A continuous-mode task was created without a goal binding.
    ContinuousWithoutGoal,
    /// An epoch task was created without the parent controller that admitted
    /// its wake.
    EpochWithoutParent,
    /// An admission on a continuous goal that never declared its epoch
    /// contract.
    EpochUndefined,
    /// The goal contract itself refused the transition.
    Goal(AgentGoalError),
    /// A goal command was delivered to a task that does not coordinate a goal.
    GoalNotCoordinated,
    /// A goal resume that widened nothing in the exhausted dimension.
    GoalResumeUnrelieved {
        /// The exhaustion still in force.
        exhaustion: AgentBudgetExhaustion,
    },
    /// A resume arrived at the door that does not own the wait on record: the
    /// gate's resume for a budget or stagnation park, or the goal's resume
    /// for an admission suspension.
    GoalWaitOwnedElsewhere {
        /// The wait reason's code.
        code: &'static str,
    },
    /// A criteria decision was commanded while the spec configures an
    /// evaluator: only the goal-evaluation exchange — whose sender fence and
    /// digest-bearing record are the attestation — may make one
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    GoalDecisionUnattested,
    /// The wake contract itself refused the command.
    Wake(AgentWakeError),
    /// The durable wake-timer store refused a re-wake parking.
    WakeTimer(AgentWakeTimerError),
    /// A wake command was delivered to a task that does not coordinate a
    /// continuous goal.
    WakeNotContinuous,
    /// A wake was delivered to a task that does not bind its goal.
    WakeGoalMismatch {
        /// The goal the wake was constructed for.
        offered: AgentGoalId,
    },
    /// A wake command's operation id disagrees with the one its own binding
    /// derives.
    WakeOperationMismatch,
    /// A schedule update whose revision does not move strictly forward.
    ScheduleNotMonotonic {
        /// The revision the update carried.
        offered: ScheduleRevision,
        /// The revision currently in force.
        current: ScheduleRevision,
    },
    /// A wake-policy update whose revision does not move strictly forward.
    WakePolicyNotNewer {
        /// The revision the update carried.
        offered: AgentRevisionNumber,
        /// The revision currently in force.
        current: AgentRevisionNumber,
    },
    /// An agent-owned task's id is too long to derive assignment run ids from.
    TaskIdTooLong {
        /// The task id's length, in bytes.
        length: usize,
        /// The longest id an agent-owned task may use, in bytes.
        maximum: usize,
    },
    /// The task's admission facts were not resolved before a decision.
    NotReady,
    /// A dependency would make the graph cyclic.
    DependencyCycle {
        /// The dependency that would close the cycle.
        dependency: AgentTaskId,
    },
    /// A dependency declaration exceeded the bounded ancestor depth.
    DependencyDepthExceeded {
        /// The depth the declaration carried.
        depth: usize,
        /// The maximum accepted depth.
        maximum: usize,
    },
    /// The task already declares as many dependencies as it may.
    DependencyLimitExceeded {
        /// The maximum number of dependencies.
        maximum: usize,
    },
    /// A dependency was redeclared or resolved with a conflicting value.
    DependencyConflict {
        /// The dependency in question.
        dependency: AgentTaskId,
    },
    /// An outcome was recorded for a dependency the task does not declare.
    UnknownDependency {
        /// The dependency in question.
        dependency: AgentTaskId,
    },
    /// Inline content exceeded the inline bound and belongs behind an artifact
    /// reference.
    ContentTooLarge {
        /// The size of the rejected content, in bytes.
        bytes: usize,
        /// The maximum accepted size, in bytes.
        maximum: usize,
    },
    /// The materialized task record would exceed its bound.
    MaterializedStateTooLarge {
        /// The size of the rejected record, in bytes.
        bytes: usize,
        /// The maximum accepted size, in bytes.
        maximum: usize,
    },
    /// A history sequence already holds a different entry.
    HistoryConflict {
        /// The contested sequence.
        sequence: AgentTaskHistorySequence,
    },
    /// The task owes its history sink more entries than it may hold, so it
    /// cannot transition again until the sink accepts them.
    HistoryBacklog {
        /// How many entries the task owes.
        pending: usize,
        /// The maximum it may owe.
        maximum: usize,
    },
    /// The agent's durable admission state could not be read.
    AgentRead {
        /// The agent whose state could not be read.
        agent: AgentId,
        /// The failure detail.
        message: String,
    },
    /// A result was decoded under a different task definition than it was
    /// accepted under.
    DefinitionMismatch {
        /// The definition and revision that were expected.
        expected: String,
        /// The definition and revision the record carried.
        actual: String,
    },
    /// A result was decoded under a different schema than it was accepted under.
    SchemaMismatch {
        /// The schema that was expected.
        expected: String,
        /// The schema the record carried.
        actual: String,
    },
    /// The accepted result is held behind an artifact reference and cannot be
    /// decoded without loading it.
    ResultBehindArtifact,
    /// A value could not be encoded.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
    /// A value could not be decoded.
    Decoding {
        /// The decoding failure detail.
        message: String,
    },
}

impl AgentTaskError {
    /// The stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Choreography(error) => error.code(),
            Self::Escrow(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::InvalidDefinition { .. } => "invalid-task-definition",
            Self::NotCreated { .. } => "task-not-created",
            Self::AlreadyCreated { .. } => "task-already-created",
            Self::Terminal { .. } => "task-terminal",
            Self::MissingAssignee => "task-missing-assignee",
            Self::HandoffRefused { code, .. } => code,
            Self::SubmissionRefused { code, .. } => code,
            Self::TeamClaimRefused { code, .. } => code,
            Self::DelegationProvenanceInvalid { .. } => "task-delegation-provenance-invalid",
            Self::ContinuousWithoutGoal => "task-continuous-without-goal",
            Self::EpochWithoutParent => "task-epoch-without-parent",
            Self::EpochUndefined => "task-epoch-undefined",
            Self::Goal(error) => error.code(),
            Self::GoalNotCoordinated => "task-goal-not-coordinated",
            Self::GoalResumeUnrelieved { .. } => "task-goal-resume-unrelieved",
            Self::GoalWaitOwnedElsewhere { .. } => "task-goal-wait-owned-elsewhere",
            Self::GoalDecisionUnattested => "task-goal-decision-unattested",
            Self::Wake(error) => error.code(),
            Self::WakeTimer(error) => error.code(),
            Self::WakeNotContinuous => "task-wake-not-continuous",
            Self::WakeGoalMismatch { .. } => "task-wake-goal-mismatch",
            Self::WakeOperationMismatch => "task-wake-operation-mismatch",
            Self::ScheduleNotMonotonic { .. } => "task-schedule-not-monotonic",
            Self::WakePolicyNotNewer { .. } => "task-wake-policy-not-newer",
            Self::TaskIdTooLong { .. } => "task-id-too-long",
            Self::NotReady => "task-admission-not-resolved",
            Self::DependencyCycle { .. } => "task-dependency-cycle",
            Self::DependencyDepthExceeded { .. } => "task-dependency-depth-exceeded",
            Self::DependencyLimitExceeded { .. } => "task-dependency-limit-exceeded",
            Self::DependencyConflict { .. } => "task-dependency-conflict",
            Self::UnknownDependency { .. } => "task-unknown-dependency",
            Self::ContentTooLarge { .. } => "task-content-too-large",
            Self::MaterializedStateTooLarge { .. } => "task-state-too-large",
            Self::HistoryConflict { .. } => "task-history-conflict",
            Self::HistoryBacklog { .. } => "task-history-backlog",
            Self::AgentRead { .. } => "task-agent-read-failed",
            Self::DefinitionMismatch { .. } => "task-definition-mismatch",
            Self::SchemaMismatch { .. } => "task-schema-mismatch",
            Self::ResultBehindArtifact => "task-result-behind-artifact",
            Self::Encoding { .. } => "task-encoding-failed",
            Self::Decoding { .. } => "task-decoding-failed",
        }
    }
}

impl Display for AgentTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Choreography(error) => Display::fmt(error, f),
            Self::Escrow(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::InvalidDefinition { detail } => {
                write!(f, "the task definition is not bounded: {detail}")
            }
            Self::NotCreated { scope } => write!(f, "task {scope} is not created"),
            Self::AlreadyCreated { scope } => write!(f, "task {scope} is already created"),
            Self::Terminal { status } => write!(
                f,
                "the task is {status} and accepts no further transition"
            ),
            Self::MissingAssignee => {
                write!(f, "an agent-owned task must name the agent it is created for")
            }
            Self::HandoffRefused { code, message } => {
                write!(f, "the handoff was refused ({code}): {message}")
            }
            Self::SubmissionRefused { code, message } => {
                write!(f, "the submission was refused ({code}): {message}")
            }
            Self::TeamClaimRefused { code, message } => {
                write!(f, "the team claim was refused ({code}): {message}")
            }
            Self::DelegationProvenanceInvalid { message } => {
                write!(f, "the creation's delegation provenance is invalid: {message}")
            }
            Self::ContinuousWithoutGoal => {
                write!(f, "a continuous-mode task must bind the goal its controller drives")
            }
            Self::EpochWithoutParent => write!(
                f,
                "an epoch task must bind the parent controller that admitted its wake"
            ),
            Self::EpochUndefined => write!(
                f,
                "the continuous goal declares no epoch contract, so no epoch can be admitted"
            ),
            Self::Goal(error) => Display::fmt(error, f),
            Self::GoalNotCoordinated => write!(
                f,
                "a goal command addresses a task that does not coordinate a goal"
            ),
            Self::GoalResumeUnrelieved { exhaustion } => write!(
                f,
                "the resume leaves the goal exhausted: {exhaustion}"
            ),
            Self::GoalWaitOwnedElsewhere { code } => write!(
                f,
                "the wait on record ({code}) is owned by the other resume door"
            ),
            Self::GoalDecisionUnattested => write!(
                f,
                "the spec configures an evaluator, so a criteria decision may only arrive \
                 through the goal-evaluation exchange"
            ),
            Self::Wake(error) => Display::fmt(error, f),
            Self::WakeTimer(error) => Display::fmt(error, f),
            Self::WakeNotContinuous => write!(
                f,
                "a wake command addresses a task that does not coordinate a continuous goal"
            ),
            Self::WakeGoalMismatch { offered } => write!(
                f,
                "the wake was constructed for goal {offered}, which this task does not bind"
            ),
            Self::WakeOperationMismatch => write!(
                f,
                "the command's operation id is not the one its own wake binding derives"
            ),
            Self::ScheduleNotMonotonic { offered, current } => write!(
                f,
                "a schedule update must move strictly forward: revision {offered} does not follow {current}"
            ),
            Self::WakePolicyNotNewer { offered, current } => write!(
                f,
                "a wake-policy update must move strictly forward: revision {offered} does not follow {current}"
            ),
            Self::TaskIdTooLong { length, maximum } => write!(
                f,
                "the task id is {length} bytes, and an agent-owned task id may use at most {maximum} so the run ids derived from it stay valid"
            ),
            Self::NotReady => write!(
                f,
                "the agent's admission state was not resolved before the assignment decision"
            ),
            Self::DependencyCycle { dependency } => write!(
                f,
                "a dependency on {dependency} would make the task graph cyclic"
            ),
            Self::DependencyDepthExceeded { depth, maximum } => write!(
                f,
                "a dependency declared {depth} ancestors, which exceeds the maximum depth {maximum}"
            ),
            Self::DependencyLimitExceeded { maximum } => write!(
                f,
                "a task may declare at most {maximum} dependencies"
            ),
            Self::DependencyConflict { dependency } => write!(
                f,
                "dependency {dependency} was redeclared or resolved with a conflicting value"
            ),
            Self::UnknownDependency { dependency } => write!(
                f,
                "the task does not declare a dependency on {dependency}"
            ),
            Self::ContentTooLarge { bytes, maximum } => write!(
                f,
                "inline content is {bytes} bytes, which exceeds the {maximum} byte limit; it belongs behind an artifact reference"
            ),
            Self::MaterializedStateTooLarge { bytes, maximum } => write!(
                f,
                "the materialized task record is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::HistoryConflict { sequence } => write!(
                f,
                "history sequence {sequence} already holds a different entry"
            ),
            Self::HistoryBacklog { pending, maximum } => write!(
                f,
                "the task owes its history sink {pending} entries, the most it may hold is {maximum}, and it may not transition again until the sink accepts them"
            ),
            Self::AgentRead { agent, message } => write!(
                f,
                "the durable admission state of agent {agent} could not be read: {message}"
            ),
            Self::DefinitionMismatch { expected, actual } => write!(
                f,
                "the result was accepted under task definition {actual} but was decoded as {expected}"
            ),
            Self::SchemaMismatch { expected, actual } => write!(
                f,
                "the result was accepted under schema {actual} but was decoded as {expected}"
            ),
            Self::ResultBehindArtifact => write!(
                f,
                "the accepted result is held behind an artifact reference and must be loaded through a bounded adapter"
            ),
            Self::Encoding { message } => write!(f, "a task value could not be encoded: {message}"),
            Self::Decoding { message } => write!(f, "a task value could not be decoded: {message}"),
        }
    }
}

impl Error for AgentTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Choreography(error) => Some(error),
            Self::Escrow(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::Wake(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentWakeError> for AgentTaskError {
    fn from(error: AgentWakeError) -> Self {
        Self::Wake(error)
    }
}

impl From<AgentGoalError> for AgentTaskError {
    fn from(error: AgentGoalError) -> Self {
        Self::Goal(error)
    }
}

impl From<AgentIdentityError> for AgentTaskError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentTaskError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentChoreographyError> for AgentTaskError {
    fn from(error: AgentChoreographyError) -> Self {
        Self::Choreography(Box::new(error))
    }
}

impl From<AgentEscrowError> for AgentTaskError {
    fn from(error: AgentEscrowError) -> Self {
        Self::Escrow(error)
    }
}

impl From<DurableError> for AgentTaskError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

impl From<AgentTaskError> for AgentChoreographyError {
    fn from(error: AgentTaskError) -> Self {
        match error {
            AgentTaskError::Identity(error) => Self::Identity(error),
            AgentTaskError::Schema(error) => Self::Schema(error),
            AgentTaskError::Choreography(error) => *error,
            AgentTaskError::Persistence(error) => Self::Persistence(error),
            other => Self::PayloadEncoding {
                message: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod team_claim_bounds_tests {
    use super::*;
    use crate::coordination::{
        team_claim_id_for, team_claim_operation_id, AgentTeamClaimAction, AgentTeamClaimCommand,
        AGENT_TEAM_CLAIM_PAYLOAD_TYPE,
    };
    use crate::identity::{AgentTeamId, AgentTeamScope};

    #[test]
    fn a_claim_refused_for_bounds_persists_no_partial_mutation() {
        let tenant = TenantId::new("acme");
        let task_id = AgentTaskId::new("board-task").expect("the task id is valid");
        let scope =
            AgentTaskScope::new(tenant.clone(), task_id.clone()).expect("the scope is valid");
        let team_scope = AgentTeamScope::new(
            tenant.clone(),
            AgentTeamId::new("support-team").expect("the team id is valid"),
        )
        .expect("the team scope is valid");
        let member = AgentId::new("worker-a").expect("the member id is valid");

        let mut state = AgentTaskState::uncreated(scope.clone(), AgentTimestampMillis::new(1));
        let create_op = AgentOperationId::new(
            AgentOperationKind::TaskCreation,
            ["acme", "board-task", "1"],
        )
        .expect("the operation id derives");
        let definition = AgentTaskDefinition::new(
            AgentTaskDefinitionId::new("triage").expect("the definition id is valid"),
            "Triage one ticket.",
            AgentSchemaRef::new(
                AgentSchemaId::new("in").expect("the schema id is valid"),
                AgentRevisionNumber::INITIAL,
            ),
            AgentSchemaRef::new(
                AgentSchemaId::new("out").expect("the schema id is valid"),
                AgentRevisionNumber::INITIAL,
            ),
        )
        .expect("the definition is valid");
        let creation = AgentTaskCreation {
            definition,
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: None,
            team: Some(team_scope.team().clone()),
            goal: None,
            goal_mode: Default::default(),
            goal_spec: None,
            parent: None,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        };
        create_task(
            &mut state,
            &create_op,
            creation,
            AgentTimestampMillis::new(1),
        )
        .expect("the board task creates");

        // Inflate the record to one byte under its cap, bypassing the
        // admission reserve the way a lifetime of zero-reserve growth would:
        // the recorded claim itself must push it over.
        {
            let task = state.task.as_mut().expect("the task exists");
            let size = task.materialized_size_bytes();
            let previous = task.definition.description.len();
            task.definition.description =
                "x".repeat(AGENT_TASK_MATERIALIZED_MAX_BYTES - 1 - size + previous);
        }

        let claim =
            team_claim_id_for(&team_scope, &task_id, &member, 1).expect("the claim id derives");
        let command = AgentTeamClaimCommand {
            team: team_scope.clone(),
            claim: claim.clone(),
            task: task_id,
            epoch: 1,
            action: AgentTeamClaimAction::Claim {
                member: member.clone(),
            },
            policy_revision: AgentRevisionNumber::INITIAL,
            lease_expires_at: AgentTimestampMillis::new(300_000),
        };
        let operation = team_claim_operation_id(&tenant, &claim).expect("the operation derives");
        let envelope = AgentExchangeEnvelope::new(
            operation.clone(),
            AgentExchangeKind::TeamClaim,
            AgentEntityAddress::Team(team_scope),
            AgentEntityAddress::Task(scope),
            AgentExchangePayload::encode(AGENT_TEAM_CLAIM_PAYLOAD_TYPE, &command)
                .expect("the payload encodes"),
            AgentCorrelationId::new(operation.as_str()),
            AgentTimestampMillis::new(2),
        )
        .expect("the envelope builds");

        let before = state.clone();
        let result = apply_team_claim(&mut state, &envelope, AgentTimestampMillis::new(2));
        assert_eq!(
            result.status().rejection_code(),
            Some("task-state-too-large"),
            "the oversized record refuses the claim"
        );
        assert_eq!(
            state, before,
            "a bounds refusal persists no partial mutation: the board treats \
             a definitive refusal as proof no claim was recorded"
        );
    }
}

#[cfg(test)]
mod epoch_result_tests {
    use rakka_agent_workflow::{AgentAuditEventId, PrincipalRef};

    use super::*;
    use crate::definition::AgentPolicyRef;
    use crate::definition::AgentRevisionProvenance;
    use crate::goal::AgentEpochSpec;
    use crate::wake::{AgentWakeOccurrence, AgentWakePolicy, AgentWakeTriggerKind};

    fn ts(at: u64) -> AgentTimestampMillis {
        AgentTimestampMillis::new(at)
    }

    fn schema(id: &str) -> AgentSchemaRef {
        AgentSchemaRef::new(
            AgentSchemaId::new(id).expect("the schema id is valid"),
            AgentRevisionNumber::INITIAL,
        )
    }

    fn definition() -> AgentTaskDefinition {
        AgentTaskDefinition::new(
            AgentTaskDefinitionId::new("reconcile").expect("the definition id is valid"),
            "Reconcile one nightly window.",
            schema("epoch-input"),
            schema("epoch-result"),
        )
        .expect("the definition is valid")
    }

    fn continuous_mode() -> AgentGoalMode {
        let mut budget = AgentBudgetAllocation::unbounded();
        budget.set(crate::budget::AgentBudgetDimension::ModelCalls, Some(8));
        let policy =
            AgentWakePolicy::new([AgentWakeTriggerKind::DurableTimer], budget, Some(60_000))
                .expect("the policy is valid");
        let provenance = AgentRevisionProvenance {
            principal: PrincipalRef {
                principal_type: "service".to_string(),
                principal_id: "test".to_string(),
                display_name: None,
            },
            accepted_at: ts(1),
            causation_id: AgentCausationId::new("cause-1"),
            audit_ref: AgentAuditEventId::new("audit-1"),
        };
        AgentGoalMode::Continuous(Box::new(crate::goal::AgentContinuousGoalSpec {
            schedule_revision: ScheduleRevision::INITIAL,
            wake_policy: AgentWakePolicyRevision::initial(policy, provenance)
                .expect("the revision is valid"),
            health_condition: AgentPolicyRef::new("health").expect("the policy ref is valid"),
            epoch: Some(Box::new(AgentEpochSpec {
                definition: definition(),
                assignee: AgentId::new("worker").expect("the agent id is valid"),
                observation_scope: None,
            })),
        }))
    }

    fn binding(due_at: u64) -> AgentWakeBinding {
        AgentWakeBinding::new(
            TenantId::new("acme"),
            AgentGoalId::new("nightly").expect("the goal id is valid"),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Scheduled { due_at: ts(due_at) },
            AgentWakeTriggerKind::DurableTimer,
            ts(due_at),
            AgentRevisionNumber::INITIAL,
        )
        .expect("the binding is valid")
    }

    #[test]
    fn a_refused_epoch_result_persists_no_partial_mutation() {
        let tenant = TenantId::new("acme");
        let scope = AgentTaskScope::new(
            tenant.clone(),
            AgentTaskId::new("root").expect("the task id is valid"),
        )
        .expect("the scope is valid");
        let mut state = AgentTaskState::uncreated(scope.clone(), ts(1));
        let create_op =
            AgentOperationId::new(AgentOperationKind::TaskCreation, ["acme", "root", "1"])
                .expect("the operation id derives");
        let creation = AgentTaskCreation {
            definition: definition().with_ownership(AgentTaskOwnership::Human),
            input: AgentTaskContent::inline(serde_json::json!({ "goal": 1 }))
                .expect("the input is inline-bounded"),
            assignee: None,
            team: None,
            goal: Some(AgentGoalId::new("nightly").expect("the goal id is valid")),
            goal_mode: continuous_mode(),
            goal_spec: None,
            parent: None,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        };
        create_task(&mut state, &create_op, creation, ts(1)).expect("the control task creates");

        // One occurrence admits and one parks behind it, so the epoch result
        // below has both an escrow settlement and a promotion to run.
        let first = binding(1_000);
        let admit_op = first
            .admission_operation_id()
            .expect("the admission id derives");
        admit_wake(&mut state, &admit_op, first.clone(), ts(1_010))
            .expect("the first occurrence admits");
        let second = binding(120_000);
        let admit_op = second
            .admission_operation_id()
            .expect("the admission id derives");
        admit_wake(&mut state, &admit_op, second, ts(120_010))
            .expect("the second occurrence parks");

        // Make the promotion fail *after* the settlement and the release have
        // already mutated: strip the epoch contract, so owing the promoted
        // occurrence's creation refuses `task-epoch-undefined` mid-sequence.
        {
            let task = state.task.as_mut().expect("the task exists");
            let AgentGoalMode::Continuous(spec) = &mut task.goal_mode else {
                panic!("the goal is continuous");
            };
            spec.epoch = None;
        }

        let before = state.clone();
        let wake = first.wake_id().clone();
        let epoch_task = epoch_task_id_for_wake(&wake).expect("the epoch task derives");
        let result = AgentEpochResult {
            wake: wake.clone(),
            task: epoch_task.clone(),
            status: AgentTaskStatus::Completed,
            consumed: AgentBudgetConsumption::zero(),
            result_digest: None,
        };
        let operation_id = epoch_result_operation_id(
            &tenant,
            &AgentGoalId::new("nightly").expect("the goal id is valid"),
            &wake,
        )
        .expect("the result operation id derives");
        let payload = AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the result encodes");
        let envelope = AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::EpochResult,
            AgentEntityAddress::Task(
                AgentTaskScope::new(tenant, epoch_task).expect("the epoch scope is valid"),
            ),
            AgentEntityAddress::Task(scope),
            payload,
            AgentCorrelationId::new(operation_id.as_str()),
            ts(200_000),
        )
        .expect("the envelope is valid");

        let transition = apply_epoch_result(&mut state, &envelope, ts(200_000));
        assert_eq!(
            transition.result().status().rejection_code(),
            Some("task-epoch-undefined"),
            "the mid-sequence failure refuses the exchange"
        );
        assert!(transition.owed().is_empty());
        assert_eq!(state, before, "a refusal persists no partial mutation");
    }
}

#[cfg(test)]
mod digest_tests {
    use super::{AgentContentDigest, AgentDigestAlgorithm};
    use serde_json::json;

    #[test]
    fn sha256_matches_the_standard_vectors() {
        // FIPS 180-4 / RFC 6234 known-answer vectors.
        assert_eq!(
            AgentContentDigest::sha256_of_bytes(b"").value,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            AgentContentDigest::sha256_of_bytes(b"abc").value,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            AgentContentDigest::sha256_of_bytes(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )
            .value,
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn the_cryptographic_digest_is_canonical_and_labelled() {
        let a = AgentContentDigest::sha256_of_json(&json!({"a": 1, "b": 2}));
        let b = AgentContentDigest::sha256_of_json(&json!({"b": 2, "a": 1}));
        assert_eq!(a, b, "key order must not change the digest");
        assert_eq!(a.algorithm, AgentDigestAlgorithm::Sha256);
        assert!(a.algorithm.is_cryptographic());
        assert!(!AgentDigestAlgorithm::Fnv1a128.is_cryptographic());
        assert_eq!(a.to_string(), format!("sha2-256:{}", a.value));
    }

    #[test]
    fn a_changed_argument_changes_the_cryptographic_digest() {
        let before = AgentContentDigest::sha256_of_json(&json!({"amount": 100}));
        let after = AgentContentDigest::sha256_of_json(&json!({"amount": 101}));
        assert_ne!(before, after);
    }
}
