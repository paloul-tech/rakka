//! The sharded run entity and its durable status.
//!
//! Owns [`AgentRunEntity`], keyed by `(TenantId, AgentId, AgentRunId)`, with a
//! serializable command protocol; the run status enum including the
//! `WaitingForHuman` compatibility note; and the run's participation in the
//! result proposal and decision exchange with its task. A run is bound to one
//! task for its lifetime and never makes the public task terminal by itself.
//!
//! Passivation is the default: after any persisted wait the entity is idle and
//! holds no per-run live resources.
//!
//! Specification: sections 6.5 and 9.3. Filled by slice 1.5. The loop state it
//! carries across those waits belongs to [`crate::loop_runtime`].
//!
//! # The entity is a choreography participant
//!
//! The run's durable state carries the [`AgentExchangeJournal`], so the two
//! exchanges of its life commit inside the transitions that cause them:
//!
//! ```text
//! AgentTaskEntity: assignment decision
//!     -> assignment exchange ──▶ AgentRunEntity
//!                                    │  create the run, bind it to its task    [1 CAS]
//!                                    │  reply: AgentRunAcceptance
//!     ◀──────────────────────────────┘
//! task InProgress
//!                                the loop runs — one CAS per transition,
//!                                passivated across every wait
//!                                    │
//!                                    │  persist the result proposal + the
//!                                    ▼  exchange that carries it              [1 CAS]
//!     ◀── result-proposal exchange ──┘
//! deterministic validation, durable decision                                  [1 CAS]
//!     └── decision reply ────────▶ the run records its consequence            [1 CAS]
//! ```
//!
//! The task's persisted decision is the source of truth for the *validation
//! outcome*; the run's persisted state is the source of truth for the run's
//! *consequence* of it ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
//! Losing either side mid-exchange converges on recovery: the run re-drives the
//! proposal under the same id, and the task returns its original decision rather
//! than validating a second time.
//!
//! # Passivation is the default, not an optimization
//!
//! The run entity has no timer, no open request, no background task, and no
//! resident loop. Every wait it takes is a durable phase in
//! [`crate::loop_runtime::AgentLoopState`], and what wakes it is a durable
//! command — an effect result the dispatcher returned through the inbox, or an
//! exchange reply — delivered to whichever node owns its shard at that moment
//! ([specification 15](../../../docs/plans/rakka-agent/spec.md)). A run waiting
//! a week for an approval costs exactly what a run waiting a millisecond for a
//! model costs: one durable record.
//!
//! This is what [`AgentRunEntityStore::settle_side_effects`] exists for. It
//! cranks the loop from durable state alone — advance, dispatch, drive — so
//! calling it after a transition, after recovery, or from a sweep are the same
//! operation.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentEffectId, AgentTimestampMillis, HumanCheckpointId,
    PrincipalRef, StateSchemaVersion,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ReplyTo,
};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeResult, ClusterSharding, ClusterShardingResult, Entity,
    EntityContext, EntityId, EntityTypeKey, EntityTypeRegistration, ShardBufferConfig,
    ShardedEntityRef,
};
use serde::{Deserialize, Serialize};

use crate::budget::{
    AgentBudgetAllocation, AgentBudgetExhaustion, AgentRunBudget,
    AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN,
};
use crate::checkpoints::{
    AgentCheckpoint, AgentCheckpointDecision, AgentCheckpointError, AgentCheckpointKind,
    AgentCheckpointOutcome, AgentCheckpointTimerOutcome, AgentCompensationRef,
};
use crate::choreography::{
    drive_pending_exchanges, AgentChoreographyError, AgentEntityAddress, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter,
    AgentExchangeState, AgentExchangeTransition,
};
use crate::definition::AgentRevisionNumber;
use crate::effect::{
    AgentEffectError, AgentEffectGeneration, AgentEffectPolicies, AgentEffectResolution,
    AgentEffectSpec, AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunEffectSink, AgentRunEffectStatus, AgentToolResult,
};
use crate::identity::{
    AgentId, AgentIdentityError, AgentOperationId, AgentOperationKind, AgentRunBinding, AgentRunId,
    AgentRunScope, AgentTaskId, AgentTaskScope, TenantId,
};
use crate::loop_runtime::{AgentLoopPhase, AgentLoopState, AgentPendingTopUp, AgentRunProposal};
use crate::memory::AgentContextSnapshotRef;
use crate::model::AgentModelError;
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION,
};
use crate::task::{
    AgentAcceptedResult, AgentAssignmentGeneration, AgentBudgetLedgerOutcome, AgentRunAcceptance,
    AgentRunAssignment, AgentTaskContent, AgentTaskDecision, AgentTaskDefinition, AgentTaskError,
    AgentTaskResultProposal, AgentTaskStatus, AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE,
    AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE, AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE,
    AGENT_TASK_DECISION_PAYLOAD_TYPE, AGENT_TASK_REFUSAL_STALE_GENERATION,
    AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE,
};

/// Default sharded entity type of the run entity.
pub const DEFAULT_AGENT_RUN_ENTITY_TYPE: &str = "RakkaAgentRun";

/// How many resolved operation ids the run entity remembers for deduplication.
///
/// The window is the fast path; every command transition is additionally fenced
/// by the run's own durable state — an effect result for an effect the run does
/// not hold, or holds as resolved, is refused — so a replay older than the
/// window is still refused rather than applied twice.
pub const AGENT_RUN_OPERATION_LOG_CAPACITY: usize = 64;

/// Largest materialized run record, in bytes, once serialized.
///
/// It is [`AGENT_RUN_STATE_GROWTH_RESERVE_BYTES`] of working-set growth on top
/// of the admission budget acceptance enforces, so raising the reserve must
/// raise this bound with it rather than silently shrinking what a run may be
/// admitted with.
pub const AGENT_RUN_MATERIALIZED_MAX_BYTES: usize = 176 * 1024;

/// Bytes of growth headroom an accepted run record keeps below
/// [`AGENT_RUN_MATERIALIZED_MAX_BYTES`].
///
/// After acceptance the record may still grow by the whole working set of one
/// turn, every part of which is individually bounded — so the reserve is the
/// sum of those bounds, not an estimate: the pending turn
/// ([`crate::model::AGENT_MODEL_TURN_MAX_BYTES`]); the turn's tool calls copied
/// into their effects (bounded again by the turn that carried them), plus each
/// effect's envelope of derived identifiers, which scale with the scope's own
/// ids ([`crate::identity::AGENT_IDENTITY_MAX_LENGTH`] per segment); one
/// bounded tool result per effect
/// ([`crate::effect::AGENT_TOOL_RESULT_MAX_BYTES`]); the result proposal; the
/// accepted result ([`crate::task::AGENT_TASK_INLINE_CONTENT_MAX_BYTES`] plus
/// its envelope); and the bounded feedback and terminal details
/// ([`AGENT_RUN_DETAIL_MAX_LENGTH`]).
///
/// Acceptance therefore enforces the materialized bound *minus* this reserve,
/// so a run the entity admits can never later be unable to record the very turn
/// it was created to take — a record refused mid-turn would refuse the same
/// retry forever, exactly the committed-record-bricks-its-own-recovery class
/// the schema gate exists to prevent. It is the same reservation the task
/// entity makes for its own lifecycle
/// ([`crate::task::AGENT_TASK_STATE_GROWTH_RESERVE_BYTES`]). The test
/// `the_growth_reserve_covers_the_maximal_working_set` holds this constant to
/// that claim: it materializes a superset of every reachable working set, under
/// maximal identifiers, and measures it.
pub const AGENT_RUN_STATE_GROWTH_RESERVE_BYTES: usize = 96 * 1024;

/// Most bounded loop transitions one settle pass may perform.
///
/// The loop always reaches a durable wait within a few transitions — every turn
/// needs a model effect, and an effect is a wait — so this is a fence, not a
/// schedule. It exists because a handler must perform *bounded* work
/// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)), and a bound
/// that is only true by construction is not a bound.
pub const AGENT_RUN_MAX_LOOP_STEPS_PER_PASS: usize = 8;

/// Most advance/dispatch/drive rounds one settle pass may perform.
///
/// A round that settles a rejected proposal can put the loop back to work, so a
/// pass makes progress until it cannot — under this fence, for the same reason.
pub const AGENT_RUN_MAX_SETTLE_ROUNDS: usize = 4;

/// Maximum length, in bytes, of any bounded free-text detail a run persists: a
/// cancellation reason, a failure detail, or the sanitized feedback a rejected
/// proposal returned.
pub const AGENT_RUN_DETAIL_MAX_LENGTH: usize = 512;

const DEFAULT_AGENT_RUN_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

/// The source of the durable timestamps a run's transitions are stamped with.
pub type AgentRunClock = Arc<dyn Fn() -> AgentTimestampMillis + Send + Sync>;

/// A clock reading the system's wall clock.
#[must_use]
pub fn system_run_clock() -> AgentRunClock {
    Arc::new(|| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        AgentTimestampMillis::new(millis)
    })
}

/// Result type for run operations.
pub type AgentRunResult<T> = Result<T, AgentRunError>;

/// The lifecycle status of one run
/// ([specification 9.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// [`Self::HandedOff`], [`Self::Superseded`], [`Self::Completed`],
/// [`Self::Failed`], and [`Self::Cancelled`] are terminal *for one run*, not for
/// the task it served: one task may have several sequential runs, and at most one
/// is the current owner.
///
/// None of these is a residency guarantee. [`Self::Accepted`], [`Self::Running`],
/// and [`Self::Cancelling`] describe what the run is *logically* doing, and the
/// entity passivates whenever no bounded transition is immediately executable —
/// which, in a loop whose every turn waits on a model, is nearly always.
///
/// # The `WaitingForHuman` compatibility note
///
/// [Specification 9.3](../../../docs/plans/rakka-agent/spec.md) permits an
/// implementation to keep the durable substrate's single `WaitingForHuman`
/// variant as a compatibility representation of `WaitingForApproval`, provided
/// that "public behavior and persisted migrations MUST be explicit before the
/// status is split".
///
/// This enum makes the split at its first commit and preserves no such alias, so
/// there is no migration to owe: no agent-domain record has ever been written
/// under an unsplit status. What the substrate renders as one `WaitingForHuman`
/// is here three distinct waits, because they resolve through three different
/// durable decisions and an operator answering one is not answering another:
/// [`Self::WaitingForApproval`] (a human approves an effect),
/// [`Self::WaitingForAuthorization`] (a principal authorizes a capability or
/// credential), and [`Self::WaitingForReconciliation`] (an operator establishes
/// the outcome of an ambiguous effect —
/// [specification 12.5](../../../docs/plans/rakka-agent/spec.md), where a
/// generic retry is forbidden precisely because those three are not the same
/// question). [`Self::is_waiting_for_human`] is the explicit public behavior: it
/// is the set the substrate's variant corresponds to, and it is what the A2A
/// projection of slice 1.12 maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunStatus {
    /// The run durably accepted its assignment and has not begun its first turn.
    Accepted,
    /// The run is executing a bounded transition, or is between two of them.
    Running,
    /// The run is waiting for a durable timer.
    WaitingForTimer,
    /// The run is waiting for a durable effect result.
    WaitingForEffect,
    /// The run is waiting for a human to approve an effect.
    WaitingForApproval,
    /// The run is waiting for a principal to authorize a capability or
    /// credential.
    WaitingForAuthorization,
    /// The run is waiting for an operator to establish the outcome of an
    /// ambiguous effect. It is not cancellable to a terminal state until they
    /// have ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
    WaitingForReconciliation,
    /// The run is suspended by policy or by an administrative decision.
    Suspended,
    /// The run is quiescing towards a terminal status it has already recorded:
    /// no further loop transition may commit new work, and it becomes terminal
    /// once nothing is outstanding. An effect committed *before* the wind-down
    /// is not new work: it still flushes to its sink — an interrupted flush may
    /// already have reached it — and its result is recorded, never refused. A
    /// requested cancellation and a failed effect with still-outstanding
    /// siblings both wind down through here
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    Cancelling,
    /// The run is compensating work it already performed.
    Compensating,
    /// The run completed a handoff of its task to another agent.
    HandedOff,
    /// The run was replaced by a newer assignment generation.
    Superseded,
    /// The task accepted the run's typed result.
    Completed,
    /// The run failed.
    Failed,
    /// The run was cancelled.
    Cancelled,
}

impl AgentRunStatus {
    /// Stable kebab-case label for errors, logs, and bounded metric labels.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::WaitingForTimer => "waiting-for-timer",
            Self::WaitingForEffect => "waiting-for-effect",
            Self::WaitingForApproval => "waiting-for-approval",
            Self::WaitingForAuthorization => "waiting-for-authorization",
            Self::WaitingForReconciliation => "waiting-for-reconciliation",
            Self::Suspended => "suspended",
            Self::Cancelling => "cancelling",
            Self::Compensating => "compensating",
            Self::HandedOff => "handed-off",
            Self::Superseded => "superseded",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the run has reached a terminal status.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::HandedOff | Self::Superseded | Self::Completed | Self::Failed | Self::Cancelled
        )
    }

    /// Whether the run is in an interrupted, non-executing wait.
    ///
    /// A wait is not a resident state: the entity passivates, and a durable
    /// command resumes it.
    #[must_use]
    pub const fn is_waiting(self) -> bool {
        matches!(
            self,
            Self::WaitingForTimer
                | Self::WaitingForEffect
                | Self::WaitingForApproval
                | Self::WaitingForAuthorization
                | Self::WaitingForReconciliation
        )
    }

    /// Whether this is one of the three waits the durable substrate renders as
    /// its single `WaitingForHuman`.
    ///
    /// See the compatibility note on this type. Slice 1.12 projects exactly this
    /// set onto the A2A `input-required` state.
    #[must_use]
    pub const fn is_waiting_for_human(self) -> bool {
        matches!(
            self,
            Self::WaitingForApproval
                | Self::WaitingForAuthorization
                | Self::WaitingForReconciliation
        )
    }

    /// Whether the run may still perform a bounded loop transition.
    ///
    /// A suspended or cancelling run may not: cancellation fences *new* work
    /// immediately — no further transition commits an effect — even though the
    /// run does not become terminal until its outstanding effects resolve, and
    /// an effect that already reached the dispatch layer still settles
    /// truthfully ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    /// A run waiting for reconciliation may not either: automatic progress
    /// past an effect whose outcome is unknown is exactly what
    /// [specification 11.5](../../../docs/plans/rakka-agent/spec.md) revokes.
    /// A run waiting for an approval or authorization decision is parked the
    /// same way: it may not crank another turn until a principal resolves the
    /// checkpoint gating its effect
    /// ([specification 12](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn permits_progress(self) -> bool {
        !self.is_terminal()
            && !matches!(
                self,
                Self::Suspended
                    | Self::Cancelling
                    | Self::WaitingForReconciliation
                    | Self::WaitingForApproval
                    | Self::WaitingForAuthorization
            )
    }
}

impl Display for AgentRunStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Why a run reached its terminal status.
///
/// Every variant carries a stable code and the facts an operator needs. Budget
/// exhaustion in particular is a *structured* stop
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)): it names the
/// dimension, the limit, and what was consumed, so the top-up exchange of slice
/// 1.9 has something to act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunTerminalReason {
    /// The task accepted the run's typed result.
    ResultAccepted,
    /// The task's deterministic rules refused the run's proposals until its
    /// rejection budget was spent.
    ResultRejectionsExhausted,
    /// The task refused the proposal without making a validation decision.
    TaskRefusedProposal {
        /// The task's stable refusal code.
        code: String,
        /// The task's status when it refused.
        status: AgentTaskStatus,
    },
    /// A newer assignment generation replaced this run.
    Superseded,
    /// A hard budget ceiling was reached.
    BudgetExhausted {
        /// Which ceiling, and what had been consumed.
        exhaustion: AgentBudgetExhaustion,
    },
    /// A durable effect failed and the run could not continue.
    EffectFailed {
        /// The effect that failed.
        effect_id: AgentEffectId,
        /// Its stable failure code.
        code: String,
    },
    /// An ambiguous effect was closed by an explicitly scheduled compensation
    /// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)); the run
    /// winds down once the compensation settles.
    EffectCompensated {
        /// The effect whose ambiguous generation was compensated.
        effect_id: AgentEffectId,
        /// The application-defined compensation that was scheduled.
        compensation: AgentCompensationRef,
    },
    /// Cancellation was requested.
    CancellationRequested {
        /// A bounded, stable reason.
        reason: String,
    },
    /// The run could not interpret the task's decision, so it stopped rather
    /// than guess.
    UndecodableDecision {
        /// The stable decoding error code.
        code: String,
    },
}

impl AgentRunTerminalReason {
    /// The status this reason puts the run in.
    #[must_use]
    pub fn status(&self) -> AgentRunStatus {
        match self {
            Self::ResultAccepted => AgentRunStatus::Completed,
            Self::Superseded => AgentRunStatus::Superseded,
            Self::CancellationRequested { .. } => AgentRunStatus::Cancelled,
            Self::TaskRefusedProposal { status, .. } => match status {
                // The task was cancelled out from under the run, so the run is
                // cancelled too — not failed. The distinction matters to every
                // policy that treats a failure as something to retry.
                AgentTaskStatus::Cancelled => AgentRunStatus::Cancelled,
                // Another run already completed the task, so this one is not the
                // current owner and never will be.
                AgentTaskStatus::Completed => AgentRunStatus::Superseded,
                _ => AgentRunStatus::Failed,
            },
            Self::ResultRejectionsExhausted
            | Self::BudgetExhausted { .. }
            | Self::EffectFailed { .. }
            | Self::EffectCompensated { .. }
            | Self::UndecodableDecision { .. } => AgentRunStatus::Failed,
        }
    }

    /// Stable machine-readable code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ResultAccepted => "result-accepted",
            Self::ResultRejectionsExhausted => "result-rejections-exhausted",
            Self::TaskRefusedProposal { .. } => "task-refused-proposal",
            Self::Superseded => "superseded",
            Self::BudgetExhausted { .. } => "budget-exhausted",
            Self::EffectFailed { .. } => "effect-failed",
            Self::EffectCompensated { .. } => "effect-compensated",
            Self::CancellationRequested { .. } => "cancellation-requested",
            Self::UndecodableDecision { .. } => "undecodable-decision",
        }
    }
}

/// How far a run has got in handing its escrow back to its task
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The two steps are two exchanges and they are ordered, because the order is a
/// correctness property rather than a style choice: see
/// [`crate::budget`]. This status is what sequences them across passivation,
/// recovery, and shard movement — the run does not hold the sequence in memory,
/// it reads it back out of its own durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunSettlementStatus {
    /// The run has not yet reported what it consumed. A live run is always
    /// here: settlement travels only after a known terminal outcome.
    Owed,
    /// The task has recorded the run's consumption. Its escrow is still
    /// outstanding, so the parent is conservatively short of headroom until the
    /// return lands.
    Settled,
    /// The task has released the run's unused escrow. The run owes its parent
    /// nothing further, ever.
    Returned,
}

impl AgentRunSettlementStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Owed => "owed",
            Self::Settled => "settled",
            Self::Returned => "returned",
        }
    }
}

impl Display for AgentRunSettlementStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The bounded materialized record of one run.
///
/// It holds what the next legal transition needs: the immutable binding to its
/// task, the generation it owns, the typed contract it must satisfy, its status,
/// its loop state, and its terminal reason. It never accumulates: a turn's
/// content leaves the record when the turn is recorded, and resolved effects
/// leave with it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRun {
    /// The task this run serves, fixed for its lifetime
    /// ([specification 6.5](../../../docs/plans/rakka-agent/spec.md)).
    pub binding: AgentRunBinding,
    /// The assignment generation this run owns. A run serves exactly one.
    pub generation: AgentAssignmentGeneration,
    /// The typed contract the run must satisfy.
    pub definition: AgentTaskDefinition,
    /// The task's bounded input.
    pub input: AgentTaskContent,
    /// The run's lifecycle status.
    pub status: AgentRunStatus,
    /// The durable loop state.
    pub loop_state: AgentLoopState,
    /// Why the run reached its terminal status.
    pub terminal_reason: Option<AgentRunTerminalReason>,
    /// How far the run has got in handing its escrow back to its task
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    pub settlement: AgentRunSettlementStatus,
    /// When the run durably accepted its assignment, stamped by the owner that
    /// wrote it.
    pub accepted_at: AgentTimestampMillis,
}

impl AgentRun {
    /// The task this run serves.
    #[must_use]
    pub const fn task(&self) -> &AgentTaskId {
        self.binding.task()
    }

    /// Whether the run may perform another bounded loop transition right now.
    ///
    /// This is the authoritative crank condition, and it is deliberately a
    /// property of the *run* rather than of the phase alone: a
    /// `DecidingContinuation` that has already proposed its result is waiting for
    /// the task's decision, and a run that is cancelling may not dispatch
    /// anything further however executable its phase looks.
    #[must_use]
    pub fn can_advance(&self) -> bool {
        if !self.status.permits_progress() {
            return false;
        }
        if self.loop_state.pending_top_up().is_some() {
            // The run exhausted a ceiling and is parked on a top-up request. It
            // may not crank until the parent's grant relieves it or the run
            // fails: cranking now would re-hit the same ceiling, or worse,
            // proceed on budget it does not hold
            // ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
            return false;
        }
        match self.loop_state.phase() {
            AgentLoopPhase::PreparingContext
            | AgentLoopPhase::EvaluatingModelOutput
            | AgentLoopPhase::RecordingTurn => true,
            AgentLoopPhase::DecidingContinuation => self.loop_state.proposal().is_none(),
            AgentLoopPhase::AwaitingModel
            | AgentLoopPhase::AwaitingTools
            | AgentLoopPhase::Suspended
            | AgentLoopPhase::Complete => false,
        }
    }

    /// Serialized size of the materialized record, in bytes.
    #[must_use]
    pub fn materialized_size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects a record that exceeds its bounds, keeping `reserve` bytes below
    /// the materialized maximum.
    fn check_bounds(&self, reserve: usize) -> AgentRunResult<()> {
        let bytes = self.materialized_size_bytes();
        let maximum = AGENT_RUN_MATERIALIZED_MAX_BYTES.saturating_sub(reserve);
        if bytes > maximum {
            return Err(AgentRunError::MaterializedStateTooLarge { bytes, maximum });
        }
        Ok(())
    }
}

/// The compact result of one accepted run transition.
///
/// A replayed operation returns this again rather than transitioning twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunOutcome {
    /// The run's status after the transition.
    pub status: AgentRunStatus,
    /// Where its loop stands.
    pub phase: AgentLoopPhase,
    /// The turn it is on.
    pub turn: u64,
    /// The assignment generation it owns.
    pub generation: AgentAssignmentGeneration,
    /// How many effects it is still waiting on.
    pub outstanding_effects: usize,
    /// Whether it has a result proposal awaiting the task's decision.
    pub proposal_pending: bool,
}

/// Bounded log of resolved operation ids and the outcome each produced.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentRunOperationLog {
    entries: VecDeque<AgentRunOperationLogEntry>,
}

impl AgentRunOperationLog {
    /// The outcome a previously applied operation produced, if it is still in
    /// the window.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentRunOutcome> {
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

    fn record(&mut self, operation_id: AgentOperationId, outcome: AgentRunOutcome) {
        self.entries.push_back(AgentRunOperationLogEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_RUN_OPERATION_LOG_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentRunOperationLogEntry {
    operation_id: AgentOperationId,
    outcome: AgentRunOutcome,
}

/// The durable state of one run entity.
///
/// The run's materialized record, the operations it has resolved, and the
/// exchange journal the choreography substrate writes — all in one
/// compare-and-set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunState {
    schema_version: StateSchemaVersion,
    scope: AgentRunScope,
    run: Option<AgentRun>,
    applied_operations: AgentRunOperationLog,
    journal: AgentExchangeJournal,
    updated_at: AgentTimestampMillis,
}

impl AgentRunState {
    /// The state of a run that has never accepted an assignment.
    #[must_use]
    pub fn unassigned(scope: AgentRunScope, now: AgentTimestampMillis) -> Self {
        Self {
            schema_version: CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION,
            scope,
            run: None,
            applied_operations: AgentRunOperationLog::default(),
            journal: AgentExchangeJournal::new(),
            updated_at: now,
        }
    }

    /// The scope this state belongs to.
    #[must_use]
    pub const fn scope(&self) -> &AgentRunScope {
        &self.scope
    }

    /// The tenant boundary of this run.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        self.scope.tenant()
    }

    /// The materialized run, once it has accepted an assignment.
    #[must_use]
    pub const fn run(&self) -> Option<&AgentRun> {
        self.run.as_ref()
    }

    /// Whether the run has accepted an assignment.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        self.run.is_some()
    }

    /// The run's status, once it exists.
    #[must_use]
    pub fn status(&self) -> Option<AgentRunStatus> {
        self.run.as_ref().map(|run| run.status)
    }

    /// The run's loop state, once it exists.
    #[must_use]
    pub fn loop_state(&self) -> Option<&AgentLoopState> {
        self.run.as_ref().map(|run| &run.loop_state)
    }

    /// The bounded log of resolved operations.
    #[must_use]
    pub const fn applied_operations(&self) -> &AgentRunOperationLog {
        &self.applied_operations
    }

    /// The time of the last accepted transition.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// The compact outcome describing the current state.
    #[must_use]
    pub fn outcome(&self) -> AgentRunOutcome {
        let Some(run) = &self.run else {
            return AgentRunOutcome {
                status: AgentRunStatus::Accepted,
                phase: AgentLoopPhase::PreparingContext,
                turn: 0,
                generation: AgentAssignmentGeneration::UNASSIGNED,
                outstanding_effects: 0,
                proposal_pending: false,
            };
        };
        AgentRunOutcome {
            status: run.status,
            phase: run.loop_state.phase(),
            turn: run.loop_state.turn(),
            generation: run.generation,
            outstanding_effects: run.loop_state.outstanding_effects().count(),
            proposal_pending: run.loop_state.proposal().is_some(),
        }
    }

    /// A bounded, credential-free projection of this state.
    #[must_use]
    pub fn snapshot(&self) -> Option<AgentRunSnapshot> {
        let run = self.run.as_ref()?;
        Some(AgentRunSnapshot {
            scope: self.scope.clone(),
            task: run.task().clone(),
            goal: run.loop_state.goal().cloned(),
            generation: run.generation,
            status: run.status,
            phase: run.loop_state.phase(),
            turn: run.loop_state.turn(),
            outstanding_effects: run.loop_state.outstanding_effects().count(),
            proposal: run.loop_state.proposal().cloned(),
            accepted_result: run.loop_state.accepted_result().cloned().map(Box::new),
            feedback: run.loop_state.feedback().map(ToString::to_string),
            budget: *run.loop_state.budget(),
            agent_definition_revision: run.loop_state.agent_definition_revision(),
            agent_settings_revision: run.loop_state.agent_settings_revision(),
            terminal_reason: run.terminal_reason.clone(),
            settlement: run.settlement,
            pending_top_up: run.loop_state.pending_top_up().copied(),
            accepted_at: run.accepted_at,
            updated_at: self.updated_at,
        })
    }

    fn run_mut(&mut self) -> AgentRunResult<&mut AgentRun> {
        self.run.as_mut().ok_or_else(|| AgentRunError::NotAccepted {
            scope: self.scope.clone(),
        })
    }
}

impl AgentExchangeState for AgentRunState {
    fn exchange_journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }

    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal {
        &mut self.journal
    }

    fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        policy.check_record(self)?;
        if let Some(run) = &self.run {
            policy.check_record(&run.definition)?;
            run.loop_state.check_schema(policy)?;
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentRunState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::RunState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, credential-free projection of one run's durable state.
///
/// It is the authoritative point read of
/// [specification 17.18](../../../docs/plans/rakka-agent/spec.md): assembled
/// from durable state, correct while the run is passivated and while telemetry
/// is entirely unavailable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunSnapshot {
    /// The run's scope.
    pub scope: AgentRunScope,
    /// The task it serves.
    pub task: AgentTaskId,
    /// The goal it contributes to.
    pub goal: Option<crate::identity::AgentGoalId>,
    /// The assignment generation it owns.
    pub generation: AgentAssignmentGeneration,
    /// Its lifecycle status.
    pub status: AgentRunStatus,
    /// Where its loop stands.
    pub phase: AgentLoopPhase,
    /// The turn it is on.
    pub turn: u64,
    /// How many effects it is still waiting on.
    pub outstanding_effects: usize,
    /// The result proposal awaiting the task's decision.
    pub proposal: Option<AgentRunProposal>,
    /// The typed result the task accepted.
    pub accepted_result: Option<Box<AgentAcceptedResult>>,
    /// The sanitized feedback its last rejected proposal returned.
    pub feedback: Option<String>,
    /// Its own durable budget ledger.
    pub budget: AgentRunBudget,
    /// The agent definition revision it was assigned under.
    pub agent_definition_revision: AgentRevisionNumber,
    /// The agent settings revision it pinned.
    pub agent_settings_revision: AgentRevisionNumber,
    /// Why it reached its terminal status.
    pub terminal_reason: Option<AgentRunTerminalReason>,
    /// How far it has got in handing its escrow back to its task.
    pub settlement: AgentRunSettlementStatus,
    /// The top-up it is parked waiting on, when it has exhausted its budget and
    /// asked its parent for more.
    pub pending_top_up: Option<AgentPendingTopUp>,
    /// When it durably accepted its assignment.
    pub accepted_at: AgentTimestampMillis,
    /// The time of its last accepted transition.
    pub updated_at: AgentTimestampMillis,
}

/// Loads one run's durable state without waking its entity.
///
/// Correct while the run is passivated, because it reads the same record the
/// entity transitions. The schema check applies here too, so a stale reader
/// fails closed rather than projecting a record it cannot interpret.
pub async fn load_agent_run_state<Store>(
    store: &Store,
    scope: &AgentRunScope,
    policy: &AgentSchemaPolicy,
) -> AgentRunResult<Option<AgentRunState>>
where
    Store: DurableStateStore<AgentRunState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(record.state))
}

/// Derives the stable id of the result proposal one run's turn makes.
///
/// The id is derived, not generated, so a run lost before its proposal reached
/// the task re-drives *the same* proposal, and the task returns its original
/// decision rather than validating a second time
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
pub fn proposal_operation_id(
    scope: &AgentRunScope,
    turn: u64,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::ResultProposal,
        [
            scope.tenant().as_str(),
            scope.agent().as_str(),
            scope.run().as_str(),
            &turn.to_string(),
        ],
    )
}

fn bounded_detail(detail: impl Into<String>) -> String {
    let mut detail = detail.into();
    if detail.len() > AGENT_RUN_DETAIL_MAX_LENGTH {
        // Truncating at the byte limit alone would panic mid-character on
        // multi-byte UTF-8, so back off to the nearest char boundary.
        detail.truncate(
            (0..=AGENT_RUN_DETAIL_MAX_LENGTH)
                .rev()
                .find(|index| detail.is_char_boundary(*index))
                .unwrap_or(0),
        );
    }
    detail
}

/// Moves the run to a terminal status and records why.
fn terminate(
    state: &mut AgentRunState,
    reason: AgentRunTerminalReason,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let run = state.run_mut()?;
    if run.status.is_terminal() {
        return Ok(());
    }
    run.status = reason.status();
    run.loop_state.set_phase(AgentLoopPhase::Complete);
    // A terminal run owes its escrow back, not a top-up: clearing the park here
    // means a run cancelled while waiting on a grant stops asking and settles.
    run.loop_state.clear_pending_top_up();
    run.terminal_reason = Some(reason);
    state.updated_at = now;
    Ok(())
}

/// Responds to a budget exhaustion: park to ask the parent for more when the
/// dimension is a conserved quantity, or stop when nothing can grant it.
///
/// A wall-clock deadline and a concurrency ceiling are not quantities a parent
/// can hand over — the first is elapsed time, the second a level — so a run that
/// hits one stops with the structured reason rather than asking for more and
/// re-parking on a ceiling that would never move
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
fn park_or_terminate(
    state: &mut AgentRunState,
    exhaustion: AgentBudgetExhaustion,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    if exhaustion.dimension.is_conserved() {
        park_for_top_up(state, exhaustion, now)
    } else {
        terminate(
            state,
            AgentRunTerminalReason::BudgetExhausted { exhaustion },
            now,
        )
    }
}

/// Parks the run on a top-up request after a charge exhausted a ceiling
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run's status stays `Running` — a pending inter-entity exchange is its
/// own durable outbox, not a residency wait — and its phase is unchanged, so
/// the transition that hit the ceiling re-runs and re-charges once the parent's
/// grant relieves it. The [`owed_ledger_exchange`] that follows this transition
/// commits the `BudgetAllocation` request the courier then drives.
fn park_for_top_up(
    state: &mut AgentRunState,
    exhaustion: AgentBudgetExhaustion,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let run = state.run_mut()?;
    run.loop_state.park_for_top_up(exhaustion);
    run.status = AgentRunStatus::Running;
    state.updated_at = now;
    Ok(())
}

/// The run's half of the assignment exchange: create the run, bind it to its
/// task, and accept.
///
/// The transition is fenced on the run's own durable state, so a replay that has
/// aged out of the journal's deduplication window is still answered from what the
/// run already is rather than applied a second time.
fn accept_assignment(
    state: &mut AgentRunState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let assignment: AgentRunAssignment =
        match envelope.payload().decode(AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE) {
            Ok(assignment) => assignment,
            Err(error) => return refuse(error.code(), error.to_string()),
        };

    if assignment.run != state.scope {
        // The envelope was routed to this run's address, so a payload naming a
        // different run is a composition bug, not a routing one. Fail closed.
        return refuse(
            "assignment-run-mismatch",
            format!(
                "the assignment names run {} but was delivered to {}",
                assignment.run, state.scope
            ),
        );
    }

    if let Some(existing) = state.run.as_ref() {
        // The domain fence. A run serves exactly one assignment generation for
        // its whole life ([specification 6.5]), so the only assignment it can
        // accept twice is the one it already holds — and that is a replay, which
        // is answered with the original acceptance.
        if existing.generation != assignment.generation {
            return refuse(
                "run-generation-conflict",
                format!(
                    "run {} serves generation {} and cannot also serve generation {}",
                    state.scope, existing.generation, assignment.generation
                ),
            );
        }
        return acceptance(&state.scope, existing.generation, existing.accepted_at);
    }

    if let Err(error) = assignment.definition.validate() {
        return refuse(error.code(), error.to_string());
    }
    if let Err(error) = assignment.input.validate() {
        return refuse(error.code(), error.to_string());
    }

    let mut binding = AgentRunBinding::new(state.scope.clone(), assignment.task.task().clone());
    if let Some(goal) = assignment.goal.clone() {
        binding = binding.with_goal(goal);
    }

    // The run credits exactly what its parent debited and carried on this
    // command — never what the task definition's ceilings say, which is a
    // ceiling rather than an escrow and would let every generation of a task
    // spend the task's whole budget again
    // ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    let budget = AgentRunBudget::allocate(assignment.budget, now);
    let loop_state = AgentLoopState::started(
        assignment.task.task().clone(),
        assignment.goal.clone(),
        assignment.agent_definition_revision,
        assignment.agent_settings_revision,
        assignment.definition.version,
        budget,
    );

    let run = AgentRun {
        binding,
        generation: assignment.generation,
        definition: assignment.definition,
        input: assignment.input,
        status: AgentRunStatus::Accepted,
        loop_state,
        terminal_reason: None,
        settlement: AgentRunSettlementStatus::Owed,
        accepted_at: now,
    };
    // Acceptance reserves growth headroom: a run admitted here must still be able
    // to hold the turn it was created to take.
    if let Err(error) = run.check_bounds(AGENT_RUN_STATE_GROWTH_RESERVE_BYTES) {
        return refuse(error.code(), error.to_string());
    }

    let generation = run.generation;
    state.run = Some(run);
    state.updated_at = now;
    acceptance(&state.scope, generation, now)
}

fn acceptance(
    scope: &AgentRunScope,
    generation: AgentAssignmentGeneration,
    accepted_at: AgentTimestampMillis,
) -> AgentExchangeResult {
    let acceptance = AgentRunAcceptance {
        run: scope.clone(),
        generation,
        accepted_at,
    };
    let payload = AgentExchangePayload::encode(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE, &acceptance)
        .unwrap_or_else(|_| AgentExchangePayload::empty(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE));
    AgentExchangeResult::accepted(payload)
}

fn refuse(code: &str, message: impl Into<String>) -> AgentExchangeResult {
    AgentExchangeResult::rejected(
        code,
        message,
        AgentExchangePayload::empty(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE),
    )
}

/// Derives the stable id of one ledger exchange a run owes its task.
///
/// The id is derived from durable identity alone, so a run lost before its
/// settlement reached the task re-drives *the same* operation, and the task's
/// escrow answers from what it already recorded rather than crediting twice
/// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 61).
/// The step is part of the id because settlement and return are two commands
/// against one child escrow, and one id could not name both.
pub fn ledger_operation_id(
    scope: &AgentRunScope,
    kind: AgentExchangeKind,
    sequence: u64,
) -> Result<AgentOperationId, AgentIdentityError> {
    let operation = match kind {
        AgentExchangeKind::BudgetAllocation => AgentOperationKind::BudgetAllocation,
        _ => AgentOperationKind::BudgetSettlement,
    };
    AgentOperationId::new(
        operation,
        [
            scope.tenant().as_str(),
            scope.agent().as_str(),
            scope.run().as_str(),
            kind.as_label(),
            &sequence.to_string(),
        ],
    )
}

/// The ledger exchange a terminal run owes its task next, if it owes one.
///
/// A terminal run hands its escrow back in two ordered steps — settle what it
/// consumed, then return what it did not
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)) — and its
/// [`AgentRunSettlementStatus`] is what sequences them across passivation and
/// recovery. Each step's envelope is derived from durable identity alone, so a
/// run lost mid-exchange re-drives *the same* operation id and the task's escrow
/// answers it from what it already recorded, never crediting the parent twice
/// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 61).
///
/// The return carries no amount: the task recorded what it escrowed and has
/// already applied the settlement, so it computes the remainder. The run only
/// says "I am done with this escrow."
fn owed_ledger_exchange(
    state: &AgentRunState,
    now: AgentTimestampMillis,
) -> AgentRunResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let Some(run) = state.run.as_ref() else {
        return Ok(Vec::new());
    };
    let task = AgentTaskScope::new(scope.tenant().clone(), run.task().clone())?;

    // A parked run owes its parent a top-up request. A run is never both parked
    // and terminal: `terminate` clears the park, so these two branches never
    // both fire.
    if let Some(top_up) = run.loop_state.pending_top_up() {
        if !run.status.is_terminal() {
            let request = crate::task::AgentBudgetTopUpRequest {
                run: scope.clone(),
                generation: run.generation,
                sequence: top_up.sequence,
                exhaustion: top_up.exhaustion,
            };
            let payload = AgentExchangePayload::encode(
                crate::task::AGENT_BUDGET_TOP_UP_PAYLOAD_TYPE,
                &request,
            )?;
            let operation_id =
                ledger_operation_id(&scope, AgentExchangeKind::BudgetAllocation, top_up.sequence)?;
            let envelope = AgentExchangeEnvelope::new(
                operation_id.clone(),
                AgentExchangeKind::BudgetAllocation,
                AgentEntityAddress::Run(scope.clone()),
                AgentEntityAddress::Task(task),
                payload,
                AgentCorrelationId::new(operation_id.as_str()),
                now,
            )?;
            return Ok(vec![envelope]);
        }
    }

    // Settlement travels only after a known terminal outcome: a live run may
    // still spend what it holds, so its consumption is not yet a fact.
    if !run.status.is_terminal() {
        return Ok(Vec::new());
    }

    let (kind, payload) = match run.settlement {
        AgentRunSettlementStatus::Owed => {
            let settlement = crate::task::AgentBudgetSettlement {
                run: scope.clone(),
                generation: run.generation,
                consumed: *run.loop_state.budget().consumption(),
            };
            (
                AgentExchangeKind::BudgetSettlement,
                AgentExchangePayload::encode(
                    crate::task::AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE,
                    &settlement,
                )?,
            )
        }
        AgentRunSettlementStatus::Settled => {
            let release = crate::task::AgentBudgetReturn {
                run: scope.clone(),
                generation: run.generation,
            };
            (
                AgentExchangeKind::BudgetReturn,
                AgentExchangePayload::encode(
                    crate::task::AGENT_BUDGET_RETURN_PAYLOAD_TYPE,
                    &release,
                )?,
            )
        }
        AgentRunSettlementStatus::Returned => return Ok(Vec::new()),
    };

    let operation_id = ledger_operation_id(&scope, kind, 0)?;
    let envelope = AgentExchangeEnvelope::new(
        operation_id.clone(),
        kind,
        AgentEntityAddress::Run(scope.clone()),
        AgentEntityAddress::Task(task),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        now,
    )?;
    Ok(vec![envelope])
}

/// Applies the parent's decision on a top-up request
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// A grant that relieves the exhausted ceiling clears the park, so the run
/// re-attempts the charge it was blocked on and continues. A grant of nothing —
/// the parent's honest answer when it has nothing left — or a durable rejection
/// stops the run with the *original* structured exhaustion, because a run must
/// not park on an empty answer forever.
///
/// A grant that arrives after the run has moved on (it was cancelled while
/// parked) is ignored: the task's escrow already recorded the grant and frees it
/// on the run's return, so there is nothing for the run to do.
fn apply_top_up_grant(
    state: &mut AgentRunState,
    result: &AgentExchangeResult,
    now: AgentTimestampMillis,
) {
    let Some(pending) = state
        .run
        .as_ref()
        .and_then(|run| run.loop_state.pending_top_up().copied())
    else {
        return;
    };

    let granted = if result.is_accepted() {
        result
            .payload()
            .decode::<AgentBudgetLedgerOutcome>(AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE)
            .ok()
            .and_then(|outcome| outcome.granted)
            .unwrap_or_else(AgentBudgetAllocation::nothing)
    } else {
        // A durable rejection is a decision too: the parent will not grant, so
        // the run stops exactly as it would on a zero grant.
        AgentBudgetAllocation::nothing()
    };

    // The run resumes iff the parent gave it *more* in the dimension it ran out
    // of. A grant of nothing there cannot change whether the blocked charge
    // succeeds, so re-attempting would re-park on the same ceiling forever —
    // this is the "must not park on an empty answer" rule. A non-zero grant is
    // worth re-attempting: a fan-out larger than one grant re-parks and asks
    // again, and since each grant strictly reduces the parent's headroom, the
    // asking terminates when the parent finally has nothing left.
    let relieved = granted.get(pending.exhaustion.dimension) != Some(0);
    {
        let Some(run) = state.run.as_mut() else {
            return;
        };
        run.loop_state
            .budget_mut()
            .credit(&granted, pending.sequence);
        run.loop_state.clear_pending_top_up();
    }

    if relieved {
        if let Some(run) = state.run.as_mut() {
            run.status = AgentRunStatus::Running;
        }
        state.updated_at = now;
    } else {
        let _terminated = terminate(
            state,
            AgentRunTerminalReason::BudgetExhausted {
                exhaustion: pending.exhaustion,
            },
            now,
        );
    }
}

/// Records that one ledger exchange the run owed has been acknowledged.
fn settle_ledger_exchange(
    state: &mut AgentRunState,
    kind: AgentExchangeKind,
    now: AgentTimestampMillis,
) {
    let Some(run) = state.run.as_mut() else {
        return;
    };
    match kind {
        AgentExchangeKind::BudgetSettlement => run.settlement = AgentRunSettlementStatus::Settled,
        AgentExchangeKind::BudgetReturn => run.settlement = AgentRunSettlementStatus::Returned,
        _ => return,
    }
    state.updated_at = now;
}

/// One bounded loop transition, and whatever it now owes.
///
/// This is the execution rule in code: it advances the loop by exactly one
/// phase, and everything it decided — the effect, the wait, the exchange —
/// commits with it in one compare-and-set
/// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)).
fn advance_once(
    state: &mut AgentRunState,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<Vec<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let Some(run) = state.run.as_ref() else {
        return Ok(Vec::new());
    };
    if !run.can_advance() {
        return Ok(Vec::new());
    }

    let mut owed = match run.loop_state.phase() {
        AgentLoopPhase::PreparingContext => {
            prepare_context(state, &scope, policies, now).map(|()| Vec::new())
        }
        AgentLoopPhase::EvaluatingModelOutput => {
            evaluate_model_output(state, &scope, policies, now).map(|()| Vec::new())
        }
        AgentLoopPhase::RecordingTurn => record_turn(state, now).map(|()| Vec::new()),
        AgentLoopPhase::DecidingContinuation => decide_continuation(state, &scope, now),
        // `can_advance` already excluded these.
        AgentLoopPhase::AwaitingModel
        | AgentLoopPhase::AwaitingTools
        | AgentLoopPhase::Suspended
        | AgentLoopPhase::Complete => Ok(Vec::new()),
    }?;
    // A transition that reached a terminal outcome — a budget exhaustion here, a
    // rejection the loop cannot iterate past — hands the run's escrow back in the
    // same compare-and-set that made it terminal, exactly as it persists the
    // effect or wait it decided ([specification 9.7]). The settlement rides the
    // run's own journal and is delivered by the courier, never from inside this
    // transition.
    owed.extend(owed_ledger_exchange(state, now)?);
    Ok(owed)
}

/// Prepares the turn's immutable context and persists the model effect it will
/// wait on.
///
/// The effect and the wait commit together, so there is no instant at which the
/// run is durably waiting for an effect that was never recorded.
fn prepare_context(
    state: &mut AgentRunState,
    scope: &AgentRunScope,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let turn = {
        let run = state.run_mut()?;
        run.loop_state.turn()
    };

    let context = AgentContextSnapshotRef::for_turn(scope, turn)?;
    let profile = None;
    let request = AgentRunEffectRequest::Model {
        context: context.clone(),
        profile,
    };
    // The spec — safety class, attempt bound, protocol, credential binding —
    // is trusted deployment/definition data resolved by the run's transition,
    // never something the model's output can choose or widen
    // ([specification 11.2]).
    let spec = policies.spec_for(&request).clone();

    // Everything the turn costs is charged *before* the effect is persisted, in
    // one all-or-nothing reservation: the iteration, the model call, the effect,
    // and its whole attempt bound. An effect that reaches durable dispatch has
    // been paid for whether or not its outcome is ever known, and reserving the
    // attempt bound up front is what stops a run from starting work it could not
    // afford to finish retrying ([specification 9.7]). A conserved-dimension
    // exhaustion parks the run — nothing charged — to ask its parent for more; a
    // deadline stops it.
    {
        let run = state.run_mut()?;
        let outstanding = run.loop_state.outstanding_effects().count();
        if let Err(exhaustion) =
            run.loop_state
                .budget_mut()
                .reserve_model_turn(spec.max_attempts, outstanding, now)
        {
            return park_or_terminate(state, exhaustion, now);
        }
    }

    let (slot, settings_revision) = {
        let run = state.run_mut()?;
        (
            run.loop_state.next_effect_slot(),
            run.loop_state.agent_settings_revision(),
        )
    };
    let effect = AgentRunEffect::new(scope, turn, slot, request, &spec, settings_revision, now)?;

    let run = state.run_mut()?;
    run.loop_state.record_effect(effect)?;
    run.loop_state.set_context_snapshot(context);
    run.loop_state.set_phase(AgentLoopPhase::AwaitingModel);
    run.status = AgentRunStatus::WaitingForEffect;
    run.check_bounds(0)?;
    state.updated_at = now;
    Ok(())
}

/// Acts on the turn the model produced: persist its tool effects, or move on to
/// record it.
fn evaluate_model_output(
    state: &mut AgentRunState,
    scope: &AgentRunScope,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let (turn, calls) = {
        let run = state.run_mut()?;
        let Some(model_turn) = run.loop_state.pending_turn() else {
            // Nothing to evaluate. The turn is recorded, which is the only way
            // out of this phase that does not invent an output the model never
            // produced.
            run.loop_state.set_phase(AgentLoopPhase::RecordingTurn);
            run.status = AgentRunStatus::Running;
            state.updated_at = now;
            return Ok(());
        };
        (run.loop_state.turn(), model_turn.tool_calls.clone())
    };

    if calls.is_empty() {
        let run = state.run_mut()?;
        run.loop_state.set_phase(AgentLoopPhase::RecordingTurn);
        run.status = AgentRunStatus::Running;
        state.updated_at = now;
        return Ok(());
    }

    // Each tool's spec — safety class, attempt bound, protocol — comes from its
    // registration, defaulting to a single non-idempotent attempt for a tool the
    // deployment has not classified: the model asked for the call, but it cannot
    // choose how failure and ambiguity are handled ([specification 11.2]).
    let requests: Vec<(AgentRunEffectRequest, AgentEffectSpec)> = calls
        .into_iter()
        .map(|call| {
            let request = AgentRunEffectRequest::Tool {
                call: Box::new(call),
            };
            let spec = policies.spec_for(&request).clone();
            (request, spec)
        })
        .collect();

    // The whole fan-out is reserved before any effect is recorded, all-or-nothing:
    // one tool call and one effect per tool, plus the sum of their attempt bounds,
    // checked against the concurrency ceiling. A turn either fits its budget or
    // commits none of it, so the record never holds a partial fan-out; a run that
    // cannot afford it parks with nothing charged and re-attempts this exact
    // fan-out once its parent tops it up ([specification 9.7]).
    {
        let count = u64::try_from(requests.len()).unwrap_or(u64::MAX);
        let total_attempts = requests
            .iter()
            .map(|(_, spec)| u64::from(spec.max_attempts))
            .fold(0, u64::saturating_add);
        let run = state.run_mut()?;
        let outstanding = run.loop_state.outstanding_effects().count();
        if let Err(exhaustion) =
            run.loop_state
                .budget_mut()
                .reserve_tool_turn(count, total_attempts, outstanding, now)
        {
            return park_or_terminate(state, exhaustion, now);
        }
    }

    for (request, spec) in requests {
        let (slot, settings_revision) = {
            let run = state.run_mut()?;
            (
                run.loop_state.next_effect_slot(),
                run.loop_state.agent_settings_revision(),
            )
        };
        let effect =
            AgentRunEffect::new(scope, turn, slot, request, &spec, settings_revision, now)?;
        state.run_mut()?.loop_state.record_effect(effect)?;
    }

    // A tool the deployment marked checkpoint- or authorization-required is not
    // dispatched: the run opens a checkpoint of the matching kind bound to the
    // exact effect and parks until a principal resolves it
    // ([specification 12](../../../docs/plans/rakka-agent/spec.md)). The
    // checkpoint and the effect commit in this one transition, so there is no
    // instant at which a gated effect exists without its gate.
    let gated: Vec<(AgentEffectId, AgentCheckpointKind)> = {
        let run = state.run_mut()?;
        run.loop_state
            .effects()
            .iter()
            .filter(|effect| {
                effect.turn == turn
                    && (effect.checkpoint_required || effect.authorization_required)
                    && run.loop_state.grant_for(effect).is_none()
            })
            .map(|effect| {
                // A tool that requires both gates opens one security
                // authorization: its resolution is an approval-family grant
                // too, and the dispatch authority accepts it for either gate.
                let kind = if effect.authorization_required {
                    AgentCheckpointKind::SecurityAuthorization
                } else {
                    AgentCheckpointKind::Approval
                };
                (effect.effect_id.clone(), kind)
            })
            .collect()
    };
    for (effect_id, kind) in &gated {
        open_effect_checkpoint(state, policies, effect_id, *kind, now)?;
    }

    let run = state.run_mut()?;
    run.loop_state.set_phase(AgentLoopPhase::AwaitingTools);
    run.status = checkpoint_wait_status(&run.loop_state);
    run.check_bounds(0)?;
    state.updated_at = now;
    Ok(())
}

/// The status of a run whose next step is dispatch: parked on whichever
/// approval-family checkpoint wait it holds, or waiting on its effects.
fn checkpoint_wait_status(loop_state: &AgentLoopState) -> AgentRunStatus {
    match loop_state.approval_family_wait() {
        Some(AgentCheckpointKind::SecurityAuthorization) => AgentRunStatus::WaitingForAuthorization,
        Some(_) => AgentRunStatus::WaitingForApproval,
        None => AgentRunStatus::WaitingForEffect,
    }
}

/// Records the turn.
///
/// The usage the provider billed is charged here, because it is only knowable
/// from the turn itself. Slice 1.11 appends the turn and its tool results to
/// session memory at exactly this point; the loop's own record keeps no content.
fn record_turn(state: &mut AgentRunState, now: AgentTimestampMillis) -> AgentRunResult<()> {
    let run = state.run_mut()?;
    if let Some(turn) = run.loop_state.pending_turn() {
        let usage = turn.usage;
        run.loop_state.budget_mut().record_usage(usage);
    }
    run.loop_state
        .set_phase(AgentLoopPhase::DecidingContinuation);
    run.status = AgentRunStatus::Running;
    state.updated_at = now;
    Ok(())
}

/// Decides what follows the turn: propose the typed result, or iterate again.
///
/// A proposal persists the run's own record of it *and* the exchange that
/// carries it, in one compare-and-set, and then waits. The run does not complete
/// the task; the task's validation decision does.
fn decide_continuation(
    state: &mut AgentRunState,
    scope: &AgentRunScope,
    now: AgentTimestampMillis,
) -> AgentRunResult<Vec<AgentExchangeEnvelope>> {
    let proposed = state
        .run_mut()?
        .loop_state
        .pending_turn()
        .and_then(|turn| turn.proposal.clone());

    let Some(content) = proposed else {
        // No result was proposed, so the loop takes another bounded iteration.
        // Whether it may is decided by `prepare_context`, which charges the
        // iteration and stops the run with a structured reason if the ceiling is
        // reached.
        let run = state.run_mut()?;
        run.loop_state.clear_turn();
        run.loop_state.begin_turn(None);
        run.status = AgentRunStatus::Running;
        state.updated_at = now;
        return Ok(Vec::new());
    };

    let (proposal, envelope) = build_proposal(state, scope, content, now)?;

    let run = state.run_mut()?;
    run.loop_state.clear_turn();
    run.loop_state.set_proposal(proposal);
    // The run is not *waiting on an effect*, a timer, or a human: it is waiting
    // for a peer entity's durable decision, and the courier re-drives the
    // exchange until it arrives. `Running` is the honest status, and it is not a
    // residency claim ([specification 9.3]) — the entity passivates here like
    // anywhere else.
    run.status = AgentRunStatus::Running;
    run.check_bounds(0)?;
    state.updated_at = now;
    Ok(vec![envelope])
}

/// Builds the run's persisted proposal and the exchange that carries it to the
/// task.
fn build_proposal(
    state: &mut AgentRunState,
    scope: &AgentRunScope,
    content: AgentTaskContent,
    now: AgentTimestampMillis,
) -> AgentRunResult<(AgentRunProposal, AgentExchangeEnvelope)> {
    let run = state.run_mut()?;
    let turn = run.loop_state.turn();
    let proposal_id = proposal_operation_id(scope, turn)?;
    let digest = content.digest();
    let task_scope = run.binding.task_scope()?;

    let command = AgentTaskResultProposal {
        proposal_id: proposal_id.clone(),
        agent: scope.agent().clone(),
        run: scope.run().clone(),
        generation: run.generation,
        definition_id: run.definition.definition_id.clone(),
        definition_version: run.definition.version,
        result_schema: run.definition.result_schema.clone(),
        content,
        evidence: Vec::new(),
        causation_id: AgentCausationId::new(proposal_id.as_str()),
        proposed_at: now,
    };

    let payload = AgentExchangePayload::encode(AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE, &command)?;
    let envelope = AgentExchangeEnvelope::new(
        proposal_id.clone(),
        AgentExchangeKind::ResultProposal,
        AgentEntityAddress::Run(scope.clone()),
        AgentEntityAddress::Task(task_scope),
        payload,
        AgentCorrelationId::new(proposal_id.as_str()),
        now,
    )?;

    let proposal = AgentRunProposal {
        proposal_id,
        turn,
        result_schema: run.definition.result_schema.clone(),
        definition_id: run.definition.definition_id.clone(),
        definition_version: run.definition.version,
        digest,
        proposed_at: now,
    };
    Ok((proposal, envelope))
}

/// Records the durable result a dispatcher returned for one effect generation.
///
/// It is fenced on the run's own state: a result for an effect the run does not
/// hold, holds under a different generation, or holds as already resolved, is
/// refused rather than applied — which is what makes a duplicate or stale
/// completion unable to advance the loop twice
/// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md);
/// [specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 10).
#[allow(clippy::too_many_arguments)]
fn record_effect_result(
    state: &mut AgentRunState,
    effect_id: &AgentEffectId,
    generation: AgentEffectGeneration,
    attempt: u32,
    fence: u64,
    outcome: AgentRunEffectOutcome,
    policy: &AgentSchemaPolicy,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    outcome.validate()?;
    // The schema gate runs where the outcome enters, like the bounds above: a
    // turn this policy cannot interpret must never commit as the pending turn,
    // because recovery applies the same policy and would fail closed on the
    // committed record on every later activation.
    outcome.check_schema(policy)?;

    let run = state.run_mut()?;
    if run.status.is_terminal() {
        return Err(AgentRunError::Terminal { status: run.status });
    }

    let Some(effect) = run.loop_state.effect_mut(effect_id) else {
        return Err(AgentRunError::UnknownEffect {
            effect_id: effect_id.clone(),
        });
    };
    if effect.generation != generation {
        // A result for a superseded — or fabricated future — generation must
        // not resolve the current one: the reconciliation that minted the
        // current generation did so precisely because the old attempt's
        // outcome could not be trusted.
        return Err(AgentRunError::StaleEffectGeneration {
            effect_id: effect_id.clone(),
            held: effect.generation,
            received: generation,
        });
    }
    if !effect.is_outstanding() {
        return Err(AgentRunError::StaleEffectResult {
            effect_id: effect_id.clone(),
            status: effect.status,
        });
    }
    effect.record_attempt(attempt, fence);

    apply_effect_outcome(state, effect_id, &outcome, now)?;
    // An ambiguous outcome parks the run behind a durable reconciliation
    // checkpoint in the same transition that recorded it, so the wait carries
    // the full record surface — decision set, dedup key, SLA escalation,
    // roles — a bare indeterminate effect could not
    // ([specification 12.1](../../../docs/plans/rakka-agent/spec.md),
    // [12.2](../../../docs/plans/rakka-agent/spec.md)).
    if matches!(outcome, AgentRunEffectOutcome::Indeterminate { .. }) {
        open_effect_checkpoint(
            state,
            policies,
            effect_id,
            AgentCheckpointKind::IndeterminateEffectReconciliation,
            now,
        )?;
    }
    settle_run_disposition(state, now)
}

/// Applies one established outcome to the effect that owes it and to the loop.
///
/// The caller has already fenced: the effect is held, at this generation, and
/// unresolved (or being resolved by an explicit reconciliation decision).
fn apply_effect_outcome(
    state: &mut AgentRunState,
    effect_id: &AgentEffectId,
    outcome: &AgentRunEffectOutcome,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    // Whether the run is already winding down: recording the outcome of work
    // already in flight is not further dispatch, but it must not resume a run
    // that is quiescing ([specification 8.7]).
    let winding_down = {
        let run = state.run_mut()?;
        run.terminal_reason.is_some() || run.status == AgentRunStatus::Cancelling
    };

    let run = state.run_mut()?;
    let Some(effect) = run.loop_state.effect_mut(effect_id) else {
        return Err(AgentRunError::UnknownEffect {
            effect_id: effect_id.clone(),
        });
    };
    // Whether this outcome is the generation's first resolution. A resolved
    // generation re-enters here on exactly one path — the reconciliation of an
    // `Indeterminate` one confirming it executed — and its attempts were
    // already settled when the ambiguity was recorded, so settling again below
    // would bill the same attempts twice.
    let first_resolution = effect.is_outstanding();

    match outcome {
        AgentRunEffectOutcome::Model { turn } => {
            effect.status = AgentRunEffectStatus::Succeeded;
            let turn = (**turn).clone();
            run.loop_state.set_pending_turn(turn);
            run.loop_state
                .set_phase(AgentLoopPhase::EvaluatingModelOutput);
            if !winding_down {
                run.status = AgentRunStatus::Running;
            }
        }
        AgentRunEffectOutcome::Tool { call_id, content } => {
            effect.status = AgentRunEffectStatus::Succeeded;
            let result = AgentToolResult {
                call_id: call_id.clone(),
                content: content.clone(),
                recorded_at: now,
            };
            run.loop_state.record_tool_result(result);
            if !winding_down && !run.loop_state.awaits_effect() {
                // The last tool of the turn came back, so the turn is complete.
                run.loop_state.set_phase(AgentLoopPhase::RecordingTurn);
                run.status = AgentRunStatus::Running;
            }
        }
        AgentRunEffectOutcome::Failed { code, .. }
        | AgentRunEffectOutcome::Exhausted { code, .. } => {
            // Final for the generation: the dispatch layer already applied the
            // effect's own retry policy, so what arrives here is a definitive
            // failure or a spent retry budget — never a retryable attempt
            // ([specification 11.3], [11.5]). The run winds down: the failure
            // fences new dispatch, unsent effects are cancelled in place, and
            // work already at the dispatch layer settles truthfully.
            effect.status = outcome.resolved_status();
            effect.last_error_code = Some(bounded_detail(code.clone()));
            let code = code.clone();
            let run = state.run_mut()?;
            run.loop_state.fence_unsent_effects();
            if run.terminal_reason.is_none() {
                run.terminal_reason = Some(AgentRunTerminalReason::EffectFailed {
                    effect_id: effect_id.clone(),
                    code: bounded_detail(code),
                });
            }
        }
        AgentRunEffectOutcome::Indeterminate { code, .. } => {
            // The ambiguous case of [specification 11.5]: the attempt may have
            // invoked the target and nothing can establish its outcome. The
            // generation parks, every automatic path to redispatch is already
            // revoked at the dispatch layer, and the run waits for an explicit
            // reconciliation decision. No terminal reason is recorded: parking
            // is not a failure, and a wind-down reason an operator already
            // recorded is preserved untouched.
            effect.status = AgentRunEffectStatus::Indeterminate;
            effect.last_error_code = Some(bounded_detail(code.clone()));
        }
        AgentRunEffectOutcome::Cancelled { .. } => {
            // The dispatch layer fenced and settled the generation without
            // invocation. Recording that never resumes the run.
            effect.status = AgentRunEffectStatus::Cancelled;
        }
    }

    // The generation has resolved — succeeded, failed, exhausted, indeterminate,
    // or cancelled — so settle its attempt reservation from the durable result
    // ([specification 9.7]). Every attempt that reached durable `Started`,
    // including one now `Indeterminate`, consumes its attempt budget; the retries
    // the generation did not use are released back to the run. A generation that
    // never dispatched (a cancelled or fenced effect) reports zero attempts and
    // so consumes none. The settle runs exactly once, on the generation's first
    // resolution: a reconciliation that later confirms an `Indeterminate`
    // generation executed changes what is known, not what was attempted, and
    // billing the same attempts again would inflate the run's consumption — and,
    // through its settlement, its parent's — with attempts nobody made.
    if first_resolution {
        let reservation = {
            let run = state.run_mut()?;
            run.loop_state
                .effect_mut(effect_id)
                .map(|effect| (effect.max_attempts, effect.attempts))
        };
        if let Some((reserved, made)) = reservation {
            let run = state.run_mut()?;
            run.loop_state.budget_mut().settle_effect(reserved, made);
        }
    }

    let run = state.run_mut()?;
    run.check_bounds(0)?;
    state.updated_at = now;
    Ok(())
}

/// Recomputes where the run stands after effects settled or were resolved.
///
/// This is the single place the wind-down and reconciliation rules meet:
///
/// - an indeterminate effect parks the run in `WaitingForReconciliation`,
///   cancellation requested or not — the run is *nonterminal* until every
///   ambiguous outcome is explicitly resolved
///   ([specification 8.7](../../../docs/plans/rakka-agent/spec.md),
///   [11.5](../../../docs/plans/rakka-agent/spec.md); scenario 57);
/// - a run that is winding down becomes terminal exactly when nothing blocks
///   settlement, under the reason recorded when the wind-down began — a run
///   cancelled before an effect failed is cancelled, not failed;
/// - otherwise the run keeps whatever live status the outcome application set.
fn settle_run_disposition(
    state: &mut AgentRunState,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let run = state.run_mut()?;
    if run.status.is_terminal() {
        return Ok(());
    }

    if run.loop_state.has_indeterminate_effect() {
        run.status = AgentRunStatus::WaitingForReconciliation;
        state.updated_at = now;
        return Ok(());
    }

    let winding_down = run.terminal_reason.is_some() || run.status == AgentRunStatus::Cancelling;
    if winding_down {
        if run.loop_state.awaits_settlement() {
            run.status = AgentRunStatus::Cancelling;
            state.updated_at = now;
            return Ok(());
        }
        let reason =
            run.terminal_reason
                .clone()
                .unwrap_or(AgentRunTerminalReason::CancellationRequested {
                    reason: "cancelled".to_string(),
                });
        return terminate(state, reason, now);
    }

    state.updated_at = now;
    Ok(())
}

/// Applies an explicit reconciliation decision to an indeterminate effect
/// through the effect-layer command, dropping the reconciliation checkpoint
/// that was parked on it ([specification 11.5](../../../docs/plans/rakka-agent/spec.md),
/// [12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the effect-layer twin of a
/// [`AgentCheckpointOutcome::EffectResolution`] checkpoint decision: both end
/// in [`apply_indeterminate_resolution`], and whichever path resolves the
/// generation retires the wait the other would have answered.
fn resolve_indeterminate_effect(
    state: &mut AgentRunState,
    effect_id: &AgentEffectId,
    generation: AgentEffectGeneration,
    resolution: AgentEffectResolution,
    policy: &AgentSchemaPolicy,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    apply_indeterminate_resolution(state, effect_id, generation, resolution, policy, now)?;
    drop_reconciliation_checkpoint(state, effect_id, generation)?;
    settle_run_disposition(state, now)
}

/// Drops the reconciliation checkpoint parked on one resolved effect
/// generation, when the run holds one: the wait is over, however its decision
/// arrived.
fn drop_reconciliation_checkpoint(
    state: &mut AgentRunState,
    effect_id: &AgentEffectId,
    generation: AgentEffectGeneration,
) -> AgentRunResult<()> {
    let run = state.run_mut()?;
    let retired: Vec<HumanCheckpointId> = run
        .loop_state
        .open_checkpoints()
        .iter()
        .filter(|checkpoint| {
            checkpoint.kind == AgentCheckpointKind::IndeterminateEffectReconciliation
                && checkpoint.bound_effect.effect_id == *effect_id
                && checkpoint.bound_effect.generation == generation
        })
        .map(|checkpoint| checkpoint.checkpoint_id.clone())
        .collect();
    for checkpoint_id in &retired {
        run.loop_state.drop_checkpoint(checkpoint_id);
    }
    Ok(())
}

/// The shared core of both reconciliation paths
/// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// `ConfirmedExecuted` records the established outcome exactly as a dispatcher
/// result would have. `ConfirmedNotExecuted` authorizes a new effect
/// generation where the run still wants the work — and settles the effect as
/// cancelled where it does not, because a winding-down run re-invokes nothing.
fn apply_indeterminate_resolution(
    state: &mut AgentRunState,
    effect_id: &AgentEffectId,
    generation: AgentEffectGeneration,
    resolution: AgentEffectResolution,
    policy: &AgentSchemaPolicy,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    resolution.validate()?;
    let scope = state.scope.clone();

    let run = state.run_mut()?;
    if run.status.is_terminal() {
        return Err(AgentRunError::Terminal { status: run.status });
    }
    let winding_down = run.terminal_reason.is_some();

    let Some(effect) = run.loop_state.effect_mut(effect_id) else {
        return Err(AgentRunError::UnknownEffect {
            effect_id: effect_id.clone(),
        });
    };
    if effect.generation != generation {
        return Err(AgentRunError::StaleEffectGeneration {
            effect_id: effect_id.clone(),
            held: effect.generation,
            received: generation,
        });
    }
    if effect.status != AgentRunEffectStatus::Indeterminate {
        // Only an ambiguous generation takes a reconciliation decision; a
        // duplicate decision finds the effect already resolved and is refused,
        // so it cannot resume the run twice (scenario 11's effect-layer edge).
        return Err(AgentRunError::StaleEffectResult {
            effect_id: effect_id.clone(),
            status: effect.status,
        });
    }

    match resolution {
        AgentEffectResolution::ConfirmedExecuted { outcome } => {
            outcome.check_schema(policy)?;
            apply_effect_outcome(state, effect_id, &outcome, now)?;
        }
        AgentEffectResolution::ConfirmedNotExecuted => {
            if winding_down {
                // Proven never executed, and the run wants nothing further:
                // the fence holds and the generation settles as cancelled.
                effect.status = AgentRunEffectStatus::Cancelled;
                state.updated_at = now;
            } else {
                // A new invocation is authorized, and it is a new generation:
                // a fresh dispatchable intent under the same identity
                // ([specification 11.3]). The ambiguous original is never
                // mutated back into a routine retry.
                //
                // The new generation's attempt bound is reserved before it
                // becomes dispatchable, exactly as the original turn's was: the
                // ambiguous generation's settle released its reservation and
                // consumed only the attempts it made, so re-invocation is new
                // spend the run must still be able to afford
                // ([specification 9.7]). A run that cannot afford it keeps the
                // effect parked `Indeterminate` and refuses the resolution —
                // the operator's remaining decision is to cancel the run, whose
                // wind-down settles the generation without invocation.
                let max_attempts = effect.max_attempts;
                {
                    let run = state.run_mut()?;
                    if let Err(exhaustion) =
                        run.loop_state.budget_mut().reserve_attempts(max_attempts)
                    {
                        return Err(AgentRunError::RedispatchUnaffordable { exhaustion });
                    }
                }
                let run = state.run_mut()?;
                let Some(effect) = run.loop_state.effect_mut(effect_id) else {
                    return Err(AgentRunError::UnknownEffect {
                        effect_id: effect_id.clone(),
                    });
                };
                effect.begin_next_generation(&scope, now)?;
                let run = state.run_mut()?;
                run.status = AgentRunStatus::WaitingForEffect;
                state.updated_at = now;
            }
        }
    }

    Ok(())
}

/// Requests cancellation.
///
/// Acceptance fences new dispatch *in the same compare-and-set*
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)): no further
/// loop transition commits an effect, and every committed effect that provably
/// never reached the sink — `Pending`, which the flush ordering guarantees was
/// never handed over — is cancelled in place. What already reached the dispatch
/// layer is not abandoned: it settles truthfully or, if its outcome turns out
/// to be unknowable, parks the run in reconciliation. A run with such work, or
/// a result proposal the task may already have decided, stays nonterminal
/// until everything settles.
fn cancel(
    state: &mut AgentRunState,
    reason: String,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let run = state.run_mut()?;
    if run.status.is_terminal() {
        return Err(AgentRunError::Terminal { status: run.status });
    }

    run.loop_state.fence_unsent_effects();
    run.loop_state.cancel_open_checkpoints(now);
    if run.terminal_reason.is_none() {
        run.terminal_reason = Some(AgentRunTerminalReason::CancellationRequested {
            reason: bounded_detail(reason),
        });
    }
    run.status = AgentRunStatus::Cancelling;
    settle_run_disposition(state, now)
}

/// The principal a run records as the opener of a checkpoint it raises on its
/// own behalf: the agent, never a resolved credential.
fn agent_principal(scope: &AgentRunScope) -> PrincipalRef {
    PrincipalRef {
        principal_type: "agent".to_string(),
        principal_id: scope.agent().as_str().to_string(),
        display_name: None,
    }
}

/// The stable, derived id of the checkpoint of one kind gating one effect
/// generation, so a re-driven transition opens the same checkpoint rather than
/// a second one. The kind is folded in because one generation can wait on more
/// than one kind over its life — an approval before dispatch, a reconciliation
/// after an ambiguous loss — and the two are different records.
fn checkpoint_id_for(effect: &AgentRunEffect, kind: AgentCheckpointKind) -> HumanCheckpointId {
    let tag = match kind {
        AgentCheckpointKind::Approval => "approval",
        AgentCheckpointKind::SecurityAuthorization => "authz",
        AgentCheckpointKind::IndeterminateEffectReconciliation => "reconcile",
    };
    HumanCheckpointId::new(format!(
        "{}#ck-{tag}-g{}",
        effect.effect_id.as_str(),
        effect.generation.get()
    ))
}

/// Opens a checkpoint of `kind` bound to the exact effect intent, carrying the
/// run's identity and pinned revisions, and records it on the loop state.
fn open_effect_checkpoint(
    state: &mut AgentRunState,
    policies: &AgentEffectPolicies,
    effect_id: &AgentEffectId,
    kind: AgentCheckpointKind,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let scope = state.scope.clone();
    let run = state.run_mut()?;
    let Some(effect) = run
        .loop_state
        .effects()
        .iter()
        .find(|effect| &effect.effect_id == effect_id)
        .cloned()
    else {
        return Err(AgentRunError::UnknownEffect {
            effect_id: effect_id.clone(),
        });
    };
    let checkpoint_id = checkpoint_id_for(&effect, kind);
    let verb = match kind {
        AgentCheckpointKind::Approval => "Approve",
        AgentCheckpointKind::SecurityAuthorization => "Authorize",
        AgentCheckpointKind::IndeterminateEffectReconciliation => "Reconcile indeterminate",
    };
    let summary = match effect.request.tool_call() {
        Some(call) => format!("{verb} tool call {} on turn {}", call.tool, effect.turn),
        None => format!("{verb} effect on turn {}", effect.turn),
    };
    let task = run.loop_state.task().clone();
    let goal = run.loop_state.goal().cloned();
    let settings_revision = run.loop_state.agent_settings_revision();
    let definition_revision = run.loop_state.agent_definition_revision();
    let mut checkpoint = AgentCheckpoint::open(
        checkpoint_id,
        kind,
        scope.clone(),
        &effect,
        summary,
        agent_principal(&scope),
        now,
    )?
    .with_task(task)
    .with_revisions(settings_revision, definition_revision);
    if let Some(goal) = goal {
        checkpoint = checkpoint.with_goal(goal);
    }
    // The deployment-configured SLA becomes durable deadlines on the record, so
    // a durable timer can escalate or expire the wait without any live task
    // ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)). A
    // reconciliation checkpoint never hard-expires: expiry fails the gated
    // effect, and fabricating a definitive failure for an effect whose outcome
    // is unknown is exactly the guess reconciliation exists to prevent — it
    // escalates on the SLA deadline and then waits for the explicit decision.
    let sla = policies.checkpoint_sla();
    if sla.is_set() {
        let (due_at, expires_at) = sla.deadlines(now);
        let expires_at = match kind {
            AgentCheckpointKind::IndeterminateEffectReconciliation => None,
            _ => expires_at,
        };
        checkpoint = checkpoint.with_deadlines(due_at, expires_at, sla.escalation_target.clone());
    }
    run.loop_state.record_checkpoint(checkpoint)?;
    Ok(())
}

/// Applies a decision to a checkpoint the run is waiting on
/// ([specification 12](../../../docs/plans/rakka-agent/spec.md)).
///
/// An `Approve`/`Grant` stores the digest-bound grant and resumes the run so the
/// gated effect dispatches under it; a `Deny` fails the gated effect's
/// generation. A reconciliation decision resolves the ambiguous generation the
/// checkpoint is parked on: the confirming decisions flow through the shared
/// effect-resolution core, `Compensate` schedules the named compensation and
/// winds the run down behind it, `Escalate` keeps the wait, and
/// `AbandonAndFail` fails the run terminally. The decision is deduplicated
/// inside the checkpoint on its decision key, so a duplicate submission never
/// resumes the run twice (scenario 11). A decision for a checkpoint the run no
/// longer holds, or one that does not fit the checkpoint kind, is refused.
#[allow(clippy::too_many_arguments)]
fn resolve_checkpoint(
    state: &mut AgentRunState,
    checkpoint_id: &HumanCheckpointId,
    resolver: PrincipalRef,
    decision: AgentCheckpointDecision,
    decision_key: AgentOperationId,
    policy: &AgentSchemaPolicy,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    {
        let run = state.run_mut()?;
        if run.status.is_terminal() {
            return Err(AgentRunError::Terminal { status: run.status });
        }
    }

    let (report, bound_effect_id, bound_generation) = {
        let run = state.run_mut()?;
        let Some(checkpoint) = run.loop_state.open_checkpoint_mut(checkpoint_id) else {
            return Err(AgentRunError::UnknownCheckpoint {
                checkpoint_id: checkpoint_id.clone(),
            });
        };
        let bound_effect_id = checkpoint.bound_effect.effect_id.clone();
        let bound_generation = checkpoint.bound_effect.generation;
        let report = checkpoint.resolve(decision_key, resolver, decision, now)?;
        (report, bound_effect_id, bound_generation)
    };

    if report.deduplicated {
        // The decision was already applied and its consequences committed in
        // the transition that first carried it; a replay makes no second
        // transition (scenario 11).
        state.updated_at = now;
        return settle_run_disposition(state, now);
    }

    match report.outcome {
        AgentCheckpointOutcome::Granted(grant) => {
            let run = state.run_mut()?;
            run.loop_state.record_grant(*grant);
            run.loop_state.drop_checkpoint(checkpoint_id);
            // Resume: the gated effect is still `Pending`, and the next dispatch
            // pass now finds its grant and hands it to the sink. Another gated
            // effect may still hold the run on its own wait.
            run.status = checkpoint_wait_status(&run.loop_state);
            state.updated_at = now;
        }
        AgentCheckpointOutcome::Denied { reason } => {
            state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
            // A denied consequential effect fails its generation: the run winds
            // down under a truthful code, exactly as a failed effect would.
            let denial = AgentRunEffectOutcome::Failed {
                code: "checkpoint-denied".to_string(),
                message: reason,
            };
            apply_effect_outcome(state, &bound_effect_id, &denial, now)?;
        }
        AgentCheckpointOutcome::EffectResolution(resolution) => {
            state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
            apply_indeterminate_resolution(
                state,
                &bound_effect_id,
                bound_generation,
                *resolution,
                policy,
                now,
            )?;
        }
        AgentCheckpointOutcome::Compensate { compensation } => {
            state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
            schedule_compensation(
                state,
                &bound_effect_id,
                bound_generation,
                compensation,
                policies,
                now,
            )?;
        }
        AgentCheckpointOutcome::Abandoned => {
            state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
            // The operator abandoned the ambiguity: the generation fails under
            // a truthful code and the run winds down — or finishes the
            // wind-down a cancellation already began, under that earlier
            // reason (scenario 57).
            let abandonment = AgentRunEffectOutcome::Failed {
                code: "reconciliation-abandoned".to_string(),
                message: "the operator abandoned the ambiguous effect".to_string(),
            };
            apply_effect_outcome(state, &bound_effect_id, &abandonment, now)?;
        }
        AgentCheckpointOutcome::Escalated => {
            // The checkpoint stays open and the run stays parked; the escalation
            // target now owns the decision. Nothing else changes.
            state.updated_at = now;
        }
        other => {
            // `Expired` and `Cancelled` are produced by the timer sweep and the
            // run's own cancellation, never by a submitted decision; `resolve`
            // refuses a decision against a terminal checkpoint before this
            // point, so this is unreachable in practice.
            return Err(AgentRunError::Checkpoint(Box::new(
                AgentCheckpointError::InvalidDecision {
                    message: format!("unexpected checkpoint outcome {other:?}"),
                },
            )));
        }
    }

    settle_run_disposition(state, now)
}

/// Schedules the explicitly defined compensation an operator chose for an
/// ambiguous effect, and winds the run down behind it
/// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// The ambiguous generation settles as
/// [`AgentRunEffectStatus::Compensated`]: its outcome was never established —
/// what closes it is the decision that the scheduled compensation, not further
/// evidence, settles the ambiguity. The compensation itself is a new durable
/// effect: committed after the wind-down fence so the fence cannot cancel it,
/// dispatched through the same sink, and blocking terminal settlement until
/// its own outcome arrives.
fn schedule_compensation(
    state: &mut AgentRunState,
    effect_id: &AgentEffectId,
    generation: AgentEffectGeneration,
    compensation: AgentCompensationRef,
    policies: &AgentEffectPolicies,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let scope = state.scope.clone();
    let request = AgentRunEffectRequest::Compensation {
        compensation: compensation.clone(),
        compensated: effect_id.clone(),
        compensated_generation: generation,
    };
    let spec = policies.spec_for(&request).clone();

    let run = state.run_mut()?;
    {
        let Some(effect) = run.loop_state.effect_mut(effect_id) else {
            return Err(AgentRunError::UnknownEffect {
                effect_id: effect_id.clone(),
            });
        };
        if effect.generation != generation {
            return Err(AgentRunError::StaleEffectGeneration {
                effect_id: effect_id.clone(),
                held: effect.generation,
                received: generation,
            });
        }
        if effect.status != AgentRunEffectStatus::Indeterminate {
            return Err(AgentRunError::StaleEffectResult {
                effect_id: effect_id.clone(),
                status: effect.status,
            });
        }
    }

    // The compensation is new spend, reserved exactly like a re-authorized
    // generation's attempts ([specification 9.7](../../../docs/plans/rakka-agent/spec.md));
    // a run that cannot afford it keeps the effect parked `Indeterminate` and
    // refuses the decision, leaving cancellation as the operator's remaining
    // move.
    if let Err(exhaustion) = run
        .loop_state
        .budget_mut()
        .reserve_attempts(spec.max_attempts)
    {
        return Err(AgentRunError::RedispatchUnaffordable { exhaustion });
    }

    if let Some(effect) = run.loop_state.effect_mut(effect_id) {
        effect.status = AgentRunEffectStatus::Compensated;
        effect.last_error_code = Some(bounded_detail("compensated".to_string()));
    }

    // Wind down first, then commit the compensation: the fence cancels only
    // what was pending before it, and the effect recorded after it is exactly
    // the one piece of new work the decision authorizes.
    run.loop_state.fence_unsent_effects();
    if run.terminal_reason.is_none() {
        run.terminal_reason = Some(AgentRunTerminalReason::EffectCompensated {
            effect_id: effect_id.clone(),
            compensation,
        });
    }

    let turn = run.loop_state.turn();
    let slot = run.loop_state.next_effect_slot();
    let settings_revision = run.loop_state.agent_settings_revision();
    let effect = AgentRunEffect::new(&scope, turn, slot, request, &spec, settings_revision, now)?;
    run.loop_state.record_effect(effect)?;
    state.updated_at = now;
    Ok(())
}

/// Fires the durable SLA and expiration timers on every open checkpoint
/// ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// A checkpoint whose SLA deadline has passed escalates and stays open; one whose
/// expiration has passed is denied — the gated effect fails, and the run winds
/// down. A timeout never resolves the checkpoint into a grant: sensitive work no
/// one decided in time fails closed, it does not auto-approve.
fn fire_checkpoint_timers(
    state: &mut AgentRunState,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    {
        let run = state.run_mut()?;
        if run.status.is_terminal() {
            return Ok(());
        }
    }

    // Collect the ids first: applying an expiration mutates the effect list and
    // the checkpoint set, so the sweep works from a stable snapshot of ids.
    let checkpoint_ids: Vec<HumanCheckpointId> = state
        .run()
        .map(|run| {
            run.loop_state
                .open_checkpoints()
                .iter()
                .filter(|checkpoint| checkpoint.status.is_waiting())
                .map(|checkpoint| checkpoint.checkpoint_id.clone())
                .collect()
        })
        .unwrap_or_default();

    for checkpoint_id in &checkpoint_ids {
        let (fired, bound_effect_id, kind) = {
            let run = state.run_mut()?;
            let Some(checkpoint) = run.loop_state.open_checkpoint_mut(checkpoint_id) else {
                continue;
            };
            let bound_effect_id = checkpoint.bound_effect.effect_id.clone();
            let kind = checkpoint.kind;
            (checkpoint.on_timer(now), bound_effect_id, kind)
        };
        match fired {
            AgentCheckpointTimerOutcome::Expired => {
                state.run_mut()?.loop_state.drop_checkpoint(checkpoint_id);
                let expiry = AgentRunEffectOutcome::Failed {
                    code: "checkpoint-expired".to_string(),
                    message: format!("the {kind} checkpoint expired without a decision"),
                };
                apply_effect_outcome(state, &bound_effect_id, &expiry, now)?;
            }
            AgentCheckpointTimerOutcome::Escalated | AgentCheckpointTimerOutcome::Pending => {}
        }
    }

    settle_run_disposition(state, now)
}

/// Applies the task's decision on a result proposal.
///
/// The task's persisted decision is the source of truth for the validation
/// outcome; this is the run's consequence of it, and the run's state is the
/// source of truth for *that*
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
fn settle_proposal(
    state: &mut AgentRunState,
    result: &AgentExchangeResult,
    now: AgentTimestampMillis,
) -> AgentRunResult<()> {
    let cancelling = {
        let run = state.run_mut()?;
        if run.status.is_terminal() {
            // The exchange settles, but a terminal run takes no consequence
            // from a decision that arrives late: nothing here may resurrect it
            // or amend the terminal record it already persisted.
            return Ok(());
        }
        run.status == AgentRunStatus::Cancelling
    };

    let decision: AgentTaskDecision =
        match result.payload().decode(AGENT_TASK_DECISION_PAYLOAD_TYPE) {
            Ok(decision) => decision,
            Err(error) => {
                // `AgentRunParticipant::check_settle` refuses an undecodable
                // decision before the exchange settles, so this branch is the
                // unreachable last resort of a settle that may not fail: the
                // run stops rather than guess. Guessing "accepted" would
                // complete a task the rules may have refused; guessing
                // "rejected" would burn an iteration the task never charged.
                return terminate(
                    state,
                    AgentRunTerminalReason::UndecodableDecision {
                        code: error.code().to_string(),
                    },
                    now,
                );
            }
        };

    match decision {
        AgentTaskDecision::Accepted { result } => {
            // Acceptance wins the race with a wind-down: the task durably
            // completed on this result, and the run records that truthfully
            // rather than holding an accepted result under a cancelled status.
            let run = state.run_mut()?;
            run.loop_state.set_accepted_result(*result);
            terminate(state, AgentRunTerminalReason::ResultAccepted, now)
        }
        AgentTaskDecision::Rejected {
            feedback,
            remaining_iterations,
            ..
        } => {
            if cancelling {
                // A rejection would begin another turn, and a cancelling run
                // takes no further turn: the decision settles the outstanding
                // proposal, and the wind-down finishes with the reason the
                // operator recorded.
                let reason = state.run_mut()?.terminal_reason.clone().unwrap_or(
                    AgentRunTerminalReason::CancellationRequested {
                        reason: "cancelled".to_string(),
                    },
                );
                return terminate(state, reason, now);
            }
            if remaining_iterations == 0 {
                // The task's rejection budget is spent, and the task has failed.
                // The run never silently accepts the proposal its rules refused
                // ([specification 9.2]).
                return terminate(
                    state,
                    AgentRunTerminalReason::ResultRejectionsExhausted,
                    now,
                );
            }
            let run = state.run_mut()?;
            run.loop_state.begin_turn(Some(bounded_detail(feedback)));
            run.status = AgentRunStatus::Running;
            state.updated_at = now;
            Ok(())
        }
        AgentTaskDecision::Refused { code, status } => {
            // A refusal is not a validation decision: the task would not even
            // evaluate the proposal, because the run is fenced by a newer
            // generation or the task has moved on. There is nothing for the run
            // to correct, so it stops with the task's own reason on record.
            terminate(
                state,
                if code == AGENT_TASK_REFUSAL_STALE_GENERATION {
                    AgentRunTerminalReason::Superseded
                } else {
                    AgentRunTerminalReason::TaskRefusedProposal {
                        code: bounded_detail(code),
                        status,
                    }
                },
                now,
            )
        }
    }
}

/// The domain half of the run entity.
///
/// It supplies bounded, pure transitions and nothing else; the choreography
/// substrate owns durability, deduplication, re-drive, and routing.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentRunParticipant;

impl AgentExchangeParticipant for AgentRunParticipant {
    type State = AgentRunState;

    fn initialize(&self, address: &AgentEntityAddress, now: AgentTimestampMillis) -> Self::State {
        let scope = match address {
            AgentEntityAddress::Run(scope) => scope.clone(),
            // The host builds a participant for the address it serves, and the
            // entity refuses an id that does not parse into a run scope, so this
            // is unreachable in practice. Panicking would take down a shard owner
            // over a routing bug; an unassigned run under an address that can
            // never receive an assignment is inert instead.
            other => AgentRunScope::new(
                other.tenant().clone(),
                unroutable_agent_id(),
                unroutable_run_id(),
            )
            .expect("the unroutable run scope is well formed"),
        };
        AgentRunState::unassigned(scope, now)
    }

    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        let result = match envelope.kind() {
            AgentExchangeKind::Assignment => accept_assignment(state, envelope, now),
            kind => refuse(
                "unsupported-exchange",
                format!("a run entity does not receive a {kind} exchange"),
            ),
        };
        AgentExchangeTransition::new(result)
    }

    fn check_settle(
        &self,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
    ) -> Result<(), AgentChoreographyError> {
        match envelope.kind() {
            AgentExchangeKind::ResultProposal => {
                // A decision this binary cannot decode is refused *before* the
                // exchange settles, so it stays outstanding and is re-driven
                // later — a rolling upgrade must not turn a valid durable
                // decision into a terminally failed run. The task returns the
                // same decision on every re-drive, so an owner that can decode
                // it converges.
                result
                    .payload()
                    .decode::<AgentTaskDecision>(AGENT_TASK_DECISION_PAYLOAD_TYPE)
                    .map(|_| ())
            }
            AgentExchangeKind::BudgetAllocation if result.is_accepted() => {
                // Same rule for a top-up grant: an accepted outcome this binary
                // cannot decode stays outstanding rather than being read as a
                // grant of nothing, which would fail a run the parent actually
                // funded.
                result
                    .payload()
                    .decode::<AgentBudgetLedgerOutcome>(AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE)
                    .map(|_| ())
            }
            kind @ (AgentExchangeKind::BudgetSettlement | AgentExchangeKind::BudgetReturn)
                if !result.is_accepted() =>
            {
                // A rejected settlement or return settles only when the refusal
                // is the ledger's own replay answer: `escrow-child-unknown`
                // proves the escrow was already settled and returned, so this
                // step is done. Any other refusal — an `unsupported-exchange`
                // from a task owner that predates the ledger, a payload it
                // could not decode — is the receiver's inability, not the
                // ledger answering. Settling on it would mark the run
                // `Settled`/`Returned` while the task never recorded the
                // consumption, leaking the child's escrow forever; the exchange
                // stays outstanding instead and is re-driven until an owner
                // that can answer it does.
                match result.status().rejection_code() {
                    Some(AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN) => Ok(()),
                    code => Err(AgentChoreographyError::UnsettleableRefusal {
                        kind,
                        code: code.unwrap_or_default().to_string(),
                    }),
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
        match envelope.kind() {
            AgentExchangeKind::ResultProposal => {
                // A settle may not fail: the exchange is already settled in the
                // same compare-and-set, and there is no one to report a failure
                // to. Every path inside `settle_proposal` therefore ends in a
                // durable decision, including the one where the decision cannot
                // be decoded — which `check_settle` above makes unreachable in
                // practice.
                let _terminated = settle_proposal(state, result, now);
            }
            kind @ (AgentExchangeKind::BudgetSettlement | AgentExchangeKind::BudgetReturn) => {
                // A rejection that reaches here passed `check_settle`, so it is
                // the ledger's own replay answer — the escrow already closed —
                // and advancing on it is the point: a run that treated it as
                // unfinished business would re-drive it forever. Either way the
                // parent's ledger holds the truth, and this step is done.
                settle_ledger_exchange(state, kind, now);
            }
            AgentExchangeKind::BudgetAllocation => {
                // The reply to a top-up request: credit and resume, or stop with
                // the original exhaustion.
                apply_top_up_grant(state, result, now);
            }
            _ => {}
        }
        owed_ledger_exchange(state, now).unwrap_or_default()
    }
}

fn unroutable_agent_id() -> AgentId {
    AgentId::new("unroutable").expect("the literal is a valid agent id")
}

fn unroutable_run_id() -> AgentRunId {
    AgentRunId::new("unroutable").expect("the literal is a valid run id")
}

/// The durable facade over one run entity.
///
/// It owns the three things a bounded transition cannot do for itself: cranking
/// the loop one compare-and-set at a time, handing the effects the loop
/// committed to their sink, and driving the exchanges it owes. Every decision is
/// still a pure transition, and the actor is a thin shell over this type.
pub struct AgentRunEntityStore<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    scope: AgentRunScope,
    host: AgentExchangeHost<AgentRunParticipant, Store>,
    effects: Effects,
    policies: AgentEffectPolicies,
    recovered: bool,
}

impl<Store, Effects> Debug for AgentRunEntityStore<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRunEntityStore")
            .field("scope", &self.scope)
            .field("effects", &self.effects.backend_name())
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}

impl<Store, Effects> AgentRunEntityStore<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    /// Creates a durable facade for one run scope.
    #[must_use]
    pub fn new(scope: AgentRunScope, store: Store, effects: Effects) -> Self {
        let host = AgentExchangeHost::new(
            AgentEntityAddress::Run(scope.clone()),
            AgentRunParticipant,
            store,
        );
        Self {
            scope,
            host,
            effects,
            policies: AgentEffectPolicies::default(),
            recovered: false,
        }
    }

    /// Uses an explicit schema-compatibility policy.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.host = self.host.with_schema_policy(policy);
        self
    }

    /// Uses explicit effect specs for the effects the loop commits
    /// ([specification 11.2](../../../docs/plans/rakka-agent/spec.md): the
    /// registration supplies the permitted safety declaration).
    #[must_use]
    pub fn with_effect_policies(mut self, policies: AgentEffectPolicies) -> Self {
        self.policies = policies;
        self
    }

    /// The scope this facade addresses.
    #[must_use]
    pub const fn scope(&self) -> &AgentRunScope {
        &self.scope
    }

    /// The durable persistence id of this run's state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        self.scope.persistence_id()
    }

    /// Loads the run's durable state, failing closed on an unsupported schema
    /// version.
    pub async fn recover(&mut self, now: AgentTimestampMillis) -> AgentRunResult<&AgentRunState> {
        let state = self.host.recover(now).await?;
        self.recovered = true;
        Ok(state)
    }

    /// The currently recovered state.
    pub fn state(&self) -> AgentRunResult<&AgentRunState> {
        Ok(self.host.state()?)
    }

    /// The bounded projection of the run, once it has accepted an assignment.
    pub fn snapshot(&self) -> AgentRunResult<Option<AgentRunSnapshot>> {
        Ok(self.state()?.snapshot())
    }

    /// Applies one command, then settles whatever it made possible.
    ///
    /// # Errors
    ///
    /// An error does not prove the command was not applied: the transition
    /// commits before the settle pass, so a settlement failure — an effect-sink
    /// outage, a delivery fault — surfaces here after the command has durably
    /// applied. Retrying with the same operation id is always safe; a command
    /// that committed answers [`AgentRunEntityReply::Duplicate`] with its
    /// original outcome rather than transitioning twice.
    pub async fn apply(
        &mut self,
        command: AgentRunEntityCommand,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentRunResult<AgentRunEntityReply> {
        self.ensure_recovered(now).await?;

        if let Some(operation_id) = command.operation_id() {
            if let Some(outcome) = self
                .state()?
                .applied_operations
                .outcome(operation_id)
                .cloned()
            {
                // The retry contract sends the same operation id after a
                // settlement failure, so the replay must re-drive the settle
                // pass the committed transition still owes — answering from
                // the log alone would leave the run stalled on work it durably
                // recorded but never dispatched.
                self.settle_side_effects(router, now).await?;
                return Ok(AgentRunEntityReply::Duplicate { outcome });
            }
        }

        let reply = match command {
            AgentRunEntityCommand::Describe => {
                return Ok(AgentRunEntityReply::Snapshot(
                    self.snapshot()?.map(Box::new),
                ))
            }
            AgentRunEntityCommand::RecordEffectResult {
                operation_id,
                effect_id,
                generation,
                attempt,
                fence,
                outcome,
            } => {
                let policy = *self.host.schema_policy();
                let policies = self.policies.clone();
                self.transition(now, move |state| {
                    record_effect_result(
                        state, &effect_id, generation, attempt, fence, *outcome, &policy,
                        &policies, now,
                    )?;
                    Ok(operation_id)
                })
                .await?
            }
            AgentRunEntityCommand::ResolveIndeterminateEffect {
                operation_id,
                effect_id,
                generation,
                resolution,
            } => {
                let policy = *self.host.schema_policy();
                self.transition(now, move |state| {
                    resolve_indeterminate_effect(
                        state,
                        &effect_id,
                        generation,
                        *resolution,
                        &policy,
                        now,
                    )?;
                    Ok(operation_id)
                })
                .await?
            }
            AgentRunEntityCommand::ResolveCheckpoint {
                operation_id,
                checkpoint_id,
                resolver,
                decision,
            } => {
                let policy = *self.host.schema_policy();
                let policies = self.policies.clone();
                self.transition(now, move |state| {
                    resolve_checkpoint(
                        state,
                        &checkpoint_id,
                        resolver,
                        *decision,
                        operation_id.clone(),
                        &policy,
                        &policies,
                        now,
                    )?;
                    Ok(operation_id)
                })
                .await?
            }
            AgentRunEntityCommand::FireCheckpointTimers { operation_id } => {
                self.transition(now, move |state| {
                    fire_checkpoint_timers(state, now)?;
                    Ok(operation_id)
                })
                .await?
            }
            AgentRunEntityCommand::Cancel {
                operation_id,
                reason,
            } => {
                self.transition(now, move |state| {
                    cancel(state, reason, now)?;
                    Ok(operation_id)
                })
                .await?
            }
        };

        self.settle_side_effects(router, now).await?;
        Ok(reply)
    }

    /// Accepts one delivered exchange, then settles what it made possible.
    pub async fn accept(
        &mut self,
        envelope: &AgentExchangeEnvelope,
        _router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentRunResult<AgentExchangeReply> {
        self.ensure_recovered(now).await?;
        let reply = self.host.accept(envelope, now).await?;
        // Accepting a delivered exchange makes *local* progress only: it cranks
        // the loop and hands new effects to the sink. It does **not** deliver the
        // cross-entity exchanges that crank may have committed to the run's
        // journal (a result proposal, a budget settlement). Those are drained by
        // the courier — a command's settle pass, a recovery sweep, `pump` — never
        // synchronously from inside a delivery.
        //
        // Deferring the delivery is what keeps the choreography acyclic. The
        // initiator of `envelope` is, by definition, mid-delivery to this run
        // right now; driving an owed exchange back to it here would re-enter its
        // `accept` before this reply has settled, and a run that owes a
        // settlement to the very task that is re-driving its assignment would
        // recurse without bound. Committing to the journal and returning the
        // reply lets the initiator settle first, exactly as a durable outbox
        // drains on a later turn rather than inside the transition that filled
        // it.
        self.make_local_progress(now).await?;
        Ok(reply)
    }

    /// Cranks the loop and dispatches effects, without delivering any owed
    /// cross-entity exchange.
    ///
    /// This is the half of [`Self::settle_side_effects`] that touches only the
    /// run's own state and the effect sink. It is what a delivered exchange is
    /// allowed to trigger; see [`Self::accept`] for why the drive half is not.
    async fn make_local_progress(&mut self, now: AgentTimestampMillis) -> AgentRunResult<()> {
        for _round in 0..AGENT_RUN_MAX_SETTLE_ROUNDS {
            let advanced = self.advance_loop(now).await?;
            let dispatched = self.dispatch_effects(now).await?;
            if advanced == 0 && dispatched == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Cranks the loop from durable state alone: advance, dispatch, drive.
    ///
    /// It is safe to call at any time and from any node, because every step reads
    /// what it needs from the durable record. Calling it after a transition, after
    /// recovery, or from a sweep are the same operation — which is exactly what
    /// makes a run that was lost between persisting a wait and dispatching the
    /// effect it waits on recoverable.
    ///
    /// Each *advance* is one bounded transition and one compare-and-set, and the
    /// pass stops as soon as the loop reaches a durable wait. Both fences —
    /// [`AGENT_RUN_MAX_LOOP_STEPS_PER_PASS`] and [`AGENT_RUN_MAX_SETTLE_ROUNDS`]
    /// — are there so the handler's work is bounded by construction and not only
    /// by argument ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)).
    pub async fn settle_side_effects(
        &mut self,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentRunResult<AgentRunProgress> {
        self.ensure_recovered(now).await?;

        let mut progress = AgentRunProgress::default();
        for _round in 0..AGENT_RUN_MAX_SETTLE_ROUNDS {
            let advanced = self.advance_loop(now).await?;
            let dispatched = self.dispatch_effects(now).await?;
            let report = drive_pending_exchanges(&mut self.host, router, now).await?;

            progress.transitions += advanced;
            progress.effects_dispatched += dispatched;
            progress.settled += report.settled;
            progress.failed += report.failed;

            if advanced == 0 && dispatched == 0 && report.settled == 0 {
                break;
            }
        }

        progress.outstanding = self.host.outstanding()?.len();
        Ok(progress)
    }

    /// Advances the loop one bounded transition at a time until it reaches a
    /// durable wait.
    async fn advance_loop(&mut self, now: AgentTimestampMillis) -> AgentRunResult<usize> {
        let mut transitions = 0;
        for _step in 0..AGENT_RUN_MAX_LOOP_STEPS_PER_PASS {
            let can_advance = self.state()?.run().is_some_and(AgentRun::can_advance);
            if !can_advance {
                break;
            }

            let mut rejection = None;
            let policies = self.policies.clone();
            let committed = self
                .host
                .initiate(now, |state| match advance_once(state, &policies, now) {
                    Ok(owed) => Ok(owed),
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
            transitions += 1;
        }
        Ok(transitions)
    }

    /// Makes the loop's committed effects dispatchable, then hands every
    /// dispatchable, unresolved effect to the sink.
    ///
    /// The ordering is the fence's foundation: the transition that marks an
    /// effect `Ready` commits **before** any sink write for it starts, so a
    /// `Pending` effect has provably never reached the outbox and a
    /// cancellation can fence it in place with nothing abandoned
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)). The
    /// price is that `Ready` does not prove the sink write landed — so the
    /// flush re-drives the idempotent write for every `Ready` effect until a
    /// result resolves it. The sink deduplicates on the dispatch ticket id,
    /// which is why neither a crash between the two steps nor shard movement
    /// can dispatch one generation twice
    /// ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A run that is winding down — cancelled, or stopped by a failed effect —
    /// flushes nothing: handing a fenced run's effect to the outbox would be
    /// exactly the new dispatch the fence forbids. Work that already reached
    /// the dispatch layer settles truthfully there.
    async fn dispatch_effects(&mut self, now: AgentTimestampMillis) -> AgentRunResult<usize> {
        let (terminal, winding_down) = {
            let state = self.state()?;
            (
                state.status().is_none_or(AgentRunStatus::is_terminal),
                state.run().is_some_and(|run| run.terminal_reason.is_some())
                    || state.status() == Some(AgentRunStatus::Cancelling),
            )
        };
        if terminal {
            return Ok(0);
        }

        // Only a *dispatchable* pending effect is made ready: a checkpoint-gated
        // effect with no grant stays `Pending`, parked behind its approval
        // checkpoint, and never reaches the sink until a resolution stores the
        // grant ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
        //
        // A run that is winding down flushes only a compensation effect: handing
        // any other fenced work to the outbox would be exactly the new dispatch
        // the fence forbids, while the compensation is the one piece of new work
        // an operator's `Compensate` decision explicitly authorized after the
        // fence ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
        let dispatchable: Vec<AgentEffectId> = self
            .state()?
            .loop_state()
            .map(|loop_state| {
                loop_state
                    .undispatched_effects()
                    .into_iter()
                    .filter(|effect| {
                        loop_state.is_dispatchable(effect)
                            && (!winding_down
                                || effect.kind() == AgentRunEffectKind::CompensationCall)
                    })
                    .map(|effect| effect.effect_id)
                    .collect()
            })
            .unwrap_or_default();

        if !dispatchable.is_empty() {
            self.host
                .initiate(now, |state| {
                    if let Some(run) = state.run.as_mut() {
                        for effect_id in &dispatchable {
                            if let Some(effect) = run.loop_state.effect_mut(effect_id) {
                                if effect.is_pending() {
                                    effect.mark_ready(now);
                                }
                            }
                        }
                    }
                    state.updated_at = now;
                    Ok(Vec::new())
                })
                .await?;
        }

        let ready = self
            .state()?
            .loop_state()
            .map(AgentLoopState::ready_effects)
            .unwrap_or_default();
        for effect in &ready {
            if winding_down && effect.kind() != AgentRunEffectKind::CompensationCall {
                continue;
            }
            let record = effect.to_workflow_effect(&self.scope);
            self.effects.dispatch(&self.scope, &record).await?;
        }

        Ok(dispatchable.len())
    }

    /// Runs one bounded command transition and records its resolved operation id
    /// in the same compare-and-set.
    ///
    /// A rejected transition never reaches the store, so it leaves no trace in the
    /// operation log and a corrected retry under the same operation id is still
    /// accepted.
    async fn transition<F>(
        &mut self,
        now: AgentTimestampMillis,
        transition: F,
    ) -> AgentRunResult<AgentRunEntityReply>
    where
        F: FnOnce(&mut AgentRunState) -> AgentRunResult<AgentOperationId>,
    {
        let mut outcome = None;
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let apply =
                    |state: &mut AgentRunState| -> AgentRunResult<Vec<AgentExchangeEnvelope>> {
                        let operation_id = transition(state)?;
                        let result = state.outcome();
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
                        state.updated_at = now;
                        outcome = Some(result);
                        owed_ledger_exchange(state, now)
                    };

                match apply(state) {
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
        Ok(AgentRunEntityReply::Applied {
            outcome: outcome.expect("an accepted transition produces an outcome"),
        })
    }

    async fn ensure_recovered(&mut self, now: AgentTimestampMillis) -> AgentRunResult<()> {
        if !self.recovered || self.host.state().is_err() {
            self.recover(now).await?;
        }
        Ok(())
    }
}

/// What one pass of the run entity's settlement did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunProgress {
    /// How many bounded loop transitions it committed.
    pub transitions: usize,
    /// How many effects it handed to the sink.
    pub effects_dispatched: usize,
    /// How many exchanges it settled.
    pub settled: usize,
    /// How many delivery attempts failed, leaving their exchange outstanding.
    pub failed: usize,
    /// How many exchanges the run still owes.
    pub outstanding: usize,
}

/// The serializable command protocol of the run entity.
///
/// Nothing in it is an `Arc` or an in-process reply channel: the protocol is
/// serializable from this first commit, so no later slice has to retrofit
/// remoting into an entity whose commands cannot cross a node boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEntityCommand {
    /// The durable result the dispatcher returned for one effect generation
    /// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md): the
    /// dispatcher performs bounded I/O and returns a durable result command
    /// through the inbox). It carries the effect id, generation, attempt, and
    /// lease fence, as [specification 11.4](../../../docs/plans/rakka-agent/spec.md)
    /// requires; the run refuses a result whose generation it has passed.
    RecordEffectResult {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The effect the result answers.
        effect_id: AgentEffectId,
        /// The generation the result answers for.
        generation: AgentEffectGeneration,
        /// The dispatch attempt that produced the result.
        attempt: u32,
        /// The lease fencing token of that attempt; zero for an in-process
        /// driver that holds no fleet lease.
        fence: u64,
        /// What the dispatcher found.
        outcome: Box<AgentRunEffectOutcome>,
    },
    /// An explicit reconciliation decision on an indeterminate effect
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md),
    /// [12.5](../../../docs/plans/rakka-agent/spec.md)). Slice 1.10 wraps this
    /// command in the reconciliation checkpoint; the effect-layer semantics
    /// live here.
    ResolveIndeterminateEffect {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The parked effect.
        effect_id: AgentEffectId,
        /// The generation the decision resolves.
        generation: AgentEffectGeneration,
        /// The decision.
        resolution: Box<AgentEffectResolution>,
    },
    /// An authenticated decision on an approval or authorization checkpoint the
    /// run is waiting on
    /// ([specification 12](../../../docs/plans/rakka-agent/spec.md)). An
    /// `Approve`/`Grant` stores the digest-bound grant and resumes the run; a
    /// `Deny` fails the gated effect. Duplicate submissions deduplicate on the
    /// operation id and never resume the run twice.
    ResolveCheckpoint {
        /// The stable operation id this command deduplicates on; it is also the
        /// checkpoint's decision key.
        operation_id: AgentOperationId,
        /// The checkpoint being resolved.
        checkpoint_id: HumanCheckpointId,
        /// The authenticated principal that resolved it.
        resolver: PrincipalRef,
        /// The decision.
        decision: Box<AgentCheckpointDecision>,
    },
    /// Fire the durable SLA and expiration timers on the run's open checkpoints
    /// ([specification 12.6](../../../docs/plans/rakka-agent/spec.md)). A
    /// scheduler delivers it at a checkpoint's due or expiration instant; it can
    /// only escalate or expire a waiting checkpoint, never auto-approve one.
    FireCheckpointTimers {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
    },
    /// Request cancellation of the run.
    Cancel {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// A bounded, stable reason.
        reason: String,
    },
    /// Read the run's bounded durable projection.
    Describe,
}

impl AgentRunEntityCommand {
    /// The operation id this command deduplicates on, when it mutates state.
    #[must_use]
    pub const fn operation_id(&self) -> Option<&AgentOperationId> {
        match self {
            Self::RecordEffectResult { operation_id, .. }
            | Self::ResolveIndeterminateEffect { operation_id, .. }
            | Self::ResolveCheckpoint { operation_id, .. }
            | Self::FireCheckpointTimers { operation_id }
            | Self::Cancel { operation_id, .. } => Some(operation_id),
            Self::Describe => None,
        }
    }
}

/// The serializable reply protocol of the run entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRunEntityReply {
    /// The command transitioned the run.
    Applied {
        /// The outcome of the transition.
        outcome: AgentRunOutcome,
    },
    /// The operation id was already applied; this is the original outcome, and no
    /// second transition happened.
    Duplicate {
        /// The outcome the original application produced.
        outcome: AgentRunOutcome,
    },
    /// The run's bounded durable projection, absent if it never accepted an
    /// assignment.
    Snapshot(Option<Box<AgentRunSnapshot>>),
    /// What one settlement pass did.
    Progressed {
        /// The pass's report.
        progress: AgentRunProgress,
    },
    /// The command was rejected.
    ///
    /// A rejection is not proof the command did not apply: a settlement failure
    /// after the transition committed reaches the caller as this reply too.
    /// Retrying with the same operation id is always safe.
    Rejected {
        /// Stable machine-readable error code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentRunEntityReply {
    fn rejected(error: &AgentRunError) -> Self {
        Self::Rejected {
            code: error.code().to_string(),
            message: error.to_string(),
        }
    }
}

/// The process-local message the run entity accepts.
///
/// The reply channels never cross a node boundary:
/// [`init_agent_run_entity_remote_sharding`] reconstructs the exchange arm on the
/// owning node from the [`AgentExchangeEnvelope`] that arrived over
/// `rakka-remote`, which is the surface every cross-entity command travels.
pub enum AgentRunEntityMessage {
    /// A dispatcher result or an administrative command.
    Command {
        /// The command to apply.
        command: Box<AgentRunEntityCommand>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentRunEntityReply>,
    },
    /// A cross-entity exchange: the task's assignment.
    Exchange {
        /// The exchange to apply.
        envelope: Box<AgentExchangeEnvelope>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentExchangeReply>,
    },
    /// Crank the loop, dispatch owed effects, and drive owed exchanges.
    ///
    /// The entity does this itself after every transition. The command exists so
    /// that a recovery sweep or a test can drive a run that was lost between
    /// persisting what it owed and delivering it.
    Settle {
        /// Where the reply goes.
        reply_to: ReplyTo<AgentRunEntityReply>,
    },
}

impl Debug for AgentRunEntityMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Command { command, .. } => f
                .debug_struct("AgentRunEntityMessage::Command")
                .field("command", command)
                .finish_non_exhaustive(),
            Self::Exchange { envelope, .. } => f
                .debug_struct("AgentRunEntityMessage::Exchange")
                .field("envelope", envelope)
                .finish_non_exhaustive(),
            Self::Settle { .. } => f
                .debug_struct("AgentRunEntityMessage::Settle")
                .finish_non_exhaustive(),
        }
    }
}

/// The actor-backed host of one sharded run entity.
///
/// The actor is a routing and recovery shell: every decision lives in
/// [`AgentRunEntityStore`] and every durable fact lives in the state store, so
/// the entity can passivate after any message and recover on another pod
/// ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
pub struct AgentRunEntity<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    entity: Result<AgentRunEntityStore<Store, Effects>, AgentIdentityError>,
    router: AgentExchangeRouter,
    clock: AgentRunClock,
}

impl<Store, Effects> AgentRunEntity<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    /// Creates an entity for one sharded entity id.
    #[must_use]
    pub fn new(
        entity_id: &EntityId,
        store: Store,
        effects: Effects,
        router: AgentExchangeRouter,
        clock: AgentRunClock,
        policy: AgentSchemaPolicy,
        effect_policies: AgentEffectPolicies,
    ) -> Self {
        let entity = AgentRunScope::from_entity_id(entity_id).map(|scope| {
            AgentRunEntityStore::new(scope, store, effects)
                .with_schema_policy(policy)
                .with_effect_policies(effect_policies)
        });
        Self {
            entity,
            router,
            clock,
        }
    }

    fn store(&mut self) -> Result<&mut AgentRunEntityStore<Store, Effects>, AgentRunError> {
        self.entity
            .as_mut()
            .map_err(|error| AgentRunError::Identity(error.clone()))
    }
}

impl<Store, Effects> Actor for AgentRunEntity<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    type Msg = AgentRunEntityMessage;

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
                AgentRunEntityMessage::Command { command, reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentRunEntityReply::rejected(&error),
                        Ok(entity) => match entity.apply(*command, &router, now).await {
                            Ok(reply) => reply,
                            Err(error) => AgentRunEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
                AgentRunEntityMessage::Exchange { envelope, reply_to } => {
                    let Ok(entity) = self.store() else {
                        // A misrouted entity cannot answer an exchange. Dropping
                        // the reply leaves it outstanding on its initiator, which
                        // re-drives it — exactly what a lost delivery does, and
                        // what the substrate is built to converge from.
                        return Ok(ActorAction::Continue);
                    };
                    if let Ok(reply) = entity.accept(&envelope, &router, now).await {
                        let _reply_dropped = reply_to.reply(reply);
                    }
                }
                AgentRunEntityMessage::Settle { reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentRunEntityReply::rejected(&error),
                        Ok(entity) => match entity.settle_side_effects(&router, now).await {
                            Ok(progress) => AgentRunEntityReply::Progressed { progress },
                            Err(error) => AgentRunEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// The entity type key of the run entity.
pub type AgentRunEntityTypeKey = EntityTypeKey<AgentRunEntityMessage>;

/// The registration returned after initializing sharded run entities.
pub type AgentRunEntityRegistration = EntityTypeRegistration<AgentRunEntityMessage>;

/// A sharded reference to one run entity.
pub type AgentRunEntityRef = ShardedEntityRef<AgentRunEntityMessage>;

/// The sharding settings of run entities.
#[derive(Clone)]
pub struct AgentRunEntityShardingSettings {
    key: AgentRunEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    schema_policy: AgentSchemaPolicy,
    effect_policies: AgentEffectPolicies,
    clock: AgentRunClock,
}

impl Debug for AgentRunEntityShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRunEntityShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("schema_policy", &self.schema_policy)
            .finish_non_exhaustive()
    }
}

impl AgentRunEntityShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentRunEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_RUN_PASSIVATION_BUFFER_DURATION,
            schema_policy: AgentSchemaPolicy::default(),
            effect_policies: AgentEffectPolicies::default(),
            clock: system_run_clock(),
        }
    }

    /// The entity type key used for run entities.
    #[must_use]
    pub const fn key(&self) -> &AgentRunEntityTypeKey {
        &self.key
    }

    /// Uses an explicit clock for the timestamps hosted entities persist.
    #[must_use]
    pub fn with_clock(mut self, clock: AgentRunClock) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the options used when each run entity actor is spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for quiescent run entities.
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

    /// Uses explicit effect specs for the effects hosted runs commit.
    #[must_use]
    pub fn with_effect_policies(mut self, policies: AgentEffectPolicies) -> Self {
        self.effect_policies = policies;
        self
    }
}

impl Default for AgentRunEntityShardingSettings {
    fn default() -> Self {
        Self::new(agent_run_entity_type_key())
    }
}

/// Creates the default sharded entity type key for run entities.
#[must_use]
pub fn agent_run_entity_type_key() -> AgentRunEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_RUN_ENTITY_TYPE)
}

/// Maps a run scope to its sharded entity id.
#[must_use]
pub fn agent_run_entity_id(scope: &AgentRunScope) -> EntityId {
    scope.entity_id()
}

/// The durable persistence id of one run entity's state.
#[must_use]
pub fn agent_run_entity_persistence_id(scope: &AgentRunScope) -> PersistenceId {
    scope.persistence_id()
}

/// Initializes node-local sharded run entities.
pub fn init_agent_run_entity_sharding<Store, Effects>(
    sharding: &ClusterSharding,
    store: Store,
    effects: Effects,
    router: AgentExchangeRouter,
    settings: AgentRunEntityShardingSettings,
) -> ClusterShardingResult<AgentRunEntityRegistration>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    sharding.init(agent_run_entity(store, effects, router, &settings))
}

/// Initializes sharded run entities that a non-owning node can reach over
/// `rakka-remote`.
///
/// The remote ask surface is the [`AgentExchangeEnvelope`], because that is what
/// every cross-entity command is: the task's assignment arrives as an exchange,
/// and [`crate::choreography::ShardedExchangeRoute`] delivers it to the owning
/// node unchanged.
pub fn init_agent_run_entity_remote_sharding<Store, Effects>(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    store: Store,
    effects: Effects,
    router: AgentExchangeRouter,
    settings: AgentRunEntityShardingSettings,
) -> ClusterNodeRuntimeResult<AgentRunEntityRegistration>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    let entity = agent_run_entity(store, effects, router, &settings);
    sharding.init_remote_with_ask(
        runtime,
        entity,
        |envelope: AgentExchangeEnvelope, reply_to: ReplyTo<AgentExchangeReply>| {
            AgentRunEntityMessage::Exchange {
                envelope: Box::new(envelope),
                reply_to,
            }
        },
    )
}

// The run entity is generic over its own state store and its effect sink, so the
// entity type it builds is unavoidably wide.
#[allow(clippy::type_complexity)]
fn agent_run_entity<Store, Effects>(
    store: Store,
    effects: Effects,
    router: AgentExchangeRouter,
    settings: &AgentRunEntityShardingSettings,
) -> Entity<
    AgentRunEntityMessage,
    AgentRunEntity<Store, Effects>,
    impl Fn(EntityContext<AgentRunEntityMessage>) -> AgentRunEntity<Store, Effects>
        + Send
        + Sync
        + 'static,
>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    let schema_policy = settings.schema_policy;
    let effect_policies = settings.effect_policies.clone();
    let clock = settings.clock.clone();
    let mut entity = Entity::of(settings.key.clone(), move |context: EntityContext<_>| {
        AgentRunEntity::new(
            context.entity_id(),
            store.clone(),
            effects.clone(),
            router.clone(),
            clock.clone(),
            schema_policy,
            effect_policies.clone(),
        )
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

/// Returns a sharded reference to one run entity.
pub fn agent_run_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentRunEntityTypeKey,
    scope: &AgentRunScope,
) -> ClusterShardingResult<AgentRunEntityRef> {
    sharding.entity_ref_for(key, scope.key())
}

/// Returns a sharded reference to one run entity from a registration.
#[must_use]
pub fn registered_agent_run_entity_ref(
    registration: &AgentRunEntityRegistration,
    scope: &AgentRunScope,
) -> AgentRunEntityRef {
    registration.entity_ref_for(scope.key())
}

/// Explicitly passivates one local run entity.
pub fn passivate_agent_run_entity(
    sharding: &ClusterSharding,
    key: &AgentRunEntityTypeKey,
    scope: &AgentRunScope,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &scope.entity_id())
}

/// The rejection of a run operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentRunError {
    /// An identifier or scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The choreography substrate rejected an exchange.
    Choreography(Box<AgentChoreographyError>),
    /// The durable store rejected a load or write.
    Persistence(DurableError),
    /// An effect could not be committed or dispatched.
    Effect(Box<AgentEffectError>),
    /// A model turn could not be bounded.
    Model(Box<AgentModelError>),
    /// The task contract carried by an assignment was rejected.
    Task(Box<AgentTaskError>),
    /// The run has not accepted an assignment.
    NotAccepted {
        /// The scope of the run.
        scope: AgentRunScope,
    },
    /// The run is terminal and accepts no further transition.
    Terminal {
        /// Its terminal status.
        status: AgentRunStatus,
    },
    /// A result arrived for an effect the run does not hold.
    UnknownEffect {
        /// The effect the result named.
        effect_id: AgentEffectId,
    },
    /// A checkpoint decision could not be applied.
    Checkpoint(Box<AgentCheckpointError>),
    /// A decision arrived for a checkpoint the run does not hold, or has already
    /// resolved and dropped.
    UnknownCheckpoint {
        /// The checkpoint the decision named.
        checkpoint_id: HumanCheckpointId,
    },
    /// A result arrived for an effect the run has already resolved.
    StaleEffectResult {
        /// The effect the result named.
        effect_id: AgentEffectId,
        /// The status the run holds it under.
        status: AgentRunEffectStatus,
    },
    /// A result arrived for a generation the run does not hold.
    StaleEffectGeneration {
        /// The effect the result named.
        effect_id: AgentEffectId,
        /// The generation the run holds.
        held: AgentEffectGeneration,
        /// The generation the result carried.
        received: AgentEffectGeneration,
    },
    /// A reconciliation authorized a re-invocation the run's budget cannot
    /// afford: the new generation's attempt bound could not be reserved
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The resolution is refused and the effect stays parked `Indeterminate`;
    /// the operator's remaining decision is to cancel the run, whose wind-down
    /// settles the generation without invocation.
    RedispatchUnaffordable {
        /// The ceiling the reservation would cross.
        exhaustion: AgentBudgetExhaustion,
    },
    /// The materialized run record would exceed its bound.
    MaterializedStateTooLarge {
        /// The size of the rejected record, in bytes.
        bytes: usize,
        /// The maximum accepted size, in bytes.
        maximum: usize,
    },
}

impl AgentRunError {
    /// The stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Choreography(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::Effect(error) => error.code(),
            Self::Model(error) => error.code(),
            Self::Task(error) => error.code(),
            Self::NotAccepted { .. } => "run-not-accepted",
            Self::Terminal { .. } => "run-terminal",
            Self::UnknownEffect { .. } => "run-unknown-effect",
            Self::Checkpoint(error) => error.code(),
            Self::UnknownCheckpoint { .. } => "run-unknown-checkpoint",
            Self::StaleEffectResult { .. } => "run-stale-effect-result",
            Self::StaleEffectGeneration { .. } => "run-stale-effect-generation",
            Self::RedispatchUnaffordable { .. } => "run-redispatch-unaffordable",
            Self::MaterializedStateTooLarge { .. } => "run-state-too-large",
        }
    }
}

impl Display for AgentRunError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Choreography(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::Effect(error) => Display::fmt(error, f),
            Self::Model(error) => Display::fmt(error, f),
            Self::Task(error) => Display::fmt(error, f),
            Self::NotAccepted { scope } => {
                write!(f, "run {scope} has not accepted an assignment")
            }
            Self::Terminal { status } => write!(
                f,
                "the run is {status} and accepts no further transition"
            ),
            Self::UnknownEffect { effect_id } => write!(
                f,
                "a result arrived for effect {effect_id}, which this run does not hold"
            ),
            Self::Checkpoint(error) => Display::fmt(error, f),
            Self::UnknownCheckpoint { checkpoint_id } => write!(
                f,
                "a decision arrived for checkpoint {checkpoint_id}, which this run does not hold"
            ),
            Self::StaleEffectResult { effect_id, status } => write!(
                f,
                "a result arrived for effect {effect_id}, which this run already resolved as {status}"
            ),
            Self::StaleEffectGeneration {
                effect_id,
                held,
                received,
            } => write!(
                f,
                "a result arrived for generation {received} of effect {effect_id}, but this run \
                 holds generation {held}"
            ),
            Self::RedispatchUnaffordable { exhaustion } => write!(
                f,
                "the authorized re-invocation cannot reserve its attempt bound ({exhaustion}); \
                 cancel the run to settle the generation without invocation"
            ),
            Self::MaterializedStateTooLarge { bytes, maximum } => write!(
                f,
                "the materialized run record is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
        }
    }
}

impl Error for AgentRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Choreography(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::Effect(error) => Some(error),
            Self::Model(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentCheckpointError> for AgentRunError {
    fn from(error: AgentCheckpointError) -> Self {
        Self::Checkpoint(Box::new(error))
    }
}

impl From<AgentIdentityError> for AgentRunError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentRunError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentChoreographyError> for AgentRunError {
    fn from(error: AgentChoreographyError) -> Self {
        Self::Choreography(Box::new(error))
    }
}

impl From<DurableError> for AgentRunError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

impl From<AgentEffectError> for AgentRunError {
    fn from(error: AgentEffectError) -> Self {
        Self::Effect(Box::new(error))
    }
}

impl From<AgentModelError> for AgentRunError {
    fn from(error: AgentModelError) -> Self {
        Self::Model(Box::new(error))
    }
}

impl From<AgentTaskError> for AgentRunError {
    fn from(error: AgentTaskError) -> Self {
        Self::Task(Box::new(error))
    }
}

impl From<AgentRunError> for AgentChoreographyError {
    fn from(error: AgentRunError) -> Self {
        match error {
            AgentRunError::Identity(error) => Self::Identity(error),
            AgentRunError::Schema(error) => Self::Schema(error),
            AgentRunError::Choreography(error) => *error,
            AgentRunError::Persistence(error) => Self::Persistence(error),
            other => Self::PayloadEncoding {
                message: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::{AgentCredentialBindingRef, AgentTaskDefinitionId};
    use crate::effect::{AgentEffectSpec, AgentReconciliationProtocolRef};
    use crate::loop_runtime::CURRENT_AGENT_LOOP_ADAPTER_VERSION;
    use crate::model::{AgentModelTurn, AgentToolCallId, AgentToolCallRequest};
    use crate::schema::CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION;
    use crate::task::{
        AgentAcceptedResult, AgentSchemaId, AgentSchemaRef, AgentTaskContent, AgentTaskDefinition,
    };
    use crate::AgentToolId;

    /// A superset of every working set one turn can hold, under maximal
    /// identifiers, must fit the growth reserve.
    ///
    /// The reserve exists so a run the entity admits can never later be refused
    /// its own turn: `record_effect_result` checks the full materialized bound,
    /// a rejected transition never enters the operation log, and the
    /// dispatcher's retry would be refused identically forever. This test holds
    /// [`AGENT_RUN_STATE_GROWTH_RESERVE_BYTES`] to that claim empirically: it
    /// materializes more than any reachable state carries at once — the maximal
    /// pending turn, its full effect fan-out, every tool result, the proposal,
    /// the accepted result, the feedback, and the terminal reason together —
    /// and measures the growth over the record acceptance admitted.
    #[test]
    fn the_growth_reserve_covers_the_maximal_working_set() {
        let now = AgentTimestampMillis::new(1);
        // Maximal identifiers: every derived id in the working set — effect
        // ids, idempotency keys, the proposal id — scales with these.
        let long = "a".repeat(crate::identity::AGENT_IDENTITY_MAX_LENGTH);
        let scope = AgentRunScope::new(
            TenantId::new(&long),
            AgentId::new(&long).expect("the agent id is valid"),
            AgentRunId::new(&long).expect("the run id is valid"),
        )
        .expect("the scope is valid");
        let task = AgentTaskId::new(&long).expect("the task id is valid");
        let schema = AgentSchemaRef::new(
            AgentSchemaId::new("result").expect("the schema id is valid"),
            AgentRevisionNumber::INITIAL,
        );
        let definition = AgentTaskDefinition::new(
            AgentTaskDefinitionId::new("definition").expect("the definition id is valid"),
            "The growth-reserve fixture.",
            schema.clone(),
            schema.clone(),
        )
        .expect("the definition is valid");

        let budget = AgentRunBudget::allocate(
            crate::budget::AgentBudgetGrant::from_ceilings(&definition.budgets),
            now,
        );
        let loop_state = AgentLoopState::started(
            task.clone(),
            None,
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            definition.version,
            budget,
        );
        let mut run = AgentRun {
            binding: AgentRunBinding::new(scope.clone(), task),
            generation: AgentAssignmentGeneration::new(1),
            definition: definition.clone(),
            input: AgentTaskContent::inline(serde_json::json!({ "input": "x" }))
                .expect("the input is inline-bounded"),
            status: AgentRunStatus::Running,
            loop_state,
            terminal_reason: None,
            settlement: AgentRunSettlementStatus::Owed,
            accepted_at: now,
        };
        let baseline = run.materialized_size_bytes();

        // The feedback of a rejected proposal, at its bound.
        run.loop_state
            .begin_turn(Some("f".repeat(AGENT_RUN_DETAIL_MAX_LENGTH)));
        let turn = run.loop_state.turn();

        // The spec below is deliberately maximal: a reconciliation protocol, a
        // credential binding, and a timeout all enlarge the record, and the
        // reserve must cover the largest intent a deployment can configure.
        let spec = AgentEffectSpec::reconcileable(
            AgentReconciliationProtocolRef::new("r".repeat(64)).expect("the protocol ref is valid"),
            3,
        )
        .expect("the spec is valid")
        .with_credential_binding(
            AgentCredentialBindingRef::new("c".repeat(64)).expect("the binding ref is valid"),
        )
        .with_timeout_ms(60_000);

        // The context reference and the completed model effect of the turn.
        let context =
            AgentContextSnapshotRef::for_turn(&scope, turn).expect("the reference derives");
        run.loop_state.set_context_snapshot(context.clone());
        let mut model_effect = AgentRunEffect::new(
            &scope,
            turn,
            0,
            AgentRunEffectRequest::Model {
                context,
                profile: None,
            },
            &spec,
            AgentRevisionNumber::INITIAL,
            now,
        )
        .expect("the model effect derives");
        model_effect.status = AgentRunEffectStatus::Succeeded;
        run.loop_state
            .record_effect(model_effect)
            .expect("the model effect records");

        // The maximal pending turn: the full tool fan-out, with the arguments
        // sized so the turn sits just under its own bound.
        let calls: Vec<AgentToolCallRequest> = (0..crate::model::AGENT_MODEL_MAX_TOOL_CALLS)
            .map(|index| {
                AgentToolCallRequest::new(
                    AgentToolCallId::new(format!("call-{index}")).expect("the call id is valid"),
                    AgentToolId::new(format!("tool-{index}")).expect("the tool id is valid"),
                    serde_json::json!("b".repeat(1850)),
                )
                .expect("the arguments are bounded")
            })
            .collect();
        let mut pending = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION);
        assert_eq!(
            pending.schema_version(),
            CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION
        );
        for call in &calls {
            pending = pending.with_tool_call(call.clone());
        }
        pending
            .validate()
            .expect("the maximal turn is still bounded");
        run.loop_state.set_pending_turn(pending);

        // One outstanding effect per call — each copies its call — and one
        // maximal tool result per call besides.
        for (index, call) in calls.into_iter().enumerate() {
            let effect = AgentRunEffect::new(
                &scope,
                turn,
                index + 1,
                AgentRunEffectRequest::Tool {
                    call: Box::new(call.clone()),
                },
                &spec,
                AgentRevisionNumber::INITIAL,
                now,
            )
            .expect("the tool effect derives");
            run.loop_state
                .record_effect(effect)
                .expect("the tool effect records");
            run.loop_state.record_tool_result(AgentToolResult {
                call_id: call.call_id,
                content: AgentTaskContent::inline(serde_json::json!("c".repeat(1900)))
                    .expect("the result is inline-bounded"),
                recorded_at: now,
            });
        }

        // The proposal, the accepted result at its inline bound, and the
        // terminal reason — none of which coexist with a full fan-out in any
        // reachable state, which is what makes this a superset.
        let proposal_id = proposal_operation_id(&scope, turn).expect("the proposal id derives");
        let content = AgentTaskContent::inline(serde_json::json!("d".repeat(7900)))
            .expect("the result is inline-bounded");
        let digest = content.digest();
        run.loop_state.set_proposal(AgentRunProposal {
            proposal_id: proposal_id.clone(),
            turn,
            result_schema: definition.result_schema.clone(),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.version,
            digest: digest.clone(),
            proposed_at: now,
        });
        run.loop_state.set_accepted_result(AgentAcceptedResult {
            proposal_id,
            run: scope.run().clone(),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.version,
            result_schema: definition.result_schema,
            content,
            digest,
            evidence: Vec::new(),
            accepted_at: now,
        });
        run.terminal_reason = Some(AgentRunTerminalReason::CancellationRequested {
            reason: "r".repeat(AGENT_RUN_DETAIL_MAX_LENGTH),
        });

        let growth = run.materialized_size_bytes().saturating_sub(baseline);
        assert!(
            growth <= AGENT_RUN_STATE_GROWTH_RESERVE_BYTES,
            "the maximal working set grows the record by {growth} bytes, which exceeds the \
             {AGENT_RUN_STATE_GROWTH_RESERVE_BYTES} byte reserve: a run admitted at the bound \
             could be refused its own turn"
        );
    }
}
