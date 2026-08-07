//! The durable agent loop.
//!
//! Owns the loop phase enum and the versioned durable loop-state record, plus
//! the execution rule that governs every handler: a transition is bounded, and
//! it persists the next effect or the next wait before it returns. Nothing that
//! matters to the loop lives only in memory, so a crash at any point resumes
//! from the last persisted transition rather than replaying model or tool work.
//!
//! Specification: sections 9.4 and 9.5. Filled by slice 1.5, and retrofitted
//! onto the full effect state machine of [`crate::effect`] in slice 1.7.
//!
//! # The execution rule
//!
//! [Specification 9.5](../../../docs/plans/rakka-agent/spec.md) is two
//! sentences, and everything here follows from them: a handler performs bounded
//! state transitions and never awaits a model, tool, human, peer, or long timer
//! inside itself; and **the run transition persists the next effect or the next
//! wait before it returns**.
//!
//! The loop is therefore not a `while` loop that calls a provider. It is a state
//! machine whose every arrow is one compare-and-set on the run's durable state,
//! and whose every rest position is a durable wait:
//!
//! ```text
//!   PreparingContext ──[persist model effect]──▶ AwaitingModel
//!          ▲                                          ┆
//!          │                                          ┆ (dispatcher returns a durable result command)
//!          │                                          ▼
//!          │                                 EvaluatingModelOutput
//!          │                                     │            │
//!          │              [persist tool effects] │            │ (no tools)
//!          │                                     ▼            │
//!          │                              AwaitingTools       │
//!          │                                     ┆            │
//!          │                                     ┆ (dispatcher)
//!          │                                     ▼            │
//!          │                              RecordingTurn ◀─────┘
//!          │                                     │
//!          │                                     ▼
//!          └────[another iteration]──── DecidingContinuation
//!                                               │
//!                          [persist the result  │
//!                           proposal + exchange]▼
//!                                     the task entity decides
//!                                               ┆
//!                                    ┌──────────┴──────────┐
//!                                    ▼                     ▼
//!                                 Complete        another iteration
//! ```
//!
//! A dotted line (`┆`) is where the run is **not running**: it has persisted a
//! wait, holds no live execution resource, and its entity has passivated. What
//! resumes it is a durable command — an effect result the dispatcher returned
//! through the inbox, or an exchange reply — routed to whichever node owns the
//! shard at that moment
//! ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
//!
//! Two arrows are where the correctness lives:
//!
//! - **`PreparingContext` → `AwaitingModel`** persists the effect *and* the wait
//!   in one compare-and-set. There is no instant at which a run is durably
//!   waiting for an effect that was never recorded. See [`crate::effect`] for
//!   why the effect record must be a field of the run's own state for that to be
//!   true.
//! - **`DecidingContinuation` → the task** persists the result proposal and the
//!   exchange that carries it in one compare-and-set, and then waits for the
//!   task's decision. The run never makes the public task terminal by mutating
//!   its own state ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)):
//!   the task entity's persisted decision is the source of truth for the
//!   validation outcome, and the run's state is the source of truth only for
//!   what the run does about it.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    AgentEffectId, AgentTelemetryContext, AgentTimerId, AgentTimestampMillis, HumanCheckpointId,
    StateSchemaVersion,
};
use serde::{Deserialize, Serialize};

use crate::budget::{AgentBudgetExhaustion, AgentRunBudget};
use crate::checkpoints::{AgentCheckpoint, AgentCheckpointGrant};
use crate::definition::{AgentRevisionNumber, AgentTaskDefinitionId};
use crate::delegation::{AgentDelegationCell, AgentDelegationStatus, AgentRunDelegationEnvelope};
use crate::effect::{
    AgentEffectError, AgentRunEffect, AgentToolResult, AGENT_RUN_MAX_PENDING_EFFECTS,
};
use crate::identity::{
    AgentDelegationId, AgentGoalId, AgentOperationId, AgentRunScope, AgentTaskId,
    AgentWorkflowInvocationId,
};
use crate::memory::{
    AgentContextSnapshotRef, AgentPromotedMemoryRef, MemoryClassification, MemoryEntryId,
    MemoryEntryRole, MemoryError, MemoryOperationId, MemorySequence, SessionMemoryEntry,
};
use crate::model::AgentModelTurn;
use crate::observability::{AgentDecisionDraft, AgentDecisionEvent};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
};
use crate::task::{AgentAcceptedResult, AgentContentDigest, AgentSchemaRef, AgentTaskContent};

/// Version of the loop adapter whose turns a loop-state record carries.
///
/// It is persisted alongside the loop-state schema version
/// ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)) because the
/// two evolve independently: the *shape* of the record is the schema version,
/// and the semantics an adapter gives a turn is this one. Upgrading a model
/// adapter is therefore an explicit migration rather than a silent
/// reinterpretation of the turns the previous adapter wrote
/// ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
pub const CURRENT_AGENT_LOOP_ADAPTER_VERSION: AgentRevisionNumber = AgentRevisionNumber::INITIAL;

/// Most session-memory entries a run may owe its store before a flush drains
/// them ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// A turn records at most one assistant entry plus one entry per tool result, so
/// the outbox holds a bounded working set: it is drained on every settle pass,
/// and a transition that would cross the bound fails closed rather than persist an
/// unbounded record — the same discipline the effect list and the task history
/// outbox keep. A run with no session memory configured never records into it, so
/// it stays empty and adds nothing to the run's durable state.
pub const AGENT_RUN_SESSION_OUTBOX_CAPACITY: usize = 32;

/// Most decision events a run may owe its sink before the oldest are dropped.
///
/// Decision events are observability, never correctness
/// ([specification 17.1](../../../docs/plans/rakka-agent/spec.md)): where the
/// session-memory outbox fails a transition closed at its bound — a snapshot
/// depends on what it holds — the decision outbox is a bounded ring that
/// drops its *oldest unflushed* event and counts the drop, because a
/// transition that failed over telemetry would make observability a
/// correctness input. Drops are visible through
/// [`AgentLoopState::decision_drops`].
pub const AGENT_RUN_DECISION_OUTBOX_CAPACITY: usize = 32;

/// Most settled memory-promotion receipts a run's loop state retains.
///
/// A receipt is provenance, never correctness — the private store is the
/// source of truth for what a promotion wrote
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)) — so the
/// list is a bounded ring: one past the bound drops the oldest receipt rather
/// than growing the run's durable state.
pub const AGENT_RUN_MAX_MEMORY_PROMOTIONS: usize = 16;

/// The bounded receipt of one settled memory promotion: which effect promoted,
/// and the identities and revisions the store now holds — never content
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMemoryPromotionRecord {
    /// The promotion effect the receipt settles.
    pub effect_id: AgentEffectId,
    /// The memories the promotion wrote or converged on.
    pub promoted: Vec<AgentPromotedMemoryRef>,
    /// When the receipt was recorded.
    pub recorded_at: AgentTimestampMillis,
}

/// Where one run stands in its durable loop
/// ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// The phase is durable state, not a program counter: it is what a run
/// re-materialized on another pod reads in order to know what to do next,
/// without rerunning a decision it has already made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentLoopPhase {
    /// Assemble the immutable context snapshot and persist the model effect.
    PreparingContext,
    /// A model effect is outstanding. The run is not executing.
    AwaitingModel,
    /// A model turn has been recorded and is being acted on.
    EvaluatingModelOutput,
    /// One or more tool effects are outstanding. The run is not executing.
    AwaitingTools,
    /// A closed fan-in group awaits its children's durable results. The run
    /// is not executing and not resident: each result is an inter-entity
    /// exchange that re-activates the owner, and the fan-in resolution is
    /// what completes the awaiting turn ([specification 8.7]).
    ///
    /// [specification 8.7]: ../../../docs/plans/rakka-agent/spec.md
    AwaitingChildren,
    /// Fold the turn into the durable session.
    RecordingTurn,
    /// Decide what follows the turn: propose the result, or iterate again.
    DecidingContinuation,
    /// The run is suspended by policy or by an administrative decision.
    Suspended,
    /// The loop is finished.
    Complete,
}

impl AgentLoopPhase {
    /// Stable kebab-case label for errors, logs, and bounded metric labels.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::PreparingContext => "preparing-context",
            Self::AwaitingModel => "awaiting-model",
            Self::EvaluatingModelOutput => "evaluating-model-output",
            Self::AwaitingTools => "awaiting-tools",
            Self::AwaitingChildren => "awaiting-children",
            Self::RecordingTurn => "recording-turn",
            Self::DecidingContinuation => "deciding-continuation",
            Self::Suspended => "suspended",
            Self::Complete => "complete",
        }
    }

    /// Whether the run can advance this phase on its own, with no external
    /// input.
    ///
    /// This is what makes the loop crank: the entity performs *executable*
    /// phases one bounded transition at a time until it reaches one that is not
    /// — which is by definition a durable wait — and then it passivates.
    ///
    /// [`Self::DecidingContinuation`] is executable only until the run has
    /// proposed its result. After that it is waiting for the task's decision, so
    /// the run consults its own persisted proposal and not this flag alone.
    #[must_use]
    pub const fn is_executable(self) -> bool {
        matches!(
            self,
            Self::PreparingContext
                | Self::EvaluatingModelOutput
                | Self::RecordingTurn
                | Self::DecidingContinuation
        )
    }

    /// Whether the phase is a durable wait on an effect or on children.
    #[must_use]
    pub const fn is_waiting(self) -> bool {
        matches!(
            self,
            Self::AwaitingModel | Self::AwaitingTools | Self::AwaitingChildren
        )
    }
}

impl Display for AgentLoopPhase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The result proposal a run has persisted and not yet settled
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run persists it *before* the exchange that carries it, so a run lost
/// before the send re-drives the same proposal under the same id rather than
/// composing a second one — and a duplicate proposal id returns the task's
/// original decision rather than validating twice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunProposal {
    /// The stable proposal identity, derived from the run and the turn.
    pub proposal_id: AgentOperationId,
    /// The turn that proposed it.
    pub turn: u64,
    /// The schema the proposed result is expressed in.
    pub result_schema: AgentSchemaRef,
    /// The task definition it was proposed under.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition. A mismatch fails closed at the task.
    pub definition_version: AgentRevisionNumber,
    /// A fingerprint of the proposed content, so an operator reading history can
    /// tell one proposal from another.
    pub digest: AgentContentDigest,
    /// When the run persisted it.
    pub proposed_at: AgentTimestampMillis,
}

/// The top-up a run is waiting on after it exhausted its escrowed allocation
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// A run that exhausts its budget parks with this record rather than failing at
/// once: it asks its parent for more allocation through a deduplicated
/// `BudgetAllocation` exchange, and resumes if the grant relieves the ceiling it
/// hit. The record carries the exhaustion so the run can terminate with the
/// original structured reason if the parent has nothing to give, and the
/// sequence so a re-driven request returns the parent's original grant rather
/// than debiting it twice.
///
/// The run's status stays `Running` while it holds this: a pending inter-entity
/// exchange is the run's own durable outbox, re-driven by the courier, not a
/// residency wait — the same argument the result-proposal exchange makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPendingTopUp {
    /// Which top-up this is for the run, counting from one. It fences the
    /// replay window the exchange journal's bounded ring cannot.
    pub sequence: u64,
    /// The ceiling the run reached, carried to the parent so it decides on
    /// facts and kept so the run can fail with the original reason.
    pub exhaustion: AgentBudgetExhaustion,
}

/// The durable, versioned loop state of one run
/// ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// It carries what the next transition needs and nothing that could grow without
/// bound: no conversation, no tool-payload history, no observations. A turn's
/// content belongs in session memory (slice 1.11) and in artifacts; the loop
/// keeps only the working set of the turn in flight, and clears it when the turn
/// is recorded, so a run that iterates a hundred times persists no more than a
/// run that iterates once
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Content capture is off by default
/// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)), and a
/// resolved credential never appears here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLoopState {
    schema_version: StateSchemaVersion,
    adapter_version: AgentRevisionNumber,
    goal: Option<AgentGoalId>,
    task: AgentTaskId,
    turn: u64,
    phase: AgentLoopPhase,
    agent_definition_revision: AgentRevisionNumber,
    agent_settings_revision: AgentRevisionNumber,
    task_definition_version: AgentRevisionNumber,
    context_snapshot: Option<AgentContextSnapshotRef>,
    effects: Vec<AgentRunEffect>,
    pending_turn: Option<Box<AgentModelTurn>>,
    tool_results: Vec<AgentToolResult>,
    proposal: Option<AgentRunProposal>,
    accepted_result: Option<Box<AgentAcceptedResult>>,
    feedback: Option<String>,
    budget: AgentRunBudget,
    #[serde(default)]
    pending_top_up: Option<AgentPendingTopUp>,
    pending_checkpoint: Option<HumanCheckpointId>,
    pending_timer: Option<AgentTimerId>,
    /// The approval/authorization checkpoints the run has opened and is waiting
    /// on ([specification 12](../../../docs/plans/rakka-agent/spec.md)). Bounded
    /// by [`AGENT_RUN_MAX_PENDING_EFFECTS`]: at most one per effect a turn gates.
    #[serde(default)]
    open_checkpoints: Vec<AgentCheckpoint>,
    /// The digest-bound grants a resolved checkpoint issued, keyed implicitly by
    /// the effect id and generation each binds. A grant lives only until its
    /// effect resolves, then leaves with the turn.
    #[serde(default)]
    checkpoint_grants: Vec<AgentCheckpointGrant>,
    /// The session-memory entries a recorded turn owes its store, drained by the
    /// run entity's flush ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
    /// It is populated only when the run entity is wired with a session-memory
    /// backend; an unwired run leaves it empty, so an existing run's durable
    /// state is unchanged. The loop keeps no turn content of its own — the outbox
    /// holds only what it still owes the store, and it is cleared as each entry
    /// is durably appended.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    session_outbox: Vec<SessionMemoryEntry>,
    /// How many session-memory entries this run has assigned a sequence, so the
    /// next entry's monotonic order key is stable across a re-driven transition.
    #[serde(default, skip_serializing_if = "is_zero")]
    session_sequence: u64,
    /// Trace context of the bounded segment that last committed this state —
    /// what a resume after plain passivation links back to when no checkpoint,
    /// effect, or timer holds the parked span
    /// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)). Every
    /// effect a transition commits is stamped from it. Observability only,
    /// never correctness: a loop state persisted before this field decodes to
    /// the empty context, and no transition reads it to decide anything.
    #[serde(default)]
    telemetry: AgentTelemetryContext,
    /// The decision events recorded transitions owe the sink, drained by the
    /// settle pass after each transition commits
    /// ([specification 17.7](../../../docs/plans/rakka-agent/spec.md),
    /// [17.13](../../../docs/plans/rakka-agent/spec.md)). Populated only when
    /// the run entity is wired with a decision-event sink; a bounded ring, so
    /// overflow drops the oldest owed event rather than failing the
    /// transition.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    decision_outbox: Vec<AgentDecisionEvent>,
    /// How many decision events this run has assigned a sequence, so an
    /// event's monotonic order key is stable across a re-driven transition.
    #[serde(default, skip_serializing_if = "is_zero")]
    decision_sequence: u64,
    /// How many owed decision events the bounded ring has dropped, which is
    /// the bounded visibility [specification 17.1](../../../docs/plans/rakka-agent/spec.md)
    /// requires of telemetry loss.
    #[serde(default, skip_serializing_if = "is_zero")]
    decision_drops: u64,
    /// Bounded receipts of settled memory promotions
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)):
    /// identities and revisions only, never content. A ring capped at
    /// [`AGENT_RUN_MAX_MEMORY_PROMOTIONS`]; the private store is the source of
    /// truth, so overflow drops the oldest receipt. A loop state persisted
    /// before this field decodes to the empty list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    memory_promotions: Vec<AgentMemoryPromotionRecord>,
    /// The completed goal evaluation the run holds until — and after — the
    /// exchange carrying it to the coordinating task settles
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)). One
    /// cell: a run evaluates its goal one evaluation at a time, and a new
    /// evaluation replaces a settled cell. A loop state persisted before this
    /// field decodes without one.
    #[serde(default)]
    goal_evaluation: Option<Box<AgentGoalEvaluationCell>>,
    /// The delegations this run has committed, keyed by their derived
    /// identity ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
    /// Each cell holds the record persisted before its send and where the
    /// delegation stands; the map is bounded by
    /// [`crate::delegation::AGENT_RUN_MAX_DELEGATIONS`] at the interception
    /// door. A loop state persisted before this field decodes to the empty
    /// map.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    delegations: BTreeMap<AgentDelegationId, Box<AgentDelegationCell>>,
    /// The delegation authority the assignment carried: goal-scope skill and
    /// tool narrowing, advisory ceilings, and the lineage/depth of the task
    /// this run serves. A loop state persisted before this field decodes
    /// without one, which means no goal narrowing.
    #[serde(default)]
    delegation_envelope: Option<Box<AgentRunDelegationEnvelope>>,
    /// The run's one durable fan-out group, opened in the compare-and-set
    /// that commits its first delegation and replaced by the next delegation
    /// after resolution ([specification 8.7]). A loop state persisted before
    /// this field decodes without one.
    ///
    /// [specification 8.7]: ../../../docs/plans/rakka-agent/spec.md
    #[serde(default)]
    fan_in: Option<Box<crate::fan_in::AgentFanInCell>>,
    /// The workflow invocations this run has committed, keyed by their
    /// derived identity ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)).
    /// Each cell holds the record persisted before its start and where the
    /// invocation stands; the combined delegation-and-invocation count is
    /// bounded by [`crate::fan_in::AGENT_RUN_MAX_FAN_IN_MEMBERS`] at the
    /// interception door. A loop state persisted before this field decodes to
    /// the empty map.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    workflow_invocations:
        BTreeMap<AgentWorkflowInvocationId, Box<crate::workflow_tool::AgentWorkflowInvocationCell>>,
}

/// One completed goal evaluation and where its report to the coordinating
/// task stands ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The exchange operation id is derived when the outcome is applied — a pure
/// function of the effect generation that produced the record — so a re-drive
/// after any crash re-owes the identical exchange and the journal deduplicates
/// it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentGoalEvaluationCell {
    /// The completed record.
    pub record: Box<crate::evaluation::AgentGoalEvaluationRecord>,
    /// The derived operation id of the exchange that reports the record.
    pub exchange_operation_id: AgentOperationId,
    /// When the evaluation's outcome was applied.
    pub completed_at: AgentTimestampMillis,
    /// Whether the exchange settled — the decision door accepted or refused.
    #[serde(default)]
    pub reported: bool,
    /// The decision door's refusal code, when it refused. A refused
    /// evaluation is settled, never re-driven: the caller re-evaluates.
    #[serde(default)]
    pub refusal: Option<String>,
}

/// Whether a defaulted count is zero, so it is omitted from a run's serialized
/// state until the run actually uses session memory.
fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl AgentLoopState {
    /// The loop state of a run that has just accepted its assignment.
    #[must_use]
    pub fn started(
        task: AgentTaskId,
        goal: Option<AgentGoalId>,
        agent_definition_revision: AgentRevisionNumber,
        agent_settings_revision: AgentRevisionNumber,
        task_definition_version: AgentRevisionNumber,
        budget: AgentRunBudget,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
            adapter_version: CURRENT_AGENT_LOOP_ADAPTER_VERSION,
            goal,
            task,
            turn: 1,
            phase: AgentLoopPhase::PreparingContext,
            agent_definition_revision,
            agent_settings_revision,
            task_definition_version,
            context_snapshot: None,
            effects: Vec::new(),
            pending_turn: None,
            tool_results: Vec::new(),
            proposal: None,
            accepted_result: None,
            feedback: None,
            budget,
            pending_top_up: None,
            pending_checkpoint: None,
            pending_timer: None,
            open_checkpoints: Vec::new(),
            checkpoint_grants: Vec::new(),
            session_outbox: Vec::new(),
            session_sequence: 0,
            telemetry: AgentTelemetryContext::default(),
            decision_outbox: Vec::new(),
            decision_sequence: 0,
            decision_drops: 0,
            memory_promotions: Vec::new(),
            goal_evaluation: None,
            delegations: BTreeMap::new(),
            delegation_envelope: None,
            fan_in: None,
            workflow_invocations: BTreeMap::new(),
        }
    }

    /// Records the trace context of the bounded segment committing this state.
    ///
    /// The context is admitted through
    /// [`crate::observability::sanitize_agent_telemetry_context`]: strict on
    /// write so the read side never has to fail closed over telemetry. An
    /// entity never invents context — it records what the command that
    /// activated it carried, or leaves the previous segment's context in
    /// place.
    pub fn record_telemetry(&mut self, telemetry: AgentTelemetryContext) {
        self.telemetry = crate::observability::sanitize_agent_telemetry_context(telemetry);
    }

    /// Trace context of the bounded segment that last committed this state.
    #[must_use]
    pub const fn telemetry(&self) -> &AgentTelemetryContext {
        &self.telemetry
    }

    /// Records one decision the committing transition made
    /// ([specification 17.7](../../../docs/plans/rakka-agent/spec.md)),
    /// returning whether it was newly owed.
    ///
    /// The event's identity is *derived* from the run, the turn, and the
    /// draft's slot, so a re-driven transition records the same decision at
    /// the same sequence rather than duplicating it. The outbox is a bounded
    /// ring: a decision that would cross the bound drops the oldest owed
    /// event and counts the drop, never failing the transition — telemetry is
    /// not a correctness input. A draft whose derived identity cannot be
    /// formed is dropped and counted the same way.
    pub(crate) fn record_decision(
        &mut self,
        scope: &AgentRunScope,
        draft: AgentDecisionDraft,
        now: AgentTimestampMillis,
    ) -> bool {
        let sequence = self.decision_sequence.saturating_add(1);
        let Ok(event) = AgentDecisionEvent::assemble(
            scope,
            Some(self.task.clone()),
            self.goal.clone(),
            sequence,
            self.turn,
            self.phase,
            self.agent_definition_revision,
            self.agent_settings_revision,
            self.context_snapshot.clone(),
            self.telemetry.clone(),
            draft,
            now,
        ) else {
            self.decision_drops = self.decision_drops.saturating_add(1);
            return false;
        };
        if self
            .decision_outbox
            .iter()
            .any(|owed| owed.operation_id == event.operation_id)
        {
            return false;
        }
        if self.decision_outbox.len() >= AGENT_RUN_DECISION_OUTBOX_CAPACITY {
            self.decision_outbox.remove(0);
            self.decision_drops = self.decision_drops.saturating_add(1);
        }
        self.decision_sequence = sequence;
        self.decision_outbox.push(event);
        true
    }

    /// The decision events recorded transitions still owe the sink.
    #[must_use]
    pub fn decision_outbox(&self) -> &[AgentDecisionEvent] {
        &self.decision_outbox
    }

    /// How many owed decision events the bounded ring has dropped.
    #[must_use]
    pub const fn decision_drops(&self) -> u64 {
        self.decision_drops
    }

    /// The durable decision-event cursor: how many decisions this run has
    /// assigned a sequence.
    #[must_use]
    pub const fn decision_sequence(&self) -> u64 {
        self.decision_sequence
    }

    /// Drops the owed decision events the sink durably accepted.
    pub(crate) fn clear_flushed_decisions(&mut self, flushed: &[AgentOperationId]) {
        self.decision_outbox
            .retain(|owed| !flushed.contains(&owed.operation_id));
    }

    /// The version of the adapter whose turns this state carries.
    #[must_use]
    pub const fn adapter_version(&self) -> AgentRevisionNumber {
        self.adapter_version
    }

    /// The task this run serves. Fixed for the run's lifetime.
    #[must_use]
    pub const fn task(&self) -> &AgentTaskId {
        &self.task
    }

    /// The collaborative goal the run contributes to.
    #[must_use]
    pub const fn goal(&self) -> Option<&AgentGoalId> {
        self.goal.as_ref()
    }

    /// The current turn, counting from one.
    #[must_use]
    pub const fn turn(&self) -> u64 {
        self.turn
    }

    /// Where the loop stands.
    #[must_use]
    pub const fn phase(&self) -> AgentLoopPhase {
        self.phase
    }

    /// The agent definition revision the run was assigned under.
    #[must_use]
    pub const fn agent_definition_revision(&self) -> AgentRevisionNumber {
        self.agent_definition_revision
    }

    /// The agent settings revision the run pinned at acceptance.
    ///
    /// Run-pinned settings fields resolve against this revision for the run's
    /// whole life; turn-bound and immediate-safety fields resolve against the
    /// agent's *current* revision at each turn
    /// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md), and
    /// [`crate::definition::effective_settings_for_turn`], which is that
    /// resolution). Slice 1.8 performs it at dispatch, which is the only place
    /// an immediate-safety revocation can be enforced.
    #[must_use]
    pub const fn agent_settings_revision(&self) -> AgentRevisionNumber {
        self.agent_settings_revision
    }

    /// The task definition revision the run proposes its result under.
    #[must_use]
    pub const fn task_definition_version(&self) -> AgentRevisionNumber {
        self.task_definition_version
    }

    /// The snapshot the current model effect was prepared against.
    #[must_use]
    pub const fn context_snapshot(&self) -> Option<&AgentContextSnapshotRef> {
        self.context_snapshot.as_ref()
    }

    /// Every effect the run holds.
    #[must_use]
    pub fn effects(&self) -> &[AgentRunEffect] {
        &self.effects
    }

    /// Every effect the run is still waiting on.
    pub fn outstanding_effects(&self) -> impl Iterator<Item = &AgentRunEffect> {
        self.effects.iter().filter(|effect| effect.is_outstanding())
    }

    /// Every effect committed by a transition but not yet made dispatchable.
    #[must_use]
    pub fn undispatched_effects(&self) -> Vec<AgentRunEffect> {
        self.effects
            .iter()
            .filter(|effect| effect.is_pending())
            .cloned()
            .collect()
    }

    /// Every effect that is dispatchable and not yet resolved.
    ///
    /// The flush re-drives the sink for exactly this set: `Ready` proves the
    /// effect was made dispatchable, not that the sink write landed, so the
    /// idempotent write is repeated until a result resolves the effect.
    #[must_use]
    pub fn ready_effects(&self) -> Vec<AgentRunEffect> {
        self.effects
            .iter()
            .filter(|effect| effect.status == crate::effect::AgentRunEffectStatus::Ready)
            .cloned()
            .collect()
    }

    /// Every effect whose generation is parked as indeterminate, awaiting an
    /// explicit reconciliation decision
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
    pub fn indeterminate_effects(&self) -> impl Iterator<Item = &AgentRunEffect> {
        self.effects
            .iter()
            .filter(|effect| effect.status == crate::effect::AgentRunEffectStatus::Indeterminate)
    }

    /// Whether any effect is parked as indeterminate.
    #[must_use]
    pub fn has_indeterminate_effect(&self) -> bool {
        self.indeterminate_effects().next().is_some()
    }

    /// Whether the run is waiting on any effect.
    #[must_use]
    pub fn awaits_effect(&self) -> bool {
        self.outstanding_effects().next().is_some()
    }

    /// Whether the run still has work in flight whose outcome must settle
    /// before it may become terminal: an outstanding effect, an indeterminate
    /// effect whose outcome is unknown, or a result proposal awaiting the
    /// task's decision.
    ///
    /// This is the quiesce condition of
    /// [specification 8.7](../../../docs/plans/rakka-agent/spec.md): work whose
    /// outcome is unknown must not be abandoned as though it never happened.
    /// An indeterminate effect is the sharpest case — the run stays
    /// nonterminal in reconciliation until an explicit decision resolves it,
    /// cancellation requested or not.
    #[must_use]
    pub fn awaits_settlement(&self) -> bool {
        self.effects.iter().any(AgentRunEffect::blocks_settlement) || self.proposal.is_some()
    }

    /// Whether a child this run created has not yet recorded its terminal
    /// outcome: a delegation or workflow-invocation cell still pending, or
    /// settled with a live child and no result.
    ///
    /// This is the subtree half of the specification-8.7 quiesce condition a
    /// winding-down parent holds against: every started child is a started
    /// consequential effect, and the parent must not project terminal
    /// `Cancelled` until each has a known outcome — a child parked in its own
    /// reconciliation deliberately holds the whole ancestry nonterminal until
    /// an explicit decision resolves it. A cell settled `Conflicted` or
    /// `Failed` never had a child and blocks nothing.
    ///
    /// A child whose delegation-cancel was *definitively refused* blocks
    /// nothing either, and that release is what keeps the wait finite: the
    /// child's own settle rule accepts only `delegation-cancel-forged` and
    /// `delegation-cancel-not-delegated` as definitive, and both prove the
    /// addressed task will never report to this run — it carries no
    /// provenance naming this delegation, so its terminal transition owes
    /// this parent nothing. Waiting on it would be waiting for a report that
    /// cannot arrive.
    ///
    /// # The workflow half makes the result relay load-bearing
    ///
    /// A started workflow invocation is released only by its recorded child
    /// result, and nothing in this crate delivers one: the application owes
    /// [`crate::run::AgentRunEntityCommand::RecordWorkflowResult`]. A
    /// deployment that invokes workflow tools without wiring that relay
    /// therefore has no way to complete a cancellation — which is the honest
    /// reading of [specification 8.7](../../../docs/plans/rakka-agent/spec.md),
    /// not a regression to route around: the parent genuinely does not know
    /// whether the child stopped, and terminalizing anyway would be the false
    /// claim the specification forbids. Delivering the relay command *is* the
    /// "explicit reconciliation decision" the specification names as the
    /// other way out, so an operator can always resolve a child whose real
    /// outcome was established out of band.
    #[must_use]
    pub fn awaits_children(&self) -> bool {
        let delegation_open = self.delegations.values().any(|cell| match &cell.status {
            crate::delegation::AgentDelegationStatus::Pending => true,
            crate::delegation::AgentDelegationStatus::ChildCreated { .. } => {
                !cell.child_settled() && !cell.cancel_refused()
            }
            _ => false,
        });
        if delegation_open {
            return true;
        }
        self.workflow_invocations
            .values()
            .any(|cell| match &cell.status {
                crate::workflow_tool::AgentWorkflowInvocationStatus::Pending => true,
                crate::workflow_tool::AgentWorkflowInvocationStatus::Started { .. } => {
                    !cell.child_settled()
                }
                _ => false,
            })
    }

    /// Fences every effect that provably never reached the sink, marking it
    /// cancelled in place.
    ///
    /// Only a `Pending` effect qualifies: the flush hands an effect to the
    /// sink strictly after the transition that marked it `Ready` committed, so
    /// `Pending` proves no dispatch ticket exists and no invocation can be
    /// abandoned by the fence
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A fenced delegation send or workflow start settles its cell as failed
    /// in the same pass: the send provably never left the run, so the
    /// winding-down parent never spawned the child, and recovery after the
    /// wind-down uses a new delegation or invocation, never this one. Fencing
    /// anywhere else would leave a `Pending` cell under a cancelled effect —
    /// exactly the disagreement the cell's commit discipline forbids.
    ///
    /// The two kinds the wind-down itself authorizes — a scheduled
    /// compensation and a chased workflow-cancel — are exempt
    /// ([`crate::effect::AgentRunEffectKind::exempt_from_wind_down_fence`]),
    /// the same exemption the flush and the dispatcher's claim path apply.
    /// Without it a *re-entered* wind-down (a duplicate run-cancel past the
    /// journal's bounded window, or a second cancel command) would fence the
    /// very work the first one committed, and the cell's recorded disposition
    /// would then claim a request that was never delivered.
    pub(crate) fn fence_unsent_effects(&mut self, now: AgentTimestampMillis) -> usize {
        let mut fenced = 0;
        let mut fenced_sends = Vec::new();
        let mut fenced_starts = Vec::new();
        for effect in &mut self.effects {
            if effect.is_pending() && !effect.kind().exempt_from_wind_down_fence() {
                effect.status = crate::effect::AgentRunEffectStatus::Cancelled;
                match effect.kind() {
                    crate::effect::AgentRunEffectKind::A2aSendCall => {
                        fenced_sends.push(effect.effect_id.clone());
                    }
                    crate::effect::AgentRunEffectKind::WorkflowStartCall => {
                        fenced_starts.push(effect.effect_id.clone());
                    }
                    _ => {}
                }
                fenced += 1;
            }
        }
        for effect_id in fenced_sends {
            if let Some(cell) = self
                .delegations
                .values_mut()
                .find(|cell| cell.record.effect == effect_id)
            {
                cell.settle_failed("run-winding-down", now);
            }
        }
        for effect_id in fenced_starts {
            if let Some(cell) = self
                .workflow_invocations
                .values_mut()
                .find(|cell| cell.record.effect == effect_id)
            {
                cell.settle_failed("run-winding-down", now);
            }
        }
        fenced
    }

    /// The turn the model produced and the loop has not yet recorded.
    #[must_use]
    pub fn pending_turn(&self) -> Option<&AgentModelTurn> {
        self.pending_turn.as_deref()
    }

    /// The tool results the current turn has collected.
    #[must_use]
    pub fn tool_results(&self) -> &[AgentToolResult] {
        &self.tool_results
    }

    /// The result proposal the run has persisted and not yet settled.
    #[must_use]
    pub const fn proposal(&self) -> Option<&AgentRunProposal> {
        self.proposal.as_ref()
    }

    /// The typed result the task accepted.
    #[must_use]
    pub fn accepted_result(&self) -> Option<&AgentAcceptedResult> {
        self.accepted_result.as_deref()
    }

    /// The sanitized feedback a rejected proposal returned for the next bounded
    /// iteration ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn feedback(&self) -> Option<&str> {
        self.feedback.as_deref()
    }

    /// The run's own durable budget ledger.
    #[must_use]
    pub const fn budget(&self) -> &AgentRunBudget {
        &self.budget
    }

    /// The top-up the run is parked waiting on, when it has exhausted its
    /// budget and asked its parent for more
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn pending_top_up(&self) -> Option<&AgentPendingTopUp> {
        self.pending_top_up.as_ref()
    }

    /// The checkpoint the run is waiting on. Filled by slice 1.10.
    #[must_use]
    pub const fn pending_checkpoint(&self) -> Option<&HumanCheckpointId> {
        self.pending_checkpoint.as_ref()
    }

    /// The timer the run is waiting on. Filled by slice 1.10.
    #[must_use]
    pub const fn pending_timer(&self) -> Option<&AgentTimerId> {
        self.pending_timer.as_ref()
    }

    /// The approval/authorization checkpoints the run is waiting on.
    #[must_use]
    pub fn open_checkpoints(&self) -> &[AgentCheckpoint] {
        &self.open_checkpoints
    }

    /// Whether the run is waiting on any checkpoint that is not yet resolved.
    #[must_use]
    pub fn has_open_checkpoint(&self) -> bool {
        self.open_checkpoints
            .iter()
            .any(|checkpoint| checkpoint.status.is_waiting())
    }

    /// The kind of approval-family wait the run is parked on, when any: an
    /// open [`crate::checkpoints::AgentCheckpointKind::Approval`] checkpoint
    /// wins over an open
    /// [`crate::checkpoints::AgentCheckpointKind::SecurityAuthorization`] one,
    /// because a pending human decision is the wait an operator can act on
    /// first. A reconciliation checkpoint is not an approval-family wait — the
    /// indeterminate effect it gates drives the run's status instead.
    #[must_use]
    pub fn approval_family_wait(&self) -> Option<crate::checkpoints::AgentCheckpointKind> {
        let mut wait = None;
        for checkpoint in &self.open_checkpoints {
            if !checkpoint.status.is_waiting() {
                continue;
            }
            match checkpoint.kind {
                crate::checkpoints::AgentCheckpointKind::Approval => {
                    return Some(crate::checkpoints::AgentCheckpointKind::Approval)
                }
                crate::checkpoints::AgentCheckpointKind::SecurityAuthorization => {
                    wait = Some(crate::checkpoints::AgentCheckpointKind::SecurityAuthorization);
                }
                crate::checkpoints::AgentCheckpointKind::IndeterminateEffectReconciliation => {}
            }
        }
        wait
    }

    /// The digest-bound grant the run holds for exactly this effect intent, when
    /// a checkpoint for it has been resolved.
    #[must_use]
    pub fn grant_for(&self, effect: &AgentRunEffect) -> Option<&AgentCheckpointGrant> {
        self.checkpoint_grants.iter().find(|grant| {
            grant.effect_id == effect.effect_id && grant.generation == effect.generation
        })
    }

    /// Whether the effect may be handed to the sink: either it needs no
    /// checkpoint or authorization, or a valid grant for its exact generation
    /// is held. The grant's *kind* is enforced at the dispatch authority, not
    /// here: the run only ever stores the grant its own checkpoint issued.
    #[must_use]
    pub fn is_dispatchable(&self, effect: &AgentRunEffect) -> bool {
        !(effect.checkpoint_required || effect.authorization_required)
            || self.grant_for(effect).is_some()
    }

    pub(crate) fn open_checkpoint_mut(
        &mut self,
        checkpoint_id: &HumanCheckpointId,
    ) -> Option<&mut AgentCheckpoint> {
        self.open_checkpoints
            .iter_mut()
            .find(|checkpoint| &checkpoint.checkpoint_id == checkpoint_id)
    }

    /// Records a newly opened checkpoint, bounded like the effect list, and
    /// tracks its id in [`Self::pending_checkpoint`]. A checkpoint whose id the
    /// run already holds is a replay and adds nothing.
    pub(crate) fn record_checkpoint(
        &mut self,
        checkpoint: AgentCheckpoint,
    ) -> Result<(), AgentEffectError> {
        if self
            .open_checkpoints
            .iter()
            .any(|held| held.checkpoint_id == checkpoint.checkpoint_id)
        {
            return Ok(());
        }
        if self.open_checkpoints.len() >= AGENT_RUN_MAX_PENDING_EFFECTS {
            return Err(AgentEffectError::PendingOverflow {
                maximum: AGENT_RUN_MAX_PENDING_EFFECTS,
            });
        }
        self.pending_checkpoint = Some(checkpoint.checkpoint_id.clone());
        self.open_checkpoints.push(checkpoint);
        Ok(())
    }

    /// Records the grant a resolved checkpoint issued, replacing any prior grant
    /// bound to the same effect generation.
    pub(crate) fn record_grant(&mut self, grant: AgentCheckpointGrant) {
        self.checkpoint_grants.retain(|held| {
            !(held.effect_id == grant.effect_id && held.generation == grant.generation)
        });
        self.checkpoint_grants.push(grant);
    }

    /// Drops the resolved (or otherwise terminal) checkpoint from the waiting
    /// set, and points [`Self::pending_checkpoint`] at whatever remains open.
    pub(crate) fn drop_checkpoint(&mut self, checkpoint_id: &HumanCheckpointId) {
        self.open_checkpoints
            .retain(|checkpoint| &checkpoint.checkpoint_id != checkpoint_id);
        self.resync_pending_checkpoint();
    }

    fn resync_pending_checkpoint(&mut self) {
        self.pending_checkpoint = self
            .open_checkpoints
            .iter()
            .find(|checkpoint| checkpoint.status.is_waiting())
            .map(|checkpoint| checkpoint.checkpoint_id.clone());
    }

    /// Cancels every waiting approval-family checkpoint the run holds, because
    /// the run itself is winding down.
    ///
    /// A reconciliation checkpoint survives: cancellation does not make an
    /// unknown outcome known, and the parked ambiguity must stay resolvable
    /// through its checkpoint until an explicit decision settles it
    /// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 57).
    pub(crate) fn cancel_open_checkpoints(&mut self, now: AgentTimestampMillis) {
        for checkpoint in &mut self.open_checkpoints {
            if checkpoint.kind
                != crate::checkpoints::AgentCheckpointKind::IndeterminateEffectReconciliation
            {
                checkpoint.cancel(now);
            }
        }
        self.resync_pending_checkpoint();
    }

    /// Fails closed on a loop state, or a record inside it, that this binary
    /// cannot interpret ([specification 20](../../../docs/plans/rakka-agent/spec.md)).
    pub fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        policy.check_record(self)?;
        if let Some(turn) = &self.pending_turn {
            policy.check_record(turn.as_ref())?;
        }
        for effect in &self.effects {
            policy.check_record(effect)?;
        }
        for checkpoint in &self.open_checkpoints {
            policy.check_record(checkpoint)?;
        }
        for entry in &self.session_outbox {
            policy.check_record(entry)?;
        }
        Ok(())
    }

    pub(crate) fn set_phase(&mut self, phase: AgentLoopPhase) {
        self.phase = phase;
    }

    pub(crate) fn budget_mut(&mut self) -> &mut AgentRunBudget {
        &mut self.budget
    }

    /// Parks the run on a top-up request after a charge exhausted a ceiling.
    ///
    /// The phase is deliberately left where it was, so the transition that hit
    /// the ceiling re-runs and re-attempts the *same* charge once the parent's
    /// grant relieves it — the charge that failed never mutated the budget, so
    /// there is nothing to undo.
    pub(crate) fn park_for_top_up(&mut self, exhaustion: AgentBudgetExhaustion) {
        let sequence = self.budget.top_ups().saturating_add(1);
        self.pending_top_up = Some(AgentPendingTopUp {
            sequence,
            exhaustion,
        });
    }

    pub(crate) fn clear_pending_top_up(&mut self) {
        self.pending_top_up = None;
    }

    pub(crate) fn set_context_snapshot(&mut self, context: AgentContextSnapshotRef) {
        self.context_snapshot = Some(context);
    }

    /// The slot the next effect of the current turn takes.
    ///
    /// Slots are counted per turn, so the effect id a re-driven transition
    /// derives is the same value it derived the first time.
    pub(crate) fn next_effect_slot(&self) -> usize {
        self.effects
            .iter()
            .filter(|effect| effect.turn == self.turn)
            .count()
    }

    /// Commits one effect a transition decided.
    ///
    /// A run may not hold more outstanding effects than
    /// [`AGENT_RUN_MAX_PENDING_EFFECTS`]: an unbounded pending list is an
    /// unbounded durable record. A transition that would cross the bound fails
    /// closed rather than persisting a wait the run cannot hold.
    pub(crate) fn record_effect(
        &mut self,
        mut effect: AgentRunEffect,
    ) -> Result<(), AgentEffectError> {
        if self
            .effects
            .iter()
            .any(|held| held.effect_id == effect.effect_id)
        {
            // The derived id already names an effect this run holds, so the
            // transition is a replay and there is nothing to add.
            return Ok(());
        }
        if self.outstanding_effects().count() >= AGENT_RUN_MAX_PENDING_EFFECTS {
            return Err(AgentEffectError::PendingOverflow {
                maximum: AGENT_RUN_MAX_PENDING_EFFECTS,
            });
        }
        // The committing segment's context rides the effect to its dispatch
        // ticket; a replayed transition returned above, so the stamp is
        // first-commit-only and a re-drive cannot re-stamp a newer segment.
        effect.telemetry = self.telemetry.clone();
        self.effects.push(effect);
        Ok(())
    }

    pub(crate) fn effect_mut(&mut self, effect_id: &AgentEffectId) -> Option<&mut AgentRunEffect> {
        self.effects
            .iter_mut()
            .find(|effect| &effect.effect_id == effect_id)
    }

    pub(crate) fn set_pending_turn(&mut self, turn: AgentModelTurn) {
        self.pending_turn = Some(Box::new(turn));
    }

    pub(crate) fn record_tool_result(&mut self, result: AgentToolResult) {
        if self
            .tool_results
            .iter()
            .any(|held| held.call_id == result.call_id)
        {
            return;
        }
        self.tool_results.push(result);
    }

    /// The session-memory entries this run still owes its store.
    #[must_use]
    pub fn session_outbox(&self) -> &[SessionMemoryEntry] {
        &self.session_outbox
    }

    /// How many more session-memory entries the outbox can hold before it is
    /// full.
    #[must_use]
    pub fn session_outbox_headroom(&self) -> usize {
        AGENT_RUN_SESSION_OUTBOX_CAPACITY.saturating_sub(self.session_outbox.len())
    }

    /// The highest session-memory sequence this run has durably assigned.
    ///
    /// A promotion may only select sequences at or below it: an entry with a
    /// sequence is either already in the store or owed by the outbox the
    /// settle pass flushes before dispatching effects.
    #[must_use]
    pub const fn session_sequence(&self) -> u64 {
        self.session_sequence
    }

    /// The completed goal evaluation the run holds, when one exists.
    #[must_use]
    pub fn goal_evaluation(&self) -> Option<&AgentGoalEvaluationCell> {
        self.goal_evaluation.as_deref()
    }

    /// Whether an evaluation is open — a committed evaluation effect not yet
    /// resolved, or a completed record whose exchange has not settled. One
    /// evaluation at a time: the commit door refuses a second while one is
    /// open.
    #[must_use]
    pub fn has_open_goal_evaluation(&self) -> bool {
        if self
            .goal_evaluation
            .as_deref()
            .is_some_and(|cell| !cell.reported)
        {
            return true;
        }
        self.effects.iter().any(|effect| {
            effect.kind() == crate::effect::AgentRunEffectKind::GoalEvaluationCall
                && effect.is_outstanding()
        })
    }

    /// Records one completed evaluation, replacing any settled predecessor.
    pub(crate) fn record_goal_evaluation(
        &mut self,
        record: crate::evaluation::AgentGoalEvaluationRecord,
        exchange_operation_id: AgentOperationId,
        now: AgentTimestampMillis,
    ) {
        self.goal_evaluation = Some(Box::new(AgentGoalEvaluationCell {
            record: Box::new(record),
            exchange_operation_id,
            completed_at: now,
            reported: false,
            refusal: None,
        }));
    }

    /// Marks the held evaluation reported — the exchange settled — with the
    /// decision door's refusal code when it refused.
    pub(crate) fn settle_goal_evaluation(&mut self, refusal: Option<String>) {
        if let Some(cell) = self.goal_evaluation.as_deref_mut() {
            cell.reported = true;
            cell.refusal = refusal;
        }
    }

    /// The delegations this run has committed, keyed by their identity.
    #[must_use]
    pub const fn delegations(&self) -> &BTreeMap<AgentDelegationId, Box<AgentDelegationCell>> {
        &self.delegations
    }

    /// One delegation's cell, when the run holds it.
    #[must_use]
    pub fn delegation(&self, delegation: &AgentDelegationId) -> Option<&AgentDelegationCell> {
        self.delegations.get(delegation).map(Box::as_ref)
    }

    /// Mutable access to one delegation's cell, for the outcome transition
    /// that settles it.
    pub(crate) fn delegation_mut(
        &mut self,
        delegation: &AgentDelegationId,
    ) -> Option<&mut AgentDelegationCell> {
        self.delegations.get_mut(delegation).map(Box::as_mut)
    }

    /// Commits one delegation cell alongside its send effect.
    ///
    /// Idempotent on the delegation identity: a replayed transition finds the
    /// cell already present and leaves the original in place.
    pub(crate) fn record_delegation(&mut self, cell: AgentDelegationCell) {
        self.delegations
            .entry(cell.record.delegation.clone())
            .or_insert_with(|| Box::new(cell));
    }

    /// How many delegation cells this run retains.
    #[must_use]
    pub fn delegation_count(&self) -> usize {
        self.delegations.len()
    }

    /// Direct children this run has spent against its fan-out ceiling: every
    /// cell whose send is pending or created a child. A `Failed` or
    /// `Conflicted` cell provably created nothing, which is the known terminal
    /// send outcome that releases its debit.
    #[must_use]
    pub fn delegation_fan_out_spent(&self) -> u64 {
        self.delegations
            .values()
            .filter(|cell| {
                matches!(
                    cell.status,
                    AgentDelegationStatus::Pending | AgentDelegationStatus::ChildCreated { .. }
                )
            })
            .count() as u64
    }

    /// Concurrently unsettled direct children: a pending send counts —
    /// deny-when-unknown — and a created child counts until its terminal
    /// result is recorded.
    #[must_use]
    pub fn delegation_active_children(&self) -> u64 {
        self.delegations
            .values()
            .filter(|cell| match cell.status {
                AgentDelegationStatus::Pending => true,
                AgentDelegationStatus::ChildCreated { .. } => !cell.child_settled(),
                _ => false,
            })
            .count() as u64
    }

    /// The descendants this run's live delegations hold: one per child plus
    /// its granted sub-quota, over every cell whose send is pending or created
    /// a child.
    ///
    /// `None` when the spend is unaccountable: a live cell committed before
    /// the descendants dimension existed carries no grant, and a bounded run
    /// cannot know what that child's subtree may still create — the door then
    /// refuses further delegation, deny-when-unknown. A run whose allocation
    /// is unbounded never consults this.
    #[must_use]
    pub fn delegation_descendants_spent(&self) -> Option<u64> {
        let mut spent = 0_u64;
        for cell in self.delegations.values() {
            if !matches!(
                cell.status,
                AgentDelegationStatus::Pending | AgentDelegationStatus::ChildCreated { .. }
            ) {
                continue;
            }
            let granted = cell.record.granted_descendants?;
            spent = spent.saturating_add(1).saturating_add(granted);
        }
        Some(spent)
    }

    /// The descendants spend the run's terminal transition folds into its
    /// consumption: the same live cells as
    /// [`Self::delegation_descendants_spent`], with an unaccountable pre-slice
    /// grant folded at its known floor of one — the child itself — rather than
    /// blocking the settlement the parent is owed.
    #[must_use]
    pub fn delegation_descendants_consumed(&self) -> u64 {
        self.delegations
            .values()
            .filter(|cell| {
                matches!(
                    cell.status,
                    AgentDelegationStatus::Pending | AgentDelegationStatus::ChildCreated { .. }
                )
            })
            .map(|cell| 1_u64.saturating_add(cell.record.granted_descendants.unwrap_or(0)))
            .fold(0, u64::saturating_add)
    }

    /// The workflow invocations this run has committed, keyed by their
    /// identity.
    #[must_use]
    pub const fn workflow_invocations(
        &self,
    ) -> &BTreeMap<AgentWorkflowInvocationId, Box<crate::workflow_tool::AgentWorkflowInvocationCell>>
    {
        &self.workflow_invocations
    }

    /// One workflow invocation's cell, when the run holds it.
    #[must_use]
    pub fn workflow_invocation(
        &self,
        invocation: &AgentWorkflowInvocationId,
    ) -> Option<&crate::workflow_tool::AgentWorkflowInvocationCell> {
        self.workflow_invocations.get(invocation).map(Box::as_ref)
    }

    /// Mutable access to one workflow invocation's cell, for the outcome and
    /// result transitions that settle it.
    pub(crate) fn workflow_invocation_mut(
        &mut self,
        invocation: &AgentWorkflowInvocationId,
    ) -> Option<&mut crate::workflow_tool::AgentWorkflowInvocationCell> {
        self.workflow_invocations
            .get_mut(invocation)
            .map(Box::as_mut)
    }

    /// Commits one workflow-invocation cell alongside its start effect.
    ///
    /// Idempotent on the invocation identity: a replayed transition finds the
    /// cell already present and leaves the original in place.
    pub(crate) fn record_workflow_invocation(
        &mut self,
        cell: crate::workflow_tool::AgentWorkflowInvocationCell,
    ) {
        self.workflow_invocations
            .entry(cell.record.invocation.clone())
            .or_insert_with(|| Box::new(cell));
    }

    /// How many workflow-invocation cells this run retains.
    #[must_use]
    pub fn workflow_invocation_count(&self) -> usize {
        self.workflow_invocations.len()
    }

    /// The delegation authority the assignment carried, when it carried one.
    #[must_use]
    pub fn delegation_envelope(&self) -> Option<&AgentRunDelegationEnvelope> {
        self.delegation_envelope.as_deref()
    }

    /// Stores the delegation authority the accepted assignment carried.
    pub(crate) fn set_delegation_envelope(&mut self, envelope: AgentRunDelegationEnvelope) {
        self.delegation_envelope = Some(Box::new(envelope));
    }

    /// The run's one durable fan-out group, when it holds one.
    #[must_use]
    pub fn fan_in(&self) -> Option<&crate::fan_in::AgentFanInCell> {
        self.fan_in.as_deref()
    }

    /// Mutable access to the group, for the transitions that close, mark, and
    /// resolve it.
    pub(crate) fn fan_in_mut(&mut self) -> Option<&mut crate::fan_in::AgentFanInCell> {
        self.fan_in.as_deref_mut()
    }

    /// Whether a closed, unresolved group awaits its children — the run's
    /// non-resident wait between the await verb and the fan-in resolution.
    #[must_use]
    pub fn awaits_fan_in(&self) -> bool {
        self.fan_in.as_deref().is_some_and(|cell| cell.awaiting())
    }

    /// Joins one committed delegation to the group, opening it — or replacing
    /// a resolved, absorbing predecessor — with the policy fixed from trusted
    /// state in the same compare-and-set
    /// ([specification 8.7]: the fan-in rule is durable before any child can
    /// report). Idempotent on the member: a replayed transition re-inserts
    /// into the same set.
    ///
    /// A closed-but-unresolved group is unreachable here — the planner
    /// refuses a delegation planned after the same turn's await
    /// (`delegation-after-await`), and a parked run plans nothing — and left
    /// untouched if it ever were.
    ///
    /// [specification 8.7]: ../../../docs/plans/rakka-agent/spec.md
    pub(crate) fn join_fan_in(
        &mut self,
        policy: crate::fan_in::AgentFanInPolicy,
        member: crate::fan_in::AgentFanInMemberId,
        turn: u64,
        now: AgentTimestampMillis,
    ) {
        match self.fan_in.as_deref_mut() {
            Some(cell) if cell.resolution.is_none() => {
                if !cell.closed {
                    cell.members.insert(member);
                }
            }
            _ => {
                self.fan_in = Some(Box::new(crate::fan_in::AgentFanInCell::open(
                    policy, member, turn, now,
                )));
            }
        }
    }

    /// Closes the group's membership under the model's await call, returning
    /// whether an open group with members was there to close. Replay-
    /// idempotent: a group already closed under the same call reports closed.
    pub(crate) fn close_fan_in(
        &mut self,
        call_id: crate::model::AgentToolCallId,
        deadline: Option<AgentTimestampMillis>,
        turn: u64,
    ) -> bool {
        let Some(cell) = self.fan_in.as_deref_mut() else {
            return false;
        };
        if cell.resolution.is_some() || cell.members.is_empty() {
            return false;
        }
        if cell.closed {
            return cell.await_call.as_ref() == Some(&call_id);
        }
        cell.closed = true;
        cell.await_call = Some(call_id);
        cell.await_turn = Some(turn);
        cell.deadline = deadline;
        true
    }

    /// Persists the group's resolution, first-writer-wins: the resolution is
    /// absorbing, and a recomputation can never rewrite it.
    pub(crate) fn resolve_fan_in(&mut self, resolution: crate::fan_in::AgentFanInResolution) {
        if let Some(cell) = self.fan_in.as_deref_mut() {
            if cell.resolution.is_none() {
                cell.resolution = Some(resolution);
            }
        }
    }

    /// Bounded receipts of the run's settled memory promotions, newest last.
    #[must_use]
    pub fn memory_promotions(&self) -> &[AgentMemoryPromotionRecord] {
        &self.memory_promotions
    }

    /// Records the bounded receipt of one settled memory promotion, returning
    /// whether it was newly recorded.
    ///
    /// Idempotent on the effect id, so a redelivered result records one
    /// receipt; a receipt past [`AGENT_RUN_MAX_MEMORY_PROMOTIONS`] drops the
    /// oldest, because the private store — not this list — is the source of
    /// truth for what a promotion wrote.
    pub(crate) fn record_memory_promotion(
        &mut self,
        effect_id: AgentEffectId,
        promoted: Vec<AgentPromotedMemoryRef>,
        now: AgentTimestampMillis,
    ) -> bool {
        if self
            .memory_promotions
            .iter()
            .any(|receipt| receipt.effect_id == effect_id)
        {
            return false;
        }
        self.memory_promotions.push(AgentMemoryPromotionRecord {
            effect_id,
            promoted,
            recorded_at: now,
        });
        if self.memory_promotions.len() > AGENT_RUN_MAX_MEMORY_PROMOTIONS {
            self.memory_promotions.remove(0);
        }
        true
    }

    /// Records the current turn — the assistant turn and every tool result it
    /// collected — into the session-memory outbox, to be flushed to the store
    /// after the transition ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// This is where slice 1.11 hands a turn to session memory; the loop keeps no
    /// content of its own. It is called only when the run entity is wired with a
    /// session-memory backend, and it is idempotent on each entry's derived
    /// operation id, so a re-driven `RecordingTurn` transition records the same
    /// entries at the same sequences rather than duplicating them.
    pub(crate) fn record_session_turn(
        &mut self,
        scope: &AgentRunScope,
        now: AgentTimestampMillis,
    ) -> Result<usize, MemoryError> {
        let turn = self.turn;
        let mut recorded = 0;

        if let Some(model_turn) = self.pending_turn.as_deref() {
            let tools: Vec<String> = model_turn
                .tool_calls
                .iter()
                .map(|call| call.tool.to_string())
                .collect();
            let payload = serde_json::json!({
                "text": model_turn.text,
                "has_proposal": model_turn.proposal.is_some(),
                "tool_calls": tools,
            });
            let content =
                AgentTaskContent::inline(payload).map_err(|error| MemoryError::Encoding {
                    message: error.to_string(),
                })?;
            if self.push_session_entry(
                scope,
                turn,
                MemoryEntryRole::Assistant,
                "assistant",
                content,
                None,
                now,
            )? {
                recorded += 1;
            }
        }

        for result in self.tool_results.clone() {
            let discriminator = format!("tool-{}", result.call_id);
            if self.push_session_entry(
                scope,
                turn,
                MemoryEntryRole::ToolResult,
                &discriminator,
                result.content,
                Some(result.call_id.to_string()),
                now,
            )? {
                recorded += 1;
            }
        }

        Ok(recorded)
    }

    /// Records the task's bounded input as the run's opening
    /// [`MemoryEntryRole::User`] session entry, so the first turn's context
    /// snapshot carries the input the run was created to serve
    /// ([specification 13.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The entry is recorded at turn zero — turns count from one, so zero names
    /// what preceded them all — in the same compare-and-set that prepares the
    /// first model call. It is called only when the run entity is wired with a
    /// session-memory backend, and it is idempotent on the entry's derived
    /// operation id, so a re-driven transition records the same entry at the
    /// same sequence rather than duplicating it. The input fits by
    /// construction: task content and session entries share one inline bound.
    pub(crate) fn record_session_input(
        &mut self,
        scope: &AgentRunScope,
        input: &AgentTaskContent,
        now: AgentTimestampMillis,
    ) -> Result<bool, MemoryError> {
        self.push_session_entry(
            scope,
            0,
            MemoryEntryRole::User,
            "input",
            input.clone(),
            None,
            now,
        )
    }

    /// Builds one session entry and pushes it to the outbox, returning whether it
    /// was new. A slot whose derived operation id is already owed is a replay and
    /// adds nothing; a full outbox fails closed.
    #[allow(clippy::too_many_arguments)]
    fn push_session_entry(
        &mut self,
        scope: &AgentRunScope,
        turn: u64,
        role: MemoryEntryRole,
        slot: &str,
        content: AgentTaskContent,
        source: Option<String>,
        now: AgentTimestampMillis,
    ) -> Result<bool, MemoryError> {
        let discriminator = format!("turn-{turn}-{slot}");
        let operation_id = MemoryOperationId::derive(scope, &discriminator).map_err(|error| {
            MemoryError::Encoding {
                message: error.to_string(),
            }
        })?;
        if self
            .session_outbox
            .iter()
            .any(|entry| entry.operation_id == operation_id)
        {
            return Ok(false);
        }
        if self.session_outbox.len() >= AGENT_RUN_SESSION_OUTBOX_CAPACITY {
            return Err(MemoryError::OutboxOverflow {
                maximum: AGENT_RUN_SESSION_OUTBOX_CAPACITY,
            });
        }
        let entry_id = MemoryEntryId::derive(scope, &discriminator).map_err(|error| {
            MemoryError::Encoding {
                message: error.to_string(),
            }
        })?;
        let sequence = MemorySequence::new(self.session_sequence.saturating_add(1));
        let entry = SessionMemoryEntry::new(
            entry_id,
            operation_id,
            sequence,
            role,
            content,
            turn,
            source,
            MemoryClassification::Unclassified,
            now,
        )?;
        self.session_sequence = sequence.get();
        self.session_outbox.push(entry);
        Ok(true)
    }

    /// Drops the session-memory entries a flush has durably appended, keyed by
    /// their operation ids.
    pub(crate) fn clear_flushed_session_entries(&mut self, flushed: &[MemoryOperationId]) {
        self.session_outbox
            .retain(|entry| !flushed.contains(&entry.operation_id));
    }

    /// Clears the working set of the turn that has just been recorded.
    ///
    /// The turn's content is not kept: it belongs in session memory (slice 1.11)
    /// and in artifacts. Resolved effects of the turn leave with it, so the loop
    /// holds only what it is still waiting on — and what it still *owes*: an
    /// indeterminate effect stays until its reconciliation decision, because
    /// dropping it would drop the record that a decision is owed.
    pub(crate) fn clear_turn(&mut self) {
        self.pending_turn = None;
        self.tool_results.clear();
        let turn = self.turn;
        self.effects
            .retain(|effect| effect.turn != turn || effect.blocks_settlement());
        // A grant or checkpoint outlives its effect no longer than the effect
        // itself: once the effect leaves with the turn, the authorization it
        // carried is spent and the checkpoint is history.
        let held: std::collections::BTreeSet<&AgentEffectId> = self
            .effects
            .iter()
            .map(|effect| &effect.effect_id)
            .collect();
        self.checkpoint_grants
            .retain(|grant| held.contains(&grant.effect_id));
        self.open_checkpoints
            .retain(|checkpoint| held.contains(&checkpoint.bound_effect.effect_id));
        self.resync_pending_checkpoint();
    }

    pub(crate) fn begin_turn(&mut self, feedback: Option<String>) {
        self.turn = self.turn.saturating_add(1);
        self.phase = AgentLoopPhase::PreparingContext;
        self.context_snapshot = None;
        self.proposal = None;
        self.feedback = feedback;
    }

    pub(crate) fn set_proposal(&mut self, proposal: AgentRunProposal) {
        self.proposal = Some(proposal);
    }

    pub(crate) fn set_accepted_result(&mut self, result: AgentAcceptedResult) {
        self.accepted_result = Some(Box::new(result));
    }
}

impl VersionedAgentRecord for AgentLoopState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::LoopState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent_workflow::AgentTelemetryContext;

    use super::*;
    use crate::budget::{AgentBudgetAllocation, AgentBudgetGrant, AgentBudgetLimits};
    use crate::definition::AgentToolId;
    use crate::effect::{AgentEffectSpec, AgentRunEffectRequest};
    use crate::identity::{AgentId, AgentRunId, AgentRunScope, TenantId};
    use crate::model::{AgentToolCallId, AgentToolCallRequest};

    const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support-agent").expect("the agent id is valid"),
            AgentRunId::new("run-1").expect("the run id is valid"),
        )
        .expect("the scope is valid")
    }

    fn state() -> AgentLoopState {
        AgentLoopState::started(
            AgentTaskId::new("ticket-1").expect("the task id is valid"),
            None,
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            AgentRevisionNumber::INITIAL,
            AgentRunBudget::allocate(
                AgentBudgetGrant::new(
                    AgentBudgetAllocation::unbounded(),
                    AgentBudgetLimits::unbounded(),
                ),
                AgentTimestampMillis::new(1),
            ),
        )
    }

    fn tool_effect(slot: usize) -> AgentRunEffect {
        let call = AgentToolCallRequest::new(
            AgentToolCallId::new(format!("call-{slot}")).expect("the call id is valid"),
            AgentToolId::new("charge-card").expect("the tool id is valid"),
            serde_json::json!({ "amount": 42 }),
        )
        .expect("the call is bounded");
        AgentRunEffect::new(
            &scope(),
            1,
            slot,
            AgentRunEffectRequest::Tool {
                call: Box::new(call),
            },
            &AgentEffectSpec::non_idempotent(),
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(1),
        )
        .expect("the effect derives")
    }

    fn stamped_context() -> AgentTelemetryContext {
        AgentTelemetryContext {
            trace_parent: Some(TRACE_PARENT.to_string()),
            ..AgentTelemetryContext::default()
        }
    }

    #[test]
    fn a_committed_effect_is_stamped_with_the_committing_segments_context() {
        let mut state = state();
        state.record_telemetry(stamped_context());

        state
            .record_effect(tool_effect(0))
            .expect("the effect commits");

        assert_eq!(
            state.effects()[0].telemetry,
            stamped_context(),
            "the committing segment's context rides the effect to its ticket"
        );
    }

    #[test]
    fn a_replayed_effect_keeps_the_context_of_its_first_commit() {
        let mut state = state();
        state.record_telemetry(stamped_context());
        state
            .record_effect(tool_effect(0))
            .expect("the effect commits");

        // The re-driven transition arrives inside a different segment.
        state.record_telemetry(AgentTelemetryContext::default());
        state
            .record_effect(tool_effect(0))
            .expect("the replay is absorbed");

        assert_eq!(state.effects().len(), 1);
        assert_eq!(
            state.effects()[0].telemetry,
            stamped_context(),
            "a replay must not re-stamp a newer segment onto the first commit"
        );
    }

    #[test]
    fn a_malformed_recorded_context_is_dropped_before_it_is_durable() {
        let mut state = state();
        state.record_telemetry(AgentTelemetryContext {
            trace_parent: Some("not-a-traceparent".to_string()),
            ..AgentTelemetryContext::default()
        });
        assert_eq!(state.telemetry(), &AgentTelemetryContext::default());
    }
}
