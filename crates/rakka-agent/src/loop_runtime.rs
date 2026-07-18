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

use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    AgentEffectId, AgentTimerId, AgentTimestampMillis, HumanCheckpointId, StateSchemaVersion,
};
use serde::{Deserialize, Serialize};

use crate::budget::{AgentBudgetExhaustion, AgentRunBudget};
use crate::definition::{AgentRevisionNumber, AgentTaskDefinitionId};
use crate::effect::{
    AgentEffectError, AgentRunEffect, AgentToolResult, AGENT_RUN_MAX_PENDING_EFFECTS,
};
use crate::identity::{AgentGoalId, AgentOperationId, AgentTaskId};
use crate::memory::AgentContextSnapshotRef;
use crate::model::AgentModelTurn;
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
};
use crate::task::{AgentAcceptedResult, AgentContentDigest, AgentSchemaRef};

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

    /// Whether the phase is a durable wait on an effect.
    #[must_use]
    pub const fn is_waiting(self) -> bool {
        matches!(self, Self::AwaitingModel | Self::AwaitingTools)
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
        }
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

    /// Fences every effect that provably never reached the sink, marking it
    /// cancelled in place.
    ///
    /// Only a `Pending` effect qualifies: the flush hands an effect to the
    /// sink strictly after the transition that marked it `Ready` committed, so
    /// `Pending` proves no dispatch ticket exists and no invocation can be
    /// abandoned by the fence
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    pub(crate) fn fence_unsent_effects(&mut self) -> usize {
        let mut fenced = 0;
        for effect in &mut self.effects {
            if effect.is_pending() {
                effect.status = crate::effect::AgentRunEffectStatus::Cancelled;
                fenced += 1;
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
    pub(crate) fn record_effect(&mut self, effect: AgentRunEffect) -> Result<(), AgentEffectError> {
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
