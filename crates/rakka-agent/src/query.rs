//! Bounded, authoritative operational queries.
//!
//! Owns [`AgentOperationalSnapshot`] and the session view assembled by
//! `AgentRunId`: point queries answered from durable state, bounded in the work
//! they do, and correct even when telemetry is entirely unavailable. An
//! operator asking what an agent is doing gets an answer from the same records
//! the runtime acts on, not from a metrics pipeline
//! ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
//!
//! The split matters and is deliberate:
//!
//! - the **snapshot** is authoritative — derived from the durable run record
//!   alone, returning the durable state revision it read, useful while the
//!   entity is passivated and while every telemetry path is down (scenario
//!   56); and
//! - the **session view** is a projection — it joins the snapshot with the
//!   decision events a sink retained and the trace segments the durable
//!   records carry, exposes its own lag, and never becomes an alternate
//!   execution state machine (scenario 21).
//!
//! Identifiers appear here because an authorized query projection is exactly
//! where [specification 17.3](../../../docs/plans/rakka-agent/spec.md) permits
//! them; they still never label a metric. Continuous and multi-agent
//! projections extend this module in phases 3 and 4.

use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use futures_util::future::{join, join_all};

use rakka_agent_workflow::{
    AgentEffectId, AgentRunId as WorkflowRunId, AgentTelemetryContext, AgentTimestampMillis,
    ArtifactRef, HumanCheckpointId, PrincipalRef, WorkflowDefinitionVersion,
};
use rakka_persistence::{DurableStateStore, Revision};
use serde::{Deserialize, Serialize};

use crate::budget::{AgentBudgetAllocation, AgentBudgetConsumption, AgentBudgetDimension};
use crate::checkpoints::{AgentCheckpoint, AgentCheckpointKind, AgentCheckpointStatus};
use crate::choreography::AgentExchangeState;
use crate::definition::{
    AgentCapabilityId, AgentEffectSafetyClass, AgentPolicyRef, AgentRevisionNumber,
    AgentWorkflowToolId,
};
use crate::delegation::{
    AgentDelegationCancelOutcome, AgentDelegationCell, AgentDelegationChildResult,
    AgentDelegationStatus,
};
use crate::effect::{
    AgentEffectGeneration, AgentRunEffectKind, AgentRunEffectRequest, AgentRunEffectStatus,
};
use crate::evaluation::{
    AgentGoalEvaluationMethodKind, AgentGoalEvaluationOutcome, AgentGoalEvidenceRef,
};
use crate::fan_in::{
    unreported_members, AgentFanInCell, AgentFanInMemberId, AgentFanInPolicy, AgentFanInResolution,
};
use crate::goal::{
    AgentGoalDelegationBudget, AgentGoalStatus, AgentGoalTerminalDecision, AgentGoalWaitReason,
};
use crate::identity::{
    AgentCommunalClaimId, AgentDelegationId, AgentGoalId, AgentHandoffId, AgentId,
    AgentIdentityError, AgentOperationId, AgentRunId, AgentRunScope, AgentTaskId, AgentTaskScope,
    AgentWakeId, AgentWorkflowInvocationId, KnowledgeSpaceId, TenantId,
};
use crate::loop_runtime::{AgentGoalEvaluationCell, AgentLoopPhase, AgentLoopState};
use crate::observability::{
    AgentDecisionEvent, AgentDecisionEventSink, AgentObservabilityError,
    AGENT_DECISION_EVENT_RETENTION,
};
use crate::run::{
    AgentRun, AgentRunError, AgentRunResult, AgentRunSettlementStatus, AgentRunSnapshot,
    AgentRunState, AgentRunStatus, AgentRunTerminalReason,
};
use crate::schema::AgentSchemaPolicy;
use crate::task::{
    AgentAssignmentGeneration, AgentAssignmentStatus, AgentContentDigest, AgentTaskError,
    AgentTaskResult, AgentTaskSnapshot, AgentTaskState, AgentTaskStatus, AgentTaskTerminalReason,
};
use crate::wake_timers::{AgentWakeTimerStatus, AgentWakeTimerStoreState};
use crate::workflow_tool::{
    AgentWorkflowCancelDisposition, AgentWorkflowChildResult, AgentWorkflowInvocationCell,
    AgentWorkflowInvocationStatus,
};

/// How far a requested cancellation has actually got
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md), following
/// [8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The vocabulary was complete from M1 before every state was reachable
/// (slice 1.13 resolution), and slice 4.6 made [`Self::Propagating`]
/// derivable: a cancelled scope with created, unsettled children — or owed
/// child-cancel exchanges — is propagating until every child's terminal
/// outcome records. No state is ever inferred from mere *acceptance* of a
/// cancellation request — the derivation reads what the durable record proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCancellationProgress {
    /// No cancellation has been requested.
    NotRequested,
    /// Cancellation is requested and work the run started is still resolving.
    Requested,
    /// Cancellation is propagating to descendants: a created child — a
    /// delegated task or a workflow run — has not yet recorded its terminal
    /// outcome ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    Propagating,
    /// New work is fenced and nothing the run started is still in flight, but
    /// the terminal transition has not yet committed.
    Quiesced,
    /// An ambiguous consequential effect blocks terminal cancellation until an
    /// operator establishes its outcome
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md);
    /// scenario 57).
    WaitingForReconciliation,
    /// The run reached its cancelled terminal state.
    Completed,
}

impl AgentCancellationProgress {
    /// The serde default of snapshot fields added after M1: records persisted
    /// before them decode as not requested.
    #[must_use]
    pub const fn not_requested() -> Self {
        Self::NotRequested
    }

    /// Stable kebab-case label for views and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::NotRequested => "not-requested",
            Self::Requested => "requested",
            Self::Propagating => "propagating",
            Self::Quiesced => "quiesced",
            Self::WaitingForReconciliation => "waiting-for-reconciliation",
            Self::Completed => "completed",
        }
    }

    /// Derives the progress from what the durable run record proves.
    #[must_use]
    pub fn derive(run: &AgentRun) -> Self {
        let cancellation_recorded = run
            .terminal_reason
            .as_ref()
            .is_some_and(|reason| reason.status() == AgentRunStatus::Cancelled)
            || run.status == AgentRunStatus::Cancelled;
        if !cancellation_recorded {
            return Self::NotRequested;
        }
        if run.status == AgentRunStatus::Cancelled {
            return Self::Completed;
        }
        let effects = run.loop_state.effects();
        if effects
            .iter()
            .any(|effect| matches!(effect.status, AgentRunEffectStatus::Indeterminate))
        {
            return Self::WaitingForReconciliation;
        }
        // Propagation: a created child whose terminal outcome has not
        // returned is a started consequential effect in the cancelled scope
        // ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)); the
        // parent is propagating — and nonterminal — until every delegation
        // and workflow cell holds one. A child parked in its own
        // reconciliation surfaces here as the parent's `Propagating`, and as
        // `WaitingForReconciliation` on the child's own view.
        if run.loop_state.awaits_children() {
            return Self::Propagating;
        }
        // "Still resolving" is the run's own settlement gate, not the effect set
        // alone: a cancelling run whose effects have all resolved can still owe
        // an outstanding result proposal to its task, which the task may yet
        // accept or reject ([`AgentLoopState::awaits_settlement`]). Reading
        // effects only would report `Quiesced` — "nothing the run started is
        // still in flight" — while that proposal is exactly such work.
        if run.loop_state.awaits_settlement() {
            return Self::Requested;
        }
        Self::Quiesced
    }

    /// Derives the task-level progress from what the durable task record
    /// proves ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The task's view spans the assignment: `Propagating` while a live
    /// generation — an open escrow child, or a standing assignment — has not
    /// settled back, `Quiesced` once the ledger closed and only the
    /// finalizing sweep is owed, `Completed` at terminal `Cancelled`.
    /// Reconciliation is the *run's* condition and surfaces on the run's own
    /// derivation; the task honestly reports `Propagating` while it waits.
    ///
    /// The open-escrow half is load-bearing, not belt-and-braces: the
    /// finalization gate is escrow closure, and a continuous root control
    /// task between epoch assignments holds *no* assignment while the epochs
    /// it admitted are still executing. Reading the assignment alone would
    /// report `Quiesced` — "nothing the task started is still in flight" —
    /// over running epoch work, which is exactly the claim an operator acts
    /// on before touching the resources that work is mutating.
    #[must_use]
    pub fn derive_task(task: &crate::task::AgentTaskSnapshot) -> Self {
        if task.status.is_terminal() {
            return if task.status == crate::task::AgentTaskStatus::Cancelled {
                Self::Completed
            } else {
                Self::NotRequested
            };
        }
        if task.cancellation.is_none() {
            return Self::NotRequested;
        }
        if task.assignment.is_some() || task.outstanding_escrow > 0 {
            return Self::Propagating;
        }
        Self::Quiesced
    }
}

/// One pending or unresolved effect, as the authoritative snapshot reports it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): pending
/// effects, attempts, safety classes, grants, and indeterminate work).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPendingEffectView {
    /// The effect's derived identity.
    pub effect_id: AgentEffectId,
    /// Its current dispatch generation.
    pub generation: AgentEffectGeneration,
    /// What it calls.
    pub kind: AgentRunEffectKind,
    /// Where the current generation stands.
    pub status: AgentRunEffectStatus,
    /// Dispatch attempts the current generation has made.
    pub attempts: u32,
    /// Most attempts it may make.
    pub max_attempts: u32,
    /// Its safety class.
    pub safety_class: AgentEffectSafetyClass,
    /// Whether dispatch requires a checkpoint grant.
    pub checkpoint_required: bool,
    /// Whether dispatch requires a security-authorization grant.
    pub authorization_required: bool,
    /// Whether a digest-bound grant is currently held for it.
    pub granted: bool,
}

/// One open checkpoint, as the authoritative snapshot reports it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md):
/// checkpoint/authorization state and bounded resolver requirements).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCheckpointView {
    /// The checkpoint's identity.
    pub checkpoint_id: HumanCheckpointId,
    /// What kind of decision resolves it.
    pub kind: AgentCheckpointKind,
    /// Where it stands.
    pub status: AgentCheckpointStatus,
    /// Roles a resolver must hold.
    pub required_roles: Vec<String>,
    /// How many capabilities a resolver must hold.
    pub required_capabilities: usize,
    /// When it becomes overdue and escalates, when set.
    pub due_at: Option<AgentTimestampMillis>,
    /// When it hard-expires, when set.
    pub expires_at: Option<AgentTimestampMillis>,
}

/// One delegation edge of the delegation graph, as a run's durable cell
/// records it ([specification 17.18](../../../docs/plans/rakka-agent/spec.md):
/// the goal projection assembles delegation/fan-in graphs).
///
/// Identities, stable codes, ceilings, and reference digests only — the
/// delegated input rides the durable record and never this view, and the
/// credential-binding and capability-scope references the resolution carries
/// are deliberately omitted: an observability surface needs neither
/// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalDelegationEdgeView {
    /// The delegation's derived identity.
    pub delegation: AgentDelegationId,
    /// The parent task whose run delegated.
    pub parent_task: AgentTaskId,
    /// The delegating run.
    pub parent_run: AgentRunScope,
    /// Ancestor delegations above this edge, oldest first.
    pub lineage: Vec<AgentDelegationId>,
    /// Depth of the child below the root.
    pub depth: u32,
    /// The skill the parent requested.
    pub requested_skill: AgentCapabilityId,
    /// The specialist agent the catalog resolved.
    pub target_agent: AgentId,
    /// Where the delegation stands, including the created child's identities.
    pub status: AgentDelegationStatus,
    /// When the status settled, when it has.
    pub settled_at: Option<AgentTimestampMillis>,
    /// The child's terminal outcome, once its result returned — references
    /// and codes by construction, never content.
    pub result: Option<AgentDelegationChildResult>,
    /// The settled outcome of the delegation-cancel exchange this child was
    /// chased with, once one settled
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    pub cancel: Option<AgentDelegationCancelOutcome>,
    /// The conserved descendant sub-quota escrowed to the child's subtree.
    pub granted_descendants: Option<u64>,
    /// The narrowed delegation ceilings the child runs under.
    pub budget: Option<AgentGoalDelegationBudget>,
    /// The child's deadline, when the parent set one.
    pub deadline: Option<AgentTimestampMillis>,
    /// The communal knowledge spaces explicitly delegated to the child.
    pub knowledge_spaces: BTreeSet<KnowledgeSpaceId>,
}

impl AgentGoalDelegationEdgeView {
    /// Derives the edge view from one durable delegation cell.
    #[must_use]
    pub fn derive(cell: &AgentDelegationCell) -> Self {
        Self {
            delegation: cell.record.delegation.clone(),
            parent_task: cell.record.parent_task.clone(),
            parent_run: cell.record.parent_run.clone(),
            lineage: cell.record.lineage.clone(),
            depth: cell.record.depth,
            requested_skill: cell.record.requested_skill.clone(),
            target_agent: cell.record.resolved.agent.clone(),
            status: cell.status.clone(),
            settled_at: cell.settled_at,
            result: cell.result.clone(),
            cancel: cell.cancel.clone(),
            granted_descendants: cell.record.granted_descendants,
            budget: cell.record.budget,
            deadline: cell.record.deadline,
            knowledge_spaces: cell.record.knowledge_spaces.clone(),
        }
    }
}

/// A run's one durable fan-out group, as the goal projection reports it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalFanInView {
    /// The policy fixed at group open.
    pub policy: AgentFanInPolicy,
    /// The members, keys of the run's delegation and workflow-invocation
    /// cells.
    pub members: BTreeSet<AgentFanInMemberId>,
    /// The turn that opened the group.
    pub opened_turn: u64,
    /// Whether the await verb has closed membership.
    pub closed: bool,
    /// The turn that closed the group, once closed.
    pub await_turn: Option<u64>,
    /// The parent-side wait deadline, when one is in force.
    pub deadline: Option<AgentTimestampMillis>,
    /// Members the parent's deadline marked timed out.
    pub timed_out: BTreeSet<AgentFanInMemberId>,
    /// The deterministic resolution, once computed.
    pub resolution: Option<AgentFanInResolution>,
    /// The members whose child never reported a terminal outcome: the chase
    /// set a cancelled or resolved-and-moved-on parent still owes requests to
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    pub unreported: Vec<AgentFanInMemberId>,
}

impl AgentGoalFanInView {
    /// Derives the group view from the durable cell and the sibling cell maps
    /// its members key into.
    #[must_use]
    pub fn derive(cell: &AgentFanInCell, loop_state: &AgentLoopState) -> Self {
        Self {
            policy: cell.policy,
            members: cell.members.clone(),
            opened_turn: cell.opened_turn,
            closed: cell.closed,
            await_turn: cell.await_turn,
            deadline: cell.deadline,
            timed_out: cell.timed_out.clone(),
            resolution: cell.resolution.clone(),
            unreported: unreported_members(
                cell,
                loop_state.delegations(),
                loop_state.workflow_invocations(),
            ),
        }
    }
}

/// One workflow-tool invocation, as a run's durable cell records it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): the goal
/// projection assembles workflow invocations).
///
/// Identities, the pinned descriptor coordinates, stable codes, and reference
/// digests only — the invocation input rides the durable record and never
/// this view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalWorkflowInvocationView {
    /// The invocation's derived identity.
    pub invocation: AgentWorkflowInvocationId,
    /// The workflow tool the model called.
    pub workflow_tool: AgentWorkflowToolId,
    /// The descriptor version the invocation validated under.
    pub descriptor_version: AgentRevisionNumber,
    /// The workflow type the invocation starts.
    pub workflow_type: String,
    /// The workflow definition version the invocation pins.
    pub definition_version: WorkflowDefinitionVersion,
    /// The child workflow run this invocation creates or adopts.
    pub child_run: WorkflowRunId,
    /// Where the invocation stands.
    pub status: AgentWorkflowInvocationStatus,
    /// When the status settled, when it has.
    pub settled_at: Option<AgentTimestampMillis>,
    /// The child's terminal outcome, once its result returned — references
    /// and codes by construction, never content.
    pub result: Option<AgentWorkflowChildResult>,
    /// The wind-down disposition of this invocation's cancel request, once a
    /// wind-down decided one
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    pub cancel: Option<AgentWorkflowCancelDisposition>,
    /// The child's deadline, when one is in force.
    pub deadline: Option<AgentTimestampMillis>,
}

impl AgentGoalWorkflowInvocationView {
    /// Derives the invocation view from one durable workflow-invocation cell.
    #[must_use]
    pub fn derive(cell: &AgentWorkflowInvocationCell) -> Self {
        Self {
            invocation: cell.record.invocation.clone(),
            workflow_tool: cell.record.workflow_tool.clone(),
            descriptor_version: cell.record.descriptor_version,
            workflow_type: cell.record.workflow_type.clone(),
            definition_version: cell.record.definition_version.clone(),
            child_run: cell.record.child_run.clone(),
            status: cell.status.clone(),
            settled_at: cell.settled_at,
            result: cell.result.clone(),
            cancel: cell.cancel.clone(),
            deadline: cell.record.deadline,
        }
    }
}

/// One completed goal evaluation and where its report stands, as the goal
/// projection reports it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md):
/// progress/evidence evaluations).
///
/// The record's own contract already fits this surface: stable codes,
/// classed evidence references, and digests — never model text or content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalEvaluationView {
    /// The evaluation's derived identity.
    pub evaluation_id: AgentOperationId,
    /// The evaluator the evaluation ran as.
    pub evaluator: AgentPolicyRef,
    /// How the evaluation executed.
    pub method: AgentGoalEvaluationMethodKind,
    /// The criteria revision the evaluation assessed.
    pub criteria_revision: AgentRevisionNumber,
    /// The verdict.
    pub outcome: AgentGoalEvaluationOutcome,
    /// Bounded, stable reason code for the verdict.
    pub reason_code: String,
    /// The classed evidence references the verdict rests on.
    pub evidence: Vec<AgentGoalEvidenceRef>,
    /// The human resolver, when the method was an authorized review.
    pub evaluated_by: Option<PrincipalRef>,
    /// When the evaluation completed.
    pub evaluated_at: AgentTimestampMillis,
    /// Whether the exchange reporting it to the coordinating task settled.
    pub reported: bool,
    /// The decision door's refusal code, when it refused.
    pub refusal: Option<String>,
}

impl AgentGoalEvaluationView {
    /// Derives the evaluation view from the run's durable evaluation cell.
    #[must_use]
    pub fn derive(cell: &AgentGoalEvaluationCell) -> Self {
        Self {
            evaluation_id: cell.record.evaluation_id.clone(),
            evaluator: cell.record.evaluator.clone(),
            method: cell.record.method,
            criteria_revision: cell.record.criteria_revision,
            outcome: cell.record.outcome,
            reason_code: cell.record.reason_code.clone(),
            evidence: cell.record.evidence.clone(),
            evaluated_by: cell.record.evaluated_by.clone(),
            evaluated_at: cell.record.evaluated_at,
            reported: cell.reported,
            refusal: cell.refusal.clone(),
        }
    }
}

/// One communal claim-append effect a run currently retains
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): shared
/// knowledge references).
///
/// Settled append receipts are pruned from durable run state when their turn
/// completes, so this view enumerates only what the run still holds — the
/// communal graph itself is the authoritative enumeration of landed claims.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalClaimAppendView {
    /// The effect carrying the append.
    pub effect_id: AgentEffectId,
    /// Its current dispatch generation.
    pub generation: AgentEffectGeneration,
    /// The communal knowledge space the claim lands in.
    pub space: KnowledgeSpaceId,
    /// Where the current generation stands.
    pub status: AgentRunEffectStatus,
    /// Dispatch attempts the current generation has made.
    pub attempts: u32,
}

/// One handoff, as a source run's durable cell records it
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identities, stable codes, labels, and counts only — the reason and the
/// context references ride the durable record, never this view, exactly as
/// the delegation edge omits its input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalHandoffView {
    /// The handoff's derived identity.
    pub handoff: AgentHandoffId,
    /// The agent the transfer targets.
    pub target: AgentId,
    /// The skill the source's model requested.
    pub requested_skill: AgentCapabilityId,
    /// Where the handoff stands, as its stable label.
    pub status: String,
    /// The refusal or failure code, when the handoff settled under one.
    pub reason_code: Option<String>,
    /// How many context references the transfer projected — the count, never
    /// the references.
    pub context_refs: usize,
    /// The assignment generation the task minted toward the target, once the
    /// receipt reported one.
    pub target_generation: Option<AgentAssignmentGeneration>,
    /// When the status settled, when it has.
    pub settled_at: Option<AgentTimestampMillis>,
}

impl AgentGoalHandoffView {
    /// Derives the handoff view from one durable handoff cell.
    #[must_use]
    pub fn derive(cell: &crate::coordination::AgentHandoffCell) -> Self {
        use crate::coordination::AgentHandoffStatus;
        let (reason_code, target_generation) = match &cell.status {
            AgentHandoffStatus::Pending => (None, None),
            AgentHandoffStatus::Sent { target_generation } => (None, *target_generation),
            AgentHandoffStatus::Accepted { generation, .. } => (None, Some(*generation)),
            AgentHandoffStatus::Refused { code } | AgentHandoffStatus::Failed { code } => {
                (Some(code.clone()), None)
            }
        };
        Self {
            handoff: cell.record.handoff.clone(),
            target: cell.record.resolved.agent.clone(),
            requested_skill: cell.record.requested_skill.clone(),
            status: cell.status.as_label().to_string(),
            reason_code,
            context_refs: cell.record.context.len(),
            target_generation,
            settled_at: cell.settled_at,
        }
    }
}

/// The multi-agent collaboration state one run's durable record holds: the
/// delegation, fan-in, workflow-invocation, goal-evaluation, and handoff
/// cells of slices 4.2-5.1, plus the claim-append effects still retained
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
///
/// Derived once and consumed twice: [`AgentOperationalSnapshot`] carries it
/// for the run-scoped point query, and the goal view's run nodes carry the
/// same shape, so the two surfaces can never disagree about what a cell
/// holds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentRunCollaborationView {
    /// The delegation edges this run committed, in identity order.
    pub delegations: Vec<AgentGoalDelegationEdgeView>,
    /// The run's one durable fan-out group, when it holds one.
    pub fan_in: Option<AgentGoalFanInView>,
    /// The workflow invocations this run committed, in identity order.
    pub workflow_invocations: Vec<AgentGoalWorkflowInvocationView>,
    /// The completed goal evaluation the run holds, when one exists.
    pub evaluation: Option<AgentGoalEvaluationView>,
    /// The claim-append effects the run still retains.
    pub claim_appends: Vec<AgentGoalClaimAppendView>,
    /// The run's one handoff, when it holds one
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)). Views
    /// persisted before this field load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<AgentGoalHandoffView>,
}

impl AgentRunCollaborationView {
    /// Derives the collaboration view from one run's durable loop state.
    #[must_use]
    pub fn derive(loop_state: &AgentLoopState) -> Self {
        Self {
            delegations: loop_state
                .delegations()
                .values()
                .map(|cell| AgentGoalDelegationEdgeView::derive(cell))
                .collect(),
            fan_in: loop_state
                .fan_in()
                .map(|cell| AgentGoalFanInView::derive(cell, loop_state)),
            workflow_invocations: loop_state
                .workflow_invocations()
                .values()
                .map(|cell| AgentGoalWorkflowInvocationView::derive(cell))
                .collect(),
            evaluation: loop_state
                .goal_evaluation()
                .map(AgentGoalEvaluationView::derive),
            claim_appends: loop_state
                .effects()
                .iter()
                .filter_map(|effect| match &effect.request {
                    AgentRunEffectRequest::ClaimAppend { append, .. } => {
                        Some(AgentGoalClaimAppendView {
                            effect_id: effect.effect_id.clone(),
                            generation: effect.generation,
                            space: append.space.clone(),
                            status: effect.status,
                            attempts: effect.attempts,
                        })
                    }
                    _ => None,
                })
                .collect(),
            handoff: loop_state.handoff().map(AgentGoalHandoffView::derive),
        }
    }

    /// Whether the run holds no collaboration state at all, so snapshots of
    /// pre-collaboration runs serialize exactly as they always have.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.delegations.is_empty()
            && self.fan_in.is_none()
            && self.workflow_invocations.is_empty()
            && self.evaluation.is_none()
            && self.claim_appends.is_empty()
            && self.handoff.is_none()
    }
}

/// The authoritative operational point answer for one run
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
///
/// Everything here derives from the durable run record in one read — no
/// metric, trace, event sink, or Collector is consulted — so the answer stays
/// correct when telemetry is sampled, delayed, dropped, or unavailable
/// (scenario 56), and when the entity is passivated: the status is *logical*
/// lifecycle and never a residency claim
/// ([specification 6.11](../../../docs/plans/rakka-agent/spec.md)). It is a
/// bounded read model, never an alternate execution state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOperationalSnapshot {
    /// The durable state revision this answer was derived from.
    pub revision: Revision,
    /// When the answer was derived.
    pub observed_at: AgentTimestampMillis,
    /// The run's scope.
    pub scope: AgentRunScope,
    /// The run's bounded projection, once it has accepted an assignment.
    ///
    /// Content-redacted: the proposal, accepted result, and feedback the
    /// entity's own reply surface carries are stripped here, because the
    /// operational answer is an observability surface and content stays in
    /// durable state and protected artifacts
    /// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)).
    /// [`Self::has_pending_proposal`] and [`Self::has_accepted_result`] carry
    /// the bounded facts an operator needs.
    pub run: Option<AgentRunSnapshot>,
    /// Whether a result proposal is awaiting the task's decision.
    pub has_pending_proposal: bool,
    /// Whether the task has accepted a result this run proposed. Captured
    /// before redaction: the snapshot's own `accepted_result` is always
    /// stripped, so a reader gating on it would gate on nothing.
    pub has_accepted_result: bool,
    /// The bounded label of the wait the run is in, when it is waiting.
    pub wait_reason: Option<String>,
    /// The earliest durable checkpoint deadline that will wake the run, when
    /// one is set. M1 waits park on checkpoints and effects; timer occurrences
    /// live in the substrate's timer store and join here when the wake
    /// controller lands (Phase 3).
    pub next_wake: Option<AgentTimestampMillis>,
    /// Every effect the run holds that is not yet resolved.
    pub pending_effects: Vec<AgentPendingEffectView>,
    /// Every checkpoint the run has open.
    pub open_checkpoints: Vec<AgentCheckpointView>,
    /// How far a requested cancellation has actually got.
    pub cancellation: AgentCancellationProgress,
    /// The durable decision-event cursor: how many decisions the run has
    /// sequenced ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)).
    pub decision_cursor: u64,
    /// Decision events still owed to the sink.
    pub decisions_owed: usize,
    /// Decision events the bounded ring dropped before they were flushed.
    pub decision_drops: u64,
    /// The run's multi-agent collaboration state: delegation, fan-in,
    /// workflow-invocation, and goal-evaluation cells, plus retained
    /// claim-append effects. Snapshots persisted before this field load
    /// empty, and a run holding no collaboration state serializes without it.
    #[serde(default, skip_serializing_if = "AgentRunCollaborationView::is_empty")]
    pub collaboration: AgentRunCollaborationView,
}

impl AgentOperationalSnapshot {
    /// Derives the snapshot from one durable run record.
    ///
    /// Pure over the record: deriving twice from the same revision yields the
    /// same answer, which is what makes the snapshot cacheable and
    /// comparison-friendly for operators.
    #[must_use]
    pub fn derive(
        state: &AgentRunState,
        revision: Revision,
        observed_at: AgentTimestampMillis,
    ) -> Self {
        let run = state.run();
        let loop_state = run.map(|run| &run.loop_state);
        let pending_effects = loop_state
            .map(|loop_state| {
                loop_state
                    .effects()
                    .iter()
                    .filter(|effect| effect.blocks_settlement())
                    .map(|effect| AgentPendingEffectView {
                        effect_id: effect.effect_id.clone(),
                        generation: effect.generation,
                        kind: effect.kind(),
                        status: effect.status,
                        attempts: effect.attempts,
                        max_attempts: effect.max_attempts,
                        safety_class: effect.safety.class(),
                        checkpoint_required: effect.checkpoint_required,
                        authorization_required: effect.authorization_required,
                        granted: loop_state.grant_for(effect).is_some(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let open_checkpoints: Vec<AgentCheckpointView> = loop_state
            .map(|loop_state| {
                loop_state
                    .open_checkpoints()
                    .iter()
                    // Only checkpoints the run is genuinely waiting on: a
                    // resolved-but-not-yet-dropped one is not a live wait, and
                    // its deadline must not surface as a `next_wake` — the same
                    // `is_waiting` gate `has_open_checkpoint` uses.
                    .filter(|checkpoint| checkpoint.status.is_waiting())
                    .map(|checkpoint: &AgentCheckpoint| AgentCheckpointView {
                        checkpoint_id: checkpoint.checkpoint_id.clone(),
                        kind: checkpoint.kind,
                        status: checkpoint.status,
                        required_roles: checkpoint.required_roles.clone(),
                        required_capabilities: checkpoint.required_capabilities.len(),
                        due_at: checkpoint.due_at,
                        expires_at: checkpoint.expires_at,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let next_wake = open_checkpoints
            .iter()
            .flat_map(|checkpoint| [checkpoint.due_at, checkpoint.expires_at])
            .flatten()
            .min();
        let wait_reason = run.and_then(|run| {
            run.status
                .is_waiting()
                .then(|| run.status.as_label().to_string())
        });
        // One snapshot, read once: capture the bounded pending-proposal and
        // accepted-result facts before the content is redacted, rather than
        // deriving the projection twice.
        let (run_snapshot, has_pending_proposal, has_accepted_result) = match state.snapshot() {
            Some(mut snapshot) => {
                let has_pending_proposal = snapshot.proposal.is_some();
                let has_accepted_result = snapshot.accepted_result.is_some();
                snapshot.proposal = None;
                snapshot.accepted_result = None;
                snapshot.feedback = None;
                (Some(snapshot), has_pending_proposal, has_accepted_result)
            }
            None => (None, false, false),
        };
        Self {
            revision,
            observed_at,
            scope: state.scope().clone(),
            has_pending_proposal,
            has_accepted_result,
            run: run_snapshot,
            wait_reason,
            next_wake,
            pending_effects,
            open_checkpoints,
            cancellation: run.map_or(AgentCancellationProgress::NotRequested, |run| {
                AgentCancellationProgress::derive(run)
            }),
            decision_cursor: loop_state.map_or(0, AgentLoopState::decision_sequence),
            decisions_owed: loop_state.map_or(0, |loop_state| loop_state.decision_outbox().len()),
            decision_drops: loop_state.map_or(0, AgentLoopState::decision_drops),
            collaboration: loop_state
                .map(AgentRunCollaborationView::derive)
                .unwrap_or_default(),
        }
    }
}

/// Answers the authoritative point query for one run scope.
///
/// One durable read, one derivation. `Ok(None)` means no record exists for the
/// scope; an unsupported schema version fails closed exactly as the entity's
/// own recovery would.
pub async fn agent_operational_snapshot<Store>(
    store: &Store,
    scope: &AgentRunScope,
    policy: &AgentSchemaPolicy,
    observed_at: AgentTimestampMillis,
) -> AgentRunResult<Option<AgentOperationalSnapshot>>
where
    Store: DurableStateStore<AgentRunState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(AgentOperationalSnapshot::derive(
        &record.state,
        record.revision,
        observed_at,
    )))
}

/// The authoritative operational answer for one task scope
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)'s
/// continuous-goal half).
///
/// For a continuous root, the embedded wake view answers the M3 checklist's
/// operational facts in one read: schedule and policy revisions in force,
/// lifecycle status and revision, the failure streak and any backoff, the
/// active epochs, the parked occurrences, the goal-window ledger, the monotone
/// counters, and the owed or parked controller-originated re-wakes. It is
/// derived from the durable record alone, so it answers identically while the
/// entity is passivated and while every telemetry path is down. "Next wake"
/// lives in the wake-timer store, not the task record — join it with
/// [`next_pending_wake_for_task`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentTaskOperationalSnapshot {
    /// The durable state revision this answer was derived from.
    pub revision: Revision,
    /// When the answer was derived.
    pub observed_at: AgentTimestampMillis,
    /// The task's scope.
    pub scope: AgentTaskScope,
    /// The task's bounded projection, absent if it was never created.
    ///
    /// Content-redacted: the accepted result's content is stripped, because
    /// the operational answer is an observability surface and content stays
    /// in durable state and protected artifacts
    /// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)).
    /// [`Self::has_accepted_result`] carries the bounded fact an operator
    /// needs.
    pub task: Option<AgentTaskSnapshot>,
    /// Whether the task holds an accepted typed result.
    pub has_accepted_result: bool,
    /// History entries recorded but not yet flushed to the history sink.
    pub owed_history: usize,
    /// How far a requested cancellation of this task has actually got,
    /// derived from the durable record alone
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    /// Snapshots persisted before this field load as not requested.
    #[serde(default = "AgentCancellationProgress::not_requested")]
    pub cancellation: AgentCancellationProgress,
}

impl AgentTaskOperationalSnapshot {
    /// Derives the snapshot from one durable task record.
    ///
    /// Pure over the record: deriving twice from the same revision yields the
    /// same answer.
    #[must_use]
    pub fn derive(
        state: &AgentTaskState,
        revision: Revision,
        observed_at: AgentTimestampMillis,
    ) -> Self {
        let (task, has_accepted_result) = match state.snapshot() {
            Some(mut snapshot) => {
                let has_accepted_result = snapshot.accepted_result.is_some();
                snapshot.accepted_result = None;
                (Some(snapshot), has_accepted_result)
            }
            None => (None, false),
        };
        let cancellation = task
            .as_ref()
            .map_or(AgentCancellationProgress::NotRequested, |snapshot| {
                AgentCancellationProgress::derive_task(snapshot)
            });
        Self {
            revision,
            observed_at,
            scope: state.scope().clone(),
            task,
            has_accepted_result,
            owed_history: state.pending_history().len(),
            cancellation,
        }
    }
}

/// Answers the authoritative point query for one task scope.
///
/// One durable read, one derivation, no entity activation. `Ok(None)` means no
/// record exists for the scope; an unsupported schema version fails closed
/// exactly as the entity's own recovery would.
pub async fn agent_task_operational_snapshot<Store>(
    store: &Store,
    scope: &AgentTaskScope,
    policy: &AgentSchemaPolicy,
    observed_at: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentTaskOperationalSnapshot>>
where
    Store: DurableStateStore<AgentTaskState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(AgentTaskOperationalSnapshot::derive(
        &record.state,
        record.revision,
        observed_at,
    )))
}

/// The earliest pending wake-timer entry of one task: the "next wake" an
/// operator asks about, joined from the wake-timer store's durable state.
///
/// Pure over the recovered timer state, so any node — including one that owns
/// neither the task nor a scanner — computes the same answer from the same
/// record. `None` means no pending entry exists for the task; fired, fenced,
/// and cancelled entries never surface.
#[must_use]
pub fn next_pending_wake_for_task(
    timers: &AgentWakeTimerStoreState,
    tenant: &TenantId,
    task: &AgentTaskId,
) -> Option<(AgentWakeId, AgentTimestampMillis)> {
    timers
        .entries()
        .values()
        .filter(|entry| {
            entry.status() == AgentWakeTimerStatus::Pending
                && entry.binding().tenant() == tenant
                && entry.task() == task
        })
        .map(|entry| (entry.wake_id().clone(), entry.due_time()))
        .min_by_key(|(_, due)| *due)
}

/// Where a persisted trace segment was collected from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentSessionSegmentSource {
    /// The loop state's last committing segment.
    Loop,
    /// An unresolved effect's scheduling segment.
    EffectScheduling,
    /// An open checkpoint's parked segment.
    CheckpointOpen,
}

impl AgentSessionSegmentSource {
    /// Stable kebab-case label for views and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Loop => "loop",
            Self::EffectScheduling => "effect-scheduling",
            Self::CheckpointOpen => "checkpoint-open",
        }
    }
}

/// One trace segment a durable record carries, referenced by the session view
/// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionTraceSegment {
    /// Which durable record carried it.
    pub source: AgentSessionSegmentSource,
    /// The persisted context.
    pub telemetry: AgentTelemetryContext,
}

/// The session observability view for one run, assembled by `AgentRunId`
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md); scenario
/// 21).
///
/// The view is a projection over the authoritative snapshot: it adds what the
/// decision sink retained and the trace segments the durable records carry,
/// and it is explicit about its own freshness — [`Self::decisions_available`]
/// is `false` when the sink could not answer, and [`Self::decision_lag`]
/// counts the durably sequenced decisions the projection has not seen. A
/// telemetry outage degrades this view and never the snapshot inside it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionView {
    /// The authoritative point answer.
    pub snapshot: AgentOperationalSnapshot,
    /// The retained decision events, in sequence order.
    pub decisions: Vec<AgentDecisionEvent>,
    /// Whether the decision sink answered at all.
    pub decisions_available: bool,
    /// Durably sequenced decisions the projection has not seen: the durable
    /// cursor minus the highest retained sequence.
    pub decision_lag: u64,
    /// The trace segments the durable records carry, for linking into the
    /// tracing backend.
    pub trace_segments: Vec<AgentSessionTraceSegment>,
}

/// Assembles the session view for one run scope.
///
/// One durable read plus one bounded sink read. A missing or failing sink
/// leaves [`AgentSessionView::decisions`] empty with
/// [`AgentSessionView::decisions_available`] `false` — the durable snapshot
/// half is never degraded by the telemetry half (scenario 56).
pub async fn assemble_agent_session_view<Store>(
    store: &Store,
    scope: &AgentRunScope,
    policy: &AgentSchemaPolicy,
    decisions: Option<&dyn AgentDecisionEventSink>,
    observed_at: AgentTimestampMillis,
) -> AgentRunResult<Option<AgentSessionView>>
where
    Store: DurableStateStore<AgentRunState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    let snapshot = AgentOperationalSnapshot::derive(&record.state, record.revision, observed_at);

    let mut trace_segments = Vec::new();
    if let Some(run) = record.state.run() {
        collect_segment(
            &mut trace_segments,
            AgentSessionSegmentSource::Loop,
            run.loop_state.telemetry(),
        );
        for effect in run.loop_state.effects() {
            if effect.blocks_settlement() {
                collect_segment(
                    &mut trace_segments,
                    AgentSessionSegmentSource::EffectScheduling,
                    &effect.telemetry,
                );
            }
        }
        for checkpoint in run.loop_state.open_checkpoints() {
            collect_segment(
                &mut trace_segments,
                AgentSessionSegmentSource::CheckpointOpen,
                &checkpoint.telemetry,
            );
        }
    }

    let (events, decisions_available) = match decisions {
        None => (Vec::new(), false),
        Some(sink) => read_retained_decisions(sink, scope).await,
    };
    let projected = events.iter().map(|event| event.sequence).max().unwrap_or(0);
    let decision_lag = snapshot.decision_cursor.saturating_sub(projected);

    Ok(Some(AgentSessionView {
        snapshot,
        decisions: events,
        decisions_available,
        decision_lag,
        trace_segments,
    }))
}

/// Reads everything the sink retains for one run, paging across retention
/// holes.
///
/// A hole in the retained stream — the ring dropped an unflushed event, or an
/// identity-formation failure consumed a sequence — answers
/// [`AgentObservabilityError::ReplayWindowExpired`] at the hole, naming the
/// floor past it. For the session view that is not an outage: the view resumes
/// at the floor and keeps collecting, because "the sink retained `[1, 2, 4]`"
/// must degrade to showing 1, 2, and 4 — never to a blank view claiming the
/// sink is down. The loss itself stays visible through
/// [`AgentOperationalSnapshot::decision_drops`] and
/// [`AgentSessionView::decision_lag`]. Only a sink *fault* degrades the view
/// to unavailable.
async fn read_retained_decisions(
    sink: &dyn AgentDecisionEventSink,
    scope: &AgentRunScope,
) -> (Vec<AgentDecisionEvent>, bool) {
    let mut collected: Vec<AgentDecisionEvent> = Vec::new();
    let mut after = 0_u64;
    // Bounded defensively: a compliant sink retains at most
    // `AGENT_DECISION_EVENT_RETENTION` events, and every pass below either
    // collects at least one of them or jumps one hole between two of them.
    for _ in 0..=(2 * AGENT_DECISION_EVENT_RETENTION) {
        let remaining = AGENT_DECISION_EVENT_RETENTION.saturating_sub(collected.len());
        if remaining == 0 {
            break;
        }
        match sink.read(scope, after, remaining).await {
            Ok(page) => {
                let advanced = page.events.last().map(|event| event.sequence);
                collected.extend(page.events);
                match advanced {
                    Some(sequence) if page.has_more && sequence > after => after = sequence,
                    _ => break,
                }
            }
            Err(AgentObservabilityError::ReplayWindowExpired { oldest_retained }) => {
                match oldest_retained {
                    // Resuming *at* the floor means positioning after the one
                    // before it; anything else would loop in place.
                    Some(oldest) if oldest.saturating_sub(1) > after => after = oldest - 1,
                    _ => break,
                }
            }
            Err(_) => return (Vec::new(), false),
        }
    }
    (collected, true)
}

fn collect_segment(
    segments: &mut Vec<AgentSessionTraceSegment>,
    source: AgentSessionSegmentSource,
    telemetry: &AgentTelemetryContext,
) {
    if telemetry.trace_parent.is_none() && telemetry.span_links.is_empty() {
        return;
    }
    segments.push(AgentSessionTraceSegment {
        source,
        telemetry: telemetry.clone(),
    });
}

/// Most task nodes one goal view assembles, runs included one-for-one.
///
/// The per-run fan-out and lineage-depth bounds cap the tree's shape but not
/// its total mass, so the view carries its own node budget; a larger tree
/// truncates with an explicit [`AgentGoalViewOmission`] per unvisited task
/// rather than refusing — a view that refuses on a big tree answers nothing
/// about exactly the goal an operator most needs to see.
pub const AGENT_GOAL_VIEW_MAX_TASKS: usize = 64;

/// Most joined claim references one goal view carries.
pub const AGENT_GOAL_VIEW_MAX_CLAIMS: usize = 64;

/// Stable omission codes of the goal view: the reasons
/// [`AgentGoalViewOmission`] records for a task that did not assemble, and
/// [`AgentGoalTaskNode::run_omission`] records for a resolved run that did
/// not join.
pub mod agent_goal_view_omission_code {
    /// A created child's task record does not exist in the store.
    pub const RECORD_MISSING: &str = "record-missing";
    /// The task's resolved run has no record in the store.
    pub const RUN_RECORD_MISSING: &str = "run-record-missing";
    /// The run record exists but the run never durably accepted.
    pub const RUN_NOT_ACCEPTED: &str = "run-not-accepted";
    /// The task record's schema version is not readable under the caller's
    /// policy.
    pub const SCHEMA_UNSUPPORTED: &str = "schema-unsupported";
    /// The run record's schema version is not readable under the caller's
    /// policy.
    pub const RUN_SCHEMA_UNSUPPORTED: &str = "run-schema-unsupported";
    /// The child's recorded provenance does not name the edge that reached
    /// it: the linkage fails closed rather than joining a forged child.
    pub const UNLINKED_PROVENANCE: &str = "unlinked-provenance";
    /// The child is bound to a different goal.
    pub const FOREIGN_GOAL: &str = "foreign-goal";
    /// The view's node budget was exhausted before this task was visited.
    pub const NODE_BUDGET_EXHAUSTED: &str = "node-budget-exhausted";
}

/// One task the goal view knows of but did not assemble, with the stable
/// reason ([`agent_goal_view_omission_code`]).
///
/// Omissions are how the view stays honest without failing whole: a missing
/// record, an unreadable schema, a forged linkage, or an exhausted node
/// budget marks its task here, and only the *root* record failing fails the
/// call. A task appears at most once, and never both here and in
/// [`AgentGoalView::tasks`] — a run that fails to join marks its assembled
/// task's [`AgentGoalTaskNode::run_omission`] instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalViewOmission {
    /// The task that was not assembled.
    pub task: AgentTaskId,
    /// Why, as a stable code.
    pub code: String,
}

/// The goal contract as the authorized goal view reports it
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The objective *summary* is deliberately redacted — it is user content, and
/// content never rides an observability surface
/// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)); the
/// artifact reference carries the authorized pointer. The terminal decision
/// keeps its evaluation reference whole: evaluator, criteria revision,
/// attestation digest, and classed evidence references are exactly what "the
/// terminal goal decision" of
/// [specification 17.18](../../../docs/plans/rakka-agent/spec.md) means.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalContractView {
    /// The goal-contract status.
    pub status: AgentGoalStatus,
    /// The status revision commanded transitions fence on.
    pub status_revision: AgentRevisionNumber,
    /// The spec revision in force.
    pub spec_revision: AgentRevisionNumber,
    /// The criteria revision a completion evaluation must assess.
    pub criteria_revision: AgentRevisionNumber,
    /// Content digest of the criteria, when the application fingerprints
    /// them.
    pub criteria_digest: Option<AgentContentDigest>,
    /// The objective artifact reference; the summary is redacted.
    pub objective_artifact: Option<ArtifactRef>,
    /// The configured completion evaluator, when the spec names one.
    pub evaluator: Option<AgentPolicyRef>,
    /// Evidence classes a completion evaluation must present.
    pub required_evidence: BTreeSet<String>,
    /// Priority relative to the owner's other goals.
    pub priority: Option<u32>,
    /// The goal's own deadline.
    pub deadline: Option<AgentTimestampMillis>,
    /// The conserved goal allocation.
    pub allocation: AgentBudgetAllocation,
    /// The delegation ceilings the goal runs under.
    pub delegation_ceilings: Option<AgentGoalDelegationBudget>,
    /// The communal knowledge spaces the goal's grant statement names.
    pub knowledge_spaces: BTreeSet<KnowledgeSpaceId>,
    /// Why the goal is waiting, while it is.
    pub wait: Option<AgentGoalWaitReason>,
    /// The terminal decision, once one recorded — reason and the evaluation
    /// reference it rests on.
    pub terminal: Option<AgentGoalTerminalDecision>,
    /// When the goal was first activated.
    pub activated_at: Option<AgentTimestampMillis>,
    /// When the terminal decision was made.
    pub decided_at: Option<AgentTimestampMillis>,
}

impl AgentGoalContractView {
    /// Derives the contract view from the root task's durable goal record.
    #[must_use]
    pub fn derive(state: &crate::goal::AgentGoalState) -> Self {
        let spec_revision = state.spec();
        let spec = spec_revision.spec();
        Self {
            status: state.status(),
            status_revision: state.status_revision(),
            spec_revision: spec_revision.revision(),
            criteria_revision: spec.criteria.revision,
            criteria_digest: spec.criteria.digest.clone(),
            objective_artifact: spec.objective.artifact.clone(),
            evaluator: spec.evaluator.clone(),
            required_evidence: spec.required_evidence.clone(),
            priority: spec.priority,
            deadline: spec.deadline,
            allocation: spec.allocation,
            delegation_ceilings: spec.delegation,
            knowledge_spaces: spec.knowledge_spaces.clone(),
            wait: state.wait().cloned(),
            terminal: state.terminal().cloned(),
            activated_at: state.activated_at(),
            decided_at: state.decided_at(),
        }
    }
}

/// One task's current assignment, as the goal view reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalAssignmentView {
    /// The generation this assignment owns.
    pub generation: AgentAssignmentGeneration,
    /// The assigned agent.
    pub agent: AgentId,
    /// The run created to serve this generation.
    pub run: AgentRunId,
    /// Whether the run has durably accepted.
    pub status: AgentAssignmentStatus,
    /// When the decision was recorded.
    pub assigned_at: AgentTimestampMillis,
}

/// One task node of the goal view: the root, a delegated specialist child,
/// or a continuous root's epoch task
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): root and
/// specialist tasks).
///
/// Every node carries the durable revision it was derived from — the view
/// spans independently committed records, so freshness is per node, never
/// global.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalTaskNode {
    /// The durable state revision this node was derived from.
    pub revision: Revision,
    /// The task's scope.
    pub scope: AgentTaskScope,
    /// The task that created it, when one did.
    pub parent: Option<AgentTaskId>,
    /// The delegation that created it, when one did — the graph's back-edge.
    pub created_by_delegation: Option<AgentDelegationId>,
    /// Depth below the root: zero for the root and for epoch tasks.
    pub depth: u32,
    /// Whether this is the goal's coordinating root task.
    pub is_root: bool,
    /// Whether this task was reached as a continuous root's epoch.
    pub is_epoch: bool,
    /// The task's lifecycle status.
    pub status: AgentTaskStatus,
    /// How far a requested cancellation of this task has actually got.
    pub cancellation: AgentCancellationProgress,
    /// The current assignment, when one stands.
    pub assignment: Option<AgentGoalAssignmentView>,
    /// The latest handoff recorded on this task, when one was
    /// ([specification 14.2](../../../docs/plans/rakka-agent/spec.md):
    /// source and target lineage in authorized metadata). It carries the
    /// source pair the task record itself no longer names, which is what
    /// keeps a handed-off generation out of the earlier-generations gap.
    /// Nodes assembled before the field existed load without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<AgentGoalTaskHandoffView>,
    /// The highest assignment generation the task has decided — the one
    /// whose run this view resolves, standing assignment or not. A value
    /// above one signals earlier runs this view does not assemble: their
    /// scopes are not derivable from the task record — beyond the latest
    /// handoff, whose source pair [`Self::handoff`] carries — and full run
    /// history is the task projection's job.
    pub assignment_generation: AgentAssignmentGeneration,
    /// How many assignment generations the task has consumed.
    pub assignments: u32,
    /// Why the resolved run did not join ([`agent_goal_view_omission_code`]),
    /// when it did not. Children are discovered through the run's delegation
    /// cells, so a marker here also means this node's delegated subtree is
    /// unknown rather than absent — the escrow counters still witness
    /// anything outstanding below it.
    pub run_omission: Option<String>,
    /// Escrow children the task still holds open — the cancellation
    /// finalization gate ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    pub outstanding_escrow: usize,
    /// The total the task's escrow ledger holds.
    pub escrow_allocation: AgentBudgetAllocation,
    /// What the task and its settled children have consumed.
    pub escrow_consumed: AgentBudgetConsumption,
    /// Whether the task holds an accepted typed result; the content itself
    /// never rides this view.
    pub has_accepted_result: bool,
    /// Why the task reached its terminal status, once it did.
    pub terminal_reason: Option<AgentTaskTerminalReason>,
    /// How many result proposals deterministic rules have refused.
    pub rejection_count: u32,
    /// The time of the task's last accepted transition.
    pub updated_at: AgentTimestampMillis,
}

/// The latest handoff of one task node, as its materialized provenance
/// records it ([specification 8.9 and 14.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identities, the stable status label, and timestamps only — the reason and
/// context references never ride the view. Only the latest hop is
/// materialized; the chain is the task history's job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalTaskHandoffView {
    /// The handoff's derived identity.
    pub handoff: AgentHandoffId,
    /// The agent whose run initiated the transfer.
    pub source_agent: AgentId,
    /// The assignment generation the source served.
    pub source_generation: AgentAssignmentGeneration,
    /// The source run — the pair the task record itself no longer names
    /// after the transfer.
    pub source_run: AgentRunId,
    /// The agent the transfer targets.
    pub target: AgentId,
    /// The assignment generation minted toward the target, once one was.
    pub target_generation: Option<AgentAssignmentGeneration>,
    /// Where the transfer stands, as its stable label.
    pub status: String,
    /// When the transfer was recorded.
    pub recorded_at: AgentTimestampMillis,
}

/// One run node of the goal view
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): root and
/// specialist runs). The delegation and workflow edges live on
/// [`Self::collaboration`], so the graph and its nodes can never disagree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalRunNode {
    /// The durable state revision this node was derived from.
    pub revision: Revision,
    /// The run's scope.
    pub scope: AgentRunScope,
    /// The task it serves.
    pub task: AgentTaskId,
    /// The assignment generation it owns.
    pub generation: AgentAssignmentGeneration,
    /// Its lifecycle status.
    pub status: AgentRunStatus,
    /// Where its loop stands.
    pub phase: AgentLoopPhase,
    /// The turn it is on.
    pub turn: u64,
    /// How far a requested cancellation has actually got.
    pub cancellation: AgentCancellationProgress,
    /// Its own durable budget ledger.
    pub budget: crate::budget::AgentRunBudget,
    /// How far it has got in handing its escrow back to its task.
    pub settlement: AgentRunSettlementStatus,
    /// How many effects it is still waiting on.
    pub outstanding_effects: usize,
    /// Whether a result proposal is awaiting the task's decision; the
    /// proposal itself never rides this view.
    pub has_pending_proposal: bool,
    /// Why it reached its terminal status, once it did.
    pub terminal_reason: Option<AgentRunTerminalReason>,
    /// Its delegation, fan-in, workflow-invocation, evaluation, and
    /// claim-append state.
    pub collaboration: AgentRunCollaborationView,
}

impl AgentGoalRunNode {
    /// Derives the run node from one durable run record, once the run has
    /// accepted.
    fn derive(run: &AgentRun, scope: AgentRunScope, revision: Revision) -> Self {
        Self {
            revision,
            scope,
            task: run.task().clone(),
            generation: run.generation,
            status: run.status,
            phase: run.loop_state.phase(),
            turn: run.loop_state.turn(),
            cancellation: AgentCancellationProgress::derive(run),
            budget: *run.loop_state.budget(),
            settlement: run.settlement,
            outstanding_effects: run.loop_state.outstanding_effects().count(),
            has_pending_proposal: run.loop_state.proposal().is_some(),
            terminal_reason: run.terminal_reason.clone(),
            collaboration: AgentRunCollaborationView::derive(&run.loop_state),
        }
    }
}

/// The goal's budget position, rolled up from the records the view read
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): budget
/// allocation and consumption).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalBudgetView {
    /// The conserved allocation the goal contract grants.
    pub allocation: AgentBudgetAllocation,
    /// The total the root task's escrow ledger holds.
    pub root_escrow_allocation: AgentBudgetAllocation,
    /// What the root and its settled children have durably consumed — the
    /// conserved, folded number.
    pub root_escrow_consumed: AgentBudgetConsumption,
    /// Escrow children the root still holds open.
    pub root_outstanding_children: usize,
    /// Consumption summed over the *loaded, unsettled* run nodes: advisory
    /// visibility into spend the ledgers have not folded yet. It is a sum of
    /// independently committed records, so it is never a conserved figure and
    /// must never authorize anything.
    pub live_consumption: AgentBudgetConsumption,
}

/// A joined reference to one communal claim recorded under the goal
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): shared
/// knowledge references). Identities and provenance only, never claim
/// content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalClaimRef {
    /// The claim's identity.
    pub claim: AgentCommunalClaimId,
    /// The communal knowledge space it lives in.
    pub space: KnowledgeSpaceId,
    /// The agent that asserted it.
    pub agent: AgentId,
    /// The task the assertion served, when recorded.
    pub task: Option<AgentTaskId>,
    /// The run that produced it, when recorded.
    pub run: Option<AgentRunId>,
    /// The delegation it was produced under, when recorded.
    pub delegation: Option<AgentDelegationId>,
}

impl AgentGoalClaimRef {
    /// Creates a claim reference carrying its required identities; the
    /// optional provenance joins through the `with_` builders.
    #[must_use]
    pub const fn new(claim: AgentCommunalClaimId, space: KnowledgeSpaceId, agent: AgentId) -> Self {
        Self {
            claim,
            space,
            agent,
            task: None,
            run: None,
            delegation: None,
        }
    }

    /// Sets the task the assertion served.
    #[must_use]
    pub fn with_task(mut self, task: AgentTaskId) -> Self {
        self.task = Some(task);
        self
    }

    /// Sets the run that produced the assertion.
    #[must_use]
    pub fn with_run(mut self, run: AgentRunId) -> Self {
        self.run = Some(run);
        self
    }

    /// Sets the delegation the assertion was produced under.
    #[must_use]
    pub fn with_delegation(mut self, delegation: AgentDelegationId) -> Self {
        self.delegation = Some(delegation);
        self
    }
}

/// Error one goal-claim source failed with.
///
/// The assembling view treats any failure as a degraded projection source —
/// [`AgentGoalView::claims_available`] goes `false` and the durable half of
/// the view is never degraded (the scenario-56 posture) — so the error is
/// diagnostic detail, never control flow.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AgentGoalClaimSourceError {
    /// Stable machine-readable code.
    pub code: String,
    /// Bounded human-readable detail.
    pub message: String,
}

impl AgentGoalClaimSourceError {
    /// Creates a source error carrying a stable code and bounded detail.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl Display for AgentGoalClaimSourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "goal claim source failed [{}]: {}",
            self.code, self.message
        )
    }
}

impl Error for AgentGoalClaimSourceError {}

/// Future type of [`AgentGoalClaimSource`] operations.
pub type AgentGoalClaimFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Vec<AgentGoalClaimRef>, AgentGoalClaimSourceError>> + Send + 'a>,
>;

/// A source of joined communal-claim references for one goal.
///
/// Settled claim receipts are pruned from durable run state when their turn
/// completes, so the communal graph is the only complete enumeration of what
/// a goal's runs recorded — and the graph is a separate store the view joins
/// through this port, exactly as the session view joins its decision sink.
/// The implementation decides which spaces it serves; an absent or failing
/// source degrades the projection half of the view and never the durable
/// half.
pub trait AgentGoalClaimSource: Send + Sync {
    /// Stable backend label for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// Reads up to `limit` claim references recorded under `goal`, in stable
    /// claim-id order.
    fn claims_for_goal<'a>(
        &'a self,
        tenant: &'a TenantId,
        goal: &'a AgentGoalId,
        limit: usize,
    ) -> AgentGoalClaimFuture<'a>;
}

/// The authorized goal view: one goal's tasks, runs, delegation graph,
/// workflow links, evaluations, evidence, budgets, and cancellation state,
/// assembled from durable state alone
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
///
/// The view is a *causal cut* over independently committed records, never a
/// snapshot: every node carries its own durable revision, and
/// [`Self::root_revision`] is the only anchor a caller may fence against. It
/// is a bounded read model and must never authorize or advance execution —
/// durable goal/task/run state remains the one correctness source.
///
/// Resolution is convention-bound: with no goal→task index, the root is
/// reachable exactly because the goal identity defaults to the root task's
/// value ([`AgentGoalId::for_root_task`], the recorded resolution of open
/// decision 14). Each task resolves its highest-generation run — through the
/// standing assignment, re-derived from the assignee once acceptance
/// cleared it, or resolved through a pending handoff's recorded source —
/// while generations before the latest handoff are not assembled: their
/// scopes are not derivable from the task record, the per-node generation
/// counts and handoff provenance make that gap explicit, and full run
/// history belongs to the task projection. Teams and moderated
/// conversations join in M5; the non-exhaustive views keep that additive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentGoalView {
    /// The tenant the view is scoped to.
    pub tenant: TenantId,
    /// The goal.
    pub goal: AgentGoalId,
    /// When the view was assembled.
    pub observed_at: AgentTimestampMillis,
    /// The coordinating root task.
    pub root_task: AgentTaskId,
    /// The root record's durable revision: the authoritative anchor.
    pub root_revision: Revision,
    /// How many durable records the assembly read.
    pub records_read: usize,
    /// Whether the node budget cut the traversal short.
    pub truncated: bool,
    /// Every task the view knows of but did not assemble, with its reason.
    pub omissions: Vec<AgentGoalViewOmission>,
    /// The goal contract.
    pub contract: AgentGoalContractView,
    /// How far a requested cancellation of the whole goal has actually got,
    /// derived at the root ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    pub cancellation: AgentCancellationProgress,
    /// The goal's budget position.
    pub budget: AgentGoalBudgetView,
    /// The assembled task nodes, root first, in breadth-first order.
    pub tasks: Vec<AgentGoalTaskNode>,
    /// The assembled run nodes, in the same traversal order.
    pub runs: Vec<AgentGoalRunNode>,
    /// Joined communal-claim references, when a source answered.
    pub claims: Vec<AgentGoalClaimRef>,
    /// Whether a claim source answered at all. `false` with empty
    /// [`Self::claims`] means the join is degraded or unwired, never that no
    /// claims exist.
    pub claims_available: bool,
    /// Whether the claim join was cut at [`AGENT_GOAL_VIEW_MAX_CLAIMS`]: the
    /// source held more references than the view carries.
    pub claims_truncated: bool,
    /// The stable code the claim source failed with when the join degraded —
    /// [`AgentGoalClaimSourceError::code`], never its free-text detail.
    /// `None` beside a `false` [`Self::claims_available`] means no source is
    /// wired at all.
    pub claims_error_code: Option<String>,
}

/// Error raised assembling a goal view, attributed to the store that failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AgentGoalViewError {
    /// Deriving an identity or scope failed.
    Identity(AgentIdentityError),
    /// The task store failed to load or validate a record.
    Task(AgentTaskError),
    /// The run store failed to load or validate a record.
    Run(Box<AgentRunError>),
}

impl AgentGoalViewError {
    /// The stable machine-readable error code of the underlying failure.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Task(error) => error.code(),
            Self::Run(error) => error.code(),
        }
    }
}

impl Display for AgentGoalViewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "goal view identity error: {error}"),
            Self::Task(error) => write!(f, "goal view task-store error: {error}"),
            Self::Run(error) => write!(f, "goal view run-store error: {error}"),
        }
    }
}

impl Error for AgentGoalViewError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::Run(error) => Some(error),
        }
    }
}

impl From<AgentIdentityError> for AgentGoalViewError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentTaskError> for AgentGoalViewError {
    fn from(error: AgentTaskError) -> Self {
        Self::Task(error)
    }
}

impl From<AgentRunError> for AgentGoalViewError {
    fn from(error: AgentRunError) -> Self {
        Self::Run(Box::new(error))
    }
}

/// Result type for goal-view assembly.
pub type AgentGoalViewResult<T> = Result<T, AgentGoalViewError>;

/// One frontier entry of the bounded traversal.
struct GoalViewFrontierEntry {
    task: AgentTaskId,
    parent: AgentTaskId,
    /// The delegation edge that reached the child; `None` for an epoch.
    via: Option<AgentDelegationId>,
}

/// Assembles the goal view for one tenant-scoped goal id.
///
/// One durable read per record, no entity activation, tenant-scoped by
/// construction of every derived scope. `Ok(None)` means no goal-rooted task
/// exists under this id — including a task id that exists but coordinates no
/// goal, which answers identically so the view never turns a goal id probe
/// into an existence oracle for tasks.
///
/// The traversal is breadth-first from the root task, each wave of
/// independent records loaded concurrently: each task's current assignment
/// resolves its run, each run's `ChildCreated` delegation cells resolve its
/// children, and a continuous root's admitted epochs join from the wake
/// controller's status view. Children that cannot be joined honestly —
/// missing records, unreadable schemas, provenance that does not name the
/// traversing edge, foreign goal bindings — become
/// [`AgentGoalViewOmission`]s rather than failures; a resolved run that
/// cannot be joined marks its assembled task's
/// [`AgentGoalTaskNode::run_omission`] instead; only the root record failing
/// fails the call. At most [`AGENT_GOAL_VIEW_MAX_TASKS`] task nodes
/// assemble; the rest truncate with explicit omissions.
///
/// `claims` joins shared-knowledge references when a source is wired; an
/// absent or failing source leaves [`AgentGoalView::claims_available`]
/// `false` with the durable half of the view intact, a failure's stable code
/// on [`AgentGoalView::claims_error_code`], and a join cut at
/// [`AGENT_GOAL_VIEW_MAX_CLAIMS`] sets [`AgentGoalView::claims_truncated`].
pub async fn assemble_agent_goal_view<Tasks, Runs>(
    tasks: &Tasks,
    runs: &Runs,
    tenant: &TenantId,
    goal: &AgentGoalId,
    policy: &AgentSchemaPolicy,
    claims: Option<&dyn AgentGoalClaimSource>,
    observed_at: AgentTimestampMillis,
) -> AgentGoalViewResult<Option<AgentGoalView>>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Runs: DurableStateStore<AgentRunState>,
{
    assemble_goal_view_gated(
        tasks,
        runs,
        tenant,
        goal,
        policy,
        claims,
        None,
        AGENT_GOAL_VIEW_MAX_TASKS,
        observed_at,
    )
    .await
}

/// [`assemble_agent_goal_view`] under a tighter node budget.
///
/// The budget is clamped to `1..=`[`AGENT_GOAL_VIEW_MAX_TASKS`]: a caller
/// may want a cheaper view of a large goal, never a more expensive one.
/// Everything unvisited truncates with the explicit
/// [`agent_goal_view_omission_code::NODE_BUDGET_EXHAUSTED`] omission.
#[allow(clippy::too_many_arguments)]
pub async fn assemble_agent_goal_view_bounded<Tasks, Runs>(
    tasks: &Tasks,
    runs: &Runs,
    tenant: &TenantId,
    goal: &AgentGoalId,
    policy: &AgentSchemaPolicy,
    claims: Option<&dyn AgentGoalClaimSource>,
    max_tasks: usize,
    observed_at: AgentTimestampMillis,
) -> AgentGoalViewResult<Option<AgentGoalView>>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Runs: DurableStateStore<AgentRunState>,
{
    assemble_goal_view_gated(
        tasks,
        runs,
        tenant,
        goal,
        policy,
        claims,
        None,
        max_tasks.clamp(1, AGENT_GOAL_VIEW_MAX_TASKS),
        observed_at,
    )
    .await
}

/// Assembles the goal view for the goal's owner, failing closed on anyone
/// else.
///
/// The owner check is the one authorization the goal record itself can
/// answer: [`crate::goal::AgentGoalSpec::owner`] is the principal the goal
/// is accountable to. A non-owner receives `Ok(None)` — byte-identical to a
/// missing goal, so authorization never leaks existence
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)) — and the
/// denial short-circuits after the root read alone: no fan-out happens on
/// behalf of an unauthorized caller. The fence is decided before the root
/// schema gate, from the already-decoded owner field, so even a root record
/// unreadable under the caller's schema policy answers a non-owner with
/// `Ok(None)` rather than a distinguishable error. Richer role- or
/// boundary-based policy belongs to the surface that fronts
/// [`assemble_agent_goal_view`], exactly as the A2A service authorizes its
/// operations.
#[allow(clippy::too_many_arguments)]
pub async fn authorized_agent_goal_view<Tasks, Runs>(
    tasks: &Tasks,
    runs: &Runs,
    tenant: &TenantId,
    goal: &AgentGoalId,
    principal: &PrincipalRef,
    policy: &AgentSchemaPolicy,
    claims: Option<&dyn AgentGoalClaimSource>,
    observed_at: AgentTimestampMillis,
) -> AgentGoalViewResult<Option<AgentGoalView>>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Runs: DurableStateStore<AgentRunState>,
{
    assemble_goal_view_gated(
        tasks,
        runs,
        tenant,
        goal,
        policy,
        claims,
        Some(principal),
        AGENT_GOAL_VIEW_MAX_TASKS,
        observed_at,
    )
    .await
}

/// Assembles the goal view for its owner under a caller-supplied node budget.
///
/// The authorization contract is [`authorized_agent_goal_view`]'s, unchanged;
/// what this adds is the clamp [`assemble_agent_goal_view_bounded`] already
/// gave the unauthorized core. A boundary that accepts a page size from a
/// caller needs it: without one, every authorized read fans out to
/// [`AGENT_GOAL_VIEW_MAX_TASKS`] whatever the caller asked for, so a cheap
/// question costs the same as an exhaustive one. `max_tasks` is clamped to
/// `1..=AGENT_GOAL_VIEW_MAX_TASKS`, and a tree larger than the budget truncates
/// with markers rather than refusing.
///
/// # Errors
///
/// As [`authorized_agent_goal_view`].
#[allow(clippy::too_many_arguments)]
pub async fn authorized_agent_goal_view_bounded<Tasks, Runs>(
    tasks: &Tasks,
    runs: &Runs,
    tenant: &TenantId,
    goal: &AgentGoalId,
    principal: &PrincipalRef,
    policy: &AgentSchemaPolicy,
    claims: Option<&dyn AgentGoalClaimSource>,
    max_tasks: usize,
    observed_at: AgentTimestampMillis,
) -> AgentGoalViewResult<Option<AgentGoalView>>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Runs: DurableStateStore<AgentRunState>,
{
    assemble_goal_view_gated(
        tasks,
        runs,
        tenant,
        goal,
        policy,
        claims,
        Some(principal),
        max_tasks.clamp(1, AGENT_GOAL_VIEW_MAX_TASKS),
        observed_at,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn assemble_goal_view_gated<Tasks, Runs>(
    tasks: &Tasks,
    runs: &Runs,
    tenant: &TenantId,
    goal: &AgentGoalId,
    policy: &AgentSchemaPolicy,
    claims: Option<&dyn AgentGoalClaimSource>,
    principal: Option<&PrincipalRef>,
    max_tasks: usize,
    observed_at: AgentTimestampMillis,
) -> AgentGoalViewResult<Option<AgentGoalView>>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Runs: DurableStateStore<AgentRunState>,
{
    // Resolution is the recorded open-decision-14 default: the goal identity
    // is the root task's value. A goal instituted under any other id has no
    // root to reach and answers as absent.
    let root_task_id = AgentTaskId::new(goal.as_str())?;
    let root_scope = AgentTaskScope::new(tenant.clone(), root_task_id.clone())?;

    let Some(root_record) = tasks
        .load(&root_scope.persistence_id())
        .await
        .map_err(AgentTaskError::from)?
    else {
        return Ok(None);
    };

    let Some(root_task) = root_record.state.task() else {
        return Ok(None);
    };
    // Qualification: a task that coordinates no goal record, or is bound to a
    // different goal, answers exactly like an absent goal — a child task id
    // presented as a goal id must not become an existence oracle.
    let Some(goal_state) = root_task.goal_state.as_deref() else {
        return Ok(None);
    };
    if root_task.goal.as_ref() != Some(goal) {
        return Ok(None);
    }
    // The owner fence precedes the schema gate and answers from the
    // already-decoded owner field alone: a schema error is distinguishable
    // from `Ok(None)` and would hand a non-owner exactly the existence
    // oracle the deny-is-absent contract forbids.
    if let Some(principal) = principal {
        if goal_state.spec().spec().owner != *principal {
            return Ok(None);
        }
    }
    // The root is the authoritative anchor: unreadable, the whole call fails
    // closed exactly as the entity's own recovery would.
    root_record
        .state
        .check_schema(policy)
        .map_err(AgentTaskError::from)?;

    let contract = AgentGoalContractView::derive(goal_state);
    let root_revision = root_record.revision;
    let budget = AgentGoalBudgetView {
        allocation: contract.allocation,
        root_escrow_allocation: *root_task.escrow.allocation(),
        root_escrow_consumed: *root_task.escrow.consumed(),
        root_outstanding_children: root_task.escrow.outstanding().count(),
        live_consumption: AgentBudgetConsumption::zero(),
    };
    let root_state = root_record.state;

    // The claims join depends on nothing the traversal reads, so the two run
    // concurrently. One reference beyond the cap is requested on purpose: it
    // is what makes a cut list distinguishable from a complete one.
    let claims_join = async {
        match claims {
            None => (Vec::new(), false, false, None),
            Some(source) => match source
                .claims_for_goal(tenant, goal, AGENT_GOAL_VIEW_MAX_CLAIMS + 1)
                .await
            {
                Ok(mut refs) => {
                    let cut = refs.len() > AGENT_GOAL_VIEW_MAX_CLAIMS;
                    refs.truncate(AGENT_GOAL_VIEW_MAX_CLAIMS);
                    (refs, true, cut, None)
                }
                // Only the stable code rides the view: the free-text detail
                // is unbounded backend output, and content never rides an
                // observability surface.
                Err(error) => (Vec::new(), false, false, Some(error.code)),
            },
        }
    };

    let traversal = traverse_goal_tree(
        tasks,
        runs,
        tenant,
        goal,
        policy,
        max_tasks,
        root_state,
        root_revision,
        budget,
    );

    let (traversal, (claim_refs, claims_available, claims_truncated, claims_error_code)) =
        join(traversal, claims_join).await;
    let traversal = traversal?;

    Ok(Some(AgentGoalView {
        tenant: tenant.clone(),
        goal: goal.clone(),
        observed_at,
        root_task: root_task_id,
        root_revision,
        records_read: traversal.records_read,
        truncated: traversal.truncated,
        omissions: traversal.omissions,
        contract,
        cancellation: traversal.cancellation,
        budget: traversal.budget,
        tasks: traversal.tasks,
        runs: traversal.runs,
        claims: claim_refs,
        claims_available,
        claims_truncated,
        claims_error_code,
    }))
}

/// What the bounded breadth-first traversal produced.
struct GoalViewTraversal {
    records_read: usize,
    truncated: bool,
    omissions: Vec<AgentGoalViewOmission>,
    cancellation: AgentCancellationProgress,
    budget: AgentGoalBudgetView,
    tasks: Vec<AgentGoalTaskNode>,
    runs: Vec<AgentGoalRunNode>,
}

/// One assembled node of the current wave, awaiting its run join.
struct GoalViewWaveNode {
    /// Index of the node in the assembled task list.
    node_index: usize,
    task: AgentTaskId,
    /// Admitted epoch children, enqueued beside the run's children so the
    /// frontier keeps the exact order one-at-a-time processing produced.
    epochs: Vec<AgentTaskId>,
    run: Option<AgentRunScope>,
}

/// What a derived struggle signal reports
/// ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every kind is a *projection*: it is recomputed from durable state on each
/// read, holds nothing durable of its own, and MUST NOT be allowed to mutate
/// correctness state. A signal saying an agent is stuck is a prompt for an
/// operator, never a transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentStruggleSignalKind {
    /// A conserved budget dimension is near its allocation.
    BudgetApproachingExhaustion,
    /// The loop has iterated well past the point of proposing a result.
    RepeatedIterationFailure,
    /// The task has spent much of its rejection budget.
    RepeatedResultRejection,
    /// A dependency edge is unresolved and nothing is driving it.
    StuckDependency,
    /// A board claim has held past its lease without activating.
    StalledTeamClaim,
    /// A moderated conversation can make no further turn.
    ModerationExhaustion,
}

impl AgentStruggleSignalKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::BudgetApproachingExhaustion => "budget-approaching-exhaustion",
            Self::RepeatedIterationFailure => "repeated-iteration-failure",
            Self::RepeatedResultRejection => "repeated-result-rejection",
            Self::StuckDependency => "stuck-dependency",
            Self::StalledTeamClaim => "stalled-team-claim",
            Self::ModerationExhaustion => "moderation-exhaustion",
        }
    }
}

impl Display for AgentStruggleSignalKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One derived struggle signal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentStruggleSignal {
    /// What is struggling.
    pub kind: AgentStruggleSignalKind,
    /// Bounded detail: the dimension, the counts, the stable code. Never
    /// content, never a credential.
    pub detail: String,
    /// When the signal was derived.
    pub observed_at: AgentTimestampMillis,
}

impl AgentStruggleSignal {
    fn new(
        kind: AgentStruggleSignalKind,
        detail: impl Into<String>,
        observed_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            kind,
            detail: detail.into(),
            observed_at,
        }
    }
}

/// The thresholds a struggle derivation reads.
///
/// Deployment policy, never durable state: two operators may disagree about
/// when a run is struggling without either of them changing what the run does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct AgentStrugglePolicy {
    /// Percentage of a conserved allocation past which the dimension is
    /// reported as approaching exhaustion.
    pub budget_warning_percent: u8,
    /// Loop iterations a nonterminal run may take with no result proposal
    /// standing before the loop is reported as failing to converge.
    pub iteration_failure_threshold: u64,
    /// Recorded result rejections past which a task is reported as struggling.
    pub result_rejection_threshold: u32,
    /// Milliseconds past a claim's lease before a pending claim is reported as
    /// stalled.
    pub team_claim_stall_millis: u64,
    /// Milliseconds a blocked task may sit untouched with an unregistered
    /// dependency before the edge is reported as stuck.
    ///
    /// A registration is normally outstanding for the length of one settle
    /// pass, so a threshold of zero would report every freshly blocked task.
    pub dependency_stall_millis: u64,
}

impl AgentStrugglePolicy {
    /// The default thresholds.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            budget_warning_percent: 80,
            iteration_failure_threshold: 8,
            result_rejection_threshold: 2,
            team_claim_stall_millis: 0,
            dependency_stall_millis: 15 * 60 * 1_000,
        }
    }
}

impl Default for AgentStrugglePolicy {
    fn default() -> Self {
        Self::new()
    }
}

fn approaching(consumed: u64, allocation: Option<u64>, percent: u8) -> bool {
    let Some(allocation) = allocation.filter(|allocation| *allocation > 0) else {
        return false;
    };
    // Integer arithmetic on the numerator so a 32-bit-ish allocation cannot
    // overflow the comparison, and so the threshold is exact.
    consumed.saturating_mul(100) >= allocation.saturating_mul(u64::from(percent))
}

fn push_budget_signals(
    signals: &mut Vec<AgentStruggleSignal>,
    consumed: &AgentBudgetConsumption,
    allocation: &AgentBudgetAllocation,
    policy: &AgentStrugglePolicy,
    observed_at: AgentTimestampMillis,
) {
    // The conserved set itself, not a hand-rolled copy: a dimension added to
    // `CONSERVED` is charged everywhere the ledger iterates it, and it must be
    // watched here the same day. The labels are the stable `as_label`
    // vocabulary every other surface — exhaustion reasons, metrics — speaks.
    for dimension in AgentBudgetDimension::CONSERVED {
        let spent = consumed.get(dimension);
        let granted = allocation.get(dimension);
        if approaching(spent, granted, policy.budget_warning_percent) {
            let granted = granted.unwrap_or_default();
            signals.push(AgentStruggleSignal::new(
                AgentStruggleSignalKind::BudgetApproachingExhaustion,
                format!("{} {spent}/{granted}", dimension.as_label()),
                observed_at,
            ));
        }
    }
}

/// Derives the struggle signals one run's authoritative snapshot supports.
///
/// Pure over the snapshot: it reads nothing, writes nothing, and deriving twice
/// from the same revision yields the same answer.
#[must_use]
pub fn agent_run_struggle_signals(
    snapshot: &AgentOperationalSnapshot,
    policy: &AgentStrugglePolicy,
) -> Vec<AgentStruggleSignal> {
    let mut signals = Vec::new();
    let Some(run) = snapshot.run.as_ref() else {
        return signals;
    };
    if run.status.is_terminal() {
        return signals;
    }
    push_budget_signals(
        &mut signals,
        run.budget.consumption(),
        run.budget.allocation(),
        policy,
        snapshot.observed_at,
    );
    // A run that has taken many turns without a proposal standing is either
    // exploring or looping. The signal does not decide which; it says an
    // operator should look. The accepted-result gate reads the snapshot's
    // captured fact, never `run.accepted_result` — derivation redacts that
    // field unconditionally, so a guard on it would never suppress anything.
    if run.turn >= policy.iteration_failure_threshold
        && !snapshot.has_pending_proposal
        && !snapshot.has_accepted_result
    {
        signals.push(AgentStruggleSignal::new(
            AgentStruggleSignalKind::RepeatedIterationFailure,
            format!("turn {} with no proposal standing", run.turn),
            snapshot.observed_at,
        ));
    }
    signals
}

/// Derives the struggle signals one task's authoritative snapshot supports.
///
/// Pure over the snapshot.
#[must_use]
pub fn agent_task_struggle_signals(
    snapshot: &AgentTaskOperationalSnapshot,
    policy: &AgentStrugglePolicy,
) -> Vec<AgentStruggleSignal> {
    let mut signals = Vec::new();
    let Some(task) = snapshot.task.as_ref() else {
        return signals;
    };
    if task.terminal_reason.is_some() {
        return signals;
    }
    if task.rejection_count >= policy.result_rejection_threshold {
        signals.push(AgentStruggleSignal::new(
            AgentStruggleSignalKind::RepeatedResultRejection,
            format!("{} rejections recorded", task.rejection_count),
            snapshot.observed_at,
        ));
    }
    // The stuck-dependency shape the dependents registry documented: an edge
    // whose upstream never answered leaves the dependent durably blocked, with
    // no terminal status and nothing driving it. The registry's own
    // `registration_settled` marker is what distinguishes "waiting" from
    // "waiting on nothing".
    // An unsettled registration is *normally* a window of milliseconds: the
    // declaring transition owes it and the very next settle pass drives it. It
    // is only a struggle signal when it stays that way — which is what a
    // never-created upstream looks like. Gating on how long the task has been
    // untouched is what tells the two apart; without it, every freshly blocked
    // task would report itself stuck between its own commit and settle.
    let idle_for = snapshot
        .observed_at
        .as_millis()
        .saturating_sub(task.updated_at.as_millis());
    if matches!(task.status, AgentTaskStatus::Blocked) && idle_for >= policy.dependency_stall_millis
    {
        let unresolved = task
            .dependencies
            .iter()
            .filter(|edge| edge.outcome.is_none() && !edge.registration_settled)
            .count();
        if unresolved > 0 {
            signals.push(AgentStruggleSignal::new(
                AgentStruggleSignalKind::StuckDependency,
                format!("{unresolved} unregistered dependencies for {idle_for}ms"),
                snapshot.observed_at,
            ));
        }
    }
    signals
}

/// Derives the struggle signals one team board supports.
///
/// Pure over the snapshot.
#[must_use]
pub fn agent_team_struggle_signals(
    snapshot: &crate::team::AgentTeamSnapshot,
    policy: &AgentStrugglePolicy,
    observed_at: AgentTimestampMillis,
) -> Vec<AgentStruggleSignal> {
    let mut signals = Vec::new();
    for entry in &snapshot.board {
        // A claim that activated is not stalled — the lease bounds the *pending*
        // window only, which is exactly the window a member can sit in without
        // the board making progress.
        if !matches!(
            entry.status,
            crate::team::AgentTeamBoardEntryStatus::Pending
                | crate::team::AgentTeamBoardEntryStatus::Releasing
        ) {
            continue;
        }
        let Some(claim) = entry.claim.as_ref() else {
            continue;
        };
        let stalled_at = claim
            .lease_expires_at
            .as_millis()
            .saturating_add(policy.team_claim_stall_millis);
        if observed_at.as_millis() >= stalled_at {
            signals.push(AgentStruggleSignal::new(
                AgentStruggleSignalKind::StalledTeamClaim,
                format!("claim {} past its lease", claim.claim),
                observed_at,
            ));
        }
    }
    signals
}

/// Derives the struggle signals one moderated conversation supports.
///
/// Pure over the snapshot.
#[must_use]
pub fn agent_conversation_struggle_signals(
    snapshot: &crate::conversation::AgentConversationSnapshot,
    policy: &AgentStrugglePolicy,
    observed_at: AgentTimestampMillis,
) -> Vec<AgentStruggleSignal> {
    let mut signals = Vec::new();
    if !matches!(
        snapshot.status,
        crate::conversation::AgentConversationStatus::Active
    ) {
        return signals;
    }
    // The parked cursor: the round ceiling is reached under a rule that only
    // parks, so no participant owns a next turn and the early end is the sole
    // exit. An operator watching the governing task sees a wait with no mover.
    if snapshot.current_speaker.is_none() {
        signals.push(AgentStruggleSignal::new(
            AgentStruggleSignalKind::ModerationExhaustion,
            format!("parked at round {} with no next speaker", snapshot.round),
            observed_at,
        ));
    }
    if approaching(
        snapshot.budgets.consumed.tokens,
        snapshot.budgets.tokens,
        policy.budget_warning_percent,
    ) {
        signals.push(AgentStruggleSignal::new(
            AgentStruggleSignalKind::BudgetApproachingExhaustion,
            format!(
                "tokens {}/{}",
                snapshot.budgets.consumed.tokens,
                snapshot.budgets.tokens.unwrap_or_default()
            ),
            observed_at,
        ));
    }
    if let Some(deadline) = snapshot.budgets.deadline {
        if observed_at.as_millis() >= deadline.as_millis() {
            signals.push(AgentStruggleSignal::new(
                AgentStruggleSignalKind::ModerationExhaustion,
                "the creation-fixed deadline has passed".to_string(),
                observed_at,
            ));
        }
    }
    signals
}

/// Records one omission, keeping the list a set: a task the traversal could
/// not assemble appears once, under the first reason discovered.
fn push_goal_view_omission(
    omissions: &mut Vec<AgentGoalViewOmission>,
    omitted: &mut BTreeSet<AgentTaskId>,
    task: AgentTaskId,
    code: &str,
) {
    if omitted.insert(task.clone()) {
        omissions.push(AgentGoalViewOmission {
            task,
            code: code.to_string(),
        });
    }
}

/// The bounded breadth-first traversal: waves of entries admitted against
/// the node budget, each wave's independent store loads issued concurrently.
///
/// Admission is optimistic — a wave may hold entries that fail to join and
/// hand their budget back, and the loop then admits again before truncating —
/// so the assembled nodes, omissions, and truncation are exactly what
/// one-at-a-time processing produced while the store round-trips collapse to
/// two per wave.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn traverse_goal_tree<Tasks, Runs>(
    tasks: &Tasks,
    runs: &Runs,
    tenant: &TenantId,
    goal: &AgentGoalId,
    policy: &AgentSchemaPolicy,
    max_tasks: usize,
    root_state: AgentTaskState,
    root_revision: Revision,
    mut budget: AgentGoalBudgetView,
) -> AgentGoalViewResult<GoalViewTraversal>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Runs: DurableStateStore<AgentRunState>,
{
    // The root, read by the caller.
    let mut records_read = 1_usize;
    let mut task_nodes: Vec<AgentGoalTaskNode> = Vec::new();
    let mut run_nodes: Vec<AgentGoalRunNode> = Vec::new();
    let mut omissions: Vec<AgentGoalViewOmission> = Vec::new();
    let mut omitted: BTreeSet<AgentTaskId> = BTreeSet::new();
    let mut truncated = false;
    let mut visited: BTreeSet<AgentTaskId> = BTreeSet::new();
    let mut frontier: VecDeque<GoalViewFrontierEntry> = VecDeque::new();
    let mut root_cancellation = AgentCancellationProgress::NotRequested;

    // The root is processed exactly like every other node, seeded first.
    let mut wave: Vec<(AgentTaskState, Revision, Option<GoalViewFrontierEntry>)> =
        vec![(root_state, root_revision, None)];

    loop {
        let mut wave_nodes: Vec<GoalViewWaveNode> = Vec::new();
        for (state, revision, entry) in wave.drain(..) {
            let task_id = state.scope().task().clone();
            // A duplicate edge admitted into the same wave: an earlier copy
            // already assembled this task.
            if visited.contains(&task_id) {
                continue;
            }
            let is_root = entry.is_none();
            let is_epoch = entry.as_ref().is_some_and(|entry| entry.via.is_none());

            let Some(task) = state.task() else {
                push_goal_view_omission(
                    &mut omissions,
                    &mut omitted,
                    task_id,
                    agent_goal_view_omission_code::RECORD_MISSING,
                );
                continue;
            };

            if let Some(entry) = entry.as_ref() {
                // Linkage fails closed: a child the traversing edge cannot
                // prove it created is omitted, never joined.
                if task.goal.as_ref() != Some(goal) {
                    push_goal_view_omission(
                        &mut omissions,
                        &mut omitted,
                        task_id,
                        agent_goal_view_omission_code::FOREIGN_GOAL,
                    );
                    continue;
                }
                match &entry.via {
                    Some(via) => {
                        let linked = task.delegation.as_deref().is_some_and(|provenance| {
                            provenance.delegation == *via && provenance.parent_task == entry.parent
                        });
                        if !linked {
                            push_goal_view_omission(
                                &mut omissions,
                                &mut omitted,
                                task_id,
                                agent_goal_view_omission_code::UNLINKED_PROVENANCE,
                            );
                            continue;
                        }
                    }
                    None => {
                        if task.wake.is_none() {
                            push_goal_view_omission(
                                &mut omissions,
                                &mut omitted,
                                task_id,
                                agent_goal_view_omission_code::UNLINKED_PROVENANCE,
                            );
                            continue;
                        }
                    }
                }
            }

            visited.insert(task_id.clone());
            // A task that failed to join through one edge and now joined
            // through another must not read as both assembled and omitted.
            if omitted.remove(&task_id) {
                omissions.retain(|omission| omission.task != task_id);
            }

            let snapshot = state.snapshot();
            let cancellation = snapshot
                .as_ref()
                .map_or(AgentCancellationProgress::NotRequested, |snapshot| {
                    AgentCancellationProgress::derive_task(snapshot)
                });
            if is_root {
                root_cancellation = cancellation;
            }

            task_nodes.push(AgentGoalTaskNode {
                revision,
                scope: state.scope().clone(),
                parent: task.parent.clone(),
                created_by_delegation: task
                    .delegation
                    .as_deref()
                    .map(|provenance| provenance.delegation.clone()),
                depth: task
                    .delegation
                    .as_deref()
                    .map_or(0, |provenance| provenance.depth),
                is_root,
                is_epoch,
                status: task.status,
                cancellation,
                assignment: task
                    .assignment
                    .as_ref()
                    .map(|assignment| AgentGoalAssignmentView {
                        generation: assignment.generation,
                        agent: assignment.agent.clone(),
                        run: assignment.run.clone(),
                        status: assignment.status,
                        assigned_at: assignment.assigned_at,
                    }),
                handoff: task
                    .handoff
                    .as_deref()
                    .map(|handoff| AgentGoalTaskHandoffView {
                        handoff: handoff.handoff.clone(),
                        source_agent: handoff.source_assignment.agent.clone(),
                        source_generation: handoff.source_assignment.generation,
                        source_run: handoff.source_assignment.run.clone(),
                        target: handoff.target.clone(),
                        target_generation: handoff.target_generation,
                        status: handoff.status.as_label().to_string(),
                        recorded_at: handoff.recorded_at,
                    }),
                assignment_generation: task.assignment_generation,
                assignments: task.assignments,
                run_omission: None,
                outstanding_escrow: task.escrow.outstanding().count(),
                escrow_allocation: *task.escrow.allocation(),
                escrow_consumed: *task.escrow.consumed(),
                has_accepted_result: task.accepted_result.is_some(),
                terminal_reason: task.terminal_reason.clone(),
                rejection_count: task.rejection_count,
                updated_at: snapshot.as_ref().map_or(task.created_at, |s| s.updated_at),
            });

            // A continuous root's admitted epochs are reachable children too.
            let epochs = snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.wake.as_ref())
                .map_or_else(Vec::new, |wake| {
                    wake.epochs.iter().map(|epoch| epoch.task.clone()).collect()
                });

            // The current assignment resolves the node's run. A decided task
            // whose assignment was cleared by result acceptance still names
            // its serving agent and its highest generation, so the *last*
            // run's identity re-derives exactly as assignment creation
            // derived it — a completed goal must still reconstruct its tree.
            // A standing refusal proves the opposite: deciding a generation
            // clears `last_refusal`, so one on record means the highest
            // generation was refused and no accepted run record exists to
            // re-derive — a between-assignments task, not an anomaly.
            // Generations before the highest stay an explicit gap — beyond
            // the latest handoff, whose materialized provenance carries the
            // source pair the task record itself no longer names.
            let pending_handoff_source = task
                .handoff
                .as_deref()
                .filter(|handoff| !handoff.is_settled())
                .map(|handoff| {
                    (
                        handoff.source_assignment.agent.clone(),
                        handoff.source_assignment.run.clone(),
                    )
                });
            let resolved_run = match task.assignment.as_ref() {
                Some(assignment) => Some((assignment.agent.clone(), assignment.run.clone())),
                // Mid-handoff the assignment is cleared while the *source*
                // run still owns the work: pairing the target assignee with
                // the source's highest generation would fabricate a scope
                // that never existed, so the pending transfer resolves the
                // recorded source instead ([specification 8.9]).
                None if pending_handoff_source.is_some() => pending_handoff_source,
                None if task.last_refusal.is_some() => None,
                None => match (&task.assignee, task.assignment_generation) {
                    (Some(assignee), generation)
                        if generation != AgentAssignmentGeneration::UNASSIGNED =>
                    {
                        crate::task::run_id_for_assignment(&task_id, generation)
                            .ok()
                            .map(|run| (assignee.clone(), run))
                    }
                    _ => None,
                },
            };
            let run = match resolved_run {
                Some((run_agent, run_id)) => {
                    Some(AgentRunScope::new(tenant.clone(), run_agent, run_id)?)
                }
                None => None,
            };
            wave_nodes.push(GoalViewWaveNode {
                node_index: task_nodes.len() - 1,
                task: task_id,
                epochs,
                run,
            });
        }

        // One concurrent round-trip joins the whole wave's runs.
        let run_ids: Vec<Option<_>> = wave_nodes
            .iter()
            .map(|node| node.run.as_ref().map(|scope| scope.persistence_id()))
            .collect();
        let run_records = join_all(run_ids.iter().map(|id| async move {
            match id {
                None => Ok(None),
                Some(id) => runs.load(id).await,
            }
        }))
        .await;

        for (node, loaded) in wave_nodes.into_iter().zip(run_records) {
            for epoch in node.epochs {
                frontier.push_back(GoalViewFrontierEntry {
                    task: epoch,
                    parent: node.task.clone(),
                    via: None,
                });
            }
            let Some(run_scope) = node.run else {
                continue;
            };
            let run_record = match loaded {
                Ok(record) => record,
                Err(error) => return Err(AgentRunError::from(error).into()),
            };
            let Some(run_record) = run_record else {
                task_nodes[node.node_index].run_omission =
                    Some(agent_goal_view_omission_code::RUN_RECORD_MISSING.to_string());
                continue;
            };
            records_read += 1;
            if run_record.state.check_schema(policy).is_err() {
                task_nodes[node.node_index].run_omission =
                    Some(agent_goal_view_omission_code::RUN_SCHEMA_UNSUPPORTED.to_string());
                continue;
            }
            let Some(run) = run_record.state.run() else {
                task_nodes[node.node_index].run_omission =
                    Some(agent_goal_view_omission_code::RUN_NOT_ACCEPTED.to_string());
                continue;
            };
            let run_node = AgentGoalRunNode::derive(run, run_scope, run_record.revision);
            if run_node.settlement == AgentRunSettlementStatus::Owed {
                budget.live_consumption = budget
                    .live_consumption
                    .saturating_add(run_node.budget.consumption());
            }
            for edge in &run_node.collaboration.delegations {
                if let AgentDelegationStatus::ChildCreated { child_task, .. } = &edge.status {
                    frontier.push_back(GoalViewFrontierEntry {
                        task: child_task.clone(),
                        parent: node.task.clone(),
                        via: Some(edge.delegation.clone()),
                    });
                }
            }
            run_nodes.push(run_node);
        }

        // Admit the next wave against what the node budget still allows.
        let budget_left = max_tasks.saturating_sub(task_nodes.len());
        if budget_left == 0 {
            for remaining in frontier.drain(..) {
                if visited.contains(&remaining.task) {
                    continue;
                }
                truncated = true;
                push_goal_view_omission(
                    &mut omissions,
                    &mut omitted,
                    remaining.task,
                    agent_goal_view_omission_code::NODE_BUDGET_EXHAUSTED,
                );
            }
            break;
        }
        let mut admitted: Vec<GoalViewFrontierEntry> = Vec::new();
        while admitted.len() < budget_left {
            let Some(entry) = frontier.pop_front() else {
                break;
            };
            if visited.contains(&entry.task) {
                continue;
            }
            admitted.push(entry);
        }
        if admitted.is_empty() {
            break;
        }

        // One concurrent round-trip loads the whole admitted wave.
        let mut task_ids = Vec::with_capacity(admitted.len());
        for entry in &admitted {
            task_ids
                .push(AgentTaskScope::new(tenant.clone(), entry.task.clone())?.persistence_id());
        }
        let records = join_all(task_ids.iter().map(|id| tasks.load(id))).await;
        for (entry, record) in admitted.into_iter().zip(records) {
            let record = match record {
                Ok(record) => record,
                Err(error) => return Err(AgentTaskError::from(error).into()),
            };
            let Some(record) = record else {
                push_goal_view_omission(
                    &mut omissions,
                    &mut omitted,
                    entry.task,
                    agent_goal_view_omission_code::RECORD_MISSING,
                );
                continue;
            };
            records_read += 1;
            if record.state.check_schema(policy).is_err() {
                push_goal_view_omission(
                    &mut omissions,
                    &mut omitted,
                    entry.task,
                    agent_goal_view_omission_code::SCHEMA_UNSUPPORTED,
                );
                continue;
            }
            wave.push((record.state, record.revision, Some(entry)));
        }
    }

    Ok(GoalViewTraversal {
        records_read,
        truncated,
        omissions,
        cancellation: root_cancellation,
        budget,
        tasks: task_nodes,
        runs: run_nodes,
    })
}
