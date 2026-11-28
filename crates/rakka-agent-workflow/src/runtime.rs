//! Actor-backed runtime for one durable agent run.
//!
//! `AgentRunActor` is a process-local host for the durable run engine. It does
//! not replace durable state, inbox, or outbox storage; it serializes access to
//! those facades and recovers them when the actor starts. Runtime messages carry
//! small command envelopes and domain records. Large prompts, model responses,
//! files, and tool outputs should continue to move through `ArtifactRef` values
//! stored in those records.
//!
//! A compare-and-set that loses to another writer (a second activation of the
//! same run during a cluster join, or any other writer sharing the store) marks
//! the losing component stale: the refused command still fails with its typed
//! error (`AgentRunEngineError::Persistence` carrying a revision conflict for the
//! run state, `AgentInboxError`/`AgentOutboxError` wrapping
//! `WorkflowError::RevisionConflict` for the inbox and outbox, including through
//! the graph effect bridge). The actor then recovers every component before it
//! handles its next command, so that command is computed from durable state
//! rather than from the cached revision. There is no internal retry: the caller
//! retries the refused command. A recovery that fails leaves the components
//! stale and the next command tries again.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFailure, ActorFuture, MetricsRecorder,
    RakkaError, ReplyTo, Subsystem,
};
use rakka_persistence::DurableStateStore;
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState};

use crate::run_core::AgentRunCore;
use crate::{
    AgentCommand, AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentDueEffect, AgentEffect,
    AgentGraphEffectBridge, AgentGraphEffectBridgeError, AgentGraphEffectScheduleOutcome,
    AgentGraphEffectScheduleRequest, AgentGraphRunProjection, AgentGraphRunState,
    AgentGraphScheduler, AgentGraphSchedulerError, AgentGraphSchedulerTransition,
    AgentInboxAcceptance, AgentInboxError, AgentOutboxAcceptance, AgentOutboxError,
    AgentRunCancellation, AgentRunEngineError, AgentRunId, AgentRunInbox, AgentRunState,
    AgentRunTransition, AgentRunWaitReason, AgentStepRunner, AgentStepSuccess,
    AgentTimestampMillis, AgentWorkflow, AgentWorkflowSnapshotRegistry,
};

/// Shared result type for actor-backed agent run runtime operations.
pub type AgentRunRuntimeResult<T> = Result<T, AgentRunRuntimeError>;

/// Errors surfaced by the actor-backed agent run runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRunRuntimeError {
    /// Durable run state-machine operation failed.
    RunEngine {
        /// Run engine failure.
        error: AgentRunEngineError,
    },
    /// Durable inbox operation failed.
    Inbox {
        /// Inbox failure.
        error: AgentInboxError,
    },
    /// Durable outbox operation failed.
    Outbox {
        /// Outbox failure.
        error: AgentOutboxError,
    },
    /// Durable graph state has not been initialized for this run.
    MissingGraphState {
        /// Run id.
        run_id: AgentRunId,
    },
    /// Compiled plan does not match the actor-hosted workflow.
    GraphPlanMismatch {
        /// Run id.
        run_id: AgentRunId,
        /// Mismatched field.
        field: &'static str,
        /// Expected value.
        expected: String,
        /// Actual value.
        actual: String,
    },
    /// Deterministic graph scheduler operation failed.
    GraphScheduler {
        /// Scheduler failure.
        error: AgentGraphSchedulerError,
    },
    /// Effect bridge operation failed.
    GraphEffectBridge {
        /// Effect bridge failure.
        error: AgentGraphEffectBridgeError,
    },
}

impl AgentRunRuntimeError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::RunEngine { error } => error.code(),
            Self::Inbox { error } => error.code(),
            Self::Outbox { error } => error.code(),
            Self::MissingGraphState { .. } => "missing-graph-state",
            Self::GraphPlanMismatch { .. } => "graph-plan-workflow-mismatch",
            Self::GraphScheduler { error } => error.code(),
            Self::GraphEffectBridge { error } => error.code(),
        }
    }
}

impl Display for AgentRunRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunEngine { error } => Display::fmt(error, f),
            Self::Inbox { error } => Display::fmt(error, f),
            Self::Outbox { error } => Display::fmt(error, f),
            Self::MissingGraphState { run_id } => {
                write!(f, "agent run {run_id} has no initialized graph state")
            }
            Self::GraphPlanMismatch {
                run_id,
                field,
                expected,
                actual,
            } => write!(
                f,
                "agent run {run_id} graph plan field {field} mismatch: expected {expected}, actual {actual}"
            ),
            Self::GraphScheduler { error } => Display::fmt(error, f),
            Self::GraphEffectBridge { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentRunRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RunEngine { error } => Some(error),
            Self::Inbox { error } => Some(error),
            Self::Outbox { error } => Some(error),
            Self::GraphScheduler { error } => Some(error),
            Self::GraphEffectBridge { error } => Some(error),
            Self::MissingGraphState { .. } | Self::GraphPlanMismatch { .. } => None,
        }
    }
}

impl From<AgentRunEngineError> for AgentRunRuntimeError {
    fn from(error: AgentRunEngineError) -> Self {
        Self::RunEngine { error }
    }
}

impl From<AgentInboxError> for AgentRunRuntimeError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox { error }
    }
}

impl From<AgentOutboxError> for AgentRunRuntimeError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox { error }
    }
}

impl From<AgentGraphSchedulerError> for AgentRunRuntimeError {
    fn from(error: AgentGraphSchedulerError) -> Self {
        Self::GraphScheduler { error }
    }
}

impl From<AgentGraphEffectBridgeError> for AgentRunRuntimeError {
    fn from(error: AgentGraphEffectBridgeError) -> Self {
        Self::GraphEffectBridge { error }
    }
}

/// Runtime result for one persisted graph scheduler transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentGraphRuntimeTransition {
    /// Scheduler transition that produced the graph state.
    pub graph_transition: AgentGraphSchedulerTransition,
    /// Durable run-state transition that persisted the graph state.
    pub run_transition: AgentRunTransition,
}

/// Runtime result for scheduling one graph effect node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentGraphRuntimeEffectOutcome {
    /// Effect bridge outcome, including durable outbox acceptance.
    pub effect_outcome: AgentGraphEffectScheduleOutcome,
    /// Durable run-state transition that persisted the graph state.
    pub run_transition: AgentRunTransition,
}

/// Actor-backed helper for compiled graph execution transitions.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentGraphRuntime {
    scheduler: AgentGraphScheduler,
    effect_bridge: AgentGraphEffectBridge,
}

impl AgentGraphRuntime {
    /// Creates a graph runtime from the default scheduler and effect bridge.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            scheduler: AgentGraphScheduler::new(),
            effect_bridge: AgentGraphEffectBridge::new(),
        }
    }

    /// Deterministic graph scheduler used by this runtime.
    #[must_use]
    pub const fn scheduler(&self) -> AgentGraphScheduler {
        self.scheduler
    }

    /// Effect bridge used by this runtime.
    #[must_use]
    pub const fn effect_bridge(&self) -> AgentGraphEffectBridge {
        self.effect_bridge
    }

    /// Initializes graph state and persists the accepted run.
    pub async fn start_graph_run<RunStore>(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        mut initial_state: AgentRunState,
        plan: &AgentCompiledExecutionPlan,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition>
    where
        RunStore: DurableStateStore<AgentRunState>,
    {
        validate_graph_plan_matches_runner(runner, plan)?;
        let graph = self.scheduler.initialize_state(plan, now)?;
        initial_state.graph_state = Some(graph);
        initial_state.updated_at = now;
        runner.start(initial_state).await.map_err(Into::into)
    }

    /// Marks currently ready pending graph nodes runnable and persists the run state.
    pub async fn mark_ready_nodes<RunStore>(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        plan: &AgentCompiledExecutionPlan,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition>
    where
        RunStore: DurableStateStore<AgentRunState>,
    {
        validate_graph_plan_matches_runner(runner, plan)?;
        let graph = current_graph_state(runner)?;
        let transition = self.scheduler.mark_ready_nodes_runnable(plan, graph, now)?;
        persist_graph_transition(runner, transition, now).await
    }

    /// Starts one runnable graph node and persists the run state.
    pub async fn start_node<RunStore>(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        plan: &AgentCompiledExecutionPlan,
        node_id: impl Into<AgentCompiledNodeId>,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition>
    where
        RunStore: DurableStateStore<AgentRunState>,
    {
        validate_graph_plan_matches_runner(runner, plan)?;
        let graph = current_graph_state(runner)?;
        let transition = self.scheduler.start_node(plan, graph, node_id, now)?;
        persist_graph_transition(runner, transition, now).await
    }

    /// Completes one running graph node and persists the run state.
    ///
    /// Completing a terminal node ends the graph `Completed` and cancels its
    /// unresolved nodes. This helper has only the runner, so it does not
    /// settle those nodes' outbox entries: an owner calling it settles them
    /// first, with
    /// [`AgentGraphEffectBridge::settle_cancelled_nodes`](crate::AgentGraphEffectBridge::settle_cancelled_nodes)
    /// on the transition it computes. The run core's
    /// [`complete_graph_node`](crate::AgentRunCore::complete_graph_node)
    /// settles them itself (issue #71 item 4).
    pub async fn complete_node<RunStore>(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        plan: &AgentCompiledExecutionPlan,
        node_id: impl Into<AgentCompiledNodeId>,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition>
    where
        RunStore: DurableStateStore<AgentRunState>,
    {
        validate_graph_plan_matches_runner(runner, plan)?;
        let graph = current_graph_state(runner)?;
        let transition = self.scheduler.complete_node(plan, graph, node_id, now)?;
        persist_graph_transition(runner, transition, now).await
    }

    /// Completes one running graph node as [`Self::complete_node`] does, but
    /// settles the outbox entries of the nodes the completion cancelled
    /// before persisting (issue #71 item 4). The run core's
    /// `complete_graph_node`.
    pub(crate) async fn complete_node_settling<RunStore, WorkflowStore, Clock>(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        inbox: &mut AgentRunInbox<WorkflowStore, Clock>,
        plan: &AgentCompiledExecutionPlan,
        node_id: AgentCompiledNodeId,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition>
    where
        RunStore: DurableStateStore<AgentRunState>,
        WorkflowStore: DurableStateStore<WorkflowState>,
        Clock: WorkflowClock,
    {
        validate_graph_plan_matches_runner(runner, plan)?;
        let graph = current_graph_state(runner)?;
        let transition = self.scheduler.complete_node(plan, graph, node_id, now)?;
        self.effect_bridge
            .settle_cancelled_nodes(inbox, &transition)
            .await?;
        persist_graph_transition(runner, transition, now).await
    }

    /// Schedules one running graph effect node and persists the graph wait state.
    pub async fn schedule_node_effect<RunStore, WorkflowStore, Clock>(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        inbox: &mut AgentRunInbox<WorkflowStore, Clock>,
        plan: &AgentCompiledExecutionPlan,
        request: AgentGraphEffectScheduleRequest,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeEffectOutcome>
    where
        RunStore: DurableStateStore<AgentRunState>,
        WorkflowStore: DurableStateStore<WorkflowState>,
        Clock: WorkflowClock,
    {
        validate_graph_plan_matches_runner(runner, plan)?;
        let now = request.created_at;
        let graph = current_graph_state(runner)?;
        let effect_outcome = self
            .effect_bridge
            .schedule_node_effect(plan, graph, request, inbox)
            .await?;
        let run_transition = runner
            .update_graph_state(effect_outcome.transition.state.clone(), now)
            .await?;
        Ok(AgentGraphRuntimeEffectOutcome {
            effect_outcome,
            run_transition,
        })
    }
}

fn current_graph_state<RunStore>(
    runner: &AgentStepRunner<RunStore>,
) -> AgentRunRuntimeResult<AgentGraphRunState>
where
    RunStore: DurableStateStore<AgentRunState>,
{
    runner
        .state()?
        .and_then(|state| state.graph_state.clone())
        .ok_or_else(|| AgentRunRuntimeError::MissingGraphState {
            run_id: runner.run_id().clone(),
        })
}

async fn persist_graph_transition<RunStore>(
    runner: &mut AgentStepRunner<RunStore>,
    graph_transition: AgentGraphSchedulerTransition,
    now: AgentTimestampMillis,
) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition>
where
    RunStore: DurableStateStore<AgentRunState>,
{
    let run_transition = runner
        .update_graph_state(graph_transition.state.clone(), now)
        .await?;
    Ok(AgentGraphRuntimeTransition {
        graph_transition,
        run_transition,
    })
}

fn validate_graph_plan_matches_runner<RunStore>(
    runner: &AgentStepRunner<RunStore>,
    plan: &AgentCompiledExecutionPlan,
) -> AgentRunRuntimeResult<()>
where
    RunStore: DurableStateStore<AgentRunState>,
{
    let workflow = runner.workflow();
    if plan.workflow_id != workflow.workflow_id {
        return Err(AgentRunRuntimeError::GraphPlanMismatch {
            run_id: runner.run_id().clone(),
            field: "workflow_id",
            expected: workflow.workflow_id.to_string(),
            actual: plan.workflow_id.to_string(),
        });
    }
    if plan.definition_version != workflow.definition_version {
        return Err(AgentRunRuntimeError::GraphPlanMismatch {
            run_id: runner.run_id().clone(),
            field: "definition_version",
            expected: workflow.definition_version.to_string(),
            actual: plan.definition_version.to_string(),
        });
    }
    if plan.workflow_type != workflow.workflow_type {
        return Err(AgentRunRuntimeError::GraphPlanMismatch {
            run_id: runner.run_id().clone(),
            field: "workflow_type",
            expected: workflow.workflow_type.clone(),
            actual: plan.workflow_type.clone(),
        });
    }
    Ok(())
}

/// Diagnostic snapshot for one actor-hosted run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunActorSnapshot {
    /// Run id hosted by the actor.
    pub run_id: AgentRunId,
    /// Latest recovered or persisted run state.
    pub run_state: Option<AgentRunState>,
    /// Graph execution summary, when the recovered run uses compiled graph execution.
    pub graph: Option<AgentGraphRunProjection>,
    /// Recoverable durable inbox command count.
    pub recoverable_command_count: usize,
    /// Due durable outbox effect count.
    pub due_effect_count: usize,
}

/// Typed message protocol accepted by [`AgentRunActor`].
///
/// Messages are intentionally small runtime commands. Application payloads,
/// prompts, completions, and tool outputs should be stored out of line and
/// referenced from `AgentRunState` or `AgentEffect` artifacts.
///
/// A write the enum has no arm for, such as dispatching the due outbox,
/// completing a branch, or applying an effect callback through the bridge,
/// is made by an owner that holds the run's [`AgentRunCore`] itself and
/// composes it on the core's own runner and inbox
/// ([`AgentRunCore::parts_mut`]). Do not write beside the actor through a
/// second inbox and runner over the same stores: the actor's next write then
/// loses its compare-and-set.
pub enum AgentRunActorCommand {
    /// Recover durable run state, inbox, and outbox state.
    Recover {
        /// Reply channel for the recovered snapshot.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunActorSnapshot>>,
    },
    /// Return the current recovered snapshot without forcing another load.
    Snapshot {
        /// Reply channel for the current snapshot.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunActorSnapshot>>,
    },
    /// Accept an agent command through the durable inbox facade.
    AcceptCommand {
        /// Command envelope to durably accept.
        command: AgentCommand,
        /// Reply channel for durable inbox acceptance.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentInboxAcceptance>>,
    },
    /// Persist the initial accepted run state.
    Start {
        /// Initial accepted run state.
        initial_state: AgentRunState,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Initialize compiled graph state and persist the accepted run state.
    StartGraph {
        /// Initial accepted run state.
        initial_state: AgentRunState,
        /// Compiled execution plan selected for this run.
        plan: Arc<AgentCompiledExecutionPlan>,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Mark ready graph nodes runnable.
    MarkGraphReady {
        /// Compiled execution plan selected for this run.
        plan: Arc<AgentCompiledExecutionPlan>,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted graph transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentGraphRuntimeTransition>>,
    },
    /// Start one runnable graph node.
    StartGraphNode {
        /// Compiled execution plan selected for this run.
        plan: Arc<AgentCompiledExecutionPlan>,
        /// Compiled graph node id.
        node_id: AgentCompiledNodeId,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted graph transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentGraphRuntimeTransition>>,
    },
    /// Complete one running graph node.
    CompleteGraphNode {
        /// Compiled execution plan selected for this run.
        plan: Arc<AgentCompiledExecutionPlan>,
        /// Compiled graph node id.
        node_id: AgentCompiledNodeId,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted graph transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentGraphRuntimeTransition>>,
    },
    /// Schedule one running effect-producing graph node.
    ScheduleGraphNodeEffect {
        /// Compiled execution plan selected for this run.
        plan: Arc<AgentCompiledExecutionPlan>,
        /// Effect schedule request for the graph node.
        request: AgentGraphEffectScheduleRequest,
        /// Reply channel for the durable effect and graph transition outcome.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentGraphRuntimeEffectOutcome>>,
    },
    /// Begin executing the current step.
    BeginStep {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Record current step success.
    SucceedStep {
        /// Successful step outcome.
        success: AgentStepSuccess,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Record current step failure and fail the run.
    FailStep {
        /// Stable bounded error code.
        error_code: String,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Pause the run for a durable wait condition.
    Wait {
        /// Wait reason.
        reason: AgentRunWaitReason,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Resume a waiting run.
    Resume {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Complete a running run.
    Complete {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Fail a non-terminal run outside a specific step failure.
    FailRun {
        /// Stable bounded error code.
        error_code: String,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Request run cancellation.
    RequestCancellation {
        /// Stable bounded cancellation reason code.
        reason_code: String,
        /// Optional human-readable cancellation summary.
        reason_summary: Option<String>,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Mark a cancelling run cancelled: a run with a graph is cancelled
    /// whole, its nodes and their outbox entries with it
    /// ([`AgentRunCore::cancel`](crate::AgentRunCore::cancel)).
    Cancel {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the cancellation: the persisted transition, the
        /// nodes it cancelled and the outbox entries it settled.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunCancellation>>,
    },
    /// Move a failed run into compensation.
    BeginCompensation {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Schedule a first-class effect through the durable outbox facade.
    ScheduleEffect {
        /// Effect envelope to durably schedule.
        effect: AgentEffect,
        /// Reply channel for durable outbox scheduling.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentOutboxAcceptance>>,
    },
    /// Return due effects from the durable outbox snapshot.
    DueEffects {
        /// Reply channel for due effects.
        reply_to: ReplyTo<AgentRunRuntimeResult<Vec<AgentDueEffect>>>,
    },
}

/// Actor-backed host for one durable agent run.
///
/// The actor is a mailbox over one [`AgentRunCore`]: each command runs the
/// core's operation of the same name, and the actor recovers the core when it
/// starts and when it restarts. An owner that is already a single writer of
/// the run's stores can hold the core itself instead (see
/// [`crate::run_core`]).
pub struct AgentRunActor<RunStore, WorkflowStore, Clock = SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    core: AgentRunCore<RunStore, WorkflowStore, Clock>,
}

impl<RunStore, WorkflowStore> AgentRunActor<RunStore, WorkflowStore, SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    /// Creates an actor-backed runtime with the system clock and no-op metrics.
    #[must_use]
    pub fn new(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
    ) -> Self {
        Self::from_core(AgentRunCore::new(
            workflow,
            run_id,
            run_store,
            workflow_store,
        ))
    }

    /// Creates an actor-backed runtime with the system clock and explicit
    /// metrics recorder.
    #[must_use]
    pub fn with_metrics(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::from_core(AgentRunCore::with_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            metrics,
        ))
    }
}

impl<RunStore, WorkflowStore, Clock> AgentRunActor<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates an actor-backed runtime with an explicit clock and no-op metrics.
    #[must_use]
    pub fn with_clock(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        clock: Clock,
    ) -> Self {
        Self::from_core(AgentRunCore::with_clock(
            workflow,
            run_id,
            run_store,
            workflow_store,
            clock,
        ))
    }

    /// Creates an actor-backed runtime with an explicit clock and metrics
    /// recorder.
    #[must_use]
    pub fn with_clock_and_metrics(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        clock: Clock,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::from_core(AgentRunCore::with_clock_and_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            clock,
            metrics,
        ))
    }

    /// Creates an actor-backed runtime from already constructed durable facades.
    #[must_use]
    pub const fn from_parts(
        runner: AgentStepRunner<RunStore>,
        inbox: AgentRunInbox<WorkflowStore, Clock>,
    ) -> Self {
        Self::from_core(AgentRunCore::from_parts(runner, inbox))
    }

    /// Hosts an already constructed run core.
    #[must_use]
    pub const fn from_core(core: AgentRunCore<RunStore, WorkflowStore, Clock>) -> Self {
        Self { core }
    }

    /// Publishes bounded operational snapshots to the provided registry.
    #[must_use]
    pub fn with_snapshot_registry(self, snapshots: AgentWorkflowSnapshotRegistry) -> Self {
        Self::from_core(self.core.with_snapshot_registry(snapshots))
    }

    /// The run core the actor hosts.
    #[must_use]
    pub const fn core(&self) -> &AgentRunCore<RunStore, WorkflowStore, Clock> {
        &self.core
    }

    /// Durable run state-machine facade.
    #[must_use]
    pub const fn runner(&self) -> &AgentStepRunner<RunStore> {
        self.core.runner()
    }

    /// Durable inbox and outbox facade.
    #[must_use]
    pub const fn inbox(&self) -> &AgentRunInbox<WorkflowStore, Clock> {
        self.core.inbox()
    }

    /// Compiled graph runtime facade.
    #[must_use]
    pub const fn graph_runtime(&self) -> AgentGraphRuntime {
        self.core.graph_runtime()
    }
}

impl<RunStore, WorkflowStore, Clock> Actor for AgentRunActor<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    type Msg = AgentRunActorCommand;

    fn started<'a>(&'a mut self, _ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async move {
            self.core.recover().await.map_err(runtime_error_to_rakka)?;
            Ok(ActorAction::Continue)
        })
    }

    fn restarted<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _failure: &'a ActorFailure,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.core.recover().await.map_err(runtime_error_to_rakka)?;
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            let core = &mut self.core;
            match msg {
                AgentRunActorCommand::Recover { reply_to } => {
                    let _reply_dropped = reply_to.reply(core.recover().await);
                }
                AgentRunActorCommand::Snapshot { reply_to } => {
                    let _reply_dropped = reply_to.reply(core.snapshot().await);
                }
                AgentRunActorCommand::AcceptCommand { command, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.accept_command(command).await);
                }
                AgentRunActorCommand::Start {
                    initial_state,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(core.start(initial_state).await);
                }
                AgentRunActorCommand::StartGraph {
                    initial_state,
                    plan,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped =
                        reply_to.reply(core.start_graph(initial_state, &plan, now).await);
                }
                AgentRunActorCommand::MarkGraphReady {
                    plan,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(core.mark_graph_ready(&plan, now).await);
                }
                AgentRunActorCommand::StartGraphNode {
                    plan,
                    node_id,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped =
                        reply_to.reply(core.start_graph_node(&plan, node_id, now).await);
                }
                AgentRunActorCommand::CompleteGraphNode {
                    plan,
                    node_id,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped =
                        reply_to.reply(core.complete_graph_node(&plan, node_id, now).await);
                }
                AgentRunActorCommand::ScheduleGraphNodeEffect {
                    plan,
                    request,
                    reply_to,
                } => {
                    let _reply_dropped =
                        reply_to.reply(core.schedule_graph_node_effect(&plan, request).await);
                }
                AgentRunActorCommand::BeginStep { now, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.begin_step(now).await);
                }
                AgentRunActorCommand::SucceedStep {
                    success,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(core.succeed_step(success, now).await);
                }
                AgentRunActorCommand::FailStep {
                    error_code,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(core.fail_step(error_code, now).await);
                }
                AgentRunActorCommand::Wait {
                    reason,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(core.wait(reason, now).await);
                }
                AgentRunActorCommand::Resume { now, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.resume(now).await);
                }
                AgentRunActorCommand::Complete { now, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.complete(now).await);
                }
                AgentRunActorCommand::FailRun {
                    error_code,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(core.fail_run(error_code, now).await);
                }
                AgentRunActorCommand::RequestCancellation {
                    reason_code,
                    reason_summary,
                    now,
                    reply_to,
                } => {
                    let _reply_dropped = reply_to.reply(
                        core.request_cancellation(reason_code, reason_summary, now)
                            .await,
                    );
                }
                AgentRunActorCommand::Cancel { now, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.cancel(now).await);
                }
                AgentRunActorCommand::BeginCompensation { now, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.begin_compensation(now).await);
                }
                AgentRunActorCommand::ScheduleEffect { effect, reply_to } => {
                    let _reply_dropped = reply_to.reply(core.schedule_effect(effect).await);
                }
                AgentRunActorCommand::DueEffects { reply_to } => {
                    let _reply_dropped = reply_to.reply(core.due_effects().await);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

fn runtime_error_to_rakka(error: AgentRunRuntimeError) -> RakkaError {
    RakkaError::new(Subsystem::Workflow, error.code(), error.to_string())
}
