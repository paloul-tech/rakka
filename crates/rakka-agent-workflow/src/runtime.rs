//! Actor-backed runtime for one durable agent run.
//!
//! `AgentRunActor` is a process-local host for the durable run engine. It does
//! not replace durable state, inbox, or outbox storage; it serializes access to
//! those facades and recovers them when the actor starts. Runtime messages carry
//! small command envelopes and domain records. Large prompts, model responses,
//! files, and tool outputs should continue to move through `ArtifactRef` values
//! stored in those records.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFailure, ActorFuture, MetricsRecorder,
    NoopMetricsRecorder, RakkaError, ReplyTo, Subsystem,
};
use rakka_persistence::DurableStateStore;
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState};

use crate::{
    AgentCommand, AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentDueEffect, AgentEffect,
    AgentGraphEffectBridge, AgentGraphEffectBridgeError, AgentGraphEffectScheduleOutcome,
    AgentGraphEffectScheduleRequest, AgentGraphRunProjection, AgentGraphRunState,
    AgentGraphScheduler, AgentGraphSchedulerError, AgentGraphSchedulerTransition,
    AgentInboxAcceptance, AgentInboxError, AgentOutboxAcceptance, AgentOutboxError,
    AgentOutboxResult, AgentRunEngineError, AgentRunId, AgentRunInbox, AgentRunState,
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
    /// Mark a cancelling run cancelled.
    Cancel {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
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
pub struct AgentRunActor<RunStore, WorkflowStore, Clock = SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    runner: AgentStepRunner<RunStore>,
    inbox: AgentRunInbox<WorkflowStore, Clock>,
    graph_runtime: AgentGraphRuntime,
    snapshots: Option<AgentWorkflowSnapshotRegistry>,
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
        Self::with_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            Arc::new(NoopMetricsRecorder),
        )
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
        Self::from_parts(
            AgentStepRunner::new(workflow, run_id.clone(), run_store),
            AgentRunInbox::with_metrics(run_id, workflow_store, metrics),
        )
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
        Self::with_clock_and_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            clock,
            Arc::new(NoopMetricsRecorder),
        )
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
        Self::from_parts(
            AgentStepRunner::new(workflow, run_id.clone(), run_store),
            AgentRunInbox::with_clock_and_metrics(run_id, workflow_store, clock, metrics),
        )
    }

    /// Creates an actor-backed runtime from already constructed durable facades.
    #[must_use]
    pub const fn from_parts(
        runner: AgentStepRunner<RunStore>,
        inbox: AgentRunInbox<WorkflowStore, Clock>,
    ) -> Self {
        Self {
            runner,
            inbox,
            graph_runtime: AgentGraphRuntime::new(),
            snapshots: None,
        }
    }

    /// Publishes bounded operational snapshots to the provided registry.
    #[must_use]
    pub fn with_snapshot_registry(mut self, snapshots: AgentWorkflowSnapshotRegistry) -> Self {
        self.snapshots = Some(snapshots);
        self
    }

    /// Durable run state-machine facade.
    #[must_use]
    pub const fn runner(&self) -> &AgentStepRunner<RunStore> {
        &self.runner
    }

    /// Durable inbox and outbox facade.
    #[must_use]
    pub const fn inbox(&self) -> &AgentRunInbox<WorkflowStore, Clock> {
        &self.inbox
    }

    /// Compiled graph runtime facade.
    #[must_use]
    pub const fn graph_runtime(&self) -> AgentGraphRuntime {
        self.graph_runtime
    }

    async fn recover_components(&mut self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
        let result = async {
            self.runner.recover().await?;
            self.inbox.recover().await?;
            self.reconcile_recovered_graph_effects().await?;
            self.snapshot()
        }
        .await;
        self.record_snapshot_result("recover", &result);
        result
    }

    /// Re-links durable outbox effects to their graph nodes after recovery.
    ///
    /// An effect is committed to the durable outbox before the graph transition
    /// that records it is persisted (see
    /// [`AgentGraphEffectBridge::schedule_node_effect`]). A crash in that window
    /// leaves the effect enqueued with no node link, which would orphan its
    /// completion (`node_id_for_effect` would fail with `UnknownEffect`). The
    /// run actor never re-drives in-flight nodes, so recovery restores the link
    /// here from the self-describing recovered effects. The pass is idempotent:
    /// a run with no graph state, no in-flight effects, or nothing to relink
    /// persists nothing.
    async fn reconcile_recovered_graph_effects(&mut self) -> AgentRunRuntimeResult<()> {
        let Some(graph) = self
            .runner
            .state()?
            .and_then(|state| state.graph_state.clone())
        else {
            return Ok(());
        };
        let effects: Vec<AgentEffect> = self
            .inbox
            .due_effects()?
            .into_iter()
            .map(|due| due.effect)
            .collect();
        if effects.is_empty() {
            return Ok(());
        }
        let transition = self
            .graph_runtime
            .effect_bridge()
            .reconcile_recovered_effects(graph, &effects);
        if transition.changed_node_ids.is_empty() {
            return Ok(());
        }
        let now = effects
            .iter()
            .map(|effect| effect.created_at)
            .max()
            .unwrap_or_default();
        self.runner
            .update_graph_state(transition.state, now)
            .await?;
        Ok(())
    }

    fn snapshot(&self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
        let run_state = self.runner.state()?.cloned();
        let graph = run_state
            .as_ref()
            .and_then(|state| state.graph_state.as_ref())
            .map(AgentGraphRunProjection::from_graph_state);
        let recoverable_command_count = self
            .inbox
            .inner()
            .recoverable_inbox()
            .map_err(AgentInboxError::from)?
            .len();
        let due_effect_count = self.inbox.due_effects()?.len();
        Ok(AgentRunActorSnapshot {
            run_id: self.runner.run_id().clone(),
            run_state,
            graph,
            recoverable_command_count,
            due_effect_count,
        })
    }

    fn record_snapshot_result(
        &self,
        phase: &'static str,
        result: &AgentRunRuntimeResult<AgentRunActorSnapshot>,
    ) {
        if let Some(snapshots) = &self.snapshots {
            match result {
                Ok(snapshot) => snapshots.record_run_actor_snapshot(snapshot),
                Err(error) => {
                    snapshots.record_run_runtime_error(self.runner.run_id().clone(), phase, error);
                }
            }
        }
    }

    fn record_current_snapshot(&self, phase: &'static str) {
        let result = self.snapshot();
        self.record_snapshot_result(phase, &result);
    }

    fn record_operation_result<T>(&self, phase: &'static str, result: &AgentRunRuntimeResult<T>) {
        if let Err(error) = result {
            if let Some(snapshots) = &self.snapshots {
                snapshots.record_run_runtime_error(self.runner.run_id().clone(), phase, error);
            }
        } else {
            self.record_current_snapshot(phase);
        }
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
            self.recover_components()
                .await
                .map_err(runtime_error_to_rakka)?;
            Ok(ActorAction::Continue)
        })
    }

    fn restarted<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _failure: &'a ActorFailure,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.recover_components()
                .await
                .map_err(runtime_error_to_rakka)?;
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                AgentRunActorCommand::Recover { reply_to } => {
                    let _reply_dropped = reply_to.reply(self.recover_components().await);
                }
                AgentRunActorCommand::Snapshot { reply_to } => {
                    let result = self.snapshot();
                    self.record_snapshot_result("snapshot", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::AcceptCommand { command, reply_to } => {
                    let result = self
                        .inbox
                        .accept_command(command)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("accept-command", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Start {
                    initial_state,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .start(initial_state)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("start", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::StartGraph {
                    initial_state,
                    plan,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .graph_runtime
                        .start_graph_run(&mut self.runner, initial_state, &plan, now)
                        .await;
                    self.record_operation_result("start-graph", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::MarkGraphReady {
                    plan,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .graph_runtime
                        .mark_ready_nodes(&mut self.runner, &plan, now)
                        .await;
                    self.record_operation_result("mark-graph-ready", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::StartGraphNode {
                    plan,
                    node_id,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .graph_runtime
                        .start_node(&mut self.runner, &plan, node_id, now)
                        .await;
                    self.record_operation_result("start-graph-node", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::CompleteGraphNode {
                    plan,
                    node_id,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .graph_runtime
                        .complete_node(&mut self.runner, &plan, node_id, now)
                        .await;
                    self.record_operation_result("complete-graph-node", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::ScheduleGraphNodeEffect {
                    plan,
                    request,
                    reply_to,
                } => {
                    let result = self
                        .graph_runtime
                        .schedule_node_effect(&mut self.runner, &mut self.inbox, &plan, request)
                        .await;
                    self.record_operation_result("schedule-graph-node-effect", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::BeginStep { now, reply_to } => {
                    let result = self
                        .runner
                        .begin_step(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("begin-step", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::SucceedStep {
                    success,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .succeed_step(success, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("succeed-step", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::FailStep {
                    error_code,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .fail_step(error_code, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("fail-step", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Wait {
                    reason,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .wait(reason, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("wait", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Resume { now, reply_to } => {
                    let result = self
                        .runner
                        .resume(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("resume", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Complete { now, reply_to } => {
                    let result = self
                        .runner
                        .complete(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("complete", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::FailRun {
                    error_code,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .fail_run(error_code, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("fail-run", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::RequestCancellation {
                    reason_code,
                    reason_summary,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .request_cancellation(reason_code, reason_summary, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("request-cancellation", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Cancel { now, reply_to } => {
                    let result = self
                        .runner
                        .cancel(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("cancel", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::BeginCompensation { now, reply_to } => {
                    let result = self
                        .runner
                        .begin_compensation(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("begin-compensation", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::ScheduleEffect { effect, reply_to } => {
                    let result = self
                        .inbox
                        .schedule_effect(effect)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("schedule-effect", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::DueEffects { reply_to } => {
                    let result: AgentOutboxResult<Vec<AgentDueEffect>> = self.inbox.due_effects();
                    let result = result.map_err(AgentRunRuntimeError::from);
                    self.record_operation_result("due-effects", &result);
                    let _reply_dropped = reply_to.reply(result);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

fn runtime_error_to_rakka(error: AgentRunRuntimeError) -> RakkaError {
    RakkaError::new(Subsystem::Workflow, error.code(), error.to_string())
}
