//! The owner-held core of one durable agent run.
//!
//! [`AgentRunCore`] is the run's step runner, its durable inbox and outbox,
//! the compiled graph runtime and an optional snapshot registry, with every
//! operation [`AgentRunActor`](crate::AgentRunActor) offers. It is for an
//! owner that is **already a single writer** of the run's two stores, such as
//! a sharded entity whose mailbox orders every write: that owner holds one
//! core and drives the run through it, instead of hosting the actor as a
//! child and writing beside it through a second inbox and runner over the
//! same stores. The actor is one such owner. It holds a core and delegates
//! each command to it, so a run behaves the same whichever drives it.
//!
//! Each operation first recovers a component that lost a compare-and-set
//! ([`AgentRunCore::refresh_if_stale`]), then runs, then records the run's
//! snapshot, or its error, in the registry under the operation's phase. The
//! phases are the ones the actor has always recorded under.
//!
//! An owner calls [`AgentRunCore::recover`] before the first operation, and
//! again whenever the owner itself is re-created: the actor does so when it
//! starts and when it restarts. A core never recovered refuses an operation
//! as its runner does (`not-recovered`); it recovers nothing on its own.

use std::collections::BTreeSet;
use std::sync::Arc;

use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::DurableStateStore;
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState};

use crate::runtime::{
    AgentGraphRuntime, AgentGraphRuntimeEffectOutcome, AgentGraphRuntimeTransition,
    AgentRunActorSnapshot, AgentRunRuntimeError, AgentRunRuntimeResult,
};
use crate::{
    AgentCommand, AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentDueEffect, AgentEffect,
    AgentEffectId, AgentGraphEffectScheduleRequest, AgentGraphRunProjection, AgentInboxAcceptance,
    AgentInboxError, AgentOutboxAcceptance, AgentOutboxResult, AgentRunId, AgentRunInbox,
    AgentRunState, AgentRunTransition, AgentRunWaitReason, AgentStepRunner, AgentStepSuccess,
    AgentTimestampMillis, AgentWorkflow, AgentWorkflowSnapshotRegistry,
};

/// The owner-held core of one durable agent run (see the [module
/// documentation](self)).
pub struct AgentRunCore<RunStore, WorkflowStore, Clock = SystemWorkflowClock>
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

impl<RunStore, WorkflowStore> AgentRunCore<RunStore, WorkflowStore, SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    /// Creates a run core with the system clock and no-op metrics.
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

    /// Creates a run core with the system clock and an explicit metrics
    /// recorder.
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

impl<RunStore, WorkflowStore, Clock> AgentRunCore<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates a run core with an explicit clock and no-op metrics.
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

    /// Creates a run core with an explicit clock and metrics recorder.
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

    /// Creates a run core from already constructed durable facades.
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

    /// Recovers every component when one of them lost a compare-and-set, so
    /// the next write runs against durable state.
    ///
    /// Its recovery is [`Self::recover`]'s, recorded under `recover`.
    async fn refresh_if_stale(&mut self) -> AgentRunRuntimeResult<()> {
        if self.runner.is_stale() || self.inbox.is_stale() {
            self.recover().await.map(|_snapshot| ())
        } else {
            Ok(())
        }
    }

    /// Recovers the run's state, then its inbox and outbox, then re-links
    /// outbox effects no graph node lists yet, and answers the recovered
    /// snapshot. Recorded under `recover`.
    ///
    /// # Errors
    ///
    /// A store's failure to load, or a recovered record this build cannot
    /// read.
    pub async fn recover(&mut self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
        let result = async {
            self.runner.recover().await?;
            self.inbox.recover().await?;
            self.reconcile_recovered_graph_effects().await?;
            self.read_snapshot()
        }
        .await;
        self.record_snapshot_result("recover", &result);
        result
    }

    /// Re-links durable outbox effects to their graph nodes after recovery.
    ///
    /// An effect is committed to the durable outbox before the graph transition
    /// that records it is persisted (see
    /// [`AgentGraphEffectBridge::schedule_node_effect`](crate::AgentGraphEffectBridge::schedule_node_effect)).
    /// A crash in that window
    /// leaves the effect enqueued with no node link, which would orphan its
    /// completion (`node_id_for_effect` would fail with `UnknownEffect`). The
    /// run core never re-drives in-flight nodes, so recovery restores the link
    /// here from the self-describing recovered effects. The pass is idempotent:
    /// a run with no graph state, no in-flight effects, or nothing to relink
    /// persists nothing.
    ///
    /// Only the outbox entries no node lists yet are decoded (an entry's
    /// message id is its effect id), whatever their status, so an effect that
    /// exhausted while the owner was down is still reached and a healthy run
    /// decodes nothing. An entry whose payload does not decode as an agent
    /// effect is skipped with a warning that carries no ids: it cannot be
    /// re-linked, and it must not stop the run from recovering.
    async fn reconcile_recovered_graph_effects(&mut self) -> AgentRunRuntimeResult<()> {
        let Some(graph) = self
            .runner
            .state()?
            .and_then(|state| state.graph_state.clone())
        else {
            return Ok(());
        };
        let listed: BTreeSet<&str> = graph
            .node_states
            .values()
            .flat_map(|node| node.scheduled_effect_ids.iter().map(AgentEffectId::as_str))
            .collect();
        let read = self
            .inbox
            .outbox_effects(|entry| !listed.contains(entry.message_id().as_str()))?;
        if read.undecodable > 0 {
            tracing::warn!(
                skipped = read.undecodable,
                "graph recovery skipped outbox entries that do not decode as agent effects"
            );
        }
        let effects: Vec<AgentEffect> = read.effects.into_iter().map(|due| due.effect).collect();
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

    /// Answers the run's snapshot as the core's components hold it, after
    /// refreshing a stale one. Recorded under `snapshot`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or a component that cannot be read.
    pub async fn snapshot(&mut self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
        self.refresh_if_stale().await?;
        let result = self.read_snapshot();
        self.record_snapshot_result("snapshot", &result);
        result
    }

    fn read_snapshot(&self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
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

    /// Accepts an agent command through the durable inbox. Recorded under
    /// `accept-command`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the inbox refusing the command.
    pub async fn accept_command(
        &mut self,
        command: AgentCommand,
    ) -> AgentRunRuntimeResult<AgentInboxAcceptance> {
        self.refresh_if_stale().await?;
        let result = self
            .inbox
            .accept_command(command)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("accept-command", &result);
        result
    }

    /// Persists the initial accepted run state. Recorded under `start`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the start.
    pub async fn start(
        &mut self,
        initial_state: AgentRunState,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .start(initial_state)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("start", &result);
        result
    }

    /// Initializes compiled graph state and persists the accepted run state.
    /// Recorded under `start-graph`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the graph runtime refusing the
    /// start.
    pub async fn start_graph(
        &mut self,
        initial_state: AgentRunState,
        plan: &AgentCompiledExecutionPlan,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .graph_runtime
            .start_graph_run(&mut self.runner, initial_state, plan, now)
            .await;
        self.record_operation_result("start-graph", &result);
        result
    }

    /// Marks ready graph nodes runnable. Recorded under `mark-graph-ready`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the graph runtime refusing the
    /// transition.
    pub async fn mark_graph_ready(
        &mut self,
        plan: &AgentCompiledExecutionPlan,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .graph_runtime
            .mark_ready_nodes(&mut self.runner, plan, now)
            .await;
        self.record_operation_result("mark-graph-ready", &result);
        result
    }

    /// Starts one runnable graph node. Recorded under `start-graph-node`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the graph runtime refusing the
    /// transition.
    pub async fn start_graph_node(
        &mut self,
        plan: &AgentCompiledExecutionPlan,
        node_id: AgentCompiledNodeId,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .graph_runtime
            .start_node(&mut self.runner, plan, node_id, now)
            .await;
        self.record_operation_result("start-graph-node", &result);
        result
    }

    /// Completes one running graph node. Recorded under
    /// `complete-graph-node`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the graph runtime refusing the
    /// transition.
    pub async fn complete_graph_node(
        &mut self,
        plan: &AgentCompiledExecutionPlan,
        node_id: AgentCompiledNodeId,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .graph_runtime
            .complete_node(&mut self.runner, plan, node_id, now)
            .await;
        self.record_operation_result("complete-graph-node", &result);
        result
    }

    /// Schedules one running effect-producing graph node's effect. Recorded
    /// under `schedule-graph-node-effect`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the graph runtime or the outbox
    /// refusing the schedule.
    pub async fn schedule_graph_node_effect(
        &mut self,
        plan: &AgentCompiledExecutionPlan,
        request: AgentGraphEffectScheduleRequest,
    ) -> AgentRunRuntimeResult<AgentGraphRuntimeEffectOutcome> {
        self.refresh_if_stale().await?;
        let result = self
            .graph_runtime
            .schedule_node_effect(&mut self.runner, &mut self.inbox, plan, request)
            .await;
        self.record_operation_result("schedule-graph-node-effect", &result);
        result
    }

    /// Begins executing the current step. Recorded under `begin-step`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn begin_step(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .begin_step(now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("begin-step", &result);
        result
    }

    /// Records the current step's success. Recorded under `succeed-step`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn succeed_step(
        &mut self,
        success: AgentStepSuccess,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .succeed_step(success, now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("succeed-step", &result);
        result
    }

    /// Records the current step's failure and fails the run. Recorded under
    /// `fail-step`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn fail_step(
        &mut self,
        error_code: String,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .fail_step(error_code, now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("fail-step", &result);
        result
    }

    /// Pauses the run for a durable wait condition. Recorded under `wait`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn wait(
        &mut self,
        reason: AgentRunWaitReason,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .wait(reason, now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("wait", &result);
        result
    }

    /// Resumes a waiting run. Recorded under `resume`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn resume(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .resume(now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("resume", &result);
        result
    }

    /// Completes a running run. Recorded under `complete`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn complete(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .complete(now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("complete", &result);
        result
    }

    /// Fails a non-terminal run outside a specific step failure. Recorded
    /// under `fail-run`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn fail_run(
        &mut self,
        error_code: String,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .fail_run(error_code, now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("fail-run", &result);
        result
    }

    /// Requests run cancellation. Recorded under `request-cancellation`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn request_cancellation(
        &mut self,
        reason_code: String,
        reason_summary: Option<String>,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .request_cancellation(reason_code, reason_summary, now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("request-cancellation", &result);
        result
    }

    /// Marks a cancelling run cancelled. Recorded under `cancel`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn cancel(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .cancel(now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("cancel", &result);
        result
    }

    /// Moves a failed run into compensation. Recorded under
    /// `begin-compensation`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the runner refusing the transition.
    pub async fn begin_compensation(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunRuntimeResult<AgentRunTransition> {
        self.refresh_if_stale().await?;
        let result = self
            .runner
            .begin_compensation(now)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("begin-compensation", &result);
        result
    }

    /// Schedules a first-class effect through the durable outbox. Recorded
    /// under `schedule-effect`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or the outbox refusing the effect.
    pub async fn schedule_effect(
        &mut self,
        effect: AgentEffect,
    ) -> AgentRunRuntimeResult<AgentOutboxAcceptance> {
        self.refresh_if_stale().await?;
        let result = self
            .inbox
            .schedule_effect(effect)
            .await
            .map_err(AgentRunRuntimeError::from);
        self.record_operation_result("schedule-effect", &result);
        result
    }

    /// Answers the due effects in the durable outbox. Recorded under
    /// `due-effects`.
    ///
    /// # Errors
    ///
    /// The refresh's recovery failing, or an outbox entry that cannot be
    /// read.
    pub async fn due_effects(&mut self) -> AgentRunRuntimeResult<Vec<AgentDueEffect>> {
        self.refresh_if_stale().await?;
        let result: AgentOutboxResult<Vec<AgentDueEffect>> = self.inbox.due_effects();
        let result = result.map_err(AgentRunRuntimeError::from);
        self.record_operation_result("due-effects", &result);
        result
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
        let result = self.read_snapshot();
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
