//! Operational snapshots for agent workflow runtimes.
//!
//! Snapshots are point-in-time diagnostic views for humans and automation. They
//! summarize process-local runtime observations without exposing prompt,
//! response, tool, or artifact payloads.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::{
    AgentGraphNodeStatus, AgentGraphRunProjection, AgentGraphWaitReason, AgentRunActorSnapshot,
    AgentRunId, AgentRunRuntimeError, AgentRunState,
};

/// Operational snapshot name for aggregate runtime status.
pub const SNAPSHOT_AGENT_WORKFLOW_RUNTIME: &str = "agent_workflow_runtime";

/// Operational snapshot name for sharding status.
pub const SNAPSHOT_AGENT_WORKFLOW_SHARDS: &str = "agent_workflow_shards";

/// Operational snapshot name for aggregate durable outbox status.
pub const SNAPSHOT_AGENT_WORKFLOW_OUTBOX: &str = "agent_workflow_outbox";

/// Operational snapshot name for aggregate recovery status.
pub const SNAPSHOT_AGENT_WORKFLOW_RECOVERY: &str = "agent_workflow_recovery";

/// Operational snapshot name for aggregate human checkpoint status.
pub const SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS: &str = "agent_workflow_human_checkpoints";

const DEFAULT_MAX_SAMPLED_RUNS: usize = 64;

/// Process-local registry of bounded agent workflow operational snapshots.
#[derive(Clone, Debug)]
pub struct AgentWorkflowSnapshotRegistry {
    inner: Arc<Mutex<AgentWorkflowSnapshotState>>,
}

impl AgentWorkflowSnapshotRegistry {
    /// Creates a registry with the default sampled-run limit.
    #[must_use]
    pub fn new() -> Self {
        Self::with_max_sampled_runs(DEFAULT_MAX_SAMPLED_RUNS)
    }

    /// Creates a registry with an explicit sampled-run limit.
    #[must_use]
    pub fn with_max_sampled_runs(max_sampled_runs: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AgentWorkflowSnapshotState {
                max_sampled_runs: max_sampled_runs.max(1),
                runs: BTreeMap::new(),
                recovery_errors: BTreeMap::new(),
            })),
        }
    }

    /// Maximum run summaries included in each snapshot payload.
    #[must_use]
    pub fn max_sampled_runs(&self) -> usize {
        self.lock().max_sampled_runs
    }

    /// Records the latest actor-hosted run snapshot.
    pub fn record_run_actor_snapshot(&self, snapshot: &AgentRunActorSnapshot) {
        let run = AgentRunOperationalSnapshot::from_actor_snapshot(snapshot);
        let mut state = self.lock();
        state.recovery_errors.remove(&snapshot.run_id);
        state.runs.insert(snapshot.run_id.clone(), run);
    }

    /// Records a runtime or recovery error for one run.
    pub fn record_run_runtime_error(
        &self,
        run_id: AgentRunId,
        phase: impl Into<String>,
        error: &AgentRunRuntimeError,
    ) {
        self.lock().recovery_errors.insert(
            run_id.clone(),
            AgentRunRecoveryErrorSnapshot {
                run_id: run_id.as_str().to_string(),
                phase: phase.into(),
                error_code: error.code().to_string(),
                message: error.to_string(),
            },
        );
    }

    /// Removes one run from process-local diagnostics.
    pub fn remove_run(&self, run_id: &AgentRunId) {
        let mut state = self.lock();
        state.runs.remove(run_id);
        state.recovery_errors.remove(run_id);
    }

    /// Returns a bounded aggregate runtime snapshot.
    #[must_use]
    pub fn runtime_snapshot(&self) -> AgentWorkflowRuntimeSnapshot {
        let state = self.lock();
        AgentWorkflowRuntimeSnapshot::from_state(&state)
    }

    /// Returns a bounded aggregate outbox snapshot.
    #[must_use]
    pub fn outbox_snapshot(&self) -> AgentWorkflowOutboxSnapshot {
        let state = self.lock();
        AgentWorkflowOutboxSnapshot::from_state(&state)
    }

    /// Returns a bounded aggregate recovery snapshot.
    #[must_use]
    pub fn recovery_snapshot(&self) -> AgentWorkflowRecoverySnapshot {
        let state = self.lock();
        AgentWorkflowRecoverySnapshot::from_state(&state)
    }

    /// Returns a bounded aggregate human checkpoint snapshot.
    #[must_use]
    pub fn human_checkpoint_snapshot(&self) -> AgentWorkflowHumanCheckpointSnapshot {
        let state = self.lock();
        AgentWorkflowHumanCheckpointSnapshot::from_state(&state)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AgentWorkflowSnapshotState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for AgentWorkflowSnapshotRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct AgentWorkflowSnapshotState {
    max_sampled_runs: usize,
    runs: BTreeMap<AgentRunId, AgentRunOperationalSnapshot>,
    recovery_errors: BTreeMap<AgentRunId, AgentRunRecoveryErrorSnapshot>,
}

/// Bounded summary of one observed run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentRunOperationalSnapshot {
    run_id: String,
    workflow_id: Option<String>,
    status: Option<String>,
    current_step_id: Option<String>,
    current_attempt: u32,
    pending_command_count: usize,
    due_effect_count: usize,
    pending_human_checkpoint: Option<String>,
    open_checkpoint_count: usize,
    escalated_checkpoint_count: usize,
    due_checkpoint_count: usize,
    graph: Option<AgentGraphRunProjection>,
    recovered: bool,
    terminal: bool,
    updated_at_millis: Option<u64>,
}

impl AgentRunOperationalSnapshot {
    fn from_actor_snapshot(snapshot: &AgentRunActorSnapshot) -> Self {
        let run_state = snapshot.run_state.as_ref();
        Self {
            run_id: snapshot.run_id.as_str().to_string(),
            workflow_id: run_state.map(|state| state.workflow_id.as_str().to_string()),
            status: run_state.map(|state| state.status.as_label().to_string()),
            current_step_id: run_state
                .and_then(|state| state.current_step_id.as_ref())
                .map(|step_id| step_id.as_str().to_string()),
            current_attempt: run_state.map_or(0, |state| state.current_attempt),
            pending_command_count: snapshot.recoverable_command_count,
            due_effect_count: snapshot.due_effect_count,
            pending_human_checkpoint: run_state
                .and_then(|state| state.pending_human_checkpoint.as_ref())
                .map(|checkpoint_id| checkpoint_id.as_str().to_string()),
            open_checkpoint_count: run_state.map_or(0, open_checkpoint_count),
            escalated_checkpoint_count: run_state.map_or(0, escalated_checkpoint_count),
            due_checkpoint_count: run_state.map_or(0, due_checkpoint_count),
            graph: snapshot.graph.clone().or_else(|| {
                run_state
                    .and_then(|state| state.graph_state.as_ref())
                    .map(AgentGraphRunProjection::from_graph_state)
            }),
            recovered: run_state.is_some(),
            terminal: run_state.is_some_and(is_terminal_run_state),
            updated_at_millis: run_state.map(|state| state.updated_at.as_millis()),
        }
    }

    /// Run id.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Workflow id, if durable run state has recovered.
    #[must_use]
    pub fn workflow_id(&self) -> Option<&str> {
        self.workflow_id.as_deref()
    }

    /// Status label, if durable run state has recovered.
    #[must_use]
    pub fn status(&self) -> Option<&str> {
        self.status.as_deref()
    }

    /// Current step id, if known.
    #[must_use]
    pub fn current_step_id(&self) -> Option<&str> {
        self.current_step_id.as_deref()
    }

    /// Current step attempt.
    #[must_use]
    pub const fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    /// Recoverable durable inbox command count.
    #[must_use]
    pub const fn pending_command_count(&self) -> usize {
        self.pending_command_count
    }

    /// Due durable outbox effect count.
    #[must_use]
    pub const fn due_effect_count(&self) -> usize {
        self.due_effect_count
    }

    /// Pending human checkpoint id, if the run is waiting for human input.
    #[must_use]
    pub fn pending_human_checkpoint(&self) -> Option<&str> {
        self.pending_human_checkpoint.as_deref()
    }

    /// Open or escalated checkpoint count.
    #[must_use]
    pub const fn open_checkpoint_count(&self) -> usize {
        self.open_checkpoint_count
    }

    /// Escalated checkpoint count.
    #[must_use]
    pub const fn escalated_checkpoint_count(&self) -> usize {
        self.escalated_checkpoint_count
    }

    /// Open or escalated checkpoint count with a due timestamp.
    #[must_use]
    pub const fn due_checkpoint_count(&self) -> usize {
        self.due_checkpoint_count
    }

    /// Graph execution projection, when this run uses compiled graph execution.
    #[must_use]
    pub fn graph(&self) -> Option<&AgentGraphRunProjection> {
        self.graph.as_ref()
    }

    /// Returns true when durable run state has recovered.
    #[must_use]
    pub const fn recovered(&self) -> bool {
        self.recovered
    }

    /// Returns true when the run is in a terminal status.
    #[must_use]
    pub const fn terminal(&self) -> bool {
        self.terminal
    }

    /// Last durable run update timestamp, if known.
    #[must_use]
    pub const fn updated_at_millis(&self) -> Option<u64> {
        self.updated_at_millis
    }
}

/// Count of observed runs by status label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentRunStatusCount {
    status: String,
    count: usize,
}

impl AgentRunStatusCount {
    /// Creates a status count.
    #[must_use]
    pub fn new(status: impl Into<String>, count: usize) -> Self {
        Self {
            status: status.into(),
            count,
        }
    }

    /// Status label.
    #[must_use]
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Number of observed runs in this status.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }
}

/// Aggregate runtime snapshot for process-local agent runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWorkflowRuntimeSnapshot {
    observed_run_count: usize,
    active_run_count: usize,
    terminal_run_count: usize,
    pending_command_count: usize,
    due_effect_count: usize,
    graph_run_count: usize,
    graph_drain_blocker_count: usize,
    graph_runnable_node_count: usize,
    graph_running_node_count: usize,
    graph_waiting_node_count: usize,
    graph_effect_waiting_node_count: usize,
    graph_timer_waiting_node_count: usize,
    graph_human_waiting_node_count: usize,
    graph_child_workflow_waiting_node_count: usize,
    graph_failed_node_count: usize,
    graph_blocked_run_count: usize,
    status_counts: Vec<AgentRunStatusCount>,
    max_sampled_runs: usize,
    truncated_runs: usize,
    sampled_runs: Vec<AgentRunOperationalSnapshot>,
}

impl AgentWorkflowRuntimeSnapshot {
    fn from_state(state: &AgentWorkflowSnapshotState) -> Self {
        let mut status_counts = BTreeMap::<String, usize>::new();
        let mut active_run_count = 0;
        let mut terminal_run_count = 0;
        let mut pending_command_count = 0;
        let mut due_effect_count = 0;
        let mut graph_run_count = 0;
        let mut graph_drain_blocker_count = 0;
        let mut graph_runnable_node_count = 0;
        let mut graph_running_node_count = 0;
        let mut graph_waiting_node_count = 0;
        let mut graph_effect_waiting_node_count = 0;
        let mut graph_timer_waiting_node_count = 0;
        let mut graph_human_waiting_node_count = 0;
        let mut graph_child_workflow_waiting_node_count = 0;
        let mut graph_failed_node_count = 0;
        let mut graph_blocked_run_count = 0;
        for run in state.runs.values() {
            if let Some(status) = &run.status {
                *status_counts.entry(status.clone()).or_default() += 1;
            }
            if run.terminal {
                terminal_run_count += 1;
            } else {
                active_run_count += 1;
            }
            pending_command_count += run.pending_command_count;
            due_effect_count += run.due_effect_count;
            if let Some(graph) = &run.graph {
                graph_run_count += 1;
                graph_drain_blocker_count +=
                    graph.runnable_node_count + graph.running_node_count + graph.waiting_node_count;
                graph_runnable_node_count += graph.runnable_node_count;
                graph_running_node_count += graph.running_node_count;
                graph_waiting_node_count += graph.waiting_node_count;
                graph_effect_waiting_node_count +=
                    graph_wait_reason_count(graph, AgentGraphWaitReason::Effect);
                graph_timer_waiting_node_count +=
                    graph_wait_reason_count(graph, AgentGraphWaitReason::Timer);
                graph_human_waiting_node_count +=
                    graph_wait_reason_count(graph, AgentGraphWaitReason::Human);
                graph_child_workflow_waiting_node_count +=
                    graph_wait_reason_count(graph, AgentGraphWaitReason::ChildWorkflow);
                graph_failed_node_count += graph.failed_node_count;
                if graph.blocked_reason_code.is_some() {
                    graph_blocked_run_count += 1;
                }
            }
        }

        let sampled_runs = sample_runs(&state.runs, state.max_sampled_runs);
        Self {
            observed_run_count: state.runs.len(),
            active_run_count,
            terminal_run_count,
            pending_command_count,
            due_effect_count,
            graph_run_count,
            graph_drain_blocker_count,
            graph_runnable_node_count,
            graph_running_node_count,
            graph_waiting_node_count,
            graph_effect_waiting_node_count,
            graph_timer_waiting_node_count,
            graph_human_waiting_node_count,
            graph_child_workflow_waiting_node_count,
            graph_failed_node_count,
            graph_blocked_run_count,
            status_counts: status_counts
                .into_iter()
                .map(|(status, count)| AgentRunStatusCount::new(status, count))
                .collect(),
            max_sampled_runs: state.max_sampled_runs,
            truncated_runs: truncated_count(state.runs.len(), state.max_sampled_runs),
            sampled_runs,
        }
    }

    /// Number of observed runs.
    #[must_use]
    pub const fn observed_run_count(&self) -> usize {
        self.observed_run_count
    }

    /// Number of observed non-terminal runs.
    #[must_use]
    pub const fn active_run_count(&self) -> usize {
        self.active_run_count
    }

    /// Number of observed terminal runs.
    #[must_use]
    pub const fn terminal_run_count(&self) -> usize {
        self.terminal_run_count
    }

    /// Total recoverable durable inbox commands across observed runs.
    #[must_use]
    pub const fn pending_command_count(&self) -> usize {
        self.pending_command_count
    }

    /// Total due durable outbox effects across observed runs.
    #[must_use]
    pub const fn due_effect_count(&self) -> usize {
        self.due_effect_count
    }

    /// Number of observed runs with compiled graph state.
    #[must_use]
    pub const fn graph_run_count(&self) -> usize {
        self.graph_run_count
    }

    /// Total graph nodes that can block drain across observed runs.
    #[must_use]
    pub const fn graph_drain_blocker_count(&self) -> usize {
        self.graph_drain_blocker_count
    }

    /// Total runnable graph-node count across observed runs.
    #[must_use]
    pub const fn graph_runnable_node_count(&self) -> usize {
        self.graph_runnable_node_count
    }

    /// Total running graph-node count across observed runs.
    #[must_use]
    pub const fn graph_running_node_count(&self) -> usize {
        self.graph_running_node_count
    }

    /// Total waiting graph-node count across observed runs.
    #[must_use]
    pub const fn graph_waiting_node_count(&self) -> usize {
        self.graph_waiting_node_count
    }

    /// Total graph-node count waiting for durable outbox effect completion.
    #[must_use]
    pub const fn graph_effect_waiting_node_count(&self) -> usize {
        self.graph_effect_waiting_node_count
    }

    /// Total graph-node count waiting for durable timer completion.
    #[must_use]
    pub const fn graph_timer_waiting_node_count(&self) -> usize {
        self.graph_timer_waiting_node_count
    }

    /// Total graph-node count waiting for human checkpoint decisions.
    #[must_use]
    pub const fn graph_human_waiting_node_count(&self) -> usize {
        self.graph_human_waiting_node_count
    }

    /// Total graph-node count waiting for child workflow completion.
    #[must_use]
    pub const fn graph_child_workflow_waiting_node_count(&self) -> usize {
        self.graph_child_workflow_waiting_node_count
    }

    /// Total failed graph-node count across observed runs.
    #[must_use]
    pub const fn graph_failed_node_count(&self) -> usize {
        self.graph_failed_node_count
    }

    /// Number of observed graph runs with a blocked reason.
    #[must_use]
    pub const fn graph_blocked_run_count(&self) -> usize {
        self.graph_blocked_run_count
    }

    /// Counts by run status label.
    #[must_use]
    pub fn status_counts(&self) -> &[AgentRunStatusCount] {
        &self.status_counts
    }

    /// Maximum number of sampled runs included in this payload.
    #[must_use]
    pub const fn max_sampled_runs(&self) -> usize {
        self.max_sampled_runs
    }

    /// Number of observed runs omitted from sampled run details.
    #[must_use]
    pub const fn truncated_runs(&self) -> usize {
        self.truncated_runs
    }

    /// Bounded sampled run summaries.
    #[must_use]
    pub fn sampled_runs(&self) -> &[AgentRunOperationalSnapshot] {
        &self.sampled_runs
    }
}

/// Bounded human checkpoint summary for one observed run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentRunHumanCheckpointSnapshot {
    run_id: String,
    status: Option<String>,
    pending_checkpoint_id: Option<String>,
    open_checkpoint_count: usize,
    escalated_checkpoint_count: usize,
    due_checkpoint_count: usize,
}

impl AgentRunHumanCheckpointSnapshot {
    fn from_run(run: &AgentRunOperationalSnapshot) -> Self {
        Self {
            run_id: run.run_id.clone(),
            status: run.status.clone(),
            pending_checkpoint_id: run.pending_human_checkpoint.clone(),
            open_checkpoint_count: run.open_checkpoint_count,
            escalated_checkpoint_count: run.escalated_checkpoint_count,
            due_checkpoint_count: run.due_checkpoint_count,
        }
    }

    /// Run id.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Run status label, when known.
    #[must_use]
    pub fn status(&self) -> Option<&str> {
        self.status.as_deref()
    }

    /// Pending checkpoint id, when the run is waiting on a checkpoint.
    #[must_use]
    pub fn pending_checkpoint_id(&self) -> Option<&str> {
        self.pending_checkpoint_id.as_deref()
    }

    /// Open or escalated checkpoint count.
    #[must_use]
    pub const fn open_checkpoint_count(&self) -> usize {
        self.open_checkpoint_count
    }

    /// Escalated checkpoint count.
    #[must_use]
    pub const fn escalated_checkpoint_count(&self) -> usize {
        self.escalated_checkpoint_count
    }

    /// Open or escalated checkpoint count with a due timestamp.
    #[must_use]
    pub const fn due_checkpoint_count(&self) -> usize {
        self.due_checkpoint_count
    }
}

/// Aggregate human checkpoint snapshot for process-local agent runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWorkflowHumanCheckpointSnapshot {
    observed_run_count: usize,
    waiting_run_count: usize,
    open_checkpoint_count: usize,
    escalated_checkpoint_count: usize,
    due_checkpoint_count: usize,
    max_sampled_runs: usize,
    truncated_runs: usize,
    sampled_runs: Vec<AgentRunHumanCheckpointSnapshot>,
}

impl AgentWorkflowHumanCheckpointSnapshot {
    fn from_state(state: &AgentWorkflowSnapshotState) -> Self {
        let runs: Vec<_> = state
            .runs
            .values()
            .filter(|run| run.open_checkpoint_count > 0 || run.pending_human_checkpoint.is_some())
            .collect();
        let waiting_run_count = runs
            .iter()
            .filter(|run| run.status.as_deref() == Some("waiting-for-human"))
            .count();
        let open_checkpoint_count = runs.iter().map(|run| run.open_checkpoint_count).sum();
        let escalated_checkpoint_count =
            runs.iter().map(|run| run.escalated_checkpoint_count).sum();
        let due_checkpoint_count = runs.iter().map(|run| run.due_checkpoint_count).sum();
        let sampled_runs = runs
            .iter()
            .take(state.max_sampled_runs)
            .map(|run| AgentRunHumanCheckpointSnapshot::from_run(run))
            .collect();
        Self {
            observed_run_count: state.runs.len(),
            waiting_run_count,
            open_checkpoint_count,
            escalated_checkpoint_count,
            due_checkpoint_count,
            max_sampled_runs: state.max_sampled_runs,
            truncated_runs: truncated_count(runs.len(), state.max_sampled_runs),
            sampled_runs,
        }
    }

    /// Number of observed runs.
    #[must_use]
    pub const fn observed_run_count(&self) -> usize {
        self.observed_run_count
    }

    /// Number of observed runs waiting for human input.
    #[must_use]
    pub const fn waiting_run_count(&self) -> usize {
        self.waiting_run_count
    }

    /// Open or escalated checkpoint count.
    #[must_use]
    pub const fn open_checkpoint_count(&self) -> usize {
        self.open_checkpoint_count
    }

    /// Escalated checkpoint count.
    #[must_use]
    pub const fn escalated_checkpoint_count(&self) -> usize {
        self.escalated_checkpoint_count
    }

    /// Open or escalated checkpoint count with a due timestamp.
    #[must_use]
    pub const fn due_checkpoint_count(&self) -> usize {
        self.due_checkpoint_count
    }

    /// Maximum number of sampled runs included in this payload.
    #[must_use]
    pub const fn max_sampled_runs(&self) -> usize {
        self.max_sampled_runs
    }

    /// Number of checkpoint runs omitted from sampled run details.
    #[must_use]
    pub const fn truncated_runs(&self) -> usize {
        self.truncated_runs
    }

    /// Bounded sampled run checkpoint summaries.
    #[must_use]
    pub fn sampled_runs(&self) -> &[AgentRunHumanCheckpointSnapshot] {
        &self.sampled_runs
    }
}

/// Bounded outbox summary for one observed run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentRunOutboxSnapshot {
    run_id: String,
    due_effect_count: usize,
}

impl AgentRunOutboxSnapshot {
    fn from_run(run: &AgentRunOperationalSnapshot) -> Self {
        Self {
            run_id: run.run_id.clone(),
            due_effect_count: run.due_effect_count,
        }
    }

    /// Run id.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Due durable outbox effect count.
    #[must_use]
    pub const fn due_effect_count(&self) -> usize {
        self.due_effect_count
    }
}

/// Aggregate durable outbox snapshot for process-local agent runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWorkflowOutboxSnapshot {
    observed_run_count: usize,
    due_effect_count: usize,
    runs_with_due_effects: usize,
    max_sampled_runs: usize,
    truncated_runs: usize,
    sampled_runs: Vec<AgentRunOutboxSnapshot>,
}

impl AgentWorkflowOutboxSnapshot {
    fn from_state(state: &AgentWorkflowSnapshotState) -> Self {
        let runs: Vec<_> = state
            .runs
            .values()
            .filter(|run| run.due_effect_count > 0)
            .collect();
        let due_effect_count = runs.iter().map(|run| run.due_effect_count).sum();
        let sampled_runs = runs
            .iter()
            .take(state.max_sampled_runs)
            .map(|run| AgentRunOutboxSnapshot::from_run(run))
            .collect();
        Self {
            observed_run_count: state.runs.len(),
            due_effect_count,
            runs_with_due_effects: runs.len(),
            max_sampled_runs: state.max_sampled_runs,
            truncated_runs: truncated_count(runs.len(), state.max_sampled_runs),
            sampled_runs,
        }
    }

    /// Number of observed runs.
    #[must_use]
    pub const fn observed_run_count(&self) -> usize {
        self.observed_run_count
    }

    /// Total due durable outbox effects.
    #[must_use]
    pub const fn due_effect_count(&self) -> usize {
        self.due_effect_count
    }

    /// Number of observed runs with due effects.
    #[must_use]
    pub const fn runs_with_due_effects(&self) -> usize {
        self.runs_with_due_effects
    }

    /// Maximum number of sampled runs included in this payload.
    #[must_use]
    pub const fn max_sampled_runs(&self) -> usize {
        self.max_sampled_runs
    }

    /// Number of due-effect runs omitted from sampled run details.
    #[must_use]
    pub const fn truncated_runs(&self) -> usize {
        self.truncated_runs
    }

    /// Bounded sampled run outbox summaries.
    #[must_use]
    pub fn sampled_runs(&self) -> &[AgentRunOutboxSnapshot] {
        &self.sampled_runs
    }
}

/// Runtime or recovery error observed for one run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentRunRecoveryErrorSnapshot {
    run_id: String,
    phase: String,
    error_code: String,
    message: String,
}

impl AgentRunRecoveryErrorSnapshot {
    /// Run id.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Runtime phase that observed the error.
    #[must_use]
    pub fn phase(&self) -> &str {
        &self.phase
    }

    /// Stable error code.
    #[must_use]
    pub fn error_code(&self) -> &str {
        &self.error_code
    }

    /// Diagnostic error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Aggregate recovery snapshot for process-local agent runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWorkflowRecoverySnapshot {
    observed_run_count: usize,
    recovered_run_count: usize,
    unrecovered_run_count: usize,
    pending_command_count: usize,
    runs_with_pending_commands: usize,
    recovery_error_count: usize,
    max_sampled_runs: usize,
    truncated_runs: usize,
    sampled_runs: Vec<AgentRunOperationalSnapshot>,
    sampled_errors: Vec<AgentRunRecoveryErrorSnapshot>,
}

impl AgentWorkflowRecoverySnapshot {
    fn from_state(state: &AgentWorkflowSnapshotState) -> Self {
        let recovered_run_count = state.runs.values().filter(|run| run.recovered).count();
        let pending_runs: Vec<_> = state
            .runs
            .values()
            .filter(|run| run.pending_command_count > 0)
            .collect();
        let pending_command_count = pending_runs
            .iter()
            .map(|run| run.pending_command_count)
            .sum();
        let sampled_runs = pending_runs
            .iter()
            .take(state.max_sampled_runs)
            .map(|run| (*run).clone())
            .collect();
        let sampled_errors = state
            .recovery_errors
            .values()
            .take(state.max_sampled_runs)
            .cloned()
            .collect();
        Self {
            observed_run_count: state.runs.len(),
            recovered_run_count,
            unrecovered_run_count: state.runs.len().saturating_sub(recovered_run_count),
            pending_command_count,
            runs_with_pending_commands: pending_runs.len(),
            recovery_error_count: state.recovery_errors.len(),
            max_sampled_runs: state.max_sampled_runs,
            truncated_runs: truncated_count(
                pending_runs.len() + state.recovery_errors.len(),
                state.max_sampled_runs,
            ),
            sampled_runs,
            sampled_errors,
        }
    }

    /// Number of observed runs.
    #[must_use]
    pub const fn observed_run_count(&self) -> usize {
        self.observed_run_count
    }

    /// Number of observed runs with recovered durable state.
    #[must_use]
    pub const fn recovered_run_count(&self) -> usize {
        self.recovered_run_count
    }

    /// Number of observed runs without recovered durable state.
    #[must_use]
    pub const fn unrecovered_run_count(&self) -> usize {
        self.unrecovered_run_count
    }

    /// Total recoverable durable inbox commands.
    #[must_use]
    pub const fn pending_command_count(&self) -> usize {
        self.pending_command_count
    }

    /// Number of observed runs with recoverable commands.
    #[must_use]
    pub const fn runs_with_pending_commands(&self) -> usize {
        self.runs_with_pending_commands
    }

    /// Number of recorded runtime or recovery errors.
    #[must_use]
    pub const fn recovery_error_count(&self) -> usize {
        self.recovery_error_count
    }

    /// Maximum number of sampled items included in this payload.
    #[must_use]
    pub const fn max_sampled_runs(&self) -> usize {
        self.max_sampled_runs
    }

    /// Number of pending-command/error items omitted from details.
    #[must_use]
    pub const fn truncated_runs(&self) -> usize {
        self.truncated_runs
    }

    /// Bounded sampled pending-command run summaries.
    #[must_use]
    pub fn sampled_runs(&self) -> &[AgentRunOperationalSnapshot] {
        &self.sampled_runs
    }

    /// Bounded sampled runtime or recovery errors.
    #[must_use]
    pub fn sampled_errors(&self) -> &[AgentRunRecoveryErrorSnapshot] {
        &self.sampled_errors
    }
}

/// Bounded summary of sharded agent run registrations.
#[cfg(feature = "sharding")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWorkflowShardSnapshot {
    entity_type_count: usize,
    local_entity_count: usize,
    buffered_message_count: usize,
    entity_types: Vec<AgentWorkflowShardEntityTypeSnapshot>,
}

#[cfg(feature = "sharding")]
impl AgentWorkflowShardSnapshot {
    /// Creates a shard snapshot from the sharding facade state.
    #[must_use]
    pub fn from_cluster_sharding(sharding: &rakka_sharding::ClusterSharding) -> Self {
        Self::from_cluster_sharding_state(&sharding.state())
    }

    /// Creates a shard snapshot from a sharding state value.
    #[must_use]
    pub fn from_cluster_sharding_state(state: &rakka_sharding::ClusterShardingState) -> Self {
        let entity_types: Vec<_> = state
            .entity_types()
            .iter()
            .map(AgentWorkflowShardEntityTypeSnapshot::from_registration_state)
            .collect();
        Self {
            entity_type_count: entity_types.len(),
            local_entity_count: entity_types
                .iter()
                .map(AgentWorkflowShardEntityTypeSnapshot::local_entity_count)
                .sum(),
            buffered_message_count: entity_types
                .iter()
                .map(AgentWorkflowShardEntityTypeSnapshot::buffered_message_count)
                .sum(),
            entity_types,
        }
    }

    /// Number of registered entity types.
    #[must_use]
    pub const fn entity_type_count(&self) -> usize {
        self.entity_type_count
    }

    /// Total local entity count across registrations.
    #[must_use]
    pub const fn local_entity_count(&self) -> usize {
        self.local_entity_count
    }

    /// Total buffered message count across registrations.
    #[must_use]
    pub const fn buffered_message_count(&self) -> usize {
        self.buffered_message_count
    }

    /// Per-entity-type shard summaries.
    #[must_use]
    pub fn entity_types(&self) -> &[AgentWorkflowShardEntityTypeSnapshot] {
        &self.entity_types
    }
}

/// Bounded summary of one sharded entity type registration.
#[cfg(feature = "sharding")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWorkflowShardEntityTypeSnapshot {
    entity_type: String,
    number_of_shards: u32,
    local_entity_count: usize,
    buffered_message_count: usize,
    idle_passivation_timeout_ms: Option<u64>,
    remembered_entities_enabled: bool,
    remembered_store_backend: Option<String>,
}

#[cfg(feature = "sharding")]
impl AgentWorkflowShardEntityTypeSnapshot {
    fn from_registration_state(state: &rakka_sharding::EntityTypeRegistrationState) -> Self {
        Self {
            entity_type: state.entity_type().as_str().to_string(),
            number_of_shards: state.number_of_shards(),
            local_entity_count: state.local_entity_count(),
            buffered_message_count: state.buffered_message_count(),
            idle_passivation_timeout_ms: state
                .idle_passivation_timeout()
                .map(|duration| duration.as_millis() as u64),
            remembered_entities_enabled: state.remembered_entities_enabled(),
            remembered_store_backend: state.remembered_store_backend().map(str::to_string),
        }
    }

    /// Entity type name.
    #[must_use]
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Configured shard count.
    #[must_use]
    pub const fn number_of_shards(&self) -> u32 {
        self.number_of_shards
    }

    /// Local entity count.
    #[must_use]
    pub const fn local_entity_count(&self) -> usize {
        self.local_entity_count
    }

    /// Buffered message count.
    #[must_use]
    pub const fn buffered_message_count(&self) -> usize {
        self.buffered_message_count
    }

    /// Idle passivation timeout in milliseconds.
    #[must_use]
    pub const fn idle_passivation_timeout_ms(&self) -> Option<u64> {
        self.idle_passivation_timeout_ms
    }

    /// Returns true when remembered entities are enabled.
    #[must_use]
    pub const fn remembered_entities_enabled(&self) -> bool {
        self.remembered_entities_enabled
    }

    /// Remembered entity store backend.
    #[must_use]
    pub fn remembered_store_backend(&self) -> Option<&str> {
        self.remembered_store_backend.as_deref()
    }
}

/// Creates a bounded shard snapshot from the sharding facade.
#[cfg(feature = "sharding")]
#[must_use]
pub fn agent_workflow_shards_snapshot(
    sharding: &rakka_sharding::ClusterSharding,
) -> AgentWorkflowShardSnapshot {
    AgentWorkflowShardSnapshot::from_cluster_sharding(sharding)
}

/// Registers runtime, outbox, and recovery snapshots with an HTTP operational
/// snapshot registry.
#[cfg(feature = "http")]
pub fn register_agent_workflow_operational_snapshots(
    registry: &rakka_http::OperationalSnapshotRegistry,
    snapshots: AgentWorkflowSnapshotRegistry,
) {
    let runtime_snapshots = snapshots.clone();
    registry.register_snapshot::<AgentWorkflowRuntimeSnapshot, _>(
        SNAPSHOT_AGENT_WORKFLOW_RUNTIME,
        move || runtime_snapshots.runtime_snapshot(),
    );

    let outbox_snapshots = snapshots.clone();
    registry.register_snapshot::<AgentWorkflowOutboxSnapshot, _>(
        SNAPSHOT_AGENT_WORKFLOW_OUTBOX,
        move || outbox_snapshots.outbox_snapshot(),
    );

    let recovery_snapshots = snapshots.clone();
    registry.register_snapshot::<AgentWorkflowRecoverySnapshot, _>(
        SNAPSHOT_AGENT_WORKFLOW_RECOVERY,
        move || recovery_snapshots.recovery_snapshot(),
    );

    let human_snapshots = snapshots.clone();
    registry.register_snapshot::<AgentWorkflowHumanCheckpointSnapshot, _>(
        SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS,
        move || human_snapshots.human_checkpoint_snapshot(),
    );
}

/// Registers the sharding snapshot with an HTTP operational snapshot registry.
#[cfg(all(feature = "http", feature = "sharding"))]
pub fn register_agent_workflow_shard_snapshot(
    registry: &rakka_http::OperationalSnapshotRegistry,
    sharding: Arc<rakka_sharding::ClusterSharding>,
) {
    registry.register_snapshot::<AgentWorkflowShardSnapshot, _>(
        SNAPSHOT_AGENT_WORKFLOW_SHARDS,
        move || agent_workflow_shards_snapshot(&sharding),
    );
}

fn sample_runs(
    runs: &BTreeMap<AgentRunId, AgentRunOperationalSnapshot>,
    max: usize,
) -> Vec<AgentRunOperationalSnapshot> {
    runs.values().take(max).cloned().collect()
}

const fn truncated_count(total: usize, max: usize) -> usize {
    total.saturating_sub(max)
}

const fn is_terminal_run_state(state: &AgentRunState) -> bool {
    matches!(
        state.status,
        crate::AgentRunStatus::Completed
            | crate::AgentRunStatus::Failed
            | crate::AgentRunStatus::Cancelled
    )
}

fn graph_wait_reason_count(graph: &AgentGraphRunProjection, reason: AgentGraphWaitReason) -> usize {
    graph
        .nodes
        .iter()
        .filter(|node| {
            node.status == AgentGraphNodeStatus::Waiting && node.wait_reason == Some(reason)
        })
        .count()
}

fn open_checkpoint_count(state: &AgentRunState) -> usize {
    state
        .checkpoints
        .iter()
        .filter(|checkpoint| {
            matches!(
                checkpoint.status,
                crate::HumanCheckpointStatus::Open | crate::HumanCheckpointStatus::Escalated
            )
        })
        .count()
}

fn escalated_checkpoint_count(state: &AgentRunState) -> usize {
    state
        .checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.status == crate::HumanCheckpointStatus::Escalated)
        .count()
}

fn due_checkpoint_count(state: &AgentRunState) -> usize {
    state
        .checkpoints
        .iter()
        .filter(|checkpoint| {
            matches!(
                checkpoint.status,
                crate::HumanCheckpointStatus::Open | crate::HumanCheckpointStatus::Escalated
            ) && checkpoint.due_at.is_some()
        })
        .count()
}
