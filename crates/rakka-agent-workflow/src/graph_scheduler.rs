//! Deterministic compiled graph scheduler core.
//!
//! The scheduler is intentionally side-effect free in this slice. It evaluates
//! a compiled plan against durable graph state and returns updated state for
//! the caller to persist before any node work is executed.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{
    validate_compiled_execution_plan, AgentCompiledEdgeId, AgentCompiledEdgeMergeBehavior,
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanEdge,
    AgentCompiledPlanNode, AgentCompiledPortId, AgentGraphLoopInstanceState, AgentGraphNodeState,
    AgentGraphNodeStatus, AgentGraphRunState, AgentGraphTerminalStatus, AgentGraphWaitReason,
    AgentTimestampMillis, ArtifactRef,
};

/// Result type for graph scheduler operations.
pub type AgentGraphSchedulerResult<T> = Result<T, AgentGraphSchedulerError>;

/// Stable graph scheduler error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentGraphSchedulerError {
    /// Compiled plan validation failed before scheduling.
    InvalidCompiledPlan {
        /// Stable compiled-plan validation code.
        validation_code: &'static str,
        /// Bounded diagnostic reason.
        reason: String,
    },
    /// Durable graph state does not match the compiled plan.
    PlanStateMismatch {
        /// Mismatched field.
        field: &'static str,
        /// Bounded diagnostic reason.
        reason: String,
    },
    /// Durable graph state is missing a node from the compiled plan.
    MissingNodeState {
        /// Missing node id.
        node_id: AgentCompiledNodeId,
    },
    /// A caller referenced a node not present in the compiled plan.
    UnknownNode {
        /// Unknown node id.
        node_id: AgentCompiledNodeId,
    },
    /// A node transition was not valid from the current durable status.
    InvalidNodeTransition {
        /// Node id.
        node_id: AgentCompiledNodeId,
        /// Current durable status.
        from: AgentGraphNodeStatus,
        /// Requested durable status.
        to: AgentGraphNodeStatus,
    },
    /// A branch node was completed without a valid branch selection.
    InvalidBranchSelection {
        /// Branch node id.
        node_id: AgentCompiledNodeId,
        /// Bounded diagnostic reason.
        reason: String,
    },
    /// An iterator node transition or iteration request was invalid.
    InvalidIteratorTransition {
        /// Iterator node id.
        node_id: AgentCompiledNodeId,
        /// Bounded diagnostic reason.
        reason: String,
    },
    /// Scheduler was asked to mutate a terminal graph.
    TerminalGraph {
        /// Terminal graph status.
        status: AgentGraphTerminalStatus,
    },
}

impl AgentGraphSchedulerError {
    /// Stable scheduler error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidCompiledPlan { .. } => "invalid-compiled-plan",
            Self::PlanStateMismatch { .. } => "graph-plan-state-mismatch",
            Self::MissingNodeState { .. } => "missing-graph-node-state",
            Self::UnknownNode { .. } => "unknown-graph-node",
            Self::InvalidNodeTransition { .. } => "invalid-graph-node-transition",
            Self::InvalidBranchSelection { .. } => "invalid-branch-selection",
            Self::InvalidIteratorTransition { .. } => "invalid-iterator-transition",
            Self::TerminalGraph { .. } => "terminal-graph",
        }
    }
}

impl Display for AgentGraphSchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCompiledPlan {
                validation_code,
                reason,
            } => write!(
                f,
                "compiled graph plan failed validation ({validation_code}): {reason}"
            ),
            Self::PlanStateMismatch { field, reason } => {
                write!(
                    f,
                    "graph state field `{field}` does not match plan: {reason}"
                )
            }
            Self::MissingNodeState { node_id } => {
                write!(f, "graph state is missing node `{node_id}`")
            }
            Self::UnknownNode { node_id } => {
                write!(f, "compiled graph does not contain node `{node_id}`")
            }
            Self::InvalidNodeTransition { node_id, from, to } => write!(
                f,
                "node `{node_id}` cannot transition from `{}` to `{}`",
                from.as_label(),
                to.as_label()
            ),
            Self::InvalidBranchSelection { node_id, reason } => {
                write!(f, "branch node `{node_id}` has invalid selection: {reason}")
            }
            Self::InvalidIteratorTransition { node_id, reason } => {
                write!(
                    f,
                    "iterator node `{node_id}` has invalid transition: {reason}"
                )
            }
            Self::TerminalGraph { status } => {
                write!(
                    f,
                    "graph is already terminal with status `{}`",
                    status.as_label()
                )
            }
        }
    }
}

impl Error for AgentGraphSchedulerError {}

/// Result of one scheduler state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentGraphSchedulerTransition {
    /// Updated durable graph state for the caller to persist.
    pub state: AgentGraphRunState,
    /// Nodes changed by this transition, sorted by stable node id.
    pub changed_node_ids: Vec<AgentCompiledNodeId>,
    /// Nodes currently runnable after this transition, sorted by stable node id.
    pub runnable_node_ids: Vec<AgentCompiledNodeId>,
}

impl AgentGraphSchedulerTransition {
    fn new(state: AgentGraphRunState, changed_node_ids: Vec<AgentCompiledNodeId>) -> Self {
        let runnable_node_ids = runnable_nodes_from_state(&state);
        Self {
            state,
            changed_node_ids,
            runnable_node_ids,
        }
    }
}

/// Deterministic per-run compiled graph scheduler.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentGraphScheduler;

impl AgentGraphScheduler {
    /// Creates a graph scheduler.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Initializes durable graph state from a compiled plan.
    pub fn initialize_state(
        &self,
        plan: &AgentCompiledExecutionPlan,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphRunState> {
        validate_plan(plan)?;
        let mut state =
            AgentGraphRunState::new(plan.plan_id.clone(), plan.plan_fingerprint.clone());
        for node in sorted_nodes(plan) {
            state.node_states.insert(
                node.node_id.clone(),
                AgentGraphNodeState::new(node.node_id.clone(), node.kind, now),
            );
        }
        Ok(state)
    }

    /// Computes pending nodes whose dependencies are satisfied.
    pub fn compute_ready_nodes(
        &self,
        plan: &AgentCompiledExecutionPlan,
        state: &AgentGraphRunState,
    ) -> AgentGraphSchedulerResult<Vec<AgentCompiledNodeId>> {
        validate_plan_state(plan, state)?;
        if state.terminal_status.is_some() {
            return Ok(Vec::new());
        }
        let incoming = incoming_edges_by_target(plan);
        let nodes = nodes_by_id(plan);
        let mut ready = Vec::new();
        for node in sorted_nodes(plan) {
            let Some(node_state) = state.node_states.get(&node.node_id) else {
                return Err(AgentGraphSchedulerError::MissingNodeState {
                    node_id: node.node_id.clone(),
                });
            };
            if node_state.status != AgentGraphNodeStatus::Pending {
                continue;
            }
            if dependencies_satisfied(plan, state, node, &nodes, &incoming)? {
                ready.push(node.node_id.clone());
            }
        }
        Ok(ready)
    }

    /// Returns currently runnable nodes from durable state.
    pub fn runnable_nodes(&self, state: &AgentGraphRunState) -> Vec<AgentCompiledNodeId> {
        runnable_nodes_from_state(state)
    }

    /// Marks all currently ready pending nodes as runnable.
    pub fn mark_ready_nodes_runnable(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let ready = self.compute_ready_nodes(plan, &state)?;
        for node_id in &ready {
            let node_state = state.node_states.get_mut(node_id).ok_or_else(|| {
                AgentGraphSchedulerError::MissingNodeState {
                    node_id: node_id.clone(),
                }
            })?;
            node_state.status = AgentGraphNodeStatus::Runnable;
            node_state.dependencies_ready = true;
            node_state.updated_at = now;
        }
        if !ready.is_empty() {
            state.scheduler_revision += 1;
            state.blocked_reason = None;
        }
        Ok(AgentGraphSchedulerTransition::new(state, ready))
    }

    /// Transitions a runnable node to running.
    pub fn start_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        ensure_known_node(plan, &node_id)?;
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Runnable,
            AgentGraphNodeStatus::Running,
            now,
            |node_state| {
                node_state.attempt += 1;
                node_state.started_at = Some(now);
                node_state.wait_reason = None;
                node_state.error_code = None;
            },
        )?;
        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
    }

    /// Transitions a running node to completed, or terminal for terminal nodes.
    pub fn complete_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        let node = ensure_known_node(plan, &node_id)?;
        if node.kind == AgentCompiledNodeKind::Branch {
            return Err(AgentGraphSchedulerError::InvalidBranchSelection {
                node_id,
                reason: "branch nodes must complete with selected outgoing edge ids".to_string(),
            });
        }
        if node.kind == AgentCompiledNodeKind::Iterator {
            return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
                node_id,
                reason: "iterator nodes must complete through iterator-specific transitions"
                    .to_string(),
            });
        }
        let next_status = if node.kind == AgentCompiledNodeKind::Terminal {
            AgentGraphNodeStatus::Terminal
        } else {
            AgentGraphNodeStatus::Completed
        };
        let mut changed_node_ids = vec![node_id.clone()];
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Running,
            next_status,
            now,
            |node_state| {
                node_state.completed_at = Some(now);
                node_state.wait_reason = None;
                node_state.error_code = None;
            },
        )?;
        if next_status == AgentGraphNodeStatus::Terminal {
            state.terminal_status = Some(AgentGraphTerminalStatus::Completed);
            let mut excluded_node_ids = BTreeSet::new();
            excluded_node_ids.insert(node_id);
            changed_node_ids.extend(cancel_unresolved_nodes(&mut state, now, &excluded_node_ids));
            changed_node_ids.extend(cancel_unresolved_loop_instances(&mut state, now));
        }
        changed_node_ids.sort();
        changed_node_ids.dedup();
        Ok(AgentGraphSchedulerTransition::new(state, changed_node_ids))
    }

    /// Transitions a running branch node to completed with durable selected paths.
    pub fn complete_branch_node<I, E>(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        selected_edge_ids: I,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition>
    where
        I: IntoIterator<Item = E>,
        E: Into<AgentCompiledEdgeId>,
    {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        let node = ensure_known_node(plan, &node_id)?;
        if node.kind != AgentCompiledNodeKind::Branch {
            return Err(AgentGraphSchedulerError::InvalidBranchSelection {
                node_id,
                reason: "node is not a branch".to_string(),
            });
        }
        ensure_node_status(
            &state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Completed,
        )?;

        let selected_edge_ids = validate_branch_selection(plan, &node_id, selected_edge_ids)?;

        state
            .selected_branch_paths
            .insert(node_id.clone(), selected_edge_ids);
        {
            let node_state = state.node_states.get_mut(&node_id).ok_or_else(|| {
                AgentGraphSchedulerError::MissingNodeState {
                    node_id: node_id.clone(),
                }
            })?;
            node_state.status = AgentGraphNodeStatus::Completed;
            node_state.updated_at = now;
            node_state.completed_at = Some(now);
            node_state.wait_reason = None;
            node_state.error_code = None;
        }

        let mut changed_node_ids = vec![node_id];
        changed_node_ids.extend(propagate_skips(plan, &mut state, now)?);
        changed_node_ids.sort();
        changed_node_ids.dedup();

        state.scheduler_revision += 1;
        state.blocked_reason = None;

        Ok(AgentGraphSchedulerTransition::new(state, changed_node_ids))
    }

    /// Returns the active iteration index for an iterator node, when one exists.
    pub fn current_iterator_iteration_index(
        &self,
        state: &AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
    ) -> Option<u32> {
        let node_id = node_id.into();
        current_iterator_iteration_index(state, &node_id)
    }

    /// Starts the next deterministic iteration for a running iterator node.
    pub fn start_iterator_iteration(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        let node = ensure_iterator_node(plan, &node_id)?;
        ensure_node_status(
            &state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Running,
        )?;
        if current_iterator_iteration_index(&state, &node_id).is_some() {
            return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
                node_id,
                reason: "an iteration is already active".to_string(),
            });
        }

        let max_iterations = node
            .iterator_policy
            .expect("validated iterator node should have policy")
            .max_iterations;
        let iteration_index = next_iterator_iteration_index(&state, &node_id);
        if iteration_index >= max_iterations {
            transition_node(
                &mut state,
                &node_id,
                AgentGraphNodeStatus::Running,
                AgentGraphNodeStatus::Failed,
                now,
                |node_state| {
                    node_state.completed_at = Some(now);
                    node_state.error_code = Some("iterator-bound-exceeded".to_string());
                    node_state.wait_reason = None;
                },
            )?;
            state.terminal_status = Some(AgentGraphTerminalStatus::Failed);
            return Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]));
        }

        state.loop_instances.push(
            AgentGraphLoopInstanceState::new(node_id.clone(), iteration_index, now)
                .status(AgentGraphNodeStatus::Running),
        );
        state.scheduler_revision += 1;
        state.blocked_reason = None;

        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
    }

    /// Completes one active iterator iteration.
    pub fn complete_iterator_iteration(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        iteration_index: u32,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        ensure_iterator_node(plan, &node_id)?;
        ensure_node_status(
            &state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Running,
        )?;

        let Some(loop_instance) = state.loop_instances.iter_mut().find(|instance| {
            instance.node_id == node_id && instance.iteration_index == iteration_index
        }) else {
            return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
                node_id,
                reason: format!("iteration `{iteration_index}` does not exist"),
            });
        };
        if loop_instance.status != AgentGraphNodeStatus::Running {
            return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
                node_id,
                reason: format!(
                    "iteration `{iteration_index}` is `{}`",
                    loop_instance.status.as_label()
                ),
            });
        }
        loop_instance.status = AgentGraphNodeStatus::Completed;
        loop_instance.completed_at = Some(now);
        state.scheduler_revision += 1;
        state.blocked_reason = None;

        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
    }

    /// Completes a running iterator node after zero or more iterations.
    pub fn complete_iterator_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        let node = ensure_iterator_node(plan, &node_id)?;
        ensure_node_status(
            &state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Completed,
        )?;
        if current_iterator_iteration_index(&state, &node_id).is_some() {
            return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
                node_id,
                reason: "cannot complete iterator while an iteration is active".to_string(),
            });
        }
        let max_iterations = node
            .iterator_policy
            .expect("validated iterator node should have policy")
            .max_iterations;
        if next_iterator_iteration_index(&state, &node_id) > max_iterations {
            return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
                node_id,
                reason: "recorded iterations exceed iterator bound".to_string(),
            });
        }
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Completed,
            now,
            |node_state| {
                node_state.completed_at = Some(now);
                node_state.wait_reason = None;
                node_state.error_code = None;
            },
        )?;

        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
    }

    /// Cancels a non-terminal graph run and all unresolved graph work.
    pub fn cancel_graph_run(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        validate_plan_state(plan, &state)?;
        if let Some(status) = state.terminal_status {
            if status == AgentGraphTerminalStatus::Cancelled {
                return Ok(AgentGraphSchedulerTransition::new(state, Vec::new()));
            }
            return Err(AgentGraphSchedulerError::TerminalGraph { status });
        }

        let excluded_node_ids = BTreeSet::new();
        let mut changed_node_ids = cancel_unresolved_nodes(&mut state, now, &excluded_node_ids);
        changed_node_ids.extend(cancel_unresolved_loop_instances(&mut state, now));
        changed_node_ids.sort();
        changed_node_ids.dedup();

        state.terminal_status = Some(AgentGraphTerminalStatus::Cancelled);
        state.blocked_reason = None;
        state.scheduler_revision += 1;

        Ok(AgentGraphSchedulerTransition::new(state, changed_node_ids))
    }

    /// Transitions a running node to waiting.
    pub fn wait_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        reason: AgentGraphWaitReason,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        ensure_known_node(plan, &node_id)?;
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Waiting,
            now,
            |node_state| {
                node_state.wait_reason = Some(reason);
            },
        )?;
        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
    }

    /// Transitions a waiting node to completed and records output artifact refs.
    pub fn complete_waiting_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        reason: AgentGraphWaitReason,
        output_refs: BTreeMap<AgentCompiledPortId, ArtifactRef>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        ensure_known_node(plan, &node_id)?;
        ensure_node_wait_reason(&state, &node_id, reason)?;
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Waiting,
            AgentGraphNodeStatus::Completed,
            now,
            |node_state| {
                node_state.output_refs.extend(output_refs);
                node_state.completed_at = Some(now);
                node_state.wait_reason = None;
                node_state.error_code = None;
            },
        )?;
        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
    }

    /// Transitions a waiting node to failed and marks the graph failed.
    pub fn fail_waiting_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        reason: AgentGraphWaitReason,
        error_code: impl Into<String>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        ensure_known_node(plan, &node_id)?;
        ensure_node_wait_reason(&state, &node_id, reason)?;
        let error_code = error_code.into();
        let mut changed_node_ids = vec![node_id.clone()];
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Waiting,
            AgentGraphNodeStatus::Failed,
            now,
            |node_state| {
                node_state.completed_at = Some(now);
                node_state.error_code = Some(error_code);
                node_state.wait_reason = None;
            },
        )?;
        state.terminal_status = Some(AgentGraphTerminalStatus::Failed);
        let mut excluded_node_ids = BTreeSet::new();
        excluded_node_ids.insert(node_id);
        changed_node_ids.extend(cancel_unresolved_nodes(&mut state, now, &excluded_node_ids));
        changed_node_ids.extend(cancel_unresolved_loop_instances(&mut state, now));
        changed_node_ids.sort();
        changed_node_ids.dedup();
        Ok(AgentGraphSchedulerTransition::new(state, changed_node_ids))
    }

    /// Transitions a running node to failed and marks the graph failed.
    pub fn fail_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        mut state: AgentGraphRunState,
        node_id: impl Into<AgentCompiledNodeId>,
        error_code: impl Into<String>,
        now: AgentTimestampMillis,
    ) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition> {
        ensure_not_terminal(&state)?;
        let node_id = node_id.into();
        validate_plan_state(plan, &state)?;
        ensure_known_node(plan, &node_id)?;
        let error_code = error_code.into();
        let mut changed_node_ids = vec![node_id.clone()];
        transition_node(
            &mut state,
            &node_id,
            AgentGraphNodeStatus::Running,
            AgentGraphNodeStatus::Failed,
            now,
            |node_state| {
                node_state.completed_at = Some(now);
                node_state.error_code = Some(error_code);
                node_state.wait_reason = None;
            },
        )?;
        state.terminal_status = Some(AgentGraphTerminalStatus::Failed);
        let mut excluded_node_ids = BTreeSet::new();
        excluded_node_ids.insert(node_id);
        changed_node_ids.extend(cancel_unresolved_nodes(&mut state, now, &excluded_node_ids));
        changed_node_ids.extend(cancel_unresolved_loop_instances(&mut state, now));
        changed_node_ids.sort();
        changed_node_ids.dedup();
        Ok(AgentGraphSchedulerTransition::new(state, changed_node_ids))
    }
}

fn validate_plan(plan: &AgentCompiledExecutionPlan) -> AgentGraphSchedulerResult<()> {
    validate_compiled_execution_plan(plan).map_err(|error| {
        AgentGraphSchedulerError::InvalidCompiledPlan {
            validation_code: error.code(),
            reason: error.to_string(),
        }
    })
}

fn validate_plan_state(
    plan: &AgentCompiledExecutionPlan,
    state: &AgentGraphRunState,
) -> AgentGraphSchedulerResult<()> {
    validate_plan(plan)?;
    if state.plan_id != plan.plan_id {
        return Err(AgentGraphSchedulerError::PlanStateMismatch {
            field: "plan_id",
            reason: format!("state has {}, plan has {}", state.plan_id, plan.plan_id),
        });
    }
    if state.plan_fingerprint != plan.plan_fingerprint {
        return Err(AgentGraphSchedulerError::PlanStateMismatch {
            field: "plan_fingerprint",
            reason: format!(
                "state has {}, plan has {}",
                state.plan_fingerprint, plan.plan_fingerprint
            ),
        });
    }
    let nodes = nodes_by_id(plan);
    for node_id in nodes.keys() {
        if !state.node_states.contains_key(node_id) {
            return Err(AgentGraphSchedulerError::MissingNodeState {
                node_id: node_id.clone(),
            });
        }
    }
    for node_id in state.node_states.keys() {
        if !nodes.contains_key(node_id) {
            return Err(AgentGraphSchedulerError::UnknownNode {
                node_id: node_id.clone(),
            });
        }
    }
    Ok(())
}

fn ensure_not_terminal(state: &AgentGraphRunState) -> AgentGraphSchedulerResult<()> {
    if let Some(status) = state.terminal_status {
        return Err(AgentGraphSchedulerError::TerminalGraph { status });
    }
    Ok(())
}

fn ensure_known_node<'a>(
    plan: &'a AgentCompiledExecutionPlan,
    node_id: &AgentCompiledNodeId,
) -> AgentGraphSchedulerResult<&'a AgentCompiledPlanNode> {
    plan.nodes
        .iter()
        .find(|node| node.node_id == *node_id)
        .ok_or_else(|| AgentGraphSchedulerError::UnknownNode {
            node_id: node_id.clone(),
        })
}

fn ensure_iterator_node<'a>(
    plan: &'a AgentCompiledExecutionPlan,
    node_id: &AgentCompiledNodeId,
) -> AgentGraphSchedulerResult<&'a AgentCompiledPlanNode> {
    let node = ensure_known_node(plan, node_id)?;
    if node.kind != AgentCompiledNodeKind::Iterator {
        return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
            node_id: node_id.clone(),
            reason: "node is not an iterator".to_string(),
        });
    }
    if node.iterator_policy.is_none() {
        return Err(AgentGraphSchedulerError::InvalidIteratorTransition {
            node_id: node_id.clone(),
            reason: "iterator node is missing iterator policy".to_string(),
        });
    }
    Ok(node)
}

fn sorted_nodes(plan: &AgentCompiledExecutionPlan) -> Vec<&AgentCompiledPlanNode> {
    let mut nodes: Vec<_> = plan.nodes.iter().collect();
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    nodes
}

fn nodes_by_id(
    plan: &AgentCompiledExecutionPlan,
) -> BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode> {
    plan.nodes
        .iter()
        .map(|node| (node.node_id.clone(), node))
        .collect()
}

fn incoming_edges_by_target(
    plan: &AgentCompiledExecutionPlan,
) -> BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>> {
    let mut incoming = BTreeMap::<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>::new();
    for edge in &plan.edges {
        incoming
            .entry(edge.target_node_id.clone())
            .or_default()
            .push(edge);
    }
    for edges in incoming.values_mut() {
        edges.sort_by(|left, right| left.edge_id.cmp(&right.edge_id));
    }
    incoming
}

fn outgoing_edges_by_source(
    plan: &AgentCompiledExecutionPlan,
) -> BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>> {
    let mut outgoing = BTreeMap::<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>::new();
    for edge in &plan.edges {
        outgoing
            .entry(edge.source_node_id.clone())
            .or_default()
            .push(edge);
    }
    for edges in outgoing.values_mut() {
        edges.sort_by(|left, right| left.edge_id.cmp(&right.edge_id));
    }
    outgoing
}

fn dependencies_satisfied(
    plan: &AgentCompiledExecutionPlan,
    state: &AgentGraphRunState,
    node: &AgentCompiledPlanNode,
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
    incoming: &BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>,
) -> AgentGraphSchedulerResult<bool> {
    if plan.entry_node_ids.contains(&node.node_id) {
        return Ok(true);
    }
    let required_edges = required_incoming_edges(node, incoming);
    if node.kind == AgentCompiledNodeKind::Join {
        return join_dependencies_satisfied(state, &required_edges, nodes);
    }
    if required_edges.is_empty() {
        return optional_dependencies_satisfied(state, node, nodes, incoming);
    }
    for edge in required_edges {
        let Some(source_node) = nodes.get(&edge.source_node_id) else {
            return Err(AgentGraphSchedulerError::UnknownNode {
                node_id: edge.source_node_id.clone(),
            });
        };
        let Some(source_state) = state.node_states.get(&edge.source_node_id) else {
            return Err(AgentGraphSchedulerError::MissingNodeState {
                node_id: edge.source_node_id.clone(),
            });
        };
        if !source_satisfies_dependency(state, source_node, source_state, edge) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn required_incoming_edges<'a>(
    node: &AgentCompiledPlanNode,
    incoming: &'a BTreeMap<AgentCompiledNodeId, Vec<&'a AgentCompiledPlanEdge>>,
) -> Vec<&'a AgentCompiledPlanEdge> {
    incoming
        .get(&node.node_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|edge| target_port_required(node, edge))
        .collect()
}

fn target_port_required(node: &AgentCompiledPlanNode, edge: &AgentCompiledPlanEdge) -> bool {
    node.input_ports
        .iter()
        .find(|port| port.port_id == edge.target_port_id)
        .map_or(true, |port| port.required)
}

fn join_dependencies_satisfied(
    state: &AgentGraphRunState,
    required_edges: &[&AgentCompiledPlanEdge],
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
) -> AgentGraphSchedulerResult<bool> {
    let wait_for_any = required_edges
        .iter()
        .any(|edge| edge.merge_behavior == Some(AgentCompiledEdgeMergeBehavior::WaitForAny));
    if wait_for_any {
        return required_edges.iter().try_fold(false, |ready, edge| {
            Ok(ready || edge_source_completed(state, edge, nodes)?)
        });
    }

    let mut has_completed_source = false;
    for edge in required_edges {
        if edge_source_completed(state, edge, nodes)? {
            has_completed_source = true;
            continue;
        }
        if !edge_permanently_unsatisfied(state, edge, nodes)? {
            return Ok(false);
        }
    }
    Ok(has_completed_source)
}

fn edge_source_completed(
    state: &AgentGraphRunState,
    edge: &AgentCompiledPlanEdge,
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
) -> AgentGraphSchedulerResult<bool> {
    let Some(source_node) = nodes.get(&edge.source_node_id) else {
        return Err(AgentGraphSchedulerError::UnknownNode {
            node_id: edge.source_node_id.clone(),
        });
    };
    let Some(source_state) = state.node_states.get(&edge.source_node_id) else {
        return Err(AgentGraphSchedulerError::MissingNodeState {
            node_id: edge.source_node_id.clone(),
        });
    };
    Ok(source_satisfies_dependency(
        state,
        source_node,
        source_state,
        edge,
    ))
}

fn source_satisfies_dependency(
    state: &AgentGraphRunState,
    source_node: &AgentCompiledPlanNode,
    source_state: &AgentGraphNodeState,
    edge: &AgentCompiledPlanEdge,
) -> bool {
    if !edge_is_active(state, source_node, edge) {
        return false;
    }
    matches!(
        source_state.status,
        AgentGraphNodeStatus::Completed | AgentGraphNodeStatus::Terminal
    ) || (source_node.kind == AgentCompiledNodeKind::Terminal
        && source_state.status == AgentGraphNodeStatus::Completed)
}

fn edge_is_active(
    state: &AgentGraphRunState,
    source_node: &AgentCompiledPlanNode,
    edge: &AgentCompiledPlanEdge,
) -> bool {
    if source_node.kind != AgentCompiledNodeKind::Branch {
        return true;
    }
    state
        .selected_branch_paths
        .get(&source_node.node_id)
        .is_some_and(|selected| selected.contains(&edge.edge_id))
}

fn edge_permanently_unsatisfied(
    state: &AgentGraphRunState,
    edge: &AgentCompiledPlanEdge,
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
) -> AgentGraphSchedulerResult<bool> {
    let Some(source_node) = nodes.get(&edge.source_node_id) else {
        return Err(AgentGraphSchedulerError::UnknownNode {
            node_id: edge.source_node_id.clone(),
        });
    };
    let Some(source_state) = state.node_states.get(&edge.source_node_id) else {
        return Err(AgentGraphSchedulerError::MissingNodeState {
            node_id: edge.source_node_id.clone(),
        });
    };
    if matches!(
        source_state.status,
        AgentGraphNodeStatus::Skipped | AgentGraphNodeStatus::Cancelled
    ) {
        return Ok(true);
    }
    if source_node.kind == AgentCompiledNodeKind::Branch {
        return Ok(state
            .selected_branch_paths
            .get(&source_node.node_id)
            .map_or(
                matches!(
                    source_state.status,
                    AgentGraphNodeStatus::Completed | AgentGraphNodeStatus::Terminal
                ),
                |selected| !selected.contains(&edge.edge_id),
            ));
    }
    Ok(false)
}

fn transition_node(
    state: &mut AgentGraphRunState,
    node_id: &AgentCompiledNodeId,
    expected: AgentGraphNodeStatus,
    next: AgentGraphNodeStatus,
    now: AgentTimestampMillis,
    apply: impl FnOnce(&mut AgentGraphNodeState),
) -> AgentGraphSchedulerResult<()> {
    let node_state = state.node_states.get_mut(node_id).ok_or_else(|| {
        AgentGraphSchedulerError::MissingNodeState {
            node_id: node_id.clone(),
        }
    })?;
    if node_state.status != expected {
        return Err(AgentGraphSchedulerError::InvalidNodeTransition {
            node_id: node_id.clone(),
            from: node_state.status,
            to: next,
        });
    }
    node_state.status = next;
    node_state.updated_at = now;
    apply(node_state);
    state.scheduler_revision += 1;
    state.blocked_reason = None;
    Ok(())
}

fn ensure_node_status(
    state: &AgentGraphRunState,
    node_id: &AgentCompiledNodeId,
    expected: AgentGraphNodeStatus,
    next: AgentGraphNodeStatus,
) -> AgentGraphSchedulerResult<()> {
    let node_state = state.node_states.get(node_id).ok_or_else(|| {
        AgentGraphSchedulerError::MissingNodeState {
            node_id: node_id.clone(),
        }
    })?;
    if node_state.status != expected {
        return Err(AgentGraphSchedulerError::InvalidNodeTransition {
            node_id: node_id.clone(),
            from: node_state.status,
            to: next,
        });
    }
    Ok(())
}

fn ensure_node_wait_reason(
    state: &AgentGraphRunState,
    node_id: &AgentCompiledNodeId,
    expected: AgentGraphWaitReason,
) -> AgentGraphSchedulerResult<()> {
    let node_state = state.node_states.get(node_id).ok_or_else(|| {
        AgentGraphSchedulerError::MissingNodeState {
            node_id: node_id.clone(),
        }
    })?;
    if node_state.status != AgentGraphNodeStatus::Waiting {
        return Err(AgentGraphSchedulerError::InvalidNodeTransition {
            node_id: node_id.clone(),
            from: node_state.status,
            to: AgentGraphNodeStatus::Completed,
        });
    }
    if node_state.wait_reason != Some(expected) {
        return Err(AgentGraphSchedulerError::InvalidNodeTransition {
            node_id: node_id.clone(),
            from: node_state.status,
            to: AgentGraphNodeStatus::Completed,
        });
    }
    Ok(())
}

fn current_iterator_iteration_index(
    state: &AgentGraphRunState,
    node_id: &AgentCompiledNodeId,
) -> Option<u32> {
    state
        .loop_instances
        .iter()
        .filter(|instance| {
            instance.node_id == *node_id
                && matches!(
                    instance.status,
                    AgentGraphNodeStatus::Pending | AgentGraphNodeStatus::Running
                )
        })
        .map(|instance| instance.iteration_index)
        .min()
}

fn next_iterator_iteration_index(state: &AgentGraphRunState, node_id: &AgentCompiledNodeId) -> u32 {
    state
        .loop_instances
        .iter()
        .filter(|instance| instance.node_id == *node_id)
        .map(|instance| instance.iteration_index)
        .max()
        .map_or(0, |iteration_index| iteration_index.saturating_add(1))
}

fn cancel_unresolved_nodes(
    state: &mut AgentGraphRunState,
    now: AgentTimestampMillis,
    excluded_node_ids: &BTreeSet<AgentCompiledNodeId>,
) -> Vec<AgentCompiledNodeId> {
    let mut changed_node_ids = Vec::new();
    for (node_id, node_state) in &mut state.node_states {
        if excluded_node_ids.contains(node_id) || !node_status_is_unresolved(node_state.status) {
            continue;
        }
        node_state.status = AgentGraphNodeStatus::Cancelled;
        node_state.dependencies_ready = false;
        node_state.updated_at = now;
        node_state.completed_at = Some(now);
        node_state.wait_reason = None;
        node_state.error_code = None;
        changed_node_ids.push(node_id.clone());
    }
    changed_node_ids
}

fn cancel_unresolved_loop_instances(
    state: &mut AgentGraphRunState,
    now: AgentTimestampMillis,
) -> Vec<AgentCompiledNodeId> {
    let mut changed_node_ids = Vec::new();
    for loop_instance in &mut state.loop_instances {
        if !node_status_is_unresolved(loop_instance.status) {
            continue;
        }
        loop_instance.status = AgentGraphNodeStatus::Cancelled;
        loop_instance.completed_at = Some(now);
        changed_node_ids.push(loop_instance.node_id.clone());
    }
    changed_node_ids
}

fn node_status_is_unresolved(status: AgentGraphNodeStatus) -> bool {
    matches!(
        status,
        AgentGraphNodeStatus::Pending
            | AgentGraphNodeStatus::Runnable
            | AgentGraphNodeStatus::Running
            | AgentGraphNodeStatus::Waiting
    )
}

fn validate_branch_selection<I, E>(
    plan: &AgentCompiledExecutionPlan,
    branch_node_id: &AgentCompiledNodeId,
    selected_edge_ids: I,
) -> AgentGraphSchedulerResult<Vec<AgentCompiledEdgeId>>
where
    I: IntoIterator<Item = E>,
    E: Into<AgentCompiledEdgeId>,
{
    let mut selected_edge_ids: Vec<_> = selected_edge_ids.into_iter().map(Into::into).collect();
    selected_edge_ids.sort();
    selected_edge_ids.dedup();
    if selected_edge_ids.is_empty() {
        return Err(AgentGraphSchedulerError::InvalidBranchSelection {
            node_id: branch_node_id.clone(),
            reason: "at least one outgoing edge must be selected".to_string(),
        });
    }

    let outgoing = outgoing_edges_by_source(plan);
    let valid_edge_ids: BTreeSet<_> = outgoing
        .get(branch_node_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|edge| edge.edge_id.clone())
        .collect();
    for edge_id in &selected_edge_ids {
        if !valid_edge_ids.contains(edge_id) {
            return Err(AgentGraphSchedulerError::InvalidBranchSelection {
                node_id: branch_node_id.clone(),
                reason: format!("edge `{edge_id}` is not an outgoing branch edge"),
            });
        }
    }
    Ok(selected_edge_ids)
}

fn propagate_skips(
    plan: &AgentCompiledExecutionPlan,
    state: &mut AgentGraphRunState,
    now: AgentTimestampMillis,
) -> AgentGraphSchedulerResult<Vec<AgentCompiledNodeId>> {
    let incoming = incoming_edges_by_target(plan);
    let nodes = nodes_by_id(plan);
    let mut changed_node_ids = Vec::new();

    loop {
        let mut skipped_this_pass = Vec::new();
        for node in sorted_nodes(plan) {
            let Some(node_state) = state.node_states.get(&node.node_id) else {
                return Err(AgentGraphSchedulerError::MissingNodeState {
                    node_id: node.node_id.clone(),
                });
            };
            if node_state.status != AgentGraphNodeStatus::Pending
                || plan.entry_node_ids.contains(&node.node_id)
            {
                continue;
            }
            if node_should_skip(state, node, &nodes, &incoming)? {
                skipped_this_pass.push(node.node_id.clone());
            }
        }

        if skipped_this_pass.is_empty() {
            break;
        }

        for node_id in skipped_this_pass {
            let Some(node_state) = state.node_states.get_mut(&node_id) else {
                return Err(AgentGraphSchedulerError::MissingNodeState {
                    node_id: node_id.clone(),
                });
            };
            if node_state.status != AgentGraphNodeStatus::Pending {
                continue;
            }
            node_state.status = AgentGraphNodeStatus::Skipped;
            node_state.dependencies_ready = false;
            node_state.updated_at = now;
            node_state.completed_at = Some(now);
            node_state.wait_reason = None;
            node_state.error_code = None;
            changed_node_ids.push(node_id);
        }
    }

    changed_node_ids.sort();
    changed_node_ids.dedup();
    Ok(changed_node_ids)
}

fn node_should_skip(
    state: &AgentGraphRunState,
    node: &AgentCompiledPlanNode,
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
    incoming: &BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>,
) -> AgentGraphSchedulerResult<bool> {
    let required_edges = required_incoming_edges(node, incoming);
    if node.kind == AgentCompiledNodeKind::Join {
        if required_edges.is_empty() {
            return Ok(false);
        }
        if required_edges.iter().try_fold(false, |completed, edge| {
            Ok(completed || edge_source_completed(state, edge, nodes)?)
        })? {
            return Ok(false);
        }
        return required_edges.iter().try_fold(true, |all_blocked, edge| {
            Ok(all_blocked && edge_permanently_unsatisfied(state, edge, nodes)?)
        });
    }
    if required_edges.is_empty() {
        return optional_dependencies_blocked(state, node, nodes, incoming);
    }
    required_edges.iter().try_fold(false, |should_skip, edge| {
        Ok(should_skip || edge_permanently_unsatisfied(state, edge, nodes)?)
    })
}

/// Evaluates readiness for a non-entry, non-join node that has no *required*
/// incoming edges (its incoming edges, if any, all target optional ports).
///
/// Such a node becomes ready once every incoming edge has settled — each source
/// either completed or is permanently unsatisfied — and at least one incoming
/// edge produced a usable result. A node with no incoming edges at all (an
/// unreachable non-entry node) is never ready; [`node_should_skip`] skips it
/// instead. This keeps an optional-only-input node (including a `Terminal` node
/// reached through an optional port) from stalling forever in `Pending`.
fn optional_dependencies_satisfied(
    state: &AgentGraphRunState,
    node: &AgentCompiledPlanNode,
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
    incoming: &BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>,
) -> AgentGraphSchedulerResult<bool> {
    let edges = incoming
        .get(&node.node_id)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut any_satisfied = false;
    for edge in edges {
        if edge_source_completed(state, edge, nodes)? {
            any_satisfied = true;
        } else if !edge_permanently_unsatisfied(state, edge, nodes)? {
            return Ok(false);
        }
    }
    Ok(any_satisfied)
}

/// Decides whether a non-entry, non-join node with no *required* incoming edges
/// should be skipped: it is skipped when every incoming edge is permanently
/// unsatisfied (vacuously true when the node has no incoming edges at all, i.e.
/// it is unreachable). This is the skip counterpart of
/// [`optional_dependencies_satisfied`]; together they guarantee such a node
/// always resolves to runnable or skipped rather than stalling in `Pending`.
fn optional_dependencies_blocked(
    state: &AgentGraphRunState,
    node: &AgentCompiledPlanNode,
    nodes: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
    incoming: &BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>,
) -> AgentGraphSchedulerResult<bool> {
    let edges = incoming
        .get(&node.node_id)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    for edge in edges {
        if !edge_permanently_unsatisfied(state, edge, nodes)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn runnable_nodes_from_state(state: &AgentGraphRunState) -> Vec<AgentCompiledNodeId> {
    if state.terminal_status.is_some() {
        return Vec::new();
    }
    state
        .node_states
        .iter()
        .filter_map(|(node_id, node)| {
            (node.status == AgentGraphNodeStatus::Runnable).then_some(node_id.clone())
        })
        .collect()
}
