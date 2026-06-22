//! Deterministic compiled graph scheduler core.
//!
//! The scheduler is intentionally side-effect free in this slice. It evaluates
//! a compiled plan against durable graph state and returns updated state for
//! the caller to persist before any node work is executed.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{
    validate_compiled_execution_plan, AgentCompiledExecutionPlan, AgentCompiledNodeId,
    AgentCompiledNodeKind, AgentCompiledPlanEdge, AgentCompiledPlanNode, AgentGraphNodeState,
    AgentGraphNodeStatus, AgentGraphRunState, AgentGraphTerminalStatus, AgentGraphWaitReason,
    AgentTimestampMillis,
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
        let next_status = if node.kind == AgentCompiledNodeKind::Terminal {
            AgentGraphNodeStatus::Terminal
        } else {
            AgentGraphNodeStatus::Completed
        };
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
        }
        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
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
        Ok(AgentGraphSchedulerTransition::new(state, vec![node_id]))
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
    let incoming_edges = incoming.get(&node.node_id).cloned().unwrap_or_default();
    let required_edges: Vec<_> = incoming_edges
        .into_iter()
        .filter(|edge| target_port_required(node, edge))
        .collect();
    if required_edges.is_empty() {
        return Ok(false);
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
        if !source_satisfies_dependency(source_node, source_state) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn target_port_required(node: &AgentCompiledPlanNode, edge: &AgentCompiledPlanEdge) -> bool {
    node.input_ports
        .iter()
        .find(|port| port.port_id == edge.target_port_id)
        .map_or(true, |port| port.required)
}

fn source_satisfies_dependency(
    source_node: &AgentCompiledPlanNode,
    source_state: &AgentGraphNodeState,
) -> bool {
    matches!(
        source_state.status,
        AgentGraphNodeStatus::Completed | AgentGraphNodeStatus::Terminal
    ) || (source_node.kind == AgentCompiledNodeKind::Terminal
        && source_state.status == AgentGraphNodeStatus::Completed)
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

fn runnable_nodes_from_state(state: &AgentGraphRunState) -> Vec<AgentCompiledNodeId> {
    state
        .node_states
        .iter()
        .filter_map(|(node_id, node)| {
            (node.status == AgentGraphNodeStatus::Runnable).then_some(node_id.clone())
        })
        .collect()
}
