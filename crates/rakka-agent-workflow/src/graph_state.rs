//! Durable graph execution state contracts.
//!
//! These types are persisted with graph-backed agent runs so a scheduler can
//! recover deterministic execution state after passivation, restart, or
//! dispatcher redelivery. Large values are represented by `ArtifactRef`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::compiled_plan::{
    AgentCompiledEdgeId, AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPortId,
};
use crate::domain::{
    AgentEffectId, AgentTimerId, AgentTimestampMillis, ArtifactRef, HumanCheckpointId,
};

/// Current durable graph run state schema version.
pub const CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION: AgentGraphStateSchemaVersion =
    AgentGraphStateSchemaVersion::new(1);

/// Serialized durable graph state schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentGraphStateSchemaVersion(u32);

impl AgentGraphStateSchemaVersion {
    /// Creates a graph state schema version.
    #[must_use]
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// Returns the version number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Durable execution state for one compiled graph run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphRunState {
    /// Compiled plan id selected when the run started.
    pub plan_id: AgentCompiledPlanId,
    /// Immutable compiled plan fingerprint selected when the run started.
    pub plan_fingerprint: AgentCompiledPlanFingerprint,
    /// Serialized graph state schema version.
    pub graph_schema_version: AgentGraphStateSchemaVersion,
    /// Per-node durable state keyed by compiled node id.
    #[serde(default)]
    pub node_states: BTreeMap<AgentCompiledNodeId, AgentGraphNodeState>,
    /// Branch node selections keyed by branch node id.
    #[serde(default)]
    pub selected_branch_paths: BTreeMap<AgentCompiledNodeId, Vec<AgentCompiledEdgeId>>,
    /// Explicit bounded loop or iterator instances.
    #[serde(default)]
    pub loop_instances: Vec<AgentGraphLoopInstanceState>,
    /// Current scheduler blocked reason, when no node can be made runnable.
    #[serde(default)]
    pub blocked_reason: Option<AgentGraphBlockedReason>,
    /// Graph-level output artifact refs keyed by output port id.
    #[serde(default)]
    pub output_refs: BTreeMap<AgentCompiledPortId, ArtifactRef>,
    /// Monotonic scheduler revision for recovery-safe decisions.
    #[serde(default)]
    pub scheduler_revision: u64,
    /// Last emitted durable runtime event sequence observed by this graph state.
    #[serde(default)]
    pub last_event_sequence: u64,
    /// Terminal graph status after all nodes resolve or cancellation/failure wins.
    #[serde(default)]
    pub terminal_status: Option<AgentGraphTerminalStatus>,
}

impl AgentGraphRunState {
    /// Creates empty graph run state for a compiled plan.
    #[must_use]
    pub fn new(
        plan_id: AgentCompiledPlanId,
        plan_fingerprint: AgentCompiledPlanFingerprint,
    ) -> Self {
        Self {
            plan_id,
            plan_fingerprint,
            graph_schema_version: CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION,
            node_states: BTreeMap::new(),
            selected_branch_paths: BTreeMap::new(),
            loop_instances: Vec::new(),
            blocked_reason: None,
            output_refs: BTreeMap::new(),
            scheduler_revision: 0,
            last_event_sequence: 0,
            terminal_status: None,
        }
    }

    /// Inserts or replaces one node state.
    #[must_use]
    pub fn node_state(mut self, node_state: AgentGraphNodeState) -> Self {
        self.node_states
            .insert(node_state.node_id.clone(), node_state);
        self
    }

    /// Records selected outgoing edges for a branch node.
    #[must_use]
    pub fn selected_branch_path(
        mut self,
        node_id: AgentCompiledNodeId,
        edge_ids: Vec<AgentCompiledEdgeId>,
    ) -> Self {
        self.selected_branch_paths.insert(node_id, edge_ids);
        self
    }

    /// Adds one bounded loop or iterator instance.
    #[must_use]
    pub fn loop_instance(mut self, loop_instance: AgentGraphLoopInstanceState) -> Self {
        self.loop_instances.push(loop_instance);
        self
    }

    /// Records a graph output artifact ref.
    #[must_use]
    pub fn output_ref(mut self, port_id: AgentCompiledPortId, artifact_ref: ArtifactRef) -> Self {
        self.output_refs.insert(port_id, artifact_ref);
        self
    }

    /// Sets the graph blocked reason.
    #[must_use]
    pub fn blocked_reason(mut self, blocked_reason: AgentGraphBlockedReason) -> Self {
        self.blocked_reason = Some(blocked_reason);
        self
    }

    /// Sets the terminal graph status.
    #[must_use]
    pub const fn terminal_status(mut self, terminal_status: AgentGraphTerminalStatus) -> Self {
        self.terminal_status = Some(terminal_status);
        self
    }
}

/// Query/snapshot projection for one durable graph run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphRunProjection {
    /// Compiled plan id selected when the run started.
    pub plan_id: AgentCompiledPlanId,
    /// Immutable compiled plan fingerprint selected when the run started.
    pub plan_fingerprint: AgentCompiledPlanFingerprint,
    /// Serialized graph state schema version.
    pub graph_schema_version: AgentGraphStateSchemaVersion,
    /// Monotonic scheduler revision for recovery-safe decisions.
    pub scheduler_revision: u64,
    /// Last emitted durable runtime event sequence observed by this graph state.
    #[serde(default)]
    pub last_event_sequence: u64,
    /// Terminal graph status after all nodes resolve or cancellation/failure wins.
    #[serde(default)]
    pub terminal_status: Option<AgentGraphTerminalStatus>,
    /// Stable blocked reason code when the graph cannot currently make progress.
    #[serde(default)]
    pub blocked_reason_code: Option<String>,
    /// Total node count.
    pub node_count: usize,
    /// Runnable node count.
    pub runnable_node_count: usize,
    /// Running node count.
    pub running_node_count: usize,
    /// Waiting node count.
    pub waiting_node_count: usize,
    /// Completed node count.
    pub completed_node_count: usize,
    /// Skipped node count.
    pub skipped_node_count: usize,
    /// Failed node count.
    pub failed_node_count: usize,
    /// Cancelled node count.
    pub cancelled_node_count: usize,
    /// Terminal node count.
    pub terminal_node_count: usize,
    /// Bounded per-node projections, sorted by compiled node id.
    #[serde(default)]
    pub nodes: Vec<AgentGraphNodeProjection>,
}

impl AgentGraphRunProjection {
    /// Builds a query/snapshot projection from durable graph state.
    #[must_use]
    pub fn from_graph_state(graph: &AgentGraphRunState) -> Self {
        let nodes: Vec<_> = graph
            .node_states
            .values()
            .map(AgentGraphNodeProjection::from_node_state)
            .collect();
        Self {
            plan_id: graph.plan_id.clone(),
            plan_fingerprint: graph.plan_fingerprint.clone(),
            graph_schema_version: graph.graph_schema_version,
            scheduler_revision: graph.scheduler_revision,
            last_event_sequence: graph.last_event_sequence,
            terminal_status: graph.terminal_status,
            blocked_reason_code: graph
                .blocked_reason
                .as_ref()
                .map(|reason| reason.code.clone()),
            node_count: nodes.len(),
            runnable_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Runnable),
            running_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Running),
            waiting_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Waiting),
            completed_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Completed),
            skipped_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Skipped),
            failed_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Failed),
            cancelled_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Cancelled),
            terminal_node_count: count_nodes_by_status(&nodes, AgentGraphNodeStatus::Terminal),
            nodes,
        }
    }
}

/// Query/snapshot projection for one durable graph node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphNodeProjection {
    /// Compiled node id.
    pub node_id: AgentCompiledNodeId,
    /// Product-neutral node kind from the compiled plan.
    pub kind: AgentCompiledNodeKind,
    /// Durable graph-node status.
    pub status: AgentGraphNodeStatus,
    /// Waiting reason when status is waiting.
    #[serde(default)]
    pub wait_reason: Option<AgentGraphWaitReason>,
    /// Stable error code when status is failed.
    #[serde(default)]
    pub error_code: Option<String>,
}

impl AgentGraphNodeProjection {
    /// Builds a node projection from durable node state.
    #[must_use]
    pub fn from_node_state(node: &AgentGraphNodeState) -> Self {
        Self {
            node_id: node.node_id.clone(),
            kind: node.kind,
            status: node.status,
            wait_reason: node.wait_reason,
            error_code: node.error_code.clone(),
        }
    }
}

/// Durable state for one compiled graph node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphNodeState {
    /// Compiled node id.
    pub node_id: AgentCompiledNodeId,
    /// Product-neutral node kind from the compiled plan.
    pub kind: AgentCompiledNodeKind,
    /// Durable graph-node status.
    pub status: AgentGraphNodeStatus,
    /// Attempt count for this node.
    #[serde(default)]
    pub attempt: u32,
    /// Whether required dependencies are satisfied.
    #[serde(default)]
    pub dependencies_ready: bool,
    /// Input artifact refs keyed by input port id.
    #[serde(default)]
    pub input_refs: BTreeMap<AgentCompiledPortId, ArtifactRef>,
    /// Output artifact refs keyed by output port id.
    #[serde(default)]
    pub output_refs: BTreeMap<AgentCompiledPortId, ArtifactRef>,
    /// Durable outbox effect ids scheduled by this node.
    #[serde(default)]
    pub scheduled_effect_ids: Vec<AgentEffectId>,
    /// Durable timer ids associated with this node.
    #[serde(default)]
    pub timer_ids: Vec<AgentTimerId>,
    /// Human checkpoint ids associated with this node.
    #[serde(default)]
    pub checkpoint_ids: Vec<HumanCheckpointId>,
    /// Waiting reason when status is waiting.
    #[serde(default)]
    pub wait_reason: Option<AgentGraphWaitReason>,
    /// Stable error code when status is failed.
    #[serde(default)]
    pub error_code: Option<String>,
    /// Node creation timestamp in the graph state.
    pub created_at: AgentTimestampMillis,
    /// Last node-state update timestamp.
    pub updated_at: AgentTimestampMillis,
    /// Node start timestamp.
    #[serde(default)]
    pub started_at: Option<AgentTimestampMillis>,
    /// Node terminal timestamp.
    #[serde(default)]
    pub completed_at: Option<AgentTimestampMillis>,
}

impl AgentGraphNodeState {
    /// Creates pending node state with empty dependency and output refs.
    #[must_use]
    pub fn new(
        node_id: AgentCompiledNodeId,
        kind: AgentCompiledNodeKind,
        created_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            node_id,
            kind,
            status: AgentGraphNodeStatus::Pending,
            attempt: 0,
            dependencies_ready: false,
            input_refs: BTreeMap::new(),
            output_refs: BTreeMap::new(),
            scheduled_effect_ids: Vec::new(),
            timer_ids: Vec::new(),
            checkpoint_ids: Vec::new(),
            wait_reason: None,
            error_code: None,
            created_at,
            updated_at: created_at,
            started_at: None,
            completed_at: None,
        }
    }

    /// Sets node status.
    #[must_use]
    pub const fn status(mut self, status: AgentGraphNodeStatus) -> Self {
        self.status = status;
        self
    }

    /// Sets dependency readiness.
    #[must_use]
    pub const fn dependencies_ready(mut self, dependencies_ready: bool) -> Self {
        self.dependencies_ready = dependencies_ready;
        self
    }

    /// Records an input artifact ref.
    #[must_use]
    pub fn input_ref(mut self, port_id: AgentCompiledPortId, artifact_ref: ArtifactRef) -> Self {
        self.input_refs.insert(port_id, artifact_ref);
        self
    }

    /// Records an output artifact ref.
    #[must_use]
    pub fn output_ref(mut self, port_id: AgentCompiledPortId, artifact_ref: ArtifactRef) -> Self {
        self.output_refs.insert(port_id, artifact_ref);
        self
    }

    /// Records a scheduled effect id.
    #[must_use]
    pub fn scheduled_effect_id(mut self, effect_id: AgentEffectId) -> Self {
        self.scheduled_effect_ids.push(effect_id);
        self
    }

    /// Records a timer id.
    #[must_use]
    pub fn timer_id(mut self, timer_id: AgentTimerId) -> Self {
        self.timer_ids.push(timer_id);
        self
    }

    /// Records a human checkpoint id.
    #[must_use]
    pub fn checkpoint_id(mut self, checkpoint_id: HumanCheckpointId) -> Self {
        self.checkpoint_ids.push(checkpoint_id);
        self
    }

    /// Sets node wait reason.
    #[must_use]
    pub const fn wait_reason(mut self, wait_reason: AgentGraphWaitReason) -> Self {
        self.wait_reason = Some(wait_reason);
        self
    }

    /// Sets stable node error code.
    #[must_use]
    pub fn error_code(mut self, error_code: impl Into<String>) -> Self {
        self.error_code = Some(error_code.into());
        self
    }
}

/// Durable lifecycle status for a compiled graph node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentGraphNodeStatus {
    /// Node has not yet been evaluated by the scheduler.
    Pending,
    /// Node dependencies are satisfied and it can be scheduled.
    Runnable,
    /// Node is currently running.
    Running,
    /// Node is waiting for a durable external event.
    Waiting,
    /// Node completed successfully.
    Completed,
    /// Node was skipped by deterministic branch or cancellation propagation.
    Skipped,
    /// Node failed.
    Failed,
    /// Node was cancelled.
    Cancelled,
    /// Node reached a terminal boundary.
    Terminal,
}

impl AgentGraphNodeStatus {
    /// Stable lowercase label for telemetry and query projections.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Runnable => "runnable",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Terminal => "terminal",
        }
    }
}

/// Terminal status for a graph run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentGraphTerminalStatus {
    /// Graph run completed successfully.
    Completed,
    /// Graph run failed.
    Failed,
    /// Graph run was cancelled.
    Cancelled,
}

impl AgentGraphTerminalStatus {
    /// Stable lowercase label for telemetry and query projections.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Durable wait reason for a graph node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentGraphWaitReason {
    /// Waiting for upstream dependencies.
    Dependency,
    /// Waiting for a durable outbox effect result.
    Effect,
    /// Waiting for a durable timer.
    Timer,
    /// Waiting for a human checkpoint decision.
    Human,
    /// Waiting for a child workflow.
    ChildWorkflow,
    /// Waiting for an external signal.
    Signal,
}

impl AgentGraphWaitReason {
    /// Stable lowercase label for telemetry and query projections.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Dependency => "dependency",
            Self::Effect => "effect",
            Self::Timer => "timer",
            Self::Human => "human",
            Self::ChildWorkflow => "child-workflow",
            Self::Signal => "signal",
        }
    }
}

/// One explicit bounded loop or iterator instance in a graph run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphLoopInstanceState {
    /// Iterator or loop node id.
    pub node_id: AgentCompiledNodeId,
    /// Zero-based loop iteration index.
    pub iteration_index: u32,
    /// Status of this loop iteration.
    pub status: AgentGraphNodeStatus,
    /// Optional item artifact ref for this iteration.
    #[serde(default)]
    pub item_ref: Option<ArtifactRef>,
    /// Iteration output artifact refs keyed by output port id.
    #[serde(default)]
    pub output_refs: BTreeMap<AgentCompiledPortId, ArtifactRef>,
    /// Iteration creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Iteration terminal timestamp.
    #[serde(default)]
    pub completed_at: Option<AgentTimestampMillis>,
}

impl AgentGraphLoopInstanceState {
    /// Creates a pending loop instance.
    #[must_use]
    pub fn new(
        node_id: AgentCompiledNodeId,
        iteration_index: u32,
        created_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            node_id,
            iteration_index,
            status: AgentGraphNodeStatus::Pending,
            item_ref: None,
            output_refs: BTreeMap::new(),
            created_at,
            completed_at: None,
        }
    }

    /// Sets loop instance status.
    #[must_use]
    pub const fn status(mut self, status: AgentGraphNodeStatus) -> Self {
        self.status = status;
        self
    }

    /// Records the item artifact ref for this iteration.
    #[must_use]
    pub fn item_ref(mut self, item_ref: ArtifactRef) -> Self {
        self.item_ref = Some(item_ref);
        self
    }

    /// Records one iteration output artifact ref.
    #[must_use]
    pub fn output_ref(mut self, port_id: AgentCompiledPortId, artifact_ref: ArtifactRef) -> Self {
        self.output_refs.insert(port_id, artifact_ref);
        self
    }
}

/// Current reason the graph scheduler cannot make progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphBlockedReason {
    /// Stable bounded reason code.
    pub code: String,
    /// Optional bounded detail label, not full error text.
    #[serde(default)]
    pub detail: Option<String>,
}

impl AgentGraphBlockedReason {
    /// Creates a blocked reason from a stable code.
    #[must_use]
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: None,
        }
    }

    /// Sets bounded detail.
    #[must_use]
    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

fn count_nodes_by_status(
    nodes: &[AgentGraphNodeProjection],
    status: AgentGraphNodeStatus,
) -> usize {
    nodes.iter().filter(|node| node.status == status).count()
}
