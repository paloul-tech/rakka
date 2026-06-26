//! HTTP request and response data model.

use rakka::agent_workflow::AgentCompiledExecutionPlan;
use serde::{Deserialize, Serialize};

/// A compiled workflow definition submitted over HTTP.
///
/// The graph is product-neutral: a set of nodes and the directed edges between
/// them. The server compiles it into a Rakka `AgentCompiledExecutionPlan`,
/// validates it, and executes it on whichever cluster node owns the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitWorkflowRequest {
    /// Stable run id. When omitted the server generates one.
    ///
    /// The run id is the sharded entity id, so it decides which node owns and
    /// executes the run.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Optional plan id for diagnostics. Defaults to a generated id.
    #[serde(default)]
    pub plan_id: Option<String>,
    /// Graph nodes.
    pub nodes: Vec<NodeSpec>,
    /// Directed edges between node ids.
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    /// Entry nodes the scheduler may start first.
    ///
    /// Defaults to every node with no incoming edge.
    #[serde(default)]
    pub entry_nodes: Option<Vec<String>>,
}

/// One node in the submitted graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    /// Stable node id, unique within the graph.
    pub id: String,
    /// Optional node kind label (`input`, `transform`, `terminal`).
    ///
    /// When omitted, the kind is inferred from the node's position: a node with
    /// no incoming edges becomes `input`, a node with no outgoing edges becomes
    /// `terminal`, and any other node becomes `transform`.
    #[serde(default)]
    pub kind: Option<String>,
}

/// One directed edge in the submitted graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeSpec {
    /// Source node id.
    pub from: String,
    /// Target node id.
    pub to: String,
}

/// JSON view of a workflow run returned to clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRunView {
    /// Stable run id.
    pub run_id: String,
    /// Cluster node that owns and executed the run.
    pub owner_node: String,
    /// Whether the run executed on the node that received the HTTP request.
    pub executed_locally: bool,
    /// Logical id of the node that produced this view.
    pub served_by: String,
    /// Terminal/active run status label.
    pub status: String,
    /// Compiled plan id selected for the run.
    pub plan_id: String,
    /// Deterministic compiled plan fingerprint.
    pub plan_fingerprint: String,
    /// Total node count in the compiled plan.
    pub node_count: usize,
    /// Completed node count.
    pub completed_node_count: usize,
    /// Terminal node count.
    pub terminal_node_count: usize,
    /// Per-node execution summary.
    pub nodes: Vec<NodeView>,
    /// Optional diagnostic message (e.g. for `not-found` or `error` statuses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Per-node summary in a run view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeView {
    /// Compiled node id.
    pub node_id: String,
    /// Product-neutral node kind label.
    pub kind: String,
    /// Durable graph-node status label.
    pub status: String,
}

/// Inter-node ask payload routed over `rakka-remote` to a run's owning node.
///
/// This is the serializable query a non-owning node sends to the owner. The
/// compiled plan travels by value in one round trip; the owner drives the run
/// locally and replies with a [`WorkflowRunView`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunRequest {
    /// Compile-and-run, or resume, the run with the given plan.
    Drive {
        /// Compiled execution plan to run (boxed to keep the enum small).
        plan: Box<AgentCompiledExecutionPlan>,
    },
    /// Return the current run view without changing it.
    Query,
}

/// JSON view of this node's cluster membership and ownership picture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterView {
    /// Logical id of the node serving this request.
    pub this_node: String,
    /// Up members observed by this node, sorted by logical id.
    pub up_nodes: Vec<String>,
    /// Number of up members.
    pub member_count: usize,
    /// Configured shard count used for run ownership.
    pub number_of_shards: u32,
}
