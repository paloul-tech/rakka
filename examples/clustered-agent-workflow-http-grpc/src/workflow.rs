//! Demo workflow definition, plan compilation, and the durable graph driver.
//!
//! The example hosts a single registered [`AgentWorkflow`]; every submitted graph
//! is compiled into an [`AgentCompiledExecutionPlan`] bound to that workflow's
//! identity (the run actor validates the plan against its hosted workflow). Each
//! run gets its own plan.
//!
//! Graph execution is externally driven: the per-run `AgentRunActor` exposes
//! small `ask` commands, and [`drive_to_terminal`] turns the compiled DAG into
//! the `StartGraph -> { MarkGraphReady -> StartGraphNode -> CompleteGraphNode }`
//! sequence, persisting durable state at every step. The driver runs on the
//! run's owning node against a local child actor, so this loop is never chatty
//! across the network. Nodes here are treated as deterministic local work, so
//! each is started and completed immediately.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka::agent_workflow::{
    validate_compiled_execution_plan, AgentCausationId, AgentCommand, AgentCommandId,
    AgentCommandKind, AgentCommandMetadata, AgentCompiledExecutionPlan, AgentCompiledNodeId,
    AgentCompiledNodeKind, AgentCompiledPlanEdge, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPortDirection,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata,
    AgentGraphRuntimeTransition, AgentPayloadDescriptor, AgentRunActorCommand,
    AgentRunActorSnapshot, AgentRunId, AgentRunState, AgentRunStatus, AgentStatePayload, AgentStep,
    AgentStepId, AgentStepKind, AgentTenantId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, StateSchemaVersion, WorkflowDefinitionVersion,
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
use rakka::prelude::{ActorRef, AskError};

use crate::model::{NodeView, SubmitWorkflowRequest, WorkflowRunView};
use crate::support::{
    current_timestamp_millis, example_error, stable_hash, ExampleResult, RUN_ASK_TIMEOUT,
    WORKFLOW_TYPE,
};

const WORKFLOW_ID: &str = "workflow-compiled-graph-demo";
const DEFINITION_VERSION: &str = "v1";
const TENANT: &str = "tenant-demo";
const PORT_PAYLOAD_TYPE: &str = "application/json";

static RUN_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Run actor command channel handle.
type RunChild = ActorRef<AgentRunActorCommand>;

/// The single workflow definition this cluster hosts.
///
/// Every submitted compiled plan must reference this workflow's id, type, and
/// version; the run actor rejects mismatched plans.
pub fn demo_workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new(WORKFLOW_ID),
        workflow_type: WORKFLOW_TYPE.to_string(),
        definition_version: WorkflowDefinitionVersion::new(DEFINITION_VERSION),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Compiled graph demo workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![AgentCommandKind::StartRun.type_name().to_string()],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("graph"),
            kind: AgentStepKind::Planner,
            display_name: Some("Compiled graph entry".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(60_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("graph.input").content_type(PORT_PAYLOAD_TYPE)
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            WORKFLOW_TYPE.to_string(),
        )]),
    }
}

/// A compiled plan plus the run id it executes under.
pub struct CompiledSubmission {
    pub run_id: AgentRunId,
    pub plan: AgentCompiledExecutionPlan,
}

/// Compiles a submitted graph into a validated Rakka execution plan.
///
/// Ports are generated automatically so callers only describe nodes and edges:
/// every node with outgoing edges gets one `out` output port, and every incoming
/// edge gets its own input port on the target node. The plan is validated with
/// [`validate_compiled_execution_plan`] before it is returned.
pub fn compile_submission(
    request: &SubmitWorkflowRequest,
    workflow: &AgentWorkflow,
) -> ExampleResult<CompiledSubmission> {
    if request.nodes.is_empty() {
        return Err(example_error("workflow must declare at least one node").into());
    }

    let mut node_ids = BTreeSet::new();
    for node in &request.nodes {
        if node.id.trim().is_empty() {
            return Err(example_error("node id must not be empty").into());
        }
        if !node_ids.insert(node.id.clone()) {
            return Err(example_error(format!("duplicate node id {}", node.id)).into());
        }
    }

    let mut indegree: BTreeMap<String, usize> = BTreeMap::new();
    let mut outdegree: BTreeMap<String, usize> = BTreeMap::new();
    for edge in &request.edges {
        if !node_ids.contains(&edge.from) {
            return Err(
                example_error(format!("edge references unknown node {}", edge.from)).into(),
            );
        }
        if !node_ids.contains(&edge.to) {
            return Err(example_error(format!("edge references unknown node {}", edge.to)).into());
        }
        *outdegree.entry(edge.from.clone()).or_default() += 1;
        *indegree.entry(edge.to.clone()).or_default() += 1;
    }

    // Resolve kinds and per-node ports in one pass over the request.
    let mut plan_nodes: BTreeMap<String, AgentCompiledPlanNode> = BTreeMap::new();
    for node in &request.nodes {
        let in_count = indegree.get(&node.id).copied().unwrap_or(0);
        let out_count = outdegree.get(&node.id).copied().unwrap_or(0);
        let kind = match &node.kind {
            Some(label) => parse_supported_kind(label)?,
            None => infer_kind(in_count, out_count),
        };
        let mut plan_node = AgentCompiledPlanNode::new(node.id.clone(), kind);
        if out_count > 0 {
            plan_node = plan_node.output_port(AgentCompiledPlanPort::new(
                "out",
                AgentCompiledPortDirection::Output,
                PORT_PAYLOAD_TYPE,
            ));
        }
        plan_nodes.insert(node.id.clone(), plan_node);
    }

    let mut plan_edges = Vec::with_capacity(request.edges.len());
    for (index, edge) in request.edges.iter().enumerate() {
        let target_port = format!("in:e{index}");
        let target = plan_nodes
            .get_mut(&edge.to)
            .expect("edge target validated above");
        *target = target.clone().input_port(AgentCompiledPlanPort::new(
            target_port.clone(),
            AgentCompiledPortDirection::Input,
            PORT_PAYLOAD_TYPE,
        ));
        plan_edges.push(AgentCompiledPlanEdge::new(
            format!("e{index}:{}->{}", edge.from, edge.to),
            edge.from.clone(),
            "out",
            edge.to.clone(),
            target_port,
        ));
    }

    let entry_nodes = match &request.entry_nodes {
        Some(entries) if !entries.is_empty() => {
            for entry in entries {
                if !node_ids.contains(entry) {
                    return Err(example_error(format!("entry node {entry} is not declared")).into());
                }
            }
            entries.clone()
        }
        _ => request
            .nodes
            .iter()
            .filter(|node| indegree.get(&node.id).copied().unwrap_or(0) == 0)
            .map(|node| node.id.clone())
            .collect::<Vec<_>>(),
    };
    if entry_nodes.is_empty() {
        return Err(
            example_error("workflow has no entry node (every node has an incoming edge)").into(),
        );
    }

    let run_id = AgentRunId::new(match &request.run_id {
        Some(value) if !value.trim().is_empty() => value.clone(),
        _ => generate_run_id(),
    });
    let plan_id = AgentCompiledPlanId::new(match &request.plan_id {
        Some(value) if !value.trim().is_empty() => value.clone(),
        _ => format!("plan-{}", run_id.as_str()),
    });

    let mut plan = AgentCompiledExecutionPlan::new(
        plan_id,
        workflow.workflow_id.clone(),
        workflow.workflow_type.clone(),
        workflow.definition_version.clone(),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        fingerprint(&request.nodes, &plan_edges, &entry_nodes),
    );
    for entry in entry_nodes {
        plan = plan.entry_node(entry);
    }
    for plan_node in plan_nodes.into_values() {
        plan = plan.node(plan_node);
    }
    for edge in plan_edges {
        plan = plan.edge(edge);
    }

    validate_compiled_execution_plan(&plan)
        .map_err(|error| example_error(format!("invalid compiled plan: {error:?}")))?;

    Ok(CompiledSubmission { run_id, plan })
}

/// Drives a compiled run to a terminal status against a local run actor.
///
/// The actor persists durable state at every transition, so this is resumable:
/// if a run already exists it continues from its recovered graph state instead
/// of starting again.
pub async fn drive_to_terminal(
    child: &RunChild,
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    plan: Arc<AgentCompiledExecutionPlan>,
) -> ExampleResult<AgentRunActorSnapshot> {
    let mut clock = NowClock::new();

    if snapshot(child).await?.run_state.is_none() {
        // Durable inbox acceptance is the acknowledgement boundary; it is
        // idempotent, so a duplicate submission resolves to the same run.
        accept_start_command(child, workflow, run_id).await?;
        start_graph(
            child,
            initial_state(workflow, run_id, clock.peek()),
            plan.clone(),
            clock.next(),
        )
        .await?;
    }

    let max_rounds = plan.nodes.len() * 4 + 16;
    for _ in 0..max_rounds {
        if let Some(state) = snapshot(child).await?.run_state {
            if is_terminal(state.status) {
                break;
            }
        }
        let ready = mark_graph_ready(child, plan.clone(), clock.next()).await?;
        let runnable = ready.graph_transition.runnable_node_ids;
        if runnable.is_empty() {
            break;
        }
        for node_id in runnable {
            start_graph_node(child, plan.clone(), node_id.clone(), clock.next()).await?;
            complete_graph_node(child, plan.clone(), node_id, clock.next()).await?;
        }
    }

    snapshot(child).await
}

/// Returns the current durable snapshot for a local run actor.
pub async fn fetch_snapshot(child: &RunChild) -> ExampleResult<AgentRunActorSnapshot> {
    snapshot(child).await
}

/// Builds the JSON run view returned to clients from a recovered snapshot.
pub fn run_view(
    snapshot: &AgentRunActorSnapshot,
    owner_node: String,
    executed_locally: bool,
    served_by: String,
) -> WorkflowRunView {
    let status = snapshot.run_state.as_ref().map_or_else(
        || "unknown".to_string(),
        |state| state.status.as_label().to_string(),
    );
    let (plan_id, plan_fingerprint, node_count, completed, terminal, nodes) = match &snapshot.graph
    {
        Some(graph) => (
            graph.plan_id.as_str().to_string(),
            graph.plan_fingerprint.as_str().to_string(),
            graph.node_count,
            graph.completed_node_count,
            graph.terminal_node_count,
            graph
                .nodes
                .iter()
                .map(|node| NodeView {
                    node_id: node.node_id.as_str().to_string(),
                    kind: node.kind.as_label().to_string(),
                    status: node.status.as_label().to_string(),
                })
                .collect(),
        ),
        None => (String::new(), String::new(), 0, 0, 0, Vec::new()),
    };

    WorkflowRunView {
        run_id: snapshot.run_id.as_str().to_string(),
        owner_node,
        executed_locally,
        served_by,
        status,
        plan_id,
        plan_fingerprint,
        node_count,
        completed_node_count: completed,
        terminal_node_count: terminal,
        nodes,
        message: None,
    }
}

/// Builds a minimal run view for a sentinel status such as `not-found`/`error`.
pub fn status_view(
    run_id: &str,
    owner_node: &str,
    served_by: &str,
    status: &str,
    message: Option<String>,
) -> WorkflowRunView {
    WorkflowRunView {
        run_id: run_id.to_string(),
        owner_node: owner_node.to_string(),
        executed_locally: true,
        served_by: served_by.to_string(),
        status: status.to_string(),
        plan_id: String::new(),
        plan_fingerprint: String::new(),
        node_count: 0,
        completed_node_count: 0,
        terminal_node_count: 0,
        nodes: Vec::new(),
        message,
    }
}

fn parse_supported_kind(label: &str) -> ExampleResult<AgentCompiledNodeKind> {
    match AgentCompiledNodeKind::from_label(label) {
        Some(
            kind @ (AgentCompiledNodeKind::Input
            | AgentCompiledNodeKind::Transform
            | AgentCompiledNodeKind::Terminal),
        ) => Ok(kind),
        Some(other) => Err(example_error(format!(
            "node kind {} is not supported by this example; use input, transform, or terminal",
            other.as_label()
        ))
        .into()),
        None => Err(example_error(format!("unknown node kind {label}")).into()),
    }
}

fn infer_kind(in_count: usize, out_count: usize) -> AgentCompiledNodeKind {
    if out_count == 0 {
        AgentCompiledNodeKind::Terminal
    } else if in_count == 0 {
        AgentCompiledNodeKind::Input
    } else {
        AgentCompiledNodeKind::Transform
    }
}

fn fingerprint(
    nodes: &[crate::model::NodeSpec],
    edges: &[AgentCompiledPlanEdge],
    entry_nodes: &[String],
) -> AgentCompiledPlanFingerprint {
    let mut topology = String::new();
    let mut node_ids: Vec<&str> = nodes.iter().map(|node| node.id.as_str()).collect();
    node_ids.sort_unstable();
    for node_id in node_ids {
        topology.push_str(node_id);
        topology.push(';');
    }
    topology.push('|');
    let mut edge_ids: Vec<&str> = edges.iter().map(|edge| edge.edge_id.as_str()).collect();
    edge_ids.sort_unstable();
    for edge_id in edge_ids {
        topology.push_str(edge_id);
        topology.push(';');
    }
    topology.push('|');
    let mut entries: Vec<&str> = entry_nodes.iter().map(String::as_str).collect();
    entries.sort_unstable();
    for entry in entries {
        topology.push_str(entry);
        topology.push(';');
    }
    AgentCompiledPlanFingerprint::new(format!("fp1:{:016x}", stable_hash(&topology)))
}

fn generate_run_id() -> String {
    format!(
        "run-{}-{}-{}",
        current_timestamp_millis(),
        std::process::id(),
        RUN_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn initial_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    now: AgentTimestampMillis,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new(TENANT)),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: None,
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

fn start_command(workflow: &AgentWorkflow, run_id: &AgentRunId) -> ExampleResult<AgentCommand> {
    let metadata = AgentCommandMetadata::new(
        workflow.workflow_id.clone(),
        run_id.clone(),
        AgentCommandId::new(format!("command-start-{}", run_id.as_str())),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("start:{}", run_id.as_str())),
            AgentCausationId::new("http-ingress"),
            AgentCorrelationId::new(format!("corr-{}", run_id.as_str())),
        ),
        AgentTenantId::new(TENANT),
        AgentTimestampMillis::new(current_timestamp_millis()),
    )
    .map_err(|error| example_error(format!("invalid start metadata: {error}")))?;
    AgentCommand::new(AgentCommandKind::StartRun, metadata)
        .map_err(|error| example_error(format!("invalid start command: {error}")).into())
}

const fn is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
    )
}

struct NowClock {
    value: u64,
}

impl NowClock {
    fn new() -> Self {
        Self {
            value: current_timestamp_millis(),
        }
    }

    fn peek(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.value)
    }

    fn next(&mut self) -> AgentTimestampMillis {
        let value = self.value;
        self.value = self.value.saturating_add(1);
        AgentTimestampMillis::new(value)
    }
}

async fn snapshot(child: &RunChild) -> ExampleResult<AgentRunActorSnapshot> {
    child
        .ask(
            |reply_to| AgentRunActorCommand::Snapshot { reply_to },
            RUN_ASK_TIMEOUT,
        )
        .await
        .map_err(ask_routing_error("snapshot"))?
        .map_err(|error| example_error(format!("snapshot failed: {error}")).into())
}

async fn accept_start_command(
    child: &RunChild,
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
) -> ExampleResult<()> {
    let command = start_command(workflow, run_id)?;
    child
        .ask(
            |reply_to| AgentRunActorCommand::AcceptCommand {
                command: command.clone(),
                reply_to,
            },
            RUN_ASK_TIMEOUT,
        )
        .await
        .map_err(ask_routing_error("accept-command"))?
        .map_err(|error| example_error(format!("accept-command failed: {error}")))?;
    Ok(())
}

async fn start_graph(
    child: &RunChild,
    initial_state: AgentRunState,
    plan: Arc<AgentCompiledExecutionPlan>,
    now: AgentTimestampMillis,
) -> ExampleResult<()> {
    child
        .ask(
            |reply_to| AgentRunActorCommand::StartGraph {
                initial_state: initial_state.clone(),
                plan: plan.clone(),
                now,
                reply_to,
            },
            RUN_ASK_TIMEOUT,
        )
        .await
        .map_err(ask_routing_error("start-graph"))?
        .map_err(|error| example_error(format!("start-graph failed: {error}")))?;
    Ok(())
}

async fn mark_graph_ready(
    child: &RunChild,
    plan: Arc<AgentCompiledExecutionPlan>,
    now: AgentTimestampMillis,
) -> ExampleResult<AgentGraphRuntimeTransition> {
    child
        .ask(
            |reply_to| AgentRunActorCommand::MarkGraphReady {
                plan: plan.clone(),
                now,
                reply_to,
            },
            RUN_ASK_TIMEOUT,
        )
        .await
        .map_err(ask_routing_error("mark-graph-ready"))?
        .map_err(|error| example_error(format!("mark-graph-ready failed: {error}")).into())
}

async fn start_graph_node(
    child: &RunChild,
    plan: Arc<AgentCompiledExecutionPlan>,
    node_id: AgentCompiledNodeId,
    now: AgentTimestampMillis,
) -> ExampleResult<()> {
    child
        .ask(
            |reply_to| AgentRunActorCommand::StartGraphNode {
                plan: plan.clone(),
                node_id: node_id.clone(),
                now,
                reply_to,
            },
            RUN_ASK_TIMEOUT,
        )
        .await
        .map_err(ask_routing_error("start-graph-node"))?
        .map_err(|error| example_error(format!("start-graph-node failed: {error}")))?;
    Ok(())
}

async fn complete_graph_node(
    child: &RunChild,
    plan: Arc<AgentCompiledExecutionPlan>,
    node_id: AgentCompiledNodeId,
    now: AgentTimestampMillis,
) -> ExampleResult<()> {
    child
        .ask(
            |reply_to| AgentRunActorCommand::CompleteGraphNode {
                plan: plan.clone(),
                node_id: node_id.clone(),
                now,
                reply_to,
            },
            RUN_ASK_TIMEOUT,
        )
        .await
        .map_err(ask_routing_error("complete-graph-node"))?
        .map_err(|error| example_error(format!("complete-graph-node failed: {error}")))?;
    Ok(())
}

fn ask_routing_error(operation: &'static str) -> impl Fn(AskError) -> crate::support::ExampleError {
    move |error| example_error(format!("{operation} ask failed: {error:?}")).into()
}
