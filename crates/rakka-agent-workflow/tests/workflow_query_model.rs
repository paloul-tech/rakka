//! Workflow query model tests.

use rakka_agent_workflow::{
    AgentAttributes, AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentDeduplicationKey, AgentDispatchEntry, AgentDispatchId,
    AgentDispatchIndexEntry, AgentDispatchLease, AgentDispatchQuery, AgentDispatchStatus,
    AgentDispatchTargetClass, AgentDispatcherWorkerId, AgentEffectId, AgentEffectKind,
    AgentEffectTarget, AgentGraphNodeState, AgentGraphNodeStatus, AgentGraphRunState,
    AgentGraphWaitReason, AgentRunId, AgentRunIndexEntry, AgentRunQueryWaitingReason,
    AgentRunState, AgentRunStatus, AgentStatePayload, AgentStepId, AgentTelemetryContext,
    AgentTenantId, AgentTimerId, AgentTimerIndexEntry, AgentTimerPolicy, AgentTimerQuery,
    AgentTimerStatus, AgentTimestampMillis, AgentWorkflowId, AgentWorkflowQueryIndex,
    AgentWorkflowRunQuery, AgentWorkflowShardOwnership, HumanCheckpoint, HumanCheckpointId,
    HumanCheckpointStatus, InMemoryAgentWorkflowQueryIndex, PrincipalRef, RedactionStatus,
    StateSchemaVersion, WorkflowDefinitionVersion,
};
use rakka_agent_workflow::{AgentCausationId, AgentCorrelationId, AgentTimerEntry};

#[tokio::test]
async fn query_index_lists_running_waiting_and_failed_runs() {
    let mut index = InMemoryAgentWorkflowQueryIndex::new();

    upsert_run(
        &mut index,
        run_state(
            "run-running",
            AgentRunStatus::Running,
            "step-plan",
            110,
            None,
        ),
    )
    .await;
    upsert_run(
        &mut index,
        run_state(
            "run-timer",
            AgentRunStatus::WaitingForTimer,
            "step-wait",
            120,
            None,
        ),
    )
    .await;
    upsert_run(
        &mut index,
        run_state(
            "run-human",
            AgentRunStatus::WaitingForHuman,
            "step-review",
            130,
            Some(checkpoint("checkpoint-review", 40, Some(300))),
        ),
    )
    .await;
    upsert_run(
        &mut index,
        run_state("run-failed", AgentRunStatus::Failed, "step-tool", 140, None),
    )
    .await;

    let running = index
        .query_runs(AgentWorkflowRunQuery::new().status(AgentRunStatus::Running))
        .await
        .expect("running query should succeed");
    assert_eq!(run_ids(&running), vec!["run-running"]);

    let waiting = index
        .query_runs(AgentWorkflowRunQuery::new().waiting())
        .await
        .expect("waiting query should succeed");
    assert_eq!(run_ids(&waiting), vec!["run-timer", "run-human"]);

    let human_waits = index
        .query_runs(AgentWorkflowRunQuery::new().waiting_reason(AgentRunQueryWaitingReason::Human))
        .await
        .expect("human wait query should succeed");
    assert_eq!(run_ids(&human_waits), vec!["run-human"]);

    let stale_checkpoints = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .waiting_reason(AgentRunQueryWaitingReason::Human)
                .checkpoint_created_at_or_before(ts(50)),
        )
        .await
        .expect("checkpoint age query should succeed");
    assert_eq!(run_ids(&stale_checkpoints), vec!["run-human"]);

    let failed = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .status(AgentRunStatus::Failed)
                .failed_step_id("step-tool"),
        )
        .await
        .expect("failed step query should succeed");
    assert_eq!(run_ids(&failed), vec!["run-failed"]);

    let scoped = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .tenant("tenant-a")
                .namespace("prod")
                .workflow_type("research")
                .definition_version("v1")
                .updated_at_from(ts(100))
                .updated_at_to(ts(125)),
        )
        .await
        .expect("scoped query should succeed");
    assert_eq!(run_ids(&scoped), vec!["run-running", "run-timer"]);

    let error = index
        .query_runs(AgentWorkflowRunQuery::new().limit(0))
        .await
        .expect_err("zero limit should be rejected");
    assert_eq!(error.code(), "invalid-workflow-query");
}

#[tokio::test]
async fn query_index_finds_due_timers_stuck_dispatchers_and_shard_owners() {
    let mut index = InMemoryAgentWorkflowQueryIndex::new();

    upsert_run(
        &mut index,
        run_state(
            "run-due-timer",
            AgentRunStatus::WaitingForTimer,
            "step-wait",
            100,
            None,
        ),
    )
    .await;
    upsert_run(
        &mut index,
        run_state(
            "run-future-timer",
            AgentRunStatus::WaitingForTimer,
            "step-wait",
            101,
            None,
        ),
    )
    .await;
    let stuck_run = AgentRunIndexEntry::from_run_state(
        &run_state("run-stuck", AgentRunStatus::Running, "step-tool", 102, None),
        "research",
    )
    .namespace("prod")
    .shard_ownership(AgentWorkflowShardOwnership::new("AgentRun", "7", "node-a"));
    index
        .upsert_run(stuck_run)
        .await
        .expect("stuck run should index");

    index
        .upsert_timer(AgentTimerIndexEntry::from_timer_entry(&timer(
            "timer-due",
            "run-due-timer",
            90,
            AgentTimerStatus::Pending,
        )))
        .await
        .expect("due timer should index");
    index
        .upsert_timer(AgentTimerIndexEntry::from_timer_entry(&timer(
            "timer-future",
            "run-future-timer",
            300,
            AgentTimerStatus::Pending,
        )))
        .await
        .expect("future timer should index");
    index
        .upsert_dispatch(AgentDispatchIndexEntry::from_dispatch_entry(&dispatch(
            "dispatch-stuck",
            "run-stuck",
            ts(120),
            ts(150),
        )))
        .await
        .expect("stuck dispatch should index");
    index
        .upsert_dispatch(AgentDispatchIndexEntry::from_dispatch_entry(&dispatch(
            "dispatch-active",
            "run-due-timer",
            ts(130),
            ts(500),
        )))
        .await
        .expect("active dispatch should index");

    let due_timer_runs = index
        .query_runs(AgentWorkflowRunQuery::new().due_timer_at_or_before(ts(150)))
        .await
        .expect("due timer run query should succeed");
    assert_eq!(run_ids(&due_timer_runs), vec!["run-due-timer"]);

    let due_timers = index
        .query_timers(
            AgentTimerQuery::new()
                .status(AgentTimerStatus::Pending)
                .due_at_or_before(ts(150))
                .limit(5),
        )
        .await
        .expect("due timer query should succeed");
    assert_eq!(timer_ids(&due_timers), vec!["timer-due"]);

    let stuck_runs = index
        .query_runs(AgentWorkflowRunQuery::new().stuck_dispatcher_at_or_before(ts(200)))
        .await
        .expect("stuck run query should succeed");
    assert_eq!(run_ids(&stuck_runs), vec!["run-stuck"]);

    let stuck_dispatches = index
        .query_dispatches(
            AgentDispatchQuery::new()
                .status(AgentDispatchStatus::Claimed)
                .target_class(AgentDispatchTargetClass::Tool)
                .stuck_at_or_before(ts(200)),
        )
        .await
        .expect("stuck dispatch query should succeed");
    assert_eq!(dispatch_ids(&stuck_dispatches), vec!["dispatch-stuck"]);

    let shard_owned = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .shard_owner("node-a")
                .shard_id("7"),
        )
        .await
        .expect("shard owner query should succeed");
    assert_eq!(run_ids(&shard_owned), vec!["run-stuck"]);

    index
        .remove_dispatch(AgentDispatchId::new("dispatch-stuck"))
        .await
        .expect("dispatch removal should succeed");
    let recovered = index
        .query_runs(AgentWorkflowRunQuery::new().stuck_dispatcher_at_or_before(ts(200)))
        .await
        .expect("stuck run query should still succeed");
    assert!(recovered.is_empty());
}

#[tokio::test]
async fn query_index_projects_and_filters_graph_state() {
    let mut index = InMemoryAgentWorkflowQueryIndex::new();
    let mut waiting = run_state(
        "run-graph-waiting",
        AgentRunStatus::WaitingForEffect,
        "graph",
        150,
        None,
    );
    waiting.graph_state = Some(graph_state(
        "plan-graph-v1",
        "sha256:graph-v1",
        "model",
        AgentCompiledNodeKind::ModelCall,
        AgentGraphNodeStatus::Waiting,
        Some(AgentGraphWaitReason::Effect),
        None,
    ));
    let mut failed = run_state(
        "run-graph-failed",
        AgentRunStatus::Failed,
        "graph",
        160,
        None,
    );
    failed.graph_state = Some(graph_state(
        "plan-graph-v2",
        "sha256:graph-v2",
        "tool",
        AgentCompiledNodeKind::ToolCall,
        AgentGraphNodeStatus::Failed,
        None,
        Some("tool-timeout"),
    ));

    upsert_run(&mut index, waiting).await;
    upsert_run(&mut index, failed).await;

    let waiting_projection = index
        .runs()
        .get(&AgentRunId::new("run-graph-waiting"))
        .expect("waiting graph run should be indexed");
    let graph = waiting_projection
        .graph
        .as_ref()
        .expect("graph projection should exist");
    assert_eq!(
        graph.plan_fingerprint,
        AgentCompiledPlanFingerprint::new("sha256:graph-v1")
    );
    assert_eq!(graph.waiting_node_count, 1);
    assert_eq!(graph.nodes[0].kind, AgentCompiledNodeKind::ModelCall);

    let by_fingerprint = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .graph_plan_fingerprint(AgentCompiledPlanFingerprint::new("sha256:graph-v1")),
        )
        .await
        .expect("graph fingerprint query should succeed");
    assert_eq!(run_ids(&by_fingerprint), vec!["run-graph-waiting"]);

    let waiting_model = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .graph_node_status(AgentGraphNodeStatus::Waiting)
                .graph_node_kind(AgentCompiledNodeKind::ModelCall)
                .graph_wait_reason(AgentGraphWaitReason::Effect),
        )
        .await
        .expect("graph node query should succeed");
    assert_eq!(run_ids(&waiting_model), vec!["run-graph-waiting"]);

    let failed_tool = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .graph_node_kind(AgentCompiledNodeKind::ToolCall)
                .graph_error_code("tool-timeout"),
        )
        .await
        .expect("graph error query should succeed");
    assert_eq!(run_ids(&failed_tool), vec!["run-graph-failed"]);
}

#[test]
fn dispatch_projection_preserves_last_fencing_token_after_lease_clears() {
    let mut entry = dispatch("dispatch-completed", "run-completed", ts(120), ts(150));
    entry.status = AgentDispatchStatus::Completed;
    entry.lease = None;
    entry.last_fencing_token = 7;
    entry.completed_at = Some(ts(180));
    entry.updated_at = ts(180);

    let projection = AgentDispatchIndexEntry::from_dispatch_entry(&entry);

    assert_eq!(projection.worker_id, None);
    assert_eq!(projection.claimed_at, None);
    assert_eq!(projection.lease_expires_at, None);
    assert_eq!(projection.fencing_token, Some(7));
}

async fn upsert_run(index: &mut InMemoryAgentWorkflowQueryIndex, run: AgentRunState) {
    index
        .upsert_run(AgentRunIndexEntry::from_run_state(&run, "research").namespace("prod"))
        .await
        .expect("run should index");
}

fn run_ids(entries: &[AgentRunIndexEntry]) -> Vec<&str> {
    entries.iter().map(|entry| entry.run_id.as_str()).collect()
}

fn timer_ids(entries: &[AgentTimerIndexEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.timer_id.as_str())
        .collect()
}

fn dispatch_ids(entries: &[AgentDispatchIndexEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.dispatch_id.as_str())
        .collect()
}

fn run_state(
    run_id: &str,
    status: AgentRunStatus,
    step_id: &str,
    updated_at: u64,
    checkpoint: Option<HumanCheckpoint>,
) -> AgentRunState {
    AgentRunState {
        run_id: AgentRunId::new(run_id),
        workflow_id: workflow_id(),
        tenant: Some(AgentTenantId::new("tenant-a")),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: None,
        status,
        current_step_id: Some(AgentStepId::new(step_id)),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: checkpoint.into_iter().collect(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: (status == AgentRunStatus::WaitingForHuman)
            .then(|| HumanCheckpointId::new("checkpoint-review")),
        cancellation: None,
        created_at: ts(10),
        updated_at: ts(updated_at),
        completed_at: matches!(
            status,
            AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
        )
        .then(|| ts(updated_at)),
    }
}

fn graph_state(
    plan_id: &str,
    fingerprint: &str,
    node_id: &str,
    kind: AgentCompiledNodeKind,
    status: AgentGraphNodeStatus,
    wait_reason: Option<AgentGraphWaitReason>,
    error_code: Option<&str>,
) -> AgentGraphRunState {
    let mut node = AgentGraphNodeState::new(
        AgentCompiledNodeId::new(node_id),
        kind,
        AgentTimestampMillis::new(120),
    )
    .status(status)
    .dependencies_ready(true);
    if let Some(wait_reason) = wait_reason {
        node = node.wait_reason(wait_reason);
    }
    if let Some(error_code) = error_code {
        node = node.error_code(error_code);
    }
    AgentGraphRunState::new(
        AgentCompiledPlanId::new(plan_id),
        AgentCompiledPlanFingerprint::new(fingerprint),
    )
    .node_state(node)
}

fn checkpoint(checkpoint_id: &str, created_at: u64, due_at: Option<u64>) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id),
        status: HumanCheckpointStatus::Open,
        summary: "Review query fixture".to_string(),
        available_decisions: Vec::new(),
        required_roles: vec!["reviewer".to_string()],
        due_at: due_at.map(ts),
        escalation_target: None,
        context_artifacts: Vec::new(),
        created_by: Some(PrincipalRef {
            principal_type: "test".to_string(),
            principal_id: "query-fixture".to_string(),
            display_name: Some("Query Fixture".to_string()),
        }),
        resolved_by: None,
        created_at: ts(created_at),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn timer(timer_id: &str, run_id: &str, due_at: u64, status: AgentTimerStatus) -> AgentTimerEntry {
    AgentTimerEntry {
        timer_id: AgentTimerId::new(timer_id),
        workflow_id: workflow_id(),
        run_id: AgentRunId::new(run_id),
        tenant: AgentTenantId::new("tenant-a"),
        due_at: ts(due_at),
        deduplication_key: AgentDeduplicationKey::new(format!("timer:{timer_id}")),
        causation_id: AgentCausationId::new(format!("cause:{timer_id}")),
        correlation_id: AgentCorrelationId::new(format!("corr:{run_id}")),
        telemetry_context: AgentTelemetryContext::default(),
        policy: AgentTimerPolicy::default(),
        status,
        created_at: ts(10),
        updated_at: ts(due_at),
        fired_at: None,
    }
}

fn dispatch(
    dispatch_id: &str,
    run_id: &str,
    claimed_at: AgentTimestampMillis,
    lease_expires_at: AgentTimestampMillis,
) -> AgentDispatchEntry {
    AgentDispatchEntry {
        dispatch_id: AgentDispatchId::new(dispatch_id),
        workflow_id: Some(workflow_id()),
        run_id: AgentRunId::new(run_id),
        effect_id: AgentEffectId::new(format!("effect:{dispatch_id}")),
        effect_kind: AgentEffectKind::ToolCall,
        target: AgentEffectTarget {
            target_type: "tool".to_string(),
            name: "query-tool".to_string(),
            address: None,
            attributes: AgentAttributes::new(),
        },
        target_class: AgentDispatchTargetClass::Tool,
        graph_plan_fingerprint: None,
        graph_node_id: None,
        graph_node_kind: None,
        graph_loop_instance_id: None,
        due_at: ts(100),
        status: AgentDispatchStatus::Claimed,
        lease: Some(AgentDispatchLease {
            worker_id: AgentDispatcherWorkerId::new("worker-a"),
            fencing_token: 1,
            claimed_at,
            lease_expires_at,
        }),
        last_fencing_token: 1,
        attempts: 1,
        last_error_code: None,
        created_at: ts(90),
        updated_at: claimed_at,
        completed_at: None,
        exhausted_at: None,
        attributes: AgentAttributes::from([(
            "redaction".to_string(),
            RedactionStatus::ReferenceOnly.as_label().to_string(),
        )]),
    }
}

fn workflow_id() -> AgentWorkflowId {
    AgentWorkflowId::new("workflow-research")
}

const fn ts(value: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(value)
}
