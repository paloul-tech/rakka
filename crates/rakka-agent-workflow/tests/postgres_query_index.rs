//! PostgreSQL workflow query index integration tests.

#![cfg(feature = "postgres")]

use std::time::{SystemTime, UNIX_EPOCH};

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentDeduplicationKey, AgentDispatchId,
    AgentDispatchIndexEntry, AgentDispatchQuery, AgentDispatchStatus, AgentDispatchTargetClass,
    AgentDispatcherWorkerId, AgentEffectId, AgentEffectKind, AgentRunId, AgentRunIndexEntry,
    AgentRunQueryWaitingReason, AgentRunState, AgentRunStatus, AgentStatePayload, AgentStepId,
    AgentTelemetryContext, AgentTenantId, AgentTimerEntry, AgentTimerId, AgentTimerIndexEntry,
    AgentTimerPolicy, AgentTimerQuery, AgentTimerStatus, AgentTimestampMillis, AgentWorkflowId,
    AgentWorkflowQueryIndex, AgentWorkflowRunQuery, AgentWorkflowShardOwnership, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, PostgresAgentWorkflowQueryIndex, PrincipalRef,
    StateSchemaVersion, WorkflowDefinitionVersion, AGENT_WORKFLOW_AUDIT_INDEX_TABLE,
    AGENT_WORKFLOW_CHECKPOINT_INDEX_TABLE, AGENT_WORKFLOW_DISPATCH_INDEX_TABLE,
    AGENT_WORKFLOW_RUN_INDEX_TABLE, AGENT_WORKFLOW_TIMER_INDEX_TABLE,
};
use tokio_postgres::{Client, NoTls};

#[tokio::test]
async fn postgres_query_index_round_trips_bounded_operational_queries() {
    let Some(mut index) = test_index("round_trips").await else {
        return;
    };
    let namespace = index.namespace().to_string();

    upsert_run(
        &mut index,
        run_index(run_state(
            "pg-run-running",
            AgentRunStatus::Running,
            "step-plan",
            110,
            None,
        )),
    )
    .await;
    upsert_run(
        &mut index,
        run_index(run_state(
            "pg-run-timer",
            AgentRunStatus::WaitingForTimer,
            "step-wait",
            120,
            None,
        )),
    )
    .await;
    upsert_run(
        &mut index,
        run_index(run_state(
            "pg-run-human",
            AgentRunStatus::WaitingForHuman,
            "step-review",
            130,
            Some(checkpoint("pg-checkpoint-review", 40, Some(300))),
        )),
    )
    .await;
    upsert_run(
        &mut index,
        run_index(run_state(
            "pg-run-failed",
            AgentRunStatus::Failed,
            "step-tool",
            140,
            None,
        )),
    )
    .await;
    index
        .upsert_run(
            run_index(run_state(
                "pg-run-stuck",
                AgentRunStatus::Running,
                "step-tool",
                150,
                None,
            ))
            .shard_ownership(AgentWorkflowShardOwnership::new("AgentRun", "7", "node-a")),
        )
        .await
        .expect("stuck run should index");

    index
        .upsert_timer(timer_index(
            "pg-timer-due",
            "pg-run-timer",
            90,
            AgentTimerStatus::Pending,
            91,
        ))
        .await
        .expect("due timer should index");
    index
        .upsert_timer(timer_index(
            "pg-timer-future",
            "pg-run-timer",
            400,
            AgentTimerStatus::Pending,
            92,
        ))
        .await
        .expect("future timer should index");
    index
        .upsert_dispatch(dispatch_index(
            "pg-dispatch-stuck",
            "pg-run-stuck",
            "worker-a",
            1,
            120,
            150,
            121,
        ))
        .await
        .expect("stuck dispatch should index");

    let running = index
        .query_runs(AgentWorkflowRunQuery::new().status(AgentRunStatus::Running))
        .await
        .expect("running query should succeed");
    assert_eq!(run_ids(&running), vec!["pg-run-running", "pg-run-stuck"]);

    let human_waits = index
        .query_runs(AgentWorkflowRunQuery::new().waiting_reason(AgentRunQueryWaitingReason::Human))
        .await
        .expect("human wait query should succeed");
    assert_eq!(run_ids(&human_waits), vec!["pg-run-human"]);

    let stale_checkpoints = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .waiting_reason(AgentRunQueryWaitingReason::Human)
                .checkpoint_created_at_or_before(ts(50)),
        )
        .await
        .expect("checkpoint age query should succeed");
    assert_eq!(run_ids(&stale_checkpoints), vec!["pg-run-human"]);
    assert_eq!(
        open_checkpoint_count(&namespace, "pg-run-human").await,
        1,
        "human wait should maintain one open checkpoint projection",
    );
    index
        .upsert_run(run_index(run_state(
            "pg-run-human",
            AgentRunStatus::Completed,
            "step-review",
            160,
            None,
        )))
        .await
        .expect("completed human run should re-index");
    assert_eq!(
        open_checkpoint_count(&namespace, "pg-run-human").await,
        0,
        "completed run should remove open checkpoint projection",
    );

    let failed = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .status(AgentRunStatus::Failed)
                .failed_step_id("step-tool"),
        )
        .await
        .expect("failed step query should succeed");
    assert_eq!(run_ids(&failed), vec!["pg-run-failed"]);

    let due_timer_runs = index
        .query_runs(AgentWorkflowRunQuery::new().due_timer_at_or_before(ts(150)))
        .await
        .expect("due timer run query should succeed");
    assert_eq!(run_ids(&due_timer_runs), vec!["pg-run-timer"]);

    let due_timers = index
        .query_timers(
            AgentTimerQuery::new()
                .tenant("tenant-a")
                .namespace("prod")
                .status(AgentTimerStatus::Pending)
                .due_at_or_before(ts(150)),
        )
        .await
        .expect("due timer query should succeed");
    assert_eq!(timer_ids(&due_timers), vec!["pg-timer-due"]);

    let stuck_runs = index
        .query_runs(AgentWorkflowRunQuery::new().stuck_dispatcher_at_or_before(ts(200)))
        .await
        .expect("stuck run query should succeed");
    assert_eq!(run_ids(&stuck_runs), vec!["pg-run-stuck"]);

    let stuck_dispatches = index
        .query_dispatches(
            AgentDispatchQuery::new()
                .status(AgentDispatchStatus::Claimed)
                .target_class(AgentDispatchTargetClass::Tool)
                .stuck_at_or_before(ts(200)),
        )
        .await
        .expect("stuck dispatch query should succeed");
    assert_eq!(dispatch_ids(&stuck_dispatches), vec!["pg-dispatch-stuck"]);

    let shard_owned = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .shard_owner("node-a")
                .shard_id("7"),
        )
        .await
        .expect("shard owner query should succeed");
    assert_eq!(run_ids(&shard_owned), vec!["pg-run-stuck"]);

    index
        .delete_namespace()
        .await
        .expect("test namespace should clean up");
}

#[tokio::test]
async fn postgres_query_index_rejects_stale_timer_and_dispatch_writes() {
    let Some(mut index) = test_index("stale_writes").await else {
        return;
    };

    index
        .upsert_timer(timer_index(
            "pg-timer-fenced",
            "pg-run-fenced",
            300,
            AgentTimerStatus::Fired,
            300,
        ))
        .await
        .expect("newer timer projection should index");
    let stale_timer_error = index
        .upsert_timer(timer_index(
            "pg-timer-fenced",
            "pg-run-fenced",
            100,
            AgentTimerStatus::Pending,
            100,
        ))
        .await
        .expect_err("older timer projection should be rejected");
    assert_eq!(stale_timer_error.code(), "workflow-query-store");

    let timers = index
        .query_timers(AgentTimerQuery::new().run_id("pg-run-fenced"))
        .await
        .expect("timer query should succeed");
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].status, AgentTimerStatus::Fired);
    assert_eq!(timers[0].updated_at, ts(300));

    index
        .upsert_dispatch(dispatch_index(
            "pg-dispatch-fenced",
            "pg-run-fenced",
            "worker-b",
            2,
            200,
            500,
            210,
        ))
        .await
        .expect("newer fenced dispatch projection should index");
    let stale_dispatch_error = index
        .upsert_dispatch(dispatch_index(
            "pg-dispatch-fenced",
            "pg-run-fenced",
            "worker-a",
            1,
            100,
            150,
            900,
        ))
        .await
        .expect_err("lower fencing token should be rejected");
    assert_eq!(stale_dispatch_error.code(), "workflow-query-store");

    let dispatches = index
        .query_dispatches(AgentDispatchQuery::new().run_id("pg-run-fenced"))
        .await
        .expect("dispatch query should succeed");
    assert_eq!(dispatches.len(), 1);
    assert_eq!(
        dispatches[0]
            .worker_id
            .as_ref()
            .map(|worker| worker.as_str()),
        Some("worker-b")
    );
    assert_eq!(dispatches[0].fencing_token, Some(2));

    let mut completed_dispatch = dispatch_index(
        "pg-dispatch-fenced",
        "pg-run-fenced",
        "worker-b",
        2,
        200,
        500,
        220,
    );
    completed_dispatch.status = AgentDispatchStatus::Completed;
    completed_dispatch.worker_id = None;
    completed_dispatch.claimed_at = None;
    completed_dispatch.lease_expires_at = None;
    index
        .upsert_dispatch(completed_dispatch)
        .await
        .expect("terminal dispatch update with current fencing token should index");

    let completed_dispatches = index
        .query_dispatches(
            AgentDispatchQuery::new()
                .run_id("pg-run-fenced")
                .status(AgentDispatchStatus::Completed),
        )
        .await
        .expect("completed dispatch query should succeed");
    assert_eq!(completed_dispatches.len(), 1);
    assert_eq!(completed_dispatches[0].worker_id, None);
    assert_eq!(completed_dispatches[0].fencing_token, Some(2));

    index
        .delete_namespace()
        .await
        .expect("test namespace should clean up");
}

#[tokio::test]
async fn postgres_query_index_migration_creates_expected_tables() {
    let Some(index_client) = connect_client().await else {
        return;
    };
    let index = PostgresAgentWorkflowQueryIndex::builder(index_client)
        .with_namespace(unique_namespace("migration"))
        .migrate()
        .await
        .expect("migration should succeed");
    index
        .delete_namespace()
        .await
        .expect("test namespace should clean up");

    let Some(client) = connect_client().await else {
        return;
    };
    for table in [
        AGENT_WORKFLOW_RUN_INDEX_TABLE,
        AGENT_WORKFLOW_TIMER_INDEX_TABLE,
        AGENT_WORKFLOW_CHECKPOINT_INDEX_TABLE,
        AGENT_WORKFLOW_DISPATCH_INDEX_TABLE,
        AGENT_WORKFLOW_AUDIT_INDEX_TABLE,
    ] {
        let qualified_table = format!("public.{table}");
        let regclass: Option<String> = client
            .query_one("SELECT to_regclass($1)::text", &[&qualified_table])
            .await
            .expect("table lookup should succeed")
            .get(0);
        assert!(regclass.is_some(), "migration should create {table}");
    }
}

async fn test_index(test_name: &str) -> Option<PostgresAgentWorkflowQueryIndex> {
    let client = connect_client().await?;
    let index = PostgresAgentWorkflowQueryIndex::builder(client)
        .with_namespace(unique_namespace(test_name))
        .migrate()
        .await
        .expect("query index migration should succeed");
    index
        .delete_namespace()
        .await
        .expect("test namespace should start clean");
    Some(index)
}

async fn connect_client() -> Option<Client> {
    let Ok(dsn) = std::env::var("RAKKA_POSTGRES_TEST_DSN") else {
        eprintln!(
            "skipping PostgreSQL query index test because RAKKA_POSTGRES_TEST_DSN is not set"
        );
        return None;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("PostgreSQL test database should connect");
    let _connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL test connection failed: {error}");
        }
    });
    Some(client)
}

async fn open_checkpoint_count(namespace: &str, run_id: &str) -> i64 {
    let Some(client) = connect_client().await else {
        return 0;
    };
    client
        .query_one(
            r#"
SELECT count(*)
FROM rakka_agent_workflow_checkpoint_index
WHERE store_namespace = $1
  AND run_id = $2
  AND status = 'open'
"#,
            &[&namespace, &run_id],
        )
        .await
        .expect("checkpoint count query should succeed")
        .get(0)
}

fn unique_namespace(test_name: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_millis();
    format!(
        "agent_workflow_query_{test_name}_{}_{}",
        std::process::id(),
        millis
    )
}

async fn upsert_run(index: &mut PostgresAgentWorkflowQueryIndex, entry: AgentRunIndexEntry) {
    index
        .upsert_run(entry)
        .await
        .expect("run projection should index");
}

fn run_index(run: AgentRunState) -> AgentRunIndexEntry {
    AgentRunIndexEntry::from_run_state(&run, "research").namespace("prod")
}

fn timer_index(
    timer_id: &str,
    run_id: &str,
    due_at: u64,
    status: AgentTimerStatus,
    updated_at: u64,
) -> AgentTimerIndexEntry {
    let mut entry =
        AgentTimerIndexEntry::from_timer_entry(&timer(timer_id, run_id, due_at, status));
    entry.updated_at = ts(updated_at);
    entry.namespace("prod")
}

fn dispatch_index(
    dispatch_id: &str,
    run_id: &str,
    worker_id: &str,
    fencing_token: u64,
    claimed_at: u64,
    lease_expires_at: u64,
    updated_at: u64,
) -> AgentDispatchIndexEntry {
    AgentDispatchIndexEntry {
        dispatch_id: AgentDispatchId::new(dispatch_id),
        workflow_id: Some(workflow_id()),
        run_id: AgentRunId::new(run_id),
        effect_id: AgentEffectId::new(format!("effect:{dispatch_id}")),
        effect_kind: AgentEffectKind::ToolCall,
        target_class: AgentDispatchTargetClass::Tool,
        graph_plan_fingerprint: None,
        graph_node_id: None,
        graph_node_kind: None,
        graph_loop_instance_id: None,
        due_at: ts(100),
        status: AgentDispatchStatus::Claimed,
        worker_id: Some(AgentDispatcherWorkerId::new(worker_id)),
        fencing_token: Some(fencing_token),
        claimed_at: Some(ts(claimed_at)),
        lease_expires_at: Some(ts(lease_expires_at)),
        updated_at: ts(updated_at),
    }
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
    let pending_human_checkpoint = checkpoint
        .as_ref()
        .filter(|_| status == AgentRunStatus::WaitingForHuman)
        .map(|checkpoint| checkpoint.checkpoint_id.clone());
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
        pending_human_checkpoint,
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
        fired_at: (status == AgentTimerStatus::Fired).then(|| ts(due_at)),
    }
}

fn workflow_id() -> AgentWorkflowId {
    AgentWorkflowId::new("workflow-research")
}

const fn ts(value: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(value)
}
