//! Minimal local agent workflow example.
//!
//! This is intentionally not a full workflow engine. It demonstrates the
//! Phase 1 facade boundary in a single process: public command construction,
//! durable inbox acceptance, recovery from in-memory durable state, one
//! deterministic step, and inbox completion.

use std::sync::Arc;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentPayloadDescriptor,
    AgentRunId, AgentRunInbox, AgentRunState, AgentRunStatus, AgentStatePayload, AgentStep,
    AgentStepId, AgentStepKind, AgentTenantId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, AgentWorkflowRegistry, InlineState, StateSchemaVersion,
    WorkflowDefinitionVersion, METRIC_AGENT_INBOX_COMMANDS,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    InboxEntry, InboxStatus, ManualWorkflowClock, WorkflowState, WorkflowTimestamp,
};

type TestStore = InMemoryDurableStateStore<WorkflowState>;
type TestInbox = AgentRunInbox<TestStore, ManualWorkflowClock>;

#[tokio::test]
async fn minimal_local_workflow_starts_recovers_executes_one_step_and_completes() {
    let mut registry = AgentWorkflowRegistry::new();
    let workflow = local_workflow();
    registry
        .register(workflow.clone())
        .expect("local workflow should register");

    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-local-1");

    let mut ingress = agent_inbox(run_id.clone(), store.clone(), metrics.clone(), 100);
    ingress.recover().await.expect("ingress should recover");

    let start = start_command(&workflow, &run_id);
    let accepted = ingress
        .accept_command(start)
        .await
        .expect("StartRun should be durably accepted");
    assert!(accepted.is_accepted());
    assert_eq!(store.len(), 1);

    // Recovery is represented by a fresh facade over the same durable store.
    let mut recovered = agent_inbox(run_id.clone(), store.clone(), metrics.clone(), 200);
    recovered
        .recover()
        .await
        .expect("recovered inbox should load the durable StartRun entry");

    let pending = recovered
        .inner()
        .recoverable_inbox()
        .expect("recoverable inbox should load");
    assert_eq!(pending.len(), 1);

    let registered = registry
        .get("local-deterministic", &WorkflowDefinitionVersion::new("v1"))
        .expect("registered workflow should be queryable");
    let initial_run = accepted_run_state(registered, &run_id);
    let completed =
        run_one_deterministic_step(&mut recovered, registered, initial_run, &pending[0])
            .await
            .expect("local deterministic step should complete");

    assert_eq!(completed.status, AgentRunStatus::Completed);
    assert_eq!(completed.current_step_id, None);
    assert_eq!(completed.current_attempt, 1);
    assert_eq!(completed.completed_at, Some(AgentTimestampMillis::new(250)));
    assert_eq!(
        completed.state_payload,
        AgentStatePayload::Inline(InlineState {
            content_type: "text/plain".to_string(),
            bytes: b"deterministic-plan-complete".to_vec(),
            size_bytes: 27,
        })
    );

    assert_eq!(
        recovered
            .inner()
            .recoverable_inbox()
            .expect("recoverable inbox should load after completion")
            .len(),
        0
    );
    assert_eq!(
        metrics
            .snapshot()
            .observations_named(METRIC_AGENT_INBOX_COMMANDS)
            .len(),
        1
    );
}

async fn run_one_deterministic_step(
    inbox: &mut TestInbox,
    workflow: &AgentWorkflow,
    mut run: AgentRunState,
    entry: &InboxEntry,
) -> Result<AgentRunState, String> {
    let command: AgentCommand =
        serde_json::from_slice(entry.payload()).map_err(|error| error.to_string())?;

    if !workflow
        .command_types
        .iter()
        .any(|kind| kind == command.type_name())
    {
        return Err(format!("unsupported command type {}", command.type_name()));
    }
    if command.kind != AgentCommandKind::StartRun {
        return Err("minimal local example only accepts StartRun".to_string());
    }

    let step_id = run
        .current_step_id
        .clone()
        .ok_or_else(|| "run has no current step".to_string())?;
    let step = workflow
        .steps
        .iter()
        .find(|step| step.step_id == step_id)
        .ok_or_else(|| format!("step {step_id} not found"))?;

    inbox
        .inner_mut()
        .transition_inbox(entry.message_id(), InboxStatus::Processing)
        .await
        .map_err(|error| error.to_string())?;

    run.status = AgentRunStatus::Running;
    run.current_attempt = run.current_attempt.saturating_add(1);
    run.updated_at = AgentTimestampMillis::new(225);

    if step.kind != AgentStepKind::Planner {
        return Err(format!("unexpected step kind {:?}", step.kind));
    }

    run.state_payload = AgentStatePayload::Inline(InlineState {
        content_type: "text/plain".to_string(),
        bytes: b"deterministic-plan-complete".to_vec(),
        size_bytes: 27,
    });
    run.current_step_id = None;
    run.status = AgentRunStatus::Completed;
    run.updated_at = AgentTimestampMillis::new(250);
    run.completed_at = Some(AgentTimestampMillis::new(250));

    inbox
        .inner_mut()
        .transition_inbox(entry.message_id(), InboxStatus::Completed)
        .await
        .map_err(|error| error.to_string())?;

    Ok(run)
}

fn agent_inbox(
    run_id: AgentRunId,
    store: TestStore,
    metrics: Arc<InMemoryMetricsRecorder>,
    now_millis: u64,
) -> TestInbox {
    AgentRunInbox::with_clock_and_metrics(
        run_id,
        store,
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(now_millis)),
        metrics,
    )
}

fn local_workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-local-deterministic"),
        workflow_type: "local-deterministic".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Local deterministic workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![AgentCommandKind::StartRun.type_name().to_string()],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan deterministically".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: Default::default(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("local.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: Default::default(),
    }
}

fn accepted_run_state(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-local")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(AgentStepId::new("plan")),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(100),
        completed_at: None,
    }
}

fn start_command(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            workflow.workflow_id.clone(),
            run_id.clone(),
            AgentCommandId::new("command-start-local-1"),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new("start:run-local-1"),
                AgentCausationId::new("ingress-local-1"),
                AgentCorrelationId::new("corr-local-1"),
            ),
            AgentTenantId::new("tenant-local"),
            AgentTimestampMillis::new(100),
        )
        .expect("start metadata should be valid"),
    )
    .expect("start command should be valid")
}
