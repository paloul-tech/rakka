//! Sharded agent run integration tests.

#![cfg(feature = "sharding")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    agent_run_entity_ref, init_agent_run_sharding_with_clock_and_metrics, passivate_agent_run,
    registered_agent_run_entity_ref, AgentCausationId, AgentCommand, AgentCommandId,
    AgentCommandKind, AgentCommandMetadata, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentPayloadDescriptor, AgentRunActorCommand, AgentRunActorSnapshot,
    AgentRunId, AgentRunShardingSettings, AgentRunState, AgentRunStatus, AgentRunTransition,
    AgentRunTransitionKind, AgentRunWaitReason, AgentStatePayload, AgentStep, AgentStepId,
    AgentStepKind, AgentTenantId, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    StateSchemaVersion, WorkflowDefinitionVersion,
};
use rakka_core::{ActorSystem, InMemoryMetricsRecorder};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_sharding::{
    ClusterSharding, EntityTypeKey, InMemoryRememberedEntityStore, RememberedEntities,
    ShardBufferConfig,
};
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;

const ASK_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn sharded_run_routes_by_stable_run_id_and_recovers_after_passivation() {
    let system = ActorSystem::new("agent-sharded-run-route");
    let sharding = ClusterSharding::get(&system);
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let workflow = workflow();
    let key = EntityTypeKey::new("AgentRunRouteTest")
        .with_number_of_shards(4)
        .expect("entity type key should be valid");
    let settings = AgentRunShardingSettings::new(key.clone())
        .with_idle_passivation(Duration::from_secs(60))
        .with_passivation_buffer_duration(Duration::ZERO)
        .with_buffering(ShardBufferConfig::new(8, Duration::from_millis(250)));

    let registration = init_agent_run_sharding_with_clock_and_metrics(
        &sharding,
        workflow.clone(),
        run_store,
        workflow_store,
        clock,
        settings,
        metrics,
    )
    .expect("agent run sharding should initialize");
    let run_id = AgentRunId::new("run-sharded-route");
    let run = registered_agent_run_entity_ref(&registration, &run_id);
    assert_eq!(run.entity_id().as_str(), run_id.as_str());

    let routed_run =
        agent_run_entity_ref(&sharding, registration.key(), &run_id).expect("run ref should route");
    assert_eq!(routed_run.entity_id(), run.entity_id());

    let accepted = accept_start_command(&run, &workflow, &run_id).await;
    assert!(accepted.is_accepted());

    let started = start(
        &run,
        accepted_run_state(&workflow, &run_id, AgentStepId::new("plan")),
    )
    .await;
    assert_eq!(started.kind, AgentRunTransitionKind::Start);
    assert_eq!(started.next_status, AgentRunStatus::Accepted);

    let began = begin_step(&run, AgentTimestampMillis::new(200)).await;
    assert_eq!(began.next_status, AgentRunStatus::Running);

    let waiting = wait_for_timer(&run, AgentTimestampMillis::new(250)).await;
    assert_eq!(waiting.kind, AgentRunTransitionKind::WaitForTimer);
    assert_eq!(waiting.next_status, AgentRunStatus::WaitingForTimer);

    assert!(passivate_agent_run(&sharding, registration.key(), &run_id)
        .expect("passivation should be routed"));
    assert_eq!(
        sharding
            .registration_state(registration.key())
            .expect("registration state should exist")
            .local_entity_count(),
        0
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    let recovered = snapshot(&run).await;
    let recovered_state = recovered
        .run_state
        .as_ref()
        .expect("run state should recover after passivation");
    assert_eq!(recovered_state.run_id, run_id);
    assert_eq!(recovered_state.status, AgentRunStatus::WaitingForTimer);

    let resumed = resume(&run, AgentTimestampMillis::new(300)).await;
    assert_eq!(resumed.kind, AgentRunTransitionKind::Resume);
    assert_eq!(resumed.next_status, AgentRunStatus::Running);

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn remembered_entities_are_opt_in_for_agent_run_registrations() {
    let system = ActorSystem::new("agent-sharded-run-remembered");
    let sharding = ClusterSharding::get(&system);
    let workflow = workflow();

    let default_key = EntityTypeKey::new("AgentRunNoRememberTest")
        .with_number_of_shards(4)
        .expect("default entity type key should be valid");
    let default_registration = init_agent_run_sharding_with_clock_and_metrics(
        &sharding,
        workflow.clone(),
        RunStore::new(),
        WorkflowStore::new(),
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
        AgentRunShardingSettings::new(default_key.clone()),
        Arc::new(InMemoryMetricsRecorder::new()),
    )
    .expect("default agent run sharding should initialize");
    let default_state = sharding
        .registration_state(default_registration.key())
        .expect("default registration state should exist");
    assert!(!default_state.remembered_entities_enabled());
    assert_eq!(default_state.remembered_store_backend(), None);
    assert!(default_registration
        .region()
        .remembered_entities()
        .is_none());

    let remembered_key = EntityTypeKey::new("AgentRunRememberedTest")
        .with_number_of_shards(4)
        .expect("remembered entity type key should be valid");
    let remembered_registration = init_agent_run_sharding_with_clock_and_metrics(
        &sharding,
        workflow,
        RunStore::new(),
        WorkflowStore::new(),
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
        AgentRunShardingSettings::new(remembered_key.clone()).with_remembered_entities(
            RememberedEntities::enabled()
                .with_start_batch_size(3)
                .with_start_batch_delay(Duration::from_millis(5))
                .with_store(InMemoryRememberedEntityStore::new()),
        ),
        Arc::new(InMemoryMetricsRecorder::new()),
    )
    .expect("remembered agent run sharding should initialize");
    let remembered_state = sharding
        .registration_state(remembered_registration.key())
        .expect("remembered registration state should exist");
    assert!(remembered_state.remembered_entities_enabled());
    assert_eq!(remembered_state.remembered_start_batch_size(), 3);
    assert_eq!(
        remembered_state.remembered_start_batch_delay(),
        Duration::from_millis(5)
    );
    assert_eq!(
        remembered_state.remembered_store_backend(),
        Some("in-memory")
    );
    assert!(remembered_registration
        .region()
        .remembered_entities()
        .is_some());

    system.terminate().await.expect("system should terminate");
}

async fn accept_start_command(
    run: &rakka_agent_workflow::AgentRunEntityRef,
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
) -> rakka_agent_workflow::AgentInboxAcceptance {
    run.ask(
        |reply_to| AgentRunActorCommand::AcceptCommand {
            command: start_command(workflow, run_id),
            reply_to,
        },
        ASK_TIMEOUT,
    )
    .await
    .expect("accept command ask should reply")
    .expect("start command should be accepted")
}

async fn snapshot(run: &rakka_agent_workflow::AgentRunEntityRef) -> AgentRunActorSnapshot {
    run.ask(
        |reply_to| AgentRunActorCommand::Snapshot { reply_to },
        ASK_TIMEOUT,
    )
    .await
    .expect("snapshot ask should reply")
    .expect("snapshot should succeed")
}

async fn start(
    run: &rakka_agent_workflow::AgentRunEntityRef,
    initial_state: AgentRunState,
) -> AgentRunTransition {
    run.ask(
        |reply_to| AgentRunActorCommand::Start {
            initial_state,
            reply_to,
        },
        ASK_TIMEOUT,
    )
    .await
    .expect("start ask should reply")
    .expect("start should succeed")
}

async fn begin_step(
    run: &rakka_agent_workflow::AgentRunEntityRef,
    now: AgentTimestampMillis,
) -> AgentRunTransition {
    run.ask(
        |reply_to| AgentRunActorCommand::BeginStep { now, reply_to },
        ASK_TIMEOUT,
    )
    .await
    .expect("begin step ask should reply")
    .expect("begin step should succeed")
}

async fn wait_for_timer(
    run: &rakka_agent_workflow::AgentRunEntityRef,
    now: AgentTimestampMillis,
) -> AgentRunTransition {
    run.ask(
        |reply_to| AgentRunActorCommand::Wait {
            reason: AgentRunWaitReason::Timer,
            now,
            reply_to,
        },
        ASK_TIMEOUT,
    )
    .await
    .expect("wait ask should reply")
    .expect("wait should succeed")
}

async fn resume(
    run: &rakka_agent_workflow::AgentRunEntityRef,
    now: AgentTimestampMillis,
) -> AgentRunTransition {
    run.ask(
        |reply_to| AgentRunActorCommand::Resume { now, reply_to },
        ASK_TIMEOUT,
    )
    .await
    .expect("resume ask should reply")
    .expect("resume should succeed")
}

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-sharded-run"),
        workflow_type: "sharded-run".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Sharded run workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::WaitingForTimer.as_label().to_string(),
        ],
        command_types: vec![AgentCommandKind::StartRun.type_name().to_string()],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("sharded.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            "sharded-run".to_string(),
        )]),
    }
}

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    first_step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-sharded-run")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(first_step_id),
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
            AgentCommandId::new(format!("command-start-{}", run_id.as_str())),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(format!("start:{}", run_id.as_str())),
                AgentCausationId::new("sharded-run-ingress"),
                AgentCorrelationId::new(format!("corr-{}", run_id.as_str())),
            ),
            AgentTenantId::new("tenant-sharded-run"),
            AgentTimestampMillis::new(100),
        )
        .expect("start metadata should be valid"),
    )
    .expect("start command should be valid")
}
