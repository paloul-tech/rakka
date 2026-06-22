//! Runtime operational snapshot tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffect, AgentEffectId,
    AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget,
    AgentGraphBlockedReason, AgentGraphNodeState, AgentGraphNodeStatus, AgentGraphRunState,
    AgentGraphWaitReason, AgentIdempotencyKey, AgentPayloadDescriptor, AgentRunActor,
    AgentRunActorCommand, AgentRunActorSnapshot, AgentRunId, AgentRunState, AgentRunStatus,
    AgentRunTransition, AgentRunWaitReason, AgentStatePayload, AgentStep, AgentStepId,
    AgentStepKind, AgentTenantId, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    AgentWorkflowSnapshotRegistry, StateSchemaVersion, WorkflowDefinitionVersion,
};
use rakka_core::{ActorRef, ActorSystem};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type TestActor = AgentRunActor<RunStore, WorkflowStore, ManualWorkflowClock>;
type TestActorRef = ActorRef<AgentRunActorCommand>;

const ASK_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct SnapshotFixture {
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    snapshots: AgentWorkflowSnapshotRegistry,
}

impl SnapshotFixture {
    fn new() -> Self {
        Self {
            workflow: workflow(),
            run_store: RunStore::new(),
            workflow_store: WorkflowStore::new(),
            clock: ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
            snapshots: AgentWorkflowSnapshotRegistry::with_max_sampled_runs(4),
        }
    }
}

#[tokio::test]
async fn runtime_snapshots_report_status_pending_commands_due_effects_and_recovery() {
    let system = ActorSystem::new("agent-runtime-snapshots");
    let fixture = SnapshotFixture::new();
    let run_id = AgentRunId::new("run-runtime-snapshot");
    let run = spawn_actor(&system, "runtime-snapshot", &fixture, run_id.clone());

    let accepted = run
        .ask(
            |reply_to| AgentRunActorCommand::AcceptCommand {
                command: start_command(&fixture.workflow, &run_id),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("accept ask should reply")
        .expect("start command should be accepted");
    assert!(accepted.is_accepted());

    start(
        &run,
        accepted_run_state(&fixture.workflow, &run_id, AgentStepId::new("plan")),
    )
    .await;
    begin_step(&run, AgentTimestampMillis::new(200)).await;
    wait_for_timer(&run, AgentTimestampMillis::new(250)).await;

    let scheduled = run
        .ask(
            |reply_to| AgentRunActorCommand::ScheduleEffect {
                effect: effect("effect-snapshot-1", AgentTimestampMillis::new(500)),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("schedule effect ask should reply")
        .expect("effect should schedule");
    assert!(scheduled.is_scheduled());

    fixture.clock.set(WorkflowTimestamp::from_millis(500));
    let due = run
        .ask(
            |reply_to| AgentRunActorCommand::DueEffects { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("due effects ask should reply")
        .expect("due effects should decode");
    assert_eq!(due.len(), 1);

    let runtime = fixture.snapshots.runtime_snapshot();
    assert_eq!(runtime.observed_run_count(), 1);
    assert_eq!(runtime.active_run_count(), 1);
    assert_eq!(runtime.terminal_run_count(), 0);
    assert_eq!(runtime.pending_command_count(), 1);
    assert_eq!(runtime.due_effect_count(), 1);
    assert!(runtime
        .status_counts()
        .iter()
        .any(|status| status.status() == "waiting-for-timer" && status.count() == 1));
    let sampled_run = runtime
        .sampled_runs()
        .first()
        .expect("runtime snapshot should include sampled run");
    assert_eq!(sampled_run.run_id(), run_id.as_str());
    assert_eq!(sampled_run.workflow_id(), Some("workflow-runtime-snapshot"));
    assert_eq!(sampled_run.status(), Some("waiting-for-timer"));
    assert_eq!(sampled_run.current_step_id(), Some("plan"));
    assert_eq!(sampled_run.pending_command_count(), 1);
    assert_eq!(sampled_run.due_effect_count(), 1);
    assert!(sampled_run.recovered());

    let outbox = fixture.snapshots.outbox_snapshot();
    assert_eq!(outbox.observed_run_count(), 1);
    assert_eq!(outbox.due_effect_count(), 1);
    assert_eq!(outbox.runs_with_due_effects(), 1);
    assert_eq!(outbox.sampled_runs()[0].run_id(), run_id.as_str());

    let recovery = fixture.snapshots.recovery_snapshot();
    assert_eq!(recovery.observed_run_count(), 1);
    assert_eq!(recovery.recovered_run_count(), 1);
    assert_eq!(recovery.unrecovered_run_count(), 0);
    assert_eq!(recovery.pending_command_count(), 1);
    assert_eq!(recovery.runs_with_pending_commands(), 1);
    assert_eq!(recovery.recovery_error_count(), 0);
    assert_eq!(recovery.sampled_runs()[0].run_id(), run_id.as_str());

    system.terminate().await.expect("system should terminate");
}

#[test]
fn runtime_snapshot_reports_graph_summaries() {
    let snapshots = AgentWorkflowSnapshotRegistry::new();
    let run_id = AgentRunId::new("run-graph-snapshot");
    let mut run = accepted_run_state(&workflow(), &run_id, AgentStepId::new("graph"));
    run.graph_state = Some(graph_state());
    snapshots.record_run_actor_snapshot(&AgentRunActorSnapshot {
        run_id: run_id.clone(),
        run_state: Some(run),
        graph: None,
        recoverable_command_count: 0,
        due_effect_count: 0,
    });

    let runtime = snapshots.runtime_snapshot();

    assert_eq!(runtime.graph_run_count(), 1);
    assert_eq!(runtime.graph_waiting_node_count(), 1);
    assert_eq!(runtime.graph_failed_node_count(), 1);
    assert_eq!(runtime.graph_blocked_run_count(), 1);
    let graph = runtime.sampled_runs()[0]
        .graph()
        .expect("sampled run should include graph summary");
    assert_eq!(
        graph.plan_fingerprint,
        AgentCompiledPlanFingerprint::new("sha256:graph-snapshot")
    );
    assert_eq!(graph.nodes.len(), 2);
}

#[cfg(feature = "http")]
#[test]
fn http_registration_uses_spec_snapshot_names() {
    use rakka_agent_workflow::{
        register_agent_workflow_operational_snapshots, SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS,
        SNAPSHOT_AGENT_WORKFLOW_OUTBOX, SNAPSHOT_AGENT_WORKFLOW_RECOVERY,
        SNAPSHOT_AGENT_WORKFLOW_RUNTIME,
    };
    use rakka_http::OperationalSnapshotRegistry;

    let registry = OperationalSnapshotRegistry::new();
    let snapshots = AgentWorkflowSnapshotRegistry::new();
    register_agent_workflow_operational_snapshots(&registry, snapshots);

    let value = serde_json::to_value(registry.snapshot()).expect("snapshots should serialize");
    assert!(value["snapshots"][SNAPSHOT_AGENT_WORKFLOW_RUNTIME].is_object());
    assert!(value["snapshots"][SNAPSHOT_AGENT_WORKFLOW_OUTBOX].is_object());
    assert!(value["snapshots"][SNAPSHOT_AGENT_WORKFLOW_RECOVERY].is_object());
    assert!(value["snapshots"][SNAPSHOT_AGENT_WORKFLOW_HUMAN_CHECKPOINTS].is_object());
}

#[cfg(feature = "sharding")]
#[tokio::test]
async fn shard_snapshot_reports_registered_agent_run_entity_type() {
    use rakka_agent_workflow::{
        agent_workflow_shards_snapshot, init_agent_run_sharding_with_clock_and_metrics,
        AgentRunShardingSettings,
    };
    use rakka_core::InMemoryMetricsRecorder;
    use rakka_sharding::{ClusterSharding, EntityTypeKey};

    let system = ActorSystem::new("agent-runtime-shard-snapshots");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::new("AgentRunSnapshotTest")
        .with_number_of_shards(8)
        .expect("entity type key should be valid");
    init_agent_run_sharding_with_clock_and_metrics(
        &sharding,
        workflow(),
        RunStore::new(),
        WorkflowStore::new(),
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
        AgentRunShardingSettings::new(key.clone()),
        Arc::new(InMemoryMetricsRecorder::new()),
    )
    .expect("agent run sharding should initialize");

    let snapshot = agent_workflow_shards_snapshot(&sharding);
    assert_eq!(snapshot.entity_type_count(), 1);
    assert_eq!(snapshot.local_entity_count(), 0);
    let entity_type = snapshot
        .entity_types()
        .first()
        .expect("shard snapshot should include entity type");
    assert_eq!(entity_type.entity_type(), "AgentRunSnapshotTest");
    assert_eq!(entity_type.number_of_shards(), 8);
    assert!(!entity_type.remembered_entities_enabled());

    system.terminate().await.expect("system should terminate");
}

fn spawn_actor(
    system: &ActorSystem,
    name: &str,
    fixture: &SnapshotFixture,
    run_id: AgentRunId,
) -> TestActorRef {
    system
        .spawn(
            name,
            TestActor::with_clock_and_metrics(
                fixture.workflow.clone(),
                run_id,
                fixture.run_store.clone(),
                fixture.workflow_store.clone(),
                fixture.clock.clone(),
                Arc::new(rakka_core::NoopMetricsRecorder),
            )
            .with_snapshot_registry(fixture.snapshots.clone()),
        )
        .expect("agent run actor should spawn")
}

async fn start(actor: &TestActorRef, initial_state: AgentRunState) -> AgentRunTransition {
    actor
        .ask(
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

async fn begin_step(actor: &TestActorRef, now: AgentTimestampMillis) -> AgentRunTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::BeginStep { now, reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("begin step ask should reply")
        .expect("begin step should succeed")
}

async fn wait_for_timer(actor: &TestActorRef, now: AgentTimestampMillis) -> AgentRunTransition {
    actor
        .ask(
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

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-runtime-snapshot"),
        workflow_type: "runtime-snapshot".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Runtime snapshot workflow".to_string()),
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
            AgentPayloadDescriptor::new("snapshot.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            "runtime-snapshot".to_string(),
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
        tenant: Some(AgentTenantId::new("tenant-runtime-snapshot")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
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

fn graph_state() -> AgentGraphRunState {
    let waiting = AgentGraphNodeState::new(
        AgentCompiledNodeId::new("model"),
        AgentCompiledNodeKind::ModelCall,
        AgentTimestampMillis::new(120),
    )
    .status(AgentGraphNodeStatus::Waiting)
    .dependencies_ready(true)
    .wait_reason(AgentGraphWaitReason::Effect);
    let failed = AgentGraphNodeState::new(
        AgentCompiledNodeId::new("tool"),
        AgentCompiledNodeKind::ToolCall,
        AgentTimestampMillis::new(130),
    )
    .status(AgentGraphNodeStatus::Failed)
    .dependencies_ready(true)
    .error_code("tool-timeout");

    AgentGraphRunState::new(
        AgentCompiledPlanId::new("plan-graph-snapshot"),
        AgentCompiledPlanFingerprint::new("sha256:graph-snapshot"),
    )
    .node_state(waiting)
    .node_state(failed)
    .blocked_reason(AgentGraphBlockedReason::new("waiting-effect"))
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
                AgentCausationId::new("snapshot-ingress"),
                AgentCorrelationId::new(format!("corr-{}", run_id.as_str())),
            ),
            AgentTenantId::new("tenant-runtime-snapshot"),
            AgentTimestampMillis::new(100),
        )
        .expect("start metadata should be valid"),
    )
    .expect("start command should be valid")
}

fn effect(effect_id: &str, due_at: AgentTimestampMillis) -> AgentEffect {
    AgentEffectSchedule::new(
        AgentEffectKind::ToolCall,
        AgentEffectTarget {
            target_type: "tool".to_string(),
            name: "snapshot-tool".to_string(),
            address: None,
            attributes: BTreeMap::new(),
        },
        AgentEffectMetadata::new(
            AgentEffectId::new(effect_id),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(format!("effect:{effect_id}")),
                AgentCausationId::new(format!("cause:{effect_id}")),
                AgentCorrelationId::new(format!("corr:{effect_id}")),
            ),
            AgentIdempotencyKey::new(format!("idempotency:{effect_id}")),
            AgentTimestampMillis::new(100),
        )
        .expect("effect metadata should be valid")
        .due_at(due_at),
    )
    .expect("effect schedule should be valid")
    .into_effect()
    .expect("effect should validate")
}
