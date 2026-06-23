//! Kubernetes drain and shutdown tests for agent workflows.

#![cfg(feature = "k8s")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    register_agent_workflow_ingress_stop_task, register_agent_workflow_telemetry_flush_task,
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentGraphBlockedReason,
    AgentGraphNodeState, AgentGraphNodeStatus, AgentGraphRunState, AgentGraphWaitReason,
    AgentInboxAcceptance, AgentRunActorSnapshot, AgentRunId, AgentRunInbox, AgentRunState,
    AgentRunStatus, AgentStatePayload, AgentTenantId, AgentTimestampMillis,
    AgentWorkflowDrainError, AgentWorkflowId, AgentWorkflowIngressGate,
    AgentWorkflowSnapshotRegistry, StateSchemaVersion, WorkflowDefinitionVersion,
    AGENT_WORKFLOW_FLUSH_TELEMETRY_OPERATION, AGENT_WORKFLOW_FLUSH_TELEMETRY_TASK,
    AGENT_WORKFLOW_SHUTDOWN_OPERATION_ATTR, AGENT_WORKFLOW_STOP_INGRESS_OPERATION,
    AGENT_WORKFLOW_STOP_INGRESS_TASK,
};
use rakka_cluster::{ClusterMembership, ClusterNode, MembershipConfig, NodeAddress, NodeId};
use rakka_core::{CoordinatedShutdown, ShutdownPhase, ShutdownTask};
use rakka_k8s::{KubernetesDrainController, KubernetesDrainOutcome, KubernetesNodeHealth};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type TestStore = InMemoryDurableStateStore<WorkflowState>;

#[tokio::test]
async fn coordinated_kubernetes_drain_stops_ingress_and_preserves_durable_work() {
    let health = ready_health();
    let gate = AgentWorkflowIngressGate::new(health.clone());
    let shutdown = CoordinatedShutdown::new();
    register_agent_workflow_ingress_stop_task(&shutdown, gate.clone())
        .expect("ingress stop task should register");

    let telemetry_flushes = Arc::new(AtomicUsize::new(0));
    register_agent_workflow_telemetry_flush_task(&shutdown, {
        let telemetry_flushes = telemetry_flushes.clone();
        move || {
            let telemetry_flushes = telemetry_flushes.clone();
            async move {
                telemetry_flushes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    })
    .expect("telemetry flush task should register");

    assert_registered_task(
        shutdown
            .tasks()
            .expect("shutdown tasks should list")
            .as_slice(),
        AGENT_WORKFLOW_STOP_INGRESS_TASK,
        &ShutdownPhase::stop_ingress(),
        AGENT_WORKFLOW_STOP_INGRESS_OPERATION,
    );
    assert_registered_task(
        shutdown
            .tasks()
            .expect("shutdown tasks should list")
            .as_slice(),
        AGENT_WORKFLOW_FLUSH_TELEMETRY_TASK,
        &ShutdownPhase::flush_persistence(),
        AGENT_WORKFLOW_FLUSH_TELEMETRY_OPERATION,
    );

    let store = TestStore::new();
    let mut inbox = test_inbox(store.clone());
    inbox.recover().await.expect("inbox should recover");

    let accepted = gate
        .accept_command(
            &mut inbox,
            start_command("command-1", "command:run-1:start"),
        )
        .await
        .expect("command should be accepted before drain");
    assert!(matches!(accepted, AgentInboxAcceptance::Accepted { .. }));

    let drain = KubernetesDrainController::from_coordinated_shutdown(health.clone(), shutdown);
    let report = drain.drain(Duration::from_secs(2)).await;

    assert_eq!(report.outcome(), KubernetesDrainOutcome::Complete);
    assert!(report.steps().iter().any(|step| step.name()
        == format!(
            "{}/{}",
            ShutdownPhase::stop_ingress().name(),
            AGENT_WORKFLOW_STOP_INGRESS_TASK
        )
        && step.status() == rakka_k8s::KubernetesDrainStepStatus::Completed
        && step.message().contains(AGENT_WORKFLOW_STOP_INGRESS_TASK)));
    assert_eq!(telemetry_flushes.load(Ordering::SeqCst), 1);
    assert!(!health.readiness_probe().passed());
    assert!(health
        .readiness_probe()
        .reasons()
        .contains(&"node-draining".to_string()));

    let error = gate
        .accept_command(
            &mut inbox,
            start_command("command-2", "command:run-1:after-drain"),
        )
        .await
        .expect_err("new public commands should be rejected after drain starts");
    assert_eq!(error.code(), "agent-workflow-draining");
    assert!(matches!(error, AgentWorkflowDrainError::Draining { .. }));

    let mut recovered = test_inbox(store);
    recovered
        .recover()
        .await
        .expect("accepted command should recover after drain interruption");
    let recoverable = recovered
        .inner()
        .recoverable_inbox()
        .expect("recovered inbox should be available");
    assert_eq!(recoverable.len(), 1);
    assert_eq!(recoverable[0].message_id().as_str(), "command-1");
}

#[tokio::test]
async fn kubernetes_drain_before_graph_start_rejects_public_graph_ingress() {
    let health = ready_health();
    let gate = AgentWorkflowIngressGate::new(health);
    let store = TestStore::new();
    let run_id = AgentRunId::new("run-graph-drain-before-start");
    let mut inbox = test_inbox_for_run(store.clone(), run_id.clone());
    inbox.recover().await.expect("inbox should recover");

    gate.begin_drain();

    let error = gate
        .accept_command(
            &mut inbox,
            start_command_for_run(
                &run_id,
                "command-graph-drain-start",
                "graph-command:run-graph-drain-before-start:start",
            ),
        )
        .await
        .expect_err("graph start should be rejected after drain starts");
    assert_eq!(error.code(), "agent-workflow-draining");

    let mut recovered = test_inbox_for_run(store, run_id);
    recovered
        .recover()
        .await
        .expect("recovered inbox should load");
    let recoverable = recovered
        .inner()
        .recoverable_inbox()
        .expect("recovered inbox should be available");
    assert!(recoverable.is_empty());
}

#[test]
fn runtime_snapshot_reports_graph_drain_blockers_during_kubernetes_drain() {
    let health = ready_health();
    let gate = AgentWorkflowIngressGate::new(health);
    let snapshots = AgentWorkflowSnapshotRegistry::new();

    gate.begin_drain();
    assert!(!gate.accepts_public_commands());

    record_graph_waiting_run(
        &snapshots,
        "run-graph-effect-drain",
        "effect",
        AgentCompiledNodeKind::ToolCall,
        AgentGraphWaitReason::Effect,
    );
    record_graph_waiting_run(
        &snapshots,
        "run-graph-human-drain",
        "approval",
        AgentCompiledNodeKind::HumanCheckpoint,
        AgentGraphWaitReason::Human,
    );

    let runtime = snapshots.runtime_snapshot();
    assert_eq!(runtime.graph_run_count(), 2);
    assert_eq!(runtime.graph_drain_blocker_count(), 2);
    assert_eq!(runtime.graph_waiting_node_count(), 2);
    assert_eq!(runtime.graph_effect_waiting_node_count(), 1);
    assert_eq!(runtime.graph_human_waiting_node_count(), 1);
    assert_eq!(runtime.graph_runnable_node_count(), 0);
    assert_eq!(runtime.graph_running_node_count(), 0);
}

fn assert_registered_task(
    tasks: &[ShutdownTask],
    name: &str,
    phase: &ShutdownPhase,
    operation: &str,
) {
    let task = tasks
        .iter()
        .find(|task| task.name() == name)
        .unwrap_or_else(|| panic!("missing shutdown task {name}"));
    assert_eq!(task.phase(), phase);
    assert_eq!(
        task.options()
            .attributes()
            .iter()
            .find(|attribute| attribute.key() == AGENT_WORKFLOW_SHUTDOWN_OPERATION_ATTR)
            .map(|attribute| attribute.value()),
        Some(operation)
    );
}

fn test_inbox(store: TestStore) -> AgentRunInbox<TestStore, ManualWorkflowClock> {
    test_inbox_for_run(store, AgentRunId::new("run-1"))
}

fn test_inbox_for_run(
    store: TestStore,
    run_id: AgentRunId,
) -> AgentRunInbox<TestStore, ManualWorkflowClock> {
    AgentRunInbox::with_clock(
        run_id,
        store,
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
    )
}

fn start_command(command_id: &str, deduplication_key: &str) -> AgentCommand {
    start_command_for_run(&AgentRunId::new("run-1"), command_id, deduplication_key)
}

fn start_command_for_run(
    run_id: &AgentRunId,
    command_id: &str,
    deduplication_key: &str,
) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            AgentWorkflowId::new("workflow-1"),
            run_id.clone(),
            AgentCommandId::new(command_id),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(deduplication_key),
                AgentCausationId::new("ingress-1"),
                AgentCorrelationId::new("corr-1"),
            ),
            AgentTenantId::new("tenant-a"),
            AgentTimestampMillis::new(100),
        )
        .expect("metadata should be valid"),
    )
    .expect("command should be valid")
}

fn record_graph_waiting_run(
    snapshots: &AgentWorkflowSnapshotRegistry,
    run_id_value: &str,
    node_id_value: &str,
    node_kind: AgentCompiledNodeKind,
    wait_reason: AgentGraphWaitReason,
) {
    let run_id = AgentRunId::new(run_id_value);
    snapshots.record_run_actor_snapshot(&AgentRunActorSnapshot {
        run_id: run_id.clone(),
        run_state: Some(graph_waiting_run_state(
            &run_id,
            node_id_value,
            node_kind,
            wait_reason,
        )),
        graph: None,
        recoverable_command_count: 0,
        due_effect_count: 0,
    });
}

fn graph_waiting_run_state(
    run_id: &AgentRunId,
    node_id_value: &str,
    node_kind: AgentCompiledNodeKind,
    wait_reason: AgentGraphWaitReason,
) -> AgentRunState {
    let node = AgentGraphNodeState::new(
        AgentCompiledNodeId::new(node_id_value),
        node_kind,
        AgentTimestampMillis::new(120),
    )
    .status(AgentGraphNodeStatus::Waiting)
    .dependencies_ready(true)
    .wait_reason(wait_reason);

    let graph_state = AgentGraphRunState::new(
        AgentCompiledPlanId::new(format!("plan-{node_id_value}-drain")),
        AgentCompiledPlanFingerprint::new(format!("sha256:{node_id_value}-drain")),
    )
    .node_state(node)
    .blocked_reason(AgentGraphBlockedReason::new(format!(
        "waiting-{}",
        wait_reason.as_label()
    )));

    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: AgentWorkflowId::new("workflow-graph-drain"),
        tenant: Some(AgentTenantId::new("tenant-a")),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: Some(graph_state),
        status: AgentRunStatus::Running,
        current_step_id: None,
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(120),
        completed_at: None,
    }
}

fn ready_health() -> KubernetesNodeHealth {
    let mut membership =
        ClusterMembership::new(node("rakka-agent-0", "uid-a"), membership_config());
    let local = membership.local_node_id().clone();
    membership.mark_up(&local, 1).expect("local node up");
    let health = KubernetesNodeHealth::from_membership(&membership);
    health.accept_compatibility();
    health
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(
            format!("{logical_id}.rakka-agent-internal.rakka-system.svc.cluster.local"),
            2552,
        ),
    )
}
