//! Kubernetes drain and shutdown tests for agent workflows.

#![cfg(feature = "k8s")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    register_agent_workflow_ingress_stop_task, register_agent_workflow_telemetry_flush_task,
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentInboxAcceptance,
    AgentRunId, AgentRunInbox, AgentTenantId, AgentTimestampMillis, AgentWorkflowDrainError,
    AgentWorkflowId, AgentWorkflowIngressGate, AGENT_WORKFLOW_FLUSH_TELEMETRY_OPERATION,
    AGENT_WORKFLOW_FLUSH_TELEMETRY_TASK, AGENT_WORKFLOW_SHUTDOWN_OPERATION_ATTR,
    AGENT_WORKFLOW_STOP_INGRESS_OPERATION, AGENT_WORKFLOW_STOP_INGRESS_TASK,
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
    AgentRunInbox::with_clock(
        AgentRunId::new("run-1"),
        store,
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
    )
}

fn start_command(command_id: &str, deduplication_key: &str) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            AgentWorkflowId::new("workflow-1"),
            AgentRunId::new("run-1"),
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
