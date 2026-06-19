#![cfg(feature = "k8s")]

//! Kubernetes startup and readiness tests for agent workflows.

use std::time::Duration;

use rakka_agent_workflow::{
    default_agent_workflow_required_services, parse_agent_workflow_required_services,
    AgentWorkflowKubernetesStartup, AgentWorkflowStartupStep,
    AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE, AGENT_WORKFLOW_STARTUP_DURABLE_STATE,
    AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS, AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER,
    AGENT_WORKFLOW_STARTUP_POSTGRES, AGENT_WORKFLOW_STARTUP_QUERY_INDEX,
    AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE, AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY,
    DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS,
};
use rakka_cluster::{ClusterMembership, ClusterNode, MembershipConfig, NodeAddress, NodeId};
use rakka_k8s::KubernetesNodeHealth;

#[test]
fn default_startup_services_match_manifest_readiness_vocabulary() {
    let services = default_agent_workflow_required_services();

    assert_eq!(services.len(), DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS.len());
    assert_eq!(services[0], AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE);
    assert_eq!(services[1], AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER);
    assert!(services.contains(&AGENT_WORKFLOW_STARTUP_POSTGRES));
    assert!(services.contains(&AGENT_WORKFLOW_STARTUP_DURABLE_STATE));
    assert!(services.contains(&AGENT_WORKFLOW_STARTUP_QUERY_INDEX));
    assert!(services.contains(&AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE));
    assert!(services.contains(&AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY));
    assert!(services.contains(&AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS));

    let parsed = parse_agent_workflow_required_services(&services.join(","));
    assert_eq!(parsed, DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS.to_vec());
    assert_eq!(
        AgentWorkflowStartupStep::Postgres.service_name(),
        AGENT_WORKFLOW_STARTUP_POSTGRES
    );
    assert_eq!(
        AgentWorkflowStartupStep::Postgres.description(),
        "PostgreSQL connectivity validated"
    );
}

#[test]
fn startup_checklist_keeps_readiness_false_until_required_steps_complete() {
    let health = ready_health();
    let mut startup = AgentWorkflowKubernetesStartup::new(health.clone());

    let initial = startup.readiness_probe();
    assert!(!initial.passed());
    assert!(initial
        .reasons()
        .contains(&"missing-service:postgres".to_string()));
    assert!(initial
        .reasons()
        .contains(&"missing-service:workflow-registry".to_string()));

    for step in DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS
        .into_iter()
        .filter(|step| *step != AgentWorkflowStartupStep::OperationalSnapshots)
    {
        startup.complete_step(step);
    }

    let missing_snapshots = startup.readiness_probe();
    assert!(!missing_snapshots.passed());
    assert_eq!(
        startup.pending_steps(),
        vec![AgentWorkflowStartupStep::OperationalSnapshots]
    );
    assert!(missing_snapshots
        .reasons()
        .contains(&"missing-service:operational-snapshots".to_string()));

    startup.complete_step(AgentWorkflowStartupStep::OperationalSnapshots);

    assert!(startup.readiness_probe().passed());
    assert!(startup.pending_steps().is_empty());
    assert!(health.snapshot().missing_services().is_empty());
}

#[test]
fn startup_checklist_handles_compatibility_failures_and_drain() {
    let health = ready_health();
    let mut startup = AgentWorkflowKubernetesStartup::new(health.clone());
    startup.complete_all_steps();

    assert!(startup.readiness_probe().passed());

    startup.record_compatibility_failure("state schema too old");
    let incompatible = startup.readiness_probe();
    assert!(!incompatible.passed());
    assert!(incompatible
        .reasons()
        .contains(&"compatibility-not-accepted".to_string()));

    startup.accept_compatibility();
    assert!(startup.readiness_probe().passed());

    health.begin_drain();
    let draining = startup.readiness_probe();
    assert!(!draining.passed());
    assert!(draining.reasons().contains(&"node-draining".to_string()));
}

#[test]
fn startup_snapshot_reports_required_completed_and_pending_steps() {
    let health = ready_health();
    let mut startup = AgentWorkflowKubernetesStartup::with_required_steps(
        health,
        [
            AgentWorkflowStartupStep::TelemetryResource,
            AgentWorkflowStartupStep::Postgres,
            AgentWorkflowStartupStep::WorkflowRegistry,
        ],
    );

    startup.complete_step(AgentWorkflowStartupStep::TelemetryResource);
    let snapshot = startup.snapshot();

    assert_eq!(
        snapshot.required_steps,
        vec![
            AgentWorkflowStartupStep::TelemetryResource,
            AgentWorkflowStartupStep::Postgres,
            AgentWorkflowStartupStep::WorkflowRegistry,
        ]
    );
    assert_eq!(
        snapshot.completed_steps,
        vec![AgentWorkflowStartupStep::TelemetryResource]
    );
    assert_eq!(
        snapshot.pending_steps,
        vec![
            AgentWorkflowStartupStep::Postgres,
            AgentWorkflowStartupStep::WorkflowRegistry,
        ]
    );
    assert!(!snapshot.readiness.passed());

    startup.complete_step(AgentWorkflowStartupStep::Postgres);
    startup.complete_step(AgentWorkflowStartupStep::WorkflowRegistry);
    startup.reset_step(AgentWorkflowStartupStep::Postgres);

    assert_eq!(
        startup.pending_steps(),
        vec![AgentWorkflowStartupStep::Postgres]
    );
    assert!(startup
        .readiness_probe()
        .reasons()
        .contains(&"missing-service:postgres".to_string()));
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
