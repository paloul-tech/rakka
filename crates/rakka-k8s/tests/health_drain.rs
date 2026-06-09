//! Kubernetes health and pre-stop drain behavior tests.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, MembershipState,
    NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, ReplyTo,
};
use rakka_k8s::{
    KubernetesDrainController, KubernetesDrainOutcome, KubernetesDrainStepResult,
    KubernetesDrainStepStatus, KubernetesNodeHealth,
};
use rakka_process::{ProcessActorCommand, ProcessActorState, ProcessActorStatus, ProcessHealth};
use rakka_sharding::{
    ClusterShardingRuntime, EntityType, RoutedEntityMessage, ShardRegion, ShardingConfig,
};
use rakka_stream::{bounded_channel, StreamLifecycle};
use tokio::sync::mpsc;

#[test]
fn readiness_is_false_before_join_and_true_after_required_services_register() {
    let mut membership = membership_with_local_joining();
    let health = KubernetesNodeHealth::from_membership(&membership);
    health.require_service("http");

    let before = health.readiness_probe();
    assert!(!before.passed());
    assert!(before
        .reasons()
        .contains(&"cluster-not-up:joining".to_string()));
    assert!(before
        .reasons()
        .contains(&"missing-service:http".to_string()));

    let local = membership.local_node_id().clone();
    membership
        .mark_up(&local, 1)
        .expect("local node should transition up");
    health.refresh_from_membership(&membership);
    health.register_service("http");

    let compatible_before_acceptance = health.readiness_probe();
    assert!(!compatible_before_acceptance.passed());
    assert!(compatible_before_acceptance
        .reasons()
        .contains(&"compatibility-not-accepted".to_string()));
    health.accept_compatibility();

    assert!(health.readiness_probe().passed());
}

#[test]
fn missing_required_service_keeps_readiness_false() {
    let mut membership = membership_with_local_joining();
    let local = membership.local_node_id().clone();
    membership.mark_up(&local, 1).expect("local node up");
    let health = KubernetesNodeHealth::from_membership(&membership);
    health.accept_compatibility();
    health.require_service("grpc");
    health.require_service("http");
    health.register_service("http");

    let readiness = health.readiness_probe();

    assert!(!readiness.passed());
    assert_eq!(health.snapshot().missing_services(), &["grpc".to_string()]);
    assert!(readiness
        .reasons()
        .contains(&"missing-service:grpc".to_string()));
}

#[test]
fn liveness_ignores_rebalance_and_drain_but_fails_when_runtime_is_stuck() {
    let mut membership = membership_with_local_joining();
    let local = membership.local_node_id().clone();
    membership.mark_up(&local, 1).expect("local node up");
    let health = KubernetesNodeHealth::from_membership(&membership);
    health.accept_compatibility();

    health.mark_rebalancing("Cart");
    health.begin_drain();

    assert!(health.liveness_probe().passed());

    health.mark_runtime_stuck("executor stalled");

    let liveness = health.liveness_probe();
    assert!(!liveness.passed());
    assert!(liveness
        .reasons()
        .contains(&"runtime-stuck:executor stalled".to_string()));
}

#[tokio::test]
async fn drain_marks_readiness_false_and_reports_partial_and_timeout() {
    let mut membership = membership_with_local_joining();
    let local = membership.local_node_id().clone();
    membership.mark_up(&local, 1).expect("local node up");
    let health = KubernetesNodeHealth::from_membership(&membership);
    health.accept_compatibility();
    let mut partial = KubernetesDrainController::new(health.clone());
    partial.add_step("ok", || async {
        KubernetesDrainStepResult::completed("done")
    });
    partial.add_step("fail", || async {
        KubernetesDrainStepResult::failed("not drained")
    });

    let partial_report = partial.drain(Duration::from_secs(1)).await;

    assert_eq!(partial_report.outcome(), KubernetesDrainOutcome::Partial);
    assert!(!health.readiness_probe().passed());
    assert!(health
        .readiness_probe()
        .reasons()
        .contains(&"node-draining".to_string()));

    let timeout_health = KubernetesNodeHealth::new(NodeId::new("rakka-0", "uid-a"));
    let mut timeout = KubernetesDrainController::new(timeout_health);
    timeout.add_step("slow", || async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        KubernetesDrainStepResult::completed("late")
    });

    let timeout_report = timeout.drain(Duration::from_millis(5)).await;

    assert_eq!(timeout_report.outcome(), KubernetesDrainOutcome::TimedOut);
    assert_eq!(
        timeout_report.steps()[0].status(),
        KubernetesDrainStepStatus::TimedOut
    );
}

#[tokio::test]
async fn drain_step_drains_registered_streams() {
    let health = KubernetesNodeHealth::new(NodeId::new("rakka-0", "uid-a"));
    let mut controller = KubernetesDrainController::new(health);
    let (sink, source) = bounded_channel::<String>(1).expect("stream should be created");
    controller.add_stream_sink("outbound-stream", sink.clone());

    let report = controller.drain(Duration::from_secs(1)).await;

    assert!(report.is_complete());
    assert_eq!(source.status().lifecycle(), StreamLifecycle::Completed);
}

#[tokio::test]
async fn sharding_drain_marks_local_leaving_and_reassigns_owned_shards() {
    let local = node("rakka-0", "uid-a");
    let local_id = local.id().clone();
    let remote = node("rakka-1", "uid-b");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).expect("valid sharding config");
    let mut runtime =
        ClusterShardingRuntime::new(ClusterMembership::new(local.clone(), membership_config()));
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local, remote]))
        .expect("discovery should promote members");
    runtime
        .register_region(ShardRegion::new(
            entity_type.clone(),
            config,
            |_message: RoutedEntityMessage<TestCommand>| Ok(()),
        ))
        .expect("region should register");
    assert!(runtime
        .coordinator(&entity_type)
        .expect("coordinator should exist")
        .snapshot()
        .assignments()
        .iter()
        .any(|assignment| assignment.owner() == &local_id));
    let runtime = Arc::new(Mutex::new(runtime));
    let health = KubernetesNodeHealth::new(local_id.clone());
    let mut controller = KubernetesDrainController::new(health);
    controller.add_sharding_runtime_leave("sharding", runtime.clone(), 2);

    let report = controller.drain(Duration::from_secs(1)).await;

    assert!(report.is_complete());
    let runtime = runtime
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        runtime
            .membership()
            .member(&local_id)
            .expect("local member should exist")
            .state(),
        MembershipState::Leaving
    );
    assert!(runtime
        .coordinator(&entity_type)
        .expect("coordinator should exist")
        .snapshot()
        .assignments()
        .iter()
        .all(|assignment| assignment.owner() != &local_id));
}

#[tokio::test]
async fn process_actor_stop_step_sends_graceful_stop_command() {
    let system = ActorSystem::new("k8s-process-drain-test");
    let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
    let actor = system
        .spawn_actor("process", RecordingProcessActor { stopped_tx })
        .expect("process actor should spawn");
    let health = KubernetesNodeHealth::new(NodeId::new("rakka-0", "uid-a"));
    let mut controller = KubernetesDrainController::new(health);
    controller.add_process_actor_stop("process", actor, Duration::from_secs(1));

    let report = controller.drain(Duration::from_secs(1)).await;

    assert!(report.is_complete());
    assert_eq!(stopped_rx.recv().await, Some(()));
    system.shutdown();
}

enum TestCommand {}

struct RecordingProcessActor {
    stopped_tx: mpsc::UnboundedSender<()>,
}

impl Actor for RecordingProcessActor {
    type Msg = ProcessActorCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let stopped_tx = self.stopped_tx.clone();
        actor_future(async move {
            match msg {
                ProcessActorCommand::Stop { reply_to } => {
                    let _sent = stopped_tx.send(());
                    reply_status(
                        reply_to,
                        ProcessActorStatus::new(
                            ProcessActorState::Stopped,
                            None,
                            0,
                            ProcessHealth::Unknown,
                            None,
                            None,
                            None,
                        ),
                    );
                }
                ProcessActorCommand::Status { reply_to } => {
                    let _sent = reply_to.reply(ProcessActorStatus::new(
                        ProcessActorState::Running,
                        Some(1),
                        0,
                        ProcessHealth::Healthy,
                        None,
                        None,
                        None,
                    ));
                }
                ProcessActorCommand::Start { reply_to }
                | ProcessActorCommand::Restart { reply_to }
                | ProcessActorCommand::CheckHealth { reply_to } => {
                    reply_status(
                        reply_to,
                        ProcessActorStatus::new(
                            ProcessActorState::Running,
                            Some(1),
                            0,
                            ProcessHealth::Healthy,
                            None,
                            None,
                            None,
                        ),
                    );
                }
                ProcessActorCommand::SupervisionTick { .. } => {}
            }
            Ok(ActorAction::Continue)
        })
    }
}

fn reply_status(
    reply_to: ReplyTo<rakka_process::ProcessResult<ProcessActorStatus>>,
    status: ProcessActorStatus,
) {
    let _sent = reply_to.reply(Ok(status));
}

fn membership_with_local_joining() -> ClusterMembership {
    ClusterMembership::new(node("rakka-0", "uid-a"), membership_config())
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}
