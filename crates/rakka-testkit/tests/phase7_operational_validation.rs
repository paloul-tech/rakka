//! Phase 7 coordinated shutdown operational validation.

use std::future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_cluster::{
    register_cluster_leave_task, Cluster, ClusterMembership, ClusterNode, ClusterProtocol,
    DiscoverySnapshot, MembershipConfig, MembershipState, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem,
    ActorSystemShutdownConfig, CoordinatedShutdown, CoordinatedShutdownError,
    CoordinatedShutdownReport, CoordinatedShutdownSettings, RakkaError, ReplyTo,
    ShutdownFailurePolicy, ShutdownOutcome, ShutdownPhase, ShutdownTaskStatus,
};
use rakka_grpc::{register_grpc_shutdown_task, GrpcShutdownHandle};
use rakka_http::{register_http_shutdown_task, HttpShutdownHandle};
use rakka_k8s::{
    KubernetesDrainController, KubernetesDrainOutcome, KubernetesDrainStepStatus,
    KubernetesNodeHealth,
};
use rakka_persistence::{
    register_persistence_query_cancel_task, register_persistence_shutdown_task,
    InMemoryEventJournal,
};
use rakka_process::{register_process_actor_stop_task, ProcessActorCommand, ProcessActorState};
use rakka_remote::{
    register_tcp_remote_drain_task, EncodedPayload, RemoteDestination, RemoteEndpoint,
    RemoteEnvelope, RemoteEnvelopeMetadata, RemoteTransport, RemoteTransportError,
    TcpRemoteTransport, TcpRemoteTransportConfig, TcpRemoteTransportError,
};
use rakka_sharding::{
    register_cluster_sharding_leave_task, ClusterShardingRuntime, ClusterShardingShutdownHandle,
    EntityType, RoutedEntityMessage, ShardHandoffState, ShardRegion, ShardingConfig,
};
use rakka_stream::{bounded_channel, register_stream_sink_drain, StreamError, StreamLifecycle};
use tokio::sync::mpsc;

#[tokio::test]
async fn phase7_operational_path_runs_cross_crate_adapters_once() {
    let system = ActorSystem::new("phase7-operational-validation");
    let shutdown = CoordinatedShutdown::get(&system);

    let http = HttpShutdownHandle::new();
    register_http_shutdown_task(&shutdown, "stop-public-http", http.clone())
        .expect("HTTP shutdown task should register");

    let grpc = GrpcShutdownHandle::new();
    register_grpc_shutdown_task(&shutdown, "stop-public-grpc", grpc.clone())
        .expect("gRPC shutdown task should register");

    let (orders_sink, orders_source) =
        bounded_channel::<String>(4).expect("orders stream should allocate");
    orders_sink
        .try_send("order-1".to_owned())
        .expect("orders stream should accept an item");
    register_stream_sink_drain(&shutdown, "drain-orders-stream", orders_sink)
        .expect("stream drain task should register");

    let (query_sink, query_source) =
        bounded_channel::<u64>(4).expect("query stream should allocate");
    query_sink
        .try_send(42)
        .expect("query stream should accept an item");
    register_persistence_query_cancel_task(
        &shutdown,
        "cancel-query-stream",
        query_source.clone(),
        "phase7-operational-validation",
    )
    .expect("query cancel task should register");

    register_persistence_shutdown_task(
        &shutdown,
        "flush-memory-journal",
        InMemoryEventJournal::<String>::new(),
    )
    .expect("persistence flush task should register");

    let cluster_node = node("phase7-cluster-0", "uid-a", 25520);
    let cluster = Cluster::for_local_node(cluster_node.clone(), MembershipConfig::default());
    cluster
        .manager()
        .join_self()
        .expect("local cluster member should become up");
    register_cluster_leave_task(&shutdown, "leave-cluster", cluster.clone())
        .expect("cluster leave task should register");

    let sharding_handle = sharding_shutdown_handle();
    register_cluster_sharding_leave_task(
        &shutdown,
        "handoff-local-shards",
        sharding_handle.clone(),
    )
    .expect("sharding handoff task should register");

    let (stopped_tx, mut stopped_rx) = mpsc::unbounded_channel();
    let process_actor = system
        .spawn_actor("recording-process", RecordingProcessActor { stopped_tx })
        .expect("recording process actor should spawn");
    register_process_actor_stop_task(
        &shutdown,
        "stop-process-actor",
        process_actor,
        Duration::from_secs(1),
    )
    .expect("process actor stop task should register");

    let remote_drain = maybe_register_tcp_remote_drain(&shutdown).await;

    let health = KubernetesNodeHealth::new(cluster_node.id().clone());
    health.accept_compatibility();
    let drain = KubernetesDrainController::from_coordinated_shutdown(health.clone(), shutdown);

    let drain_report = drain.drain(Duration::from_secs(2)).await;
    let first_terminate = system
        .terminate_with_report()
        .await
        .expect("terminate should reuse completed coordinated shutdown report");
    let second_terminate = system
        .terminate_with_report()
        .await
        .expect("repeated terminate should be idempotent");
    system.when_terminated().await;

    assert_eq!(drain_report.outcome(), KubernetesDrainOutcome::Complete);
    assert!(drain_report
        .steps()
        .iter()
        .any(|step| step.name() == "stop-ingress/stop-public-http"
            && step.status() == KubernetesDrainStepStatus::Completed));
    assert!(!health.readiness_probe().passed());
    assert!(health
        .readiness_probe()
        .reasons()
        .contains(&"node-draining".to_owned()));

    assert_eq!(first_terminate, second_terminate);
    assert_eq!(first_terminate.outcome(), ShutdownOutcome::Complete);
    assert_eq!(first_terminate.reason().code(), "kubernetes-prestop");
    assert_builtin_phase_order(&first_terminate);
    assert_completed(
        &first_terminate,
        ShutdownPhase::stop_ingress(),
        "stop-public-http",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::stop_ingress(),
        "stop-public-grpc",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::drain_adapters(),
        "drain-orders-stream",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::drain_adapters(),
        "cancel-query-stream",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::leave_cluster(),
        "leave-cluster",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::handoff_shards(),
        "handoff-local-shards",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::stop_process_actors(),
        "stop-process-actor",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::flush_persistence(),
        "flush-memory-journal",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::stop_user_actors(),
        "stop-user-actors",
    );
    assert_completed(
        &first_terminate,
        ShutdownPhase::stop_system_actors(),
        "stop-system-actors",
    );

    assert!(http.snapshot().shutdown_requested());
    assert!(grpc.snapshot().shutdown_requested());
    assert_eq!(
        orders_source.status().lifecycle(),
        StreamLifecycle::Draining
    );
    assert_eq!(
        orders_source
            .next()
            .await
            .expect("orders source should yield buffered item"),
        Some("order-1".to_owned())
    );
    assert_eq!(
        orders_source
            .next()
            .await
            .expect("orders source should complete after drain"),
        None
    );
    assert!(matches!(
        query_source.next().await,
        Err(StreamError::Cancelled { reason })
            if reason.as_deref() == Some("phase7-operational-validation")
    ));
    assert_eq!(
        cluster.self_member().expect("self member").state(),
        MembershipState::Leaving
    );
    let sharding_update = sharding_handle
        .last_update()
        .expect("sharding shutdown should record an update");
    assert!(sharding_update.handoffs().iter().any(|handoff| {
        handoff.state() == ShardHandoffState::Transferring
            && handoff.from().logical_id() == "phase7-shard-0"
    }));
    assert_eq!(stopped_rx.recv().await, Some(()));

    if let Some(remote) = remote_drain {
        assert_completed(
            &first_terminate,
            ShutdownPhase::stop_remoting(),
            "drain-tcp-peers",
        );
        let send_after_drain = remote.transport.send(
            remote.peer.id(),
            RemoteEnvelope::new(
                RemoteDestination::Entity {
                    entity_type: "Cart".to_owned(),
                    entity_id: "cart-1".to_owned(),
                },
                EncodedPayload::new(
                    RemoteEnvelopeMetadata::protobuf("rakka.test.Phase7", 1),
                    Vec::new(),
                ),
            ),
        );
        assert!(matches!(
            send_after_drain,
            Err(RemoteTransportError::Draining { node_id }) if node_id == *remote.peer.id()
        ));
    }
}

#[tokio::test]
async fn phase7_operational_validation_preserves_failure_policy_and_timeout_reports() {
    let continue_runs = Arc::new(AtomicUsize::new(0));
    let continue_system = ActorSystem::builder("phase7-continue-validation")
        .with_shutdown_config(
            ActorSystemShutdownConfig::new(Duration::from_secs(1))
                .with_coordinated_shutdown_settings(
                    CoordinatedShutdownSettings::new()
                        .with_failure_policy(ShutdownFailurePolicy::Continue),
                ),
        )
        .build()
        .await
        .expect("actor system should build");
    let continue_shutdown = CoordinatedShutdown::get(&continue_system);
    continue_shutdown
        .add_task(
            ShutdownPhase::stop_ingress(),
            "expected-failure",
            |_context| async { Err(RakkaError::core("phase7-expected-failure", "boom")) },
        )
        .expect("failure task should register");
    continue_shutdown
        .add_task(ShutdownPhase::drain_adapters(), "runs-after-failure", {
            let continue_runs = continue_runs.clone();
            move |_context| {
                let continue_runs = continue_runs.clone();
                async move {
                    continue_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .expect("continue task should register");

    let partial = continue_system
        .terminate_with_report()
        .await
        .expect("continue policy should return a partial report");
    continue_system.when_terminated().await;

    assert_eq!(partial.outcome(), ShutdownOutcome::Partial);
    assert_eq!(continue_runs.load(Ordering::SeqCst), 1);
    assert_status(
        &partial,
        ShutdownPhase::stop_ingress(),
        "expected-failure",
        ShutdownTaskStatus::Failed,
    );
    assert_completed(
        &partial,
        ShutdownPhase::drain_adapters(),
        "runs-after-failure",
    );

    let timeout_system = ActorSystem::builder("phase7-timeout-validation")
        .with_shutdown_config(
            ActorSystemShutdownConfig::new(Duration::from_secs(1))
                .with_coordinated_shutdown_settings(
                    CoordinatedShutdownSettings::new()
                        .with_default_task_timeout(Duration::from_millis(5)),
                ),
        )
        .build()
        .await
        .expect("actor system should build");
    let timeout_shutdown = CoordinatedShutdown::get(&timeout_system);
    let later_runs = Arc::new(AtomicUsize::new(0));
    timeout_shutdown
        .add_task(
            ShutdownPhase::stop_ingress(),
            "pending-ingress",
            |_context| async { future::pending::<rakka_core::RakkaResult<()>>().await },
        )
        .expect("pending task should register");
    timeout_shutdown
        .add_task(ShutdownPhase::drain_adapters(), "should-not-run", {
            let later_runs = later_runs.clone();
            move |_context| {
                let later_runs = later_runs.clone();
                async move {
                    later_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .expect("later task should register");

    let timeout = timeout_system
        .terminate_with_report()
        .await
        .expect_err("fail-fast timeout should surface a timed-out report");

    assert!(matches!(timeout, CoordinatedShutdownError::TimedOut { .. }));
    let report = timeout
        .report()
        .expect("timeout error should preserve a report");
    assert_eq!(report.outcome(), ShutdownOutcome::TimedOut);
    assert_status(
        report,
        ShutdownPhase::stop_ingress(),
        "pending-ingress",
        ShutdownTaskStatus::TimedOut,
    );
    assert_eq!(later_runs.load(Ordering::SeqCst), 0);
}

struct RemoteDrainFixture {
    transport: TcpRemoteTransport,
    peer: ClusterNode,
}

async fn maybe_register_tcp_remote_drain(
    shutdown: &CoordinatedShutdown,
) -> Option<RemoteDrainFixture> {
    let local = NodeId::new("phase7-remote-0", "uid-a");
    let peer = node("phase7-remote-1", "uid-b", 25521);
    let endpoint = RemoteEndpoint::new(local.clone());
    let transport = match TcpRemoteTransport::bind(
        local,
        ClusterProtocol::default(),
        endpoint,
        TcpRemoteTransportConfig::new()
            .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .connect_timeout(Duration::from_millis(10))
            .reconnect_backoff(Duration::from_millis(1)),
    )
    .await
    {
        Ok(transport) => transport,
        Err(TcpRemoteTransportError::Io { message })
            if message.contains("Operation not permitted")
                || message.contains("Permission denied") =>
        {
            eprintln!("skipping Phase 7K TCP drain assertion; loopback bind denied: {message}");
            return None;
        }
        Err(error) => panic!("TCP remote transport should bind for Phase 7K validation: {error:?}"),
    };
    transport
        .register_peer(peer.clone())
        .expect("remote peer should register");
    register_tcp_remote_drain_task(shutdown, "drain-tcp-peers", transport.clone())
        .expect("TCP remote drain task should register");
    Some(RemoteDrainFixture { transport, peer })
}

fn sharding_shutdown_handle() -> ClusterShardingShutdownHandle {
    let local = node("phase7-shard-0", "uid-a", 25530);
    let remote = node("phase7-shard-1", "uid-b", 25531);
    let entity_type = EntityType::new("Phase7Cart");
    let config = ShardingConfig::new(4).expect("valid sharding config");
    let mut runtime = ClusterShardingRuntime::new(ClusterMembership::new(
        local.clone(),
        MembershipConfig::default(),
    ));
    runtime
        .apply_discovery(DiscoverySnapshot::new("phase7", 1, [local, remote]))
        .expect("discovery should promote members");
    runtime
        .register_region(ShardRegion::new(
            entity_type,
            config,
            |_message: RoutedEntityMessage<ValidationEntityCommand>| Ok(()),
        ))
        .expect("shard region should register");
    ClusterShardingShutdownHandle::new(runtime)
}

#[derive(Debug)]
enum ValidationEntityCommand {}

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
                    reply_process_result(reply_to, ProcessActorState::Stopped);
                }
                ProcessActorCommand::Status { reply_to } => {
                    let _sent = reply_to.reply(process_status(ProcessActorState::Running));
                }
                ProcessActorCommand::Start { reply_to }
                | ProcessActorCommand::Restart { reply_to }
                | ProcessActorCommand::CheckHealth { reply_to } => {
                    reply_process_result(reply_to, ProcessActorState::Running);
                }
                ProcessActorCommand::SupervisionTick { .. } => {}
            }
            Ok(ActorAction::Continue)
        })
    }
}

fn reply_process_result(
    reply_to: ReplyTo<rakka_process::ProcessResult<rakka_process::ProcessActorStatus>>,
    state: ProcessActorState,
) {
    let _sent = reply_to.reply(Ok(process_status(state)));
}

fn process_status(state: ProcessActorState) -> rakka_process::ProcessActorStatus {
    rakka_process::ProcessActorStatus::new(
        state,
        Some(1),
        0,
        rakka_process::ProcessHealth::Healthy,
        None,
        None,
        None,
    )
}

fn node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
}

fn assert_builtin_phase_order(report: &CoordinatedShutdownReport) {
    let phases = report
        .phases()
        .iter()
        .map(|phase| phase.phase().name())
        .collect::<Vec<_>>();
    assert_eq!(
        phases,
        [
            "stop-ingress",
            "drain-http-grpc-and-streams",
            "leave-cluster",
            "handoff-shards",
            "stop-process-actors",
            "flush-persistence",
            "stop-user-actors",
            "stop-system-actors",
            "stop-remoting",
        ]
    );
}

fn assert_completed(report: &CoordinatedShutdownReport, phase: ShutdownPhase, task_name: &str) {
    assert_status(report, phase, task_name, ShutdownTaskStatus::Completed);
}

fn assert_status(
    report: &CoordinatedShutdownReport,
    phase: ShutdownPhase,
    task_name: &str,
    expected: ShutdownTaskStatus,
) {
    assert_eq!(task_status(report, &phase, task_name), Some(expected));
}

fn task_status(
    report: &CoordinatedShutdownReport,
    phase: &ShutdownPhase,
    task_name: &str,
) -> Option<ShutdownTaskStatus> {
    report
        .phases()
        .iter()
        .find(|phase_report| phase_report.phase() == phase)?
        .tasks()
        .iter()
        .find(|task_report| task_report.task_name() == task_name)
        .map(|task_report| task_report.status())
}
