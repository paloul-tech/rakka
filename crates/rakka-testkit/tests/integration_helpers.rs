//! Integration tests for reusable Phase 5 testkit helpers.

use std::any::type_name;
use std::time::Duration;

use axum::http::StatusCode;
use rakka_cluster::{
    Cluster, ClusterNode, ClusterSubscriptionReplay, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_fn, ActorAction, ActorContext, ActorPath, ActorSystem, ActorUid, InMemoryMetricsRecorder,
    MetricKind, MetricsRecorder, Receptionist, Routers, ServiceKey,
};
use rakka_grpc::{server_streaming_response, unary_service, GrpcUnaryConfig};
use rakka_http::{json_service_route, HttpRouteConfig};
use rakka_k8s::{KubernetesDrainController, KubernetesDrainStepResult, KubernetesNodeHealth};
use rakka_persistence::{
    DurableEffect, DurableStateBehavior, DurableStateStore, EventJournal, EventSourcedBehavior,
    EventSourcedEffect, PersistenceId, Revision, SequenceNr, SnapshotSelection, SnapshotStore,
    TaggedEvent,
};
use rakka_remote::{
    RemoteActorRef, RemoteReceptionistListing, RemoteServiceProxyRegistrySnapshot,
    RemoteServiceRoutee,
};
use rakka_stream::{bounded_channel, StreamLifecycle};
use rakka_testkit::{
    assert_cluster_event_node, assert_counter_total, assert_drain_complete,
    assert_group_routee_count, assert_group_router_snapshot_routee_count, assert_http_status,
    assert_metric_attribute, assert_pool_routee_count, assert_probe_failed_with_reason,
    assert_receptionist_listing_contains, assert_remote_receptionist_listing_count,
    assert_remote_receptionist_listing_service, assert_remote_service_listing_count,
    assert_remote_service_proxy_count, assert_stream_lifecycle, expect_cluster_member_up,
    expect_grpc_stream_items, expect_grpc_unary_ok, expect_metric_observation,
    expect_receptionist_listing_count, expect_remote_proxy_registry_snapshot,
    expect_stream_source_items, expect_terminated, grpc_request, http_post_json,
    spawn_actor_context_probe, spawn_echo_probe, spawn_stop_probe, ActorContextProbeCommand,
    ActorContextProbeEvent, DurableStateBehaviorTestKit, EventSourcedBehaviorTestKit,
    PersistenceTestKit, StopProbeCommand, TestProbe,
};
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn testkit_helpers_cover_phase_5_surfaces() {
    let router = json_service_route(
        "/double",
        HttpRouteConfig::default(),
        |request: NumberRequest| async move {
            Ok(NumberReply {
                value: request.value * 2,
            })
        },
    );
    let http = http_post_json(router, "/double", &NumberRequest { value: 4 }).await;
    assert_http_status(&http, StatusCode::OK);
    assert_eq!(http.json::<NumberReply>().value, 8);

    let grpc = expect_grpc_unary_ok(unary_service(
        grpc_request(NumberRequest { value: 5 }),
        GrpcUnaryConfig::default(),
        |request: NumberRequest| async move {
            Ok(NumberReply {
                value: request.value + 1,
            })
        },
    ))
    .await;
    assert_eq!(grpc.value, 6);

    let (grpc_sink, grpc_source) = bounded_channel(2).expect("gRPC stream channel");
    grpc_sink
        .send(NumberReply { value: 1 })
        .await
        .expect("first gRPC stream item");
    grpc_sink
        .send(NumberReply { value: 2 })
        .await
        .expect("second gRPC stream item");
    grpc_sink.drain().expect("gRPC stream drain");
    let grpc_stream = server_streaming_response(grpc_source);
    assert_eq!(
        expect_grpc_stream_items(grpc_stream.into_inner()).await,
        vec![NumberReply { value: 1 }, NumberReply { value: 2 }]
    );

    let (sink, source) = bounded_channel(2).expect("Rakka stream channel");
    sink.send("one".to_owned()).await.expect("first item");
    sink.send("two".to_owned()).await.expect("second item");
    sink.drain().expect("stream drain");
    expect_stream_source_items(&source, vec!["one".to_owned(), "two".to_owned()]).await;
    assert_stream_lifecycle(&source, StreamLifecycle::Completed);

    let health = KubernetesNodeHealth::new(rakka_cluster::NodeId::new("rakka-0", "uid-a"));
    assert_probe_failed_with_reason(&health.readiness_probe(), "cluster-not-up:joining");
    let mut drain = KubernetesDrainController::new(health);
    drain.add_step("custom", || async {
        KubernetesDrainStepResult::completed("done")
    });
    let report = drain.drain(Duration::from_secs(1)).await;
    assert_drain_complete(&report);

    let recorder = InMemoryMetricsRecorder::new();
    recorder.increment_counter("rakka.test.events", 2, &[("surface", "testkit")]);
    assert_counter_total(&recorder.snapshot(), "rakka.test.events", 2.0);
    let observation = expect_metric_observation(
        &recorder.snapshot(),
        "rakka.test.events",
        MetricKind::Counter,
    );
    assert_metric_attribute(&observation, "surface", "testkit");
}

#[tokio::test]
async fn testkit_helpers_cover_phase_5_cluster_receptionist_and_router_surfaces() {
    let system = ActorSystem::new("phase-5-testkit");
    let key = ServiceKey::<Phase5Command>::new("phase-5-workers");
    let receptionist = Receptionist::get(&system);
    let mut listings = receptionist
        .subscribe(&key)
        .expect("receptionist subscription should start");

    let initial = expect_receptionist_listing_count(&mut listings, 0, Duration::from_secs(1))
        .await
        .expect("initial listing should be empty");
    assert_eq!(initial.revision(), 0);

    let worker = system
        .spawn(
            "phase-5-worker",
            actor_fn(
                |_ctx: &mut ActorContext<Phase5Command>, _msg: Phase5Command| {
                    Ok(ActorAction::Continue)
                },
            ),
        )
        .expect("worker should spawn");
    let _registration = receptionist
        .register(&key, worker.clone())
        .expect("worker should register");
    let listing = expect_receptionist_listing_count(&mut listings, 1, Duration::from_secs(1))
        .await
        .expect("registration listing should arrive");
    assert_receptionist_listing_contains(&listing, &worker);

    let group = Routers::group(key)
        .spawn(&system, "phase-5-group")
        .expect("group router should spawn");
    assert_group_routee_count(&group, 1);
    assert_group_router_snapshot_routee_count(&group.snapshot(), 1);

    let pool = Routers::pool("phase-5-pool", 2, || {
        actor_fn(
            |_ctx: &mut ActorContext<Phase5Command>, _msg: Phase5Command| Ok(ActorAction::Continue),
        )
    })
    .spawn(&system)
    .expect("pool router should spawn");
    assert_pool_routee_count(&pool, 2);

    let node = cluster_node("rakka-testkit", "uid-a", 25520);
    let cluster = Cluster::for_local_node(node.clone(), MembershipConfig::default());
    let mut cluster_events = cluster
        .subscriptions()
        .subscribe(ClusterSubscriptionReplay::LiveOnly);
    cluster
        .manager()
        .join_self()
        .expect("local cluster node should join");
    let member_up =
        expect_cluster_member_up(&mut cluster_events, node.id(), Duration::from_secs(1))
            .await
            .expect("member-up event should arrive");
    assert_cluster_event_node(&member_up, node.id());

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn testkit_helpers_cover_remote_receptionist_surfaces() {
    let routee = RemoteServiceRoutee::new(
        RemoteActorRef::new(
            NodeId::new("remote-testkit-a", "uid-a"),
            "remote-testkit",
            ActorPath::new("rakka://remote-testkit/user/worker"),
            ActorUid::new(1),
            type_name::<Phase5Command>(),
        )
        .expect("remote actor ref should be valid"),
    );
    let listing = RemoteReceptionistListing::new(
        NodeId::new("remote-testkit-a", "uid-a"),
        "phase-5-workers",
        type_name::<Phase5Command>(),
        vec![routee],
        3,
        99,
    )
    .expect("remote listing should be valid");
    assert_remote_receptionist_listing_count(&listing, 1);
    assert_remote_receptionist_listing_service(
        &listing,
        "phase-5-workers",
        type_name::<Phase5Command>(),
    );

    let snapshot = RemoteServiceProxyRegistrySnapshot::new(1, 1);
    assert_remote_service_proxy_count(&snapshot, 1);
    assert_remote_service_listing_count(&snapshot, 1);

    let mut polls = 0usize;
    let eventual = expect_remote_proxy_registry_snapshot(
        || {
            polls += 1;
            if polls > 2 {
                RemoteServiceProxyRegistrySnapshot::new(2, 1)
            } else {
                RemoteServiceProxyRegistrySnapshot::new(0, 0)
            }
        },
        2,
        1,
        Duration::from_secs(1),
    )
    .await
    .expect("remote proxy snapshot should converge");
    assert_remote_service_proxy_count(&eventual, 2);
    assert_remote_service_listing_count(&eventual, 1);
}

#[tokio::test]
async fn testkit_helpers_cover_phase_2_actor_context_surfaces() {
    let system = rakka_core::ActorSystem::new("phase-2-testkit");
    let mut events = TestProbe::<ActorContextProbeEvent>::spawn(&system, "events")
        .expect("event probe should spawn");
    let context = spawn_actor_context_probe(&system, "context", events.actor_ref())
        .expect("context probe should spawn");

    context
        .tell(ActorContextProbeCommand::StartTimer {
            key: "tick".to_owned(),
            delay: Duration::from_millis(10),
        })
        .expect("timer command should send");
    events
        .expect_message_eq(
            ActorContextProbeEvent::TimerFired("tick".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .expect("timer should fire");

    context
        .tell(ActorContextProbeCommand::EnableReceiveTimeout {
            delay: Duration::from_millis(10),
        })
        .expect("receive-timeout command should send");
    events
        .expect_message_eq(
            ActorContextProbeEvent::ReceiveTimeout,
            Duration::from_secs(1),
        )
        .await
        .expect("receive timeout should fire");

    let watched = spawn_stop_probe(&system, "watched").expect("watched actor should spawn");
    context
        .tell(ActorContextProbeCommand::WatchStopper {
            target: watched.clone(),
        })
        .expect("watch command should send");
    events
        .expect_message_eq(
            ActorContextProbeEvent::WatchRegistered,
            Duration::from_secs(1),
        )
        .await
        .expect("watch should register");
    watched
        .tell(StopProbeCommand::Stop)
        .expect("stop command should send");
    let observed = events
        .expect_message(Duration::from_secs(1))
        .await
        .expect("watch should observe termination");
    assert!(matches!(
        observed,
        ActorContextProbeEvent::WatchObserved(terminated)
            if terminated.path == watched.path().clone() && terminated.uid == watched.uid()
    ));

    let unwatched = spawn_stop_probe(&system, "unwatched").expect("unwatched actor should spawn");
    context
        .tell(ActorContextProbeCommand::WatchAndUnwatchStopper {
            target: unwatched.clone(),
        })
        .expect("watch/unwatch command should send");
    events
        .expect_message_eq(
            ActorContextProbeEvent::WatchCancelled,
            Duration::from_secs(1),
        )
        .await
        .expect("watch should cancel");
    unwatched
        .tell(StopProbeCommand::Stop)
        .expect("stop command should send");
    let _terminated = expect_terminated(&unwatched, Duration::from_secs(1))
        .await
        .expect("unwatched actor should terminate");
    events
        .expect_no_message(Duration::from_millis(50))
        .await
        .expect("unwatched termination should not be observed");

    let echo = spawn_echo_probe(&system, "echo").expect("echo probe should spawn");
    context
        .tell(ActorContextProbeCommand::AskEcho {
            target: echo,
            value: "pong".to_owned(),
            timeout: Duration::from_secs(1),
        })
        .expect("ask command should send");
    events
        .expect_message_eq(
            ActorContextProbeEvent::AskCompleted(Ok("pong".to_owned())),
            Duration::from_secs(1),
        )
        .await
        .expect("context ask should complete");

    context
        .tell(ActorContextProbeCommand::PipeValue {
            value: "done".to_owned(),
        })
        .expect("pipe command should send");
    events
        .expect_message_eq(
            ActorContextProbeEvent::PipeCompleted("done".to_owned()),
            Duration::from_secs(1),
        )
        .await
        .expect("pipe-to-self should complete");

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn persistence_testkit_bundles_phase_3_in_memory_stores() {
    let kit = PersistenceTestKit::<CounterEvent, CounterSnapshot, CounterSnapshot>::new();
    let id = PersistenceId::of("counter", "testkit").expect("persistence id should be valid");

    let journal = kit.journal();
    journal
        .append(
            &id,
            SequenceNr::INITIAL,
            vec![TaggedEvent::with_tags(
                CounterEvent::Incremented(3),
                ["counter"],
            )],
        )
        .await
        .expect("journal append should succeed");
    let tagged = journal
        .events_by_tag("counter")
        .await
        .expect("tag query should succeed");
    assert_eq!(tagged[0].event, CounterEvent::Incremented(3));

    let snapshots = kit.snapshots();
    snapshots
        .save(&id, SequenceNr::FIRST, CounterSnapshot { value: 3 })
        .await
        .expect("snapshot save should succeed");
    let snapshot = snapshots
        .load(&id, SnapshotSelection::latest())
        .await
        .expect("snapshot load should succeed")
        .expect("snapshot should exist");
    assert_eq!(snapshot.snapshot.value, 3);

    let durable_state = kit.durable_state();
    durable_state
        .compare_and_set(&id, Revision::INITIAL, CounterSnapshot { value: 4 })
        .await
        .expect("durable state write should succeed");
    assert_eq!(durable_state.persistence_ids().await.unwrap(), vec![id]);
}

#[test]
fn behavior_testkits_cover_phase_3_effects() {
    let event_sourced = EventSourcedBehavior::builder(
        PersistenceId::of("counter", "behavior-testkit").unwrap(),
        CounterSnapshot { value: 0 },
    )
    .on_command(|_state, command| match command {
        CounterTestCommand::Increment(by) => {
            EventSourcedEffect::persist(CounterEvent::Incremented(by))
        }
        CounterTestCommand::Snapshot => EventSourcedEffect::none().then_snapshot(),
        CounterTestCommand::Get => EventSourcedEffect::no_reply(),
        CounterTestCommand::Stop => EventSourcedEffect::stop(),
    })
    .on_event(|state, event| match event {
        CounterEvent::Incremented(by) => CounterSnapshot {
            value: state.value + by,
        },
    })
    .build()
    .expect("event-sourced behavior should build");
    let mut event_kit = EventSourcedBehaviorTestKit::new(event_sourced);

    let outcome = event_kit
        .run_command(CounterTestCommand::Increment(5))
        .expect("event command should run");
    assert_eq!(outcome.state.value, 5);
    assert_eq!(outcome.sequence_nr, SequenceNr::FIRST);
    let snapshot = event_kit
        .run_command(CounterTestCommand::Snapshot)
        .expect("snapshot command should run");
    assert!(snapshot.snapshot);
    assert_eq!(event_kit.snapshots()[0].value, 5);
    assert!(
        event_kit
            .run_command(CounterTestCommand::Stop)
            .expect("stop command should run")
            .stop
    );

    let durable = DurableStateBehavior::builder(
        PersistenceId::of("counter", "durable-testkit").unwrap(),
        CounterSnapshot { value: 0 },
    )
    .on_command(|state, command| match command {
        CounterTestCommand::Increment(by) => DurableEffect::persist(CounterSnapshot {
            value: state.value + by,
        }),
        CounterTestCommand::Snapshot | CounterTestCommand::Get => DurableEffect::none(),
        CounterTestCommand::Stop => DurableEffect::stop(),
    })
    .build()
    .expect("durable behavior should build");
    let mut durable_kit = DurableStateBehaviorTestKit::new(durable);

    let outcome = durable_kit
        .run_command(CounterTestCommand::Increment(2))
        .expect("durable command should run");
    assert_eq!(outcome.state.value, 2);
    assert_eq!(outcome.revision, Revision::new(1));
    assert_eq!(
        durable_kit
            .run_command(CounterTestCommand::Get)
            .expect("get command should run")
            .revision,
        Revision::new(1)
    );
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NumberRequest {
    value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NumberReply {
    value: i64,
}

#[derive(Debug)]
struct Phase5Command;

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    Incremented(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterTestCommand {
    Increment(i64),
    Snapshot,
    Get,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterSnapshot {
    value: i64,
}

fn cluster_node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
}
