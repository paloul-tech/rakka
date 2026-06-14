//! Stream facade vocabulary tests.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorSystem,
};
use rakka_sharding::{
    EntityDeliveryFailure, EntityId, EntityRef, EntityTellError, EntityType, RoutedEntityMessage,
    ShardBufferConfig, ShardCoordinator, ShardRegion, ShardedEntityRef, ShardingConfig,
};
use rakka_stream::{
    bounded_channel, AckProtocol, ActorSinkMessage, ActorSourceMessage, ActorStreamError, Flow,
    Sink, Source, StreamError, StreamRunError, StreamRunSettings, DEFAULT_BUFFER_CAPACITY,
};
use tokio::sync::{mpsc, Notify};

#[test]
fn stream_run_settings_defaults_are_bounded_and_named_later() {
    let settings = StreamRunSettings::default();

    assert_eq!(settings.default_buffer_capacity(), DEFAULT_BUFFER_CAPACITY);
    assert_eq!(settings.operator_buffer_capacity(), DEFAULT_BUFFER_CAPACITY);
    assert_eq!(settings.stream_name(), None);
    assert_eq!(settings.cancellation_reason(), "stream cancelled");

    let named = settings
        .with_stream_name("orders")
        .with_cancellation_reason("orders stream cancelled");
    assert_eq!(named.stream_name(), Some("orders"));
    assert_eq!(named.cancellation_reason(), "orders stream cancelled");
    assert_eq!(named.without_stream_name().stream_name(), None);
}

#[test]
fn stream_run_settings_reject_zero_capacities() {
    assert_eq!(
        StreamRunSettings::new(0, 1).unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
    assert_eq!(
        StreamRunSettings::new(1, 0).unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
    assert_eq!(
        StreamRunSettings::default()
            .with_default_buffer_capacity(0)
            .unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
    assert_eq!(
        StreamRunSettings::default()
            .with_operator_buffer_capacity(0)
            .unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
}

#[test]
fn facade_vocabulary_constructs_without_materializing() {
    let settings = StreamRunSettings::new(8, 4)
        .expect("valid settings")
        .with_stream_name("facade-test");
    let source = Source::<u64>::empty().with_settings(settings.clone());
    let flow = Flow::<u64, u64>::identity().with_settings(settings.clone());
    let sink = Sink::<u64, ()>::ignore().with_settings(settings.clone());

    assert!(source.is_empty());
    assert!(flow.is_identity());
    assert!(sink.is_ignore());
    assert_eq!(source.settings().stream_name(), Some("facade-test"));
    assert_eq!(flow.settings().operator_buffer_capacity(), 4);
    assert_eq!(sink.settings().default_buffer_capacity(), 8);

    let runnable = source.to(sink);
    assert_eq!(
        runnable.source_settings().stream_name(),
        Some("facade-test")
    );
    assert_eq!(runnable.sink_settings().stream_name(), Some("facade-test"));
}

#[tokio::test]
async fn source_constructors_materialize_to_collect_sink() {
    assert_eq!(
        Source::<u64>::empty().run_collect().await.unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(Source::single(7).run_collect().await.unwrap(), vec![7]);
    assert_eq!(
        Source::from_iter([1, 2, 3]).run_collect().await.unwrap(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn sink_foreach_and_fold_materialize_results() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_for_sink = Arc::clone(&observed);
    Source::from_iter([1, 2, 3])
        .run_foreach(move |item| {
            observed_for_sink
                .lock()
                .expect("observed mutex should not poison")
                .push(item);
        })
        .await
        .unwrap();
    assert_eq!(
        *observed.lock().expect("observed mutex should not poison"),
        vec![1, 2, 3]
    );

    let sum = Source::from_iter([1, 2, 3])
        .run_with(Sink::fold(0, |sum, item| sum + item))
        .await
        .unwrap();
    assert_eq!(sum, 6);
}

#[tokio::test]
async fn runnable_stream_runs_connected_source_and_sink() {
    let runnable = Source::from_iter(["a".to_owned(), "b".to_owned()]).to(Sink::collect());

    assert_eq!(
        runnable.run().await.unwrap(),
        vec!["a".to_owned(), "b".to_owned()]
    );
}

#[tokio::test]
async fn source_and_sink_wrap_low_level_bounded_primitives() {
    let (input_sink, input_source) = bounded_channel(2).unwrap();
    input_sink.try_send("one".to_owned()).unwrap();
    input_sink.try_send("two".to_owned()).unwrap();
    input_sink.drain().unwrap();

    assert_eq!(
        Source::from_stream_source(input_source)
            .run_collect()
            .await
            .unwrap(),
        vec!["one".to_owned(), "two".to_owned()]
    );

    let (output_sink, output_source) = bounded_channel(2).unwrap();
    let forwarded = Source::from_iter(["three".to_owned(), "four".to_owned()])
        .run_with(Sink::from_stream_sink(output_sink.clone()))
        .await
        .unwrap();
    output_sink.drain().unwrap();

    assert_eq!(forwarded, 2);
    assert_eq!(
        output_source.next().await.unwrap(),
        Some("three".to_owned())
    );
    assert_eq!(output_source.next().await.unwrap(), Some("four".to_owned()));
    assert_eq!(output_source.next().await.unwrap(), None);
}

#[tokio::test]
async fn facade_run_errors_preserve_source_and_sink_lifecycle() {
    let (sink, source) = bounded_channel::<u64>(1).unwrap();
    sink.cancel("source cancelled");
    let source_error = Source::from_stream_source(source)
        .run_collect()
        .await
        .unwrap_err();
    assert!(matches!(
        source_error,
        StreamRunError::Source {
            error: StreamError::Cancelled { .. }
        }
    ));

    let (closed_sink, _closed_source) = bounded_channel(1).unwrap();
    closed_sink.close();
    let sink_error = Source::single(9)
        .run_with(Sink::from_stream_sink(closed_sink))
        .await
        .unwrap_err();
    assert_eq!(sink_error.code(), "sink-error");
    assert!(matches!(
        sink_error.sink_error().map(|error| error.error()),
        Some(StreamError::Closed)
    ));
}

#[tokio::test]
async fn linear_operators_compose_and_preserve_order() {
    let result = Source::from_iter([1, 2, 3, 4])
        .map(|item| item * 2)
        .filter(|item| *item > 4)
        .take(2)
        .run_collect()
        .await
        .unwrap();

    assert_eq!(result, vec![6, 8]);
}

#[tokio::test]
async fn flow_identity_and_from_fn_apply_through_via() {
    let identity = Source::from_iter([1, 2, 3])
        .via(Flow::identity())
        .run_collect()
        .await
        .unwrap();
    assert_eq!(identity, vec![1, 2, 3]);

    let mapped = Source::from_iter([1, 2, 3])
        .via(Flow::from_fn(|item| format!("item-{item}")))
        .run_collect()
        .await
        .unwrap();
    assert_eq!(
        mapped,
        vec![
            "item-1".to_owned(),
            "item-2".to_owned(),
            "item-3".to_owned()
        ]
    );
}

#[tokio::test]
async fn take_zero_completes_without_waiting_and_cancels_upstream_queue() {
    let (sink, source) = bounded_channel(1).unwrap();
    sink.try_send("buffered".to_owned()).unwrap();
    let pending_send = tokio::spawn({
        let sink = sink.clone();
        async move { sink.send("blocked".to_owned()).await }
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !pending_send.is_finished(),
        "sender should wait while the source buffer is full"
    );

    let collected = Source::from_stream_source(source)
        .take(0)
        .run_collect()
        .await
        .unwrap();
    assert!(collected.is_empty());

    let send_error = pending_send
        .await
        .expect("pending send task should finish")
        .expect_err("take(0) should cancel the upstream source");
    assert!(matches!(
        send_error.error(),
        StreamError::Cancelled { reason: Some(reason) }
            if reason == "take(0) completed"
    ));
    assert_eq!(send_error.item(), "blocked");
    assert_eq!(sink.status().dropped_items(), 1);
}

#[tokio::test]
async fn stream_facade_async_map_async_one_behaves_like_sequential_map() {
    let values = Source::from_iter([1, 2, 3])
        .map_async(1, |item| async move { item * 2 })
        .unwrap()
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![2, 4, 6]);
}

#[tokio::test]
async fn stream_facade_async_map_async_preserves_order_when_futures_complete_out_of_order() {
    let values = Source::from_iter([1_u64, 2, 3])
        .map_async(3, |item| async move {
            tokio::time::sleep(Duration::from_millis((4 - item) * 10)).await;
            item
        })
        .unwrap()
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![1, 2, 3]);
}

#[tokio::test]
async fn stream_facade_async_map_async_rejects_zero_parallelism() {
    let error = Source::from_iter([1])
        .map_async(0, |item| async move { item })
        .unwrap_err();

    assert_eq!(error.code(), "operator-error");
    assert!(matches!(
        error,
        StreamError::Operator { message }
            if message == "map_async parallelism must be greater than zero"
    ));
}

#[tokio::test]
async fn stream_facade_async_map_async_limits_in_flight_work_to_parallelism() {
    let current = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));

    let values = Source::from_iter([1, 2, 3, 4])
        .map_async(2, {
            let current = Arc::clone(&current);
            let max = Arc::clone(&max);
            move |item| {
                let current = Arc::clone(&current);
                let max = Arc::clone(&max);
                async move {
                    let in_flight = current.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(in_flight, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    item
                }
            }
        })
        .unwrap()
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![1, 2, 3, 4]);
    assert_eq!(max.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stream_facade_async_flow_from_async_fn_applies_through_via() {
    let flow = Flow::from_async_fn(2, |item| async move { format!("item-{item}") }).unwrap();
    let values = Source::from_iter([1, 2, 3])
        .via(flow)
        .run_collect()
        .await
        .unwrap();

    assert_eq!(
        values,
        vec![
            "item-1".to_owned(),
            "item-2".to_owned(),
            "item-3".to_owned()
        ]
    );
}

#[tokio::test]
async fn stream_facade_async_map_async_task_failure_surfaces_operator_error() {
    let error = Source::from_iter([1_u64])
        .map_async(1, |item| async move {
            if item == 1 {
                panic!("boom");
            }
            item
        })
        .unwrap()
        .run_collect()
        .await
        .unwrap_err();

    assert_eq!(error.code(), "source-error");
    assert!(matches!(
        error.source_error(),
        Some(StreamError::Operator { message }) if message.contains("map_async task failed")
    ));
}

#[tokio::test]
async fn stream_facade_async_take_cancels_in_flight_map_async_tasks() {
    struct DropGuard(Arc<AtomicUsize>);

    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let never_release = Arc::new(tokio::sync::Notify::new());

    let values = Source::from_iter([1, 2])
        .map_async(2, {
            let dropped = Arc::clone(&dropped);
            let never_release = Arc::clone(&never_release);
            move |item| {
                let dropped = Arc::clone(&dropped);
                let never_release = Arc::clone(&never_release);
                async move {
                    if item == 2 {
                        let _guard = DropGuard(dropped);
                        never_release.notified().await;
                    }
                    item
                }
            }
        })
        .unwrap()
        .take(1)
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![1]);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while dropped.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_facade_fanout_merge_forwards_all_items_from_both_sources() {
    let mut values = Source::from_iter([1, 2])
        .merge(Source::from_iter([3, 4]))
        .run_collect()
        .await
        .unwrap();

    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn stream_facade_fanout_merge_all_empty_completes() {
    let values = Source::<u64>::merge_all(Vec::new())
        .run_collect()
        .await
        .unwrap();

    assert!(values.is_empty());
}

#[tokio::test]
async fn stream_facade_fanout_merge_surfaces_source_failure() {
    let (sink, failed_source) = bounded_channel::<u64>(1).unwrap();
    sink.cancel("merge input cancelled");

    let error = Source::from_iter([1])
        .merge(Source::from_stream_source(failed_source))
        .run_collect()
        .await
        .unwrap_err();

    assert!(matches!(
        error.source_error(),
        Some(StreamError::Cancelled {
            reason: Some(reason)
        }) if reason == "merge input cancelled"
    ));
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_rejects_zero_branches() {
    let error = Source::from_iter([1])
        .broadcast(0)
        .expect_err("zero broadcast branches should be rejected");

    assert_eq!(error.code(), "operator-error");
    assert!(matches!(
        error,
        StreamError::Operator { message }
            if message == "broadcast branch count must be greater than zero"
    ));
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_forwards_each_item_to_each_branch() {
    let mut branches = Source::from_iter([1, 2, 3]).broadcast(2).unwrap();
    let right = branches.pop().expect("right branch");
    let left = branches.pop().expect("left branch");

    let (left, right) = tokio::join!(left.run_collect(), right.run_collect());

    assert_eq!(left.unwrap(), vec![1, 2, 3]);
    assert_eq!(right.unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_backpressures_on_full_live_branch() {
    let settings = StreamRunSettings::new(8, 1).expect("valid stream settings");
    let mut branches = Source::from_iter([1, 2])
        .with_settings(settings)
        .broadcast(2)
        .unwrap();
    let slow = branches.pop().expect("slow branch");
    let fast = branches.pop().expect("fast branch");

    let fast_run = tokio::spawn(async move { fast.run_collect().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !fast_run.is_finished(),
        "fast branch should wait for slow live branch capacity"
    );

    let slow_run = tokio::spawn(async move { slow.run_collect().await.unwrap() });
    let (fast_values, slow_values) = tokio::join!(fast_run, slow_run);

    assert_eq!(fast_values.expect("fast task should finish"), vec![1, 2]);
    assert_eq!(slow_values.expect("slow task should finish"), vec![1, 2]);
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_cancelled_branch_drops_out() {
    let settings = StreamRunSettings::new(8, 1).expect("valid stream settings");
    let mut branches = Source::from_iter([1, 2, 3])
        .with_settings(settings)
        .broadcast(2)
        .unwrap();
    let mut cancelled = branches.pop().expect("cancelled branch");
    let live = branches.pop().expect("live branch");

    cancelled.cancel("branch no longer needed");

    assert_eq!(live.run_collect().await.unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn entity_facade_sink_routes_items_to_entity_in_order() {
    let entity_type = EntityType::new("FacadeCart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership_with_up_nodes(vec![node("rakka-0", "uid-a")]));

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_route = Arc::clone(&delivered);
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        move |message: RoutedEntityMessage<String>| {
            delivered_for_route
                .lock()
                .expect("delivered mutex should not poison")
                .push((message.entity_id().clone(), message.into_message()));
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");

    let count = Source::from_iter(["add-1".to_owned(), "add-2".to_owned()])
        .run_with(Sink::entity_ref(region, entity))
        .await
        .unwrap();

    assert_eq!(count, 2);
    assert_eq!(
        *delivered.lock().expect("delivered mutex should not poison"),
        vec![
            (EntityId::new("cart-1"), "add-1".to_owned()),
            (EntityId::new("cart-1"), "add-2".to_owned())
        ]
    );
}

#[tokio::test]
async fn entity_facade_sink_surfaces_no_route_without_losing_item() {
    let entity_type = EntityType::new("FacadeNoRouteCart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let region = ShardRegion::new(
        entity_type.clone(),
        config,
        |_message: RoutedEntityMessage<String>| unreachable!("route should not run without owner"),
    );
    let entity = EntityRef::new(entity_type, EntityId::new("cart-1"));

    let error = Source::single("add-item".to_owned())
        .run_with(Sink::entity_ref(region, entity))
        .await
        .unwrap_err();

    assert_eq!(error.code(), "entity-error");
    assert!(matches!(
        error.entity_error(),
        Some(rakka_stream::EntitySinkError::NoRoute { message, .. })
            if message == "add-item"
    ));
}

#[tokio::test]
async fn entity_facade_sink_surfaces_delivery_failure_without_losing_item() {
    let entity_type = EntityType::new("FacadeFailingCart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership_with_up_nodes(vec![node("rakka-0", "uid-a")]));
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        |message: RoutedEntityMessage<String>| {
            Err(EntityTellError::Delivery {
                message: message.into_message(),
                failure: EntityDeliveryFailure::MailboxFull,
            })
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");

    let error = Source::single("full".to_owned())
        .run_with(Sink::entity_ref(region, entity))
        .await
        .unwrap_err();

    assert!(matches!(
        error.entity_error(),
        Some(rakka_stream::EntitySinkError::Delivery {
            message,
            failure: EntityDeliveryFailure::MailboxFull
        }) if message == "full"
    ));
}

#[tokio::test]
async fn entity_facade_sink_accepts_sharded_entity_ref_convenience() {
    let entity_type = EntityType::new("FacadeShardedRefCart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership_with_up_nodes(vec![node("rakka-0", "uid-a")]));

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_route = Arc::clone(&delivered);
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        move |message: RoutedEntityMessage<String>| {
            delivered_for_route
                .lock()
                .expect("delivered mutex should not poison")
                .push(message.into_message());
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = ShardedEntityRef::new(region.entity_ref("cart-1"), region);

    let count = Source::single("via-ref".to_owned())
        .run_with(Sink::sharded_entity_ref(entity))
        .await
        .unwrap();

    assert_eq!(count, 1);
    assert_eq!(
        *delivered.lock().expect("delivered mutex should not poison"),
        vec!["via-ref".to_owned()]
    );
}

#[tokio::test]
async fn entity_facade_sink_respects_passivation_buffering() {
    let entity_type = EntityType::new("FacadePassivatingCart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership_with_up_nodes(vec![node("rakka-0", "uid-a")]));

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_route = Arc::clone(&delivered);
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        move |message: RoutedEntityMessage<String>| {
            delivered_for_route
                .lock()
                .expect("delivered mutex should not poison")
                .push(message.into_message());
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot")
    .with_buffering(ShardBufferConfig::new(8, Duration::from_secs(1)));
    let entity = region.entity_ref("cart-1");

    region.begin_entity_passivation(entity.entity_id().clone(), Duration::from_secs(1));
    let count = Source::single("after-passivate".to_owned())
        .run_with(Sink::entity_ref(region.clone(), entity.clone()))
        .await
        .unwrap();

    assert_eq!(count, 1);
    assert_eq!(
        region.buffered_message_count_for_shard(entity.shard_id(region.config())),
        1
    );
    assert!(delivered
        .lock()
        .expect("delivered mutex should not poison")
        .is_empty());

    region.end_entity_passivation(entity.entity_id());
    assert_eq!(
        *delivered.lock().expect("delivered mutex should not poison"),
        vec!["after-passivate".to_owned()]
    );
}

#[tokio::test]
async fn actor_ack_sink_delivers_items_in_order_when_acks_arrive() {
    let system = ActorSystem::new("actor-ack-sink-delivery");
    let (events, mut receiver) = mpsc::channel(8);
    let actor = system
        .spawn_actor(
            "sink",
            AckingSinkActor {
                events,
                ack_init: true,
                ack_elements: true,
            },
        )
        .unwrap();

    let delivered = Source::from_iter([1_u64, 2])
        .run_with(Sink::actor_ref_with_ack(
            actor,
            AckProtocol::new("ack").with_timeout(Duration::from_secs(1)),
        ))
        .await
        .unwrap();

    assert_eq!(delivered, 2);
    assert_eq!(recv_event(&mut receiver).await, AckSinkEvent::Init);
    assert_eq!(recv_event(&mut receiver).await, AckSinkEvent::Element(1));
    assert_eq!(recv_event(&mut receiver).await, AckSinkEvent::Element(2));
    assert_eq!(recv_event(&mut receiver).await, AckSinkEvent::Complete);
    system.shutdown();
}

#[tokio::test]
async fn actor_ack_sink_does_not_overrun_missing_ack() {
    let system = ActorSystem::new("actor-ack-sink-timeout");
    let (events, mut receiver) = mpsc::channel(8);
    let actor = system
        .spawn_actor(
            "sink",
            AckingSinkActor {
                events,
                ack_init: true,
                ack_elements: false,
            },
        )
        .unwrap();

    let error = Source::from_iter([1_u64, 2])
        .run_with(Sink::actor_ref_with_ack(
            actor,
            AckProtocol::new("ack").with_timeout(Duration::from_millis(25)),
        ))
        .await
        .unwrap_err();

    assert!(matches!(
        error.actor_error(),
        Some(ActorStreamError::AckTimeout { .. } | ActorStreamError::AckDropped)
    ));
    assert_eq!(recv_event(&mut receiver).await, AckSinkEvent::Init);
    assert_eq!(recv_event(&mut receiver).await, AckSinkEvent::Element(1));
    if let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), receiver.recv()).await
    {
        assert!(matches!(event, AckSinkEvent::Cancelled(_)));
    }
    system.shutdown();
}

#[tokio::test]
async fn actor_ack_sink_preserves_item_when_actor_mailbox_is_full() {
    let system = ActorSystem::new("actor-sink-mailbox-full");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_wait = entered.notified();
    let actor = system
        .spawn_actor_with_options(
            "blocking",
            {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move || BlockingSinkActor {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }
            },
            ActorOptions::default().with_mailbox_capacity(1),
        )
        .unwrap();

    actor.tell(BlockingSinkMessage::Block).unwrap();
    entered_wait.await;

    let error = Source::from_iter([BlockingSinkMessage::Queued, BlockingSinkMessage::Extra])
        .run_with(Sink::actor_ref(actor))
        .await
        .unwrap_err();

    assert!(matches!(
        error.actor_error(),
        Some(ActorStreamError::MailboxFull {
            item: BlockingSinkMessage::Extra
        })
    ));

    release.notify_waiters();
    system.shutdown();
}

#[tokio::test]
async fn actor_ack_source_exposes_actor_ref_and_bounded_source() {
    let system = ActorSystem::new("actor-source-facade");
    let (actor, source) = Source::actor_ref(&system, "source", 2).unwrap();

    actor.tell(1_u64).unwrap();
    actor.tell(2_u64).unwrap();

    assert_eq!(source.take(2).run_collect().await.unwrap(), vec![1, 2]);
    system.shutdown();
}

#[tokio::test]
async fn actor_ack_source_replies_after_bounded_capacity_accepts_item() {
    let system = ActorSystem::new("actor-source-ack");
    let (actor, source) =
        Source::actor_ref_with_ack(&system, "source", 1, AckProtocol::new("ack")).unwrap();

    let first = actor
        .ask(
            |reply_to| ActorSourceMessage::Element {
                item: 1_u64,
                reply_to,
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(first, "ack");

    let second = tokio::spawn({
        let actor = actor.clone();
        async move {
            actor
                .ask(
                    |reply_to| ActorSourceMessage::Element {
                        item: 2_u64,
                        reply_to,
                    },
                    Duration::from_secs(1),
                )
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !second.is_finished(),
        "second ack should wait while source buffer is full"
    );

    let collected = source.take(2).run_collect().await.unwrap();
    assert_eq!(second.await.unwrap(), "ack");
    assert_eq!(collected, vec![1, 2]);
    system.shutdown();
}

#[tokio::test]
async fn actor_ack_sink_receives_failure_signal_when_upstream_fails() {
    let system = ActorSystem::new("actor-ack-sink-failure-signal");
    let (events, mut receiver) = mpsc::channel(8);
    let actor = system
        .spawn_actor(
            "sink",
            AckingSinkActor {
                events,
                ack_init: true,
                ack_elements: true,
            },
        )
        .unwrap();
    let (sink, source) = bounded_channel::<u64>(1).unwrap();
    sink.cancel("upstream cancelled");

    let error = Source::from_stream_source(source)
        .run_with(Sink::actor_ref_with_ack(actor, AckProtocol::new("ack")))
        .await
        .unwrap_err();

    assert!(matches!(
        error.source_error(),
        Some(StreamError::Cancelled {
            reason: Some(reason)
        }) if reason == "upstream cancelled"
    ));
    assert_eq!(
        recv_event(&mut receiver).await,
        AckSinkEvent::Failure("cancelled".to_owned())
    );
    system.shutdown();
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AckSinkEvent {
    Init,
    Element(u64),
    Complete,
    Failure(String),
    Cancelled(String),
}

struct AckingSinkActor {
    events: mpsc::Sender<AckSinkEvent>,
    ack_init: bool,
    ack_elements: bool,
}

impl Actor for AckingSinkActor {
    type Msg = ActorSinkMessage<u64, &'static str>;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let events = self.events.clone();
        let ack_init = self.ack_init;
        let ack_elements = self.ack_elements;
        actor_future(async move {
            match msg {
                ActorSinkMessage::Init { reply_to } => {
                    events.send(AckSinkEvent::Init).await.unwrap();
                    if ack_init {
                        let _ignored = reply_to.reply("ack");
                    }
                }
                ActorSinkMessage::Element { item, reply_to } => {
                    events.send(AckSinkEvent::Element(item)).await.unwrap();
                    if ack_elements {
                        let _ignored = reply_to.reply("ack");
                    }
                }
                ActorSinkMessage::Complete => {
                    events.send(AckSinkEvent::Complete).await.unwrap();
                }
                ActorSinkMessage::Failure { error } => {
                    events
                        .send(AckSinkEvent::Failure(error.code().to_owned()))
                        .await
                        .unwrap();
                }
                ActorSinkMessage::Cancelled { reason } => {
                    events.send(AckSinkEvent::Cancelled(reason)).await.unwrap();
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockingSinkMessage {
    Block,
    Queued,
    Extra,
}

struct BlockingSinkActor {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for BlockingSinkActor {
    type Msg = BlockingSinkMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        actor_future(async move {
            if matches!(msg, BlockingSinkMessage::Block) {
                entered.notify_one();
                release.notified().await;
            }
            Ok(ActorAction::Continue)
        })
    }
}

async fn recv_event(receiver: &mut mpsc::Receiver<AckSinkEvent>) -> AckSinkEvent {
    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("event should arrive")
        .expect("event sender should stay open")
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}

fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
    let local = nodes[0].clone();
    let mut membership = ClusterMembership::new(
        local,
        MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
    );

    membership
        .record_discovery(DiscoverySnapshot::new("test", 1, nodes))
        .expect("discovery should be accepted");

    for member in membership
        .members()
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>()
    {
        membership
            .mark_up(&member, 2)
            .expect("member should transition up");
    }

    membership
}
