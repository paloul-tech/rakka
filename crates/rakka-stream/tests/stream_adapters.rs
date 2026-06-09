//! Stream adapter behavior tests.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{actor_future, Actor, ActorAction, ActorContext, ActorOptions, ActorSystem};
use rakka_sharding::{
    EntityDeliveryFailure, EntityId, EntityRef, EntityTellError, EntityType, RoutedEntityMessage,
    ShardCoordinator, ShardRegion, ShardingConfig,
};
use rakka_stream::{
    bounded_channel, process_input_sink_from_writer, process_output_stream_from_reader,
    protocol_actor_process_stream_unsupported, spawn_actor_source, ActorSink, ActorSinkError,
    EntitySink, EntitySinkError, ProcessIoOwner, ProcessIoStream, ProcessOutputConfig,
    ProcessStreamError, StreamError,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, Notify};

#[tokio::test]
async fn actor_source_forwards_actor_messages_into_bounded_stream() {
    let system = ActorSystem::new("actor-source-stream-test");
    let source =
        spawn_actor_source::<String>(&system, "source", 1).expect("actor source should be created");

    source
        .actor()
        .tell("hello".to_owned())
        .expect("source actor should accept message");

    assert_eq!(
        source.source().next().await.expect("stream item"),
        Some("hello".to_owned())
    );

    source.actor().stop().expect("source actor should stop");
}

#[tokio::test]
async fn stream_source_drains_to_actor_sink_in_order() {
    let system = ActorSystem::new("actor-sink-stream-test");
    let (observed, mut receiver) = mpsc::unbounded_channel();
    let actor = system
        .spawn_actor("collector", CollectActor { observed })
        .expect("collector actor should spawn");
    let sink = ActorSink::new(actor);

    let (source_sink, source) = bounded_channel(2).expect("stream should be created");
    source_sink
        .try_send("one".to_owned())
        .expect("first source item should fit");
    source_sink
        .try_send("two".to_owned())
        .expect("second source item should fit");
    source_sink.drain().expect("source should drain");

    let delivered = sink
        .drain_from(&source)
        .await
        .expect("actor drain should complete");

    assert_eq!(delivered, 2);
    assert_eq!(receiver.recv().await, Some("one".to_owned()));
    assert_eq!(receiver.recv().await, Some("two".to_owned()));
}

#[tokio::test]
async fn actor_sink_surfaces_full_and_closed_mailboxes() {
    let system = ActorSystem::new("actor-sink-pressure-test");
    let release = Arc::new(Notify::new());
    let (entered, mut entered_rx) = mpsc::unbounded_channel();
    let release_for_factory = Arc::clone(&release);
    let entered_for_factory = entered.clone();
    let slow_actor = system
        .spawn_actor_with_options(
            "slow",
            move || BlockingActor {
                entered: entered_for_factory.clone(),
                release: Arc::clone(&release_for_factory),
            },
            ActorOptions::default().with_mailbox_capacity(1),
        )
        .expect("slow actor should spawn");

    slow_actor
        .tell("held".to_owned())
        .expect("first message should start actor");
    entered_rx
        .recv()
        .await
        .expect("actor should enter first handler");
    slow_actor
        .tell("queued".to_owned())
        .expect("second message should fill mailbox");

    let sink = ActorSink::new(slow_actor.clone());
    let full = sink
        .try_send("full".to_owned())
        .expect_err("third message should observe full mailbox");
    assert!(matches!(full, ActorSinkError::MailboxFull { .. }));
    assert_eq!(full.message(), Some(&"full".to_owned()));

    release.notify_waiters();

    let stopped_actor = system
        .spawn_actor("stopped", NoopActor)
        .expect("stopped actor should spawn");
    stopped_actor.stop().expect("actor should accept stop");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let closed = ActorSink::new(stopped_actor)
        .try_send("closed".to_owned())
        .expect_err("stopped actor should reject messages");
    assert!(matches!(closed, ActorSinkError::MailboxClosed { .. }));
    assert_eq!(closed.message(), Some(&"closed".to_owned()));
}

#[test]
fn entity_sink_surfaces_missing_owner_without_losing_message() {
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let region = ShardRegion::new(
        entity_type.clone(),
        config,
        |_message: RoutedEntityMessage<String>| unreachable!("route should not run without owner"),
    );
    let entity = EntityRef::new(entity_type, EntityId::new("cart-1"));

    let error = EntitySink::new(region, entity)
        .try_send("add-item".to_owned())
        .expect_err("entity should have no owner");

    assert!(matches!(error, EntitySinkError::NoRoute { .. }));
    assert_eq!(error.message(), Some(&"add-item".to_owned()));
}

#[test]
fn entity_sink_routes_to_region_and_surfaces_delivery_failure() {
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).expect("valid shard config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    coordinator.reconcile(&membership);

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_route = Arc::clone(&delivered);
    let region = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &coordinator.snapshot(),
        move |message: RoutedEntityMessage<String>| {
            delivered_for_route
                .lock()
                .expect("delivery mutex should not poison")
                .push((message.entity_id().clone(), message.into_message()));
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");

    EntitySink::new(region, entity)
        .try_send("add-item".to_owned())
        .expect("entity delivery should succeed");
    assert_eq!(
        delivered.lock().expect("delivery mutex should not poison")[0],
        (EntityId::new("cart-1"), "add-item".to_owned())
    );

    let failing_region = ShardRegion::from_snapshot(
        entity_type.clone(),
        config,
        &coordinator.snapshot(),
        |message: RoutedEntityMessage<String>| {
            Err(EntityTellError::Delivery {
                message: message.into_message(),
                failure: EntityDeliveryFailure::MailboxFull,
            })
        },
    )
    .expect("failing region should accept ownership snapshot");
    let failing_entity = failing_region.entity_ref("cart-1");

    let failure = EntitySink::new(failing_region, failing_entity)
        .try_send("full".to_owned())
        .expect_err("route should surface mailbox pressure");
    assert!(matches!(failure, EntitySinkError::Delivery { .. }));
    assert_eq!(failure.message(), Some(&"full".to_owned()));
}

#[tokio::test]
async fn process_output_stream_completes_on_eof() {
    let (mut writer, reader) = tokio::io::duplex(64);
    let mut output = process_output_stream_from_reader(
        reader,
        ProcessIoStream::Stdout,
        ProcessOutputConfig::new(2).chunk_size(64),
    )
    .expect("output stream should be created");

    writer
        .write_all(b"hello")
        .await
        .expect("duplex writer should accept bytes");
    drop(writer);

    assert_eq!(
        output.source().next().await.expect("stdout chunk"),
        Some(b"hello".to_vec())
    );
    assert_eq!(output.source().next().await.expect("stdout eof"), None);
    assert_eq!(output.join().await.expect("pump should finish"), 5);
}

#[tokio::test]
async fn process_output_stream_reports_read_error_and_cancels_source() {
    let mut output = process_output_stream_from_reader(
        FailingReader,
        ProcessIoStream::Stderr,
        ProcessOutputConfig::new(1).chunk_size(8),
    )
    .expect("output stream should be created");

    let stream_error = output
        .source()
        .next()
        .await
        .expect_err("source should observe cancellation");
    assert!(matches!(stream_error, StreamError::Cancelled { .. }));

    let pump_error = output
        .join()
        .await
        .expect_err("pump should report read error");
    assert!(matches!(
        pump_error,
        ProcessStreamError::Read {
            stream: ProcessIoStream::Stderr,
            ..
        }
    ));
}

#[tokio::test]
async fn process_output_cancel_aborts_reader_and_marks_source_cancelled() {
    let (mut writer, reader) = tokio::io::duplex(64);
    let mut output = process_output_stream_from_reader(
        reader,
        ProcessIoStream::Stdout,
        ProcessOutputConfig::new(1).chunk_size(8),
    )
    .expect("output stream should be created");

    assert_eq!(output.cancel("consumer cancelled"), 0);
    assert_eq!(
        output
            .source()
            .next()
            .await
            .expect_err("source should observe cancellation"),
        StreamError::Cancelled {
            reason: Some("consumer cancelled".to_owned())
        }
    );
    assert!(matches!(
        output
            .join()
            .await
            .expect_err("aborted pump should report join"),
        ProcessStreamError::PumpJoin { .. }
    ));
    assert!(
        writer.write_all(b"after-cancel").await.is_err(),
        "dropping the read half should close the pipe"
    );
}

#[tokio::test]
async fn process_input_sink_drains_source_and_closes_writer() {
    let (writer, mut reader) = tokio::io::duplex(64);
    let mut input = process_input_sink_from_writer(writer, ProcessIoStream::Stdin);
    let (sink, source) = bounded_channel(2).expect("source stream should be created");

    sink.try_send(b"hello ".to_vec())
        .expect("first stdin chunk should fit");
    sink.try_send(b"world".to_vec())
        .expect("second stdin chunk should fit");
    sink.drain().expect("stdin source should drain");

    let drain = tokio::spawn(async move { input.drain_from(&source).await });
    let mut observed = Vec::new();
    reader
        .read_to_end(&mut observed)
        .await
        .expect("reader should observe writer close");

    assert_eq!(
        drain
            .await
            .expect("drain task should finish")
            .expect("stdin drain should succeed"),
        2
    );
    assert_eq!(observed, b"hello world");
}

#[test]
fn protocol_actor_owned_process_pipes_return_typed_unsupported_error() {
    let error = protocol_actor_process_stream_unsupported(ProcessIoStream::Stdout)
        .expect_err("protocol actors own their stdio pipes");

    assert_eq!(
        error,
        ProcessStreamError::UnsupportedOwner {
            stream: ProcessIoStream::Stdout,
            owner: ProcessIoOwner::ProtocolActor
        }
    );
}

struct CollectActor {
    observed: mpsc::UnboundedSender<String>,
}

impl Actor for CollectActor {
    type Msg = String;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            let _sent = self.observed.send(msg);
            Ok(ActorAction::Continue)
        })
    }
}

struct BlockingActor {
    entered: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

impl Actor for BlockingActor {
    type Msg = String;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            let _sent = self.entered.send(());
            self.release.notified().await;
            Ok(ActorAction::Continue)
        })
    }
}

struct NoopActor;

impl Actor for NoopActor {
    type Msg = String;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }
}

struct FailingReader;

impl AsyncRead for FailingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "synthetic read failure",
        )))
    }
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
