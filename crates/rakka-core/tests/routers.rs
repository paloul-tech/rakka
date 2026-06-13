//! Local router integration tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorSystem,
    PoolRouterTellError, Routers,
};
use tokio::sync::{mpsc, Notify};

#[derive(Debug)]
enum RecordCommand {
    Record { sequence: usize },
    Stop,
}

struct RecordingWorker {
    id: usize,
    delivered: mpsc::UnboundedSender<(usize, usize)>,
}

impl Actor for RecordingWorker {
    type Msg = RecordCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let id = self.id;
        let delivered = self.delivered.clone();
        actor_future(async move {
            match msg {
                RecordCommand::Record { sequence } => {
                    let _ = delivered.send((sequence, id));
                    Ok(ActorAction::Continue)
                }
                RecordCommand::Stop => Ok(ActorAction::Stop),
            }
        })
    }
}

#[derive(Debug)]
enum BlockingCommand {
    Block,
    Queued,
    Extra,
}

struct BlockingWorker {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for BlockingWorker {
    type Msg = BlockingCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        actor_future(async move {
            if matches!(msg, BlockingCommand::Block) {
                entered.notify_one();
                release.notified().await;
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn pool_router_spawns_configured_routees() {
    let system = ActorSystem::new("pool-spawn");
    let (delivered, _received) = mpsc::unbounded_channel();
    let next_id = Arc::new(AtomicUsize::new(0));

    let router = Routers::pool("workers", 4, {
        let next_id = next_id.clone();
        move || RecordingWorker {
            id: next_id.fetch_add(1, Ordering::SeqCst),
            delivered: delivered.clone(),
        }
    })
    .spawn(&system)
    .expect("pool router should spawn");

    assert_eq!(router.name(), "workers");
    assert_eq!(router.routee_count(), 4);
    assert!(router
        .routees()
        .iter()
        .all(|routee| routee.path().as_str().contains("/user/workers-")));

    system.shutdown();
}

#[tokio::test]
async fn round_robin_pool_routes_deterministically() {
    let system = ActorSystem::new("pool-round-robin");
    let (delivered, mut received) = mpsc::unbounded_channel();
    let next_id = Arc::new(AtomicUsize::new(0));

    let router = Routers::pool("workers", 3, {
        let next_id = next_id.clone();
        move || RecordingWorker {
            id: next_id.fetch_add(1, Ordering::SeqCst),
            delivered: delivered.clone(),
        }
    })
    .with_round_robin()
    .spawn(&system)
    .expect("pool router should spawn");

    for sequence in 0..6 {
        router
            .tell(RecordCommand::Record { sequence })
            .expect("message should route");
    }

    let observed = receive_records(&mut received, 6).await;
    assert_eq!(
        observed,
        vec![(0, 0), (1, 1), (2, 2), (3, 0), (4, 1), (5, 2)]
    );

    system.shutdown();
}

#[tokio::test]
async fn random_pool_does_not_select_terminated_routees() {
    let system = ActorSystem::new("pool-random-live");
    let (delivered, mut received) = mpsc::unbounded_channel();
    let next_id = Arc::new(AtomicUsize::new(0));

    let router = Routers::pool("workers", 3, {
        let next_id = next_id.clone();
        move || RecordingWorker {
            id: next_id.fetch_add(1, Ordering::SeqCst),
            delivered: delivered.clone(),
        }
    })
    .with_random()
    .spawn(&system)
    .expect("pool router should spawn");

    let first_routee = router
        .routees()
        .into_iter()
        .next()
        .expect("routee should exist");
    first_routee
        .tell(RecordCommand::Stop)
        .expect("routee should stop");
    let _terminated = first_routee.when_terminated().await;

    for sequence in 0..12 {
        router
            .tell(RecordCommand::Record { sequence })
            .expect("message should route to live routee");
    }

    let observed = receive_records(&mut received, 12).await;
    assert!(observed
        .iter()
        .all(|(_sequence, routee_id)| *routee_id != 0));
    assert_eq!(router.routee_count(), 2);

    system.shutdown();
}

#[tokio::test]
async fn no_live_routees_returns_message() {
    let system = ActorSystem::new("pool-no-routees");
    let (delivered, _received) = mpsc::unbounded_channel();
    let next_id = Arc::new(AtomicUsize::new(0));

    let router = Routers::pool("workers", 1, {
        let next_id = next_id.clone();
        move || RecordingWorker {
            id: next_id.fetch_add(1, Ordering::SeqCst),
            delivered: delivered.clone(),
        }
    })
    .spawn(&system)
    .expect("pool router should spawn");
    let routee = router
        .routees()
        .into_iter()
        .next()
        .expect("routee should exist");
    routee
        .tell(RecordCommand::Stop)
        .expect("routee should stop");
    let _terminated = routee.when_terminated().await;

    let error = router
        .tell(RecordCommand::Record { sequence: 7 })
        .expect_err("no live routee should fail");
    assert!(matches!(error, PoolRouterTellError::NoRoutees { .. }));
    assert!(matches!(
        error.into_message(),
        RecordCommand::Record { sequence: 7 }
    ));
    assert!(router.is_empty());

    system.shutdown();
}

#[tokio::test]
async fn routee_mailbox_full_returns_message() {
    let system = ActorSystem::new("pool-mailbox-full");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_wait = entered.notified();

    let router = Routers::pool("blocking", 1, {
        let entered = entered.clone();
        let release = release.clone();
        move || BlockingWorker {
            entered: entered.clone(),
            release: release.clone(),
        }
    })
    .with_options(ActorOptions::default().with_mailbox_capacity(1))
    .spawn(&system)
    .expect("pool router should spawn");

    router
        .tell(BlockingCommand::Block)
        .expect("blocking command should route");
    entered_wait.await;
    router
        .tell(BlockingCommand::Queued)
        .expect("queued command should fit in mailbox");

    let error = router
        .tell(BlockingCommand::Extra)
        .expect_err("full mailbox should fail");
    assert!(matches!(error, PoolRouterTellError::Full { .. }));
    assert!(matches!(error.into_message(), BlockingCommand::Extra));

    release.notify_waiters();
    system.shutdown();
}

#[tokio::test]
async fn zero_sized_pool_fails_before_spawning() {
    let system = ActorSystem::new("pool-zero");
    let (delivered, _received) = mpsc::unbounded_channel();

    let error = Routers::pool("workers", 0, move || RecordingWorker {
        id: 0,
        delivered: delivered.clone(),
    })
    .spawn(&system)
    .expect_err("zero-sized pool should fail");

    assert_eq!(error.code(), "invalid-pool-size");
    assert_eq!(system.snapshot().active_actors(), 0);
}

async fn receive_records(
    received: &mut mpsc::UnboundedReceiver<(usize, usize)>,
    count: usize,
) -> Vec<(usize, usize)> {
    let mut observed = Vec::new();
    for _ in 0..count {
        observed.push(
            tokio::time::timeout(Duration::from_secs(1), received.recv())
                .await
                .expect("record should arrive")
                .expect("record channel should remain open"),
        );
    }
    observed.sort_by_key(|(sequence, _routee_id)| *sequence);
    observed
}
