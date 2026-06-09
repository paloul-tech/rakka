//! Operational metrics and actor-system snapshot tests.

use std::sync::Arc;
use std::sync::Mutex;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorSystem,
    InMemoryMetricsRecorder, METRIC_ACTOR_COUNT, METRIC_ACTOR_MAILBOX_DEPTH,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn actor_system_snapshot_records_active_count_and_mailbox_depth() {
    let recorder = Arc::new(InMemoryMetricsRecorder::new());
    let system = ActorSystem::with_metrics("ops", recorder.clone());
    let (release_started, wait_started) = oneshot::channel();
    let wait_started = Arc::new(Mutex::new(Some(wait_started)));
    let actor = system
        .spawn_actor_with_options(
            "blocked",
            move || BlockingActor {
                wait_started: wait_started
                    .lock()
                    .expect("started gate mutex poisoned")
                    .take(),
            },
            ActorOptions::default().with_mailbox_capacity(4),
        )
        .expect("actor should spawn");

    actor
        .tell(TestCommand::Ping)
        .expect("message should enqueue");

    let snapshot = system.record_metrics();

    assert_eq!(snapshot.active_actors(), 1);
    assert_eq!(snapshot.total_actors(), 1);
    assert_eq!(snapshot.actors()[0].mailbox_depth(), 1);
    assert_eq!(actor.mailbox_depth(), 1);
    assert!(serde_json::to_string(&snapshot)
        .expect("snapshot should serialize")
        .contains("\"active_actors\":1"));

    let metrics = recorder.snapshot();
    assert_eq!(metrics.last_gauge(METRIC_ACTOR_COUNT), Some(1.0));
    assert_eq!(metrics.last_gauge(METRIC_ACTOR_MAILBOX_DEPTH), Some(1.0));
    assert_eq!(
        metrics
            .last_observation(METRIC_ACTOR_MAILBOX_DEPTH, rakka_core::MetricKind::Gauge)
            .and_then(|observation| observation.attribute("capacity")),
        Some("4")
    );

    release_started
        .send(())
        .expect("actor started gate should release");
    system.shutdown();
}

#[derive(Debug)]
enum TestCommand {
    Ping,
}

struct BlockingActor {
    wait_started: Option<oneshot::Receiver<()>>,
}

impl Actor for BlockingActor {
    type Msg = TestCommand;

    fn started<'a>(&'a mut self, _ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        let wait_started = self
            .wait_started
            .take()
            .expect("started future should be created once");
        actor_future(async move {
            let _released = wait_started.await;
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }
}
