//! Operational metrics and actor-system snapshot tests.

use std::future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rakka_core::{
    actor_future, export_open_telemetry_metrics, export_prometheus_text, Actor, ActorAction,
    ActorContext, ActorFuture, ActorOptions, ActorSystem, CoordinatedShutdown,
    CoordinatedShutdownReason, CoordinatedShutdownSettings, InMemoryMetricsRecorder, MetricKind,
    RakkaError, ShutdownFailurePolicy, ShutdownOutcome, ShutdownPhase, ShutdownTaskStatus,
    METRIC_ACTOR_COUNT, METRIC_ACTOR_MAILBOX_DEPTH, METRIC_SHUTDOWN_PHASE_DURATION_MS,
    METRIC_SHUTDOWN_RUNNING, METRIC_SHUTDOWN_TASK_DURATION_MS, METRIC_SHUTDOWN_TASK_FAILURES,
    METRIC_SHUTDOWN_TIMEOUTS,
};
use tokio::sync::{oneshot, Notify};

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

#[tokio::test]
async fn operational_metrics_actor_system_terminate_records_shutdown_metrics() {
    let recorder = Arc::new(InMemoryMetricsRecorder::new());
    let system = ActorSystem::with_metrics("ops", recorder.clone());

    system
        .terminate_with_report()
        .await
        .expect("actor system termination should complete");

    let metrics = recorder.snapshot();
    assert_eq!(metrics.last_gauge(METRIC_SHUTDOWN_RUNNING), Some(0.0));
    let running = metrics
        .last_observation(METRIC_SHUTDOWN_RUNNING, MetricKind::Gauge)
        .expect("shutdown running gauge should be recorded");
    assert_eq!(running.attribute("system"), Some("ops"));
    assert_eq!(running.attribute("reason"), Some("actor-system-terminate"));
}

#[tokio::test]
async fn operational_metrics_record_coordinated_shutdown_outcomes() {
    let recorder = Arc::new(InMemoryMetricsRecorder::new());
    let shutdown = CoordinatedShutdown::with_settings_and_metrics(
        CoordinatedShutdownSettings::new()
            .with_default_task_timeout(Duration::from_millis(5))
            .with_failure_policy(ShutdownFailurePolicy::Continue),
        "ops",
        recorder.clone(),
    );

    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "complete", |_| async {
            Ok(())
        })
        .expect("complete task should register");
    shutdown
        .add_task(ShutdownPhase::drain_adapters(), "fail", |_| async {
            Err(RakkaError::core("expected-failure", "boom"))
        })
        .expect("failure task should register");
    shutdown
        .add_task(ShutdownPhase::leave_cluster(), "timeout", |_| async {
            future::pending::<rakka_core::RakkaResult<()>>().await
        })
        .expect("timeout task should register");

    let error = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .expect_err("timeout should be reported");
    assert_eq!(error.outcome(), ShutdownOutcome::TimedOut);

    let metrics = recorder.snapshot();
    assert_eq!(metrics.last_gauge(METRIC_SHUTDOWN_RUNNING), Some(0.0));
    assert_eq!(metrics.counter_total(METRIC_SHUTDOWN_TASK_FAILURES), 1.0);
    assert_eq!(metrics.counter_total(METRIC_SHUTDOWN_TIMEOUTS), 1.0);

    let task_durations = metrics.observations_named(METRIC_SHUTDOWN_TASK_DURATION_MS);
    assert!(task_durations.iter().any(|observation| {
        observation.attribute("system") == Some("ops")
            && observation.attribute("phase") == Some("stop-ingress")
            && observation.attribute("task") == Some("complete")
            && observation.attribute("reason") == Some("user-request")
            && observation.attribute("status") == Some(ShutdownTaskStatus::Completed.as_str())
    }));
    assert!(task_durations.iter().any(|observation| {
        observation.attribute("task") == Some("fail")
            && observation.attribute("status") == Some(ShutdownTaskStatus::Failed.as_str())
    }));
    assert!(task_durations.iter().any(|observation| {
        observation.attribute("task") == Some("timeout")
            && observation.attribute("status") == Some(ShutdownTaskStatus::TimedOut.as_str())
    }));

    let phase_durations = metrics.observations_named(METRIC_SHUTDOWN_PHASE_DURATION_MS);
    assert!(phase_durations.iter().any(|observation| {
        observation.attribute("phase") == Some("leave-cluster")
            && observation.attribute("status") == Some(ShutdownOutcome::TimedOut.as_str())
    }));

    let prometheus = export_prometheus_text(&metrics);
    assert!(prometheus.contains("rakka_shutdown_task_duration_ms_count"));
    assert!(prometheus.contains("rakka_shutdown_task_failures"));
    assert!(prometheus.contains("rakka_shutdown_timeouts"));

    let open_telemetry = export_open_telemetry_metrics(&metrics, &[]);
    assert!(open_telemetry
        .metrics()
        .iter()
        .any(|metric| metric.name() == METRIC_SHUTDOWN_TASK_DURATION_MS));

    let failure = metrics
        .last_observation(METRIC_SHUTDOWN_TASK_FAILURES, MetricKind::Counter)
        .expect("failure counter should be recorded");
    assert_eq!(
        failure.attribute("phase"),
        Some("drain-http-grpc-and-streams")
    );

    let snapshot = shutdown.snapshot();
    let snapshot_json =
        serde_json::to_value(&snapshot).expect("shutdown snapshot should serialize");
    assert_eq!(snapshot_json["outcome"], "timed-out");
    assert!(snapshot_json["current_phase"].is_null());
    assert!(snapshot_json["current_task"].is_null());
    assert_eq!(snapshot_json["report"]["outcome"], "timed-out");
    assert_eq!(
        snapshot_json["report"]["phases"][0]["phase"]["name"],
        "stop-ingress"
    );
}

#[tokio::test]
async fn operational_metrics_shutdown_snapshot_reports_running_phase_and_task() {
    let recorder = Arc::new(InMemoryMetricsRecorder::new());
    let shutdown = CoordinatedShutdown::with_metrics("ops", recorder);
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let release = Arc::new(Notify::new());

    shutdown
        .add_task(ShutdownPhase::handoff_shards(), "controlled", {
            let entered_tx = entered_tx.clone();
            let release = release.clone();
            move |_| {
                let entered_tx = entered_tx.clone();
                let release = release.clone();
                async move {
                    if let Some(entered_tx) = entered_tx
                        .lock()
                        .expect("entered gate mutex poisoned")
                        .take()
                    {
                        let _sent = entered_tx.send(());
                    }
                    release.notified().await;
                    Ok(())
                }
            }
        })
        .expect("controlled task should register");

    let running_shutdown = shutdown.clone();
    let running = tokio::spawn(async move {
        running_shutdown
            .run(CoordinatedShutdownReason::user_request())
            .await
    });

    entered_rx.await.expect("task should enter");
    let snapshot = shutdown.snapshot();
    assert_eq!(snapshot.outcome(), ShutdownOutcome::Running);
    assert_eq!(
        snapshot.current_phase().map(ShutdownPhase::name),
        Some("handoff-shards")
    );
    assert_eq!(snapshot.current_task(), Some("controlled"));
    assert!(snapshot.report().is_none());

    release.notify_waiters();
    running
        .await
        .expect("shutdown task should join")
        .expect("shutdown should complete");
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
