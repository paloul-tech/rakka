//! Coordinated shutdown registry tests.

use std::time::Duration;
use std::{
    future,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
};

use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownError, CoordinatedShutdownReason,
    CoordinatedShutdownSettings, RakkaError, ShutdownFailurePolicy, ShutdownOutcome, ShutdownPhase,
    ShutdownTaskOptions, ShutdownTaskStatus,
};
use tokio::sync::Notify;
use tokio::time::Instant;

#[test]
fn coordinated_shutdown_builtin_phases_are_ordered() {
    let shutdown = CoordinatedShutdown::new();

    let phase_names = shutdown
        .phases()
        .unwrap()
        .into_iter()
        .map(|phase| phase.name().to_owned())
        .collect::<Vec<_>>();

    assert_eq!(
        phase_names,
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

#[test]
fn coordinated_shutdown_custom_phase_before_and_after_are_ordered() {
    let shutdown = CoordinatedShutdown::new();

    let before_flush = shutdown
        .add_phase_before("flush-search-index", ShutdownPhase::flush_persistence())
        .unwrap();
    let after_remoting = shutdown
        .add_phase_after("publish-final-metrics", ShutdownPhase::stop_remoting())
        .unwrap();

    let phases = shutdown.phases().unwrap();
    let before_flush_position = position_of(&phases, &before_flush);
    let flush_position = position_of(&phases, &ShutdownPhase::flush_persistence());
    let remoting_position = position_of(&phases, &ShutdownPhase::stop_remoting());
    let after_remoting_position = position_of(&phases, &after_remoting);

    assert!(before_flush_position < flush_position);
    assert!(remoting_position < after_remoting_position);
}

#[test]
fn coordinated_shutdown_duplicate_phase_is_rejected() {
    let shutdown = CoordinatedShutdown::new();

    let error = shutdown
        .add_phase_after("flush-persistence", ShutdownPhase::stop_process_actors())
        .unwrap_err();

    assert_eq!(error.code(), "duplicate-shutdown-phase");
}

#[test]
fn coordinated_shutdown_unknown_phase_is_rejected() {
    let shutdown = CoordinatedShutdown::new();
    let missing = ShutdownPhase::new("missing-phase").unwrap();

    let error = shutdown
        .add_phase_after("custom-phase", missing)
        .unwrap_err();

    assert_eq!(error.code(), "unknown-shutdown-phase");
}

#[test]
fn coordinated_shutdown_dependency_cycle_is_rejected_and_rolled_back() {
    let shutdown = CoordinatedShutdown::new();

    let error = shutdown
        .add_phase_dependency(
            ShutdownPhase::stop_ingress(),
            ShutdownPhase::stop_remoting(),
        )
        .unwrap_err();

    assert_eq!(error.code(), "shutdown-phase-cycle");

    let phase_names = shutdown
        .phases()
        .unwrap()
        .into_iter()
        .map(|phase| phase.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(phase_names.first().unwrap(), "stop-ingress");
}

#[test]
fn coordinated_shutdown_registers_tasks_without_actor_system() {
    let shutdown = CoordinatedShutdown::with_settings(
        CoordinatedShutdownSettings::new()
            .with_default_task_timeout(Duration::from_secs(5))
            .with_failure_policy(ShutdownFailurePolicy::Continue),
    );
    let options = ShutdownTaskOptions::new()
        .with_timeout(Duration::from_secs(1))
        .with_failure_policy(ShutdownFailurePolicy::FailFast)
        .with_attribute("resource", "http")
        .unwrap();

    let task = shutdown
        .add_task_with_options(
            ShutdownPhase::stop_ingress(),
            "stop-public-http",
            options.clone(),
            |_context| async { Ok(()) },
        )
        .unwrap();

    assert_eq!(task.phase(), &ShutdownPhase::stop_ingress());
    assert_eq!(task.name(), "stop-public-http");
    assert_eq!(task.options(), &options);
    assert_eq!(
        shutdown
            .tasks_for_phase(&ShutdownPhase::stop_ingress())
            .unwrap(),
        vec![task]
    );
}

#[test]
fn coordinated_shutdown_duplicate_task_in_same_phase_is_rejected() {
    let shutdown = CoordinatedShutdown::new();

    shutdown
        .add_task(
            ShutdownPhase::stop_ingress(),
            "stop-public-http",
            |_| async { Ok(()) },
        )
        .unwrap();
    let error = shutdown
        .add_task(
            ShutdownPhase::stop_ingress(),
            "stop-public-http",
            |_| async { Ok(()) },
        )
        .unwrap_err();

    assert_eq!(error.code(), "duplicate-shutdown-task");
}

#[test]
fn coordinated_shutdown_rejects_invalid_names() {
    let shutdown = CoordinatedShutdown::new();

    let phase_error = ShutdownPhase::new(" padded").unwrap_err();
    let task_error = shutdown
        .add_task(ShutdownPhase::stop_ingress(), "bad task", |_| async {
            Ok(())
        })
        .unwrap_err();

    assert_eq!(phase_error.code(), "invalid-shutdown-name");
    assert_eq!(task_error.code(), "invalid-shutdown-name");
}

#[tokio::test]
async fn coordinated_shutdown_run_executes_tasks_in_phase_order() {
    let shutdown = CoordinatedShutdown::new();
    let order = Arc::new(Mutex::new(Vec::new()));

    shutdown
        .add_task(ShutdownPhase::flush_persistence(), "flush-store", {
            let order = order.clone();
            move |_| {
                let order = order.clone();
                async move {
                    order.lock().unwrap().push("flush-store");
                    Ok(())
                }
            }
        })
        .unwrap();
    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "stop-http", {
            let order = order.clone();
            move |_| {
                let order = order.clone();
                async move {
                    order.lock().unwrap().push("stop-http");
                    Ok(())
                }
            }
        })
        .unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert_eq!(
        order.lock().unwrap().as_slice(),
        ["stop-http", "flush-store"]
    );
    assert_eq!(report.phases().len(), shutdown.phase_count());
    assert_eq!(shutdown.snapshot().outcome(), ShutdownOutcome::Complete);
}

#[tokio::test]
async fn coordinated_shutdown_repeated_run_returns_same_report_without_rerunning_tasks() {
    let shutdown = CoordinatedShutdown::new();
    let runs = Arc::new(AtomicUsize::new(0));

    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "once", {
            let runs = runs.clone();
            move |_| {
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .unwrap();

    let first = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();
    let second = shutdown
        .run(CoordinatedShutdownReason::kubernetes_prestop())
        .await
        .unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(first, second);
    assert_eq!(second.reason(), &CoordinatedShutdownReason::user_request());
}

#[tokio::test]
async fn coordinated_shutdown_concurrent_run_calls_share_in_flight_result() {
    let shutdown = CoordinatedShutdown::new();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runs = Arc::new(AtomicUsize::new(0));

    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "controlled", {
            let entered = entered.clone();
            let release = release.clone();
            let runs = runs.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    entered.notify_waiters();
                    release.notified().await;
                    Ok(())
                }
            }
        })
        .unwrap();

    let first_shutdown = shutdown.clone();
    let first = tokio::spawn(async move {
        first_shutdown
            .run(CoordinatedShutdownReason::user_request())
            .await
    });
    entered.notified().await;
    assert_eq!(shutdown.snapshot().outcome(), ShutdownOutcome::Running);

    let second_shutdown = shutdown.clone();
    let second = tokio::spawn(async move {
        second_shutdown
            .run(CoordinatedShutdownReason::kubernetes_prestop())
            .await
    });

    release.notify_waiters();

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(first, second);
}

#[tokio::test]
async fn coordinated_shutdown_fail_fast_stops_later_tasks_and_returns_report_error() {
    let shutdown = CoordinatedShutdown::new();
    let later_runs = Arc::new(AtomicUsize::new(0));

    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "fail", |_| async {
            Err(RakkaError::core("expected-failure", "boom"))
        })
        .unwrap();
    shutdown
        .add_task(ShutdownPhase::drain_adapters(), "later", {
            let later_runs = later_runs.clone();
            move |_| {
                let later_runs = later_runs.clone();
                async move {
                    later_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .unwrap();

    let error = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap_err();

    assert!(matches!(error, CoordinatedShutdownError::Failed { .. }));
    let report = error.report().unwrap();
    assert_eq!(report.outcome(), ShutdownOutcome::Failed);
    assert_eq!(report.phases().len(), 1);
    assert_eq!(
        report.phases()[0].tasks()[0].status(),
        ShutdownTaskStatus::Failed
    );
    assert_eq!(later_runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn coordinated_shutdown_continue_policy_runs_later_tasks_and_returns_partial_report() {
    let shutdown = CoordinatedShutdown::with_settings(
        CoordinatedShutdownSettings::new().with_failure_policy(ShutdownFailurePolicy::Continue),
    );
    let later_runs = Arc::new(AtomicUsize::new(0));

    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "fail", |_| async {
            Err(RakkaError::core("expected-failure", "boom"))
        })
        .unwrap();
    shutdown
        .add_task(ShutdownPhase::drain_adapters(), "later", {
            let later_runs = later_runs.clone();
            move |_| {
                let later_runs = later_runs.clone();
                async move {
                    later_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    assert_eq!(report.outcome(), ShutdownOutcome::Partial);
    assert_eq!(later_runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn coordinated_shutdown_task_timeout_stops_later_tasks_under_fail_fast_policy() {
    let shutdown = CoordinatedShutdown::with_settings(
        CoordinatedShutdownSettings::new().with_default_task_timeout(Duration::from_millis(10)),
    );
    let later_runs = Arc::new(AtomicUsize::new(0));

    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "pending", |_| async {
            future::pending::<rakka_core::RakkaResult<()>>().await
        })
        .unwrap();
    shutdown
        .add_task(ShutdownPhase::drain_adapters(), "later", {
            let later_runs = later_runs.clone();
            move |_| {
                let later_runs = later_runs.clone();
                async move {
                    later_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .unwrap();

    let error = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap_err();

    assert!(matches!(error, CoordinatedShutdownError::TimedOut { .. }));
    let report = error.report().unwrap();
    assert_eq!(report.outcome(), ShutdownOutcome::TimedOut);
    assert_eq!(
        report.phases()[0].tasks()[0].status(),
        ShutdownTaskStatus::TimedOut
    );
    assert_eq!(later_runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn coordinated_shutdown_run_with_deadline_times_out() {
    let shutdown = CoordinatedShutdown::new();
    shutdown
        .add_task(ShutdownPhase::stop_ingress(), "pending", |_| async {
            future::pending::<rakka_core::RakkaResult<()>>().await
        })
        .unwrap();

    let error = shutdown
        .run_with_deadline(
            CoordinatedShutdownReason::kubernetes_prestop(),
            Instant::now() + Duration::from_millis(10),
        )
        .await
        .unwrap_err();

    assert_eq!(error.outcome(), ShutdownOutcome::TimedOut);
    assert_eq!(shutdown.snapshot().outcome(), ShutdownOutcome::TimedOut);
    assert!(shutdown.snapshot().report().is_some());
}

fn position_of(phases: &[ShutdownPhase], expected: &ShutdownPhase) -> usize {
    phases
        .iter()
        .position(|phase| phase == expected)
        .expect("phase should be present")
}
