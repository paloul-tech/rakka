//! Coordinated shutdown testkit tests.

use rakka_core::{
    ActorSystem, CoordinatedShutdown, CoordinatedShutdownReason, CoordinatedShutdownSettings,
    InMemoryMetricsRecorder, ShutdownFailurePolicy, ShutdownOutcome, ShutdownPhase,
    ShutdownTaskStatus,
};
use rakka_testkit::{
    assert_shutdown_outcome, assert_shutdown_phase_order, assert_shutdown_task_start_order,
    assert_shutdown_task_status, assert_shutdown_timeout_metric, CoordinatedShutdownTestKit,
};
use std::sync::Arc;

#[tokio::test]
async fn coordinated_shutdown_testkit_records_order_and_releases_controlled_task_without_sleeps() {
    let kit = CoordinatedShutdownTestKit::new();
    let stop_ingress = ShutdownPhase::stop_ingress();
    let drain_adapters = ShutdownPhase::drain_adapters();
    let leave_cluster = ShutdownPhase::leave_cluster();

    kit.register_task(stop_ingress.clone(), "first")
        .expect("first task should register");
    let controlled = kit
        .register_controlled_task(drain_adapters.clone(), "blocked")
        .expect("controlled task should register");
    kit.register_task(leave_cluster.clone(), "after")
        .expect("later task should register");

    let shutdown = kit.shutdown();
    let run = tokio::spawn(async move {
        shutdown
            .run(CoordinatedShutdownReason::user_request())
            .await
    });

    controlled
        .wait_started()
        .await
        .expect("controlled task should start");
    assert_eq!(
        kit.shutdown().snapshot().outcome(),
        ShutdownOutcome::Running
    );
    controlled.release();
    controlled
        .wait_finished()
        .await
        .expect("controlled task should finish");

    let report = run
        .await
        .expect("shutdown join should succeed")
        .expect("shutdown should complete");

    assert_shutdown_outcome(&report, ShutdownOutcome::Complete);
    assert_shutdown_phase_order(
        &report,
        &[
            stop_ingress.clone(),
            drain_adapters.clone(),
            leave_cluster.clone(),
        ],
    );
    assert_shutdown_task_start_order(
        &kit.events(),
        &[
            (&stop_ingress, "first"),
            (&drain_adapters, "blocked"),
            (&leave_cluster, "after"),
        ],
    );
    assert_shutdown_task_status(
        &report,
        &drain_adapters,
        "blocked",
        ShutdownTaskStatus::Completed,
    );
}

#[tokio::test]
async fn coordinated_shutdown_testkit_asserts_failure_and_idempotency() {
    let kit = CoordinatedShutdownTestKit::with_settings(
        CoordinatedShutdownSettings::new().with_failure_policy(ShutdownFailurePolicy::Continue),
    );
    let stop_ingress = ShutdownPhase::stop_ingress();
    let drain_adapters = ShutdownPhase::drain_adapters();

    kit.register_task(stop_ingress.clone(), "first")
        .expect("first task should register");
    kit.register_failing_task(
        drain_adapters.clone(),
        "fail",
        "expected-shutdown-failure",
        "boom",
    )
    .expect("failing task should register");

    let report = kit
        .assert_idempotent(CoordinatedShutdownReason::user_request())
        .await
        .expect("continue policy should return partial report");

    assert_shutdown_outcome(&report, ShutdownOutcome::Partial);
    assert_shutdown_task_status(
        &report,
        &stop_ingress,
        "first",
        ShutdownTaskStatus::Completed,
    );
    assert_shutdown_task_status(&report, &drain_adapters, "fail", ShutdownTaskStatus::Failed);
}

#[tokio::test]
async fn coordinated_shutdown_testkit_supports_actor_system_owned_registry() {
    let system = ActorSystem::new("shutdown-testkit-system");
    let kit = CoordinatedShutdownTestKit::for_system(&system);
    let stop_ingress = ShutdownPhase::stop_ingress();

    kit.register_task(stop_ingress.clone(), "application-stop-ingress")
        .expect("application task should register");

    let report = system
        .terminate_with_report()
        .await
        .expect("actor-system shutdown should complete");

    assert_shutdown_task_status(
        &report,
        &stop_ingress,
        "application-stop-ingress",
        ShutdownTaskStatus::Completed,
    );
}

#[tokio::test]
async fn coordinated_shutdown_testkit_asserts_timeout_metric_labels() {
    let recorder = Arc::new(InMemoryMetricsRecorder::new());
    let shutdown = CoordinatedShutdown::with_settings_and_metrics(
        CoordinatedShutdownSettings::new().with_default_task_timeout(std::time::Duration::ZERO),
        "timeout-system",
        recorder.clone(),
    );
    let kit = CoordinatedShutdownTestKit::from_shutdown(shutdown);
    let stop_ingress = ShutdownPhase::stop_ingress();
    kit.register_task(stop_ingress.clone(), "blocked")
        .expect("timed task should register");

    let shutdown = kit.shutdown();
    let run = tokio::spawn(async move {
        shutdown
            .run(CoordinatedShutdownReason::user_request())
            .await
    });

    let error = run
        .await
        .expect("shutdown join should succeed")
        .expect_err("controlled task should time out");
    let report = error.report().expect("timeout should preserve report");
    assert_shutdown_task_status(
        report,
        &stop_ingress,
        "blocked",
        ShutdownTaskStatus::TimedOut,
    );
    assert_shutdown_timeout_metric(&recorder.snapshot(), &stop_ingress, "blocked", "task");
}
