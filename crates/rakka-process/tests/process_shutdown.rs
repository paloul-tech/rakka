//! Coordinated shutdown tests for process actors and managed processes.

use std::time::Duration;

use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownReason, CoordinatedShutdownReport, ShutdownPhase,
    ShutdownTaskStatus,
};
use rakka_process::{
    register_configured_process_actor_stop_task, register_managed_process_shutdown_task,
    register_process_actor_stop_task, spawn_process_actor, testkit as process_testkit,
    ExecutableAllowlist, ManagedProcess, ProcessActorConfig, ProcessActorState, ProcessSpec,
    ProcessStdio,
};

#[tokio::test]
async fn process_actor_stop_task_stops_running_child() {
    let system = rakka_core::ActorSystem::new("process-shutdown-running");
    let shutdown = CoordinatedShutdown::new();
    let config = ProcessActorConfig::new(
        fixture_spec("fixture_waits_for_stdin_eof")
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        fixture_allowlist(),
    );
    let actor = spawn_process_actor(&system, "fixture", config.clone())
        .expect("process actor should spawn");
    process_testkit::expect_process_start(&actor).await;

    let task = register_configured_process_actor_stop_task(
        &shutdown,
        "stop-process-fixture",
        actor,
        &config,
    )
    .expect("process stop task should register");
    assert_eq!(task.phase(), &ShutdownPhase::stop_process_actors());

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .expect("process shutdown should complete");

    assert_eq!(
        task_status(
            &report,
            ShutdownPhase::stop_process_actors(),
            "stop-process-fixture"
        ),
        Some(ShutdownTaskStatus::Completed)
    );
    system.shutdown();
}

#[tokio::test]
async fn process_actor_stop_task_treats_not_running_as_complete() {
    let system = rakka_core::ActorSystem::new("process-shutdown-idle");
    let shutdown = CoordinatedShutdown::new();
    let config = ProcessActorConfig::new(
        fixture_spec("fixture_waits_for_stdin_eof")
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        fixture_allowlist(),
    );
    let actor =
        spawn_process_actor(&system, "idle-fixture", config).expect("process actor should spawn");

    register_process_actor_stop_task(
        &shutdown,
        "stop-idle-process",
        actor.clone(),
        process_testkit::DEFAULT_TEST_TIMEOUT,
    )
    .expect("process stop task should register");

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .expect("idle process shutdown should complete");

    assert_eq!(
        task_status(
            &report,
            ShutdownPhase::stop_process_actors(),
            "stop-idle-process"
        ),
        Some(ShutdownTaskStatus::Completed)
    );
    assert_eq!(
        process_testkit::process_status(&actor).await.state(),
        ProcessActorState::Stopped
    );
    system.shutdown();
}

#[tokio::test]
async fn managed_process_shutdown_task_uses_configured_graceful_timeout() {
    let shutdown = CoordinatedShutdown::new();
    let process = ManagedProcess::spawn(
        fixture_spec("fixture_ignores_stdin")
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(Duration::from_millis(50)),
        &fixture_allowlist(),
    )
    .expect("managed process should spawn");

    let task =
        register_managed_process_shutdown_task(&shutdown, "shutdown-managed-process", process)
            .expect("managed process shutdown task should register");
    assert_eq!(task.phase(), &ShutdownPhase::stop_process_actors());

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .expect("managed process shutdown should complete");

    assert_eq!(
        task_status(
            &report,
            ShutdownPhase::stop_process_actors(),
            "shutdown-managed-process"
        ),
        Some(ShutdownTaskStatus::Completed)
    );
}

fn task_status(
    report: &CoordinatedShutdownReport,
    phase: ShutdownPhase,
    task_name: &str,
) -> Option<ShutdownTaskStatus> {
    report
        .phases()
        .iter()
        .find(|phase_report| phase_report.phase() == &phase)?
        .tasks()
        .iter()
        .find(|task_report| task_report.task_name() == task_name)
        .map(|task_report| task_report.status())
}

fn fixture_spec(test_name: &str) -> ProcessSpec {
    fixture().spec(test_name)
}

fn fixture_allowlist() -> ExecutableAllowlist {
    fixture().allowlist()
}

fn fixture() -> process_testkit::ProcessFixture {
    process_testkit::ProcessFixture::new(env!("CARGO_BIN_EXE_rakka-process-fixture"))
}
