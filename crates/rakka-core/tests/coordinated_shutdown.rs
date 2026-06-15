//! Coordinated shutdown registry tests.

use std::time::Duration;

use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownSettings, ShutdownFailurePolicy, ShutdownPhase,
    ShutdownTaskOptions,
};

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

fn position_of(phases: &[ShutdownPhase], expected: &ShutdownPhase) -> usize {
    phases
        .iter()
        .position(|phase| phase == expected)
        .expect("phase should be present")
}
