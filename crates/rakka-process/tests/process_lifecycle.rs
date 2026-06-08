//! Integration tests for process configuration, lifecycle, and stdio protocols.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::ActorSystem;
use rakka_process::{
    process_backed_entity_route, run_file_watch, run_one_shot, spawn_process_actor,
    start_local_grpc_process, start_socket_process, testkit as process_testkit,
    EndpointReadinessConfig, ExecutableAllowlist, FileWatchCleanup, FileWatchCompletion,
    FileWatchConfig, FileWatchInput, FileWatchOutcome, LocalEndpoint, LocalGrpcEndpoint,
    LocalGrpcProcessConfig, ManagedProcess, OneShotConfig, OneShotOutcome, ProcessActorCommand,
    ProcessActorConfig, ProcessActorState, ProcessBackedEntityAction, ProcessBackedEntityBehavior,
    ProcessBackedEntityContext, ProcessBackedEntityFuture, ProcessBackedEntityProcess,
    ProcessCheck, ProcessError, ProcessHealth, ProcessRestartPolicy, ProcessShutdownOutcome,
    ProcessSpec, ProcessStdio, RawLineStdioCodec, SocketProcessConfig, StdioCommand,
    StdioProtocolConfig, StdioStatus,
};
use rakka_sharding::{
    EntityId, EntityRef, EntityType, ShardCoordinator, ShardRegion, ShardingConfig,
};
use serde::{Deserialize, Serialize};

#[test]
fn process_spec_validation_rejects_unsafe_or_incomplete_specs() {
    let executable = fixture_executable();
    let allowlist = ExecutableAllowlist::from_exact_paths([executable.clone()]);

    assert!(matches!(
        ProcessSpec::new("relative-program").validate(&allowlist),
        Err(ProcessError::RelativeProgram { .. })
    ));
    assert!(matches!(
        ProcessSpec::new(&executable).validate(&ExecutableAllowlist::empty()),
        Err(ProcessError::ProgramNotAllowed { .. })
    ));
    assert!(matches!(
        ProcessSpec::new(&executable)
            .env("BAD=NAME", "value")
            .validate(&allowlist),
        Err(ProcessError::InvalidEnvironmentName { .. })
    ));
    assert!(matches!(
        ProcessSpec::new(&executable)
            .cwd("relative-cwd")
            .validate(&allowlist),
        Err(ProcessError::RelativeWorkingDirectory { .. })
    ));
}

#[tokio::test]
async fn managed_process_starts_and_stops_gracefully_by_closing_stdin() {
    let mut process = ManagedProcess::spawn(
        fixture_spec("fixture_waits_for_stdin_eof")
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        &fixture_allowlist(),
    )
    .expect("fixture process should spawn");

    assert!(process.pid().is_some());
    assert!(process.try_wait().unwrap().is_none());

    let shutdown = process
        .shutdown()
        .await
        .expect("fixture process should stop gracefully");

    assert!(matches!(
        shutdown.outcome(),
        ProcessShutdownOutcome::Graceful
    ));
    assert!(shutdown.exit().success());
    assert_eq!(shutdown.exit().code(), Some(0));
}

#[tokio::test]
async fn managed_process_kills_child_that_ignores_graceful_shutdown() {
    let mut process = ManagedProcess::spawn(
        fixture_spec("fixture_ignores_stdin")
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(Duration::from_millis(50)),
        &fixture_allowlist(),
    )
    .expect("fixture process should spawn");

    let shutdown = process
        .shutdown()
        .await
        .expect("fixture process should be killed after timeout");

    assert!(matches!(
        shutdown.outcome(),
        ProcessShutdownOutcome::KilledAfterTimeout { .. }
    ));
    assert!(shutdown.killed_after_timeout());
    assert!(!shutdown.exit().success());
}

#[tokio::test]
async fn managed_process_reports_non_zero_exit_status() {
    let mut process = ManagedProcess::spawn(
        fixture_spec("fixture_exits_with_status_17"),
        &fixture_allowlist(),
    )
    .expect("fixture process should spawn");

    let exit = process.wait().await.expect("fixture process should exit");

    assert!(!exit.success());
    assert_eq!(exit.code(), Some(17));
}

#[tokio::test]
async fn managed_process_reports_spawn_failure() {
    let missing = std::env::temp_dir().join(format!(
        "rakka-process-missing-binary-{}",
        std::process::id()
    ));
    let allowlist = ExecutableAllowlist::from_exact_paths([missing.clone()]);
    let error = ManagedProcess::spawn(ProcessSpec::new(&missing), &allowlist).unwrap_err();

    assert!(matches!(
        error,
        ProcessError::Spawn {
            program,
            ..
        } if program == missing
    ));
}

#[tokio::test]
async fn process_environment_is_clear_by_default_and_declared_vars_are_set() {
    let mut process = ManagedProcess::spawn(
        fixture_spec("fixture_asserts_environment_policy").env("RAKKA_DECLARED_TEST", "present"),
        &fixture_allowlist(),
    )
    .expect("fixture process should spawn");

    let exit = process.wait().await.expect("fixture process should exit");

    assert!(exit.success());
}

#[tokio::test]
async fn process_actor_starts_on_explicit_command_and_reports_status() {
    let system = ActorSystem::new("process-actor-explicit-start");
    let actor = spawn_process_actor(
        &system,
        "managed",
        ProcessActorConfig::new(
            fixture_spec("fixture_waits_for_stdin_eof")
                .stdin(ProcessStdio::Piped)
                .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
            fixture_allowlist(),
        )
        .without_supervision_interval(),
    )
    .expect("process actor should spawn");

    let started = ask_start(&actor).await.expect("process should start");
    assert_eq!(started.state(), ProcessActorState::Running);
    assert!(started.pid().is_some());

    let status = ask_status(&actor).await;
    assert_eq!(status.state(), ProcessActorState::Running);
    assert_eq!(status.pid(), started.pid());

    let stopped = ask_stop(&actor).await.expect("process should stop");
    assert_eq!(stopped.state(), ProcessActorState::Stopped);

    system.shutdown();
}

#[tokio::test]
async fn process_actor_can_start_child_when_actor_starts() {
    let system = ActorSystem::new("process-actor-auto-start");
    let actor = spawn_process_actor(
        &system,
        "managed",
        ProcessActorConfig::new(
            fixture_spec("fixture_waits_for_stdin_eof")
                .stdin(ProcessStdio::Piped)
                .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
            fixture_allowlist(),
        )
        .start_on_actor_start()
        .without_supervision_interval(),
    )
    .expect("process actor should spawn");

    let status = wait_for_state(&actor, ProcessActorState::Running).await;
    assert!(status.pid().is_some());

    let stopped = ask_stop(&actor).await.expect("process should stop");
    assert_eq!(stopped.state(), ProcessActorState::Stopped);

    system.shutdown();
}

#[tokio::test]
async fn process_actor_start_request_fails_when_child_exits_during_readiness() {
    let system = ActorSystem::new("process-actor-start-fails");
    let actor = spawn_process_actor(
        &system,
        "managed",
        ProcessActorConfig::new(
            fixture_spec("fixture_exits_with_status_17").startup_timeout(Duration::from_secs(1)),
            fixture_allowlist(),
        )
        .readiness_check(ProcessCheck::unknown())
        .without_supervision_interval(),
    )
    .expect("process actor should spawn");

    let error = ask_start(&actor)
        .await
        .expect_err("startup should report child exit");

    assert!(matches!(
        error,
        ProcessError::ExitedDuringStartup { code: Some(17), .. }
    ));
    assert_eq!(ask_status(&actor).await.state(), ProcessActorState::Failed);

    system.shutdown();
}

#[tokio::test]
async fn process_actor_restarts_unexpected_exit_until_budget_is_exhausted() {
    let system = ActorSystem::new("process-actor-restart-budget");
    let actor = spawn_process_actor(
        &system,
        "managed",
        ProcessActorConfig::new(
            fixture_spec("fixture_exits_after_delay").startup_timeout(Duration::from_secs(1)),
            fixture_allowlist(),
        )
        .restart_policy(ProcessRestartPolicy::exponential(
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
        ))
        .supervision_interval(Duration::from_millis(10)),
    )
    .expect("process actor should spawn");

    let started = ask_start(&actor).await.expect("process should start");
    assert_eq!(started.state(), ProcessActorState::Running);

    let failed = wait_for_state(&actor, ProcessActorState::Failed).await;
    assert_eq!(failed.restart_count(), 1);
    process_testkit::assert_restart_budget_exhausted(&failed, 1);

    system.shutdown();
}

#[tokio::test]
async fn process_actor_health_check_failure_triggers_supervision() {
    let system = ActorSystem::new("process-actor-health-fails");
    let actor = spawn_process_actor(
        &system,
        "managed",
        ProcessActorConfig::new(
            fixture_spec("fixture_waits_for_stdin_eof")
                .stdin(ProcessStdio::Piped)
                .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
            fixture_allowlist(),
        )
        .health_check(ProcessCheck::unhealthy("not ready"))
        .supervision_interval(Duration::from_millis(10)),
    )
    .expect("process actor should spawn");

    let started = ask_start(&actor).await.expect("process should start");
    assert_eq!(started.health(), &ProcessHealth::Healthy);

    let failed = wait_for_state(&actor, ProcessActorState::Failed).await;
    assert!(matches!(
        failed.last_error(),
        Some(ProcessError::Unhealthy { message }) if message == "not ready"
    ));

    system.shutdown();
}

#[tokio::test]
async fn raw_stdio_actor_round_trips_line_framed_bytes() {
    let system = ActorSystem::new("raw-stdio");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_raw_echo"),
        fixture_allowlist(),
        RawLineStdioCodec::new(),
        StdioProtocolConfig::new(),
    )
    .expect("stdio actor should spawn");

    let response = ask_stdio(&actor, b"hello raw".to_vec(), Duration::from_secs(1))
        .await
        .expect("raw stdio request should receive a reply");

    assert_eq!(response, b"hello raw".to_vec());
    system.shutdown();
}

#[tokio::test]
async fn line_json_actor_round_trips_typed_payload_and_captures_stderr() {
    let system = ActorSystem::new("line-json");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_line_json_echo"),
        fixture_allowlist(),
        rakka_process::LineJsonCodec::<JsonPayload, JsonPayload>::new(),
        StdioProtocolConfig::new(),
    )
    .expect("stdio actor should spawn");

    let response = ask_stdio(
        &actor,
        JsonPayload {
            value: "hello json".to_string(),
        },
        Duration::from_secs(1),
    )
    .await
    .expect("line-json request should receive a reply");

    assert_eq!(
        response,
        JsonPayload {
            value: "hello json".to_string()
        }
    );
    let stderr = process_testkit::wait_for_stderr_line(
        &actor,
        "line-json:stdio-1",
        process_testkit::DEFAULT_TEST_TIMEOUT,
    )
    .await;
    process_testkit::assert_stderr_line_contains(&stderr, "line-json:stdio-1");

    system.shutdown();
}

#[tokio::test]
async fn stdio_actor_rejects_requests_when_pending_capacity_is_full() {
    let system = ActorSystem::new("stdio-capacity");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_line_json_delayed"),
        fixture_allowlist(),
        rakka_process::LineJsonCodec::<JsonPayload, JsonPayload>::new(),
        StdioProtocolConfig::new().pending_capacity(1),
    )
    .expect("stdio actor should spawn");
    let first_actor = actor.clone();
    let first = tokio::spawn(async move {
        ask_stdio(
            &first_actor,
            JsonPayload {
                value: "first".to_string(),
            },
            Duration::from_secs(1),
        )
        .await
    });

    wait_for_pending_count(&actor, 1).await;
    let second = ask_stdio(
        &actor,
        JsonPayload {
            value: "second".to_string(),
        },
        Duration::from_secs(1),
    )
    .await;

    assert!(matches!(
        second,
        Err(ProcessError::PendingCapacity { capacity: 1 })
    ));
    assert_eq!(
        first.await.expect("task should complete").unwrap(),
        JsonPayload {
            value: "first".to_string()
        }
    );

    system.shutdown();
}

#[tokio::test]
async fn stdio_actor_removes_pending_request_on_timeout() {
    let system = ActorSystem::new("stdio-timeout");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_line_json_delayed"),
        fixture_allowlist(),
        rakka_process::LineJsonCodec::<JsonPayload, JsonPayload>::new(),
        StdioProtocolConfig::new(),
    )
    .expect("stdio actor should spawn");

    let response = ask_stdio(
        &actor,
        JsonPayload {
            value: "slow".to_string(),
        },
        Duration::from_millis(20),
    )
    .await;

    assert!(matches!(response, Err(ProcessError::RequestTimeout { .. })));
    assert_eq!(ask_stdio_status(&actor).await.pending_count(), 0);

    system.shutdown();
}

#[tokio::test]
async fn malformed_stdout_fails_pending_request_and_closes_protocol() {
    let system = ActorSystem::new("stdio-malformed");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_line_json_malformed"),
        fixture_allowlist(),
        rakka_process::LineJsonCodec::<JsonPayload, JsonPayload>::new(),
        StdioProtocolConfig::new(),
    )
    .expect("stdio actor should spawn");

    let response = ask_stdio(
        &actor,
        JsonPayload {
            value: "bad".to_string(),
        },
        Duration::from_secs(1),
    )
    .await;
    let status = ask_stdio_status(&actor).await;

    assert!(matches!(
        response,
        Err(ProcessError::MalformedStdout { .. })
    ));
    assert!(status.closed());
    assert_eq!(status.pending_count(), 0);

    system.shutdown();
}

#[tokio::test]
async fn process_exit_cleans_up_pending_stdio_requests() {
    let system = ActorSystem::new("stdio-crash");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_line_json_crash"),
        fixture_allowlist(),
        rakka_process::LineJsonCodec::<JsonPayload, JsonPayload>::new(),
        StdioProtocolConfig::new().supervision_interval(Duration::from_millis(10)),
    )
    .expect("stdio actor should spawn");

    let response = ask_stdio(
        &actor,
        JsonPayload {
            value: "crash".to_string(),
        },
        Duration::from_secs(1),
    )
    .await;
    let status = ask_stdio_status(&actor).await;

    assert!(matches!(
        response,
        Err(ProcessError::StdoutClosed | ProcessError::UnexpectedExit { .. })
    ));
    assert_eq!(status.pending_count(), 0);
    assert!(status.closed());

    system.shutdown();
}

#[tokio::test]
async fn actor_stop_fails_pending_stdio_requests() {
    let system = ActorSystem::new("stdio-stop");
    let actor = rakka_process::spawn_stdio_actor(
        &system,
        "stdio",
        stdio_fixture_spec("fixture_line_json_delayed"),
        fixture_allowlist(),
        rakka_process::LineJsonCodec::<JsonPayload, JsonPayload>::new(),
        StdioProtocolConfig::new(),
    )
    .expect("stdio actor should spawn");
    let pending_actor = actor.clone();
    let pending = tokio::spawn(async move {
        ask_stdio(
            &pending_actor,
            JsonPayload {
                value: "pending".to_string(),
            },
            Duration::from_secs(1),
        )
        .await
    });

    wait_for_pending_count(&actor, 1).await;
    actor.stop().expect("stdio actor should accept stop");

    assert!(matches!(
        pending.await.expect("task should complete"),
        Err(ProcessError::ProtocolClosed { .. })
    ));

    system.shutdown();
}

#[tokio::test]
async fn one_shot_returns_stdout_stderr_and_exit_status() {
    let outcome = run_one_shot(
        fixture_spec("fixture_one_shot_echo"),
        &fixture_allowlist(),
        OneShotConfig::new().stdin("hello one-shot"),
    )
    .await
    .expect("one-shot process should run");

    let OneShotOutcome::Exited(output) = outcome else {
        panic!("one-shot should exit before timeout");
    };
    assert!(output.exit().success());
    process_testkit::assert_output_contains("stdout", output.stdout(), b"stdout:hello one-shot");
    process_testkit::assert_output_contains("stderr", output.stderr(), b"stderr:hello one-shot");
    assert_eq!(output.stdout(), b"stdout:hello one-shot\n");
    assert_eq!(output.stderr(), b"stderr:hello one-shot\n");
}

#[tokio::test]
async fn one_shot_timeout_returns_typed_timeout_result() {
    let outcome = run_one_shot(
        fixture_spec("fixture_one_shot_sleeps"),
        &fixture_allowlist(),
        OneShotConfig::new().runtime_timeout(Duration::from_millis(25)),
    )
    .await
    .expect("one-shot timeout should be a typed outcome");

    process_testkit::assert_one_shot_timed_out(&outcome);
    assert!(!outcome.exit().success());
}

#[tokio::test]
async fn one_shot_enforces_output_capture_limits() {
    let error = run_one_shot(
        fixture_spec("fixture_one_shot_large_stdout"),
        &fixture_allowlist(),
        OneShotConfig::new().stdout_limit(16),
    )
    .await
    .expect_err("stdout limit should fail the run");

    assert!(matches!(
        error,
        ProcessError::OutputLimitExceeded {
            stream,
            limit: 16
        } if stream == "stdout"
    ));
}

#[tokio::test]
async fn file_watch_completes_in_sandbox_collects_outputs_and_cleans_up() {
    let sandbox = unique_temp_dir("file-watch-success");
    let outcome = run_file_watch(
        fixture_spec("fixture_file_watch_success"),
        &fixture_allowlist(),
        FileWatchConfig::new(
            sandbox.clone(),
            FileWatchCompletion::file_exists("output.txt"),
        )
        .input(FileWatchInput::new("input.txt", "payload"))
        .required_output("output.txt")
        .cleanup(FileWatchCleanup::RemoveOnSuccess)
        .timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
    )
    .await
    .expect("file-watch process should complete");

    let FileWatchOutcome::Completed(completed) = outcome else {
        panic!("file-watch should complete");
    };
    assert_eq!(completed.outputs.len(), 1);
    assert_eq!(completed.outputs[0].contents(), b"processed:payload");
    assert!(completed
        .stderr
        .windows(b"file-watch-ready".len())
        .any(|window| window == b"file-watch-ready"));
    assert!(!sandbox.exists());
}

#[tokio::test]
async fn file_watch_rejects_paths_that_escape_the_sandbox() {
    let sandbox = unique_temp_dir("file-watch-escape");
    let error = run_file_watch(
        fixture_spec("fixture_file_watch_success"),
        &fixture_allowlist(),
        FileWatchConfig::new(
            sandbox.clone(),
            FileWatchCompletion::file_exists("../output.txt"),
        ),
    )
    .await
    .expect_err("escaping completion path should fail");

    assert!(matches!(error, ProcessError::SandboxPathEscape { .. }));
    let _removed = fs::remove_dir_all(sandbox);
}

#[tokio::test]
async fn file_watch_timeout_shuts_down_child_process() {
    let sandbox = unique_temp_dir("file-watch-timeout");
    let outcome = run_file_watch(
        fixture_spec("fixture_waits_for_stdin_eof"),
        &fixture_allowlist(),
        FileWatchConfig::new(
            sandbox.clone(),
            FileWatchCompletion::file_exists("missing.txt"),
        )
        .cleanup(FileWatchCleanup::RemoveAlways)
        .timeout(Duration::from_millis(25)),
    )
    .await
    .expect("file-watch timeout should be a typed outcome");

    let FileWatchOutcome::TimedOut(timed_out) = outcome else {
        panic!("file-watch should time out");
    };
    assert!(timed_out.shutdown.exit().success());
    assert!(!sandbox.exists());
}

#[tokio::test]
async fn socket_process_waits_for_tcp_readiness() {
    let Some(port) = available_tcp_port() else {
        eprintln!("skipping tcp readiness test because local tcp bind is unavailable");
        return;
    };
    let mut socket_process = start_socket_process(
        fixture_spec("fixture_tcp_server")
            .arg(port.to_string())
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        &fixture_allowlist(),
        LocalEndpoint::tcp("127.0.0.1", port),
        SocketProcessConfig::new().readiness(
            EndpointReadinessConfig::new().timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        ),
    )
    .await
    .expect("socket process should become ready");

    assert!(socket_process.ready().attempts() >= 1);
    assert!(socket_process
        .shutdown()
        .await
        .expect("socket process should stop")
        .exit()
        .success());
}

#[tokio::test]
async fn socket_process_times_out_when_endpoint_never_opens() {
    let error = start_socket_process(
        fixture_spec("fixture_waits_for_stdin_eof")
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(Duration::from_secs(1)),
        &fixture_allowlist(),
        LocalEndpoint::tcp("127.0.0.1", 0),
        SocketProcessConfig::new()
            .readiness(EndpointReadinessConfig::new().timeout(Duration::from_millis(25))),
    )
    .await
    .expect_err("socket process should fail readiness");

    assert!(matches!(error, ProcessError::EndpointTimeout { .. }));
}

#[cfg(unix)]
#[tokio::test]
async fn socket_process_waits_for_unix_readiness() {
    let Some((sandbox, socket_path)) = available_unix_socket_path("unix-socket") else {
        eprintln!("skipping unix socket readiness test because local unix bind is unavailable");
        return;
    };
    let mut socket_process = start_socket_process(
        fixture_spec("fixture_unix_server")
            .arg(socket_path.clone())
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        &fixture_allowlist(),
        LocalEndpoint::unix(socket_path.clone()),
        SocketProcessConfig::new().readiness(
            EndpointReadinessConfig::new().timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        ),
    )
    .await
    .expect("unix socket process should become ready");

    assert_eq!(socket_process.endpoint(), &LocalEndpoint::unix(socket_path));
    socket_process
        .shutdown()
        .await
        .expect("unix socket process should stop");
    let _removed = fs::remove_dir_all(sandbox);
}

#[tokio::test]
async fn local_grpc_process_waits_for_local_endpoint_readiness() {
    let Some(port) = available_tcp_port() else {
        eprintln!("skipping local grpc readiness test because local tcp bind is unavailable");
        return;
    };
    let endpoint =
        LocalGrpcEndpoint::tcp("127.0.0.1", port).with_service_name("fixture.EchoService");
    let mut grpc_process = start_local_grpc_process(
        fixture_spec("fixture_tcp_server")
            .arg(port.to_string())
            .stdin(ProcessStdio::Piped)
            .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        &fixture_allowlist(),
        endpoint,
        LocalGrpcProcessConfig::new().readiness(
            EndpointReadinessConfig::new().timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
        ),
    )
    .await
    .expect("local grpc process should become ready");

    assert_eq!(
        grpc_process.endpoint().service_name(),
        Some("fixture.EchoService")
    );
    grpc_process
        .shutdown()
        .await
        .expect("local grpc process should stop");
}

#[tokio::test]
async fn process_backed_entity_starts_child_on_first_entity_ref_message_after_recovery() {
    let system = ActorSystem::new("process-backed-entity-start");
    let node_id = NodeId::new("rakka-0", "uid-a");
    let entity_type = EntityType::new("Legacy");
    let config = ShardingConfig::new(1).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership_with_up_nodes(vec![cluster_node(
        "rakka-0", "uid-a",
    )]));
    let log_dir = unique_temp_dir("process-backed-start");
    fs::create_dir_all(&log_dir).expect("log directory should be created");
    let log_path = log_dir.join("entity.log");
    let working_root = log_dir.join("work");
    let route = process_backed_entity_route(
        node_id,
        system.clone(),
        process_behavior_factory(log_path.clone(), working_root.clone()),
    );
    let region = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &coordinator.snapshot(),
        route.clone(),
    )
    .unwrap();
    let entity_id = EntityId::new("legacy-1");
    let entity = EntityRef::<ProcessEntityCommand>::new(entity_type, entity_id.clone());

    let status = entity
        .ask(
            &region,
            |reply_to| ProcessEntityCommand::Status { reply_to },
            process_testkit::DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("entity ask should route")
        .expect("process-backed status should succeed");

    assert!(status.pid.is_some());
    assert_eq!(status.fencing_key, "Legacy:legacy-1");
    assert!(status.log_label.contains("Legacy:legacy-1"));
    assert_eq!(
        status.working_dir,
        working_root.join("Legacy").join("shard-0").join("legacy-1")
    );
    wait_for_log_line(&log_path, "start").await;
    assert_eq!(read_lines(&log_path)[..2], ["recover", "start"]);
    assert_eq!(route.entity_count(), 1);

    assert!(route.passivate_entity(&entity_id));
    wait_for_log_line(&log_path, "stop").await;
    system.shutdown();
    let _removed = fs::remove_dir_all(log_dir);
}

#[tokio::test]
async fn process_backed_entity_handoff_stops_old_child_before_new_owner_activates() {
    let system_a = ActorSystem::new("process-backed-handoff-a");
    let system_b = ActorSystem::new("process-backed-handoff-b");
    let node_a = NodeId::new("rakka-0", "uid-a");
    let node_b = NodeId::new("rakka-1", "uid-b");
    let entity_type = EntityType::new("Legacy");
    let config = ShardingConfig::new(1).unwrap();
    let mut coordinator_a = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator_a.reconcile(&membership_with_up_nodes(vec![cluster_node(
        "rakka-0", "uid-a",
    )]));
    let mut coordinator_b = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator_b.reconcile(&membership_with_up_nodes(vec![cluster_node(
        "rakka-1", "uid-b",
    )]));
    let root = unique_temp_dir("process-backed-handoff");
    fs::create_dir_all(&root).expect("root directory should be created");
    let log_a = root.join("owner-a.log");
    let log_b = root.join("owner-b.log");
    let entity_id = EntityId::new("legacy-1");

    let route_a = process_backed_entity_route(
        node_a,
        system_a.clone(),
        process_behavior_factory(log_a.clone(), root.join("work-a")),
    );
    let region_a = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &coordinator_a.snapshot(),
        route_a.clone(),
    )
    .unwrap();
    let entity = EntityRef::<ProcessEntityCommand>::new(entity_type.clone(), entity_id.clone());
    entity
        .ask(
            &region_a,
            |reply_to| ProcessEntityCommand::Status { reply_to },
            process_testkit::DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("owner a ask should route")
        .expect("owner a status should succeed");
    wait_for_log_line(&log_a, "start").await;
    assert_eq!(read_lines(&log_a)[..2], ["recover", "start"]);

    let shard_id = entity.shard_id(&config);
    assert_eq!(region_a.begin_shard_handoff(shard_id).unwrap(), 0);
    assert_eq!(region_a.complete_shard_handoff(shard_id).unwrap(), 1);
    wait_for_log_line(&log_a, "stop").await;

    let route_b = process_backed_entity_route(
        node_b,
        system_b.clone(),
        process_behavior_factory(log_b.clone(), root.join("work-b")),
    );
    let region_b = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator_b.snapshot(),
        route_b.clone(),
    )
    .unwrap();
    entity
        .ask(
            &region_b,
            |reply_to| ProcessEntityCommand::Status { reply_to },
            process_testkit::DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("owner b ask should route")
        .expect("owner b status should succeed");

    wait_for_log_line(&log_b, "start").await;
    assert_eq!(read_lines(&log_b)[..2], ["recover", "start"]);
    system_a.shutdown();
    system_b.shutdown();
    let _removed = fs::remove_dir_all(root);
}

fn fixture_spec(test_name: &str) -> ProcessSpec {
    fixture().spec(test_name)
}

fn stdio_fixture_spec(test_name: &str) -> ProcessSpec {
    fixture().stdio_spec(test_name)
}

fn fixture_allowlist() -> ExecutableAllowlist {
    fixture().allowlist()
}

fn fixture_executable() -> PathBuf {
    fixture().executable().to_path_buf()
}

fn fixture() -> process_testkit::ProcessFixture {
    process_testkit::ProcessFixture::new(env!("CARGO_BIN_EXE_rakka-process-fixture"))
}

fn process_behavior_factory(
    log_path: PathBuf,
    working_root: PathBuf,
) -> impl Fn(ProcessBackedEntityContext) -> ProcessEntityBehavior + Send + Sync + 'static {
    move |_context| ProcessEntityBehavior {
        log_path: log_path.clone(),
        working_root: working_root.clone(),
    }
}

fn cluster_node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
    let local = nodes[0].clone();
    let mut membership = ClusterMembership::new(local, membership_config());

    membership
        .record_discovery(DiscoverySnapshot::new("test", 1, nodes))
        .expect("discovery snapshot should be accepted");

    for member in membership
        .snapshot()
        .members()
        .iter()
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>()
    {
        membership
            .mark_up(&member, 2)
            .expect("member should be marked up");
    }

    membership
}

fn append_test_line(path: &PathBuf, line: &str) -> Result<(), ProcessError> {
    process_testkit::append_line(path, line).map_err(|error| ProcessError::FileIo {
        path: path.clone(),
        message: error.to_string(),
    })
}

fn read_lines(path: &PathBuf) -> Vec<String> {
    process_testkit::read_lines(path).expect("log file should be readable")
}

async fn wait_for_log_line(path: &PathBuf, expected: &str) {
    process_testkit::wait_for_file_line(path, expected, process_testkit::DEFAULT_TEST_TIMEOUT)
        .await
        .expect("expected log line should be written");
}

fn unique_temp_dir(name: &str) -> PathBuf {
    process_testkit::unique_temp_dir(format!("rakka-process-{name}"))
}

#[cfg(unix)]
fn available_unix_socket_path(name: &str) -> Option<(PathBuf, PathBuf)> {
    process_testkit::available_unix_socket_path(format!("rakka-process-{name}"))
}

fn available_tcp_port() -> Option<u16> {
    process_testkit::available_tcp_port()
}

async fn ask_start(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
) -> Result<rakka_process::ProcessActorStatus, ProcessError> {
    process_testkit::start_process(actor).await
}

async fn ask_stop(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
) -> Result<rakka_process::ProcessActorStatus, ProcessError> {
    process_testkit::stop_process(actor).await
}

async fn ask_status(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
) -> rakka_process::ProcessActorStatus {
    process_testkit::process_status(actor).await
}

async fn wait_for_state(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
    expected: ProcessActorState,
) -> rakka_process::ProcessActorStatus {
    process_testkit::wait_for_process_state(actor, expected, process_testkit::DEFAULT_TEST_TIMEOUT)
        .await
}

async fn ask_stdio<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
    request: Req,
    timeout: Duration,
) -> Result<Resp, ProcessError>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    process_testkit::request_stdio(actor, request, timeout).await
}

async fn ask_stdio_status<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
) -> StdioStatus
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    process_testkit::stdio_status(actor).await
}

async fn wait_for_pending_count<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
    expected: usize,
) where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    process_testkit::wait_for_stdio_pending_count(
        actor,
        expected,
        process_testkit::DEFAULT_TEST_TIMEOUT,
    )
    .await;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JsonPayload {
    value: String,
}

#[derive(Debug)]
enum ProcessEntityCommand {
    Status {
        reply_to: rakka_core::ReplyTo<Result<ProcessEntityStatus, ProcessError>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessEntityStatus {
    pid: Option<u32>,
    fencing_key: String,
    log_label: String,
    working_dir: PathBuf,
}

struct ProcessEntityBehavior {
    log_path: PathBuf,
    working_root: PathBuf,
}

impl ProcessBackedEntityBehavior<ProcessEntityCommand> for ProcessEntityBehavior {
    fn process(&self, _context: &ProcessBackedEntityContext) -> ProcessBackedEntityProcess {
        ProcessBackedEntityProcess::new(
            fixture_spec("fixture_process_entity_lifecycle")
                .arg(self.log_path.clone())
                .stdin(ProcessStdio::Piped)
                .shutdown_timeout(process_testkit::DEFAULT_TEST_TIMEOUT),
            fixture_allowlist(),
        )
    }

    fn recover<'a>(
        &'a mut self,
        _context: &'a ProcessBackedEntityContext,
    ) -> ProcessBackedEntityFuture<'a, ()> {
        let log_path = self.log_path.clone();
        Box::pin(async move {
            if let Some(parent) = log_path.parent() {
                fs::create_dir_all(parent).map_err(|error| ProcessError::FileIo {
                    path: parent.to_path_buf(),
                    message: error.to_string(),
                })?;
            }
            append_test_line(&log_path, "recover")?;
            Ok(())
        })
    }

    fn handle<'a>(
        &'a mut self,
        context: &'a ProcessBackedEntityContext,
        process: &'a mut ManagedProcess,
        message: ProcessEntityCommand,
    ) -> ProcessBackedEntityFuture<'a, ProcessBackedEntityAction> {
        Box::pin(async move {
            match message {
                ProcessEntityCommand::Status { reply_to } => {
                    let _sent = reply_to.reply(Ok(ProcessEntityStatus {
                        pid: process.pid(),
                        fencing_key: context.fencing_key().to_string(),
                        log_label: context.log_label().to_string(),
                        working_dir: context.working_dir_under(&self.working_root),
                    }));
                }
            }
            Ok(ProcessBackedEntityAction::Continue)
        })
    }
}
