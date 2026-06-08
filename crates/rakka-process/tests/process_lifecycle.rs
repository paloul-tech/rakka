//! Integration tests for process configuration, lifecycle, and stdio protocols.

use std::path::PathBuf;
use std::time::Duration;

use rakka_core::ActorSystem;
use rakka_process::{
    spawn_process_actor, ExecutableAllowlist, ManagedProcess, ProcessActorCommand,
    ProcessActorConfig, ProcessActorState, ProcessCheck, ProcessError, ProcessHealth,
    ProcessRestartPolicy, ProcessShutdownOutcome, ProcessSpec, ProcessStdio, RawLineStdioCodec,
    StdioCommand, StdioProtocolConfig, StdioStatus,
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
            .shutdown_timeout(Duration::from_secs(2)),
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
                .shutdown_timeout(Duration::from_secs(2)),
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
                .shutdown_timeout(Duration::from_secs(2)),
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
    assert!(matches!(
        failed.last_error(),
        Some(ProcessError::RestartBudgetExhausted { max_restarts: 1 })
    ));

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
                .shutdown_timeout(Duration::from_secs(2)),
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
    let stderr = wait_for_stderr(&actor).await;
    assert!(stderr.iter().any(|line| line.contains("line-json:stdio-1")));

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

fn fixture_spec(test_name: &str) -> ProcessSpec {
    ProcessSpec::new(fixture_executable()).arg(test_name)
}

fn stdio_fixture_spec(test_name: &str) -> ProcessSpec {
    fixture_spec(test_name)
        .stdin(ProcessStdio::Piped)
        .stdout(ProcessStdio::Piped)
        .stderr(ProcessStdio::Piped)
        .shutdown_timeout(Duration::from_secs(2))
}

fn fixture_allowlist() -> ExecutableAllowlist {
    ExecutableAllowlist::from_exact_paths([fixture_executable()])
}

fn fixture_executable() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rakka-process-fixture"))
}

async fn ask_start(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
) -> Result<rakka_process::ProcessActorStatus, ProcessError> {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Start { reply_to },
            Duration::from_secs(2),
        )
        .await
        .expect("start ask should receive a reply")
}

async fn ask_stop(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
) -> Result<rakka_process::ProcessActorStatus, ProcessError> {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Stop { reply_to },
            Duration::from_secs(2),
        )
        .await
        .expect("stop ask should receive a reply")
}

async fn ask_status(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
) -> rakka_process::ProcessActorStatus {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Status { reply_to },
            Duration::from_secs(2),
        )
        .await
        .expect("status ask should receive a reply")
}

async fn wait_for_state(
    actor: &rakka_core::ActorRef<ProcessActorCommand>,
    expected: ProcessActorState,
) -> rakka_process::ProcessActorStatus {
    for _attempt in 0..100 {
        let status = ask_status(actor).await;
        if status.state() == expected {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let status = ask_status(actor).await;
    assert_eq!(status.state(), expected);
    status
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
    actor
        .ask(
            |reply_to| StdioCommand::RequestWithTimeout {
                request,
                timeout,
                reply_to,
            },
            Duration::from_secs(2),
        )
        .await
        .expect("stdio ask should receive a reply")
}

async fn ask_stdio_status<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
) -> StdioStatus
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    actor
        .ask(
            |reply_to| StdioCommand::Status { reply_to },
            Duration::from_secs(2),
        )
        .await
        .expect("stdio status ask should receive a reply")
}

async fn ask_stdio_stderr<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
) -> Vec<String>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    actor
        .ask(
            |reply_to| StdioCommand::Stderr { reply_to },
            Duration::from_secs(2),
        )
        .await
        .expect("stdio stderr ask should receive a reply")
}

async fn wait_for_pending_count<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
    expected: usize,
) where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    for _attempt in 0..100 {
        if ask_stdio_status(actor).await.pending_count() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(ask_stdio_status(actor).await.pending_count(), expected);
}

async fn wait_for_stderr<Req, Resp>(
    actor: &rakka_core::ActorRef<StdioCommand<Req, Resp>>,
) -> Vec<String>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    for _attempt in 0..100 {
        let stderr = ask_stdio_stderr(actor).await;
        if !stderr.is_empty() {
            return stderr;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    ask_stdio_stderr(actor).await
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JsonPayload {
    value: String,
}
