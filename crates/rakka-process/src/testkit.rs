//! Test helpers for process actor and process protocol integration tests.

use std::fs;
use std::io::{self, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rakka_core::ActorRef;

use crate::{
    ExecutableAllowlist, OneShotOutcome, ProcessActorCommand, ProcessActorState,
    ProcessActorStatus, ProcessError, ProcessSpec, ProcessStdio, StdioCommand, StdioStatus,
};

/// Default actor ask timeout used by process testkit helpers.
pub const DEFAULT_TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Fixture executable plus convenience methods for allowlisted process specs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessFixture {
    executable: PathBuf,
}

impl ProcessFixture {
    /// Creates a fixture from an executable path.
    #[must_use]
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }

    /// Fixture executable path.
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// Allowlist that accepts exactly the fixture executable.
    #[must_use]
    pub fn allowlist(&self) -> ExecutableAllowlist {
        ExecutableAllowlist::from_exact_paths([self.executable.clone()])
    }

    /// Creates a process spec that invokes the fixture with one command argument.
    #[must_use]
    pub fn spec(&self, command: impl Into<std::ffi::OsString>) -> ProcessSpec {
        ProcessSpec::new(self.executable.clone()).arg(command)
    }

    /// Creates a process spec with piped stdin/stdout/stderr for protocol tests.
    #[must_use]
    pub fn stdio_spec(&self, command: impl Into<std::ffi::OsString>) -> ProcessSpec {
        self.spec(command)
            .stdin(ProcessStdio::Piped)
            .stdout(ProcessStdio::Piped)
            .stderr(ProcessStdio::Piped)
            .shutdown_timeout(Duration::from_secs(2))
    }
}

/// Creates a unique path under the host temporary directory.
#[must_use]
pub fn unique_temp_dir(name: impl AsRef<str>) -> PathBuf {
    std::env::temp_dir().join(unique_name(name.as_ref()))
}

/// Creates a unique path under `/tmp` for Unix resources with short path limits.
#[cfg(unix)]
#[must_use]
pub fn unique_short_temp_dir(name: impl AsRef<str>) -> PathBuf {
    PathBuf::from("/tmp").join(unique_name(name.as_ref()))
}

/// Returns an available local TCP port, or `None` when local bind is denied.
#[must_use]
pub fn available_tcp_port() -> Option<u16> {
    let listener = match TcpListener::bind(("127.0.0.1", 0)) {
        Ok(listener) => listener,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return None,
        Err(error) => panic!("ephemeral tcp port should bind: {error}"),
    };
    Some(
        listener
            .local_addr()
            .expect("listener address should be available")
            .port(),
    )
}

/// Returns a bindable Unix socket path, or `None` when local bind is denied.
#[cfg(unix)]
#[must_use]
pub fn available_unix_socket_path(name: impl AsRef<str>) -> Option<(PathBuf, PathBuf)> {
    let sandbox = unique_short_temp_dir(name);
    fs::create_dir_all(&sandbox).expect("sandbox should be created");
    let socket_path = sandbox.join("fixture.sock");
    match std::os::unix::net::UnixListener::bind(&socket_path) {
        Ok(listener) => {
            drop(listener);
            fs::remove_file(&socket_path).expect("probe unix socket should be removable");
            Some((sandbox, socket_path))
        }
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            let _removed = fs::remove_dir_all(&sandbox);
            None
        }
        Err(error) => panic!("unix socket should bind: {error}"),
    }
}

/// Appends one line to a test log file, creating it when needed.
pub fn append_line(path: impl AsRef<Path>, line: impl AsRef<str>) -> io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{}", line.as_ref())
}

/// Reads a file as newline-separated strings.
pub fn read_lines(path: impl AsRef<Path>) -> io::Result<Vec<String>> {
    Ok(fs::read_to_string(path)?
        .lines()
        .map(ToString::to_string)
        .collect())
}

/// Waits until a file contains an exact line or the timeout elapses.
pub async fn wait_for_file_line(
    path: impl AsRef<Path>,
    expected: impl AsRef<str>,
    timeout: Duration,
) -> io::Result<Vec<String>> {
    let path = path.as_ref().to_path_buf();
    let expected = expected.as_ref().to_string();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if path.exists() {
            let lines = read_lines(&path)?;
            if lines.iter().any(|line| line == &expected) {
                return Ok(lines);
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for line {expected:?} in {}",
                    path.display()
                ),
            ));
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Asserts that captured output contains the expected byte sequence.
pub fn assert_output_contains(stream: &str, output: &[u8], expected: &[u8]) {
    assert!(
        output
            .windows(expected.len())
            .any(|window| window == expected),
        "expected {stream} to contain {:?}, got {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(output)
    );
}

/// Asserts that captured stderr lines contain a substring.
pub fn assert_stderr_line_contains(stderr: &[String], expected: &str) {
    assert!(
        stderr.iter().any(|line| line.contains(expected)),
        "expected stderr to contain {expected:?}, got {stderr:?}"
    );
}

/// Asserts that a one-shot outcome timed out.
pub fn assert_one_shot_timed_out(outcome: &OneShotOutcome) {
    assert!(
        matches!(outcome, OneShotOutcome::TimedOut { .. }),
        "expected one-shot timeout, got {outcome:?}"
    );
}

/// Sends a start command and expects the process actor to start successfully.
pub async fn expect_process_start(actor: &ActorRef<ProcessActorCommand>) -> ProcessActorStatus {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Start { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("start ask should receive a reply")
        .expect("process should start")
}

/// Sends a stop command and expects the process actor to stop successfully.
pub async fn expect_process_stop(actor: &ActorRef<ProcessActorCommand>) -> ProcessActorStatus {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Stop { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("stop ask should receive a reply")
        .expect("process should stop")
}

/// Sends a start command and returns the process actor result.
pub async fn start_process(
    actor: &ActorRef<ProcessActorCommand>,
) -> Result<ProcessActorStatus, ProcessError> {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Start { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("start ask should receive a reply")
}

/// Sends a stop command and returns the process actor result.
pub async fn stop_process(
    actor: &ActorRef<ProcessActorCommand>,
) -> Result<ProcessActorStatus, ProcessError> {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Stop { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("stop ask should receive a reply")
}

/// Reads current process actor status.
pub async fn process_status(actor: &ActorRef<ProcessActorCommand>) -> ProcessActorStatus {
    actor
        .ask(
            |reply_to| ProcessActorCommand::Status { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("status ask should receive a reply")
}

/// Waits until the process actor reaches the expected state.
pub async fn wait_for_process_state(
    actor: &ActorRef<ProcessActorCommand>,
    expected: ProcessActorState,
    timeout: Duration,
) -> ProcessActorStatus {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let status = process_status(actor).await;
        if status.state() == expected {
            return status;
        }

        if tokio::time::Instant::now() >= deadline {
            assert_eq!(status.state(), expected);
            return status;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Asserts that a status reports restart-budget exhaustion.
pub fn assert_restart_budget_exhausted(status: &ProcessActorStatus, max_restarts: usize) {
    assert!(matches!(
        status.last_error(),
        Some(ProcessError::RestartBudgetExhausted { max_restarts: observed })
            if *observed == max_restarts
    ));
}

/// Sends a stdio request and returns the typed process response or process error.
pub async fn request_stdio<Req, Resp>(
    actor: &ActorRef<StdioCommand<Req, Resp>>,
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
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("stdio ask should receive a reply")
}

/// Sends a stdio request and expects a successful typed response.
pub async fn expect_stdio_response<Req, Resp>(
    actor: &ActorRef<StdioCommand<Req, Resp>>,
    request: Req,
    timeout: Duration,
) -> Resp
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    request_stdio(actor, request, timeout)
        .await
        .expect("stdio request should receive a reply")
}

/// Reads current stdio protocol actor status.
pub async fn stdio_status<Req, Resp>(actor: &ActorRef<StdioCommand<Req, Resp>>) -> StdioStatus
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    actor
        .ask(
            |reply_to| StdioCommand::Status { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("stdio status ask should receive a reply")
}

/// Reads retained child stderr lines from a stdio protocol actor.
pub async fn stdio_stderr<Req, Resp>(actor: &ActorRef<StdioCommand<Req, Resp>>) -> Vec<String>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    actor
        .ask(
            |reply_to| StdioCommand::Stderr { reply_to },
            DEFAULT_TEST_TIMEOUT,
        )
        .await
        .expect("stdio stderr ask should receive a reply")
}

/// Waits until the stdio actor has the expected pending request count.
pub async fn wait_for_stdio_pending_count<Req, Resp>(
    actor: &ActorRef<StdioCommand<Req, Resp>>,
    expected: usize,
    timeout: Duration,
) where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let status = stdio_status(actor).await;
        if status.pending_count() == expected {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            assert_eq!(status.pending_count(), expected);
            return;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until retained stderr contains a line with the expected substring.
pub async fn wait_for_stderr_line<Req, Resp>(
    actor: &ActorRef<StdioCommand<Req, Resp>>,
    expected: &str,
    timeout: Duration,
) -> Vec<String>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let stderr = stdio_stderr(actor).await;
        if stderr.iter().any(|line| line.contains(expected)) {
            return stderr;
        }

        if tokio::time::Instant::now() >= deadline {
            assert_stderr_line_contains(&stderr, expected);
            return stderr;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn unique_name(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    format!("{}-{}-{nanos}", sanitize_name(name), std::process::id())
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character,
            _ => '-',
        })
        .collect()
}
