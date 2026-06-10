//! Process actor error types.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use rakka_core::{RakkaError, Subsystem};

/// Convenient result alias for process actor operations.
pub type ProcessResult<T> = Result<T, ProcessError>;

/// Failure returned by process configuration and lifecycle primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessError {
    /// The configured executable path was empty.
    EmptyProgram,
    /// The configured executable path was not absolute.
    RelativeProgram {
        /// Program path that was rejected.
        program: PathBuf,
    },
    /// The configured executable path was not accepted by the allowlist.
    ProgramNotAllowed {
        /// Program path that was rejected.
        program: PathBuf,
    },
    /// An environment variable name was empty or contained `=`.
    InvalidEnvironmentName {
        /// Environment variable name that was rejected.
        name: String,
    },
    /// A configured working directory was not an absolute path.
    RelativeWorkingDirectory {
        /// Working directory path that was rejected.
        cwd: PathBuf,
    },
    /// The child process could not be spawned.
    Spawn {
        /// Program path used for spawn.
        program: PathBuf,
        /// Operating-system error message.
        message: String,
    },
    /// Waiting for the child process failed.
    Wait {
        /// Program path used for spawn.
        program: PathBuf,
        /// Operating-system error message.
        message: String,
    },
    /// Killing the child process failed.
    Kill {
        /// Program path used for spawn.
        program: PathBuf,
        /// Operating-system error message.
        message: String,
    },
    /// The process had already been reaped by this owner.
    AlreadyReaped {
        /// Program path used for spawn.
        program: PathBuf,
    },
    /// The process did not stop within the configured timeout.
    ShutdownTimeout {
        /// Program path used for spawn.
        program: PathBuf,
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// The process actor already owns a running child process.
    AlreadyRunning {
        /// Running process id, when available.
        pid: Option<u32>,
    },
    /// The process actor does not currently own a child process.
    NotRunning,
    /// The process actor failed startup readiness before the configured timeout.
    StartupTimeout {
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// The child exited before startup readiness completed.
    ExitedDuringStartup {
        /// Process exit code, when available.
        code: Option<i32>,
        /// Unix signal that terminated the process, when available.
        signal: Option<i32>,
    },
    /// A running child process exited unexpectedly.
    UnexpectedExit {
        /// Process exit code, when available.
        code: Option<i32>,
        /// Unix signal that terminated the process, when available.
        signal: Option<i32>,
    },
    /// A readiness or health check failed.
    Unhealthy {
        /// Health failure detail.
        message: String,
    },
    /// The restart policy exhausted its allowed restart budget.
    RestartBudgetExhausted {
        /// Maximum allowed restarts.
        max_restarts: usize,
    },
    /// The process actor is in a terminal failed state.
    Terminal {
        /// Terminal failure detail.
        message: String,
    },
    /// A required standard IO pipe was not configured as piped.
    MissingPipe {
        /// Standard IO stream name.
        stream: String,
    },
    /// The child stdin pipe was closed before a request could be written.
    StdinClosed,
    /// The child stdout pipe closed before a pending reply was received.
    StdoutClosed,
    /// Reading a child standard IO stream failed.
    StdioRead {
        /// Standard IO stream name.
        stream: String,
        /// Operating-system error message.
        message: String,
    },
    /// Writing to child stdin failed.
    StdioWrite {
        /// Operating-system error message.
        message: String,
    },
    /// Captured stdout or stderr exceeded its configured byte limit.
    OutputLimitExceeded {
        /// Standard IO stream name.
        stream: String,
        /// Configured byte limit.
        limit: usize,
    },
    /// The pending request table reached its configured capacity.
    PendingCapacity {
        /// Configured pending request capacity.
        capacity: usize,
    },
    /// A request timed out before a matching reply arrived.
    RequestTimeout {
        /// Request id that timed out.
        request_id: String,
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// A request could not be encoded for the process protocol.
    ProtocolEncode {
        /// Encoding failure detail.
        message: String,
    },
    /// A stdout frame could not be decoded.
    MalformedStdout {
        /// Decode failure detail.
        message: String,
    },
    /// A reply arrived for a request that is no longer pending.
    UnknownReply {
        /// Reply request id.
        request_id: String,
    },
    /// A reply arrived for a request id that has already completed or timed out.
    DuplicateReply {
        /// Reply request id.
        request_id: String,
    },
    /// The process protocol has closed and cannot accept new requests.
    ProtocolClosed {
        /// Closure detail.
        message: String,
    },
    /// A file-watch sandbox directory was missing or unsafe.
    InvalidSandboxDirectory {
        /// Sandbox directory path that was rejected.
        path: PathBuf,
    },
    /// A configured sandbox-relative path escaped the sandbox.
    SandboxPathEscape {
        /// Path that was rejected.
        path: PathBuf,
    },
    /// Reading, writing, creating, or removing a sandbox file failed.
    FileIo {
        /// File or directory path involved in the failure.
        path: PathBuf,
        /// Operating-system error message.
        message: String,
    },
    /// A local endpoint did not become reachable before timeout.
    EndpointTimeout {
        /// Human-readable endpoint description.
        endpoint: String,
        /// Timeout that elapsed.
        timeout: Duration,
        /// Last connection error observed before timing out.
        last_error: Option<String>,
    },
}

impl ProcessError {
    /// Converts this error to a framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Process, self.code(), self.to_string())
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyProgram => "empty-program",
            Self::RelativeProgram { .. } => "relative-program",
            Self::ProgramNotAllowed { .. } => "program-not-allowed",
            Self::InvalidEnvironmentName { .. } => "invalid-environment-name",
            Self::RelativeWorkingDirectory { .. } => "relative-working-directory",
            Self::Spawn { .. } => "spawn-error",
            Self::Wait { .. } => "wait-error",
            Self::Kill { .. } => "kill-error",
            Self::AlreadyReaped { .. } => "already-reaped",
            Self::ShutdownTimeout { .. } => "shutdown-timeout",
            Self::AlreadyRunning { .. } => "already-running",
            Self::NotRunning => "not-running",
            Self::StartupTimeout { .. } => "startup-timeout",
            Self::ExitedDuringStartup { .. } => "exited-during-startup",
            Self::UnexpectedExit { .. } => "unexpected-exit",
            Self::Unhealthy { .. } => "unhealthy",
            Self::RestartBudgetExhausted { .. } => "restart-budget-exhausted",
            Self::Terminal { .. } => "terminal",
            Self::MissingPipe { .. } => "missing-pipe",
            Self::StdinClosed => "stdin-closed",
            Self::StdoutClosed => "stdout-closed",
            Self::StdioRead { .. } => "stdio-read",
            Self::StdioWrite { .. } => "stdio-write",
            Self::OutputLimitExceeded { .. } => "output-limit-exceeded",
            Self::PendingCapacity { .. } => "pending-capacity",
            Self::RequestTimeout { .. } => "request-timeout",
            Self::ProtocolEncode { .. } => "protocol-encode",
            Self::MalformedStdout { .. } => "malformed-stdout",
            Self::UnknownReply { .. } => "unknown-reply",
            Self::DuplicateReply { .. } => "duplicate-reply",
            Self::ProtocolClosed { .. } => "protocol-closed",
            Self::InvalidSandboxDirectory { .. } => "invalid-sandbox-directory",
            Self::SandboxPathEscape { .. } => "sandbox-path-escape",
            Self::FileIo { .. } => "file-io",
            Self::EndpointTimeout { .. } => "endpoint-timeout",
        }
    }
}

impl Display for ProcessError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyProgram => f.write_str("process program path cannot be empty"),
            Self::RelativeProgram { program } => {
                write!(
                    f,
                    "process program path must be absolute: {}",
                    program.display()
                )
            }
            Self::ProgramNotAllowed { program } => {
                write!(
                    f,
                    "process program is not allowlisted: {}",
                    program.display()
                )
            }
            Self::InvalidEnvironmentName { name } => {
                write!(f, "process environment variable name is invalid: {name}")
            }
            Self::RelativeWorkingDirectory { cwd } => {
                write!(
                    f,
                    "process working directory must be absolute: {}",
                    cwd.display()
                )
            }
            Self::Spawn { program, message } => {
                write!(
                    f,
                    "failed to spawn process {}: {message}",
                    program.display()
                )
            }
            Self::Wait { program, message } => {
                write!(
                    f,
                    "failed to wait for process {}: {message}",
                    program.display()
                )
            }
            Self::Kill { program, message } => {
                write!(f, "failed to kill process {}: {message}", program.display())
            }
            Self::AlreadyReaped { program } => {
                write!(f, "process {} has already been reaped", program.display())
            }
            Self::ShutdownTimeout { program, timeout } => {
                write!(
                    f,
                    "process {} did not stop within {:?}",
                    program.display(),
                    timeout
                )
            }
            Self::AlreadyRunning { pid } => {
                write!(f, "process actor already owns running child {pid:?}")
            }
            Self::NotRunning => f.write_str("process actor does not own a running child"),
            Self::StartupTimeout { timeout } => {
                write!(f, "process startup readiness timed out after {timeout:?}")
            }
            Self::ExitedDuringStartup { code, signal } => write!(
                f,
                "process exited during startup readiness with code {code:?} signal {signal:?}"
            ),
            Self::UnexpectedExit { code, signal } => write!(
                f,
                "process exited unexpectedly with code {code:?} signal {signal:?}"
            ),
            Self::Unhealthy { message } => write!(f, "process health check failed: {message}"),
            Self::RestartBudgetExhausted { max_restarts } => write!(
                f,
                "process restart budget exhausted after {max_restarts} restarts"
            ),
            Self::Terminal { message } => {
                write!(f, "process actor is terminally failed: {message}")
            }
            Self::MissingPipe { stream } => {
                write!(f, "process {stream} must be configured as piped")
            }
            Self::StdinClosed => f.write_str("process stdin pipe is closed"),
            Self::StdoutClosed => f.write_str("process stdout pipe is closed"),
            Self::StdioRead { stream, message } => {
                write!(f, "failed to read process {stream}: {message}")
            }
            Self::StdioWrite { message } => {
                write!(f, "failed to write process stdin: {message}")
            }
            Self::OutputLimitExceeded { stream, limit } => {
                write!(
                    f,
                    "captured process {stream} exceeded configured limit of {limit} bytes"
                )
            }
            Self::PendingCapacity { capacity } => {
                write!(
                    f,
                    "process protocol pending request capacity {capacity} was reached"
                )
            }
            Self::RequestTimeout {
                request_id,
                timeout,
            } => {
                write!(
                    f,
                    "process protocol request {request_id} timed out after {timeout:?}"
                )
            }
            Self::ProtocolEncode { message } => {
                write!(f, "process protocol encode failed: {message}")
            }
            Self::MalformedStdout { message } => {
                write!(f, "process stdout frame was malformed: {message}")
            }
            Self::UnknownReply { request_id } => {
                write!(
                    f,
                    "process stdout reply referenced unknown request {request_id}"
                )
            }
            Self::DuplicateReply { request_id } => {
                write!(
                    f,
                    "process stdout reply duplicated completed request {request_id}"
                )
            }
            Self::ProtocolClosed { message } => {
                write!(f, "process protocol is closed: {message}")
            }
            Self::InvalidSandboxDirectory { path } => {
                write!(
                    f,
                    "file-watch sandbox directory is invalid: {}",
                    path.display()
                )
            }
            Self::SandboxPathEscape { path } => {
                write!(f, "file-watch path escapes sandbox: {}", path.display())
            }
            Self::FileIo { path, message } => {
                write!(f, "file-watch IO failed for {}: {message}", path.display())
            }
            Self::EndpointTimeout {
                endpoint,
                timeout,
                last_error,
            } => {
                write!(
                    f,
                    "local endpoint {endpoint} did not become ready within {timeout:?}"
                )?;
                if let Some(last_error) = last_error {
                    write!(f, ": {last_error}")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for ProcessError {}
