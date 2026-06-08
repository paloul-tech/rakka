//! Managed child-process lifecycle primitive.

use std::process::ExitStatus;
use std::time::Duration;

use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};

use crate::{ExecutableAllowlist, GracefulShutdown, ProcessError, ProcessResult, ProcessSpec};

/// Typed process start information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessStart {
    pid: Option<u32>,
}

impl ProcessStart {
    /// Creates process start information.
    #[must_use]
    pub const fn new(pid: Option<u32>) -> Self {
        Self { pid }
    }

    /// Operating-system process id, when available.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.pid
    }
}

/// Typed process exit information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessExit {
    pid: Option<u32>,
    success: bool,
    code: Option<i32>,
    signal: Option<i32>,
}

impl ProcessExit {
    /// Creates typed exit information from an operating-system exit status.
    #[must_use]
    pub fn from_status(pid: Option<u32>, status: ExitStatus) -> Self {
        Self {
            pid,
            success: status.success(),
            code: status.code(),
            signal: exit_signal(status),
        }
    }

    /// Operating-system process id, when available.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Returns true when the process exited successfully.
    #[must_use]
    pub const fn success(&self) -> bool {
        self.success
    }

    /// Process exit code, when available.
    #[must_use]
    pub const fn code(&self) -> Option<i32> {
        self.code
    }

    /// Unix signal that terminated the process, when available.
    #[must_use]
    pub const fn signal(&self) -> Option<i32> {
        self.signal
    }
}

/// Outcome of a managed shutdown attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessShutdownOutcome {
    /// The process had already exited before shutdown was requested.
    AlreadyExited,
    /// The process exited during the graceful shutdown window.
    Graceful,
    /// The process was killed after the graceful shutdown timeout elapsed.
    KilledAfterTimeout {
        /// Timeout that elapsed before the kill.
        timeout: Duration,
    },
}

/// Result of a managed shutdown attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessShutdown {
    outcome: ProcessShutdownOutcome,
    exit: ProcessExit,
}

impl ProcessShutdown {
    /// Creates a managed shutdown result.
    #[must_use]
    pub const fn new(outcome: ProcessShutdownOutcome, exit: ProcessExit) -> Self {
        Self { outcome, exit }
    }

    /// Shutdown outcome.
    #[must_use]
    pub const fn outcome(&self) -> &ProcessShutdownOutcome {
        &self.outcome
    }

    /// Final process exit information.
    #[must_use]
    pub const fn exit(&self) -> &ProcessExit {
        &self.exit
    }

    /// Returns true when the child had to be killed after timeout.
    #[must_use]
    pub const fn killed_after_timeout(&self) -> bool {
        matches!(
            self.outcome,
            ProcessShutdownOutcome::KilledAfterTimeout { .. }
        )
    }
}

/// Owned child process spawned from a validated `ProcessSpec`.
#[derive(Debug)]
pub struct ManagedProcess {
    spec: ProcessSpec,
    start: ProcessStart,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    exit: Option<ProcessExit>,
}

impl ManagedProcess {
    /// Spawns a child process after validating its spec against the allowlist.
    pub fn spawn(
        spec: ProcessSpec,
        allowlist: &ExecutableAllowlist,
    ) -> ProcessResult<ManagedProcess> {
        spec.validate(allowlist)?;
        let mut child = spec
            .build_command()
            .spawn()
            .map_err(|error| ProcessError::Spawn {
                program: spec.program().to_path_buf(),
                message: error.to_string(),
            })?;
        let start = ProcessStart::new(child.id());
        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        Ok(Self {
            spec,
            start,
            child: Some(child),
            stdin,
            stdout,
            stderr,
            exit: None,
        })
    }

    /// Process spec used to spawn the child.
    #[must_use]
    pub const fn spec(&self) -> &ProcessSpec {
        &self.spec
    }

    /// Process start information.
    #[must_use]
    pub const fn start(&self) -> &ProcessStart {
        &self.start
    }

    /// Operating-system process id, when available.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.start.pid()
    }

    /// Takes the owned stdin pipe, when configured as piped.
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.stdin.take()
    }

    /// Takes the owned stdout pipe, when configured as piped.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.stdout.take()
    }

    /// Takes the owned stderr pipe, when configured as piped.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.stderr.take()
    }

    /// Returns the already observed process exit, when available.
    #[must_use]
    pub const fn exit(&self) -> Option<&ProcessExit> {
        self.exit.as_ref()
    }

    /// Attempts a non-blocking wait for process exit.
    pub fn try_wait(&mut self) -> ProcessResult<Option<ProcessExit>> {
        if let Some(exit) = &self.exit {
            return Ok(Some(exit.clone()));
        }

        let Some(child) = self.child.as_mut() else {
            return Err(ProcessError::AlreadyReaped {
                program: self.spec.program().to_path_buf(),
            });
        };
        let Some(status) = child.try_wait().map_err(|error| ProcessError::Wait {
            program: self.spec.program().to_path_buf(),
            message: error.to_string(),
        })?
        else {
            return Ok(None);
        };

        let exit = ProcessExit::from_status(self.pid(), status);
        self.exit = Some(exit.clone());
        self.child = None;
        Ok(Some(exit))
    }

    /// Waits until the child exits and returns typed exit information.
    pub async fn wait(&mut self) -> ProcessResult<ProcessExit> {
        if let Some(exit) = &self.exit {
            return Ok(exit.clone());
        }

        let mut child = self
            .child
            .take()
            .ok_or_else(|| ProcessError::AlreadyReaped {
                program: self.spec.program().to_path_buf(),
            })?;
        let status = child.wait().await.map_err(|error| ProcessError::Wait {
            program: self.spec.program().to_path_buf(),
            message: error.to_string(),
        })?;
        let exit = ProcessExit::from_status(self.pid(), status);
        self.exit = Some(exit.clone());
        Ok(exit)
    }

    /// Attempts graceful shutdown and kills the child after the configured timeout.
    pub async fn shutdown(&mut self) -> ProcessResult<ProcessShutdown> {
        if let Some(exit) = &self.exit {
            return Ok(ProcessShutdown::new(
                ProcessShutdownOutcome::AlreadyExited,
                exit.clone(),
            ));
        }

        if let GracefulShutdown::CloseStdin = self.spec.graceful_shutdown_policy() {
            drop(self.stdin.take());
        }

        let timeout_duration = self.spec.shutdown_timeout_duration();
        let program = self.spec.program().to_path_buf();
        let mut child = self
            .child
            .take()
            .ok_or_else(|| ProcessError::AlreadyReaped {
                program: program.clone(),
            })?;

        match tokio::time::timeout(timeout_duration, child.wait()).await {
            Ok(Ok(status)) => {
                let exit = ProcessExit::from_status(self.pid(), status);
                self.exit = Some(exit.clone());
                Ok(ProcessShutdown::new(ProcessShutdownOutcome::Graceful, exit))
            }
            Ok(Err(error)) => Err(ProcessError::Wait {
                program,
                message: error.to_string(),
            }),
            Err(_elapsed) => {
                child.start_kill().map_err(|error| ProcessError::Kill {
                    program: program.clone(),
                    message: error.to_string(),
                })?;
                let status = child.wait().await.map_err(|error| ProcessError::Wait {
                    program,
                    message: error.to_string(),
                })?;
                let exit = ProcessExit::from_status(self.pid(), status);
                self.exit = Some(exit.clone());
                Ok(ProcessShutdown::new(
                    ProcessShutdownOutcome::KilledAfterTimeout {
                        timeout: timeout_duration,
                    },
                    exit,
                ))
            }
        }
    }
}

#[cfg(unix)]
fn exit_signal(status: ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;

    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: ExitStatus) -> Option<i32> {
    None
}
