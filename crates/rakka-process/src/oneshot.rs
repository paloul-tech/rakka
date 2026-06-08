//! One-shot child-process execution mode.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    capture::{join_limited_reader, spawn_limited_reader},
    ExecutableAllowlist, ProcessError, ProcessExit, ProcessResult, ProcessSpec, ProcessStdio,
};

const DEFAULT_ONE_SHOT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_OUTPUT_LIMIT: usize = 1024 * 1024;

/// Configuration for one-shot process execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneShotConfig {
    runtime_timeout: Duration,
    stdout_limit: usize,
    stderr_limit: usize,
    stdin: Option<Vec<u8>>,
}

impl OneShotConfig {
    /// Creates default one-shot execution configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            runtime_timeout: DEFAULT_ONE_SHOT_TIMEOUT,
            stdout_limit: DEFAULT_OUTPUT_LIMIT,
            stderr_limit: DEFAULT_OUTPUT_LIMIT,
            stdin: None,
        }
    }

    /// Sets the maximum runtime before the child is killed.
    #[must_use]
    pub const fn runtime_timeout(mut self, runtime_timeout: Duration) -> Self {
        self.runtime_timeout = runtime_timeout;
        self
    }

    /// Sets the maximum captured stdout bytes.
    #[must_use]
    pub const fn stdout_limit(mut self, stdout_limit: usize) -> Self {
        self.stdout_limit = stdout_limit;
        self
    }

    /// Sets the maximum captured stderr bytes.
    #[must_use]
    pub const fn stderr_limit(mut self, stderr_limit: usize) -> Self {
        self.stderr_limit = stderr_limit;
        self
    }

    /// Sets stdin bytes written before waiting for process exit.
    #[must_use]
    pub fn stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
        self.stdin = Some(stdin.into());
        self
    }

    /// Maximum runtime before the child is killed.
    #[must_use]
    pub const fn runtime_timeout_duration(&self) -> Duration {
        self.runtime_timeout
    }

    /// Maximum captured stdout bytes.
    #[must_use]
    pub const fn stdout_limit_value(&self) -> usize {
        self.stdout_limit
    }

    /// Maximum captured stderr bytes.
    #[must_use]
    pub const fn stderr_limit_value(&self) -> usize {
        self.stderr_limit
    }

    /// Optional stdin bytes.
    #[must_use]
    pub fn stdin_bytes(&self) -> Option<&[u8]> {
        self.stdin.as_deref()
    }
}

impl Default for OneShotConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Successful one-shot process output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneShotOutput {
    exit: ProcessExit,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl OneShotOutput {
    /// Creates one-shot process output.
    #[must_use]
    pub const fn new(exit: ProcessExit, stdout: Vec<u8>, stderr: Vec<u8>) -> Self {
        Self {
            exit,
            stdout,
            stderr,
        }
    }

    /// Process exit information.
    #[must_use]
    pub const fn exit(&self) -> &ProcessExit {
        &self.exit
    }

    /// Captured stdout.
    #[must_use]
    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    /// Captured stderr.
    #[must_use]
    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }
}

/// Result of a one-shot process run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OneShotOutcome {
    /// The process exited before the runtime timeout.
    Exited(OneShotOutput),
    /// The process exceeded the runtime timeout and was killed.
    TimedOut {
        /// Timeout that elapsed.
        timeout: Duration,
        /// Captured stdout before process termination.
        stdout: Vec<u8>,
        /// Captured stderr before process termination.
        stderr: Vec<u8>,
        /// Exit information after killing the process.
        exit: ProcessExit,
    },
}

impl OneShotOutcome {
    /// Captured stdout for either outcome.
    #[must_use]
    pub fn stdout(&self) -> &[u8] {
        match self {
            Self::Exited(output) => output.stdout(),
            Self::TimedOut { stdout, .. } => stdout,
        }
    }

    /// Captured stderr for either outcome.
    #[must_use]
    pub fn stderr(&self) -> &[u8] {
        match self {
            Self::Exited(output) => output.stderr(),
            Self::TimedOut { stderr, .. } => stderr,
        }
    }

    /// Process exit information for either outcome.
    #[must_use]
    pub const fn exit(&self) -> &ProcessExit {
        match self {
            Self::Exited(output) => output.exit(),
            Self::TimedOut { exit, .. } => exit,
        }
    }
}

/// Runs one process to completion with bounded runtime and output capture.
pub async fn run_one_shot(
    spec: ProcessSpec,
    allowlist: &ExecutableAllowlist,
    config: OneShotConfig,
) -> ProcessResult<OneShotOutcome> {
    let run_spec = spec
        .stdin(if config.stdin.is_some() {
            ProcessStdio::Piped
        } else {
            ProcessStdio::Null
        })
        .stdout(ProcessStdio::Piped)
        .stderr(ProcessStdio::Piped);
    run_spec.validate(allowlist)?;

    let mut child = run_spec
        .build_command()
        .spawn()
        .map_err(|error| ProcessError::Spawn {
            program: run_spec.program().to_path_buf(),
            message: error.to_string(),
        })?;
    let pid = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ProcessError::MissingPipe {
            stream: "stdout".to_string(),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ProcessError::MissingPipe {
            stream: "stderr".to_string(),
        })?;
    let stdout_task = spawn_limited_reader("stdout", stdout, config.stdout_limit);
    let stderr_task = spawn_limited_reader("stderr", stderr, config.stderr_limit);

    if let Some(stdin) = config.stdin {
        let mut child_stdin = child.stdin.take().ok_or(ProcessError::StdinClosed)?;
        child_stdin
            .write_all(&stdin)
            .await
            .map_err(|error| ProcessError::StdioWrite {
                message: error.to_string(),
            })?;
    }
    drop(child.stdin.take());

    match tokio::time::timeout(config.runtime_timeout, child.wait()).await {
        Ok(Ok(status)) => {
            let exit = ProcessExit::from_status(pid, status);
            let stdout = join_limited_reader("stdout", stdout_task).await?;
            let stderr = join_limited_reader("stderr", stderr_task).await?;
            Ok(OneShotOutcome::Exited(OneShotOutput::new(
                exit, stdout, stderr,
            )))
        }
        Ok(Err(error)) => Err(ProcessError::Wait {
            program: run_spec.program().to_path_buf(),
            message: error.to_string(),
        }),
        Err(_elapsed) => {
            child.start_kill().map_err(|error| ProcessError::Kill {
                program: run_spec.program().to_path_buf(),
                message: error.to_string(),
            })?;
            let status = child.wait().await.map_err(|error| ProcessError::Wait {
                program: run_spec.program().to_path_buf(),
                message: error.to_string(),
            })?;
            let exit = ProcessExit::from_status(pid, status);
            let stdout = join_limited_reader("stdout", stdout_task).await?;
            let stderr = join_limited_reader("stderr", stderr_task).await?;
            Ok(OneShotOutcome::TimedOut {
                timeout: config.runtime_timeout,
                stdout,
                stderr,
                exit,
            })
        }
    }
}
