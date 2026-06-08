//! File-watch child-process interaction mode.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use crate::{
    capture::{join_limited_reader, spawn_limited_reader, CaptureTask},
    ExecutableAllowlist, ManagedProcess, ProcessError, ProcessExit, ProcessResult, ProcessShutdown,
    ProcessSpec, ProcessStdio,
};

const DEFAULT_FILE_WATCH_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_FILE_WATCH_POLL_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_FILE_WATCH_OUTPUT_LIMIT: usize = 1024 * 1024;

/// File written into a sandbox before a file-watch process starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchInput {
    relative_path: PathBuf,
    contents: Vec<u8>,
    overwrite: bool,
}

impl FileWatchInput {
    /// Creates a sandbox-relative input file.
    #[must_use]
    pub fn new(relative_path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) -> Self {
        Self {
            relative_path: relative_path.into(),
            contents: contents.into(),
            overwrite: true,
        }
    }

    /// Prevents replacing an existing sandbox file.
    #[must_use]
    pub const fn without_overwrite(mut self) -> Self {
        self.overwrite = false;
        self
    }

    /// Sandbox-relative input path.
    #[must_use]
    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// Input file contents.
    #[must_use]
    pub fn contents(&self) -> &[u8] {
        &self.contents
    }

    /// Returns true when this input may replace an existing file.
    #[must_use]
    pub const fn overwrite(&self) -> bool {
        self.overwrite
    }
}

/// Completion condition for a file-watch process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileWatchCompletion {
    /// Complete when a sandbox-relative file exists.
    FileExists {
        /// Sandbox-relative file path.
        relative_path: PathBuf,
    },
    /// Complete when a sandbox-relative file exists and is not empty.
    FileNonEmpty {
        /// Sandbox-relative file path.
        relative_path: PathBuf,
    },
}

impl FileWatchCompletion {
    /// Completes when a sandbox-relative file exists.
    #[must_use]
    pub fn file_exists(relative_path: impl Into<PathBuf>) -> Self {
        Self::FileExists {
            relative_path: relative_path.into(),
        }
    }

    /// Completes when a sandbox-relative file exists and is not empty.
    #[must_use]
    pub fn file_non_empty(relative_path: impl Into<PathBuf>) -> Self {
        Self::FileNonEmpty {
            relative_path: relative_path.into(),
        }
    }

    fn relative_path(&self) -> &Path {
        match self {
            Self::FileExists { relative_path } | Self::FileNonEmpty { relative_path } => {
                relative_path
            }
        }
    }
}

/// Declared output file collected from the sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchOutputPolicy {
    relative_path: PathBuf,
    required: bool,
}

impl FileWatchOutputPolicy {
    /// Creates a required sandbox-relative output policy.
    #[must_use]
    pub fn required(relative_path: impl Into<PathBuf>) -> Self {
        Self {
            relative_path: relative_path.into(),
            required: true,
        }
    }

    /// Creates an optional sandbox-relative output policy.
    #[must_use]
    pub fn optional(relative_path: impl Into<PathBuf>) -> Self {
        Self {
            relative_path: relative_path.into(),
            required: false,
        }
    }

    /// Sandbox-relative output path.
    #[must_use]
    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// Returns true when missing output is an error for completed runs.
    #[must_use]
    pub const fn required_value(&self) -> bool {
        self.required
    }
}

/// Captured sandbox output file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchOutput {
    relative_path: PathBuf,
    contents: Vec<u8>,
}

impl FileWatchOutput {
    /// Creates captured sandbox output.
    #[must_use]
    pub fn new(relative_path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) -> Self {
        Self {
            relative_path: relative_path.into(),
            contents: contents.into(),
        }
    }

    /// Sandbox-relative output path.
    #[must_use]
    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// Output file contents.
    #[must_use]
    pub fn contents(&self) -> &[u8] {
        &self.contents
    }
}

/// Sandbox cleanup policy after a file-watch run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileWatchCleanup {
    /// Leave the sandbox on disk.
    Keep,
    /// Remove the sandbox after a completed run.
    RemoveOnSuccess,
    /// Remove the sandbox after any terminal outcome.
    RemoveAlways,
}

/// Configuration for file-watch process mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchConfig {
    sandbox_dir: PathBuf,
    completion: FileWatchCompletion,
    inputs: Vec<FileWatchInput>,
    outputs: Vec<FileWatchOutputPolicy>,
    timeout: Duration,
    poll_interval: Duration,
    cleanup: FileWatchCleanup,
    stdout_limit: usize,
    stderr_limit: usize,
}

impl FileWatchConfig {
    /// Creates file-watch configuration for an explicit sandbox and completion condition.
    #[must_use]
    pub fn new(sandbox_dir: impl Into<PathBuf>, completion: FileWatchCompletion) -> Self {
        Self {
            sandbox_dir: sandbox_dir.into(),
            completion,
            inputs: Vec::new(),
            outputs: Vec::new(),
            timeout: DEFAULT_FILE_WATCH_TIMEOUT,
            poll_interval: DEFAULT_FILE_WATCH_POLL_INTERVAL,
            cleanup: FileWatchCleanup::Keep,
            stdout_limit: DEFAULT_FILE_WATCH_OUTPUT_LIMIT,
            stderr_limit: DEFAULT_FILE_WATCH_OUTPUT_LIMIT,
        }
    }

    /// Adds one sandbox input file.
    #[must_use]
    pub fn input(mut self, input: FileWatchInput) -> Self {
        self.inputs.push(input);
        self
    }

    /// Adds one required sandbox output file.
    #[must_use]
    pub fn required_output(mut self, relative_path: impl Into<PathBuf>) -> Self {
        self.outputs
            .push(FileWatchOutputPolicy::required(relative_path));
        self
    }

    /// Adds one optional sandbox output file.
    #[must_use]
    pub fn optional_output(mut self, relative_path: impl Into<PathBuf>) -> Self {
        self.outputs
            .push(FileWatchOutputPolicy::optional(relative_path));
        self
    }

    /// Sets completion timeout.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets completion polling interval.
    #[must_use]
    pub const fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets sandbox cleanup policy.
    #[must_use]
    pub const fn cleanup(mut self, cleanup: FileWatchCleanup) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Sets maximum captured stdout bytes.
    #[must_use]
    pub const fn stdout_limit(mut self, stdout_limit: usize) -> Self {
        self.stdout_limit = stdout_limit;
        self
    }

    /// Sets maximum captured stderr bytes.
    #[must_use]
    pub const fn stderr_limit(mut self, stderr_limit: usize) -> Self {
        self.stderr_limit = stderr_limit;
        self
    }

    /// Sandbox directory.
    #[must_use]
    pub fn sandbox_dir(&self) -> &Path {
        &self.sandbox_dir
    }

    /// Completion condition.
    #[must_use]
    pub const fn completion(&self) -> &FileWatchCompletion {
        &self.completion
    }

    /// Input file policies.
    #[must_use]
    pub fn inputs(&self) -> &[FileWatchInput] {
        &self.inputs
    }

    /// Output file policies.
    #[must_use]
    pub fn outputs(&self) -> &[FileWatchOutputPolicy] {
        &self.outputs
    }

    /// Completion timeout.
    #[must_use]
    pub const fn timeout_duration(&self) -> Duration {
        self.timeout
    }

    /// Completion polling interval.
    #[must_use]
    pub const fn poll_interval_duration(&self) -> Duration {
        self.poll_interval
    }

    /// Cleanup policy.
    #[must_use]
    pub const fn cleanup_policy(&self) -> FileWatchCleanup {
        self.cleanup
    }
}

/// Completed file-watch run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchCompleted {
    /// Collected output files.
    pub outputs: Vec<FileWatchOutput>,
    /// Captured process stdout.
    pub stdout: Vec<u8>,
    /// Captured process stderr.
    pub stderr: Vec<u8>,
    /// Exit observed before completion, when the child exited on its own.
    pub exit: Option<ProcessExit>,
    /// Shutdown result when Rakka stopped the child after completion.
    pub shutdown: Option<ProcessShutdown>,
}

/// Timed-out file-watch run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchTimedOut {
    /// Timeout that elapsed.
    pub timeout: Duration,
    /// Collected output files that existed before timeout.
    pub outputs: Vec<FileWatchOutput>,
    /// Captured process stdout.
    pub stdout: Vec<u8>,
    /// Captured process stderr.
    pub stderr: Vec<u8>,
    /// Shutdown result after timeout.
    pub shutdown: ProcessShutdown,
}

/// File-watch run where the process exited before completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWatchProcessExited {
    /// Exit observed before completion.
    pub exit: ProcessExit,
    /// Collected output files that existed before process exit.
    pub outputs: Vec<FileWatchOutput>,
    /// Captured process stdout.
    pub stdout: Vec<u8>,
    /// Captured process stderr.
    pub stderr: Vec<u8>,
}

/// Terminal file-watch run outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileWatchOutcome {
    /// Completion condition was satisfied.
    Completed(FileWatchCompleted),
    /// Timeout elapsed before completion.
    TimedOut(FileWatchTimedOut),
    /// Child process exited before completion.
    ProcessExited(FileWatchProcessExited),
}

/// Runs a child process in a sandbox and watches for file completion.
pub async fn run_file_watch(
    spec: ProcessSpec,
    allowlist: &ExecutableAllowlist,
    config: FileWatchConfig,
) -> ProcessResult<FileWatchOutcome> {
    validate_config(&config)?;
    prepare_sandbox(&config)?;

    let run_spec = spec
        .cwd(config.sandbox_dir.clone())
        .stdin(ProcessStdio::Piped)
        .stdout(ProcessStdio::Piped)
        .stderr(ProcessStdio::Piped);
    let mut process = ManagedProcess::spawn(run_spec, allowlist)?;
    let stdout = process
        .take_stdout()
        .ok_or_else(|| ProcessError::MissingPipe {
            stream: "stdout".to_string(),
        })?;
    let stderr = process
        .take_stderr()
        .ok_or_else(|| ProcessError::MissingPipe {
            stream: "stderr".to_string(),
        })?;
    let stdout_task = spawn_limited_reader("stdout", stdout, config.stdout_limit);
    let stderr_task = spawn_limited_reader("stderr", stderr, config.stderr_limit);
    let deadline = tokio::time::Instant::now() + config.timeout;

    loop {
        if completion_satisfied(&config)? {
            let shutdown = process.shutdown().await?;
            let output = collect_output(&config, stdout_task, stderr_task, true).await?;
            cleanup_sandbox(&config, true)?;
            return Ok(FileWatchOutcome::Completed(FileWatchCompleted {
                outputs: output.outputs,
                stdout: output.stdout,
                stderr: output.stderr,
                exit: None,
                shutdown: Some(shutdown),
            }));
        }

        if let Some(exit) = process.try_wait()? {
            let completed = completion_satisfied(&config)?;
            let output = collect_output(&config, stdout_task, stderr_task, completed).await?;
            cleanup_sandbox(&config, completed)?;
            if completed {
                return Ok(FileWatchOutcome::Completed(FileWatchCompleted {
                    outputs: output.outputs,
                    stdout: output.stdout,
                    stderr: output.stderr,
                    exit: Some(exit),
                    shutdown: None,
                }));
            }
            return Ok(FileWatchOutcome::ProcessExited(FileWatchProcessExited {
                exit,
                outputs: output.outputs,
                stdout: output.stdout,
                stderr: output.stderr,
            }));
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            let shutdown = process.shutdown().await?;
            let output = collect_output(&config, stdout_task, stderr_task, false).await?;
            cleanup_sandbox(&config, false)?;
            return Ok(FileWatchOutcome::TimedOut(FileWatchTimedOut {
                timeout: config.timeout,
                outputs: output.outputs,
                stdout: output.stdout,
                stderr: output.stderr,
                shutdown,
            }));
        }

        tokio::time::sleep(config.poll_interval.min(deadline - now)).await;
    }
}

struct CapturedFileWatchOutput {
    outputs: Vec<FileWatchOutput>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn collect_output(
    config: &FileWatchConfig,
    stdout_task: CaptureTask,
    stderr_task: CaptureTask,
    completed: bool,
) -> ProcessResult<CapturedFileWatchOutput> {
    let outputs = read_outputs(config, completed)?;
    let stdout = join_limited_reader("stdout", stdout_task).await?;
    let stderr = join_limited_reader("stderr", stderr_task).await?;
    Ok(CapturedFileWatchOutput {
        outputs,
        stdout,
        stderr,
    })
}

fn validate_config(config: &FileWatchConfig) -> ProcessResult<()> {
    if config.sandbox_dir.as_os_str().is_empty() || !config.sandbox_dir.is_absolute() {
        return Err(ProcessError::InvalidSandboxDirectory {
            path: config.sandbox_dir.clone(),
        });
    }
    validate_sandbox_relative(config.completion.relative_path())?;
    for input in &config.inputs {
        validate_sandbox_relative(input.relative_path())?;
    }
    for output in &config.outputs {
        validate_sandbox_relative(output.relative_path())?;
    }
    Ok(())
}

fn prepare_sandbox(config: &FileWatchConfig) -> ProcessResult<()> {
    std::fs::create_dir_all(&config.sandbox_dir).map_err(|error| ProcessError::FileIo {
        path: config.sandbox_dir.clone(),
        message: error.to_string(),
    })?;
    for input in &config.inputs {
        let path = sandbox_path(&config.sandbox_dir, input.relative_path())?;
        if !input.overwrite() && path.exists() {
            return Err(ProcessError::FileIo {
                path,
                message: "input file already exists".to_string(),
            });
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| ProcessError::FileIo {
                path: parent.to_path_buf(),
                message: error.to_string(),
            })?;
        }
        std::fs::write(&path, input.contents()).map_err(|error| ProcessError::FileIo {
            path,
            message: error.to_string(),
        })?;
    }
    Ok(())
}

fn completion_satisfied(config: &FileWatchConfig) -> ProcessResult<bool> {
    let path = sandbox_path(&config.sandbox_dir, config.completion.relative_path())?;
    match &config.completion {
        FileWatchCompletion::FileExists { .. } => Ok(path.exists()),
        FileWatchCompletion::FileNonEmpty { .. } => {
            let Ok(metadata) = std::fs::metadata(&path) else {
                return Ok(false);
            };
            Ok(metadata.len() > 0)
        }
    }
}

fn read_outputs(
    config: &FileWatchConfig,
    require_missing_required: bool,
) -> ProcessResult<Vec<FileWatchOutput>> {
    let mut outputs = Vec::new();
    for output in &config.outputs {
        let path = sandbox_path(&config.sandbox_dir, output.relative_path())?;
        if !path.exists() {
            if require_missing_required && output.required_value() {
                return Err(ProcessError::FileIo {
                    path,
                    message: "required output file is missing".to_string(),
                });
            }
            continue;
        }
        let contents = std::fs::read(&path).map_err(|error| ProcessError::FileIo {
            path: path.clone(),
            message: error.to_string(),
        })?;
        outputs.push(FileWatchOutput::new(
            output.relative_path().to_path_buf(),
            contents,
        ));
    }
    Ok(outputs)
}

fn cleanup_sandbox(config: &FileWatchConfig, completed: bool) -> ProcessResult<()> {
    let should_remove = matches!(config.cleanup, FileWatchCleanup::RemoveAlways)
        || (completed && matches!(config.cleanup, FileWatchCleanup::RemoveOnSuccess));
    if !should_remove || !config.sandbox_dir.exists() {
        return Ok(());
    }
    std::fs::remove_dir_all(&config.sandbox_dir).map_err(|error| ProcessError::FileIo {
        path: config.sandbox_dir.clone(),
        message: error.to_string(),
    })
}

fn sandbox_path(sandbox_dir: &Path, relative_path: &Path) -> ProcessResult<PathBuf> {
    validate_sandbox_relative(relative_path)?;
    Ok(sandbox_dir.join(relative_path))
}

fn validate_sandbox_relative(relative_path: &Path) -> ProcessResult<()> {
    if relative_path.as_os_str().is_empty() || relative_path.is_absolute() {
        return Err(ProcessError::SandboxPathEscape {
            path: relative_path.to_path_buf(),
        });
    }
    for component in relative_path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(ProcessError::SandboxPathEscape {
                    path: relative_path.to_path_buf(),
                });
            }
        }
    }
    Ok(())
}
