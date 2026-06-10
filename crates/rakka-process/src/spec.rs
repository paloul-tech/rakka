//! Process specification and validation.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

use crate::{ProcessError, ProcessResult};

/// Default child process startup timeout.
pub const DEFAULT_PROCESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Default child process graceful shutdown timeout.
pub const DEFAULT_PROCESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether child processes inherit the node environment by default.
pub const DEFAULT_PROCESS_INHERITS_ENVIRONMENT: bool = false;

/// Policy for wiring one child-process standard IO stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessStdio {
    /// Connect the stream to null.
    Null,
    /// Inherit the stream from the Rakka node process.
    Inherit,
    /// Create a pipe owned by Rakka.
    Piped,
}

/// Default stdin policy for a child process.
pub const DEFAULT_PROCESS_STDIN: ProcessStdio = ProcessStdio::Null;

/// Default stdout policy for a child process.
pub const DEFAULT_PROCESS_STDOUT: ProcessStdio = ProcessStdio::Null;

/// Default stderr policy for a child process.
pub const DEFAULT_PROCESS_STDERR: ProcessStdio = ProcessStdio::Null;

impl ProcessStdio {
    pub(crate) fn to_stdio(self) -> Stdio {
        match self {
            Self::Null => Stdio::null(),
            Self::Inherit => Stdio::inherit(),
            Self::Piped => Stdio::piped(),
        }
    }
}

/// Graceful shutdown action attempted before killing a child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GracefulShutdown {
    /// Do not attempt a graceful action before the shutdown timeout.
    None,
    /// Drop the owned stdin pipe so cooperative children can observe EOF.
    CloseStdin,
}

/// Default graceful shutdown policy for a child process.
pub const DEFAULT_PROCESS_GRACEFUL_SHUTDOWN: GracefulShutdown = GracefulShutdown::CloseStdin;

/// Optional resource hints for a child process.
///
/// These are declarative v1 hints. Enforcement belongs to deployment policy or
/// later platform integrations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceHints {
    cpu_millis: Option<u32>,
    memory_bytes: Option<u64>,
    file_descriptor_limit: Option<u32>,
    temp_dir: Option<PathBuf>,
}

impl ResourceHints {
    /// Creates empty resource hints.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cpu_millis: None,
            memory_bytes: None,
            file_descriptor_limit: None,
            temp_dir: None,
        }
    }

    /// Sets the requested CPU hint in millicores.
    #[must_use]
    pub const fn with_cpu_millis(mut self, cpu_millis: u32) -> Self {
        self.cpu_millis = Some(cpu_millis);
        self
    }

    /// Sets the requested memory hint in bytes.
    #[must_use]
    pub const fn with_memory_bytes(mut self, memory_bytes: u64) -> Self {
        self.memory_bytes = Some(memory_bytes);
        self
    }

    /// Sets the requested file descriptor limit hint.
    #[must_use]
    pub const fn with_file_descriptor_limit(mut self, file_descriptor_limit: u32) -> Self {
        self.file_descriptor_limit = Some(file_descriptor_limit);
        self
    }

    /// Sets the requested temporary directory hint.
    #[must_use]
    pub fn with_temp_dir(mut self, temp_dir: impl Into<PathBuf>) -> Self {
        self.temp_dir = Some(temp_dir.into());
        self
    }

    /// Requested CPU hint in millicores.
    #[must_use]
    pub const fn cpu_millis(&self) -> Option<u32> {
        self.cpu_millis
    }

    /// Requested memory hint in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> Option<u64> {
        self.memory_bytes
    }

    /// Requested file descriptor limit hint.
    #[must_use]
    pub const fn file_descriptor_limit(&self) -> Option<u32> {
        self.file_descriptor_limit
    }

    /// Requested temporary directory hint.
    #[must_use]
    pub fn temp_dir(&self) -> Option<&Path> {
        self.temp_dir.as_deref()
    }
}

/// Explicit executable allowlist used before spawning child processes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutableAllowlist {
    exact_paths: Vec<PathBuf>,
    directories: Vec<PathBuf>,
}

impl ExecutableAllowlist {
    /// Creates an empty allowlist that rejects every executable.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            exact_paths: Vec::new(),
            directories: Vec::new(),
        }
    }

    /// Creates an allowlist from exact executable paths.
    #[must_use]
    pub fn from_exact_paths(paths: impl IntoIterator<Item = impl Into<PathBuf>>) -> Self {
        let mut allowlist = Self::empty();
        for path in paths {
            allowlist = allowlist.allow_exact(path);
        }
        allowlist
    }

    /// Adds an exact executable path.
    #[must_use]
    pub fn allow_exact(mut self, path: impl Into<PathBuf>) -> Self {
        self.exact_paths.push(path.into());
        self
    }

    /// Adds an allowed executable directory.
    #[must_use]
    pub fn allow_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.directories.push(directory.into());
        self
    }

    /// Exact executable paths.
    #[must_use]
    pub fn exact_paths(&self) -> &[PathBuf] {
        &self.exact_paths
    }

    /// Allowed executable directories.
    #[must_use]
    pub fn directories(&self) -> &[PathBuf] {
        &self.directories
    }

    /// Returns true when the executable path is accepted by the allowlist.
    #[must_use]
    pub fn allows(&self, program: &Path) -> bool {
        self.exact_paths.iter().any(|allowed| allowed == program)
            || self
                .directories
                .iter()
                .any(|directory| program.starts_with(directory))
    }
}

/// Complete low-level child process configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    program: PathBuf,
    args: Vec<OsString>,
    env: BTreeMap<String, OsString>,
    cwd: Option<PathBuf>,
    stdin: ProcessStdio,
    stdout: ProcessStdio,
    stderr: ProcessStdio,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    graceful_shutdown: GracefulShutdown,
    inherit_environment: bool,
    resource_hints: ResourceHints,
}

impl ProcessSpec {
    /// Creates a process spec for an executable path.
    #[must_use]
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
            stdin: DEFAULT_PROCESS_STDIN,
            stdout: DEFAULT_PROCESS_STDOUT,
            stderr: DEFAULT_PROCESS_STDERR,
            startup_timeout: DEFAULT_PROCESS_STARTUP_TIMEOUT,
            shutdown_timeout: DEFAULT_PROCESS_SHUTDOWN_TIMEOUT,
            graceful_shutdown: DEFAULT_PROCESS_GRACEFUL_SHUTDOWN,
            inherit_environment: DEFAULT_PROCESS_INHERITS_ENVIRONMENT,
            resource_hints: ResourceHints::new(),
        }
    }

    /// Adds one process argument.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Adds process arguments.
    #[must_use]
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<OsString>>) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Adds one explicitly declared environment variable.
    #[must_use]
    pub fn env(mut self, name: impl Into<String>, value: impl Into<OsString>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    /// Sets the working directory.
    #[must_use]
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Sets stdin policy.
    #[must_use]
    pub const fn stdin(mut self, stdin: ProcessStdio) -> Self {
        self.stdin = stdin;
        self
    }

    /// Sets stdout policy.
    #[must_use]
    pub const fn stdout(mut self, stdout: ProcessStdio) -> Self {
        self.stdout = stdout;
        self
    }

    /// Sets stderr policy.
    #[must_use]
    pub const fn stderr(mut self, stderr: ProcessStdio) -> Self {
        self.stderr = stderr;
        self
    }

    /// Sets startup timeout.
    #[must_use]
    pub const fn startup_timeout(mut self, startup_timeout: Duration) -> Self {
        self.startup_timeout = startup_timeout;
        self
    }

    /// Sets shutdown timeout.
    #[must_use]
    pub const fn shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    /// Sets graceful shutdown policy.
    #[must_use]
    pub const fn graceful_shutdown(mut self, graceful_shutdown: GracefulShutdown) -> Self {
        self.graceful_shutdown = graceful_shutdown;
        self
    }

    /// Allows the child to inherit the Rakka node environment.
    ///
    /// This is disabled by default to avoid leaking undeclared secrets.
    #[must_use]
    pub const fn inherit_environment(mut self) -> Self {
        self.inherit_environment = true;
        self
    }

    /// Sets resource hints.
    #[must_use]
    pub fn resource_hints(mut self, resource_hints: ResourceHints) -> Self {
        self.resource_hints = resource_hints;
        self
    }

    /// Executable path.
    #[must_use]
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Process arguments.
    #[must_use]
    pub fn arguments(&self) -> &[OsString] {
        &self.args
    }

    /// Explicitly declared environment variables.
    #[must_use]
    pub fn environment(&self) -> &BTreeMap<String, OsString> {
        &self.env
    }

    /// Working directory.
    #[must_use]
    pub fn working_directory(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    /// Stdin policy.
    #[must_use]
    pub const fn stdin_policy(&self) -> ProcessStdio {
        self.stdin
    }

    /// Stdout policy.
    #[must_use]
    pub const fn stdout_policy(&self) -> ProcessStdio {
        self.stdout
    }

    /// Stderr policy.
    #[must_use]
    pub const fn stderr_policy(&self) -> ProcessStdio {
        self.stderr
    }

    /// Startup timeout.
    #[must_use]
    pub const fn startup_timeout_duration(&self) -> Duration {
        self.startup_timeout
    }

    /// Shutdown timeout.
    #[must_use]
    pub const fn shutdown_timeout_duration(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Graceful shutdown policy.
    #[must_use]
    pub const fn graceful_shutdown_policy(&self) -> GracefulShutdown {
        self.graceful_shutdown
    }

    /// Whether the child inherits the parent process environment.
    #[must_use]
    pub const fn inherits_environment(&self) -> bool {
        self.inherit_environment
    }

    /// Resource hints.
    #[must_use]
    pub const fn resource_hints_ref(&self) -> &ResourceHints {
        &self.resource_hints
    }

    /// Validates the spec against an executable allowlist.
    pub fn validate(&self, allowlist: &ExecutableAllowlist) -> ProcessResult<()> {
        if self.program.as_os_str().is_empty() {
            return Err(ProcessError::EmptyProgram);
        }

        if !self.program.is_absolute() {
            return Err(ProcessError::RelativeProgram {
                program: self.program.clone(),
            });
        }

        if !allowlist.allows(&self.program) {
            return Err(ProcessError::ProgramNotAllowed {
                program: self.program.clone(),
            });
        }

        for name in self.env.keys() {
            if name.is_empty() || name.contains('=') {
                return Err(ProcessError::InvalidEnvironmentName { name: name.clone() });
            }
        }

        if let Some(cwd) = &self.cwd {
            if !cwd.is_absolute() {
                return Err(ProcessError::RelativeWorkingDirectory { cwd: cwd.clone() });
            }
        }

        Ok(())
    }

    pub(crate) fn build_command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        if !self.inherit_environment {
            command.env_clear();
        }
        command.envs(self.env.iter().map(|(name, value)| (name.as_str(), value)));
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        command.stdin(self.stdin.to_stdio());
        command.stdout(self.stdout.to_stdio());
        command.stderr(self.stderr.to_stdio());
        command.kill_on_drop(true);
        command
    }
}

impl From<&OsStr> for ProcessSpec {
    fn from(program: &OsStr) -> Self {
        Self::new(PathBuf::from(program))
    }
}
