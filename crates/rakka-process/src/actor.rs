//! Actor-owned process runtime and supervision.

use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorRef, ActorSystem,
    MetricsRecorder, RakkaResult, ReplyTo, METRIC_PROCESS_EXITS,
};
use serde::{Deserialize, Serialize};

use crate::{
    ExecutableAllowlist, ManagedProcess, ProcessError, ProcessExit, ProcessResult, ProcessSpec,
    ProcessStart,
};

const DEFAULT_SUPERVISION_INTERVAL: Duration = Duration::from_millis(50);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(5);
const EVENT_HISTORY_LIMIT: usize = 32;

/// Child-process actor state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessActorState {
    /// No child process is currently running.
    Idle,
    /// A child process is starting and waiting for readiness.
    Starting,
    /// A child process is running.
    Running,
    /// The actor is waiting before a restart attempt.
    Restarting,
    /// The actor stopped its child process.
    Stopped,
    /// The actor reached a terminal failure state.
    Failed,
}

impl ProcessActorState {
    /// Stable state label used for metrics and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Restarting => "restarting",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

/// Health status observed by the process actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessHealth {
    /// Health has not been checked yet or readiness is still pending.
    Unknown,
    /// Process is considered healthy.
    Healthy,
    /// Process is unhealthy with a diagnostic message.
    Unhealthy {
        /// Health failure detail.
        message: String,
    },
}

impl ProcessHealth {
    /// Creates an unhealthy status.
    #[must_use]
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self::Unhealthy {
            message: message.into(),
        }
    }

    /// Returns true when the status is healthy.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Stable health label used for metrics and diagnostics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Unhealthy { .. } => "unhealthy",
        }
    }
}

/// Synchronous readiness or health check hook.
#[derive(Clone)]
pub struct ProcessCheck {
    check: Arc<dyn Fn(&ProcessActorStatus) -> ProcessHealth + Send + Sync>,
}

impl ProcessCheck {
    /// Creates a check that always reports healthy.
    #[must_use]
    pub fn healthy() -> Self {
        Self::from_fn(|_status| ProcessHealth::Healthy)
    }

    /// Creates a check that always reports unknown.
    #[must_use]
    pub fn unknown() -> Self {
        Self::from_fn(|_status| ProcessHealth::Unknown)
    }

    /// Creates a check that always reports unhealthy.
    #[must_use]
    pub fn unhealthy(message: impl Into<String>) -> Self {
        let message = message.into();
        Self::from_fn(move |_status| ProcessHealth::unhealthy(message.clone()))
    }

    /// Creates a custom synchronous check hook.
    #[must_use]
    pub fn from_fn(
        check: impl Fn(&ProcessActorStatus) -> ProcessHealth + Send + Sync + 'static,
    ) -> Self {
        Self {
            check: Arc::new(check),
        }
    }

    fn check(&self, status: &ProcessActorStatus) -> ProcessHealth {
        (self.check)(status)
    }
}

impl Debug for ProcessCheck {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessCheck").finish_non_exhaustive()
    }
}

/// Optional restart jitter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessRestartJitter {
    /// No restart jitter.
    None,
    /// Deterministic pseudo-random jitter bounded by `max_jitter`.
    Deterministic {
        /// Maximum jitter added to a restart delay.
        max_jitter: Duration,
    },
}

/// Restart policy for process actor supervision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessRestartPolicy {
    min_backoff: Duration,
    max_backoff: Duration,
    max_restarts: usize,
    jitter: ProcessRestartJitter,
}

impl ProcessRestartPolicy {
    /// Creates a policy that never restarts failed child processes.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            min_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            max_restarts: 0,
            jitter: ProcessRestartJitter::None,
        }
    }

    /// Creates an exponential backoff restart policy.
    #[must_use]
    pub const fn exponential(
        min_backoff: Duration,
        max_backoff: Duration,
        max_restarts: usize,
    ) -> Self {
        Self {
            min_backoff,
            max_backoff,
            max_restarts,
            jitter: ProcessRestartJitter::None,
        }
    }

    /// Sets restart jitter.
    #[must_use]
    pub const fn with_jitter(mut self, jitter: ProcessRestartJitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Initial restart backoff.
    #[must_use]
    pub const fn min_backoff(&self) -> Duration {
        self.min_backoff
    }

    /// Maximum restart backoff.
    #[must_use]
    pub const fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Maximum allowed restarts.
    #[must_use]
    pub const fn max_restarts(&self) -> usize {
        self.max_restarts
    }

    /// Restart jitter policy.
    #[must_use]
    pub const fn jitter(&self) -> ProcessRestartJitter {
        self.jitter
    }

    fn can_restart(self, restart_count: usize) -> bool {
        restart_count < self.max_restarts
    }

    fn delay_for(self, restart_count: usize) -> Duration {
        let factor = 1u32.checked_shl(restart_count.min(16) as u32).unwrap_or(1);
        let base = self
            .min_backoff
            .saturating_mul(factor)
            .min(self.max_backoff);
        base.saturating_add(jitter_duration(self.jitter, restart_count))
    }
}

impl Default for ProcessRestartPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Whether the process actor starts its child automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessActorStartMode {
    /// Wait for an explicit start command.
    Manual,
    /// Start the child when the actor starts.
    OnActorStart,
}

/// Supervision event emitted by the process actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessSupervisionEvent {
    /// Child process started.
    Started {
        /// Process start information.
        start: ProcessStart,
    },
    /// Child process exited.
    Exited {
        /// Process exit information.
        exit: ProcessExit,
    },
    /// Child process was stopped by command or actor shutdown.
    Stopped,
    /// Restart has been scheduled.
    RestartScheduled {
        /// Restart attempt number, starting at one.
        attempt: usize,
        /// Delay before the attempt.
        delay: Duration,
        /// Failure that caused the restart.
        reason: ProcessError,
    },
    /// Health check failed.
    HealthCheckFailed {
        /// Health failure detail.
        message: String,
    },
    /// Restart budget was exhausted.
    RestartBudgetExhausted {
        /// Maximum allowed restarts.
        max_restarts: usize,
    },
}

/// Current process actor status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessActorStatus {
    state: ProcessActorState,
    pid: Option<u32>,
    restart_count: usize,
    health: ProcessHealth,
    last_exit: Option<ProcessExit>,
    last_error: Option<ProcessError>,
    last_event: Option<ProcessSupervisionEvent>,
}

impl ProcessActorStatus {
    /// Creates a process actor status snapshot.
    #[must_use]
    pub const fn new(
        state: ProcessActorState,
        pid: Option<u32>,
        restart_count: usize,
        health: ProcessHealth,
        last_exit: Option<ProcessExit>,
        last_error: Option<ProcessError>,
        last_event: Option<ProcessSupervisionEvent>,
    ) -> Self {
        Self {
            state,
            pid,
            restart_count,
            health,
            last_exit,
            last_error,
            last_event,
        }
    }

    /// Current actor state.
    #[must_use]
    pub const fn state(&self) -> ProcessActorState {
        self.state
    }

    /// Running process id, when available.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Number of restart attempts consumed.
    #[must_use]
    pub const fn restart_count(&self) -> usize {
        self.restart_count
    }

    /// Last observed health status.
    #[must_use]
    pub const fn health(&self) -> &ProcessHealth {
        &self.health
    }

    /// Last process exit, when available.
    #[must_use]
    pub const fn last_exit(&self) -> Option<&ProcessExit> {
        self.last_exit.as_ref()
    }

    /// Last process actor error, when available.
    #[must_use]
    pub const fn last_error(&self) -> Option<&ProcessError> {
        self.last_error.as_ref()
    }

    /// Last supervision event, when available.
    #[must_use]
    pub const fn last_event(&self) -> Option<&ProcessSupervisionEvent> {
        self.last_event.as_ref()
    }

    /// Returns a serializable operational snapshot for this process actor.
    #[must_use]
    pub fn operational_snapshot(
        &self,
        process_name: impl Into<String>,
    ) -> ProcessOperationalSnapshot {
        ProcessOperationalSnapshot::from_status(process_name, self)
    }

    /// Records process exit metrics when this status contains a last exit.
    pub fn record_metrics(
        &self,
        recorder: &dyn MetricsRecorder,
        process_name: &str,
    ) -> ProcessOperationalSnapshot {
        let snapshot = self.operational_snapshot(process_name);
        if let Some(exit) = self.last_exit() {
            let success = exit.success().to_string();
            let code = exit
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "none".to_string());
            let signal = exit
                .signal()
                .map(|signal| signal.to_string())
                .unwrap_or_else(|| "none".to_string());
            recorder.increment_counter(
                METRIC_PROCESS_EXITS,
                1,
                &[
                    ("process", process_name),
                    ("state", self.state().as_str()),
                    ("success", success.as_str()),
                    ("code", code.as_str()),
                    ("signal", signal.as_str()),
                ],
            );
        }
        snapshot
    }
}

/// Serializable process actor operational snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessOperationalSnapshot {
    process_name: String,
    state: String,
    pid: Option<u32>,
    restart_count: usize,
    health: String,
    last_exit_code: Option<i32>,
    last_exit_signal: Option<i32>,
    last_exit_success: Option<bool>,
    last_error: Option<String>,
}

impl ProcessOperationalSnapshot {
    /// Creates a process operational snapshot from actor status.
    #[must_use]
    pub fn from_status(process_name: impl Into<String>, status: &ProcessActorStatus) -> Self {
        Self {
            process_name: process_name.into(),
            state: status.state().as_str().to_string(),
            pid: status.pid(),
            restart_count: status.restart_count(),
            health: status.health().as_str().to_string(),
            last_exit_code: status.last_exit().and_then(crate::ProcessExit::code),
            last_exit_signal: status.last_exit().and_then(crate::ProcessExit::signal),
            last_exit_success: status.last_exit().map(crate::ProcessExit::success),
            last_error: status.last_error().map(ToString::to_string),
        }
    }

    /// Process label supplied by the caller.
    #[must_use]
    pub fn process_name(&self) -> &str {
        &self.process_name
    }

    /// Process actor state label.
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Running process id, when available.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Restart attempts consumed.
    #[must_use]
    pub const fn restart_count(&self) -> usize {
        self.restart_count
    }

    /// Health label.
    #[must_use]
    pub fn health(&self) -> &str {
        &self.health
    }

    /// Last exit code, when available.
    #[must_use]
    pub const fn last_exit_code(&self) -> Option<i32> {
        self.last_exit_code
    }

    /// Last terminating signal, when available.
    #[must_use]
    pub const fn last_exit_signal(&self) -> Option<i32> {
        self.last_exit_signal
    }

    /// Last exit success flag, when available.
    #[must_use]
    pub const fn last_exit_success(&self) -> Option<bool> {
        self.last_exit_success
    }

    /// Last process actor error, when available.
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

/// Process actor configuration.
#[derive(Clone)]
pub struct ProcessActorConfig {
    spec: ProcessSpec,
    allowlist: ExecutableAllowlist,
    start_mode: ProcessActorStartMode,
    restart_policy: ProcessRestartPolicy,
    readiness_check: ProcessCheck,
    health_check: ProcessCheck,
    supervision_interval: Option<Duration>,
}

impl ProcessActorConfig {
    /// Creates a process actor configuration.
    #[must_use]
    pub fn new(spec: ProcessSpec, allowlist: ExecutableAllowlist) -> Self {
        Self {
            spec,
            allowlist,
            start_mode: ProcessActorStartMode::Manual,
            restart_policy: ProcessRestartPolicy::disabled(),
            readiness_check: ProcessCheck::healthy(),
            health_check: ProcessCheck::healthy(),
            supervision_interval: Some(DEFAULT_SUPERVISION_INTERVAL),
        }
    }

    /// Starts the child process when the actor starts.
    #[must_use]
    pub const fn start_on_actor_start(mut self) -> Self {
        self.start_mode = ProcessActorStartMode::OnActorStart;
        self
    }

    /// Sets process actor start mode.
    #[must_use]
    pub const fn start_mode(mut self, start_mode: ProcessActorStartMode) -> Self {
        self.start_mode = start_mode;
        self
    }

    /// Sets restart policy.
    #[must_use]
    pub const fn restart_policy(mut self, restart_policy: ProcessRestartPolicy) -> Self {
        self.restart_policy = restart_policy;
        self
    }

    /// Sets startup readiness check.
    #[must_use]
    pub fn readiness_check(mut self, readiness_check: ProcessCheck) -> Self {
        self.readiness_check = readiness_check;
        self
    }

    /// Sets periodic health check.
    #[must_use]
    pub fn health_check(mut self, health_check: ProcessCheck) -> Self {
        self.health_check = health_check;
        self
    }

    /// Sets the supervision tick interval.
    #[must_use]
    pub const fn supervision_interval(mut self, supervision_interval: Duration) -> Self {
        self.supervision_interval = Some(supervision_interval);
        self
    }

    /// Disables periodic supervision ticks.
    #[must_use]
    pub const fn without_supervision_interval(mut self) -> Self {
        self.supervision_interval = None;
        self
    }

    /// Process spec.
    #[must_use]
    pub const fn spec(&self) -> &ProcessSpec {
        &self.spec
    }

    /// Executable allowlist.
    #[must_use]
    pub const fn allowlist(&self) -> &ExecutableAllowlist {
        &self.allowlist
    }

    /// Start mode.
    #[must_use]
    pub const fn start_mode_ref(&self) -> ProcessActorStartMode {
        self.start_mode
    }

    /// Restart policy.
    #[must_use]
    pub const fn restart_policy_ref(&self) -> ProcessRestartPolicy {
        self.restart_policy
    }
}

impl Debug for ProcessActorConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessActorConfig")
            .field("spec", &self.spec)
            .field("allowlist", &self.allowlist)
            .field("start_mode", &self.start_mode)
            .field("restart_policy", &self.restart_policy)
            .field("readiness_check", &self.readiness_check)
            .field("health_check", &self.health_check)
            .field("supervision_interval", &self.supervision_interval)
            .finish()
    }
}

/// Process actor command protocol.
#[derive(Debug)]
pub enum ProcessActorCommand {
    /// Starts the child process.
    Start {
        /// Reply with the resulting status or failure.
        reply_to: ReplyTo<ProcessResult<ProcessActorStatus>>,
    },
    /// Stops the child process.
    Stop {
        /// Reply with the resulting status or failure.
        reply_to: ReplyTo<ProcessResult<ProcessActorStatus>>,
    },
    /// Restarts the child process.
    Restart {
        /// Reply with the resulting status or failure.
        reply_to: ReplyTo<ProcessResult<ProcessActorStatus>>,
    },
    /// Returns current status.
    Status {
        /// Reply with current status.
        reply_to: ReplyTo<ProcessActorStatus>,
    },
    /// Runs a health check immediately.
    CheckHealth {
        /// Reply with the resulting status or failure.
        reply_to: ReplyTo<ProcessResult<ProcessActorStatus>>,
    },
    #[doc(hidden)]
    SupervisionTick {
        #[doc(hidden)]
        token: u64,
    },
}

/// Actor that owns and supervises one child process.
pub struct ProcessActor {
    config: ProcessActorConfig,
    process: Option<ManagedProcess>,
    state: ProcessActorState,
    restart_count: usize,
    health: ProcessHealth,
    last_exit: Option<ProcessExit>,
    last_error: Option<ProcessError>,
    events: VecDeque<ProcessSupervisionEvent>,
    tick_token: u64,
}

impl ProcessActor {
    /// Creates a process actor.
    #[must_use]
    pub fn new(config: ProcessActorConfig) -> Self {
        Self {
            config,
            process: None,
            state: ProcessActorState::Idle,
            restart_count: 0,
            health: ProcessHealth::Unknown,
            last_exit: None,
            last_error: None,
            events: VecDeque::new(),
            tick_token: 0,
        }
    }

    /// Current status snapshot.
    #[must_use]
    pub fn status(&self) -> ProcessActorStatus {
        ProcessActorStatus::new(
            self.state,
            self.process.as_ref().and_then(ManagedProcess::pid),
            self.restart_count,
            self.health.clone(),
            self.last_exit.clone(),
            self.last_error.clone(),
            self.events.back().cloned(),
        )
    }

    async fn start_supervised(
        &mut self,
        ctx: &mut ActorContext<ProcessActorCommand>,
    ) -> ProcessResult<ProcessActorStatus> {
        if let Some(process) = &self.process {
            return Err(ProcessError::AlreadyRunning { pid: process.pid() });
        }

        match self.start_once(ctx).await {
            Ok(status) => Ok(status),
            Err(error) => self.supervise_failure(ctx, error, false).await,
        }
    }

    async fn restart(
        &mut self,
        ctx: &mut ActorContext<ProcessActorCommand>,
    ) -> ProcessResult<ProcessActorStatus> {
        let _shutdown = self.shutdown_owned_process().await;
        self.state = ProcessActorState::Idle;
        self.health = ProcessHealth::Unknown;
        self.start_supervised(ctx).await
    }

    async fn stop(&mut self) -> ProcessResult<ProcessActorStatus> {
        if self.process.is_none() {
            self.state = ProcessActorState::Stopped;
            return Err(ProcessError::NotRunning);
        }

        self.shutdown_owned_process().await?;
        self.state = ProcessActorState::Stopped;
        self.health = ProcessHealth::Unknown;
        self.push_event(ProcessSupervisionEvent::Stopped);
        Ok(self.status())
    }

    async fn check_health(
        &mut self,
        ctx: &mut ActorContext<ProcessActorCommand>,
    ) -> ProcessResult<ProcessActorStatus> {
        self.check_process_exit()?;
        let status = self.status();
        match self.config.health_check.check(&status) {
            ProcessHealth::Healthy => {
                self.health = ProcessHealth::Healthy;
                Ok(self.status())
            }
            ProcessHealth::Unknown => {
                self.health = ProcessHealth::Unknown;
                Ok(self.status())
            }
            ProcessHealth::Unhealthy { message } => {
                self.health = ProcessHealth::Unhealthy {
                    message: message.clone(),
                };
                self.push_event(ProcessSupervisionEvent::HealthCheckFailed {
                    message: message.clone(),
                });
                let error = ProcessError::Unhealthy { message };
                self.shutdown_owned_process().await?;
                self.supervise_failure(ctx, error, false).await
            }
        }
    }

    async fn supervision_tick(&mut self, ctx: &mut ActorContext<ProcessActorCommand>, token: u64) {
        if token != self.tick_token || self.state != ProcessActorState::Running {
            return;
        }

        let result = match self.check_process_exit() {
            Ok(()) => self.check_health(ctx).await,
            Err(error) => self.supervise_failure(ctx, error, true).await,
        };

        if result.is_ok() && self.state == ProcessActorState::Running {
            self.schedule_supervision_tick(ctx);
        }
    }

    async fn start_once(
        &mut self,
        ctx: &mut ActorContext<ProcessActorCommand>,
    ) -> ProcessResult<ProcessActorStatus> {
        self.state = ProcessActorState::Starting;
        self.health = ProcessHealth::Unknown;
        self.last_error = None;
        let process = ManagedProcess::spawn(self.config.spec.clone(), &self.config.allowlist)?;
        let start = process.start().clone();
        self.process = Some(process);
        self.push_event(ProcessSupervisionEvent::Started { start });

        if let Err(error) = self.wait_until_ready().await {
            self.last_error = Some(error.clone());
            let _shutdown = self.shutdown_owned_process().await;
            return Err(error);
        }

        self.state = ProcessActorState::Running;
        self.health = ProcessHealth::Healthy;
        self.schedule_supervision_tick(ctx);
        Ok(self.status())
    }

    async fn wait_until_ready(&mut self) -> ProcessResult<()> {
        let startup_timeout = self.config.spec.startup_timeout_duration();
        let deadline = tokio::time::Instant::now() + startup_timeout;

        loop {
            if let Some(exit) = self.try_reap_exit()? {
                return Err(ProcessError::ExitedDuringStartup {
                    code: exit.code(),
                    signal: exit.signal(),
                });
            }

            let readiness = self.config.readiness_check.check(&self.status());
            match readiness {
                ProcessHealth::Healthy => return Ok(()),
                ProcessHealth::Unhealthy { message } => {
                    return Err(ProcessError::Unhealthy { message });
                }
                ProcessHealth::Unknown => {}
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(ProcessError::StartupTimeout {
                    timeout: startup_timeout,
                });
            }

            tokio::time::sleep(READINESS_POLL_INTERVAL.min(deadline - now)).await;
        }
    }

    async fn supervise_failure(
        &mut self,
        ctx: &mut ActorContext<ProcessActorCommand>,
        error: ProcessError,
        budget_exhaustion_is_terminal: bool,
    ) -> ProcessResult<ProcessActorStatus> {
        self.last_error = Some(error.clone());
        if !self.config.restart_policy.can_restart(self.restart_count) {
            self.state = ProcessActorState::Failed;
            self.health = ProcessHealth::Unhealthy {
                message: error.to_string(),
            };
            if budget_exhaustion_is_terminal
                && (self.config.restart_policy.max_restarts() > 0 || self.restart_count > 0)
            {
                let exhausted = ProcessError::RestartBudgetExhausted {
                    max_restarts: self.config.restart_policy.max_restarts(),
                };
                self.last_error = Some(exhausted.clone());
                self.push_event(ProcessSupervisionEvent::RestartBudgetExhausted {
                    max_restarts: self.config.restart_policy.max_restarts(),
                });
                return Err(exhausted);
            }

            return Err(error);
        }

        let mut reason = error;
        loop {
            let delay = self.config.restart_policy.delay_for(self.restart_count);
            let attempt = self.restart_count + 1;
            self.restart_count = attempt;
            self.state = ProcessActorState::Restarting;
            self.health = ProcessHealth::Unknown;
            self.push_event(ProcessSupervisionEvent::RestartScheduled {
                attempt,
                delay,
                reason: reason.clone(),
            });
            tokio::time::sleep(delay).await;

            match self.start_once(ctx).await {
                Ok(status) => return Ok(status),
                Err(error) => {
                    self.last_error = Some(error.clone());
                    reason = error;
                    if !self.config.restart_policy.can_restart(self.restart_count) {
                        self.state = ProcessActorState::Failed;
                        self.health = ProcessHealth::Unhealthy {
                            message: reason.to_string(),
                        };
                        let exhausted = ProcessError::RestartBudgetExhausted {
                            max_restarts: self.config.restart_policy.max_restarts(),
                        };
                        self.last_error = Some(exhausted.clone());
                        self.push_event(ProcessSupervisionEvent::RestartBudgetExhausted {
                            max_restarts: self.config.restart_policy.max_restarts(),
                        });
                        return Err(exhausted);
                    }
                }
            }
        }
    }

    fn check_process_exit(&mut self) -> ProcessResult<()> {
        if let Some(exit) = self.try_reap_exit()? {
            return Err(ProcessError::UnexpectedExit {
                code: exit.code(),
                signal: exit.signal(),
            });
        }

        Ok(())
    }

    fn try_reap_exit(&mut self) -> ProcessResult<Option<ProcessExit>> {
        let Some(process) = self.process.as_mut() else {
            return Ok(None);
        };
        let Some(exit) = process.try_wait()? else {
            return Ok(None);
        };

        self.last_exit = Some(exit.clone());
        self.push_event(ProcessSupervisionEvent::Exited { exit: exit.clone() });
        self.process = None;
        Ok(Some(exit))
    }

    async fn shutdown_owned_process(&mut self) -> ProcessResult<()> {
        let Some(mut process) = self.process.take() else {
            return Ok(());
        };
        let shutdown = process.shutdown().await?;
        self.last_exit = Some(shutdown.exit().clone());
        Ok(())
    }

    fn schedule_supervision_tick(&mut self, ctx: &ActorContext<ProcessActorCommand>) {
        if let Some(interval) = self.config.supervision_interval {
            self.tick_token = self.tick_token.wrapping_add(1);
            let token = self.tick_token;
            let _timer =
                ctx.schedule_once(interval, ProcessActorCommand::SupervisionTick { token });
        }
    }

    fn push_event(&mut self, event: ProcessSupervisionEvent) {
        if self.events.len() == EVENT_HISTORY_LIMIT {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }
}

impl Actor for ProcessActor {
    type Msg = ProcessActorCommand;

    fn started<'a>(&'a mut self, ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async move {
            if self.config.start_mode == ProcessActorStartMode::OnActorStart {
                let _result = self.start_supervised(ctx).await;
            }
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                ProcessActorCommand::Start { reply_to } => {
                    let _sent = reply_to.reply(self.start_supervised(ctx).await);
                }
                ProcessActorCommand::Stop { reply_to } => {
                    let _sent = reply_to.reply(self.stop().await);
                }
                ProcessActorCommand::Restart { reply_to } => {
                    let _sent = reply_to.reply(self.restart(ctx).await);
                }
                ProcessActorCommand::Status { reply_to } => {
                    let _sent = reply_to.reply(self.status());
                }
                ProcessActorCommand::CheckHealth { reply_to } => {
                    let _sent = reply_to.reply(self.check_health(ctx).await);
                }
                ProcessActorCommand::SupervisionTick { token } => {
                    self.supervision_tick(ctx, token).await;
                }
            }

            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a rakka_core::TerminationReason,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            let _shutdown = self.shutdown_owned_process().await;
            Ok(ActorAction::Continue)
        })
    }
}

/// Spawns a process actor in an actor system.
pub fn spawn_process_actor(
    system: &ActorSystem,
    name: impl AsRef<str>,
    config: ProcessActorConfig,
) -> RakkaResult<ActorRef<ProcessActorCommand>> {
    system.spawn_actor(name, ProcessActor::new(config))
}

fn jitter_duration(jitter: ProcessRestartJitter, restart_count: usize) -> Duration {
    match jitter {
        ProcessRestartJitter::None => Duration::ZERO,
        ProcessRestartJitter::Deterministic { max_jitter } => {
            let max_nanos = max_jitter.as_nanos().min(u128::from(u64::MAX)) as u64;
            if max_nanos == 0 {
                return Duration::ZERO;
            }

            let seed = (restart_count as u64)
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            Duration::from_nanos(seed % max_nanos.saturating_add(1))
        }
    }
}
