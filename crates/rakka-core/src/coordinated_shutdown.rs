//! Coordinated shutdown phase and task registry.
//!
//! This module provides Rakka's Akka-shaped coordinated shutdown vocabulary.
//! Slice 7A defines the public phase graph and task registration surface. Later
//! slices attach the runner to `ActorSystem` termination and operational
//! adapters.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::metrics::{
    MetricsRecorder, NoopMetricsRecorder, METRIC_SHUTDOWN_PHASE_DURATION_MS,
    METRIC_SHUTDOWN_RUNNING, METRIC_SHUTDOWN_TASK_DURATION_MS, METRIC_SHUTDOWN_TASK_FAILURES,
    METRIC_SHUTDOWN_TIMEOUTS,
};
use crate::system::ActorSystem;
use crate::{RakkaError, RakkaResult};

const STOP_INGRESS: &str = "stop-ingress";
const DRAIN_ADAPTERS: &str = "drain-http-grpc-and-streams";
const LEAVE_CLUSTER: &str = "leave-cluster";
const HANDOFF_SHARDS: &str = "handoff-shards";
const STOP_PROCESS_ACTORS: &str = "stop-process-actors";
const FLUSH_PERSISTENCE: &str = "flush-persistence";
const STOP_USER_ACTORS: &str = "stop-user-actors";
const STOP_SYSTEM_ACTORS: &str = "stop-system-actors";
const STOP_REMOTING: &str = "stop-remoting";

const BUILT_IN_PHASES: &[&str] = &[
    STOP_INGRESS,
    DRAIN_ADAPTERS,
    LEAVE_CLUSTER,
    HANDOFF_SHARDS,
    STOP_PROCESS_ACTORS,
    FLUSH_PERSISTENCE,
    STOP_USER_ACTORS,
    STOP_SYSTEM_ACTORS,
    STOP_REMOTING,
];

const STANDALONE_METRICS_SYSTEM: &str = "standalone";

/// Future returned by a coordinated shutdown task.
pub type ShutdownTaskFuture = Pin<Box<dyn Future<Output = ShutdownTaskResult> + Send + 'static>>;

/// Result returned by a coordinated shutdown task.
pub type ShutdownTaskResult = RakkaResult<()>;

/// Result returned by coordinated shutdown runner APIs.
pub type CoordinatedShutdownResult<T> = Result<T, CoordinatedShutdownError>;

type ShutdownTaskRunner = dyn Fn(ShutdownTaskContext) -> ShutdownTaskFuture + Send + Sync + 'static;

type ShutdownRunResult = CoordinatedShutdownResult<CoordinatedShutdownReport>;

/// Registry of named shutdown phases and tasks.
#[derive(Clone)]
pub struct CoordinatedShutdown {
    inner: Arc<Mutex<ShutdownRegistry>>,
}

impl CoordinatedShutdown {
    /// Returns the coordinated shutdown registry owned by an actor system.
    ///
    /// ```no_run
    /// use rakka_core::{
    ///     ActorSystem, CoordinatedShutdown, ShutdownOutcome, ShutdownPhase,
    /// };
    ///
    /// # async fn example() -> rakka_core::RakkaResult<()> {
    /// let system = ActorSystem::new("docs");
    /// let shutdown = CoordinatedShutdown::get(&system);
    /// shutdown.add_task(ShutdownPhase::flush_persistence(), "flush-cache", |_context| async {
    ///     Ok(())
    /// })?;
    ///
    /// let report = system.terminate_with_report().await.unwrap();
    /// assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn get(system: &ActorSystem) -> Self {
        system.coordinated_shutdown()
    }

    /// Creates a registry with default settings and built-in phases.
    #[must_use]
    pub fn new() -> Self {
        Self::with_settings(CoordinatedShutdownSettings::default())
    }

    /// Creates a registry with custom settings and built-in phases.
    #[must_use]
    pub fn with_settings(settings: CoordinatedShutdownSettings) -> Self {
        Self::with_settings_and_metrics(
            settings,
            STANDALONE_METRICS_SYSTEM,
            Arc::new(NoopMetricsRecorder),
        )
    }

    /// Creates a registry with default settings, built-in phases, and a metrics recorder.
    #[must_use]
    pub fn with_metrics(system_name: impl Into<String>, metrics: Arc<dyn MetricsRecorder>) -> Self {
        Self::with_settings_and_metrics(
            CoordinatedShutdownSettings::default(),
            system_name,
            metrics,
        )
    }

    /// Creates a registry with custom settings, built-in phases, and a metrics recorder.
    #[must_use]
    pub fn with_settings_and_metrics(
        settings: CoordinatedShutdownSettings,
        system_name: impl Into<String>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ShutdownRegistry::new(
                settings,
                system_name,
                metrics,
            ))),
        }
    }

    /// Returns the registry settings.
    #[must_use]
    pub fn settings(&self) -> CoordinatedShutdownSettings {
        self.lock().settings
    }

    /// Adds a custom phase that runs after an existing phase.
    pub fn add_phase_after(
        &self,
        name: impl Into<String>,
        after: ShutdownPhase,
    ) -> RakkaResult<ShutdownPhase> {
        self.lock().add_phase_after(name.into(), after)
    }

    /// Adds a custom phase that runs before an existing phase.
    pub fn add_phase_before(
        &self,
        name: impl Into<String>,
        before: ShutdownPhase,
    ) -> RakkaResult<ShutdownPhase> {
        self.lock().add_phase_before(name.into(), before)
    }

    /// Adds a dependency requiring `phase` to run after `depends_on`.
    pub fn add_phase_dependency(
        &self,
        phase: ShutdownPhase,
        depends_on: ShutdownPhase,
    ) -> RakkaResult<()> {
        self.lock().add_phase_dependency(phase, depends_on)
    }

    /// Returns built-in and custom phases in deterministic dependency order.
    pub fn phases(&self) -> RakkaResult<Vec<ShutdownPhase>> {
        self.lock().ordered_phases()
    }

    /// Returns true when a phase is registered.
    #[must_use]
    pub fn contains_phase(&self, phase: &ShutdownPhase) -> bool {
        self.lock().phases.contains_key(phase.name())
    }

    /// Returns the number of registered phases.
    #[must_use]
    pub fn phase_count(&self) -> usize {
        self.lock().phases.len()
    }

    /// Adds a shutdown task with default task options.
    pub fn add_task<F, Fut>(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
        run: F,
    ) -> RakkaResult<ShutdownTask>
    where
        F: Fn(ShutdownTaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ShutdownTaskResult> + Send + 'static,
    {
        self.add_task_with_options(phase, name, ShutdownTaskOptions::default(), run)
    }

    /// Adds a shutdown task with explicit task options.
    pub fn add_task_with_options<F, Fut>(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
        options: ShutdownTaskOptions,
        run: F,
    ) -> RakkaResult<ShutdownTask>
    where
        F: Fn(ShutdownTaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ShutdownTaskResult> + Send + 'static,
    {
        let runner: Arc<ShutdownTaskRunner> = Arc::new(move |context| Box::pin(run(context)));
        self.lock().add_task(phase, name.into(), options, runner)
    }

    /// Returns all registered task descriptors in deterministic phase order.
    pub fn tasks(&self) -> RakkaResult<Vec<ShutdownTask>> {
        self.lock().ordered_tasks()
    }

    /// Returns task descriptors registered for one phase.
    pub fn tasks_for_phase(&self, phase: &ShutdownPhase) -> RakkaResult<Vec<ShutdownTask>> {
        self.lock().tasks_for_phase(phase)
    }

    /// Runs coordinated shutdown once with the supplied reason.
    ///
    /// Repeated calls return the original completed result. Concurrent calls
    /// await the in-flight shutdown instead of running tasks a second time.
    pub async fn run(
        &self,
        reason: CoordinatedShutdownReason,
    ) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
        self.run_internal(reason, None).await
    }

    /// Runs coordinated shutdown once with an overall deadline.
    ///
    /// If another caller already started shutdown, this call observes that
    /// in-flight run instead of replacing its deadline.
    pub async fn run_with_deadline(
        &self,
        reason: CoordinatedShutdownReason,
        deadline: Instant,
    ) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
        self.run_internal(reason, Some(deadline)).await
    }

    /// Returns a serializable snapshot of the current shutdown state.
    #[must_use]
    pub fn snapshot(&self) -> CoordinatedShutdownSnapshot {
        self.lock().snapshot()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ShutdownRegistry> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn run_internal(
        &self,
        reason: CoordinatedShutdownReason,
        deadline: Option<Instant>,
    ) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
        loop {
            let action = {
                let mut registry = self.lock();
                match &registry.run_state {
                    ShutdownRunState::NotStarted => {
                        let progress = Arc::new(Mutex::new(ShutdownProgress::default()));
                        let plan = registry.execution_plan(progress.clone())?;
                        let (sender, receiver) = watch::channel(None::<ShutdownRunResult>);
                        registry.run_state = ShutdownRunState::Running {
                            receiver: receiver.clone(),
                            progress,
                        };
                        ShutdownRunAction::Start {
                            sender,
                            plan,
                            reason: reason.clone(),
                            deadline,
                        }
                    }
                    ShutdownRunState::Running { receiver, .. } => {
                        ShutdownRunAction::Wait(receiver.clone())
                    }
                    ShutdownRunState::Finished { result } => {
                        return result.clone();
                    }
                }
            };

            match action {
                ShutdownRunAction::Start {
                    sender,
                    plan,
                    reason,
                    deadline,
                } => {
                    let result = execute_shutdown_plan(plan, reason, deadline).await;
                    let _sent = sender.send(Some(result.clone()));
                    self.lock().run_state = ShutdownRunState::Finished {
                        result: result.clone(),
                    };
                    return result;
                }
                ShutdownRunAction::Wait(mut receiver) => loop {
                    if let Some(result) = receiver.borrow().clone() {
                        return result;
                    }

                    if receiver.changed().await.is_err() {
                        break;
                    }
                },
            }
        }
    }
}

impl Default for CoordinatedShutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for CoordinatedShutdown {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let registry = self.lock();
        f.debug_struct("CoordinatedShutdown")
            .field("settings", &registry.settings)
            .field("phase_count", &registry.phases.len())
            .field("task_count", &registry.task_count())
            .field("outcome", &registry.snapshot().outcome())
            .finish()
    }
}

/// Error returned by coordinated shutdown runner APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatedShutdownError {
    /// Shutdown could not start because the registry was invalid.
    Registry {
        /// Underlying registry error.
        error: RakkaError,
    },
    /// Shutdown stopped because a fail-fast task returned an error.
    Failed {
        /// Partial shutdown report.
        report: CoordinatedShutdownReport,
    },
    /// Shutdown stopped because a task, phase, or overall deadline elapsed.
    TimedOut {
        /// Partial shutdown report.
        report: CoordinatedShutdownReport,
    },
}

impl CoordinatedShutdownError {
    /// Returns the partial shutdown report when available.
    #[must_use]
    pub const fn report(&self) -> Option<&CoordinatedShutdownReport> {
        match self {
            Self::Registry { .. } => None,
            Self::Failed { report } | Self::TimedOut { report } => Some(report),
        }
    }

    /// Returns the shutdown outcome represented by this error.
    #[must_use]
    pub const fn outcome(&self) -> ShutdownOutcome {
        match self {
            Self::Registry { .. } => ShutdownOutcome::Failed,
            Self::Failed { .. } => ShutdownOutcome::Failed,
            Self::TimedOut { .. } => ShutdownOutcome::TimedOut,
        }
    }
}

impl Display for CoordinatedShutdownError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry { error } => Display::fmt(error, f),
            Self::Failed { report } => write!(
                f,
                "coordinated shutdown '{}' failed",
                report.reason().code()
            ),
            Self::TimedOut { report } => write!(
                f,
                "coordinated shutdown '{}' timed out",
                report.reason().code()
            ),
        }
    }
}

impl Error for CoordinatedShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Registry { error } => Some(error),
            Self::Failed { .. } | Self::TimedOut { .. } => None,
        }
    }
}

impl From<RakkaError> for CoordinatedShutdownError {
    fn from(error: RakkaError) -> Self {
        Self::Registry { error }
    }
}

/// Serializable point-in-time view of coordinated shutdown state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatedShutdownSnapshot {
    outcome: ShutdownOutcome,
    current_phase: Option<ShutdownPhase>,
    current_task: Option<String>,
    report: Option<CoordinatedShutdownReport>,
}

impl CoordinatedShutdownSnapshot {
    /// Creates a shutdown snapshot.
    #[must_use]
    pub const fn new(outcome: ShutdownOutcome, report: Option<CoordinatedShutdownReport>) -> Self {
        Self {
            outcome,
            current_phase: None,
            current_task: None,
            report,
        }
    }

    /// Creates a running shutdown snapshot with current progress.
    #[must_use]
    pub fn running(current_phase: Option<ShutdownPhase>, current_task: Option<String>) -> Self {
        Self {
            outcome: ShutdownOutcome::Running,
            current_phase,
            current_task,
            report: None,
        }
    }

    /// Current shutdown outcome.
    #[must_use]
    pub const fn outcome(&self) -> ShutdownOutcome {
        self.outcome
    }

    /// Current phase while shutdown is running.
    #[must_use]
    pub const fn current_phase(&self) -> Option<&ShutdownPhase> {
        self.current_phase.as_ref()
    }

    /// Current task while shutdown is running.
    #[must_use]
    pub fn current_task(&self) -> Option<&str> {
        self.current_task.as_deref()
    }

    /// Completed or partial report, when available.
    #[must_use]
    pub const fn report(&self) -> Option<&CoordinatedShutdownReport> {
        self.report.as_ref()
    }
}

/// Settings shared by coordinated shutdown registration and execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatedShutdownSettings {
    default_phase_timeout: Option<Duration>,
    default_task_timeout: Option<Duration>,
    failure_policy: ShutdownFailurePolicy,
}

impl CoordinatedShutdownSettings {
    /// Creates settings with no phase or task timeout overrides.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            default_phase_timeout: None,
            default_task_timeout: None,
            failure_policy: ShutdownFailurePolicy::FailFast,
        }
    }

    /// Returns a copy with the default phase timeout set.
    #[must_use]
    pub const fn with_default_phase_timeout(mut self, timeout: Duration) -> Self {
        self.default_phase_timeout = Some(timeout);
        self
    }

    /// Returns a copy with the default task timeout set.
    #[must_use]
    pub const fn with_default_task_timeout(mut self, timeout: Duration) -> Self {
        self.default_task_timeout = Some(timeout);
        self
    }

    /// Returns a copy with the default failure policy set.
    #[must_use]
    pub const fn with_failure_policy(mut self, policy: ShutdownFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Default timeout for each phase, if configured.
    #[must_use]
    pub const fn default_phase_timeout(&self) -> Option<Duration> {
        self.default_phase_timeout
    }

    /// Default timeout for each task, if configured.
    #[must_use]
    pub const fn default_task_timeout(&self) -> Option<Duration> {
        self.default_task_timeout
    }

    /// Default policy used when a task does not override failure behavior.
    #[must_use]
    pub const fn failure_policy(&self) -> ShutdownFailurePolicy {
        self.failure_policy
    }
}

impl Default for CoordinatedShutdownSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable name for a coordinated shutdown phase.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShutdownPhase {
    name: String,
}

impl ShutdownPhase {
    /// Creates a custom phase name after validating it.
    pub fn new(name: impl Into<String>) -> RakkaResult<Self> {
        Ok(Self {
            name: validate_shutdown_name("phase", name.into())?,
        })
    }

    /// Built-in phase that stops accepting new public ingress.
    #[must_use]
    pub fn stop_ingress() -> Self {
        Self::trusted(STOP_INGRESS)
    }

    /// Built-in phase that drains HTTP, gRPC, and stream adapters.
    #[must_use]
    pub fn drain_adapters() -> Self {
        Self::trusted(DRAIN_ADAPTERS)
    }

    /// Built-in phase that begins graceful cluster leave.
    #[must_use]
    pub fn leave_cluster() -> Self {
        Self::trusted(LEAVE_CLUSTER)
    }

    /// Built-in phase that hands off local shards.
    #[must_use]
    pub fn handoff_shards() -> Self {
        Self::trusted(HANDOFF_SHARDS)
    }

    /// Built-in phase that stops supervised process actors.
    #[must_use]
    pub fn stop_process_actors() -> Self {
        Self::trusted(STOP_PROCESS_ACTORS)
    }

    /// Built-in phase that flushes or closes persistence resources.
    #[must_use]
    pub fn flush_persistence() -> Self {
        Self::trusted(FLUSH_PERSISTENCE)
    }

    /// Built-in phase that stops user actors.
    #[must_use]
    pub fn stop_user_actors() -> Self {
        Self::trusted(STOP_USER_ACTORS)
    }

    /// Built-in phase that stops system actors.
    #[must_use]
    pub fn stop_system_actors() -> Self {
        Self::trusted(STOP_SYSTEM_ACTORS)
    }

    /// Built-in phase that stops remoting transports.
    #[must_use]
    pub fn stop_remoting() -> Self {
        Self::trusted(STOP_REMOTING)
    }

    /// Stable phase name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    fn trusted(name: &'static str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

impl Display for ShutdownPhase {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// Reason a coordinated shutdown run was requested.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CoordinatedShutdownReason {
    code: String,
}

impl CoordinatedShutdownReason {
    /// Creates a custom shutdown reason after validating it.
    pub fn new(code: impl Into<String>) -> RakkaResult<Self> {
        Ok(Self {
            code: validate_shutdown_name("reason", code.into())?,
        })
    }

    /// Reason used when `ActorSystem::terminate` starts shutdown.
    #[must_use]
    pub fn actor_system_terminate() -> Self {
        Self::trusted("actor-system-terminate")
    }

    /// Reason used when Kubernetes pre-stop starts shutdown.
    #[must_use]
    pub fn kubernetes_prestop() -> Self {
        Self::trusted("kubernetes-prestop")
    }

    /// Reason used for an explicit application request.
    #[must_use]
    pub fn user_request() -> Self {
        Self::trusted("user-request")
    }

    /// Stable reason code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    fn trusted(code: &'static str) -> Self {
        Self {
            code: code.to_owned(),
        }
    }
}

impl Display for CoordinatedShutdownReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.code)
    }
}

/// Policy used when a shutdown task fails.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShutdownFailurePolicy {
    /// Stop shutdown after the failed task and return a failed report.
    #[default]
    FailFast,
    /// Continue later tasks and phases while preserving the failure in reports.
    Continue,
}

impl ShutdownFailurePolicy {
    /// Stable policy label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailFast => "fail-fast",
            Self::Continue => "continue",
        }
    }
}

/// Optional key/value attribute attached to a shutdown task.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShutdownTaskAttribute {
    key: String,
    value: String,
}

impl ShutdownTaskAttribute {
    /// Creates a task attribute.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> RakkaResult<Self> {
        Ok(Self {
            key: validate_shutdown_name("task attribute key", key.into())?,
            value: value.into(),
        })
    }

    /// Attribute key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Attribute value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Registration options for a shutdown task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownTaskOptions {
    timeout: Option<Duration>,
    failure_policy: Option<ShutdownFailurePolicy>,
    attributes: Vec<ShutdownTaskAttribute>,
}

impl ShutdownTaskOptions {
    /// Creates task options with registry defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timeout: None,
            failure_policy: None,
            attributes: Vec::new(),
        }
    }

    /// Returns a copy with a task-specific timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Returns a copy with a task-specific failure policy.
    #[must_use]
    pub const fn with_failure_policy(mut self, policy: ShutdownFailurePolicy) -> Self {
        self.failure_policy = Some(policy);
        self
    }

    /// Returns a copy with one additional task attribute.
    pub fn with_attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RakkaResult<Self> {
        self.attributes
            .push(ShutdownTaskAttribute::new(key, value)?);
        Ok(self)
    }

    /// Task-specific timeout, if configured.
    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Task-specific failure policy, if configured.
    #[must_use]
    pub const fn failure_policy(&self) -> Option<ShutdownFailurePolicy> {
        self.failure_policy
    }

    /// Task attributes.
    #[must_use]
    pub fn attributes(&self) -> &[ShutdownTaskAttribute] {
        &self.attributes
    }
}

impl Default for ShutdownTaskOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Descriptor for one registered shutdown task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownTask {
    phase: ShutdownPhase,
    name: String,
    options: ShutdownTaskOptions,
}

impl ShutdownTask {
    /// Creates a task descriptor after validating the name.
    pub fn new(
        phase: ShutdownPhase,
        name: impl Into<String>,
        options: ShutdownTaskOptions,
    ) -> RakkaResult<Self> {
        Ok(Self {
            phase,
            name: validate_shutdown_name("task", name.into())?,
            options,
        })
    }

    /// Phase this task belongs to.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        &self.phase
    }

    /// Stable task name within its phase.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Task registration options.
    #[must_use]
    pub const fn options(&self) -> &ShutdownTaskOptions {
        &self.options
    }
}

/// Context passed to a shutdown task when it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownTaskContext {
    reason: CoordinatedShutdownReason,
    phase: ShutdownPhase,
    task_name: String,
}

impl ShutdownTaskContext {
    /// Creates a task context.
    #[must_use]
    pub fn new(
        reason: CoordinatedShutdownReason,
        phase: ShutdownPhase,
        task_name: impl Into<String>,
    ) -> Self {
        Self {
            reason,
            phase,
            task_name: task_name.into(),
        }
    }

    /// Shutdown reason.
    #[must_use]
    pub const fn reason(&self) -> &CoordinatedShutdownReason {
        &self.reason
    }

    /// Phase currently running.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        &self.phase
    }

    /// Task currently running.
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}

/// Overall outcome for a coordinated shutdown run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShutdownOutcome {
    /// Shutdown has not started.
    NotStarted,
    /// Shutdown is currently running.
    Running,
    /// Every required phase and task completed.
    Complete,
    /// Shutdown completed with at least one non-fatal task failure.
    Partial,
    /// Shutdown stopped because a task failed.
    Failed,
    /// Shutdown stopped because a phase or task timed out.
    TimedOut,
}

impl ShutdownOutcome {
    /// Stable outcome label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not-started",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Failed => "failed",
            Self::TimedOut => "timed-out",
        }
    }
}

/// Status of one shutdown task in a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShutdownTaskStatus {
    /// Task has not started.
    Pending,
    /// Task is currently running.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task returned an error.
    Failed,
    /// Task did not finish before its deadline.
    TimedOut,
    /// Task was skipped because shutdown stopped earlier.
    Skipped,
}

impl ShutdownTaskStatus {
    /// Stable task status label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed-out",
            Self::Skipped => "skipped",
        }
    }
}

/// Report for one shutdown task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownTaskReport {
    phase: ShutdownPhase,
    task_name: String,
    status: ShutdownTaskStatus,
    duration: Option<Duration>,
    message: Option<String>,
}

impl ShutdownTaskReport {
    /// Creates a task report.
    #[must_use]
    pub fn new(
        phase: ShutdownPhase,
        task_name: impl Into<String>,
        status: ShutdownTaskStatus,
        duration: Option<Duration>,
        message: Option<String>,
    ) -> Self {
        Self {
            phase,
            task_name: task_name.into(),
            status,
            duration,
            message,
        }
    }

    /// Phase this task ran in.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        &self.phase
    }

    /// Task name.
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// Task status.
    #[must_use]
    pub const fn status(&self) -> ShutdownTaskStatus {
        self.status
    }

    /// Task duration, if known.
    #[must_use]
    pub const fn duration(&self) -> Option<Duration> {
        self.duration
    }

    /// Optional status or failure message.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Report for one shutdown phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownPhaseReport {
    phase: ShutdownPhase,
    outcome: ShutdownOutcome,
    duration: Option<Duration>,
    tasks: Vec<ShutdownTaskReport>,
}

impl ShutdownPhaseReport {
    /// Creates a phase report.
    #[must_use]
    pub const fn new(
        phase: ShutdownPhase,
        outcome: ShutdownOutcome,
        duration: Option<Duration>,
        tasks: Vec<ShutdownTaskReport>,
    ) -> Self {
        Self {
            phase,
            outcome,
            duration,
            tasks,
        }
    }

    /// Phase described by this report.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        &self.phase
    }

    /// Phase outcome.
    #[must_use]
    pub const fn outcome(&self) -> ShutdownOutcome {
        self.outcome
    }

    /// Phase duration, if known.
    #[must_use]
    pub const fn duration(&self) -> Option<Duration> {
        self.duration
    }

    /// Task reports for this phase.
    #[must_use]
    pub fn tasks(&self) -> &[ShutdownTaskReport] {
        &self.tasks
    }
}

/// Report for a coordinated shutdown run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatedShutdownReport {
    reason: CoordinatedShutdownReason,
    outcome: ShutdownOutcome,
    phases: Vec<ShutdownPhaseReport>,
}

impl CoordinatedShutdownReport {
    /// Creates a shutdown report.
    #[must_use]
    pub const fn new(
        reason: CoordinatedShutdownReason,
        outcome: ShutdownOutcome,
        phases: Vec<ShutdownPhaseReport>,
    ) -> Self {
        Self {
            reason,
            outcome,
            phases,
        }
    }

    /// Shutdown reason.
    #[must_use]
    pub const fn reason(&self) -> &CoordinatedShutdownReason {
        &self.reason
    }

    /// Overall shutdown outcome.
    #[must_use]
    pub const fn outcome(&self) -> ShutdownOutcome {
        self.outcome
    }

    /// Phase reports.
    #[must_use]
    pub fn phases(&self) -> &[ShutdownPhaseReport] {
        &self.phases
    }
}

#[derive(Clone)]
struct ShutdownTaskEntry {
    descriptor: ShutdownTask,
    run: Arc<ShutdownTaskRunner>,
}

impl Debug for ShutdownTaskEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownTaskEntry")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct ShutdownExecutionPlan {
    settings: CoordinatedShutdownSettings,
    phases: Vec<ShutdownPhase>,
    tasks: BTreeMap<String, Vec<ShutdownTaskEntry>>,
    metrics_system: String,
    metrics: Arc<dyn MetricsRecorder>,
    progress: ShutdownProgressHandle,
}

type ShutdownProgressHandle = Arc<Mutex<ShutdownProgress>>;

#[derive(Debug, Default, Clone)]
struct ShutdownProgress {
    current_phase: Option<ShutdownPhase>,
    current_task: Option<String>,
}

#[derive(Debug, Clone)]
enum ShutdownRunState {
    NotStarted,
    Running {
        receiver: watch::Receiver<Option<ShutdownRunResult>>,
        progress: ShutdownProgressHandle,
    },
    Finished {
        result: ShutdownRunResult,
    },
}

enum ShutdownRunAction {
    Start {
        sender: watch::Sender<Option<ShutdownRunResult>>,
        plan: ShutdownExecutionPlan,
        reason: CoordinatedShutdownReason,
        deadline: Option<Instant>,
    },
    Wait(watch::Receiver<Option<ShutdownRunResult>>),
}

#[derive(Debug, Clone)]
struct PhaseNode {
    phase: ShutdownPhase,
    depends_on: BTreeSet<String>,
}

struct ShutdownRegistry {
    settings: CoordinatedShutdownSettings,
    phases: BTreeMap<String, PhaseNode>,
    tasks: BTreeMap<String, BTreeMap<String, ShutdownTaskEntry>>,
    metrics_system: String,
    metrics: Arc<dyn MetricsRecorder>,
    run_state: ShutdownRunState,
}

impl ShutdownRegistry {
    fn new(
        settings: CoordinatedShutdownSettings,
        system_name: impl Into<String>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        let mut phases = BTreeMap::new();
        for (index, name) in BUILT_IN_PHASES.iter().enumerate() {
            let mut depends_on = BTreeSet::new();
            if index > 0 {
                depends_on.insert(BUILT_IN_PHASES[index - 1].to_owned());
            }

            phases.insert(
                (*name).to_owned(),
                PhaseNode {
                    phase: ShutdownPhase::trusted(name),
                    depends_on,
                },
            );
        }

        let system_name = system_name.into();
        let metrics_system = if system_name.is_empty() {
            STANDALONE_METRICS_SYSTEM.to_owned()
        } else {
            system_name
        };

        Self {
            settings,
            phases,
            tasks: BTreeMap::new(),
            metrics_system,
            metrics,
            run_state: ShutdownRunState::NotStarted,
        }
    }

    fn add_phase_after(
        &mut self,
        name: String,
        after: ShutdownPhase,
    ) -> RakkaResult<ShutdownPhase> {
        self.ensure_not_started()?;
        self.ensure_phase_exists(&after)?;
        let phase = ShutdownPhase::new(name)?;
        self.ensure_phase_absent(&phase)?;

        let mut depends_on = BTreeSet::new();
        depends_on.insert(after.name().to_owned());
        self.phases.insert(
            phase.name().to_owned(),
            PhaseNode {
                phase: phase.clone(),
                depends_on,
            },
        );
        Ok(phase)
    }

    fn add_phase_before(
        &mut self,
        name: String,
        before: ShutdownPhase,
    ) -> RakkaResult<ShutdownPhase> {
        self.ensure_not_started()?;
        self.ensure_phase_exists(&before)?;
        let phase = ShutdownPhase::new(name)?;
        self.ensure_phase_absent(&phase)?;

        self.phases.insert(
            phase.name().to_owned(),
            PhaseNode {
                phase: phase.clone(),
                depends_on: BTreeSet::new(),
            },
        );
        self.phases
            .get_mut(before.name())
            .expect("phase was checked above")
            .depends_on
            .insert(phase.name().to_owned());
        Ok(phase)
    }

    fn add_phase_dependency(
        &mut self,
        phase: ShutdownPhase,
        depends_on: ShutdownPhase,
    ) -> RakkaResult<()> {
        self.ensure_not_started()?;
        self.ensure_phase_exists(&phase)?;
        self.ensure_phase_exists(&depends_on)?;

        let inserted = self
            .phases
            .get_mut(phase.name())
            .expect("phase was checked above")
            .depends_on
            .insert(depends_on.name().to_owned());

        if let Err(error) = self.ordered_phase_names() {
            let dependencies = &mut self
                .phases
                .get_mut(phase.name())
                .expect("phase was checked above")
                .depends_on;
            if inserted {
                dependencies.remove(depends_on.name());
            }
            return Err(error);
        }

        Ok(())
    }

    fn ordered_phases(&self) -> RakkaResult<Vec<ShutdownPhase>> {
        self.ordered_phase_names().map(|names| {
            names
                .iter()
                .map(|name| {
                    self.phases
                        .get(name)
                        .expect("topological sort only returns known phases")
                        .phase
                        .clone()
                })
                .collect()
        })
    }

    fn ordered_phase_names(&self) -> RakkaResult<Vec<String>> {
        for node in self.phases.values() {
            for dependency in &node.depends_on {
                if !self.phases.contains_key(dependency) {
                    return Err(unknown_phase_error(dependency));
                }
            }
        }

        let mut remaining: BTreeMap<String, BTreeSet<String>> = self
            .phases
            .iter()
            .map(|(name, node)| (name.clone(), node.depends_on.clone()))
            .collect();
        let mut ready: BTreeSet<String> = remaining
            .iter()
            .filter_map(|(name, dependencies)| {
                if dependencies.is_empty() {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        let mut ordered = Vec::with_capacity(self.phases.len());

        while let Some(name) = ready.pop_first() {
            remaining.remove(&name);
            ordered.push(name.clone());

            for (candidate, dependencies) in &mut remaining {
                dependencies.remove(&name);
                if dependencies.is_empty() {
                    ready.insert(candidate.clone());
                }
            }
        }

        if ordered.len() != self.phases.len() {
            return Err(RakkaError::core(
                "shutdown-phase-cycle",
                "coordinated shutdown phases contain a dependency cycle",
            ));
        }

        Ok(ordered)
    }

    fn add_task(
        &mut self,
        phase: ShutdownPhase,
        name: String,
        options: ShutdownTaskOptions,
        run: Arc<ShutdownTaskRunner>,
    ) -> RakkaResult<ShutdownTask> {
        self.ensure_not_started()?;
        self.ensure_phase_exists(&phase)?;
        let descriptor = ShutdownTask::new(phase.clone(), name, options)?;
        let phase_tasks = self.tasks.entry(phase.name().to_owned()).or_default();
        if phase_tasks.contains_key(descriptor.name()) {
            return Err(RakkaError::core(
                "duplicate-shutdown-task",
                format!(
                    "coordinated shutdown task '{}' is already registered in phase '{}'",
                    descriptor.name(),
                    phase.name()
                ),
            ));
        }

        phase_tasks.insert(
            descriptor.name().to_owned(),
            ShutdownTaskEntry {
                descriptor: descriptor.clone(),
                run,
            },
        );
        Ok(descriptor)
    }

    fn ordered_tasks(&self) -> RakkaResult<Vec<ShutdownTask>> {
        let mut tasks = Vec::new();
        for phase in self.ordered_phase_names()? {
            if let Some(phase_tasks) = self.tasks.get(&phase) {
                tasks.extend(phase_tasks.values().map(|entry| entry.descriptor.clone()));
            }
        }
        Ok(tasks)
    }

    fn tasks_for_phase(&self, phase: &ShutdownPhase) -> RakkaResult<Vec<ShutdownTask>> {
        self.ensure_phase_exists(phase)?;
        Ok(self
            .tasks
            .get(phase.name())
            .map(|phase_tasks| {
                phase_tasks
                    .values()
                    .map(|entry| entry.descriptor.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    fn task_count(&self) -> usize {
        self.tasks.values().map(BTreeMap::len).sum()
    }

    fn execution_plan(
        &self,
        progress: ShutdownProgressHandle,
    ) -> CoordinatedShutdownResult<ShutdownExecutionPlan> {
        let phase_names = self.ordered_phase_names()?;
        let phases = phase_names
            .iter()
            .map(|name| {
                self.phases
                    .get(name)
                    .expect("topological sort only returns known phases")
                    .phase
                    .clone()
            })
            .collect::<Vec<_>>();
        let tasks = phase_names
            .into_iter()
            .filter_map(|phase_name| {
                self.tasks.get(&phase_name).map(|phase_tasks| {
                    (
                        phase_name,
                        phase_tasks.values().cloned().collect::<Vec<_>>(),
                    )
                })
            })
            .collect();

        Ok(ShutdownExecutionPlan {
            settings: self.settings,
            phases,
            tasks,
            metrics_system: self.metrics_system.clone(),
            metrics: self.metrics.clone(),
            progress,
        })
    }

    fn snapshot(&self) -> CoordinatedShutdownSnapshot {
        match &self.run_state {
            ShutdownRunState::NotStarted => {
                CoordinatedShutdownSnapshot::new(ShutdownOutcome::NotStarted, None)
            }
            ShutdownRunState::Running { receiver, progress } => {
                receiver.borrow().as_ref().map_or_else(
                    || {
                        let progress = progress
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        CoordinatedShutdownSnapshot::running(
                            progress.current_phase.clone(),
                            progress.current_task.clone(),
                        )
                    },
                    snapshot_from_result,
                )
            }
            ShutdownRunState::Finished { result } => snapshot_from_result(result),
        }
    }

    fn ensure_not_started(&self) -> RakkaResult<()> {
        if matches!(self.run_state, ShutdownRunState::NotStarted) {
            Ok(())
        } else {
            Err(RakkaError::core(
                "shutdown-already-started",
                "cannot modify coordinated shutdown after it has started",
            ))
        }
    }

    fn ensure_phase_exists(&self, phase: &ShutdownPhase) -> RakkaResult<()> {
        if self.phases.contains_key(phase.name()) {
            Ok(())
        } else {
            Err(unknown_phase_error(phase.name()))
        }
    }

    fn ensure_phase_absent(&self, phase: &ShutdownPhase) -> RakkaResult<()> {
        if self.phases.contains_key(phase.name()) {
            Err(RakkaError::core(
                "duplicate-shutdown-phase",
                format!(
                    "coordinated shutdown phase '{}' is already registered",
                    phase.name()
                ),
            ))
        } else {
            Ok(())
        }
    }
}

async fn execute_shutdown_plan(
    plan: ShutdownExecutionPlan,
    reason: CoordinatedShutdownReason,
    overall_deadline: Option<Instant>,
) -> ShutdownRunResult {
    record_shutdown_running(&plan, &reason, 1.0);
    let result = execute_shutdown_plan_inner(&plan, reason.clone(), overall_deadline).await;
    clear_shutdown_progress(&plan.progress);
    record_shutdown_running(&plan, &reason, 0.0);
    result
}

async fn execute_shutdown_plan_inner(
    plan: &ShutdownExecutionPlan,
    reason: CoordinatedShutdownReason,
    overall_deadline: Option<Instant>,
) -> ShutdownRunResult {
    let mut final_outcome = ShutdownOutcome::Complete;
    let mut phase_reports = Vec::new();

    for phase in plan.phases.clone() {
        let phase_start = Instant::now();
        update_shutdown_progress(&plan.progress, Some(&phase), None);
        let phase_timeout_deadline = plan
            .settings
            .default_phase_timeout()
            .map(|timeout| phase_start + timeout);
        let phase_deadline = min_deadline(overall_deadline, phase_timeout_deadline);
        let mut task_reports = Vec::new();
        let mut phase_outcome = ShutdownOutcome::Complete;
        let mut stop_after_phase = false;
        let phase_tasks = plan.tasks.get(phase.name()).cloned().unwrap_or_default();

        if deadline_elapsed(phase_deadline) {
            phase_outcome = ShutdownOutcome::TimedOut;
            stop_after_phase = true;
        }

        for task in phase_tasks {
            if stop_after_phase {
                break;
            }

            if deadline_elapsed(phase_deadline) {
                update_shutdown_progress(
                    &plan.progress,
                    Some(&phase),
                    Some(task.descriptor.name()),
                );
                let task_report = ShutdownTaskReport::new(
                    phase.clone(),
                    task.descriptor.name().to_owned(),
                    ShutdownTaskStatus::TimedOut,
                    Some(Duration::ZERO),
                    Some("shutdown phase deadline elapsed before task started".to_owned()),
                );
                record_shutdown_task_report(plan, &reason, &task_report);
                task_reports.push(task_report);
                phase_outcome = ShutdownOutcome::TimedOut;
                stop_after_phase = true;
                break;
            }

            let task_deadline = task_deadline(&plan.settings, &task, phase_deadline);
            update_shutdown_progress(&plan.progress, Some(&phase), Some(task.descriptor.name()));
            let task_report = run_shutdown_task(&task, reason.clone(), task_deadline).await;
            let task_status = task_report.status();
            let task_policy = task
                .descriptor
                .options()
                .failure_policy()
                .unwrap_or(plan.settings.failure_policy());

            record_shutdown_task_report(plan, &reason, &task_report);
            task_reports.push(task_report);
            update_shutdown_progress(&plan.progress, Some(&phase), None);

            match task_status {
                ShutdownTaskStatus::Completed => {}
                ShutdownTaskStatus::Failed => {
                    if task_policy == ShutdownFailurePolicy::FailFast {
                        phase_outcome = ShutdownOutcome::Failed;
                        stop_after_phase = true;
                    } else {
                        phase_outcome =
                            combine_shutdown_outcome(phase_outcome, ShutdownOutcome::Partial);
                        final_outcome =
                            combine_shutdown_outcome(final_outcome, ShutdownOutcome::Partial);
                    }
                }
                ShutdownTaskStatus::TimedOut => {
                    phase_outcome =
                        combine_shutdown_outcome(phase_outcome, ShutdownOutcome::TimedOut);
                    final_outcome =
                        combine_shutdown_outcome(final_outcome, ShutdownOutcome::TimedOut);
                    if task_policy == ShutdownFailurePolicy::FailFast
                        || deadline_elapsed(phase_deadline)
                    {
                        stop_after_phase = true;
                    }
                }
                ShutdownTaskStatus::Pending
                | ShutdownTaskStatus::Running
                | ShutdownTaskStatus::Skipped => {}
            }
        }

        if phase_outcome == ShutdownOutcome::Complete && deadline_elapsed(phase_deadline) {
            phase_outcome = ShutdownOutcome::TimedOut;
            stop_after_phase = true;
        }

        final_outcome = combine_shutdown_outcome(final_outcome, phase_outcome);
        let phase_report = ShutdownPhaseReport::new(
            phase,
            phase_outcome,
            Some(phase_start.elapsed()),
            task_reports,
        );
        record_shutdown_phase_report(plan, &reason, &phase_report);
        phase_reports.push(phase_report);

        if stop_after_phase {
            let report = CoordinatedShutdownReport::new(reason, final_outcome, phase_reports);
            return report_to_result(report);
        }
    }

    report_to_result(CoordinatedShutdownReport::new(
        reason,
        final_outcome,
        phase_reports,
    ))
}

async fn run_shutdown_task(
    task: &ShutdownTaskEntry,
    reason: CoordinatedShutdownReason,
    deadline: Option<Instant>,
) -> ShutdownTaskReport {
    let task_start = Instant::now();
    let descriptor = task.descriptor.clone();
    let context = ShutdownTaskContext::new(
        reason,
        descriptor.phase().clone(),
        descriptor.name().to_owned(),
    );

    let result = if deadline_elapsed(deadline) {
        TaskRunOutcome::TimedOut
    } else {
        let run = (task.run)(context);
        match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, run).await {
                Ok(Ok(())) => TaskRunOutcome::Completed,
                Ok(Err(error)) => TaskRunOutcome::Failed(error.to_string()),
                Err(_elapsed) => TaskRunOutcome::TimedOut,
            },
            None => match run.await {
                Ok(()) => TaskRunOutcome::Completed,
                Err(error) => TaskRunOutcome::Failed(error.to_string()),
            },
        }
    };

    match result {
        TaskRunOutcome::Completed => ShutdownTaskReport::new(
            descriptor.phase().clone(),
            descriptor.name().to_owned(),
            ShutdownTaskStatus::Completed,
            Some(task_start.elapsed()),
            None,
        ),
        TaskRunOutcome::Failed(message) => ShutdownTaskReport::new(
            descriptor.phase().clone(),
            descriptor.name().to_owned(),
            ShutdownTaskStatus::Failed,
            Some(task_start.elapsed()),
            Some(message),
        ),
        TaskRunOutcome::TimedOut => ShutdownTaskReport::new(
            descriptor.phase().clone(),
            descriptor.name().to_owned(),
            ShutdownTaskStatus::TimedOut,
            Some(task_start.elapsed()),
            Some("shutdown task timed out".to_owned()),
        ),
    }
}

fn update_shutdown_progress(
    progress: &ShutdownProgressHandle,
    phase: Option<&ShutdownPhase>,
    task: Option<&str>,
) {
    let mut progress = progress
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    progress.current_phase = phase.cloned();
    progress.current_task = task.map(ToOwned::to_owned);
}

fn clear_shutdown_progress(progress: &ShutdownProgressHandle) {
    update_shutdown_progress(progress, None, None);
}

fn record_shutdown_running(
    plan: &ShutdownExecutionPlan,
    reason: &CoordinatedShutdownReason,
    value: f64,
) {
    plan.metrics.record_gauge(
        METRIC_SHUTDOWN_RUNNING,
        value,
        &[
            ("system", plan.metrics_system.as_str()),
            ("reason", reason.code()),
        ],
    );
}

fn record_shutdown_task_report(
    plan: &ShutdownExecutionPlan,
    reason: &CoordinatedShutdownReason,
    report: &ShutdownTaskReport,
) {
    let status = report.status().as_str();
    plan.metrics.record_histogram(
        METRIC_SHUTDOWN_TASK_DURATION_MS,
        duration_millis(report.duration().unwrap_or(Duration::ZERO)),
        &[
            ("system", plan.metrics_system.as_str()),
            ("phase", report.phase().name()),
            ("task", report.task_name()),
            ("reason", reason.code()),
            ("status", status),
        ],
    );

    if report.status() == ShutdownTaskStatus::Failed {
        plan.metrics.increment_counter(
            METRIC_SHUTDOWN_TASK_FAILURES,
            1,
            &[
                ("system", plan.metrics_system.as_str()),
                ("phase", report.phase().name()),
                ("task", report.task_name()),
                ("reason", reason.code()),
                ("status", status),
            ],
        );
    }

    if report.status() == ShutdownTaskStatus::TimedOut {
        record_shutdown_timeout(
            plan,
            reason,
            "task",
            report.phase(),
            report.task_name(),
            status,
        );
    }
}

fn record_shutdown_phase_report(
    plan: &ShutdownExecutionPlan,
    reason: &CoordinatedShutdownReason,
    report: &ShutdownPhaseReport,
) {
    let status = report.outcome().as_str();
    plan.metrics.record_histogram(
        METRIC_SHUTDOWN_PHASE_DURATION_MS,
        duration_millis(report.duration().unwrap_or(Duration::ZERO)),
        &[
            ("system", plan.metrics_system.as_str()),
            ("phase", report.phase().name()),
            ("reason", reason.code()),
            ("status", status),
        ],
    );

    let has_timed_out_task = report
        .tasks()
        .iter()
        .any(|task| task.status() == ShutdownTaskStatus::TimedOut);
    if report.outcome() == ShutdownOutcome::TimedOut && !has_timed_out_task {
        record_shutdown_timeout(plan, reason, "phase", report.phase(), "none", status);
    }
}

fn record_shutdown_timeout(
    plan: &ShutdownExecutionPlan,
    reason: &CoordinatedShutdownReason,
    scope: &'static str,
    phase: &ShutdownPhase,
    task: &str,
    status: &'static str,
) {
    plan.metrics.increment_counter(
        METRIC_SHUTDOWN_TIMEOUTS,
        1,
        &[
            ("system", plan.metrics_system.as_str()),
            ("phase", phase.name()),
            ("task", task),
            ("reason", reason.code()),
            ("status", status),
            ("scope", scope),
        ],
    );
}

fn duration_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

enum TaskRunOutcome {
    Completed,
    Failed(String),
    TimedOut,
}

fn task_deadline(
    settings: &CoordinatedShutdownSettings,
    task: &ShutdownTaskEntry,
    phase_deadline: Option<Instant>,
) -> Option<Instant> {
    let task_timeout = task
        .descriptor
        .options()
        .timeout()
        .or(settings.default_task_timeout());
    let task_deadline = task_timeout.map(|timeout| Instant::now() + timeout);
    min_deadline(phase_deadline, task_deadline)
}

fn min_deadline(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

fn deadline_elapsed(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

fn combine_shutdown_outcome(left: ShutdownOutcome, right: ShutdownOutcome) -> ShutdownOutcome {
    match (left, right) {
        (ShutdownOutcome::TimedOut, _) | (_, ShutdownOutcome::TimedOut) => {
            ShutdownOutcome::TimedOut
        }
        (ShutdownOutcome::Failed, _) | (_, ShutdownOutcome::Failed) => ShutdownOutcome::Failed,
        (ShutdownOutcome::Partial, _) | (_, ShutdownOutcome::Partial) => ShutdownOutcome::Partial,
        (ShutdownOutcome::Running, _) | (_, ShutdownOutcome::Running) => ShutdownOutcome::Running,
        (ShutdownOutcome::NotStarted, outcome) => outcome,
        (outcome, ShutdownOutcome::NotStarted) => outcome,
        (ShutdownOutcome::Complete, ShutdownOutcome::Complete) => ShutdownOutcome::Complete,
    }
}

fn report_to_result(report: CoordinatedShutdownReport) -> ShutdownRunResult {
    match report.outcome() {
        ShutdownOutcome::Failed => Err(CoordinatedShutdownError::Failed { report }),
        ShutdownOutcome::TimedOut => Err(CoordinatedShutdownError::TimedOut { report }),
        ShutdownOutcome::NotStarted
        | ShutdownOutcome::Running
        | ShutdownOutcome::Complete
        | ShutdownOutcome::Partial => Ok(report),
    }
}

fn snapshot_from_result(result: &ShutdownRunResult) -> CoordinatedShutdownSnapshot {
    match result {
        Ok(report) => CoordinatedShutdownSnapshot::new(report.outcome(), Some(report.clone())),
        Err(CoordinatedShutdownError::Registry { .. }) => {
            CoordinatedShutdownSnapshot::new(ShutdownOutcome::Failed, None)
        }
        Err(CoordinatedShutdownError::Failed { report }) => {
            CoordinatedShutdownSnapshot::new(ShutdownOutcome::Failed, Some(report.clone()))
        }
        Err(CoordinatedShutdownError::TimedOut { report }) => {
            CoordinatedShutdownSnapshot::new(ShutdownOutcome::TimedOut, Some(report.clone()))
        }
    }
}

fn unknown_phase_error(phase: &str) -> RakkaError {
    RakkaError::core(
        "unknown-shutdown-phase",
        format!("coordinated shutdown phase '{phase}' is not registered"),
    )
}

fn validate_shutdown_name(kind: &str, name: String) -> RakkaResult<String> {
    if name.is_empty() || name.trim() != name {
        return Err(RakkaError::core(
            "invalid-shutdown-name",
            format!("coordinated shutdown {kind} name must not be empty or padded"),
        ));
    }

    let valid = name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'));
    if !valid {
        return Err(RakkaError::core(
            "invalid-shutdown-name",
            format!(
                "coordinated shutdown {kind} name '{name}' must use ASCII letters, digits, '-', '_', or '.'"
            ),
        ));
    }

    Ok(name)
}
