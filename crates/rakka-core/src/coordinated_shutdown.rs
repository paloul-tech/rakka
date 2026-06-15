//! Coordinated shutdown phase and task registry.
//!
//! This module provides Rakka's Akka-shaped coordinated shutdown vocabulary.
//! Slice 7A defines the public phase graph and task registration surface. Later
//! slices attach the runner to `ActorSystem` termination and operational
//! adapters.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

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

/// Future returned by a coordinated shutdown task.
pub type ShutdownTaskFuture = Pin<Box<dyn Future<Output = ShutdownTaskResult> + Send + 'static>>;

/// Result returned by a coordinated shutdown task.
pub type ShutdownTaskResult = RakkaResult<()>;

type ShutdownTaskRunner = dyn Fn(ShutdownTaskContext) -> ShutdownTaskFuture + Send + Sync + 'static;

/// Registry of named shutdown phases and tasks.
#[derive(Clone)]
pub struct CoordinatedShutdown {
    inner: Arc<Mutex<ShutdownRegistry>>,
}

impl CoordinatedShutdown {
    /// Creates a registry with default settings and built-in phases.
    #[must_use]
    pub fn new() -> Self {
        Self::with_settings(CoordinatedShutdownSettings::default())
    }

    /// Creates a registry with custom settings and built-in phases.
    #[must_use]
    pub fn with_settings(settings: CoordinatedShutdownSettings) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ShutdownRegistry::new(settings))),
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

    fn lock(&self) -> std::sync::MutexGuard<'_, ShutdownRegistry> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            .finish()
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
    _run: Arc<ShutdownTaskRunner>,
}

impl Debug for ShutdownTaskEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownTaskEntry")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct PhaseNode {
    phase: ShutdownPhase,
    depends_on: BTreeSet<String>,
}

#[derive(Debug)]
struct ShutdownRegistry {
    settings: CoordinatedShutdownSettings,
    phases: BTreeMap<String, PhaseNode>,
    tasks: BTreeMap<String, BTreeMap<String, ShutdownTaskEntry>>,
}

impl ShutdownRegistry {
    fn new(settings: CoordinatedShutdownSettings) -> Self {
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

        Self {
            settings,
            phases,
            tasks: BTreeMap::new(),
        }
    }

    fn add_phase_after(
        &mut self,
        name: String,
        after: ShutdownPhase,
    ) -> RakkaResult<ShutdownPhase> {
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
                _run: run,
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
