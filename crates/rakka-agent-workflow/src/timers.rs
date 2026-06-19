//! Durable timer model and scanner for agent workflow runs.
//!
//! Timers are persisted independently from a live actor. When a timer becomes
//! due, the scanner injects a `TimerFired` command through the durable inbox and
//! resumes the run if it is still waiting for a timer.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision, StateRecord};
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState, WorkflowTimestamp};
use serde::{Deserialize, Serialize};

use crate::metrics::METRIC_AGENT_TIMERS_LATE_BY_MS;
use crate::{
    AgentAttributes, AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind,
    AgentCommandMetadata, AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata,
    AgentFacadeError, AgentInboxAcceptance, AgentInboxError, AgentRunEngineError, AgentRunId,
    AgentRunInbox, AgentRunState, AgentRunStatus, AgentRunTransition, AgentStepRunner,
    AgentTelemetryContext, AgentTenantId, AgentTimerId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId,
};

/// Prefix used for durable timer-store state persistence ids.
pub const AGENT_TIMER_PERSISTENCE_PREFIX: &str = "agent-timers";

/// Default timer-store persistence id.
pub const DEFAULT_AGENT_TIMER_STORE_ID: &str = "default";

/// Counter for durable timer firing attempts.
pub const METRIC_AGENT_TIMERS: &str = "rakka.agent_workflow.timers";

/// Creates the default timer-store persistence id.
#[must_use]
pub fn agent_timer_store_persistence_id() -> PersistenceId {
    PersistenceId::new(format!(
        "{AGENT_TIMER_PERSISTENCE_PREFIX}:{DEFAULT_AGENT_TIMER_STORE_ID}"
    ))
}

/// Shared result type for durable timer operations.
pub type AgentTimerResult<T> = Result<T, AgentTimerError>;

/// Durable timer failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTimerError {
    /// The timer entry failed validation.
    InvalidTimer {
        /// Timer id.
        timer_id: AgentTimerId,
        /// Invalid field.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
    /// A timer with the same id already exists.
    TimerAlreadyExists {
        /// Timer id.
        timer_id: AgentTimerId,
    },
    /// A timer with the requested id does not exist.
    TimerNotFound {
        /// Timer id.
        timer_id: AgentTimerId,
    },
    /// Timer targets a different workflow than the scanner owns.
    WorkflowMismatch {
        /// Timer id.
        timer_id: AgentTimerId,
        /// Expected workflow id.
        expected: AgentWorkflowId,
        /// Actual workflow id.
        actual: AgentWorkflowId,
    },
    /// Timer fired for a run that has no durable state.
    MissingRunState {
        /// Run id.
        run_id: AgentRunId,
    },
    /// Timer command construction failed.
    Command {
        /// Command validation error.
        error: AgentFacadeError,
    },
    /// Durable inbox operation failed.
    Inbox {
        /// Inbox failure.
        error: AgentInboxError,
    },
    /// Durable run state-machine operation failed.
    RunEngine {
        /// Run-engine failure.
        error: AgentRunEngineError,
    },
    /// Timer persistence failed.
    Persistence {
        /// Durable persistence error.
        error: DurableError,
    },
}

impl AgentTimerError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidTimer { .. } => "invalid-timer",
            Self::TimerAlreadyExists { .. } => "timer-already-exists",
            Self::TimerNotFound { .. } => "timer-not-found",
            Self::WorkflowMismatch { .. } => "workflow-mismatch",
            Self::MissingRunState { .. } => "missing-run-state",
            Self::Command { error } => facade_error_code(error),
            Self::Inbox { error } => error.code(),
            Self::RunEngine { error } => error.code(),
            Self::Persistence { error } => error.code(),
        }
    }
}

impl Display for AgentTimerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimer {
                timer_id,
                field,
                reason,
            } => write!(f, "agent timer {timer_id} has invalid {field}: {reason}"),
            Self::TimerAlreadyExists { timer_id } => {
                write!(f, "agent timer {timer_id} already exists")
            }
            Self::TimerNotFound { timer_id } => write!(f, "agent timer {timer_id} was not found"),
            Self::WorkflowMismatch {
                timer_id,
                expected,
                actual,
            } => write!(
                f,
                "agent timer {timer_id} targets workflow {actual}, expected {expected}"
            ),
            Self::MissingRunState { run_id } => {
                write!(f, "agent timer target run {run_id} has no durable state")
            }
            Self::Command { error } => Display::fmt(error, f),
            Self::Inbox { error } => Display::fmt(error, f),
            Self::RunEngine { error } => Display::fmt(error, f),
            Self::Persistence { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentTimerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Command { error } => Some(error),
            Self::Inbox { error } => Some(error),
            Self::RunEngine { error } => Some(error),
            Self::Persistence { error } => Some(error),
            Self::InvalidTimer { .. }
            | Self::TimerAlreadyExists { .. }
            | Self::TimerNotFound { .. }
            | Self::WorkflowMismatch { .. }
            | Self::MissingRunState { .. } => None,
        }
    }
}

impl From<AgentFacadeError> for AgentTimerError {
    fn from(error: AgentFacadeError) -> Self {
        Self::Command { error }
    }
}

impl From<AgentInboxError> for AgentTimerError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox { error }
    }
}

impl From<AgentRunEngineError> for AgentTimerError {
    fn from(error: AgentRunEngineError) -> Self {
        Self::RunEngine { error }
    }
}

impl From<DurableError> for AgentTimerError {
    fn from(error: DurableError) -> Self {
        Self::Persistence { error }
    }
}

/// Durable timer lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTimerStatus {
    /// Timer has not fired yet.
    Pending,
    /// Timer fired and injected `TimerFired` into the durable inbox.
    Fired,
    /// Timer was cancelled before firing.
    Cancelled,
}

impl AgentTimerStatus {
    /// Stable lowercase label for diagnostics and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Fired => "fired",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Policy metadata attached to one timer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTimerPolicy {
    /// Optional stable policy name.
    pub policy_name: Option<String>,
    /// Optional max lateness budget in milliseconds.
    pub max_lateness_ms: Option<u64>,
    /// Bounded policy attributes.
    pub attributes: AgentAttributes,
}

impl AgentTimerPolicy {
    /// Creates empty timer policy metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the policy name.
    #[must_use]
    pub fn policy_name(mut self, policy_name: impl Into<String>) -> Self {
        self.policy_name = Some(policy_name.into());
        self
    }

    /// Sets the max lateness budget.
    #[must_use]
    pub const fn max_lateness_ms(mut self, max_lateness_ms: u64) -> Self {
        self.max_lateness_ms = Some(max_lateness_ms);
        self
    }

    /// Adds a bounded policy attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Durable timer entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTimerEntry {
    /// Stable timer id.
    pub timer_id: AgentTimerId,
    /// Workflow definition id the timer targets.
    pub workflow_id: AgentWorkflowId,
    /// Run id the timer targets.
    pub run_id: AgentRunId,
    /// Tenant or namespace that owns the timer.
    pub tenant: AgentTenantId,
    /// Due timestamp.
    pub due_at: AgentTimestampMillis,
    /// Stable durable inbox deduplication key for `TimerFired`.
    pub deduplication_key: AgentDeduplicationKey,
    /// Command or event that caused the timer.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related work.
    pub correlation_id: AgentCorrelationId,
    /// Optional trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
    /// Policy metadata.
    pub policy: AgentTimerPolicy,
    /// Current timer status.
    pub status: AgentTimerStatus,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
    /// Fired timestamp, if fired.
    pub fired_at: Option<AgentTimestampMillis>,
}

impl AgentTimerEntry {
    /// Creates a pending durable timer entry.
    pub fn new(
        timer_id: AgentTimerId,
        workflow_id: AgentWorkflowId,
        run_id: AgentRunId,
        tenant: AgentTenantId,
        due_at: AgentTimestampMillis,
        durability: AgentDurabilityMetadata,
        created_at: AgentTimestampMillis,
    ) -> AgentTimerResult<Self> {
        let entry = Self {
            timer_id,
            workflow_id,
            run_id,
            tenant,
            due_at,
            deduplication_key: durability.deduplication_key,
            causation_id: durability.causation_id,
            correlation_id: durability.correlation_id,
            telemetry_context: durability.telemetry_context,
            policy: AgentTimerPolicy::default(),
            status: AgentTimerStatus::Pending,
            created_at,
            updated_at: created_at,
            fired_at: None,
        };
        entry.validate()?;
        Ok(entry)
    }

    /// Sets policy metadata.
    pub fn policy(mut self, policy: AgentTimerPolicy) -> AgentTimerResult<Self> {
        self.policy = policy;
        self.validate()?;
        Ok(self)
    }

    /// Returns true when this timer is pending and due at `now`.
    #[must_use]
    pub const fn is_due(&self, now: AgentTimestampMillis) -> bool {
        matches!(self.status, AgentTimerStatus::Pending)
            && self.due_at.as_millis() <= now.as_millis()
    }

    /// Lateness in milliseconds at `now`.
    #[must_use]
    pub const fn late_by_ms(&self, now: AgentTimestampMillis) -> u64 {
        now.as_millis().saturating_sub(self.due_at.as_millis())
    }

    fn fired(mut self, fired_at: AgentTimestampMillis) -> Self {
        self.status = AgentTimerStatus::Fired;
        self.updated_at = fired_at;
        self.fired_at = Some(fired_at);
        self
    }

    fn cancelled(mut self, cancelled_at: AgentTimestampMillis) -> Self {
        self.status = AgentTimerStatus::Cancelled;
        self.updated_at = cancelled_at;
        self
    }

    fn validate(&self) -> AgentTimerResult<()> {
        require_timer(&self.timer_id, self.timer_id.as_str(), "timer_id")?;
        require_timer(&self.timer_id, self.workflow_id.as_str(), "workflow_id")?;
        require_timer(&self.timer_id, self.run_id.as_str(), "run_id")?;
        require_timer(&self.timer_id, self.tenant.as_str(), "tenant")?;
        require_timer(
            &self.timer_id,
            self.deduplication_key.as_str(),
            "deduplication_key",
        )?;
        require_timer(&self.timer_id, self.causation_id.as_str(), "causation_id")?;
        require_timer(
            &self.timer_id,
            self.correlation_id.as_str(),
            "correlation_id",
        )?;
        if let Some(policy_name) = &self.policy.policy_name {
            require_timer(&self.timer_id, policy_name, "policy_name")?;
        }
        for key in self.policy.attributes.keys() {
            require_timer(&self.timer_id, key, "policy.attributes.key")?;
        }
        Ok(())
    }
}

/// Durable timer index state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTimerStoreState {
    timers: BTreeMap<AgentTimerId, AgentTimerEntry>,
    updated_at: AgentTimestampMillis,
}

impl AgentTimerStoreState {
    /// Creates an empty timer store state.
    #[must_use]
    pub fn empty(now: AgentTimestampMillis) -> Self {
        Self {
            timers: BTreeMap::new(),
            updated_at: now,
        }
    }

    /// All timers by id in deterministic order.
    #[must_use]
    pub const fn timers(&self) -> &BTreeMap<AgentTimerId, AgentTimerEntry> {
        &self.timers
    }

    /// Last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// Returns one timer by id.
    #[must_use]
    pub fn timer(&self, timer_id: &AgentTimerId) -> Option<&AgentTimerEntry> {
        self.timers.get(timer_id)
    }

    /// Returns pending due timers in due-time order.
    #[must_use]
    pub fn due_timers(&self, now: AgentTimestampMillis, limit: usize) -> Vec<AgentTimerEntry> {
        let mut due: Vec<_> = self
            .timers
            .values()
            .filter(|entry| entry.is_due(now))
            .cloned()
            .collect();
        due.sort_by(|left, right| {
            left.due_at
                .cmp(&right.due_at)
                .then_with(|| left.timer_id.cmp(&right.timer_id))
        });
        due.truncate(limit.max(1));
        due
    }

    /// Number of pending due timers.
    #[must_use]
    pub fn due_timer_count(&self, now: AgentTimestampMillis) -> usize {
        self.timers
            .values()
            .filter(|entry| entry.is_due(now))
            .count()
    }
}

/// Durable timer store facade.
pub struct AgentTimerStore<Store>
where
    Store: DurableStateStore<AgentTimerStoreState>,
{
    persistence_id: PersistenceId,
    store: Store,
    record: Option<StateRecord<AgentTimerStoreState>>,
}

impl<Store> AgentTimerStore<Store>
where
    Store: DurableStateStore<AgentTimerStoreState>,
{
    /// Creates a timer store using the default persistence id.
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self::with_persistence_id(store, agent_timer_store_persistence_id())
    }

    /// Creates a timer store using an explicit persistence id.
    #[must_use]
    pub const fn with_persistence_id(store: Store, persistence_id: PersistenceId) -> Self {
        Self {
            persistence_id,
            store,
            record: None,
        }
    }

    /// Timer-store persistence id.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Recovers the latest timer-store state.
    pub async fn recover(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentTimerResult<&AgentTimerStoreState> {
        self.record = Some(
            self.store
                .load(&self.persistence_id)
                .await?
                .unwrap_or_else(|| StateRecord::missing(AgentTimerStoreState::empty(now))),
        );
        Ok(&self.record.as_ref().expect("record set above").state)
    }

    /// Current recovered state.
    pub fn state(&self) -> AgentTimerResult<&AgentTimerStoreState> {
        self.record
            .as_ref()
            .map(|record| &record.state)
            .ok_or_else(|| AgentTimerError::Persistence {
                error: DurableError::store(
                    self.store.backend_name(),
                    "timer store is not recovered",
                ),
            })
    }

    /// Schedules a pending durable timer.
    pub async fn schedule_timer(
        &mut self,
        entry: AgentTimerEntry,
    ) -> AgentTimerResult<AgentTimerEntry> {
        entry.validate()?;
        if self.record.is_none() {
            self.recover(entry.created_at).await?;
        }
        let record = self.current_record()?;
        if record.state.timers.contains_key(&entry.timer_id) {
            return Err(AgentTimerError::TimerAlreadyExists {
                timer_id: entry.timer_id,
            });
        }
        let mut next = record.state;
        next.updated_at = entry.updated_at;
        next.timers.insert(entry.timer_id.clone(), entry.clone());
        self.persist(record.revision, next).await?;
        Ok(entry)
    }

    /// Returns due pending timers up to `limit`.
    pub async fn due_timers(
        &mut self,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> AgentTimerResult<Vec<AgentTimerEntry>> {
        if self.record.is_none() {
            self.recover(now).await?;
        }
        Ok(self.state()?.due_timers(now, limit))
    }

    /// Counts pending due timers.
    pub async fn due_timer_count(&mut self, now: AgentTimestampMillis) -> AgentTimerResult<usize> {
        if self.record.is_none() {
            self.recover(now).await?;
        }
        Ok(self.state()?.due_timer_count(now))
    }

    /// Marks one timer fired. Re-marking an already fired timer is idempotent.
    pub async fn mark_fired(
        &mut self,
        timer_id: &AgentTimerId,
        fired_at: AgentTimestampMillis,
    ) -> AgentTimerResult<AgentTimerEntry> {
        self.update_timer(timer_id, |entry| match entry.status {
            AgentTimerStatus::Fired => entry,
            AgentTimerStatus::Pending => entry.fired(fired_at),
            AgentTimerStatus::Cancelled => entry,
        })
        .await
    }

    /// Cancels one timer. Cancelling a fired or already cancelled timer is
    /// idempotent and leaves the existing terminal status intact.
    pub async fn cancel_timer(
        &mut self,
        timer_id: &AgentTimerId,
        cancelled_at: AgentTimestampMillis,
    ) -> AgentTimerResult<AgentTimerEntry> {
        self.update_timer(timer_id, |entry| match entry.status {
            AgentTimerStatus::Pending => entry.cancelled(cancelled_at),
            AgentTimerStatus::Fired | AgentTimerStatus::Cancelled => entry,
        })
        .await
    }

    async fn update_timer(
        &mut self,
        timer_id: &AgentTimerId,
        update: impl FnOnce(AgentTimerEntry) -> AgentTimerEntry,
    ) -> AgentTimerResult<AgentTimerEntry> {
        if self.record.is_none() {
            self.recover(AgentTimestampMillis::new(0)).await?;
        }
        let record = self.current_record()?;
        let Some(entry) = record.state.timers.get(timer_id).cloned() else {
            return Err(AgentTimerError::TimerNotFound {
                timer_id: timer_id.clone(),
            });
        };
        let updated = update(entry);
        let mut next = record.state;
        next.updated_at = updated.updated_at;
        next.timers.insert(timer_id.clone(), updated.clone());
        self.persist(record.revision, next).await?;
        Ok(updated)
    }

    async fn persist(
        &mut self,
        expected_revision: Revision,
        next: AgentTimerStoreState,
    ) -> AgentTimerResult<StateRecord<AgentTimerStoreState>> {
        let persisted = self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, next)
            .await?;
        self.record = Some(persisted.clone());
        Ok(persisted)
    }

    fn current_record(&self) -> AgentTimerResult<StateRecord<AgentTimerStoreState>> {
        self.record
            .clone()
            .ok_or_else(|| AgentTimerError::Persistence {
                error: DurableError::store(
                    self.store.backend_name(),
                    "timer store is not recovered",
                ),
            })
    }
}

/// Durable timer scanner settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentTimerScannerSettings {
    max_batch_size: usize,
}

impl AgentTimerScannerSettings {
    /// Creates scanner settings.
    #[must_use]
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size: max_batch_size.max(1),
        }
    }

    /// Max timers fired in one scan.
    #[must_use]
    pub const fn max_batch_size(self) -> usize {
        self.max_batch_size
    }
}

impl Default for AgentTimerScannerSettings {
    fn default() -> Self {
        Self::new(32)
    }
}

/// Scanner that injects due timers through durable inboxes.
pub struct AgentTimerScanner<TimerStore, WorkflowStore, RunStore, Clock = SystemWorkflowClock>
where
    TimerStore: DurableStateStore<AgentTimerStoreState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    RunStore: DurableStateStore<AgentRunState>,
    Clock: WorkflowClock,
{
    workflow: AgentWorkflow,
    timers: AgentTimerStore<TimerStore>,
    workflow_store: WorkflowStore,
    run_store: RunStore,
    clock: Clock,
    settings: AgentTimerScannerSettings,
    metrics: Arc<dyn MetricsRecorder>,
}

impl<TimerStore, WorkflowStore, RunStore>
    AgentTimerScanner<TimerStore, WorkflowStore, RunStore, SystemWorkflowClock>
where
    TimerStore: DurableStateStore<AgentTimerStoreState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    RunStore: DurableStateStore<AgentRunState>,
{
    /// Creates a scanner with the system clock and no-op metrics.
    #[must_use]
    pub fn new(
        workflow: AgentWorkflow,
        timers: AgentTimerStore<TimerStore>,
        workflow_store: WorkflowStore,
        run_store: RunStore,
    ) -> Self {
        Self::with_metrics(
            workflow,
            timers,
            workflow_store,
            run_store,
            Arc::new(NoopMetricsRecorder),
        )
    }

    /// Creates a scanner with the system clock and explicit metrics.
    #[must_use]
    pub fn with_metrics(
        workflow: AgentWorkflow,
        timers: AgentTimerStore<TimerStore>,
        workflow_store: WorkflowStore,
        run_store: RunStore,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::with_clock_and_metrics(
            workflow,
            timers,
            workflow_store,
            run_store,
            SystemWorkflowClock,
            AgentTimerScannerSettings::default(),
            metrics,
        )
    }
}

impl<TimerStore, WorkflowStore, RunStore, Clock>
    AgentTimerScanner<TimerStore, WorkflowStore, RunStore, Clock>
where
    TimerStore: DurableStateStore<AgentTimerStoreState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    RunStore: DurableStateStore<AgentRunState>,
    Clock: WorkflowClock,
{
    /// Creates a scanner with an explicit clock and no-op metrics.
    #[must_use]
    pub fn with_clock(
        workflow: AgentWorkflow,
        timers: AgentTimerStore<TimerStore>,
        workflow_store: WorkflowStore,
        run_store: RunStore,
        clock: Clock,
        settings: AgentTimerScannerSettings,
    ) -> Self {
        Self::with_clock_and_metrics(
            workflow,
            timers,
            workflow_store,
            run_store,
            clock,
            settings,
            Arc::new(NoopMetricsRecorder),
        )
    }

    /// Creates a scanner with an explicit clock, settings, and metrics.
    #[must_use]
    pub fn with_clock_and_metrics(
        workflow: AgentWorkflow,
        timers: AgentTimerStore<TimerStore>,
        workflow_store: WorkflowStore,
        run_store: RunStore,
        clock: Clock,
        settings: AgentTimerScannerSettings,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            workflow,
            timers,
            workflow_store,
            run_store,
            clock,
            settings,
            metrics,
        }
    }

    /// Durable timer store.
    #[must_use]
    pub const fn timers(&self) -> &AgentTimerStore<TimerStore> {
        &self.timers
    }

    /// Mutably accesses the durable timer store.
    #[must_use]
    pub fn timers_mut(&mut self) -> &mut AgentTimerStore<TimerStore> {
        &mut self.timers
    }

    /// Scans and fires due timers, bounded by the configured max batch size.
    pub async fn scan_due(&mut self) -> AgentTimerResult<AgentTimerScan> {
        let now = agent_timestamp_from_workflow_timestamp(self.clock.now());
        let due_count = self.timers.due_timer_count(now).await?;
        let due = self
            .timers
            .due_timers(now, self.settings.max_batch_size)
            .await?;
        let mut fired = Vec::with_capacity(due.len());
        for entry in due {
            fired.push(self.fire_timer(entry, now).await?);
        }
        Ok(AgentTimerScan {
            scanned_at: now,
            due_timer_count: due_count,
            max_batch_size: self.settings.max_batch_size,
            backpressure_limited: due_count > fired.len(),
            fired,
        })
    }

    async fn fire_timer(
        &mut self,
        entry: AgentTimerEntry,
        now: AgentTimestampMillis,
    ) -> AgentTimerResult<AgentTimerFiring> {
        if entry.workflow_id != self.workflow.workflow_id {
            return Err(AgentTimerError::WorkflowMismatch {
                timer_id: entry.timer_id,
                expected: self.workflow.workflow_id.clone(),
                actual: entry.workflow_id,
            });
        }

        let command = timer_fired_command(&entry, now)?;
        let mut inbox = AgentRunInbox::with_clock_and_metrics(
            entry.run_id.clone(),
            self.workflow_store.clone(),
            self.clock.clone(),
            self.metrics.clone(),
        );
        inbox.recover().await?;
        let inbox_acceptance = inbox.accept_command(command).await?;
        let transition = self.resume_waiting_run(&entry, now).await?;
        let marked = self.timers.mark_fired(&entry.timer_id, now).await?;
        self.record_timer_metric("fired", "none", &marked, now);
        Ok(AgentTimerFiring {
            timer: marked,
            inbox_acceptance,
            transition,
            late_by_ms: entry.late_by_ms(now),
        })
    }

    async fn resume_waiting_run(
        &self,
        entry: &AgentTimerEntry,
        now: AgentTimestampMillis,
    ) -> AgentTimerResult<Option<AgentRunTransition>> {
        let mut runner = AgentStepRunner::new(
            self.workflow.clone(),
            entry.run_id.clone(),
            self.run_store.clone(),
        );
        runner.recover().await?;
        match runner.state()? {
            Some(state) if state.status == AgentRunStatus::WaitingForTimer => {
                Ok(Some(runner.resume(now).await?))
            }
            Some(_) => Ok(None),
            None => Err(AgentTimerError::MissingRunState {
                run_id: entry.run_id.clone(),
            }),
        }
    }

    fn record_timer_metric(
        &self,
        outcome: &'static str,
        detail: &'static str,
        entry: &AgentTimerEntry,
        now: AgentTimestampMillis,
    ) {
        self.metrics.increment_counter(
            METRIC_AGENT_TIMERS,
            1,
            &[
                ("outcome", outcome),
                ("detail", detail),
                ("timer_status", entry.status.as_label()),
            ],
        );
        self.metrics.record_gauge(
            METRIC_AGENT_TIMERS_LATE_BY_MS,
            entry.late_by_ms(now) as f64,
            &[("outcome", outcome)],
        );
    }
}

/// Result of one bounded timer scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTimerScan {
    /// Scan timestamp.
    pub scanned_at: AgentTimestampMillis,
    /// Number of timers due at scan time before applying the batch limit.
    pub due_timer_count: usize,
    /// Max timers allowed in this scan.
    pub max_batch_size: usize,
    /// True when more timers were due than the scan was allowed to fire.
    pub backpressure_limited: bool,
    /// Timers fired by this scan.
    pub fired: Vec<AgentTimerFiring>,
}

/// Result of firing one timer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTimerFiring {
    /// Timer after being marked fired.
    pub timer: AgentTimerEntry,
    /// Durable inbox acceptance for the `TimerFired` command.
    pub inbox_acceptance: AgentInboxAcceptance,
    /// Run transition when the timer resumed a waiting run.
    pub transition: Option<AgentRunTransition>,
    /// Timer lateness at fire time.
    pub late_by_ms: u64,
}

/// Converts a timer into the durable inbox command injected at fire time.
pub fn timer_fired_command(
    entry: &AgentTimerEntry,
    received_at: AgentTimestampMillis,
) -> AgentTimerResult<AgentCommand> {
    let durability = AgentDurabilityMetadata::new(
        entry.deduplication_key.clone(),
        entry.causation_id.clone(),
        entry.correlation_id.clone(),
    )
    .telemetry_context(entry.telemetry_context.clone());
    let metadata = AgentCommandMetadata::new(
        entry.workflow_id.clone(),
        entry.run_id.clone(),
        AgentCommandId::new(format!("timer-fired:{}", entry.timer_id.as_str())),
        durability,
        entry.tenant.clone(),
        received_at,
    )?;
    Ok(AgentCommand::new(
        AgentCommandKind::TimerFired {
            timer_id: entry.timer_id.as_str().to_string(),
        },
        metadata,
    )?
    .attribute("timer_id", entry.timer_id.as_str())?)
}

#[must_use]
const fn agent_timestamp_from_workflow_timestamp(
    timestamp: WorkflowTimestamp,
) -> AgentTimestampMillis {
    AgentTimestampMillis::new(timestamp.as_millis())
}

fn require_timer(
    timer_id: &AgentTimerId,
    value: &str,
    field: &'static str,
) -> AgentTimerResult<()> {
    if value.trim().is_empty() {
        Err(AgentTimerError::InvalidTimer {
            timer_id: timer_id.clone(),
            field,
            reason: "required",
        })
    } else {
        Ok(())
    }
}

const fn facade_error_code(error: &AgentFacadeError) -> &'static str {
    match error {
        AgentFacadeError::InvalidCommandMetadata { .. } => "invalid-command-metadata",
        AgentFacadeError::InvalidCommand { .. } => "invalid-command",
        AgentFacadeError::InvalidEffectMetadata { .. } => "invalid-effect-metadata",
        AgentFacadeError::InvalidEffect { .. } => "invalid-effect",
    }
}
