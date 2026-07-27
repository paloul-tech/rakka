//! Durable wake-timer store: the shared scanner's index of parked
//! occurrences.
//!
//! One durable record holds every occurrence waiting to be delivered to a
//! continuous goal's controller, keyed by the derived [`AgentWakeId`] — so
//! scheduling the same occurrence twice is a construction-level duplicate,
//! not a second timer. The record is compare-and-set state: any pod may host
//! a scanner over it, two scanners may race, and the loser of a write simply
//! recovers and rescans, because every downstream effect of a scan is
//! deduplicated by the wake's admission operation id.
//!
//! The store never *creates* an occurrence. Scheduling is an explicit act of
//! the application's schedule or ingress layer; scanning only recovers what
//! was durably scheduled and became due
//! ([specification 15](../../../docs/plans/rakka-agent/spec.md)). Scanner and
//! pod uptime are invisible here — only entries and logical time exist.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentTimestampMillis, StateSchemaVersion};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision, StateRecord};
use serde::{Deserialize, Serialize};

use crate::identity::{AgentTaskId, AgentWakeId};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_WAKE_TIMER_SCHEMA_VERSION,
};
use crate::wake::AgentWakeBinding;

/// Prefix used for durable wake-timer-store persistence ids.
pub const AGENT_WAKE_TIMER_PERSISTENCE_PREFIX: &str = "agent-wake-timers";

/// Default wake-timer-store persistence id.
pub const DEFAULT_AGENT_WAKE_TIMER_STORE_ID: &str = "default";

/// Creates the default wake-timer-store persistence id.
#[must_use]
pub fn agent_wake_timer_store_persistence_id() -> PersistenceId {
    PersistenceId::new(format!(
        "{AGENT_WAKE_TIMER_PERSISTENCE_PREFIX}:{DEFAULT_AGENT_WAKE_TIMER_STORE_ID}"
    ))
}

/// Result type for durable wake-timer operations.
pub type AgentWakeTimerResult<T> = Result<T, AgentWakeTimerError>;

/// Lifecycle status of one parked occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeTimerStatus {
    /// Waiting to become due and be delivered.
    Pending,
    /// Delivered to the controller, which dispositioned it.
    Fired,
    /// Refused by the controller for carrying an obsolete schedule revision;
    /// terminal, so a rescan never redelivers it.
    Fenced,
    /// Cancelled before delivery.
    Cancelled,
}

impl AgentWakeTimerStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Fired => "fired",
            Self::Fenced => "fenced",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the status is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

impl Display for AgentWakeTimerStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One durably parked occurrence: the wake binding to deliver and the root
/// control task to deliver it to.
///
/// The binding *is* the payload — its deserialization re-derives the wake
/// identity and fails closed on a record its own components do not derive, so
/// loading an entry is already the integrity check a scanner needs before
/// delivering it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeTimerEntry {
    binding: AgentWakeBinding,
    task: AgentTaskId,
    status: AgentWakeTimerStatus,
    created_at: AgentTimestampMillis,
    updated_at: AgentTimestampMillis,
    fired_at: Option<AgentTimestampMillis>,
}

impl AgentWakeTimerEntry {
    /// Parks one occurrence for delivery to a root control task.
    #[must_use]
    pub const fn new(
        binding: AgentWakeBinding,
        task: AgentTaskId,
        created_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            binding,
            task,
            status: AgentWakeTimerStatus::Pending,
            created_at,
            updated_at: created_at,
            fired_at: None,
        }
    }

    /// The wake binding to deliver.
    #[must_use]
    pub const fn binding(&self) -> &AgentWakeBinding {
        &self.binding
    }

    /// The root control task the occurrence is delivered to.
    #[must_use]
    pub const fn task(&self) -> &AgentTaskId {
        &self.task
    }

    /// The derived wake identity keying this entry.
    #[must_use]
    pub const fn wake_id(&self) -> &AgentWakeId {
        self.binding.wake_id()
    }

    /// Current lifecycle status.
    #[must_use]
    pub const fn status(&self) -> AgentWakeTimerStatus {
        self.status
    }

    /// When the entry was parked.
    #[must_use]
    pub const fn created_at(&self) -> AgentTimestampMillis {
        self.created_at
    }

    /// When the entry last changed.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// When the entry was delivered, once it has been.
    #[must_use]
    pub const fn fired_at(&self) -> Option<AgentTimestampMillis> {
        self.fired_at
    }

    /// When the occurrence becomes deliverable: its scheduled due time, or the
    /// moment it was parked for occurrence kinds without one.
    #[must_use]
    pub fn due_time(&self) -> AgentTimestampMillis {
        self.binding.due_at().unwrap_or(self.created_at)
    }

    /// Whether the entry is pending and due.
    #[must_use]
    pub fn is_due(&self, now: AgentTimestampMillis) -> bool {
        matches!(self.status, AgentWakeTimerStatus::Pending)
            && self.due_time().as_millis() <= now.as_millis()
    }

    fn with_status(mut self, status: AgentWakeTimerStatus, at: AgentTimestampMillis) -> Self {
        self.status = status;
        self.updated_at = at;
        if matches!(status, AgentWakeTimerStatus::Fired) {
            self.fired_at = Some(at);
        }
        self
    }
}

/// Durable wake-timer index state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeTimerStoreState {
    schema_version: StateSchemaVersion,
    entries: BTreeMap<AgentWakeId, AgentWakeTimerEntry>,
    updated_at: AgentTimestampMillis,
}

impl AgentWakeTimerStoreState {
    /// Creates an empty wake-timer store state.
    #[must_use]
    pub fn empty(now: AgentTimestampMillis) -> Self {
        Self {
            schema_version: CURRENT_AGENT_WAKE_TIMER_SCHEMA_VERSION,
            entries: BTreeMap::new(),
            updated_at: now,
        }
    }

    /// All entries by wake id in deterministic order.
    #[must_use]
    pub const fn entries(&self) -> &BTreeMap<AgentWakeId, AgentWakeTimerEntry> {
        &self.entries
    }

    /// Last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// Returns one entry by wake id.
    #[must_use]
    pub fn entry(&self, wake: &AgentWakeId) -> Option<&AgentWakeTimerEntry> {
        self.entries.get(wake)
    }

    /// Returns pending due entries in due-time order.
    #[must_use]
    pub fn due_entries(&self, now: AgentTimestampMillis, limit: usize) -> Vec<AgentWakeTimerEntry> {
        let mut due: Vec<_> = self
            .entries
            .values()
            .filter(|entry| entry.is_due(now))
            .cloned()
            .collect();
        due.sort_by(|left, right| {
            left.due_time()
                .as_millis()
                .cmp(&right.due_time().as_millis())
                .then_with(|| left.wake_id().cmp(right.wake_id()))
        });
        due.truncate(limit.max(1));
        due
    }

    /// Number of pending due entries.
    #[must_use]
    pub fn due_entry_count(&self, now: AgentTimestampMillis) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.is_due(now))
            .count()
    }
}

impl VersionedAgentRecord for AgentWakeTimerStoreState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::WakeTimerState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// What scheduling one occurrence did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWakeTimerScheduled {
    /// The durable entry: freshly parked, or the identical entry an earlier
    /// scheduling already parked.
    pub entry: AgentWakeTimerEntry,
    /// Whether an identical entry already existed.
    pub duplicate: bool,
}

/// Durable wake-timer store facade.
///
/// Writes go through compare-and-set with the recovered revision: there is no
/// scanner lease and no ownership, because losing a race costs a recover and
/// rescan while every delivery stays deduplicated downstream.
pub struct AgentWakeTimerStore<Store>
where
    Store: DurableStateStore<AgentWakeTimerStoreState>,
{
    persistence_id: PersistenceId,
    store: Store,
    schema_policy: AgentSchemaPolicy,
    record: Option<StateRecord<AgentWakeTimerStoreState>>,
}

impl<Store> AgentWakeTimerStore<Store>
where
    Store: DurableStateStore<AgentWakeTimerStoreState>,
{
    /// Creates a wake-timer store using the default persistence id.
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self::with_persistence_id(store, agent_wake_timer_store_persistence_id())
    }

    /// Creates a wake-timer store using an explicit persistence id.
    #[must_use]
    pub fn with_persistence_id(store: Store, persistence_id: PersistenceId) -> Self {
        Self {
            persistence_id,
            store,
            schema_policy: AgentSchemaPolicy::n_plus_one(),
            record: None,
        }
    }

    /// Replaces the schema-compatibility policy applied on recovery.
    #[must_use]
    pub const fn with_schema_policy(mut self, schema_policy: AgentSchemaPolicy) -> Self {
        self.schema_policy = schema_policy;
        self
    }

    /// Wake-timer-store persistence id.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Recovers the latest wake-timer state, failing closed on a schema
    /// version this binary cannot read.
    pub async fn recover(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentWakeTimerResult<&AgentWakeTimerStoreState> {
        let record = self
            .store
            .load(&self.persistence_id)
            .await?
            .unwrap_or_else(|| StateRecord::missing(AgentWakeTimerStoreState::empty(now)));
        self.schema_policy.check_record(&record.state)?;
        self.record = Some(record);
        Ok(&self.record.as_ref().expect("record set above").state)
    }

    /// Current recovered state.
    pub fn state(&self) -> AgentWakeTimerResult<&AgentWakeTimerStoreState> {
        self.record
            .as_ref()
            .map(|record| &record.state)
            .ok_or_else(|| self.not_recovered())
    }

    /// Parks one occurrence durably.
    ///
    /// Scheduling is idempotent on the derived wake id: parking the identical
    /// occurrence again returns the existing entry as a duplicate, while an
    /// entry that collides on the wake id but differs in binding or target
    /// task fails closed — that is not a redelivery, it is a disagreement.
    pub async fn schedule_occurrence(
        &mut self,
        entry: AgentWakeTimerEntry,
    ) -> AgentWakeTimerResult<AgentWakeTimerScheduled> {
        if self.record.is_none() {
            self.recover(entry.created_at).await?;
        }
        let record = self.current_record()?;
        if let Some(existing) = record.state.entries.get(entry.wake_id()) {
            if existing.binding == entry.binding && existing.task == entry.task {
                return Ok(AgentWakeTimerScheduled {
                    entry: existing.clone(),
                    duplicate: true,
                });
            }
            return Err(AgentWakeTimerError::Mismatch {
                wake: entry.wake_id().clone(),
            });
        }
        let mut next = record.state;
        next.updated_at = entry.updated_at;
        next.entries.insert(entry.wake_id().clone(), entry.clone());
        self.persist(record.revision, next).await?;
        Ok(AgentWakeTimerScheduled {
            entry,
            duplicate: false,
        })
    }

    /// Returns pending due entries up to `limit`.
    pub async fn due_entries(
        &mut self,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> AgentWakeTimerResult<Vec<AgentWakeTimerEntry>> {
        if self.record.is_none() {
            self.recover(now).await?;
        }
        Ok(self.state()?.due_entries(now, limit))
    }

    /// Counts pending due entries.
    pub async fn due_entry_count(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentWakeTimerResult<usize> {
        if self.record.is_none() {
            self.recover(now).await?;
        }
        Ok(self.state()?.due_entry_count(now))
    }

    /// Marks one entry fired. Re-marking a terminal entry is idempotent.
    pub async fn mark_fired(
        &mut self,
        wake: &AgentWakeId,
        fired_at: AgentTimestampMillis,
    ) -> AgentWakeTimerResult<AgentWakeTimerEntry> {
        self.update_entry(wake, |entry| match entry.status {
            AgentWakeTimerStatus::Pending => {
                entry.with_status(AgentWakeTimerStatus::Fired, fired_at)
            }
            _ => entry,
        })
        .await
    }

    /// Marks one entry fenced. Re-marking a terminal entry is idempotent.
    pub async fn mark_fenced(
        &mut self,
        wake: &AgentWakeId,
        fenced_at: AgentTimestampMillis,
    ) -> AgentWakeTimerResult<AgentWakeTimerEntry> {
        self.update_entry(wake, |entry| match entry.status {
            AgentWakeTimerStatus::Pending => {
                entry.with_status(AgentWakeTimerStatus::Fenced, fenced_at)
            }
            _ => entry,
        })
        .await
    }

    /// Cancels one pending entry. Cancelling a terminal entry is idempotent
    /// and leaves the existing terminal status intact.
    pub async fn cancel(
        &mut self,
        wake: &AgentWakeId,
        cancelled_at: AgentTimestampMillis,
    ) -> AgentWakeTimerResult<AgentWakeTimerEntry> {
        self.update_entry(wake, |entry| match entry.status {
            AgentWakeTimerStatus::Pending => {
                entry.with_status(AgentWakeTimerStatus::Cancelled, cancelled_at)
            }
            _ => entry,
        })
        .await
    }

    async fn update_entry(
        &mut self,
        wake: &AgentWakeId,
        update: impl FnOnce(AgentWakeTimerEntry) -> AgentWakeTimerEntry,
    ) -> AgentWakeTimerResult<AgentWakeTimerEntry> {
        if self.record.is_none() {
            self.recover(AgentTimestampMillis::new(0)).await?;
        }
        let record = self.current_record()?;
        let Some(entry) = record.state.entries.get(wake).cloned() else {
            return Err(AgentWakeTimerError::NotFound { wake: wake.clone() });
        };
        let updated = update(entry.clone());
        if updated == entry {
            return Ok(entry);
        }
        let mut next = record.state;
        next.updated_at = updated.updated_at;
        next.entries.insert(wake.clone(), updated.clone());
        self.persist(record.revision, next).await?;
        Ok(updated)
    }

    async fn persist(
        &mut self,
        expected_revision: Revision,
        next: AgentWakeTimerStoreState,
    ) -> AgentWakeTimerResult<StateRecord<AgentWakeTimerStoreState>> {
        let persisted = self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, next)
            .await?;
        self.record = Some(persisted.clone());
        Ok(persisted)
    }

    fn current_record(&self) -> AgentWakeTimerResult<StateRecord<AgentWakeTimerStoreState>> {
        self.record.clone().ok_or_else(|| self.not_recovered())
    }

    fn not_recovered(&self) -> AgentWakeTimerError {
        AgentWakeTimerError::Persistence {
            error: DurableError::store(
                self.store.backend_name(),
                "wake-timer store is not recovered",
            ),
        }
    }
}

/// Durable wake-timer failures.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentWakeTimerError {
    /// An entry already exists under this wake id with a different binding or
    /// target task.
    Mismatch {
        /// The colliding wake id.
        wake: AgentWakeId,
    },
    /// No entry exists under this wake id.
    NotFound {
        /// The unknown wake id.
        wake: AgentWakeId,
    },
    /// The persisted store state failed its schema-compatibility check.
    Schema(AgentSchemaError),
    /// The durable store refused the operation.
    Persistence {
        /// The store failure.
        error: DurableError,
    },
}

impl AgentWakeTimerError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Mismatch { .. } => "wake-timer-mismatch",
            Self::NotFound { .. } => "wake-timer-not-found",
            Self::Schema(error) => error.code(),
            Self::Persistence { .. } => "wake-timer-persistence",
        }
    }
}

impl Display for AgentWakeTimerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mismatch { wake } => write!(
                f,
                "an entry for the wake {wake} already exists with a different binding or task"
            ),
            Self::NotFound { wake } => write!(f, "no wake-timer entry exists for the wake {wake}"),
            Self::Schema(error) => write!(f, "wake-timer state failed its schema check: {error:?}"),
            Self::Persistence { error } => write!(f, "wake-timer store failed: {error}"),
        }
    }
}

impl Error for AgentWakeTimerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Persistence { error } => Some(error),
            _ => None,
        }
    }
}

impl From<AgentSchemaError> for AgentWakeTimerError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<DurableError> for AgentWakeTimerError {
    fn from(error: DurableError) -> Self {
        Self::Persistence { error }
    }
}
