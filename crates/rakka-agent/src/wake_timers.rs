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
use std::future::Future;
use std::pin::Pin;

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
    /// The durable entry: freshly parked, or the entry an earlier scheduling
    /// of the same occurrence already parked.
    pub entry: AgentWakeTimerEntry,
    /// Whether an entry for the same occurrence and task already existed.
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

    /// How many times a lost compare-and-set is retried against the
    /// re-recovered record before the loss is surfaced to the caller.
    ///
    /// The store is shared: scanners mark entries while the entities they
    /// deliver to park re-wakes into the same record from their own settle
    /// passes, so losing a write race is normal operation, not a fault. Every
    /// mutation here is idempotent over the re-read record, which is what
    /// makes the retry safe.
    const CAS_ATTEMPTS: usize = 4;

    /// Parks one occurrence durably.
    ///
    /// Scheduling is idempotent on the derived wake id: parking the same
    /// occurrence for the same task again returns the existing entry as a
    /// duplicate, however the re-park's delivery metadata (trigger, source,
    /// accepted time, policy revision) differs — a settle pass re-parking
    /// after a crash builds a fresh binding, and the durable entry is the
    /// truth it converges on. An entry that collides on the wake id but
    /// disagrees on the occurrence identity or the target task fails closed —
    /// that is not a redelivery, it is a disagreement. A lost compare-and-set
    /// re-recovers and retries, so racing a concurrent scanner or parker
    /// converges instead of failing.
    pub async fn schedule_occurrence(
        &mut self,
        entry: AgentWakeTimerEntry,
    ) -> AgentWakeTimerResult<AgentWakeTimerScheduled> {
        let mut lost = None;
        for _attempt in 0..Self::CAS_ATTEMPTS {
            if self.record.is_none() {
                self.recover(entry.created_at).await?;
            }
            let record = self.current_record()?;
            if let Some(existing) = record.state.entries.get(entry.wake_id()) {
                if existing.binding.same_identity(&entry.binding) && existing.task == entry.task {
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
            match self.persist(record.revision, next).await {
                Ok(_) => {
                    return Ok(AgentWakeTimerScheduled {
                        entry,
                        duplicate: false,
                    })
                }
                // The race loser re-reads and retries; `persist` already
                // dropped the stale cached record.
                Err(
                    error @ AgentWakeTimerError::Persistence {
                        error: DurableError::RevisionConflict { .. },
                    },
                ) => lost = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(lost.expect("the loop only exits with a lost race"))
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

    /// Removes terminal entries last touched before `before`, returning how
    /// many were pruned.
    ///
    /// Terminal entries otherwise accumulate: nothing rescans them, but the
    /// single durable record grows with each occurrence a goal ever
    /// consumed. Pruning is an explicit operational act, never something a
    /// scanner does implicitly — a pruned entry's redelivery window is over
    /// only when the operator says the controllers' deduplication no longer
    /// needs it.
    pub async fn prune_terminal(
        &mut self,
        before: AgentTimestampMillis,
    ) -> AgentWakeTimerResult<usize> {
        if self.record.is_none() {
            self.recover(before).await?;
        }
        let record = self.current_record()?;
        let mut next = record.state;
        let before_len = next.entries.len();
        next.entries.retain(|_, entry| {
            !(entry.status().is_terminal() && entry.updated_at().as_millis() < before.as_millis())
        });
        let pruned = before_len - next.entries.len();
        if pruned == 0 {
            return Ok(0);
        }
        next.updated_at = before;
        self.persist(record.revision, next).await?;
        Ok(pruned)
    }

    async fn update_entry(
        &mut self,
        wake: &AgentWakeId,
        update: impl Fn(AgentWakeTimerEntry) -> AgentWakeTimerEntry,
    ) -> AgentWakeTimerResult<AgentWakeTimerEntry> {
        let mut lost = None;
        for _attempt in 0..Self::CAS_ATTEMPTS {
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
            match self.persist(record.revision, next).await {
                Ok(_) => return Ok(updated),
                // The race loser re-reads and retries: a mark racing the
                // parker of the very entity the scanner just delivered to is
                // normal operation. The update is recomputed from the re-read
                // record, and a transition another writer already made is
                // answered idempotently above.
                Err(
                    error @ AgentWakeTimerError::Persistence {
                        error: DurableError::RevisionConflict { .. },
                    },
                ) => lost = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(lost.expect("the loop only exits with a lost race"))
    }

    async fn persist(
        &mut self,
        expected_revision: Revision,
        next: AgentWakeTimerStoreState,
    ) -> AgentWakeTimerResult<StateRecord<AgentWakeTimerStoreState>> {
        match self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, next)
            .await
        {
            Ok(persisted) => {
                self.record = Some(persisted.clone());
                Ok(persisted)
            }
            Err(error) => {
                if matches!(error, DurableError::RevisionConflict { .. }) {
                    // Another scanner won this write, so every operation
                    // computed from the cached record is now wrong. Drop it:
                    // the next call recovers the authoritative record instead
                    // of failing forever against a revision that no longer
                    // exists.
                    self.record = None;
                }
                Err(error.into())
            }
        }
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
    /// An entry already exists under this wake id for a different occurrence
    /// identity or target task.
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
                "an entry for the wake {wake} already exists with a different occurrence or task"
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

/// Boxed future returned by a re-wake parker.
pub type AgentWakeRewakeParkFuture<'a> =
    Pin<Box<dyn Future<Output = AgentWakeTimerResult<AgentWakeTimerScheduled>> + Send + 'a>>;

/// Parks controller-originated re-wake entries into a durable wake-timer
/// store — object-safely, so the task entity can hold one without growing a
/// store generic through every construction site.
///
/// Parking is idempotent on the derived wake id: a settle pass that crashes
/// between parking and marking re-parks the same occurrence — with fresh
/// delivery metadata, since the re-park is a new act at a later time — and
/// gets the durable entry back as a duplicate.
pub trait AgentWakeRewakeParker: Send + Sync {
    /// Parks one entry idempotently.
    fn park<'a>(&'a self, entry: AgentWakeTimerEntry) -> AgentWakeRewakeParkFuture<'a>;
}

/// A re-wake parker over a shared durable store.
///
/// Every park builds a fresh store facade over a clone of the shared store
/// and recovers it: two parkers — or a parker racing a scanner — may lose a
/// compare-and-set, and the loser's next operation recovers the
/// authoritative revision and converges, so no lock is needed.
pub struct SharedWakeTimerParker<Store>
where
    Store: DurableStateStore<AgentWakeTimerStoreState> + Clone,
{
    store: Store,
    persistence_id: PersistenceId,
}

impl<Store> SharedWakeTimerParker<Store>
where
    Store: DurableStateStore<AgentWakeTimerStoreState> + Clone,
{
    /// Creates a parker over the default wake-timer persistence id.
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self {
            store,
            persistence_id: agent_wake_timer_store_persistence_id(),
        }
    }

    /// Creates a parker over an explicit persistence id.
    #[must_use]
    pub const fn with_persistence_id(store: Store, persistence_id: PersistenceId) -> Self {
        Self {
            store,
            persistence_id,
        }
    }
}

impl<Store> AgentWakeRewakeParker for SharedWakeTimerParker<Store>
where
    Store: DurableStateStore<AgentWakeTimerStoreState> + Clone + Send + Sync + 'static,
{
    fn park<'a>(&'a self, entry: AgentWakeTimerEntry) -> AgentWakeRewakeParkFuture<'a> {
        Box::pin(async move {
            let mut timers = AgentWakeTimerStore::with_persistence_id(
                self.store.clone(),
                self.persistence_id.clone(),
            );
            timers.schedule_occurrence(entry).await
        })
    }
}

impl From<DurableError> for AgentWakeTimerError {
    fn from(error: DurableError) -> Self {
        Self::Persistence { error }
    }
}

#[cfg(test)]
mod tests {
    use rakka_persistence::InMemoryDurableStateStore;

    use super::*;
    use crate::definition::AgentRevisionNumber;
    use crate::identity::{AgentGoalId, TenantId};
    use crate::wake::{AgentWakeOccurrence, AgentWakeTriggerKind, ScheduleRevision};

    fn entry_for(due_at: u64, accepted_at: u64, task: &str) -> AgentWakeTimerEntry {
        let binding = AgentWakeBinding::new(
            TenantId::new("acme"),
            AgentGoalId::new("nightly-reconciliation").expect("goal id should be valid"),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Scheduled {
                due_at: AgentTimestampMillis::new(due_at),
            },
            AgentWakeTriggerKind::DurableTimer,
            AgentTimestampMillis::new(accepted_at),
            AgentRevisionNumber::INITIAL,
        )
        .expect("the binding derives");
        let task = AgentTaskId::new(task).expect("task id should be valid");
        AgentWakeTimerEntry::new(binding, task, AgentTimestampMillis::new(accepted_at))
    }

    fn entry(due_at: u64) -> AgentWakeTimerEntry {
        entry_for(due_at, due_at, "task-root")
    }

    fn shared_stores() -> (
        AgentWakeTimerStore<InMemoryDurableStateStore<AgentWakeTimerStoreState>>,
        AgentWakeTimerStore<InMemoryDurableStateStore<AgentWakeTimerStoreState>>,
    ) {
        let store = InMemoryDurableStateStore::new();
        (
            AgentWakeTimerStore::new(store.clone()),
            AgentWakeTimerStore::new(store),
        )
    }

    #[tokio::test]
    async fn the_loser_of_a_write_race_recovers_and_converges() {
        // Two scanners over the same durable record, both recovered at the
        // same revision — the topology the module doc promises is safe.
        let (mut winner, mut loser) = shared_stores();
        winner
            .recover(AgentTimestampMillis::new(0))
            .await
            .expect("the winner recovers");
        loser
            .recover(AgentTimestampMillis::new(0))
            .await
            .expect("the loser recovers");

        winner
            .schedule_occurrence(entry(1_000))
            .await
            .expect("the winner's write applies");

        // The loser's first compare-and-set is fenced by the revision the
        // winner consumed; the same call re-recovers the authoritative record
        // and retries, so losing the race converges without surfacing — no
        // manual recovery, no restart, no failed pass.
        let scheduled = loser
            .schedule_occurrence(entry(2_000))
            .await
            .expect("the race loser converges in one call");
        assert!(!scheduled.duplicate);

        let state = loser.state().expect("the loser holds the latest record");
        assert_eq!(state.entries().len(), 2);
        assert!(state.entry(entry(1_000).wake_id()).is_some());
        assert!(state.entry(entry(2_000).wake_id()).is_some());
    }

    #[tokio::test]
    async fn a_lost_mark_race_converges_in_the_same_call() {
        let (mut winner, mut loser) = shared_stores();
        winner
            .schedule_occurrence(entry(1_000))
            .await
            .expect("the entry parks");
        loser
            .recover(AgentTimestampMillis::new(0))
            .await
            .expect("the loser recovers");

        let wake = entry(1_000).wake_id().clone();
        winner
            .mark_fired(&wake, AgentTimestampMillis::new(1_500))
            .await
            .expect("the winner marks the entry fired");

        // The loser's stale mark loses the compare-and-set, re-recovers,
        // finds the entry already terminal, and answers idempotently — the
        // exact race a scanner runs against the parker of the entity it just
        // delivered to.
        let marked = loser
            .mark_fired(&wake, AgentTimestampMillis::new(1_600))
            .await
            .expect("the race loser converges in one call");
        assert_eq!(marked.status(), AgentWakeTimerStatus::Fired);
        assert_eq!(marked.fired_at(), Some(AgentTimestampMillis::new(1_500)));
    }

    #[tokio::test]
    async fn a_re_park_with_fresh_delivery_metadata_is_a_duplicate() {
        // A settle pass that crashed between parking and marking re-parks the
        // same occurrence later: same derived identity, later accepted time.
        // The re-park must converge on the durable entry as a duplicate — the
        // wake identity deliberately excludes delivery metadata, so a binding
        // rebuilt at a later moment is the same wake, not a disagreement.
        let (mut store, _) = shared_stores();
        let first = store
            .schedule_occurrence(entry(1_000))
            .await
            .expect("the first park applies");
        assert!(!first.duplicate);

        let reparked = store
            .schedule_occurrence(entry_for(1_000, 4_000, "task-root"))
            .await
            .expect("the re-park converges");
        assert!(reparked.duplicate);
        assert_eq!(
            reparked.entry, first.entry,
            "the durable entry is the truth the re-park converges on"
        );
    }

    #[tokio::test]
    async fn a_same_wake_bound_to_another_task_fails_closed() {
        // Same derived wake id aimed at a different root task is not a
        // redelivery — it is a wiring disagreement, and it stays an error.
        let (mut store, _) = shared_stores();
        store
            .schedule_occurrence(entry(1_000))
            .await
            .expect("the entry parks");

        let error = store
            .schedule_occurrence(entry_for(1_000, 1_000, "task-other"))
            .await
            .expect_err("a different target task fails closed");
        assert!(matches!(error, AgentWakeTimerError::Mismatch { .. }));
        assert_eq!(error.code(), "wake-timer-mismatch");
    }
}
