//! Durable workflow data model.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use rakka_persistence::PersistenceId;
use serde::{Deserialize, Serialize};

/// Stable workflow identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkflowId(String);

impl WorkflowId {
    /// Creates a workflow id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Durable persistence id used by the v1 snapshot storage shape.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new(format!("workflow:{}", self.0))
    }
}

impl Display for WorkflowId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable id for one inbound workflow message.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkflowMessageId(String);

impl WorkflowMessageId {
    /// Creates a workflow message id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for WorkflowMessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable id for one outbound workflow message.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OutboxMessageId(String);

impl OutboxMessageId {
    /// Creates an outbox message id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for OutboxMessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Application-provided idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeduplicationKey(String);

impl DeduplicationKey {
    /// Creates a deduplication key.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for DeduplicationKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Durable outbox dispatch target.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OutboxTarget {
    /// Dispatch to a local or remote actor path.
    Actor {
        /// Actor path or logical address.
        path: String,
    },
    /// Dispatch to a sharded entity.
    Entity {
        /// Entity type name.
        entity_type: String,
        /// Entity id.
        entity_id: String,
    },
    /// Dispatch through an application-defined handler.
    Application {
        /// Application handler name.
        name: String,
    },
}

impl OutboxTarget {
    /// Creates an actor target.
    #[must_use]
    pub fn actor(path: impl Into<String>) -> Self {
        Self::Actor { path: path.into() }
    }

    /// Creates a sharded entity target.
    #[must_use]
    pub fn entity(entity_type: impl Into<String>, entity_id: impl Into<String>) -> Self {
        Self::Entity {
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
        }
    }

    /// Creates an application-defined target.
    #[must_use]
    pub fn application(name: impl Into<String>) -> Self {
        Self::Application { name: name.into() }
    }
}

/// Deterministic workflow timestamp in milliseconds.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct WorkflowTimestamp(u64);

impl WorkflowTimestamp {
    /// Creates a timestamp from milliseconds.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns a timestamp advanced by milliseconds.
    #[must_use]
    pub const fn add_millis(self, millis: u64) -> Self {
        Self(self.0 + millis)
    }
}

/// Current workflow lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    /// Workflow can accept inbox work.
    Active,
    /// Workflow completed.
    Completed,
    /// Workflow failed.
    Failed,
}

/// Durable inbox entry status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxStatus {
    /// Accepted and waiting for processing.
    Pending,
    /// Handler is processing this inbox item.
    Processing,
    /// Inbox item completed.
    Completed,
    /// Inbox item failed.
    Failed,
}

/// Durable outbox entry status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboxStatus {
    /// Scheduled and waiting for dispatch.
    Pending,
    /// Dispatch is in progress.
    Dispatching,
    /// Dispatch succeeded.
    Dispatched,
    /// Dispatch failed but can be retried.
    Failed,
    /// Retry budget is exhausted.
    Exhausted,
}

/// Retry attempt metadata shared by inbox and outbox entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryAttempt {
    attempts: u32,
    max_attempts: Option<u32>,
    next_retry_at: Option<WorkflowTimestamp>,
    last_error: Option<String>,
}

impl RetryAttempt {
    /// Creates empty retry attempt metadata.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            attempts: 0,
            max_attempts: None,
            next_retry_at: None,
            last_error: None,
        }
    }

    /// Sets maximum attempts.
    #[must_use]
    pub const fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Records a retryable failure.
    #[must_use]
    pub fn record_failure(
        mut self,
        now: WorkflowTimestamp,
        delay_millis: u64,
        error: impl Into<String>,
    ) -> Self {
        self.attempts += 1;
        self.next_retry_at = Some(now.add_millis(delay_millis));
        self.last_error = Some(error.into());
        self
    }

    /// Records a terminal failure with no next retry scheduled.
    #[must_use]
    pub fn record_exhaustion(mut self, error: impl Into<String>) -> Self {
        self.attempts += 1;
        self.next_retry_at = None;
        self.last_error = Some(error.into());
        self
    }

    /// Number of attempts already made.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Maximum attempts, when configured.
    #[must_use]
    pub const fn max_attempts_value(&self) -> Option<u32> {
        self.max_attempts
    }

    /// Next retry time, when scheduled.
    #[must_use]
    pub const fn next_retry_at(&self) -> Option<WorkflowTimestamp> {
        self.next_retry_at
    }

    /// Last failure detail, when available.
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

impl Default for RetryAttempt {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic retry jitter option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryJitter {
    /// No jitter.
    None,
    /// Add a fixed number of milliseconds to each computed delay.
    FixedMillis(u64),
}

impl RetryJitter {
    const fn millis(self) -> u64 {
        match self {
            Self::None => 0,
            Self::FixedMillis(millis) => millis,
        }
    }
}

/// Retry policy for durable outbox dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    max_attempts: u32,
    initial_backoff_millis: u64,
    max_backoff_millis: u64,
    multiplier: u32,
    jitter: RetryJitter,
}

impl RetryPolicy {
    /// Creates a retry policy.
    #[must_use]
    pub const fn new(
        max_attempts: u32,
        initial_backoff_millis: u64,
        max_backoff_millis: u64,
    ) -> Self {
        Self {
            max_attempts,
            initial_backoff_millis,
            max_backoff_millis,
            multiplier: 2,
            jitter: RetryJitter::None,
        }
    }

    /// Sets the exponential backoff multiplier.
    #[must_use]
    pub const fn multiplier(mut self, multiplier: u32) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Sets deterministic jitter.
    #[must_use]
    pub const fn jitter(mut self, jitter: RetryJitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Maximum dispatch attempts before exhaustion.
    #[must_use]
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Initial backoff in milliseconds.
    #[must_use]
    pub const fn initial_backoff_millis(&self) -> u64 {
        self.initial_backoff_millis
    }

    /// Maximum backoff in milliseconds.
    #[must_use]
    pub const fn max_backoff_millis(&self) -> u64 {
        self.max_backoff_millis
    }

    /// Backoff multiplier.
    #[must_use]
    pub const fn multiplier_value(&self) -> u32 {
        self.multiplier
    }

    /// Jitter option.
    #[must_use]
    pub const fn jitter_value(&self) -> RetryJitter {
        self.jitter
    }

    /// Returns true when the given failed-attempt count exhausts the budget.
    #[must_use]
    pub const fn is_exhausted_after(&self, attempts: u32) -> bool {
        attempts >= self.max_attempts
    }

    /// Computes the retry delay after a failed attempt.
    #[must_use]
    pub fn delay_after_failure(&self, attempts: u32) -> u64 {
        let exponent = attempts.saturating_sub(1);
        let mut delay = self.initial_backoff_millis;
        for _step in 0..exponent {
            delay = delay.saturating_mul(u64::from(self.multiplier.max(1)));
        }
        delay
            .min(self.max_backoff_millis)
            .saturating_add(self.jitter.millis())
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3, 1_000, 30_000)
    }
}

/// One durable inbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxEntry {
    message_id: WorkflowMessageId,
    deduplication_key: Option<DeduplicationKey>,
    message_type: String,
    payload: Vec<u8>,
    status: InboxStatus,
    attempts: RetryAttempt,
    accepted_at: WorkflowTimestamp,
    updated_at: WorkflowTimestamp,
}

impl InboxEntry {
    /// Creates a durable inbox entry.
    #[must_use]
    pub fn new(
        message_id: WorkflowMessageId,
        deduplication_key: Option<DeduplicationKey>,
        message_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        now: WorkflowTimestamp,
    ) -> Self {
        Self {
            message_id,
            deduplication_key,
            message_type: message_type.into(),
            payload: payload.into(),
            status: InboxStatus::Pending,
            attempts: RetryAttempt::new(),
            accepted_at: now,
            updated_at: now,
        }
    }

    /// Message id.
    #[must_use]
    pub const fn message_id(&self) -> &WorkflowMessageId {
        &self.message_id
    }

    /// Deduplication key, when supplied.
    #[must_use]
    pub const fn deduplication_key(&self) -> Option<&DeduplicationKey> {
        self.deduplication_key.as_ref()
    }

    /// Message type label.
    #[must_use]
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Opaque durable payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Inbox status.
    #[must_use]
    pub const fn status(&self) -> InboxStatus {
        self.status
    }

    /// Retry attempt metadata.
    #[must_use]
    pub const fn attempts(&self) -> &RetryAttempt {
        &self.attempts
    }

    /// Accepted timestamp.
    #[must_use]
    pub const fn accepted_at(&self) -> WorkflowTimestamp {
        self.accepted_at
    }

    /// Last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> WorkflowTimestamp {
        self.updated_at
    }

    pub(crate) fn set_status(&mut self, status: InboxStatus, now: WorkflowTimestamp) {
        self.status = status;
        self.updated_at = now;
    }
}

/// One durable outbox entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    message_id: OutboxMessageId,
    deduplication_key: Option<DeduplicationKey>,
    target: OutboxTarget,
    message_type: String,
    payload: Vec<u8>,
    status: OutboxStatus,
    attempts: RetryAttempt,
    retry_policy: RetryPolicy,
    scheduled_at: WorkflowTimestamp,
    updated_at: WorkflowTimestamp,
}

impl OutboxEntry {
    /// Creates a durable outbox entry.
    #[must_use]
    pub fn new(
        message_id: OutboxMessageId,
        deduplication_key: Option<DeduplicationKey>,
        target: OutboxTarget,
        message_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        scheduled_at: WorkflowTimestamp,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            message_id,
            deduplication_key,
            target,
            message_type: message_type.into(),
            payload: payload.into(),
            status: OutboxStatus::Pending,
            attempts: RetryAttempt::new().max_attempts(retry_policy.max_attempts()),
            retry_policy,
            scheduled_at,
            updated_at: scheduled_at,
        }
    }

    /// Message id.
    #[must_use]
    pub const fn message_id(&self) -> &OutboxMessageId {
        &self.message_id
    }

    /// Deduplication key, when supplied.
    #[must_use]
    pub const fn deduplication_key(&self) -> Option<&DeduplicationKey> {
        self.deduplication_key.as_ref()
    }

    /// Dispatch target.
    #[must_use]
    pub const fn target(&self) -> &OutboxTarget {
        &self.target
    }

    /// Message type label.
    #[must_use]
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Opaque durable payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Outbox status.
    #[must_use]
    pub const fn status(&self) -> OutboxStatus {
        self.status
    }

    /// Retry attempt metadata.
    #[must_use]
    pub const fn attempts(&self) -> &RetryAttempt {
        &self.attempts
    }

    /// Retry policy.
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    /// Scheduled dispatch timestamp.
    #[must_use]
    pub const fn scheduled_at(&self) -> WorkflowTimestamp {
        self.scheduled_at
    }

    /// Last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> WorkflowTimestamp {
        self.updated_at
    }

    /// Returns true when this entry is due for dispatch or recovery.
    #[must_use]
    pub fn is_due(&self, now: WorkflowTimestamp) -> bool {
        match self.status {
            OutboxStatus::Pending => self.scheduled_at <= now,
            OutboxStatus::Failed => self
                .attempts
                .next_retry_at()
                .is_some_and(|next| next <= now),
            OutboxStatus::Dispatching => true,
            OutboxStatus::Dispatched | OutboxStatus::Exhausted => false,
        }
    }

    pub(crate) fn set_status(&mut self, status: OutboxStatus, now: WorkflowTimestamp) {
        self.status = status;
        self.updated_at = now;
    }

    pub(crate) fn mark_dispatched(&mut self, now: WorkflowTimestamp) {
        self.set_status(OutboxStatus::Dispatched, now);
    }

    pub(crate) fn record_failure(
        &mut self,
        now: WorkflowTimestamp,
        message: impl Into<String>,
    ) -> OutboxFailureTransition {
        let failed_attempts = self.attempts.attempts().saturating_add(1);
        if self.retry_policy.is_exhausted_after(failed_attempts) {
            self.attempts = self.attempts.clone().record_exhaustion(message);
            self.set_status(OutboxStatus::Exhausted, now);
            OutboxFailureTransition::Exhausted
        } else {
            let delay = self.retry_policy.delay_after_failure(failed_attempts);
            self.attempts = self.attempts.clone().record_failure(now, delay, message);
            self.set_status(OutboxStatus::Failed, now);
            OutboxFailureTransition::Retry {
                next_retry_at: self
                    .attempts
                    .next_retry_at()
                    .expect("retry failure should schedule next retry"),
            }
        }
    }
}

/// Result of recording a failed outbox dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxFailureTransition {
    /// The entry remains retryable.
    Retry {
        /// Next retry timestamp.
        next_retry_at: WorkflowTimestamp,
    },
    /// Retry budget was exhausted.
    Exhausted,
}

/// Durable workflow telemetry event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowTelemetryEvent {
    /// Outbox dispatch succeeded.
    OutboxDispatchSucceeded {
        /// Outbox message id.
        message_id: OutboxMessageId,
        /// Success timestamp.
        at: WorkflowTimestamp,
    },
    /// Outbox dispatch failed and was scheduled for retry.
    OutboxDispatchRetried {
        /// Outbox message id.
        message_id: OutboxMessageId,
        /// Failed attempt number.
        attempt: u32,
        /// Next retry timestamp.
        next_retry_at: WorkflowTimestamp,
        /// Failure detail.
        message: String,
    },
    /// Outbox dispatch timed out.
    OutboxDispatchTimedOut {
        /// Outbox message id.
        message_id: OutboxMessageId,
        /// Failed attempt number.
        attempt: u32,
        /// Next retry timestamp, when another attempt remains.
        next_retry_at: Option<WorkflowTimestamp>,
        /// Timeout detail.
        message: String,
    },
    /// Outbox dispatch exhausted its retry policy.
    OutboxDispatchExhausted {
        /// Outbox message id.
        message_id: OutboxMessageId,
        /// Total attempts made.
        attempts: u32,
        /// Failure detail.
        message: String,
    },
}

/// Durable workflow snapshot stored under one `PersistenceId`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowState {
    workflow_id: WorkflowId,
    status: WorkflowStatus,
    inbox: BTreeMap<WorkflowMessageId, InboxEntry>,
    inbox_deduplication: BTreeMap<DeduplicationKey, WorkflowMessageId>,
    outbox: BTreeMap<OutboxMessageId, OutboxEntry>,
    outbox_deduplication: BTreeMap<DeduplicationKey, OutboxMessageId>,
    updated_at: WorkflowTimestamp,
}

impl WorkflowState {
    /// Creates an empty active workflow snapshot.
    #[must_use]
    pub fn empty(workflow_id: WorkflowId, now: WorkflowTimestamp) -> Self {
        Self {
            workflow_id,
            status: WorkflowStatus::Active,
            inbox: BTreeMap::new(),
            inbox_deduplication: BTreeMap::new(),
            outbox: BTreeMap::new(),
            outbox_deduplication: BTreeMap::new(),
            updated_at: now,
        }
    }

    /// Workflow id.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Workflow status.
    #[must_use]
    pub const fn status(&self) -> WorkflowStatus {
        self.status
    }

    /// Durable inbox entries.
    #[must_use]
    pub const fn inbox(&self) -> &BTreeMap<WorkflowMessageId, InboxEntry> {
        &self.inbox
    }

    /// Durable outbox entries.
    #[must_use]
    pub const fn outbox(&self) -> &BTreeMap<OutboxMessageId, OutboxEntry> {
        &self.outbox
    }

    /// Last snapshot update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> WorkflowTimestamp {
        self.updated_at
    }

    /// Finds an inbox entry by message id.
    #[must_use]
    pub fn inbox_entry(&self, message_id: &WorkflowMessageId) -> Option<&InboxEntry> {
        self.inbox.get(message_id)
    }

    /// Finds an inbox entry by deduplication key.
    #[must_use]
    pub fn inbox_entry_by_deduplication_key(&self, key: &DeduplicationKey) -> Option<&InboxEntry> {
        self.inbox_deduplication
            .get(key)
            .and_then(|message_id| self.inbox.get(message_id))
    }

    /// Finds an outbox entry by message id.
    #[must_use]
    pub fn outbox_entry(&self, message_id: &OutboxMessageId) -> Option<&OutboxEntry> {
        self.outbox.get(message_id)
    }

    /// Finds an outbox entry by deduplication key.
    #[must_use]
    pub fn outbox_entry_by_deduplication_key(
        &self,
        key: &DeduplicationKey,
    ) -> Option<&OutboxEntry> {
        self.outbox_deduplication
            .get(key)
            .and_then(|message_id| self.outbox.get(message_id))
    }

    /// Inbox entries that should be resumed after recovery.
    #[must_use]
    pub fn recoverable_inbox(&self) -> Vec<InboxEntry> {
        self.inbox
            .values()
            .filter(|entry| {
                matches!(
                    entry.status(),
                    InboxStatus::Pending | InboxStatus::Processing | InboxStatus::Failed
                )
            })
            .cloned()
            .collect()
    }

    /// Outbox entries due for dispatch or retry at `now`.
    #[must_use]
    pub fn due_outbox(&self, now: WorkflowTimestamp) -> Vec<OutboxEntry> {
        self.outbox
            .values()
            .filter(|entry| entry.is_due(now))
            .cloned()
            .collect()
    }

    pub(crate) fn insert_inbox(&mut self, entry: InboxEntry) {
        if let Some(key) = entry.deduplication_key().cloned() {
            self.inbox_deduplication
                .insert(key, entry.message_id().clone());
        }
        self.updated_at = entry.updated_at();
        self.inbox.insert(entry.message_id().clone(), entry);
    }

    pub(crate) fn insert_outbox(&mut self, entry: OutboxEntry) {
        if let Some(key) = entry.deduplication_key().cloned() {
            self.outbox_deduplication
                .insert(key, entry.message_id().clone());
        }
        self.updated_at = entry.updated_at();
        self.outbox.insert(entry.message_id().clone(), entry);
    }

    pub(crate) fn update_inbox_status(
        &mut self,
        message_id: &WorkflowMessageId,
        status: InboxStatus,
        now: WorkflowTimestamp,
    ) -> Option<InboxEntry> {
        let entry = self.inbox.get_mut(message_id)?;
        entry.set_status(status, now);
        self.updated_at = now;
        Some(entry.clone())
    }

    pub(crate) fn update_outbox(
        &mut self,
        message_id: &OutboxMessageId,
        update: impl FnOnce(&mut OutboxEntry),
    ) -> Option<OutboxEntry> {
        let entry = self.outbox.get_mut(message_id)?;
        update(entry);
        self.updated_at = entry.updated_at();
        Some(entry.clone())
    }
}
