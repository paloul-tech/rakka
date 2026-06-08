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
    message_type: String,
    payload: Vec<u8>,
    status: OutboxStatus,
    attempts: RetryAttempt,
    scheduled_at: WorkflowTimestamp,
    updated_at: WorkflowTimestamp,
}

impl OutboxEntry {
    /// Creates a durable outbox entry.
    #[must_use]
    pub fn new(
        message_id: OutboxMessageId,
        deduplication_key: Option<DeduplicationKey>,
        message_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        scheduled_at: WorkflowTimestamp,
    ) -> Self {
        Self {
            message_id,
            deduplication_key,
            message_type: message_type.into(),
            payload: payload.into(),
            status: OutboxStatus::Pending,
            attempts: RetryAttempt::new(),
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
}

/// Durable workflow snapshot stored under one `PersistenceId`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowState {
    workflow_id: WorkflowId,
    status: WorkflowStatus,
    inbox: BTreeMap<WorkflowMessageId, InboxEntry>,
    inbox_deduplication: BTreeMap<DeduplicationKey, WorkflowMessageId>,
    outbox: BTreeMap<OutboxMessageId, OutboxEntry>,
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

    pub(crate) fn insert_inbox(&mut self, entry: InboxEntry) {
        if let Some(key) = entry.deduplication_key().cloned() {
            self.inbox_deduplication
                .insert(key, entry.message_id().clone());
        }
        self.updated_at = entry.updated_at();
        self.inbox.insert(entry.message_id().clone(), entry);
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
}
