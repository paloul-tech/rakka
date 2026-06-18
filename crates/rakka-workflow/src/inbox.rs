//! Durable inbox runtime.

use std::future::Future;
use std::pin::Pin;

use rakka_persistence::{DurableStateStore, PersistenceId, Revision, StateRecord};

use crate::clock::{SystemWorkflowClock, WorkflowClock};
use crate::error::{map_durable_error, WorkflowError, WorkflowResult};
use crate::{
    DeduplicationKey, InboxEntry, InboxStatus, OutboxEntry, OutboxFailureTransition,
    OutboxMessageId, OutboxStatus, OutboxTarget, RetryPolicy, WorkflowId, WorkflowMessageId,
    WorkflowState, WorkflowTelemetryEvent, WorkflowTimestamp,
};

/// Command accepted into a durable inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxCommand {
    message_id: WorkflowMessageId,
    deduplication_key: Option<DeduplicationKey>,
    message_type: String,
    payload: Vec<u8>,
}

impl InboxCommand {
    /// Creates an inbox command.
    #[must_use]
    pub fn new(
        message_id: impl Into<String>,
        message_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            message_id: WorkflowMessageId::new(message_id),
            deduplication_key: None,
            message_type: message_type.into(),
            payload: payload.into(),
        }
    }

    /// Sets an idempotency key for deduplication.
    #[must_use]
    pub fn deduplication_key(mut self, key: impl Into<String>) -> Self {
        self.deduplication_key = Some(DeduplicationKey::new(key));
        self
    }

    /// Message id.
    #[must_use]
    pub const fn message_id(&self) -> &WorkflowMessageId {
        &self.message_id
    }

    /// Deduplication key, when supplied.
    #[must_use]
    pub const fn deduplication_key_ref(&self) -> Option<&DeduplicationKey> {
        self.deduplication_key.as_ref()
    }

    /// Message type label.
    #[must_use]
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Opaque payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Result of accepting an inbox command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxAcceptance {
    /// A new durable inbox entry was persisted.
    Accepted {
        /// Persisted inbox entry.
        entry: InboxEntry,
        /// Store revision after persistence.
        revision: Revision,
    },
    /// A duplicate command was detected and no new entry was persisted.
    Duplicate {
        /// Existing inbox entry matching the message id or deduplication key.
        entry: InboxEntry,
        /// Current recovered revision.
        revision: Revision,
    },
}

impl InboxAcceptance {
    /// Returns true when this command created new durable work.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    /// Inbox entry associated with this acceptance result.
    #[must_use]
    pub const fn entry(&self) -> &InboxEntry {
        match self {
            Self::Accepted { entry, .. } | Self::Duplicate { entry, .. } => entry,
        }
    }

    /// Recovered or persisted revision associated with this result.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        match self {
            Self::Accepted { revision, .. } | Self::Duplicate { revision, .. } => *revision,
        }
    }
}

/// Command scheduled into a durable outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxCommand {
    message_id: OutboxMessageId,
    deduplication_key: Option<DeduplicationKey>,
    target: OutboxTarget,
    message_type: String,
    payload: Vec<u8>,
    scheduled_at: Option<WorkflowTimestamp>,
    retry_policy: RetryPolicy,
}

impl OutboxCommand {
    /// Creates an outbox command.
    #[must_use]
    pub fn new(
        message_id: impl Into<String>,
        target: OutboxTarget,
        message_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            message_id: OutboxMessageId::new(message_id),
            deduplication_key: None,
            target,
            message_type: message_type.into(),
            payload: payload.into(),
            scheduled_at: None,
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Sets an idempotency key for outbox deduplication.
    #[must_use]
    pub fn deduplication_key(mut self, key: impl Into<String>) -> Self {
        self.deduplication_key = Some(DeduplicationKey::new(key));
        self
    }

    /// Sets the retry policy.
    #[must_use]
    pub const fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Sets the first dispatch timestamp.
    #[must_use]
    pub const fn scheduled_at(mut self, scheduled_at: WorkflowTimestamp) -> Self {
        self.scheduled_at = Some(scheduled_at);
        self
    }

    /// Message id.
    #[must_use]
    pub const fn message_id(&self) -> &OutboxMessageId {
        &self.message_id
    }

    /// Deduplication key, when supplied.
    #[must_use]
    pub const fn deduplication_key_ref(&self) -> Option<&DeduplicationKey> {
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

    /// Opaque payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// First dispatch timestamp, when explicitly supplied.
    #[must_use]
    pub const fn scheduled_at_value(&self) -> Option<WorkflowTimestamp> {
        self.scheduled_at
    }

    /// Retry policy.
    #[must_use]
    pub const fn retry_policy_value(&self) -> RetryPolicy {
        self.retry_policy
    }
}

/// Result of scheduling an outbox command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxAcceptance {
    /// A new durable outbox entry was persisted.
    Scheduled {
        /// Persisted outbox entry.
        entry: OutboxEntry,
        /// Store revision after persistence.
        revision: Revision,
    },
    /// A duplicate command was detected and no new entry was persisted.
    Duplicate {
        /// Existing outbox entry matching the message id or deduplication key.
        entry: OutboxEntry,
        /// Current recovered revision.
        revision: Revision,
    },
}

impl OutboxAcceptance {
    /// Returns true when this command created new durable work.
    #[must_use]
    pub const fn is_scheduled(&self) -> bool {
        matches!(self, Self::Scheduled { .. })
    }

    /// Outbox entry associated with this result.
    #[must_use]
    pub const fn entry(&self) -> &OutboxEntry {
        match self {
            Self::Scheduled { entry, .. } | Self::Duplicate { entry, .. } => entry,
        }
    }

    /// Recovered or persisted revision associated with this result.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        match self {
            Self::Scheduled { revision, .. } | Self::Duplicate { revision, .. } => *revision,
        }
    }
}

/// Result returned by an application-defined outbox dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxDispatchResult {
    /// Dispatch succeeded.
    Success,
    /// Dispatch failed and may be retried according to policy.
    Failure {
        /// Failure detail.
        message: String,
    },
    /// Dispatch timed out and may be retried according to policy.
    Timeout {
        /// Timeout detail.
        message: String,
    },
}

impl OutboxDispatchResult {
    /// Creates a failure dispatch result.
    #[must_use]
    pub fn failure(message: impl Into<String>) -> Self {
        Self::Failure {
            message: message.into(),
        }
    }

    /// Creates a timeout dispatch result.
    #[must_use]
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }
}

/// Boxed future returned by outbox dispatchers.
pub type OutboxDispatchFuture<'a> = Pin<Box<dyn Future<Output = OutboxDispatchResult> + Send + 'a>>;

/// Application-supplied outbox dispatcher.
pub trait OutboxDispatcher: Send {
    /// Dispatches one durable outbox entry.
    fn dispatch<'a>(&'a mut self, entry: &'a OutboxEntry) -> OutboxDispatchFuture<'a>;
}

/// Durable workflow inbox backed by a latest-state store.
pub struct DurableInbox<Store, Clock = SystemWorkflowClock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    workflow_id: WorkflowId,
    persistence_id: PersistenceId,
    store: Store,
    clock: Clock,
    record: Option<StateRecord<WorkflowState>>,
}

impl<Store> DurableInbox<Store, SystemWorkflowClock>
where
    Store: DurableStateStore<WorkflowState>,
{
    /// Creates a durable inbox using the system clock.
    #[must_use]
    pub fn new(workflow_id: WorkflowId, store: Store) -> Self {
        Self::with_clock(workflow_id, store, SystemWorkflowClock)
    }
}

impl<Store, Clock> DurableInbox<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates a durable inbox with an explicit clock.
    #[must_use]
    pub fn with_clock(workflow_id: WorkflowId, store: Store, clock: Clock) -> Self {
        let persistence_id = workflow_id.persistence_id();
        Self {
            workflow_id,
            persistence_id,
            store,
            clock,
            record: None,
        }
    }

    /// Workflow id.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Persistence id used by this inbox.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Current recovered store revision.
    pub fn revision(&self) -> WorkflowResult<Revision> {
        self.record()
            .map(|record| record.revision)
            .ok_or_else(|| WorkflowError::NotRecovered {
                workflow_id: self.workflow_id.clone(),
            })
    }

    /// Current recovered state.
    pub fn state(&self) -> WorkflowResult<&WorkflowState> {
        self.record()
            .map(|record| &record.state)
            .ok_or_else(|| WorkflowError::NotRecovered {
                workflow_id: self.workflow_id.clone(),
            })
    }

    /// Recovers the latest workflow snapshot.
    pub async fn recover(&mut self) -> WorkflowResult<&WorkflowState> {
        let loaded = self
            .store
            .load(&self.persistence_id)
            .await
            .map_err(|error| map_durable_error(&self.workflow_id, error))?;
        let record = loaded.unwrap_or_else(|| {
            StateRecord::missing(WorkflowState::empty(
                self.workflow_id.clone(),
                self.clock.now(),
            ))
        });
        self.record = Some(record);
        Ok(&self.record.as_ref().expect("record just recovered").state)
    }

    /// Accepts a command into the durable inbox after persistence succeeds.
    pub async fn accept(&mut self, command: InboxCommand) -> WorkflowResult<InboxAcceptance> {
        let record = self.recovered_record()?;
        if let Some(entry) = record.state.inbox_entry(command.message_id()).cloned() {
            return Ok(InboxAcceptance::Duplicate {
                entry,
                revision: record.revision,
            });
        }
        if let Some(key) = command.deduplication_key_ref() {
            if let Some(entry) = record.state.inbox_entry_by_deduplication_key(key).cloned() {
                return Ok(InboxAcceptance::Duplicate {
                    entry,
                    revision: record.revision,
                });
            }
        }

        let now = self.clock.now();
        let entry = InboxEntry::new(
            command.message_id,
            command.deduplication_key,
            command.message_type,
            command.payload,
            now,
        );
        let mut next_state = record.state.clone();
        next_state.insert_inbox(entry.clone());
        let next_record = self
            .store
            .compare_and_set(&self.persistence_id, record.revision, next_state)
            .await
            .map_err(|error| map_durable_error(&self.workflow_id, error))?;
        self.record = Some(next_record.clone());
        Ok(InboxAcceptance::Accepted {
            entry,
            revision: next_record.revision,
        })
    }

    /// Persists an inbox status transition.
    pub async fn transition_inbox(
        &mut self,
        message_id: &WorkflowMessageId,
        status: InboxStatus,
    ) -> WorkflowResult<InboxEntry> {
        let record = self.recovered_record()?;
        let mut next_state = record.state.clone();
        let entry = next_state
            .update_inbox_status(message_id, status, self.clock.now())
            .ok_or_else(|| WorkflowError::InboxEntryNotFound {
                workflow_id: self.workflow_id.clone(),
                message_id: message_id.clone(),
            })?;
        let next_record = self
            .store
            .compare_and_set(&self.persistence_id, record.revision, next_state)
            .await
            .map_err(|error| map_durable_error(&self.workflow_id, error))?;
        self.record = Some(next_record);
        Ok(entry)
    }

    /// Schedules a command into the durable outbox after persistence succeeds.
    pub async fn schedule_outbox(
        &mut self,
        command: OutboxCommand,
    ) -> WorkflowResult<OutboxAcceptance> {
        let record = self.recovered_record()?;
        if let Some(entry) = record.state.outbox_entry(command.message_id()).cloned() {
            return Ok(OutboxAcceptance::Duplicate {
                entry,
                revision: record.revision,
            });
        }
        if let Some(key) = command.deduplication_key_ref() {
            if let Some(entry) = record.state.outbox_entry_by_deduplication_key(key).cloned() {
                return Ok(OutboxAcceptance::Duplicate {
                    entry,
                    revision: record.revision,
                });
            }
        }

        let now = self.clock.now();
        let scheduled_at = command.scheduled_at.unwrap_or(now);
        let entry = OutboxEntry::new(
            command.message_id,
            command.deduplication_key,
            command.target,
            command.message_type,
            command.payload,
            scheduled_at,
            command.retry_policy,
        );
        let mut next_state = record.state.clone();
        next_state.insert_outbox(entry.clone());
        let next_record = self
            .store
            .compare_and_set(&self.persistence_id, record.revision, next_state)
            .await
            .map_err(|error| map_durable_error(&self.workflow_id, error))?;
        self.record = Some(next_record.clone());
        Ok(OutboxAcceptance::Scheduled {
            entry,
            revision: next_record.revision,
        })
    }

    /// Returns recoverable inbox work from the current snapshot.
    pub fn recoverable_inbox(&self) -> WorkflowResult<Vec<InboxEntry>> {
        Ok(self.state()?.recoverable_inbox())
    }

    /// Returns due outbox work from the current snapshot.
    pub fn due_outbox(&self) -> WorkflowResult<Vec<OutboxEntry>> {
        Ok(self.state()?.due_outbox(self.clock.now()))
    }

    /// Marks one outbox entry as dispatching before an external side effect starts.
    pub async fn mark_outbox_dispatching(
        &mut self,
        message_id: &OutboxMessageId,
    ) -> WorkflowResult<OutboxEntry> {
        let record = self.recovered_record()?;
        let mut next_state = record.state.clone();
        let entry = next_state
            .update_outbox(message_id, |entry| {
                entry.set_status(OutboxStatus::Dispatching, self.clock.now());
            })
            .ok_or_else(|| WorkflowError::OutboxEntryNotFound {
                workflow_id: self.workflow_id.clone(),
                message_id: message_id.clone(),
            })?;
        self.persist_state(record.revision, next_state).await?;
        Ok(entry)
    }

    /// Records a successful outbox dispatch.
    pub async fn record_outbox_success(
        &mut self,
        message_id: &OutboxMessageId,
    ) -> WorkflowResult<WorkflowTelemetryEvent> {
        let now = self.clock.now();
        let record = self.recovered_record()?;
        let mut next_state = record.state.clone();
        let entry = next_state
            .update_outbox(message_id, |entry| entry.mark_dispatched(now))
            .ok_or_else(|| WorkflowError::OutboxEntryNotFound {
                workflow_id: self.workflow_id.clone(),
                message_id: message_id.clone(),
            })?;
        self.persist_state(record.revision, next_state).await?;
        Ok(WorkflowTelemetryEvent::OutboxDispatchSucceeded {
            message_id: entry.message_id().clone(),
            at: now,
        })
    }

    /// Records a failed outbox dispatch and either schedules retry or exhausts the entry.
    pub async fn record_outbox_failure(
        &mut self,
        message_id: &OutboxMessageId,
        message: impl Into<String>,
        timed_out: bool,
    ) -> WorkflowResult<WorkflowTelemetryEvent> {
        let now = self.clock.now();
        let message = message.into();
        let record = self.recovered_record()?;
        let mut next_state = record.state.clone();
        let mut transition = None;
        let entry = next_state
            .update_outbox(message_id, |entry| {
                transition = Some(entry.record_failure(now, message.clone()));
            })
            .ok_or_else(|| WorkflowError::OutboxEntryNotFound {
                workflow_id: self.workflow_id.clone(),
                message_id: message_id.clone(),
            })?;
        let transition = transition.expect("outbox update should record a failure transition");
        self.persist_state(record.revision, next_state).await?;

        match transition {
            OutboxFailureTransition::Retry { next_retry_at } if timed_out => {
                Ok(WorkflowTelemetryEvent::OutboxDispatchTimedOut {
                    message_id: entry.message_id().clone(),
                    attempt: entry.attempts().attempts(),
                    next_retry_at: Some(next_retry_at),
                    message,
                })
            }
            OutboxFailureTransition::Retry { next_retry_at } => {
                Ok(WorkflowTelemetryEvent::OutboxDispatchRetried {
                    message_id: entry.message_id().clone(),
                    attempt: entry.attempts().attempts(),
                    next_retry_at,
                    message,
                })
            }
            OutboxFailureTransition::Exhausted => {
                Ok(WorkflowTelemetryEvent::OutboxDispatchExhausted {
                    message_id: entry.message_id().clone(),
                    attempts: entry.attempts().attempts(),
                    message,
                })
            }
        }
    }

    /// Dispatches every currently due outbox entry through an application dispatcher.
    pub async fn dispatch_due_outbox<D>(
        &mut self,
        dispatcher: &mut D,
    ) -> WorkflowResult<Vec<WorkflowTelemetryEvent>>
    where
        D: OutboxDispatcher,
    {
        let due = self.due_outbox()?;
        let mut events = Vec::new();
        for entry in due {
            let dispatching = self.mark_outbox_dispatching(entry.message_id()).await?;
            match dispatcher.dispatch(&dispatching).await {
                OutboxDispatchResult::Success => {
                    events.push(self.record_outbox_success(entry.message_id()).await?);
                }
                OutboxDispatchResult::Failure { message } => {
                    events.push(
                        self.record_outbox_failure(entry.message_id(), message, false)
                            .await?,
                    );
                }
                OutboxDispatchResult::Timeout { message } => {
                    events.push(
                        self.record_outbox_failure(entry.message_id(), message, true)
                            .await?,
                    );
                }
            }
        }
        Ok(events)
    }

    fn record(&self) -> Option<&StateRecord<WorkflowState>> {
        self.record.as_ref()
    }

    fn recovered_record(&self) -> WorkflowResult<StateRecord<WorkflowState>> {
        self.record
            .clone()
            .ok_or_else(|| WorkflowError::NotRecovered {
                workflow_id: self.workflow_id.clone(),
            })
    }

    async fn persist_state(
        &mut self,
        expected_revision: Revision,
        next_state: WorkflowState,
    ) -> WorkflowResult<()> {
        let next_record = self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, next_state)
            .await
            .map_err(|error| map_durable_error(&self.workflow_id, error))?;
        self.record = Some(next_record);
        Ok(())
    }
}
