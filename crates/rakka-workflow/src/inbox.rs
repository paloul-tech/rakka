//! Durable inbox runtime.

use rakka_persistence::{DurableStateStore, PersistenceId, Revision, StateRecord};

use crate::clock::{SystemWorkflowClock, WorkflowClock};
use crate::error::{map_durable_error, WorkflowError, WorkflowResult};
use crate::{
    DeduplicationKey, InboxEntry, InboxStatus, WorkflowId, WorkflowMessageId, WorkflowState,
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
}
