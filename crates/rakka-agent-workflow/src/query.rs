//! Operational query model for agent workflow runs.
//!
//! Durable run state remains the correctness boundary. This module defines
//! projection records and query traits that can be maintained beside durable
//! state so operators can list waiting, failed, running, due, and stuck runs
//! without scanning every persisted workflow record.
//!
//! The in-memory implementation is intended for tests and local fixtures. A
//! PostgreSQL implementation can store the same projection records with bounded
//! indexes in a later slice.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::{
    AgentDispatchEntry, AgentDispatchId, AgentDispatchStatus, AgentDispatchTargetClass,
    AgentDispatcherWorkerId, AgentEffectId, AgentEffectKind, AgentRunId, AgentRunState,
    AgentRunStatus, AgentStepId, AgentTenantId, AgentTimerEntry, AgentTimerId, AgentTimerStatus,
    AgentTimestampMillis, AgentWorkflowId, HumanCheckpointId, WorkflowDefinitionVersion,
};

/// Shared result type for workflow query indexes.
pub type AgentWorkflowQueryResult<T> = Result<T, AgentWorkflowQueryError>;

/// Boxed future returned by workflow query indexes.
pub type AgentWorkflowQueryFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentWorkflowQueryResult<T>> + Send + 'a>>;

/// Workflow query model errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentWorkflowQueryError {
    /// Query input was invalid.
    InvalidQuery {
        /// Query field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Backing index store failed.
    Store {
        /// Store error message.
        message: String,
    },
}

impl AgentWorkflowQueryError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidQuery { .. } => "invalid-workflow-query",
            Self::Store { .. } => "workflow-query-store",
        }
    }
}

impl Display for AgentWorkflowQueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidQuery { field, reason } => {
                write!(f, "invalid workflow query field {field}: {reason}")
            }
            Self::Store { message } => write!(f, "workflow query store failed: {message}"),
        }
    }
}

impl Error for AgentWorkflowQueryError {}

/// Bounded waiting reason used by workflow run indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRunQueryWaitingReason {
    /// Run is waiting for a durable timer.
    Timer,
    /// Run is waiting for a human checkpoint.
    Human,
    /// Run is waiting for an external effect result.
    Effect,
}

impl AgentRunQueryWaitingReason {
    /// Stable lowercase label for query APIs and diagnostics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Timer => "timer",
            Self::Human => "human",
            Self::Effect => "effect",
        }
    }

    fn from_status(status: AgentRunStatus) -> Option<Self> {
        match status {
            AgentRunStatus::WaitingForTimer => Some(Self::Timer),
            AgentRunStatus::WaitingForHuman => Some(Self::Human),
            AgentRunStatus::WaitingForEffect => Some(Self::Effect),
            AgentRunStatus::Accepted
            | AgentRunStatus::Running
            | AgentRunStatus::Cancelling
            | AgentRunStatus::Completed
            | AgentRunStatus::Failed
            | AgentRunStatus::Compensating
            | AgentRunStatus::Cancelled => None,
        }
    }
}

/// Shard ownership projection attached to an indexed run.
///
/// The fields are strings so the query model can support sharding lookups
/// without forcing the base agent workflow crate to enable the `sharding`
/// feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowShardOwnership {
    /// Sharded entity type, such as `AgentRun`.
    pub entity_type: String,
    /// Stable shard id as reported by the sharding subsystem.
    pub shard_id: String,
    /// Node that currently owns the shard.
    pub owner_node_id: String,
}

impl AgentWorkflowShardOwnership {
    /// Creates shard ownership metadata.
    #[must_use]
    pub fn new(
        entity_type: impl Into<String>,
        shard_id: impl Into<String>,
        owner_node_id: impl Into<String>,
    ) -> Self {
        Self {
            entity_type: entity_type.into(),
            shard_id: shard_id.into(),
            owner_node_id: owner_node_id.into(),
        }
    }
}

/// Operational projection for one durable agent workflow run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunIndexEntry {
    /// Stable run id.
    pub run_id: AgentRunId,
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Bounded workflow type label.
    pub workflow_type: String,
    /// Workflow definition version selected by the run.
    pub definition_version: WorkflowDefinitionVersion,
    /// Tenant that owns the run, when known.
    pub tenant: Option<AgentTenantId>,
    /// Application namespace or operational partition, when known.
    pub namespace: Option<String>,
    /// Current run status.
    pub status: AgentRunStatus,
    /// Waiting reason when the run is parked.
    pub waiting_reason: Option<AgentRunQueryWaitingReason>,
    /// Current step id.
    pub current_step_id: Option<AgentStepId>,
    /// Failed step id when known.
    pub failed_step_id: Option<AgentStepId>,
    /// Open human checkpoint id when the run is waiting for human input.
    pub pending_human_checkpoint: Option<HumanCheckpointId>,
    /// Oldest open checkpoint creation timestamp.
    pub open_checkpoint_created_at: Option<AgentTimestampMillis>,
    /// Oldest open checkpoint due timestamp.
    pub open_checkpoint_due_at: Option<AgentTimestampMillis>,
    /// Shard ownership metadata, when the run is sharded.
    pub shard_ownership: Option<AgentWorkflowShardOwnership>,
    /// Run creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
    /// Terminal timestamp for completed, failed, or cancelled runs.
    pub completed_at: Option<AgentTimestampMillis>,
}

impl AgentRunIndexEntry {
    /// Builds an operational run projection from durable run state.
    #[must_use]
    pub fn from_run_state(run: &AgentRunState, workflow_type: impl Into<String>) -> Self {
        let oldest_open_checkpoint = run
            .checkpoints
            .iter()
            .filter(|checkpoint| !checkpoint.status.is_terminal())
            .min_by_key(|checkpoint| checkpoint.created_at);
        Self {
            run_id: run.run_id.clone(),
            workflow_id: run.workflow_id.clone(),
            workflow_type: workflow_type.into(),
            definition_version: run.definition_version.clone(),
            tenant: run.tenant.clone(),
            namespace: None,
            status: run.status,
            waiting_reason: AgentRunQueryWaitingReason::from_status(run.status),
            current_step_id: run.current_step_id.clone(),
            failed_step_id: (run.status == AgentRunStatus::Failed)
                .then(|| run.current_step_id.clone())
                .flatten(),
            pending_human_checkpoint: run.pending_human_checkpoint.clone(),
            open_checkpoint_created_at: oldest_open_checkpoint
                .map(|checkpoint| checkpoint.created_at),
            open_checkpoint_due_at: oldest_open_checkpoint.and_then(|checkpoint| checkpoint.due_at),
            shard_ownership: None,
            created_at: run.created_at,
            updated_at: run.updated_at,
            completed_at: run.completed_at,
        }
    }

    /// Sets an explicit namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sets shard ownership metadata.
    #[must_use]
    pub fn shard_ownership(mut self, ownership: AgentWorkflowShardOwnership) -> Self {
        self.shard_ownership = Some(ownership);
        self
    }

    /// Overrides the failed step id.
    #[must_use]
    pub fn failed_step_id(mut self, failed_step_id: AgentStepId) -> Self {
        self.failed_step_id = Some(failed_step_id);
        self
    }
}

/// Operational projection for one durable timer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTimerIndexEntry {
    /// Stable timer id.
    pub timer_id: AgentTimerId,
    /// Workflow definition id the timer targets.
    pub workflow_id: AgentWorkflowId,
    /// Run id the timer targets.
    pub run_id: AgentRunId,
    /// Tenant that owns the timer.
    pub tenant: AgentTenantId,
    /// Application namespace or operational partition, when known.
    pub namespace: Option<String>,
    /// Timer due timestamp.
    pub due_at: AgentTimestampMillis,
    /// Timer status.
    pub status: AgentTimerStatus,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
}

impl AgentTimerIndexEntry {
    /// Builds a timer projection from a durable timer entry.
    #[must_use]
    pub fn from_timer_entry(timer: &AgentTimerEntry) -> Self {
        Self {
            timer_id: timer.timer_id.clone(),
            workflow_id: timer.workflow_id.clone(),
            run_id: timer.run_id.clone(),
            tenant: timer.tenant.clone(),
            namespace: None,
            due_at: timer.due_at,
            status: timer.status,
            updated_at: timer.updated_at,
        }
    }

    /// Sets an explicit namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Returns true when this timer is pending and due at `now`.
    #[must_use]
    pub fn is_due_at(&self, now: AgentTimestampMillis) -> bool {
        self.status == AgentTimerStatus::Pending && self.due_at.as_millis() <= now.as_millis()
    }
}

/// Operational projection for one dispatcher fleet entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDispatchIndexEntry {
    /// Stable dispatch id.
    pub dispatch_id: AgentDispatchId,
    /// Workflow definition id when known.
    pub workflow_id: Option<AgentWorkflowId>,
    /// Run that owns the dispatch.
    pub run_id: AgentRunId,
    /// Effect id being dispatched.
    pub effect_id: AgentEffectId,
    /// Effect kind.
    pub effect_kind: AgentEffectKind,
    /// Target class used by dispatcher concurrency limits.
    pub target_class: AgentDispatchTargetClass,
    /// Dispatch due timestamp.
    pub due_at: AgentTimestampMillis,
    /// Dispatcher lifecycle status.
    pub status: AgentDispatchStatus,
    /// Worker holding the lease, when claimed.
    pub worker_id: Option<AgentDispatcherWorkerId>,
    /// Current dispatcher fencing token, when known.
    pub fencing_token: Option<u64>,
    /// Claim timestamp, when claimed.
    pub claimed_at: Option<AgentTimestampMillis>,
    /// Lease expiration timestamp, when claimed.
    pub lease_expires_at: Option<AgentTimestampMillis>,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
}

impl AgentDispatchIndexEntry {
    /// Builds a dispatch projection from a dispatcher fleet entry.
    #[must_use]
    pub fn from_dispatch_entry(entry: &AgentDispatchEntry) -> Self {
        Self {
            dispatch_id: entry.dispatch_id.clone(),
            workflow_id: entry.workflow_id.clone(),
            run_id: entry.run_id.clone(),
            effect_id: entry.effect_id.clone(),
            effect_kind: entry.effect_kind,
            target_class: entry.target_class,
            due_at: entry.due_at,
            status: entry.status,
            worker_id: entry.lease.as_ref().map(|lease| lease.worker_id.clone()),
            fencing_token: Some(entry.last_fencing_token),
            claimed_at: entry.lease.as_ref().map(|lease| lease.claimed_at),
            lease_expires_at: entry.lease.as_ref().map(|lease| lease.lease_expires_at),
            updated_at: entry.updated_at,
        }
    }

    /// Returns true when this dispatch has an expired claim at `now`.
    #[must_use]
    pub fn is_stuck_at(&self, now: AgentTimestampMillis) -> bool {
        self.status == AgentDispatchStatus::Claimed
            && matches!(
                self.lease_expires_at,
                Some(expires_at) if expires_at.as_millis() <= now.as_millis()
            )
    }
}

/// Query dimensions for agent workflow runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentWorkflowRunQuery {
    pub(crate) tenant: Option<AgentTenantId>,
    pub(crate) namespace: Option<String>,
    pub(crate) workflow_type: Option<String>,
    pub(crate) definition_version: Option<WorkflowDefinitionVersion>,
    pub(crate) statuses: Vec<AgentRunStatus>,
    pub(crate) updated_at_from: Option<AgentTimestampMillis>,
    pub(crate) updated_at_to: Option<AgentTimestampMillis>,
    pub(crate) waiting_reasons: Vec<AgentRunQueryWaitingReason>,
    pub(crate) checkpoint_created_at_or_before: Option<AgentTimestampMillis>,
    pub(crate) failed_step_id: Option<AgentStepId>,
    pub(crate) due_timer_at_or_before: Option<AgentTimestampMillis>,
    pub(crate) stuck_dispatcher_at_or_before: Option<AgentTimestampMillis>,
    pub(crate) shard_owner_node_id: Option<String>,
    pub(crate) shard_id: Option<String>,
    pub(crate) limit: Option<usize>,
}

impl AgentWorkflowRunQuery {
    /// Creates an empty run query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by tenant.
    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<AgentTenantId>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Filters by namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Filters by workflow type.
    #[must_use]
    pub fn workflow_type(mut self, workflow_type: impl Into<String>) -> Self {
        self.workflow_type = Some(workflow_type.into());
        self
    }

    /// Filters by workflow definition version.
    #[must_use]
    pub fn definition_version(mut self, version: impl Into<WorkflowDefinitionVersion>) -> Self {
        self.definition_version = Some(version.into());
        self
    }

    /// Adds one status filter.
    #[must_use]
    pub fn status(mut self, status: AgentRunStatus) -> Self {
        push_unique(&mut self.statuses, status);
        self
    }

    /// Filters to all parked waiting statuses.
    #[must_use]
    pub fn waiting(mut self) -> Self {
        push_unique(&mut self.waiting_reasons, AgentRunQueryWaitingReason::Timer);
        push_unique(&mut self.waiting_reasons, AgentRunQueryWaitingReason::Human);
        push_unique(
            &mut self.waiting_reasons,
            AgentRunQueryWaitingReason::Effect,
        );
        self
    }

    /// Adds one waiting reason filter.
    #[must_use]
    pub fn waiting_reason(mut self, reason: AgentRunQueryWaitingReason) -> Self {
        push_unique(&mut self.waiting_reasons, reason);
        self
    }

    /// Filters to runs updated at or after the timestamp.
    #[must_use]
    pub const fn updated_at_from(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.updated_at_from = Some(timestamp);
        self
    }

    /// Filters to runs updated at or before the timestamp.
    #[must_use]
    pub const fn updated_at_to(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.updated_at_to = Some(timestamp);
        self
    }

    /// Filters to runs whose oldest open checkpoint was created by this timestamp.
    #[must_use]
    pub const fn checkpoint_created_at_or_before(
        mut self,
        timestamp: AgentTimestampMillis,
    ) -> Self {
        self.checkpoint_created_at_or_before = Some(timestamp);
        self
    }

    /// Filters to runs with the given failed step id.
    #[must_use]
    pub fn failed_step_id(mut self, step_id: impl Into<AgentStepId>) -> Self {
        self.failed_step_id = Some(step_id.into());
        self
    }

    /// Filters to runs with a pending timer due by the timestamp.
    #[must_use]
    pub const fn due_timer_at_or_before(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.due_timer_at_or_before = Some(timestamp);
        self
    }

    /// Filters to runs with a dispatcher claim expired by the timestamp.
    #[must_use]
    pub const fn stuck_dispatcher_at_or_before(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.stuck_dispatcher_at_or_before = Some(timestamp);
        self
    }

    /// Filters to runs owned by a shard owner node.
    #[must_use]
    pub fn shard_owner(mut self, owner_node_id: impl Into<String>) -> Self {
        self.shard_owner_node_id = Some(owner_node_id.into());
        self
    }

    /// Filters to runs in a specific shard.
    #[must_use]
    pub fn shard_id(mut self, shard_id: impl Into<String>) -> Self {
        self.shard_id = Some(shard_id.into());
        self
    }

    /// Limits the result count.
    #[must_use]
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Query dimensions for durable timers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentTimerQuery {
    pub(crate) run_id: Option<AgentRunId>,
    pub(crate) workflow_id: Option<AgentWorkflowId>,
    pub(crate) tenant: Option<AgentTenantId>,
    pub(crate) namespace: Option<String>,
    pub(crate) statuses: Vec<AgentTimerStatus>,
    pub(crate) due_at_or_before: Option<AgentTimestampMillis>,
    pub(crate) limit: Option<usize>,
}

impl AgentTimerQuery {
    /// Creates an empty timer query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by run id.
    #[must_use]
    pub fn run_id(mut self, run_id: impl Into<AgentRunId>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Filters by workflow id.
    #[must_use]
    pub fn workflow_id(mut self, workflow_id: impl Into<AgentWorkflowId>) -> Self {
        self.workflow_id = Some(workflow_id.into());
        self
    }

    /// Filters by tenant.
    #[must_use]
    pub fn tenant(mut self, tenant: impl Into<AgentTenantId>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Filters by namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Adds one timer status filter.
    #[must_use]
    pub fn status(mut self, status: AgentTimerStatus) -> Self {
        push_unique(&mut self.statuses, status);
        self
    }

    /// Filters to timers due at or before the timestamp.
    #[must_use]
    pub const fn due_at_or_before(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.due_at_or_before = Some(timestamp);
        self
    }

    /// Limits the result count.
    #[must_use]
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Query dimensions for dispatcher work.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentDispatchQuery {
    pub(crate) run_id: Option<AgentRunId>,
    pub(crate) workflow_id: Option<AgentWorkflowId>,
    pub(crate) statuses: Vec<AgentDispatchStatus>,
    pub(crate) target_class: Option<AgentDispatchTargetClass>,
    pub(crate) due_at_or_before: Option<AgentTimestampMillis>,
    pub(crate) stuck_at_or_before: Option<AgentTimestampMillis>,
    pub(crate) limit: Option<usize>,
}

impl AgentDispatchQuery {
    /// Creates an empty dispatch query.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by run id.
    #[must_use]
    pub fn run_id(mut self, run_id: impl Into<AgentRunId>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Filters by workflow id.
    #[must_use]
    pub fn workflow_id(mut self, workflow_id: impl Into<AgentWorkflowId>) -> Self {
        self.workflow_id = Some(workflow_id.into());
        self
    }

    /// Adds one dispatch status filter.
    #[must_use]
    pub fn status(mut self, status: AgentDispatchStatus) -> Self {
        push_unique(&mut self.statuses, status);
        self
    }

    /// Filters by dispatch target class.
    #[must_use]
    pub const fn target_class(mut self, target_class: AgentDispatchTargetClass) -> Self {
        self.target_class = Some(target_class);
        self
    }

    /// Filters to dispatch entries due at or before the timestamp.
    #[must_use]
    pub const fn due_at_or_before(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.due_at_or_before = Some(timestamp);
        self
    }

    /// Filters to dispatch entries with expired leases by the timestamp.
    #[must_use]
    pub const fn stuck_at_or_before(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.stuck_at_or_before = Some(timestamp);
        self
    }

    /// Limits the result count.
    #[must_use]
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Operational workflow query index.
pub trait AgentWorkflowQueryIndex {
    /// Inserts or replaces one run projection.
    fn upsert_run<'a>(&'a mut self, entry: AgentRunIndexEntry) -> AgentWorkflowQueryFuture<'a, ()>;

    /// Removes one run projection.
    fn remove_run<'a>(&'a mut self, run_id: AgentRunId) -> AgentWorkflowQueryFuture<'a, ()>;

    /// Inserts or replaces one timer projection.
    fn upsert_timer<'a>(
        &'a mut self,
        entry: AgentTimerIndexEntry,
    ) -> AgentWorkflowQueryFuture<'a, ()>;

    /// Removes one timer projection.
    fn remove_timer<'a>(&'a mut self, timer_id: AgentTimerId) -> AgentWorkflowQueryFuture<'a, ()>;

    /// Inserts or replaces one dispatcher projection.
    fn upsert_dispatch<'a>(
        &'a mut self,
        entry: AgentDispatchIndexEntry,
    ) -> AgentWorkflowQueryFuture<'a, ()>;

    /// Removes one dispatcher projection.
    fn remove_dispatch<'a>(
        &'a mut self,
        dispatch_id: AgentDispatchId,
    ) -> AgentWorkflowQueryFuture<'a, ()>;

    /// Queries indexed workflow runs.
    fn query_runs<'a>(
        &'a self,
        query: AgentWorkflowRunQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentRunIndexEntry>>;

    /// Queries indexed timers.
    fn query_timers<'a>(
        &'a self,
        query: AgentTimerQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentTimerIndexEntry>>;

    /// Queries indexed dispatcher work.
    fn query_dispatches<'a>(
        &'a self,
        query: AgentDispatchQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentDispatchIndexEntry>>;
}

/// In-memory operational query index for tests and local fixtures.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentWorkflowQueryIndex {
    runs: BTreeMap<AgentRunId, AgentRunIndexEntry>,
    timers: BTreeMap<AgentTimerId, AgentTimerIndexEntry>,
    dispatches: BTreeMap<AgentDispatchId, AgentDispatchIndexEntry>,
}

impl InMemoryAgentWorkflowQueryIndex {
    /// Creates an empty in-memory query index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns indexed run projections.
    #[must_use]
    pub const fn runs(&self) -> &BTreeMap<AgentRunId, AgentRunIndexEntry> {
        &self.runs
    }

    /// Returns indexed timer projections.
    #[must_use]
    pub const fn timers(&self) -> &BTreeMap<AgentTimerId, AgentTimerIndexEntry> {
        &self.timers
    }

    /// Returns indexed dispatcher projections.
    #[must_use]
    pub const fn dispatches(&self) -> &BTreeMap<AgentDispatchId, AgentDispatchIndexEntry> {
        &self.dispatches
    }
}

impl AgentWorkflowQueryIndex for InMemoryAgentWorkflowQueryIndex {
    fn upsert_run<'a>(&'a mut self, entry: AgentRunIndexEntry) -> AgentWorkflowQueryFuture<'a, ()> {
        Box::pin(async move {
            self.runs.insert(entry.run_id.clone(), entry);
            Ok(())
        })
    }

    fn remove_run<'a>(&'a mut self, run_id: AgentRunId) -> AgentWorkflowQueryFuture<'a, ()> {
        Box::pin(async move {
            self.runs.remove(&run_id);
            Ok(())
        })
    }

    fn upsert_timer<'a>(
        &'a mut self,
        entry: AgentTimerIndexEntry,
    ) -> AgentWorkflowQueryFuture<'a, ()> {
        Box::pin(async move {
            self.timers.insert(entry.timer_id.clone(), entry);
            Ok(())
        })
    }

    fn remove_timer<'a>(&'a mut self, timer_id: AgentTimerId) -> AgentWorkflowQueryFuture<'a, ()> {
        Box::pin(async move {
            self.timers.remove(&timer_id);
            Ok(())
        })
    }

    fn upsert_dispatch<'a>(
        &'a mut self,
        entry: AgentDispatchIndexEntry,
    ) -> AgentWorkflowQueryFuture<'a, ()> {
        Box::pin(async move {
            self.dispatches.insert(entry.dispatch_id.clone(), entry);
            Ok(())
        })
    }

    fn remove_dispatch<'a>(
        &'a mut self,
        dispatch_id: AgentDispatchId,
    ) -> AgentWorkflowQueryFuture<'a, ()> {
        Box::pin(async move {
            self.dispatches.remove(&dispatch_id);
            Ok(())
        })
    }

    fn query_runs<'a>(
        &'a self,
        query: AgentWorkflowRunQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentRunIndexEntry>> {
        Box::pin(async move {
            validate_run_query(&query)?;
            let mut entries = self
                .runs
                .values()
                .filter(|entry| run_matches_query(entry, &query, &self.timers, &self.dispatches))
                .cloned()
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| {
                left.updated_at
                    .cmp(&right.updated_at)
                    .then_with(|| left.run_id.cmp(&right.run_id))
            });
            apply_limit(&mut entries, query.limit);
            Ok(entries)
        })
    }

    fn query_timers<'a>(
        &'a self,
        query: AgentTimerQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentTimerIndexEntry>> {
        Box::pin(async move {
            let mut entries = self
                .timers
                .values()
                .filter(|entry| timer_matches_query(entry, &query))
                .cloned()
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| {
                left.due_at
                    .cmp(&right.due_at)
                    .then_with(|| left.timer_id.cmp(&right.timer_id))
            });
            apply_limit(&mut entries, query.limit);
            Ok(entries)
        })
    }

    fn query_dispatches<'a>(
        &'a self,
        query: AgentDispatchQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentDispatchIndexEntry>> {
        Box::pin(async move {
            let mut entries = self
                .dispatches
                .values()
                .filter(|entry| dispatch_matches_query(entry, &query))
                .cloned()
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| {
                left.due_at
                    .cmp(&right.due_at)
                    .then_with(|| left.dispatch_id.cmp(&right.dispatch_id))
            });
            apply_limit(&mut entries, query.limit);
            Ok(entries)
        })
    }
}

fn validate_run_query(query: &AgentWorkflowRunQuery) -> AgentWorkflowQueryResult<()> {
    if matches!(query.limit, Some(0)) {
        return Err(AgentWorkflowQueryError::InvalidQuery {
            field: "limit",
            reason: "must be greater than zero",
        });
    }
    Ok(())
}

fn run_matches_query(
    entry: &AgentRunIndexEntry,
    query: &AgentWorkflowRunQuery,
    timers: &BTreeMap<AgentTimerId, AgentTimerIndexEntry>,
    dispatches: &BTreeMap<AgentDispatchId, AgentDispatchIndexEntry>,
) -> bool {
    if query
        .tenant
        .as_ref()
        .is_some_and(|tenant| entry.tenant.as_ref() != Some(tenant))
    {
        return false;
    }
    if query
        .namespace
        .as_ref()
        .is_some_and(|namespace| entry.namespace.as_ref() != Some(namespace))
    {
        return false;
    }
    if query
        .workflow_type
        .as_ref()
        .is_some_and(|workflow_type| &entry.workflow_type != workflow_type)
    {
        return false;
    }
    if query
        .definition_version
        .as_ref()
        .is_some_and(|version| &entry.definition_version != version)
    {
        return false;
    }
    if !query.statuses.is_empty() && !query.statuses.contains(&entry.status) {
        return false;
    }
    if query
        .updated_at_from
        .is_some_and(|timestamp| entry.updated_at < timestamp)
    {
        return false;
    }
    if query
        .updated_at_to
        .is_some_and(|timestamp| entry.updated_at > timestamp)
    {
        return false;
    }
    if !query.waiting_reasons.is_empty()
        && !entry
            .waiting_reason
            .is_some_and(|reason| query.waiting_reasons.contains(&reason))
    {
        return false;
    }
    if query
        .checkpoint_created_at_or_before
        .is_some_and(|timestamp| {
            entry
                .open_checkpoint_created_at
                .map_or(true, |created_at| created_at > timestamp)
        })
    {
        return false;
    }
    if query
        .failed_step_id
        .as_ref()
        .is_some_and(|step_id| entry.failed_step_id.as_ref() != Some(step_id))
    {
        return false;
    }
    if query
        .shard_owner_node_id
        .as_ref()
        .is_some_and(|owner_node_id| {
            entry
                .shard_ownership
                .as_ref()
                .map_or(true, |ownership| &ownership.owner_node_id != owner_node_id)
        })
    {
        return false;
    }
    if query.shard_id.as_ref().is_some_and(|shard_id| {
        entry
            .shard_ownership
            .as_ref()
            .map_or(true, |ownership| &ownership.shard_id != shard_id)
    }) {
        return false;
    }
    if query
        .due_timer_at_or_before
        .is_some_and(|timestamp| !run_has_due_timer(&entry.run_id, timestamp, timers))
    {
        return false;
    }
    if query
        .stuck_dispatcher_at_or_before
        .is_some_and(|timestamp| !run_has_stuck_dispatch(&entry.run_id, timestamp, dispatches))
    {
        return false;
    }
    true
}

fn timer_matches_query(entry: &AgentTimerIndexEntry, query: &AgentTimerQuery) -> bool {
    if query
        .run_id
        .as_ref()
        .is_some_and(|run_id| &entry.run_id != run_id)
    {
        return false;
    }
    if query
        .workflow_id
        .as_ref()
        .is_some_and(|workflow_id| &entry.workflow_id != workflow_id)
    {
        return false;
    }
    if query
        .tenant
        .as_ref()
        .is_some_and(|tenant| &entry.tenant != tenant)
    {
        return false;
    }
    if query
        .namespace
        .as_ref()
        .is_some_and(|namespace| entry.namespace.as_ref() != Some(namespace))
    {
        return false;
    }
    if !query.statuses.is_empty() && !query.statuses.contains(&entry.status) {
        return false;
    }
    if query
        .due_at_or_before
        .is_some_and(|timestamp| entry.due_at > timestamp)
    {
        return false;
    }
    true
}

fn dispatch_matches_query(entry: &AgentDispatchIndexEntry, query: &AgentDispatchQuery) -> bool {
    if query
        .run_id
        .as_ref()
        .is_some_and(|run_id| &entry.run_id != run_id)
    {
        return false;
    }
    if query
        .workflow_id
        .as_ref()
        .is_some_and(|workflow_id| entry.workflow_id.as_ref() != Some(workflow_id))
    {
        return false;
    }
    if !query.statuses.is_empty() && !query.statuses.contains(&entry.status) {
        return false;
    }
    if query
        .target_class
        .is_some_and(|target_class| entry.target_class != target_class)
    {
        return false;
    }
    if query
        .due_at_or_before
        .is_some_and(|timestamp| entry.due_at > timestamp)
    {
        return false;
    }
    if query
        .stuck_at_or_before
        .is_some_and(|timestamp| !entry.is_stuck_at(timestamp))
    {
        return false;
    }
    true
}

fn run_has_due_timer(
    run_id: &AgentRunId,
    timestamp: AgentTimestampMillis,
    timers: &BTreeMap<AgentTimerId, AgentTimerIndexEntry>,
) -> bool {
    timers
        .values()
        .any(|timer| &timer.run_id == run_id && timer.is_due_at(timestamp))
}

fn run_has_stuck_dispatch(
    run_id: &AgentRunId,
    timestamp: AgentTimestampMillis,
    dispatches: &BTreeMap<AgentDispatchId, AgentDispatchIndexEntry>,
) -> bool {
    dispatches
        .values()
        .any(|dispatch| &dispatch.run_id == run_id && dispatch.is_stuck_at(timestamp))
}

fn apply_limit<T>(entries: &mut Vec<T>, limit: Option<usize>) {
    if let Some(limit) = limit {
        entries.truncate(limit);
    }
}

fn push_unique<T>(values: &mut Vec<T>, value: T)
where
    T: PartialEq,
{
    if !values.contains(&value) {
        values.push(value);
    }
}
