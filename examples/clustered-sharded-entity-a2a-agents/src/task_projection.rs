//! A2A task read model and public task-event projection.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use a2a::{
    Artifact, ListTasksRequest, ListTasksResponse, Message, Part, Task, TaskId, TaskState,
    TaskStatus,
};
use chrono::{DateTime, Utc};
use rakka::agent_workflow::{
    AgentRunState, AgentRunStatus, AgentRuntimeEvent, AgentRuntimeEventKind, AgentTenantId,
    AgentTimestampMillis, ArtifactRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::a2a_mapping::DEFAULT_TENANT;

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 100;
const DEFAULT_HISTORY_LIMIT: usize = 20;
const DEFAULT_ARTIFACT_LIMIT: usize = 20;

/// Shared result type for task projections.
pub type TaskProjectionResult<T> = Result<T, TaskProjectionError>;

/// Stable projection/query failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskProjectionError {
    /// Query requires a tenant filter.
    TenantRequired,
    /// Page token is malformed.
    InvalidPageToken {
        /// Token supplied by the client.
        token: String,
    },
    /// Replay cursor is malformed or refers to a different task.
    InvalidReplayCursor {
        /// Cursor supplied by the client.
        cursor: String,
    },
    /// Task was not found.
    TaskNotFound {
        /// Missing task id.
        task_id: TaskId,
    },
    /// Events were replayed out of order.
    EventOrder {
        /// Expected next sequence.
        expected: u64,
        /// Actual sequence.
        actual: u64,
    },
}

impl TaskProjectionError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::TenantRequired => "tenant-required",
            Self::InvalidPageToken { .. } => "invalid-page-token",
            Self::InvalidReplayCursor { .. } => "invalid-replay-cursor",
            Self::TaskNotFound { .. } => "task-not-found",
            Self::EventOrder { .. } => "event-order",
        }
    }
}

impl Display for TaskProjectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TenantRequired => f.write_str("tenant filter is required"),
            Self::InvalidPageToken { token } => write!(f, "invalid page token `{token}`"),
            Self::InvalidReplayCursor { cursor } => {
                write!(f, "invalid replay cursor `{cursor}`")
            }
            Self::TaskNotFound { task_id } => write!(f, "task not found: {task_id}"),
            Self::EventOrder { expected, actual } => {
                write!(
                    f,
                    "event sequence {actual} does not follow expected {expected}"
                )
            }
        }
    }
}

impl Error for TaskProjectionError {}

/// Durable A2A task projection record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A2ATaskProjection {
    /// A2A task id, equal to Rakka run id.
    pub task_id: TaskId,
    /// A2A context id.
    pub context_id: String,
    /// Tenant that owns the task.
    pub tenant: String,
    /// Workflow id used by the underlying run.
    pub workflow_id: String,
    /// Public task state.
    pub status: TaskState,
    /// Status timestamp.
    pub status_timestamp: AgentTimestampMillis,
    /// Bounded public message history.
    pub history: Vec<Message>,
    /// Bounded public artifacts.
    pub artifacts: Vec<Artifact>,
    /// Low-cardinality metadata and reference fields.
    pub metadata: HashMap<String, Value>,
    /// Projection revision.
    pub projection_revision: u64,
}

impl A2ATaskProjection {
    /// Creates a projection from durable Rakka run state and bounded public data.
    pub fn from_run_state(
        run_state: &AgentRunState,
        context_id: impl Into<String>,
        history: Vec<Message>,
        artifact_refs: Vec<ArtifactRef>,
        projection_revision: u64,
    ) -> Self {
        let artifacts = bounded_tail(artifact_refs, DEFAULT_ARTIFACT_LIMIT)
            .iter()
            .map(a2a_artifact_from_ref)
            .collect::<Vec<_>>();
        Self {
            task_id: run_state.run_id.as_str().to_string(),
            context_id: context_id.into(),
            tenant: run_state
                .tenant
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| DEFAULT_TENANT.to_string()),
            workflow_id: run_state.workflow_id.as_str().to_string(),
            status: task_state_from_run_status(run_state.status),
            status_timestamp: run_state.updated_at,
            history: bounded_tail(history, DEFAULT_HISTORY_LIMIT),
            artifacts,
            metadata: projection_metadata(
                run_state,
                projection_revision,
                false,
                DEFAULT_HISTORY_LIMIT,
            ),
            projection_revision,
        }
    }

    /// Creates a projection from normalized command acceptance fields.
    #[must_use]
    pub fn accepted(
        task_id: impl Into<String>,
        context_id: impl Into<String>,
        tenant: impl Into<String>,
        workflow_id: impl Into<String>,
        timestamp: AgentTimestampMillis,
        history: Vec<Message>,
        projection_revision: u64,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            context_id: context_id.into(),
            tenant: tenant.into(),
            workflow_id: workflow_id.into(),
            status: TaskState::Submitted,
            status_timestamp: timestamp,
            history: bounded_tail(history, DEFAULT_HISTORY_LIMIT),
            artifacts: Vec::new(),
            metadata: HashMap::from([
                (
                    "io.rakka.projection.revision".to_string(),
                    Value::Number(projection_revision.into()),
                ),
                (
                    "io.rakka.status.source".to_string(),
                    Value::String("phase1-command-draft".to_string()),
                ),
            ]),
            projection_revision,
        }
    }

    /// Converts this projection to an A2A `Task` with bounded controls.
    #[must_use]
    pub fn to_task(&self, history_length: Option<i32>, include_artifacts: bool) -> Task {
        let history_limit = history_limit(history_length);
        let history = bounded_tail(self.history.clone(), history_limit);
        Task {
            id: self.task_id.clone(),
            context_id: self.context_id.clone(),
            status: TaskStatus {
                state: self.status.clone(),
                message: history.last().cloned(),
                timestamp: timestamp_to_datetime(self.status_timestamp),
            },
            artifacts: include_artifacts.then(|| self.artifacts.clone()),
            history: (!history.is_empty()).then_some(history),
            metadata: Some(self.metadata.clone()),
        }
    }

    /// Applies one ordered public task event.
    pub fn apply_event(&mut self, event: &A2ATaskEvent) -> TaskProjectionResult<()> {
        if event.sequence != self.projection_revision.saturating_add(1) {
            return Err(TaskProjectionError::EventOrder {
                expected: self.projection_revision.saturating_add(1),
                actual: event.sequence,
            });
        }
        self.status_timestamp = event.occurred_at;
        self.projection_revision = event.sequence;
        self.metadata.insert(
            "io.rakka.projection.revision".to_string(),
            Value::Number(event.sequence.into()),
        );
        match &event.payload {
            A2ATaskEventPayload::Snapshot(task) => {
                *self = adopted_snapshot(task, event);
            }
            A2ATaskEventPayload::StatusUpdate { state } => {
                self.status = state.clone();
            }
            A2ATaskEventPayload::ArtifactUpdate { artifact } => {
                self.artifacts.push(artifact.clone());
                self.artifacts = bounded_tail(self.artifacts.clone(), DEFAULT_ARTIFACT_LIMIT);
            }
            A2ATaskEventPayload::MessageUpdate { message } => {
                self.history.push(message.clone());
                self.history = bounded_tail(self.history.clone(), DEFAULT_HISTORY_LIMIT);
            }
            A2ATaskEventPayload::Terminal { state } => {
                self.status = state.clone();
            }
        }
        Ok(())
    }
}

/// Public task event kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ATaskEventKind {
    /// Full projection snapshot.
    Snapshot,
    /// Public task status update.
    StatusUpdate,
    /// Artifact projection update.
    ArtifactUpdate,
    /// Message history update.
    MessageUpdate,
    /// Terminal task state update.
    Terminal,
}

/// Public event payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ATaskEventPayload {
    /// Snapshot payload.
    Snapshot(A2ATaskProjection),
    /// Status update payload.
    StatusUpdate {
        /// New A2A state.
        state: TaskState,
    },
    /// Artifact update payload.
    ArtifactUpdate {
        /// Public artifact.
        artifact: Artifact,
    },
    /// Message update payload.
    MessageUpdate {
        /// Public message.
        message: Message,
    },
    /// Terminal update payload.
    Terminal {
        /// Terminal A2A state.
        state: TaskState,
    },
}

impl A2ATaskEventPayload {
    const fn kind(&self) -> A2ATaskEventKind {
        match self {
            Self::Snapshot(_) => A2ATaskEventKind::Snapshot,
            Self::StatusUpdate { .. } => A2ATaskEventKind::StatusUpdate,
            Self::ArtifactUpdate { .. } => A2ATaskEventKind::ArtifactUpdate,
            Self::MessageUpdate { .. } => A2ATaskEventKind::MessageUpdate,
            Self::Terminal { .. } => A2ATaskEventKind::Terminal,
        }
    }
}

/// Public, replayable A2A task event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A2ATaskEvent {
    /// Tenant that owns the task.
    pub tenant: String,
    /// Task id.
    pub task_id: TaskId,
    /// Context id.
    pub context_id: String,
    /// Monotonic per-task sequence number.
    pub sequence: u64,
    /// Event timestamp.
    pub occurred_at: AgentTimestampMillis,
    /// Event payload.
    pub payload: A2ATaskEventPayload,
    /// Public metadata.
    pub metadata: HashMap<String, Value>,
}

impl A2ATaskEvent {
    /// Creates a new public task event.
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        task_id: impl Into<String>,
        context_id: impl Into<String>,
        sequence: u64,
        occurred_at: AgentTimestampMillis,
        payload: A2ATaskEventPayload,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            task_id: task_id.into(),
            context_id: context_id.into(),
            sequence,
            occurred_at,
            payload,
            metadata: HashMap::new(),
        }
    }

    /// Event kind.
    #[must_use]
    pub const fn kind(&self) -> A2ATaskEventKind {
        self.payload.kind()
    }

    /// Replay cursor for `subscribe_to_task` and reconnect support.
    #[must_use]
    pub fn replay_cursor(&self) -> String {
        format!("{}:{}", self.task_id, self.sequence)
    }

    /// Returns true when this event carries a terminal task state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self.payload, A2ATaskEventPayload::Terminal { .. })
    }
}

/// In-memory local-mode projection store.
#[derive(Debug, Clone)]
pub struct InMemoryA2ATaskProjectionStore {
    inner: Arc<Mutex<ProjectionStoreState>>,
    require_tenant_filter: bool,
}

#[derive(Debug, Default)]
struct ProjectionStoreState {
    projections: BTreeMap<(String, TaskId), A2ATaskProjection>,
    events: BTreeMap<(String, TaskId), Vec<A2ATaskEvent>>,
}

impl InMemoryA2ATaskProjectionStore {
    /// Creates a local-mode store that permits global listing for examples.
    #[must_use]
    pub fn local() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProjectionStoreState::default())),
            require_tenant_filter: false,
        }
    }

    /// Creates a tenant-scoped store that requires tenant filters.
    #[must_use]
    pub fn tenant_scoped() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProjectionStoreState::default())),
            require_tenant_filter: true,
        }
    }

    /// Inserts or replaces one projection.
    pub fn upsert(&self, projection: A2ATaskProjection) {
        self.inner
            .lock()
            .expect("projection store mutex")
            .projections
            .insert(
                (projection.tenant.clone(), projection.task_id.clone()),
                projection,
            );
    }

    /// Reads one raw projection record.
    pub fn projection(
        &self,
        tenant: Option<&str>,
        task_id: &str,
    ) -> TaskProjectionResult<A2ATaskProjection> {
        if self.require_tenant_filter && tenant.is_none() {
            return Err(TaskProjectionError::TenantRequired);
        }
        let state = self.inner.lock().expect("projection store mutex");
        state
            .projections
            .values()
            .find(|projection| {
                projection.task_id == task_id
                    && tenant.is_none_or(|tenant| projection.tenant == tenant)
            })
            .cloned()
            .ok_or_else(|| TaskProjectionError::TaskNotFound {
                task_id: task_id.to_string(),
            })
    }

    /// Appends a payload as the next event for the task and returns the event.
    pub fn append_event_payload(
        &self,
        tenant: impl Into<String>,
        task_id: impl Into<String>,
        context_id: impl Into<String>,
        occurred_at: AgentTimestampMillis,
        payload: A2ATaskEventPayload,
    ) -> TaskProjectionResult<A2ATaskEvent> {
        let mut state = self.inner.lock().expect("projection store mutex");
        let tenant = tenant.into();
        let task_id = task_id.into();
        let context_id = context_id.into();
        let key = (tenant.clone(), task_id.clone());
        let sequence = state.projections.get(&key).map_or(1, |projection| {
            projection.projection_revision.saturating_add(1)
        });
        let event = A2ATaskEvent::new(tenant, task_id, context_id, sequence, occurred_at, payload);

        if let Some(projection) = state.projections.get_mut(&key) {
            projection.apply_event(&event)?;
        } else if let A2ATaskEventPayload::Snapshot(snapshot) = &event.payload {
            state
                .projections
                .insert(key.clone(), adopted_snapshot(snapshot, &event));
        } else {
            return Err(TaskProjectionError::TaskNotFound {
                task_id: event.task_id,
            });
        }
        state.events.entry(key).or_default().push(event.clone());
        Ok(event)
    }

    /// Appends a public event, updating or bootstrapping the current projection.
    ///
    /// Events for unknown tasks are rejected unless they carry a snapshot, so
    /// the replay log never records an event that no projection accepted.
    pub fn append_event(&self, event: A2ATaskEvent) -> TaskProjectionResult<()> {
        let mut state = self.inner.lock().expect("projection store mutex");
        let key = (event.tenant.clone(), event.task_id.clone());
        if let Some(projection) = state.projections.get_mut(&key) {
            projection.apply_event(&event)?;
        } else if let A2ATaskEventPayload::Snapshot(snapshot) = &event.payload {
            state
                .projections
                .insert(key.clone(), adopted_snapshot(snapshot, &event));
        } else {
            return Err(TaskProjectionError::TaskNotFound {
                task_id: event.task_id,
            });
        }
        state.events.entry(key).or_default().push(event);
        Ok(())
    }

    /// Reads one task projection.
    pub fn get(
        &self,
        tenant: Option<&str>,
        task_id: &str,
        history_length: Option<i32>,
    ) -> TaskProjectionResult<Task> {
        if self.require_tenant_filter && tenant.is_none() {
            return Err(TaskProjectionError::TenantRequired);
        }
        let state = self.inner.lock().expect("projection store mutex");
        let projection = state
            .projections
            .values()
            .find(|projection| {
                projection.task_id == task_id
                    && tenant.is_none_or(|tenant| projection.tenant == tenant)
            })
            .ok_or_else(|| TaskProjectionError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;
        Ok(projection.to_task(history_length, true))
    }

    /// Lists projections with deterministic pagination.
    pub fn list(&self, request: &ListTasksRequest) -> TaskProjectionResult<ListTasksResponse> {
        if self.require_tenant_filter && request.tenant.is_none() {
            return Err(TaskProjectionError::TenantRequired);
        }
        let offset = page_offset(request.page_token.as_deref())?;
        let page_size = page_size(request.page_size);
        let after = request.status_timestamp_after;
        let state = self.inner.lock().expect("projection store mutex");
        let filtered = state
            .projections
            .values()
            .filter(|projection| {
                request
                    .tenant
                    .as_deref()
                    .is_none_or(|tenant| projection.tenant == tenant)
            })
            .filter(|projection| {
                request
                    .context_id
                    .as_deref()
                    .is_none_or(|context_id| projection.context_id == context_id)
            })
            .filter(|projection| {
                request
                    .status
                    .as_ref()
                    .is_none_or(|status| &projection.status == status)
            })
            .filter(|projection| {
                after.is_none_or(|after| {
                    timestamp_to_datetime(projection.status_timestamp)
                        .is_some_and(|timestamp| timestamp > after)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let total_size = i32::try_from(filtered.len()).unwrap_or(i32::MAX);
        let tasks = filtered
            .iter()
            .skip(offset)
            .take(page_size)
            .map(|projection| {
                projection.to_task(
                    request.history_length,
                    request.include_artifacts.unwrap_or(false),
                )
            })
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(tasks.len());
        let next_page_token = if next_offset < filtered.len() {
            next_offset.to_string()
        } else {
            String::new()
        };
        Ok(ListTasksResponse {
            tasks,
            next_page_token,
            page_size: i32::try_from(page_size).unwrap_or(i32::MAX),
            total_size,
        })
    }

    /// Replays public task events after an optional cursor.
    ///
    /// Cursors are only valid for the task that minted them; a cursor from a
    /// different task is rejected instead of silently skipping events.
    pub fn replay_events(
        &self,
        tenant: &str,
        task_id: &str,
        after_cursor: Option<&str>,
    ) -> TaskProjectionResult<Vec<A2ATaskEvent>> {
        let after_sequence = match after_cursor {
            None => 0,
            Some(cursor) => {
                let (cursor_task_id, sequence) = parse_cursor(cursor)?;
                if cursor_task_id != task_id {
                    return Err(TaskProjectionError::InvalidReplayCursor {
                        cursor: cursor.to_string(),
                    });
                }
                sequence
            }
        };
        let state = self.inner.lock().expect("projection store mutex");
        Ok(state
            .events
            .get(&(tenant.to_string(), task_id.to_string()))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.sequence > after_sequence)
            .collect())
    }
}

impl Default for InMemoryA2ATaskProjectionStore {
    fn default() -> Self {
        Self::local()
    }
}

/// Maps durable Rakka run status to public A2A task state.
#[must_use]
pub const fn task_state_from_run_status(status: AgentRunStatus) -> TaskState {
    match status {
        AgentRunStatus::Accepted => TaskState::Submitted,
        AgentRunStatus::Running
        | AgentRunStatus::Cancelling
        | AgentRunStatus::Compensating
        | AgentRunStatus::WaitingForEffect
        | AgentRunStatus::WaitingForTimer => TaskState::Working,
        AgentRunStatus::WaitingForHuman => TaskState::InputRequired,
        AgentRunStatus::Completed => TaskState::Completed,
        AgentRunStatus::Failed => TaskState::Failed,
        AgentRunStatus::Cancelled => TaskState::Canceled,
    }
}

/// Projects selected runtime events into public A2A task events.
#[must_use]
pub fn task_event_from_runtime_event(
    tenant: &AgentTenantId,
    context_id: &str,
    event: &AgentRuntimeEvent,
    next_sequence: u64,
) -> Option<A2ATaskEvent> {
    let state = match event.kind {
        AgentRuntimeEventKind::RunAccepted => Some(TaskState::Submitted),
        AgentRuntimeEventKind::RunStarted
        | AgentRuntimeEventKind::RunWaiting
        | AgentRuntimeEventKind::RunResumed => Some(TaskState::Working),
        AgentRuntimeEventKind::HumanCheckpointOpened => Some(TaskState::InputRequired),
        AgentRuntimeEventKind::RunCompleted => Some(TaskState::Completed),
        AgentRuntimeEventKind::RunFailed => Some(TaskState::Failed),
        AgentRuntimeEventKind::RunCancelled => Some(TaskState::Canceled),
        AgentRuntimeEventKind::NodeRunnable
        | AgentRuntimeEventKind::NodeStarted
        | AgentRuntimeEventKind::NodeCompleted
        | AgentRuntimeEventKind::NodeSkipped
        | AgentRuntimeEventKind::NodeFailed
        | AgentRuntimeEventKind::EffectScheduled
        | AgentRuntimeEventKind::EffectCompleted
        | AgentRuntimeEventKind::EffectFailed
        | AgentRuntimeEventKind::TimerScheduled
        | AgentRuntimeEventKind::TimerFired
        | AgentRuntimeEventKind::HumanDecisionAccepted
        | AgentRuntimeEventKind::BranchSelected
        | AgentRuntimeEventKind::LoopIterationStarted
        | AgentRuntimeEventKind::LoopIterationCompleted => None,
    }?;

    let payload = if state.is_terminal() {
        A2ATaskEventPayload::Terminal {
            state: state.clone(),
        }
    } else {
        A2ATaskEventPayload::StatusUpdate { state }
    };
    Some(A2ATaskEvent::new(
        tenant.as_str(),
        event.run_id.as_str(),
        context_id,
        next_sequence,
        event.occurred_at,
        payload,
    ))
}

/// Converts a Rakka artifact reference into a public A2A artifact projection.
#[must_use]
pub fn a2a_artifact_from_ref(reference: &ArtifactRef) -> Artifact {
    let mut metadata = HashMap::from([
        (
            "io.rakka.artifact.kind".to_string(),
            Value::String(reference.kind.as_label().to_string()),
        ),
        (
            "io.rakka.artifact.redaction".to_string(),
            Value::String(reference.redaction.as_label().to_string()),
        ),
    ]);
    if let Some(byte_len) = reference.byte_len {
        metadata.insert("io.rakka.artifact.byte_len".to_string(), byte_len.into());
    }
    Artifact {
        artifact_id: reference.artifact_id.clone(),
        name: reference.metadata.get("filename").cloned(),
        description: Some("Rakka artifact reference".to_string()),
        parts: vec![Part::url(reference.uri.clone()).with_media_type(
            reference
                .content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        )],
        metadata: Some(metadata),
        extensions: None,
    }
}

fn projection_metadata(
    run_state: &AgentRunState,
    projection_revision: u64,
    compacted: bool,
    history_limit: usize,
) -> HashMap<String, Value> {
    HashMap::from([
        (
            "io.rakka.workflow.id".to_string(),
            Value::String(run_state.workflow_id.to_string()),
        ),
        (
            "io.rakka.workflow.definition_version".to_string(),
            Value::String(run_state.definition_version.to_string()),
        ),
        (
            "io.rakka.status.source".to_string(),
            Value::String("agent-run-state".to_string()),
        ),
        (
            "io.rakka.projection.revision".to_string(),
            Value::Number(projection_revision.into()),
        ),
        (
            "io.rakka.projection.compacted".to_string(),
            Value::Bool(compacted),
        ),
        (
            "io.rakka.projection.history_limit".to_string(),
            Value::Number(history_limit.into()),
        ),
    ])
}

fn timestamp_to_datetime(timestamp: AgentTimestampMillis) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(i64::try_from(timestamp.as_millis()).ok()?)
}

fn bounded_tail<T: Clone>(mut values: Vec<T>, limit: usize) -> Vec<T> {
    if values.len() <= limit {
        return values;
    }
    values.drain(0..values.len() - limit);
    values
}

fn history_limit(history_length: Option<i32>) -> usize {
    match history_length {
        Some(value) if value <= 0 => 0,
        Some(value) => usize::try_from(value).unwrap_or(DEFAULT_HISTORY_LIMIT),
        None => DEFAULT_HISTORY_LIMIT,
    }
}

fn page_size(page_size: Option<i32>) -> usize {
    match page_size {
        Some(value) if value > 0 => usize::try_from(value).unwrap_or(DEFAULT_PAGE_SIZE),
        _ => DEFAULT_PAGE_SIZE,
    }
    .min(MAX_PAGE_SIZE)
}

fn page_offset(page_token: Option<&str>) -> TaskProjectionResult<usize> {
    match page_token {
        None | Some("") => Ok(0),
        Some(token) => token
            .parse::<usize>()
            .map_err(|_| TaskProjectionError::InvalidPageToken {
                token: token.to_string(),
            }),
    }
}

fn parse_cursor(cursor: &str) -> TaskProjectionResult<(String, u64)> {
    let Some((task_id, sequence)) = cursor.rsplit_once(':') else {
        return Err(TaskProjectionError::InvalidReplayCursor {
            cursor: cursor.to_string(),
        });
    };
    let sequence =
        sequence
            .parse::<u64>()
            .map_err(|_| TaskProjectionError::InvalidReplayCursor {
                cursor: cursor.to_string(),
            })?;
    Ok((task_id.to_string(), sequence))
}

fn adopted_snapshot(snapshot: &A2ATaskProjection, event: &A2ATaskEvent) -> A2ATaskProjection {
    let mut adopted = snapshot.clone();
    adopted.status_timestamp = event.occurred_at;
    adopted.projection_revision = event.sequence;
    adopted.metadata.insert(
        "io.rakka.projection.revision".to_string(),
        Value::Number(event.sequence.into()),
    );
    adopted
}

#[cfg(test)]
mod tests {
    use super::*;
    use a2a::{Message, Part, Role};
    use rakka::agent_workflow::{
        AgentAttributes, AgentCausationId, AgentCompiledPlanFingerprint, AgentCorrelationId,
        AgentRunId, AgentStatePayload, AgentTelemetryContext, AgentWorkflowId, StateSchemaVersion,
        WorkflowDefinitionVersion,
    };

    fn run_state(status: AgentRunStatus) -> AgentRunState {
        AgentRunState {
            run_id: AgentRunId::new("task-1"),
            workflow_id: AgentWorkflowId::new("workflow-1"),
            tenant: Some(AgentTenantId::new("tenant-a")),
            definition_version: WorkflowDefinitionVersion::new("v1"),
            state_schema_version: StateSchemaVersion::new(1),
            graph_state: None,
            status,
            current_step_id: None,
            current_attempt: 0,
            inputs_ref: None,
            state_payload: AgentStatePayload::Empty,
            checkpoints: Vec::new(),
            pending_effects: Vec::new(),
            pending_human_checkpoint: None,
            cancellation: None,
            created_at: AgentTimestampMillis::new(10),
            updated_at: AgentTimestampMillis::new(20),
            completed_at: None,
        }
    }

    #[test]
    fn every_run_status_maps_to_valid_task_state() {
        let cases = [
            (AgentRunStatus::Accepted, TaskState::Submitted),
            (AgentRunStatus::Running, TaskState::Working),
            (AgentRunStatus::WaitingForTimer, TaskState::Working),
            (AgentRunStatus::WaitingForHuman, TaskState::InputRequired),
            (AgentRunStatus::WaitingForEffect, TaskState::Working),
            (AgentRunStatus::Cancelling, TaskState::Working),
            (AgentRunStatus::Completed, TaskState::Completed),
            (AgentRunStatus::Failed, TaskState::Failed),
            (AgentRunStatus::Compensating, TaskState::Working),
            (AgentRunStatus::Cancelled, TaskState::Canceled),
        ];
        for (run_status, task_state) in cases {
            assert_eq!(task_state_from_run_status(run_status), task_state);
        }
    }

    #[test]
    fn projection_serialization_round_trips_and_bounds_history() {
        let history = (0..5)
            .map(|index| Message {
                message_id: format!("msg-{index}"),
                context_id: Some("ctx".to_string()),
                task_id: Some("task-1".to_string()),
                role: Role::User,
                parts: vec![Part::text(format!("hello {index}"))],
                metadata: None,
                extensions: None,
                reference_task_ids: None,
            })
            .collect::<Vec<_>>();
        let projection = A2ATaskProjection::from_run_state(
            &run_state(AgentRunStatus::Completed),
            "ctx",
            history,
            Vec::new(),
            3,
        );
        let json = serde_json::to_string(&projection).expect("json");
        let decoded: A2ATaskProjection = serde_json::from_str(&json).expect("decode");
        assert_eq!(decoded.status, TaskState::Completed);

        let task = decoded.to_task(Some(2), false);
        assert_eq!(task.history.expect("history").len(), 2);
        assert!(task.artifacts.is_none());
    }

    #[test]
    fn artifact_projection_uses_url_reference_not_payload_bytes() {
        let reference = ArtifactRef {
            artifact_id: "artifact-1".to_string(),
            kind: rakka::agent_workflow::ArtifactKind::Input,
            uri: "s3://bucket/key".to_string(),
            checksum: Some("sha256:test".to_string()),
            content_type: Some("text/plain".to_string()),
            byte_len: Some(99),
            retention_class: Some("standard".to_string()),
            encryption: None,
            redaction: rakka::agent_workflow::RedactionStatus::ReferenceOnly,
            created_at: AgentTimestampMillis::new(10),
            metadata: AgentAttributes::new(),
        };
        let artifact = a2a_artifact_from_ref(&reference);
        assert_eq!(artifact.parts[0].as_text(), None);
        let serialized = serde_json::to_value(&artifact).expect("artifact json");
        assert_eq!(serialized["parts"][0]["url"], "s3://bucket/key");
    }

    #[test]
    fn query_store_filters_and_paginates_deterministically() {
        let store = InMemoryA2ATaskProjectionStore::local();
        for index in 0..3 {
            store.upsert(A2ATaskProjection::accepted(
                format!("task-{index}"),
                "ctx",
                "tenant-a",
                "workflow",
                AgentTimestampMillis::new(index + 1),
                Vec::new(),
                index + 1,
            ));
        }

        let page1 = store
            .list(&ListTasksRequest {
                context_id: Some("ctx".to_string()),
                status: Some(TaskState::Submitted),
                page_size: Some(2),
                page_token: None,
                history_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
                tenant: Some("tenant-a".to_string()),
            })
            .expect("page1");
        assert_eq!(page1.tasks.len(), 2);
        assert_eq!(page1.next_page_token, "2");

        let page2 = store
            .list(&ListTasksRequest {
                page_token: Some(page1.next_page_token),
                page_size: Some(2),
                tenant: Some("tenant-a".to_string()),
                context_id: None,
                status: None,
                history_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
            })
            .expect("page2");
        assert_eq!(page2.tasks.len(), 1);
    }

    #[test]
    fn tenant_scoped_store_requires_tenant_filter() {
        let store = InMemoryA2ATaskProjectionStore::tenant_scoped();
        let error = store
            .list(&ListTasksRequest {
                context_id: None,
                status: None,
                page_size: None,
                page_token: None,
                history_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
                tenant: None,
            })
            .expect_err("tenant required");
        assert_eq!(error.code(), "tenant-required");
    }

    #[test]
    fn public_task_event_has_replay_cursor_and_updates_snapshot() {
        let mut projection = A2ATaskProjection::accepted(
            "task-1",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            0,
        );
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-1",
            "ctx",
            1,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        );
        assert_eq!(event.replay_cursor(), "task-1:1");
        projection.apply_event(&event).expect("apply");
        assert_eq!(projection.status, TaskState::Working);
    }

    #[test]
    fn snapshot_event_revision_is_owned_by_event_sequence() {
        let mut projection = A2ATaskProjection::accepted(
            "task-1",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            0,
        );
        let snapshot = A2ATaskProjection::accepted(
            "task-1",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(11),
            Vec::new(),
            99,
        );
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-1",
            "ctx",
            1,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::Snapshot(snapshot),
        );

        projection.apply_event(&event).expect("apply snapshot");

        assert_eq!(projection.projection_revision, 1);
        assert_eq!(projection.status_timestamp, AgentTimestampMillis::new(20));
        assert_eq!(
            projection.metadata["io.rakka.projection.revision"],
            Value::Number(1.into())
        );
    }

    #[test]
    fn failed_event_apply_is_not_recorded_for_replay() {
        let store = InMemoryA2ATaskProjectionStore::local();
        store.upsert(A2ATaskProjection::accepted(
            "task-1",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            0,
        ));
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-1",
            "ctx",
            3,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        );

        let error = store.append_event(event).expect_err("sequence error");

        assert_eq!(error.code(), "event-order");
        assert!(store
            .replay_events("tenant-a", "task-1", None)
            .expect("replay")
            .is_empty());
    }

    #[test]
    fn orphan_event_for_unknown_task_is_rejected_and_not_recorded() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-unknown",
            "ctx",
            5,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        );

        let error = store.append_event(event).expect_err("orphan event");

        assert_eq!(error.code(), "task-not-found");
        assert!(store
            .replay_events("tenant-a", "task-unknown", None)
            .expect("replay")
            .is_empty());
    }

    #[test]
    fn snapshot_event_bootstraps_unknown_task() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let snapshot = A2ATaskProjection::accepted(
            "task-boot",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            0,
        );
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-boot",
            "ctx",
            1,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::Snapshot(snapshot),
        );

        store.append_event(event).expect("bootstrap snapshot");

        let task = store.get(Some("tenant-a"), "task-boot", None).expect("get");
        assert_eq!(task.id, "task-boot");
        assert_eq!(
            store
                .replay_events("tenant-a", "task-boot", None)
                .expect("replay")
                .len(),
            1
        );
    }

    #[test]
    fn replay_cursor_from_another_task_is_rejected() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let error = store
            .replay_events("tenant-a", "task-1", Some("task-2:5"))
            .expect_err("cursor task mismatch");
        assert_eq!(error.code(), "invalid-replay-cursor");

        let error = store
            .replay_events("tenant-a", "task-1", Some("not-a-cursor"))
            .expect_err("malformed cursor");
        assert_eq!(error.code(), "invalid-replay-cursor");
    }

    #[test]
    fn run_state_projection_keeps_newest_artifacts() {
        let refs = (0..DEFAULT_ARTIFACT_LIMIT + 5)
            .map(|index| ArtifactRef {
                artifact_id: format!("artifact-{index}"),
                kind: rakka::agent_workflow::ArtifactKind::Input,
                uri: format!("s3://bucket/{index}"),
                checksum: None,
                content_type: Some("text/plain".to_string()),
                byte_len: Some(1),
                retention_class: Some("standard".to_string()),
                encryption: None,
                redaction: rakka::agent_workflow::RedactionStatus::ReferenceOnly,
                created_at: AgentTimestampMillis::new(10),
                metadata: AgentAttributes::new(),
            })
            .collect::<Vec<_>>();

        let projection = A2ATaskProjection::from_run_state(
            &run_state(AgentRunStatus::Running),
            "ctx",
            Vec::new(),
            refs,
            1,
        );

        assert_eq!(projection.artifacts.len(), DEFAULT_ARTIFACT_LIMIT);
        assert_eq!(projection.artifacts[0].artifact_id, "artifact-5");
        assert_eq!(
            projection.artifacts[DEFAULT_ARTIFACT_LIMIT - 1].artifact_id,
            format!("artifact-{}", DEFAULT_ARTIFACT_LIMIT + 4)
        );
    }

    #[test]
    fn runtime_events_project_only_public_status_changes() {
        let runtime = AgentRuntimeEvent::new(
            AgentWorkflowId::new("workflow-1"),
            AgentRunId::new("task-1"),
            WorkflowDefinitionVersion::new("v1"),
            AgentCompiledPlanFingerprint::new("fingerprint"),
            1,
            1,
            AgentTimestampMillis::new(20),
            AgentRuntimeEventKind::RunCompleted,
            AgentCausationId::new("cause"),
            AgentCorrelationId::new("corr"),
            AgentTelemetryContext::default(),
        )
        .expect("runtime event");
        let task_event =
            task_event_from_runtime_event(&AgentTenantId::new("tenant-a"), "ctx", &runtime, 2)
                .expect("public event");
        assert_eq!(task_event.kind(), A2ATaskEventKind::Terminal);
        assert!(task_event.is_terminal());

        let internal = AgentRuntimeEvent::new(
            AgentWorkflowId::new("workflow-1"),
            AgentRunId::new("task-1"),
            WorkflowDefinitionVersion::new("v1"),
            AgentCompiledPlanFingerprint::new("fingerprint"),
            2,
            2,
            AgentTimestampMillis::new(21),
            AgentRuntimeEventKind::NodeStarted,
            AgentCausationId::new("cause"),
            AgentCorrelationId::new("corr"),
            AgentTelemetryContext::default(),
        )
        .expect("runtime event");
        assert!(task_event_from_runtime_event(
            &AgentTenantId::new("tenant-a"),
            "ctx",
            &internal,
            3,
        )
        .is_none());
    }
}
