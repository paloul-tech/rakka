//! A2A task read model and public task-event projection types.
//!
//! The task projection is a query/observability surface: durable run state
//! plus durable inbox/outbox state remain the source of correctness. These
//! types define the bounded public shape of a task, the ordered public task
//! events that update it, and the replay cursors clients resume from.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use a2a::{Artifact, Message, Part, Task, TaskId, TaskState, TaskStatus};
use chrono::{DateTime, Utc};
use rakka_agent_workflow::{
    AgentRunState, AgentRunStatus, AgentRuntimeEvent, AgentRuntimeEventKind, AgentTenantId,
    AgentTimestampMillis, ArtifactRef, RedactionStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mapping::DEFAULT_TENANT;

pub(crate) const DEFAULT_PAGE_SIZE: usize = 50;
pub(crate) const MAX_PAGE_SIZE: usize = 100;
pub(crate) const DEFAULT_HISTORY_LIMIT: usize = 20;
pub(crate) const DEFAULT_ARTIFACT_LIMIT: usize = 20;

/// Task metadata key carrying the projection revision.
///
/// Compatibility commitment: clients read this key from `Task.metadata` and
/// stream frames to correlate reads with replay cursors.
pub const META_PROJECTION_REVISION: &str = "io.rakka.projection.revision";
/// Task metadata key describing which surface produced the current status.
pub const META_STATUS_SOURCE: &str = "io.rakka.status.source";
/// Artifact metadata key carrying the Rakka artifact kind label.
pub const META_ARTIFACT_KIND: &str = "io.rakka.artifact.kind";
/// Artifact metadata key carrying the Rakka artifact redaction label.
pub const META_ARTIFACT_REDACTION: &str = "io.rakka.artifact.redaction";
/// Artifact metadata key carrying the artifact byte length.
pub const META_ARTIFACT_BYTE_LEN: &str = "io.rakka.artifact.byte_len";

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
    /// The requested replay window precedes the retained event log.
    ReplayWindowExpired {
        /// Task whose events were requested.
        task_id: TaskId,
        /// Earliest sequence still retained.
        earliest_sequence: u64,
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
    /// The projection store backend failed.
    Store {
        /// Stable backend name.
        backend: &'static str,
        /// Failure summary. Never contains payload or secret material.
        message: String,
    },
}

impl TaskProjectionError {
    /// Stable machine-readable code.
    ///
    /// Compatibility commitment: these codes surface in A2A error messages
    /// and adapter metrics labels.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::TenantRequired => "tenant-required",
            Self::InvalidPageToken { .. } => "invalid-page-token",
            Self::InvalidReplayCursor { .. } => "invalid-replay-cursor",
            Self::ReplayWindowExpired { .. } => "replay-window-expired",
            Self::TaskNotFound { .. } => "task-not-found",
            Self::EventOrder { .. } => "event-order",
            Self::Store { .. } => "projection-store",
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
            Self::ReplayWindowExpired {
                task_id,
                earliest_sequence,
            } => {
                write!(
                    f,
                    "replay window for task {task_id} starts at sequence {earliest_sequence}"
                )
            }
            Self::TaskNotFound { task_id } => write!(f, "task not found: {task_id}"),
            Self::EventOrder { expected, actual } => {
                write!(
                    f,
                    "event sequence {actual} does not follow expected {expected}"
                )
            }
            Self::Store { backend, message } => {
                write!(f, "projection store {backend} failed: {message}")
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
                    META_PROJECTION_REVISION.to_string(),
                    Value::Number(projection_revision.into()),
                ),
                (
                    META_STATUS_SOURCE.to_string(),
                    Value::String("a2a-command-draft".to_string()),
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
        // Monotonic: an event carrying a stale occurred_at (e.g. derived from
        // an older run-state snapshot) must not move the status time backward.
        self.status_timestamp = self.status_timestamp.max(event.occurred_at);
        self.projection_revision = event.sequence;
        self.metadata.insert(
            META_PROJECTION_REVISION.to_string(),
            Value::Number(event.sequence.into()),
        );
        match &event.payload {
            A2ATaskEventPayload::Snapshot(task) => {
                // The adopted snapshot inherits the clamped status time so a
                // re-snapshot cannot move it backward either.
                let status_timestamp = self.status_timestamp;
                *self = adopted_snapshot(task, event);
                self.status_timestamp = self.status_timestamp.max(status_timestamp);
            }
            A2ATaskEventPayload::StatusUpdate { state }
            | A2ATaskEventPayload::Terminal { state } => {
                // Enforced at this chokepoint so every writer inherits the
                // no-regression rule, not just the request handler's sync.
                if status_transition_allowed(&self.status, state) {
                    self.status = state.clone();
                }
            }
            A2ATaskEventPayload::ArtifactUpdate { artifact } => {
                self.artifacts.push(artifact.clone());
                cap_front(&mut self.artifacts, DEFAULT_ARTIFACT_LIMIT);
            }
            A2ATaskEventPayload::MessageUpdate { message } => {
                self.history.push(message.clone());
                cap_front(&mut self.history, DEFAULT_HISTORY_LIMIT);
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

impl A2ATaskEventKind {
    /// Stable lowercase label for protocol metadata and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::StatusUpdate => "status-update",
            Self::ArtifactUpdate => "artifact-update",
            Self::MessageUpdate => "message-update",
            Self::Terminal => "terminal",
        }
    }
}

/// Public redaction status carried by task events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ATaskEventRedaction {
    /// Redaction status is not known.
    Unknown,
    /// Payload is safe to expose in the public task event.
    Unredacted,
    /// Payload was redacted before public projection.
    Redacted,
    /// Payload is represented only by a reference.
    ReferenceOnly,
}

impl A2ATaskEventRedaction {
    /// Stable lowercase label for metadata and bounded metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Unredacted => "unredacted",
            Self::Redacted => "redacted",
            Self::ReferenceOnly => "reference-only",
        }
    }

    fn from_label(value: &str) -> Self {
        match value {
            "unredacted" => Self::Unredacted,
            "redacted" => Self::Redacted,
            "reference-only" => Self::ReferenceOnly,
            _ => Self::Unknown,
        }
    }
}

impl From<RedactionStatus> for A2ATaskEventRedaction {
    fn from(value: RedactionStatus) -> Self {
        match value {
            RedactionStatus::Unknown => Self::Unknown,
            RedactionStatus::Unredacted => Self::Unredacted,
            RedactionStatus::Redacted => Self::Redacted,
            RedactionStatus::ReferenceOnly => Self::ReferenceOnly,
        }
    }
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

    fn projected_state(&self) -> TaskState {
        match self {
            Self::Snapshot(projection) => projection.status.clone(),
            Self::StatusUpdate { state } | Self::Terminal { state } => state.clone(),
            Self::ArtifactUpdate { .. } | Self::MessageUpdate { .. } => TaskState::Unspecified,
        }
    }

    fn redaction(&self) -> A2ATaskEventRedaction {
        match self {
            Self::ArtifactUpdate { artifact } => artifact
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(META_ARTIFACT_REDACTION))
                .and_then(Value::as_str)
                .map(A2ATaskEventRedaction::from_label)
                .unwrap_or(A2ATaskEventRedaction::Unknown),
            Self::MessageUpdate { .. } => A2ATaskEventRedaction::Unknown,
            Self::Snapshot(_) | Self::StatusUpdate { .. } | Self::Terminal { .. } => {
                A2ATaskEventRedaction::Unredacted
            }
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
    /// Projected public task state after this event is applied.
    pub projected_state: TaskState,
    /// Public redaction status for this event payload.
    pub redaction: A2ATaskEventRedaction,
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
        let projected_state = payload.projected_state();
        let redaction = payload.redaction();
        Self {
            tenant: tenant.into(),
            task_id: task_id.into(),
            context_id: context_id.into(),
            sequence,
            occurred_at,
            projected_state,
            redaction,
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
        encode_replay_cursor(&self.task_id, self.sequence)
    }

    /// Returns true when this event carries a terminal task state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self.payload, A2ATaskEventPayload::Terminal { .. })
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
///
/// Only run-level lifecycle transitions become public task events; node,
/// effect, timer, and internal scheduler events stay private.
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
///
/// Only the reference, content type, and bounded metadata cross into the
/// public artifact; payload bytes never do.
#[must_use]
pub fn a2a_artifact_from_ref(reference: &ArtifactRef) -> Artifact {
    let mut metadata = HashMap::from([
        (
            META_ARTIFACT_KIND.to_string(),
            Value::String(reference.kind.as_label().to_string()),
        ),
        (
            META_ARTIFACT_REDACTION.to_string(),
            Value::String(reference.redaction.as_label().to_string()),
        ),
    ]);
    if let Some(byte_len) = reference.byte_len {
        metadata.insert(META_ARTIFACT_BYTE_LEN.to_string(), byte_len.into());
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
            crate::mapping::META_WORKFLOW_ID.to_string(),
            Value::String(run_state.workflow_id.to_string()),
        ),
        (
            crate::mapping::META_DEFINITION_VERSION.to_string(),
            Value::String(run_state.definition_version.to_string()),
        ),
        (
            META_STATUS_SOURCE.to_string(),
            Value::String("agent-run-state".to_string()),
        ),
        (
            META_PROJECTION_REVISION.to_string(),
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

pub(crate) fn timestamp_to_datetime(timestamp: AgentTimestampMillis) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(i64::try_from(timestamp.as_millis()).ok()?)
}

pub(crate) fn bounded_tail<T: Clone>(mut values: Vec<T>, limit: usize) -> Vec<T> {
    cap_front(&mut values, limit);
    values
}

/// Keeps the newest `limit` entries in place, dropping the oldest first.
///
/// The single definition of the drop-oldest rule shared by the projection
/// history/artifact bounds and the replay event log.
pub(crate) fn cap_front<T>(values: &mut Vec<T>, limit: usize) {
    if values.len() > limit {
        let excess = values.len() - limit;
        values.drain(0..excess);
    }
}

/// Returns true when the public task status may move from `current` to `next`.
///
/// The public status never regresses: `Submitted` is the floor once a task
/// has progressed, and a terminal state is never overwritten by a
/// non-terminal one. Shared by `apply_event` (so every writer inherits the
/// rule) and the request handler's status sync (so no-op events are skipped
/// before they are appended).
pub(crate) fn status_transition_allowed(current: &TaskState, next: &TaskState) -> bool {
    if next == current {
        return true;
    }
    if *next == TaskState::Submitted && *current != TaskState::Submitted {
        return false;
    }
    !current.is_terminal() || next.is_terminal()
}

pub(crate) fn history_limit(history_length: Option<i32>) -> usize {
    match history_length {
        Some(value) if value <= 0 => 0,
        Some(value) => usize::try_from(value).unwrap_or(DEFAULT_HISTORY_LIMIT),
        None => DEFAULT_HISTORY_LIMIT,
    }
}

pub(crate) fn page_size(page_size: Option<i32>) -> usize {
    match page_size {
        Some(value) if value > 0 => usize::try_from(value).unwrap_or(DEFAULT_PAGE_SIZE),
        _ => DEFAULT_PAGE_SIZE,
    }
    .min(MAX_PAGE_SIZE)
}

pub(crate) fn page_offset(page_token: Option<&str>) -> TaskProjectionResult<usize> {
    match page_token {
        None | Some("") => Ok(0),
        Some(token) => token
            .parse::<usize>()
            .map_err(|_| TaskProjectionError::InvalidPageToken {
                token: token.to_string(),
            }),
    }
}

/// Encodes a replay cursor for a task position.
///
/// Compatibility commitment: the `<task-id>:<sequence>` cursor shape is what
/// clients echo back through `last-event-id` / replay-cursor headers.
#[must_use]
pub fn encode_replay_cursor(task_id: &str, sequence: u64) -> String {
    format!("{task_id}:{sequence}")
}

/// Parses a replay cursor into its task id and sequence.
pub fn parse_replay_cursor(cursor: &str) -> TaskProjectionResult<(String, u64)> {
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

/// Adopts a snapshot payload, letting the event own revision and timestamp.
pub(crate) fn adopted_snapshot(
    snapshot: &A2ATaskProjection,
    event: &A2ATaskEvent,
) -> A2ATaskProjection {
    let mut adopted = snapshot.clone();
    adopted.status_timestamp = event.occurred_at;
    adopted.projection_revision = event.sequence;
    adopted.metadata.insert(
        META_PROJECTION_REVISION.to_string(),
        Value::Number(event.sequence.into()),
    );
    adopted
}

#[cfg(test)]
mod tests {
    use super::*;
    use a2a::{Message, Part, Role};
    use rakka_agent_workflow::{
        AgentAttributes, AgentCausationId, AgentCompiledPlanFingerprint, AgentCorrelationId,
        AgentRunId, AgentTelemetryContext, AgentWorkflowId, WorkflowDefinitionVersion,
    };

    use crate::testing::fixture_run_state;

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
            &fixture_run_state("task-1", AgentRunStatus::Completed),
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
            kind: rakka_agent_workflow::ArtifactKind::Input,
            uri: "s3://bucket/key".to_string(),
            checksum: Some("sha256:test".to_string()),
            content_type: Some("text/plain".to_string()),
            byte_len: Some(99),
            retention_class: Some("standard".to_string()),
            encryption: None,
            redaction: RedactionStatus::ReferenceOnly,
            created_at: AgentTimestampMillis::new(10),
            metadata: AgentAttributes::new(),
        };
        let artifact = a2a_artifact_from_ref(&reference);
        assert_eq!(artifact.parts[0].as_text(), None);
        let serialized = serde_json::to_value(&artifact).expect("artifact json");
        assert_eq!(serialized["parts"][0]["url"], "s3://bucket/key");
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
            projection.metadata[META_PROJECTION_REVISION],
            Value::Number(1.into())
        );
    }

    #[test]
    fn run_state_projection_keeps_newest_artifacts() {
        let refs = (0..DEFAULT_ARTIFACT_LIMIT + 5)
            .map(|index| ArtifactRef {
                artifact_id: format!("artifact-{index}"),
                kind: rakka_agent_workflow::ArtifactKind::Input,
                uri: format!("s3://bucket/{index}"),
                checksum: None,
                content_type: Some("text/plain".to_string()),
                byte_len: Some(1),
                retention_class: Some("standard".to_string()),
                encryption: None,
                redaction: RedactionStatus::ReferenceOnly,
                created_at: AgentTimestampMillis::new(10),
                metadata: AgentAttributes::new(),
            })
            .collect::<Vec<_>>();

        let projection = A2ATaskProjection::from_run_state(
            &fixture_run_state("task-1", AgentRunStatus::Running),
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

    #[test]
    fn replay_cursor_round_trips_and_rejects_malformed_input() {
        let cursor = encode_replay_cursor("task-1", 42);
        assert_eq!(
            parse_replay_cursor(&cursor).expect("parse"),
            ("task-1".to_string(), 42)
        );
        assert!(parse_replay_cursor("not-a-cursor").is_err());
        assert!(parse_replay_cursor("task-1:not-a-number").is_err());
    }
}
