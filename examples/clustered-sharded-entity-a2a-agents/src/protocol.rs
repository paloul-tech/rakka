//! Remote-safe protocol between public A2A ingress nodes and sharded run owners.
//!
//! These payloads are the only values serialized over Rakka remoting for this
//! example. They intentionally exclude process-local values such as `ReplyTo`,
//! actor refs, store handles, and `Arc`.

use std::collections::BTreeMap;
use std::time::Duration;

use a2a::{Message, TaskPushNotificationConfig};
use rakka::agent_workflow::{AgentTimestampMillis, ArtifactRef};
use serde::{Deserialize, Serialize};

use crate::a2a_mapping::{A2ACommandDraft, A2ATaskIntent};
use crate::task_projection::A2ATaskProjection;

/// Current adapter-owned inter-node protocol version.
pub const A2A_RUN_PROTOCOL_VERSION: u32 = 1;

/// Current remoting schema version registered in `SerializationRegistry`.
pub const A2A_RUN_REMOTE_SCHEMA_VERSION: u32 = 1;

/// Stable remote message type id for owner requests.
pub const A2A_RUN_REQUEST_TYPE_ID: &str = "rakka.examples.a2a.A2ARunRequest";

/// Stable remote message type id for owner responses.
pub const A2A_RUN_RESPONSE_TYPE_ID: &str = "rakka.examples.a2a.A2ARunResponse";

/// Adapter-owned request routed to the sharded run owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A2ARunRequest {
    /// Protocol version interpreted by the owner entity.
    pub version: u32,
    /// A2A task id, equal to Rakka run id and sharded entity id.
    pub task_id: String,
    /// Canonical tenant for authorization and durable command boundaries.
    ///
    /// `None` marks an unscoped read: the owner resolves the run's stored
    /// tenant instead of enforcing a caller-supplied one, mirroring the
    /// local-mode projection store's unscoped read semantics. Durable
    /// commands (accept, cancel) always carry `Some` canonical tenant.
    pub tenant: Option<String>,
    /// Bounded command metadata used for routing diagnostics and evolution.
    pub command: A2ARunCommandMetadata,
    /// Projection controls requested by the ingress node.
    pub projection: A2AProjectionHints,
    /// Timeout policy selected by the ingress node.
    pub timeout: A2ATimeoutPolicy,
    /// Operation requested of the owner.
    pub kind: A2ARunRequestKind,
}

impl A2ARunRequest {
    /// Creates a request with the current protocol version.
    #[must_use]
    pub fn new(
        task_id: impl Into<String>,
        tenant: Option<String>,
        command: A2ARunCommandMetadata,
        projection: A2AProjectionHints,
        timeout: A2ATimeoutPolicy,
        kind: A2ARunRequestKind,
    ) -> Self {
        Self {
            version: A2A_RUN_PROTOCOL_VERSION,
            task_id: task_id.into(),
            tenant,
            command,
            projection,
            timeout,
            kind,
        }
    }
}

/// Owner operation carried by [`A2ARunRequest`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum A2ARunRequestKind {
    /// Durably accept an A2A message command and optionally drive one step.
    AcceptMessage {
        /// Normalized durable command draft.
        draft: Box<A2ACommandDraft>,
        /// Public message projected after durable acceptance.
        projected_message: Box<Message>,
        /// Artifact references derived during normalization.
        artifacts: Vec<ArtifactRef>,
        /// Whether to return immediately after durable acceptance.
        return_immediately: bool,
        /// Ingress receipt timestamp.
        received_at: AgentTimestampMillis,
    },
    /// Return the current task projection, lazily recovering it if needed.
    QueryTaskSnapshot,
    /// Durably accept cancellation and converge to the current cancel state.
    CancelTask {
        /// Normalized durable cancellation command draft.
        draft: Box<A2ACommandDraft>,
        /// Ingress receipt timestamp.
        received_at: AgentTimestampMillis,
    },
    /// Open a stream cursor for a later streaming phase.
    OpenStreamCursor {
        /// Replay cursor supplied by the client, if any.
        after_cursor: Option<String>,
    },
    /// Record a push notification config for a later push phase.
    RecordPushConfig {
        /// Public push config to store durably in a later phase.
        config: TaskPushNotificationConfig,
    },
    /// Delete a push notification config for a later push phase.
    DeletePushConfig {
        /// Config id or URL chosen by the public adapter.
        config_id: String,
    },
}

/// Bounded command metadata duplicated outside the command draft for routing,
/// observability, and future protocol evolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2ARunCommandMetadata {
    /// Durable command id, when the operation carries one.
    pub command_id: Option<String>,
    /// Durable inbox deduplication key, when the operation carries one.
    pub deduplication_key: Option<String>,
    /// Causation id, when the operation carries one.
    pub causation_id: Option<String>,
    /// Correlation id, when the operation carries one.
    pub correlation_id: Option<String>,
    /// New-task versus continuation intent, when relevant.
    pub intent: Option<A2ATaskIntent>,
    /// Low-cardinality adapter metadata.
    pub attributes: BTreeMap<String, String>,
}

impl A2ARunCommandMetadata {
    /// Metadata for requests that are not durable commands.
    #[must_use]
    pub fn query() -> Self {
        Self {
            command_id: None,
            deduplication_key: None,
            causation_id: None,
            correlation_id: None,
            intent: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Metadata extracted from a normalized command draft.
    #[must_use]
    pub fn from_draft(draft: &A2ACommandDraft) -> Self {
        let metadata = &draft.command.metadata;
        Self {
            command_id: Some(metadata.command_id.as_str().to_string()),
            deduplication_key: Some(metadata.deduplication_key.as_str().to_string()),
            causation_id: Some(metadata.causation_id.as_str().to_string()),
            correlation_id: Some(metadata.correlation_id.as_str().to_string()),
            intent: Some(draft.normalized.intent),
            attributes: BTreeMap::from([
                (
                    "workflow_id".to_string(),
                    metadata.workflow_id.as_str().to_string(),
                ),
                (
                    "command_kind".to_string(),
                    draft.command.kind.type_name().to_string(),
                ),
            ]),
        }
    }
}

/// Projection controls sent to the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2AProjectionHints {
    /// Requested history length.
    pub history_length: Option<i32>,
    /// Whether artifact projections are needed by the caller.
    pub include_artifacts: bool,
}

impl A2AProjectionHints {
    /// Projection controls for a full current task snapshot.
    #[must_use]
    pub const fn new(history_length: Option<i32>, include_artifacts: bool) -> Self {
        Self {
            history_length,
            include_artifacts,
        }
    }
}

impl Default for A2AProjectionHints {
    fn default() -> Self {
        Self {
            history_length: None,
            include_artifacts: true,
        }
    }
}

/// Bounded timeout policy chosen before routing to the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2ATimeoutPolicy {
    /// Ask timeout in milliseconds.
    pub ask_timeout_millis: u64,
}

impl A2ATimeoutPolicy {
    /// Creates timeout policy from a duration.
    #[must_use]
    pub fn from_duration(timeout: Duration) -> Self {
        Self {
            ask_timeout_millis: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        }
    }
}

/// Response returned by the sharded run owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A2ARunResponse {
    /// Protocol version emitted by the owner entity.
    pub version: u32,
    /// A2A task id.
    pub task_id: String,
    /// Canonical tenant.
    pub tenant: String,
    /// Owner outcome.
    pub outcome: A2ARunResponseKind,
}

impl A2ARunResponse {
    /// Successful projection response.
    #[must_use]
    pub fn task(
        task_id: impl Into<String>,
        tenant: impl Into<String>,
        projection: A2ATaskProjection,
    ) -> Self {
        Self {
            version: A2A_RUN_PROTOCOL_VERSION,
            task_id: task_id.into(),
            tenant: tenant.into(),
            outcome: A2ARunResponseKind::TaskSnapshot { projection },
        }
    }

    /// Failure response.
    #[must_use]
    pub fn failure(
        task_id: impl Into<String>,
        tenant: impl Into<String>,
        failure: A2ARunFailure,
    ) -> Self {
        Self {
            version: A2A_RUN_PROTOCOL_VERSION,
            task_id: task_id.into(),
            tenant: tenant.into(),
            outcome: A2ARunResponseKind::Failure { failure },
        }
    }
}

/// Owner response payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum A2ARunResponseKind {
    /// Current public task projection.
    TaskSnapshot {
        /// Adapter-owned projection record.
        projection: A2ATaskProjection,
    },
    /// Stream cursor opened by a later streaming phase.
    StreamCursor {
        /// Replay cursor.
        cursor: String,
    },
    /// Push config recorded by a later push phase.
    PushConfigRecorded {
        /// Stored push notification config.
        config: TaskPushNotificationConfig,
    },
    /// Push config deleted by a later push phase.
    PushConfigDeleted,
    /// Request failed on the owner.
    Failure {
        /// Stable failure payload.
        failure: A2ARunFailure,
    },
}

/// Owner failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ARunFailureKind {
    /// Task is not visible to the caller.
    TaskNotFound,
    /// Task is terminal or otherwise not cancelable.
    TaskNotCancelable,
    /// Request failed validation.
    InvalidRequest,
    /// Owner or peer is temporarily unavailable.
    Unavailable,
    /// Request used an unsupported protocol version.
    VersionMismatch,
    /// Operation is intentionally deferred to a later phase.
    Unsupported,
    /// Internal owner failure.
    Internal,
}

/// Stable failure payload returned over remoting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2ARunFailure {
    /// Machine-readable adapter code.
    pub code: String,
    /// Human-readable summary.
    pub message: String,
    /// Failure class used by the ingress adapter.
    pub kind: A2ARunFailureKind,
    /// Whether clients may retry on any public node.
    pub retryable: bool,
}

impl A2ARunFailure {
    /// Creates a failure payload.
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        kind: A2ARunFailureKind,
        retryable: bool,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            kind,
            retryable,
        }
    }

    /// Failure for an unsupported protocol version.
    #[must_use]
    pub fn version_mismatch(version: u32) -> Self {
        Self::new(
            "a2a-run-protocol-version",
            format!("unsupported A2A run protocol version {version}"),
            A2ARunFailureKind::VersionMismatch,
            false,
        )
    }
}
