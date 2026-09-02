//! Remote-safe protocol between public A2A ingress nodes and sharded run
//! owners.
//!
//! These payloads are the only values this crate serializes over Rakka
//! remoting. They intentionally exclude process-local values such as
//! `ReplyTo`, actor refs, store handles, and `Arc`. The protocol version,
//! remote schema version, and message type ids below are compatibility
//! commitments across rolling updates.

use std::collections::BTreeMap;
use std::time::Duration;

use a2a::{AgentInterface, Message, TaskPushNotificationConfig};
use rakka_agent_workflow::{AgentTimestampMillis, ArtifactRef};
use serde::{Deserialize, Serialize};

use crate::mapping::{A2ACommandDraft, A2ATaskIntent};
use crate::task::{A2ATaskEvent, A2ATaskProjection};

/// The A2A wire protocol version this adapter advertises and is reviewed
/// against.
///
/// It is stamped explicitly on every `AgentInterface` of the card the adapter
/// builds, so the version a client reads is the one Rakka pinned rather than
/// whatever the SDK's constructor defaulted to. It is distinct from
/// [`A2A_RUN_PROTOCOL_VERSION`], which versions Rakka's own owner protocol
/// between ingress nodes and sharded owners and never leaves the cluster.
///
/// Held equal to the SDK's `a2a::VERSION` by a unit test rather than aliasing
/// it: an SDK upgrade that moves the wire version fails that test, which is
/// the specification 20 review point — a bridged or retained older surface
/// needs a documented compatibility matrix and explicit negotiation, never
/// accidental mixed-version behavior. The pin is recorded in
/// `docs/rakka-compatibility.md`.
pub const A2A_PROTOCOL_VERSION: &str = "1.0";

/// An agent-card interface carrying [`A2A_PROTOCOL_VERSION`] rather than the
/// version the SDK's constructor defaults to. The two agree today and a test
/// holds them equal; stamping it explicitly is what makes the card advertise
/// Rakka's reviewed version rather than inherit one. Every interface the
/// adapter's card builder produces, and the test fixture card, go through it.
#[must_use]
pub fn pinned_interface(url: impl Into<String>, transport: impl Into<String>) -> AgentInterface {
    let mut interface = AgentInterface::new(url, transport);
    interface.protocol_version = A2A_PROTOCOL_VERSION.to_string();
    interface
}

/// Current adapter-owned inter-node protocol version.
pub const A2A_RUN_PROTOCOL_VERSION: u32 = 1;

/// Current remoting schema version registered in `SerializationRegistry`.
pub const A2A_RUN_REMOTE_SCHEMA_VERSION: u32 = 1;

/// Stable remote message type id for owner requests.
pub const A2A_RUN_REQUEST_TYPE_ID: &str = "rakka.a2a.A2ARunRequest";

/// Stable remote message type id for owner responses.
pub const A2A_RUN_RESPONSE_TYPE_ID: &str = "rakka.a2a.A2ARunResponse";

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
    /// single-tenant projection store's unscoped read semantics. This is a
    /// single-tenant/local-mode affordance only; tenant-scoped services
    /// never issue unscoped reads. Durable commands (accept, cancel) always
    /// carry `Some` canonical tenant.
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
        /// Request-supplied push config to persist before emitting task events.
        request_push_config: Option<TaskPushNotificationConfig>,
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
    /// Converge the owner projection and replay public task events after a
    /// cursor, so ingress nodes can serve live streams for owner-held tasks.
    OpenStreamCursor {
        /// Replay cursor supplied by the client, if any.
        after_cursor: Option<String>,
    },
    /// Record a push notification config through the owner.
    RecordPushConfig {
        /// Public push config to store durably.
        config: TaskPushNotificationConfig,
    },
    /// Delete a push notification config through the owner.
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

    /// Successful stream cursor replay response.
    #[must_use]
    pub fn stream_cursor(
        task_id: impl Into<String>,
        tenant: impl Into<String>,
        projection: A2ATaskProjection,
        events: Vec<A2ATaskEvent>,
        resync: bool,
    ) -> Self {
        Self {
            version: A2A_RUN_PROTOCOL_VERSION,
            task_id: task_id.into(),
            tenant: tenant.into(),
            outcome: A2ARunResponseKind::StreamCursor {
                projection,
                events,
                resync,
            },
        }
    }

    /// Successful push config record response.
    #[must_use]
    pub fn push_config_recorded(
        task_id: impl Into<String>,
        tenant: impl Into<String>,
        config: TaskPushNotificationConfig,
    ) -> Self {
        Self {
            version: A2A_RUN_PROTOCOL_VERSION,
            task_id: task_id.into(),
            tenant: tenant.into(),
            outcome: A2ARunResponseKind::PushConfigRecorded { config },
        }
    }

    /// Successful push config delete response.
    #[must_use]
    pub fn push_config_deleted(task_id: impl Into<String>, tenant: impl Into<String>) -> Self {
        Self {
            version: A2A_RUN_PROTOCOL_VERSION,
            task_id: task_id.into(),
            tenant: tenant.into(),
            outcome: A2ARunResponseKind::PushConfigDeleted,
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
    /// Owner-side replay served for a stream subscriber.
    StreamCursor {
        /// Current public task projection on the owner.
        projection: A2ATaskProjection,
        /// Public events after the requested cursor, in sequence order.
        events: Vec<A2ATaskEvent>,
        /// True when the cursor could not be honored and the subscriber must
        /// re-bootstrap from the projection snapshot.
        resync: bool,
    },
    /// Push config recorded through the owner.
    PushConfigRecorded {
        /// Stored push notification config.
        config: TaskPushNotificationConfig,
    },
    /// Push config deleted through the owner.
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
    /// Operation is not supported by this owner.
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

    /// Failure for an operation the owner does not support.
    #[must_use]
    pub fn unsupported(operation: &str) -> Self {
        Self::new(
            "a2a-run-unsupported-operation",
            format!("unsupported A2A run owner operation {operation}"),
            A2ARunFailureKind::Unsupported,
            false,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::A2A_PROTOCOL_VERSION;

    #[test]
    fn the_advertised_protocol_version_is_the_sdk_s() {
        assert_eq!(
            A2A_PROTOCOL_VERSION,
            a2a::VERSION,
            "the SDK implements a different A2A wire version than the one this adapter \
             pins; this is the specification 20 review, not a constant to update"
        );
    }

    #[test]
    fn the_compatibility_document_pins_the_advertised_protocol_version() {
        // Read at runtime rather than `include_str!`: this crate is in the
        // publishable set, and a packaged crate carries no `docs/`.
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/rakka-compatibility.md");
        let document = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let row = format!("| `A2A protocol` | `{A2A_PROTOCOL_VERSION}` |");
        assert!(
            document.contains(&row),
            "docs/rakka-compatibility.md must carry the pinned dependency row {row:?}"
        );
    }
}
