//! HTTP adapter error types and status mapping.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rakka_core::{AskError, RakkaError, Subsystem, TellError};
use rakka_sharding::{EntityAskError, EntityDeliveryFailure, EntityTellError, ShardingError};
use serde::Serialize;

/// Convenient result alias for HTTP integration operations.
pub type HttpResult<T> = Result<T, HttpError>;

/// HTTP adapter failure with stable status and error code mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpError {
    /// Request body exceeded the configured payload limit.
    PayloadTooLarge {
        /// Configured payload limit in bytes.
        limit: usize,
    },
    /// Request body could not be read.
    BodyRead {
        /// Body read failure detail.
        message: String,
    },
    /// JSON request payload could not be decoded.
    JsonDecode {
        /// JSON decode failure detail.
        message: String,
    },
    /// JSON response payload could not be encoded.
    JsonEncode {
        /// JSON encode failure detail.
        message: String,
    },
    /// Service handler returned a domain failure.
    Service {
        /// Service failure detail.
        message: String,
    },
    /// Service handler did not finish before the configured timeout.
    ServiceTimeout {
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// A Rakka stream ended with an error while backing an HTTP stream.
    Stream {
        /// Stream failure detail.
        message: String,
    },
    /// HTTP streaming stopped because the client disconnected.
    ClientDisconnected,
    /// WebSocket bridge failed.
    WebSocket {
        /// WebSocket failure detail.
        message: String,
    },
    /// Actor mailbox was full.
    ActorMailboxFull,
    /// Actor mailbox was closed.
    ActorMailboxClosed,
    /// Actor ask timed out.
    ActorTimeout,
    /// Actor dropped the reply channel before replying.
    ActorReplyDropped,
    /// Entity owner could not be resolved.
    EntityNoRoute {
        /// Routing failure detail.
        message: String,
    },
    /// Entity route mailbox was full.
    EntityMailboxFull,
    /// Entity route mailbox was closed.
    EntityMailboxClosed,
    /// Entity shard is owned by another node.
    EntityNotLocal {
        /// Current shard owner.
        owner: String,
    },
    /// Entity actor failed to spawn.
    EntitySpawnFailed {
        /// Spawn failure detail.
        message: String,
    },
    /// Entity message failed remote encoding.
    EntityRemoteEncode {
        /// Remote encode failure detail.
        message: String,
    },
    /// Entity message failed remote send.
    EntityRemoteSend {
        /// Remote send failure detail.
        message: String,
    },
    /// Entity shard is temporarily unavailable during handoff.
    EntityShardHandoff {
        /// Shard id being handed off.
        shard_id: String,
        /// Current handoff state.
        state: String,
    },
    /// Entity shard movement buffer is full.
    EntityShardBufferFull {
        /// Shard id whose buffer was full.
        shard_id: String,
        /// Configured capacity per shard.
        capacity: usize,
    },
    /// Entity route rejected the request.
    EntityRejected {
        /// Rejection detail.
        message: String,
    },
    /// Entity ask timed out.
    EntityTimeout,
    /// Entity dropped the reply channel before replying.
    EntityReplyDropped,
    /// TCP listener could not bind.
    Bind {
        /// Address that failed to bind.
        address: SocketAddr,
        /// Bind failure detail.
        message: String,
    },
    /// HTTP server returned an error.
    Serve {
        /// Server failure detail.
        message: String,
    },
}

impl HttpError {
    /// Creates a service failure.
    #[must_use]
    pub fn service(message: impl Into<String>) -> Self {
        Self::Service {
            message: message.into(),
        }
    }

    /// Converts this error to a framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Http, self.code(), self.to_string())
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::PayloadTooLarge { .. } => "payload-too-large",
            Self::BodyRead { .. } => "body-read",
            Self::JsonDecode { .. } => "json-decode",
            Self::JsonEncode { .. } => "json-encode",
            Self::Service { .. } => "service-error",
            Self::ServiceTimeout { .. } => "service-timeout",
            Self::Stream { .. } => "stream-error",
            Self::ClientDisconnected => "client-disconnected",
            Self::WebSocket { .. } => "websocket-error",
            Self::ActorMailboxFull => "actor-mailbox-full",
            Self::ActorMailboxClosed => "actor-mailbox-closed",
            Self::ActorTimeout => "actor-timeout",
            Self::ActorReplyDropped => "actor-reply-dropped",
            Self::EntityNoRoute { .. } => "entity-no-route",
            Self::EntityMailboxFull => "entity-mailbox-full",
            Self::EntityMailboxClosed => "entity-mailbox-closed",
            Self::EntityNotLocal { .. } => "entity-not-local",
            Self::EntitySpawnFailed { .. } => "entity-spawn-failed",
            Self::EntityRemoteEncode { .. } => "entity-remote-encode",
            Self::EntityRemoteSend { .. } => "entity-remote-send",
            Self::EntityShardHandoff { .. } => "entity-shard-handoff",
            Self::EntityShardBufferFull { .. } => "entity-shard-buffer-full",
            Self::EntityRejected { .. } => "entity-rejected",
            Self::EntityTimeout => "entity-timeout",
            Self::EntityReplyDropped => "entity-reply-dropped",
            Self::Bind { .. } => "bind-error",
            Self::Serve { .. } => "serve-error",
        }
    }

    /// HTTP status code used for this error.
    #[must_use]
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::JsonDecode { .. } | Self::BodyRead { .. } => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ActorTimeout | Self::EntityTimeout | Self::ServiceTimeout { .. } => {
                StatusCode::GATEWAY_TIMEOUT
            }
            Self::ClientDisconnected => StatusCode::BAD_REQUEST,
            Self::ActorMailboxFull
            | Self::ActorMailboxClosed
            | Self::Stream { .. }
            | Self::EntityNoRoute { .. }
            | Self::EntityMailboxFull
            | Self::EntityMailboxClosed
            | Self::EntityNotLocal { .. }
            | Self::EntityShardHandoff { .. }
            | Self::EntityShardBufferFull { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::ActorReplyDropped | Self::EntityReplyDropped => StatusCode::BAD_GATEWAY,
            Self::EntityRejected { .. } => StatusCode::CONFLICT,
            Self::JsonEncode { .. }
            | Self::Service { .. }
            | Self::WebSocket { .. }
            | Self::EntitySpawnFailed { .. }
            | Self::EntityRemoteEncode { .. }
            | Self::EntityRemoteSend { .. }
            | Self::Bind { .. }
            | Self::Serve { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn from_actor_ask(error: AskError) -> Self {
        match error {
            AskError::MailboxFull => Self::ActorMailboxFull,
            AskError::MailboxClosed => Self::ActorMailboxClosed,
            AskError::Timeout => Self::ActorTimeout,
            AskError::ReplyDropped => Self::ActorReplyDropped,
        }
    }

    pub(crate) fn from_stream_error(error: rakka_stream::StreamError) -> Self {
        Self::Stream {
            message: error.to_string(),
        }
    }

    pub(crate) fn from_actor_tell<M>(error: TellError<M>) -> Self {
        match error {
            TellError::Full(_) => Self::ActorMailboxFull,
            TellError::Closed(_) => Self::ActorMailboxClosed,
        }
    }

    pub(crate) fn from_entity_ask(error: EntityAskError) -> Self {
        match error {
            EntityAskError::NoRoute(error) => Self::from_sharding_error(error),
            EntityAskError::MailboxFull => Self::EntityMailboxFull,
            EntityAskError::MailboxClosed => Self::EntityMailboxClosed,
            EntityAskError::NotLocal { owner } => Self::EntityNotLocal {
                owner: owner.to_string(),
            },
            EntityAskError::SpawnFailed(message) => Self::EntitySpawnFailed { message },
            EntityAskError::RemoteEncode(message) => Self::EntityRemoteEncode { message },
            EntityAskError::RemoteSend(message) => Self::EntityRemoteSend { message },
            EntityAskError::ShardHandoff { shard_id, state } => Self::EntityShardHandoff {
                shard_id: shard_id.to_string(),
                state: state.to_string(),
            },
            EntityAskError::ShardBufferFull { shard_id, capacity } => Self::EntityShardBufferFull {
                shard_id: shard_id.to_string(),
                capacity,
            },
            EntityAskError::Rejected(message) => Self::EntityRejected { message },
            EntityAskError::Timeout => Self::EntityTimeout,
            EntityAskError::ReplyDropped => Self::EntityReplyDropped,
        }
    }

    pub(crate) fn from_entity_tell<M>(error: EntityTellError<M>) -> Self {
        match error {
            EntityTellError::NoRoute { error, .. } => Self::from_sharding_error(error),
            EntityTellError::Delivery { failure, .. } => Self::from_entity_delivery(failure),
        }
    }

    fn from_sharding_error(error: ShardingError) -> Self {
        Self::EntityNoRoute {
            message: error.to_string(),
        }
    }

    fn from_entity_delivery(failure: EntityDeliveryFailure) -> Self {
        match failure {
            EntityDeliveryFailure::MailboxFull => Self::EntityMailboxFull,
            EntityDeliveryFailure::MailboxClosed => Self::EntityMailboxClosed,
            EntityDeliveryFailure::NotLocal { owner } => Self::EntityNotLocal {
                owner: owner.to_string(),
            },
            EntityDeliveryFailure::SpawnFailed(message) => Self::EntitySpawnFailed { message },
            EntityDeliveryFailure::RemoteEncode(message) => Self::EntityRemoteEncode { message },
            EntityDeliveryFailure::RemoteSend(message) => Self::EntityRemoteSend { message },
            EntityDeliveryFailure::ShardHandoff { shard_id, state } => Self::EntityShardHandoff {
                shard_id: shard_id.to_string(),
                state: state.to_string(),
            },
            EntityDeliveryFailure::ShardBufferFull { shard_id, capacity } => {
                Self::EntityShardBufferFull {
                    shard_id: shard_id.to_string(),
                    capacity,
                }
            }
            EntityDeliveryFailure::Rejected(message) => Self::EntityRejected { message },
        }
    }
}

impl Display for HttpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { limit } => {
                write!(f, "request payload exceeded {limit} byte limit")
            }
            Self::BodyRead { message } => write!(f, "failed to read request body: {message}"),
            Self::JsonDecode { message } => write!(f, "failed to decode JSON request: {message}"),
            Self::JsonEncode { message } => write!(f, "failed to encode JSON response: {message}"),
            Self::Service { message } => write!(f, "service handler failed: {message}"),
            Self::ServiceTimeout { timeout } => {
                write!(f, "service handler timed out after {timeout:?}")
            }
            Self::Stream { message } => write!(f, "HTTP stream failed: {message}"),
            Self::ClientDisconnected => f.write_str("HTTP client disconnected"),
            Self::WebSocket { message } => write!(f, "WebSocket bridge failed: {message}"),
            Self::ActorMailboxFull => f.write_str("actor mailbox was full"),
            Self::ActorMailboxClosed => f.write_str("actor mailbox was closed"),
            Self::ActorTimeout => f.write_str("actor ask timed out"),
            Self::ActorReplyDropped => f.write_str("actor reply channel was dropped"),
            Self::EntityNoRoute { message } => write!(f, "entity route was unavailable: {message}"),
            Self::EntityMailboxFull => f.write_str("entity route mailbox was full"),
            Self::EntityMailboxClosed => f.write_str("entity route mailbox was closed"),
            Self::EntityNotLocal { owner } => write!(f, "entity shard is owned by {owner}"),
            Self::EntitySpawnFailed { message } => write!(f, "entity spawn failed: {message}"),
            Self::EntityRemoteEncode { message } => {
                write!(f, "remote entity encode failed: {message}")
            }
            Self::EntityRemoteSend { message } => write!(f, "remote entity send failed: {message}"),
            Self::EntityShardHandoff { shard_id, state } => {
                write!(f, "entity shard {shard_id} is {state}")
            }
            Self::EntityShardBufferFull { shard_id, capacity } => {
                write!(
                    f,
                    "entity shard {shard_id} buffer is full at capacity {capacity}"
                )
            }
            Self::EntityRejected { message } => {
                write!(f, "entity route rejected request: {message}")
            }
            Self::EntityTimeout => f.write_str("entity ask timed out"),
            Self::EntityReplyDropped => f.write_str("entity reply channel was dropped"),
            Self::Bind { address, message } => {
                write!(f, "failed to bind HTTP server at {address}: {message}")
            }
            Self::Serve { message } => write!(f, "HTTP server failed: {message}"),
        }
    }
}

impl Error for HttpError {}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = HttpErrorBody {
            code: self.code(),
            message: self.to_string(),
        };
        (status, axum::Json(body)).into_response()
    }
}

/// JSON error response body emitted by route adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HttpErrorBody {
    /// Stable machine-readable error code.
    pub code: &'static str,
    /// Human-readable error detail.
    pub message: String,
}
