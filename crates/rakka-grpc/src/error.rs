//! gRPC adapter errors and tonic status mapping.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use rakka_core::{AskError, RakkaError, Subsystem, TellError};
use rakka_sharding::{EntityAskError, EntityDeliveryFailure, EntityTellError, ShardingError};
use rakka_stream::StreamError;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

/// Metadata key that carries a stable Rakka error code on tonic statuses.
pub const RAKKA_GRPC_ERROR_CODE_METADATA: &str = "rakka-error-code";

/// gRPC adapter failure with stable status and error code mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrpcError {
    /// Request payload could not be decoded into the expected protobuf type.
    Decode {
        /// Decode failure detail.
        message: String,
    },
    /// Request payload failed validation after protobuf decoding.
    Validation {
        /// Validation failure detail.
        message: String,
    },
    /// Service handler returned a domain failure.
    Service {
        /// Service failure detail.
        message: String,
    },
    /// Service handler did not finish before the effective timeout.
    ServiceTimeout {
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// Streaming handler or pump did not finish before the effective timeout.
    StreamTimeout {
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// Streaming pump failed outside ordinary stream lifecycle.
    StreamPump {
        /// Pump failure detail.
        message: String,
    },
    /// Stream was configured with an invalid bounded capacity.
    StreamInvalidCapacity {
        /// Rejected capacity.
        capacity: usize,
    },
    /// Stream buffer was full.
    StreamFull {
        /// Configured bounded buffer capacity.
        capacity: usize,
    },
    /// Stream was draining and rejected new work.
    StreamDraining,
    /// Stream closed before the operation completed.
    StreamClosed,
    /// Stream was cancelled.
    StreamCancelled,
    /// RPC was cancelled by the caller.
    Cancelled,
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
    /// Entity route rejected the request.
    EntityRejected {
        /// Rejection detail.
        message: String,
    },
    /// Entity ask timed out.
    EntityTimeout,
    /// Entity dropped the reply channel before replying.
    EntityReplyDropped,
}

impl GrpcError {
    /// Creates a protobuf decode failure.
    #[must_use]
    pub fn decode(message: impl Into<String>) -> Self {
        Self::Decode {
            message: message.into(),
        }
    }

    /// Creates a validation failure.
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// Creates a service failure.
    #[must_use]
    pub fn service(message: impl Into<String>) -> Self {
        Self::Service {
            message: message.into(),
        }
    }

    /// Creates a streaming pump failure.
    #[must_use]
    pub fn stream_pump(message: impl Into<String>) -> Self {
        Self::StreamPump {
            message: message.into(),
        }
    }

    /// Converts this error to a framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Grpc, self.code(), self.to_string())
    }

    /// Converts this error to a tonic status with stable Rakka metadata.
    #[must_use]
    pub fn into_status(self) -> Status {
        let code = self.code();
        let mut status = Status::new(self.grpc_code(), self.to_string());
        status.metadata_mut().insert(
            RAKKA_GRPC_ERROR_CODE_METADATA,
            MetadataValue::try_from(code).expect("Rakka gRPC error codes are valid metadata"),
        );
        status
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Decode { .. } => "decode-error",
            Self::Validation { .. } => "validation-error",
            Self::Service { .. } => "service-error",
            Self::ServiceTimeout { .. } => "service-timeout",
            Self::StreamTimeout { .. } => "stream-timeout",
            Self::StreamPump { .. } => "stream-pump",
            Self::StreamInvalidCapacity { .. } => "stream-invalid-capacity",
            Self::StreamFull { .. } => "stream-full",
            Self::StreamDraining => "stream-draining",
            Self::StreamClosed => "stream-closed",
            Self::StreamCancelled => "stream-cancelled",
            Self::Cancelled => "cancelled",
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
            Self::EntityRejected { .. } => "entity-rejected",
            Self::EntityTimeout => "entity-timeout",
            Self::EntityReplyDropped => "entity-reply-dropped",
        }
    }

    /// Tonic status code used for this error.
    #[must_use]
    pub const fn grpc_code(&self) -> Code {
        match self {
            Self::Decode { .. } | Self::Validation { .. } => Code::InvalidArgument,
            Self::ServiceTimeout { .. }
            | Self::StreamTimeout { .. }
            | Self::ActorTimeout
            | Self::EntityTimeout => Code::DeadlineExceeded,
            Self::Cancelled | Self::StreamCancelled => Code::Cancelled,
            Self::ActorMailboxFull | Self::EntityMailboxFull | Self::StreamFull { .. } => {
                Code::ResourceExhausted
            }
            Self::ActorMailboxClosed
            | Self::ActorReplyDropped
            | Self::StreamDraining
            | Self::StreamClosed
            | Self::EntityNoRoute { .. }
            | Self::EntityMailboxClosed
            | Self::EntityNotLocal { .. }
            | Self::EntityRemoteSend { .. }
            | Self::EntityShardHandoff { .. }
            | Self::EntityReplyDropped => Code::Unavailable,
            Self::EntityRejected { .. } => Code::FailedPrecondition,
            Self::Service { .. }
            | Self::StreamPump { .. }
            | Self::StreamInvalidCapacity { .. }
            | Self::EntitySpawnFailed { .. }
            | Self::EntityRemoteEncode { .. } => Code::Internal,
        }
    }

    /// Converts an actor ask failure to a gRPC adapter error.
    #[must_use]
    pub const fn from_actor_ask(error: AskError) -> Self {
        match error {
            AskError::MailboxFull => Self::ActorMailboxFull,
            AskError::MailboxClosed => Self::ActorMailboxClosed,
            AskError::Timeout => Self::ActorTimeout,
            AskError::ReplyDropped => Self::ActorReplyDropped,
        }
    }

    /// Converts an actor tell failure to a gRPC adapter error.
    #[must_use]
    pub fn from_actor_tell<M>(error: TellError<M>) -> Self {
        match error {
            TellError::Full(_) => Self::ActorMailboxFull,
            TellError::Closed(_) => Self::ActorMailboxClosed,
        }
    }

    /// Converts an entity ask failure to a gRPC adapter error.
    #[must_use]
    pub fn from_entity_ask(error: EntityAskError) -> Self {
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
            EntityAskError::Rejected(message) => Self::EntityRejected { message },
            EntityAskError::Timeout => Self::EntityTimeout,
            EntityAskError::ReplyDropped => Self::EntityReplyDropped,
        }
    }

    /// Converts an entity tell failure to a gRPC adapter error.
    #[must_use]
    pub fn from_entity_tell<M>(error: EntityTellError<M>) -> Self {
        match error {
            EntityTellError::NoRoute { error, .. } => Self::from_sharding_error(error),
            EntityTellError::Delivery { failure, .. } => Self::from_entity_delivery(failure),
        }
    }

    /// Converts a bounded Rakka stream failure to a gRPC adapter error.
    #[must_use]
    pub fn from_stream_error(error: StreamError) -> Self {
        match error {
            StreamError::InvalidCapacity { capacity } => Self::StreamInvalidCapacity { capacity },
            StreamError::Full { capacity } => Self::StreamFull { capacity },
            StreamError::Draining => Self::StreamDraining,
            StreamError::Closed => Self::StreamClosed,
            StreamError::Cancelled { .. } => Self::StreamCancelled,
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
            EntityDeliveryFailure::Rejected(message) => Self::EntityRejected { message },
        }
    }
}

impl Display for GrpcError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { message } => write!(f, "gRPC request decode failed: {message}"),
            Self::Validation { message } => write!(f, "gRPC request validation failed: {message}"),
            Self::Service { message } => write!(f, "gRPC service handler failed: {message}"),
            Self::ServiceTimeout { timeout } => {
                write!(f, "gRPC service handler timed out after {timeout:?}")
            }
            Self::StreamTimeout { timeout } => {
                write!(f, "gRPC stream timed out after {timeout:?}")
            }
            Self::StreamPump { message } => write!(f, "gRPC stream pump failed: {message}"),
            Self::StreamInvalidCapacity { capacity } => {
                write!(
                    f,
                    "gRPC stream capacity must be greater than zero: {capacity}"
                )
            }
            Self::StreamFull { capacity } => {
                write!(f, "gRPC stream buffer is full at capacity {capacity}")
            }
            Self::StreamDraining => f.write_str("gRPC stream is draining"),
            Self::StreamClosed => f.write_str("gRPC stream is closed"),
            Self::StreamCancelled => f.write_str("gRPC stream was cancelled"),
            Self::Cancelled => f.write_str("gRPC request was cancelled"),
            Self::ActorMailboxFull => f.write_str("actor mailbox was full"),
            Self::ActorMailboxClosed => f.write_str("actor mailbox was closed"),
            Self::ActorTimeout => f.write_str("actor ask timed out"),
            Self::ActorReplyDropped => f.write_str("actor ask reply channel was dropped"),
            Self::EntityNoRoute { message } => write!(f, "entity route was unavailable: {message}"),
            Self::EntityMailboxFull => f.write_str("entity route mailbox was full"),
            Self::EntityMailboxClosed => f.write_str("entity route mailbox was closed"),
            Self::EntityNotLocal { owner } => {
                write!(f, "entity shard is owned by remote node {owner}")
            }
            Self::EntitySpawnFailed { message } => {
                write!(f, "entity actor spawn failed: {message}")
            }
            Self::EntityRemoteEncode { message } => {
                write!(f, "remote entity encode failed: {message}")
            }
            Self::EntityRemoteSend { message } => {
                write!(f, "remote entity send failed: {message}")
            }
            Self::EntityShardHandoff { shard_id, state } => {
                write!(f, "shard {shard_id} is {state} during graceful handoff")
            }
            Self::EntityRejected { message } => {
                write!(f, "entity route rejected request: {message}")
            }
            Self::EntityTimeout => f.write_str("entity ask timed out"),
            Self::EntityReplyDropped => f.write_str("entity ask reply channel was dropped"),
        }
    }
}

impl Error for GrpcError {}

impl From<GrpcError> for Status {
    fn from(error: GrpcError) -> Self {
        error.into_status()
    }
}

/// Creates a tonic `invalid_argument` status for protobuf decode failures.
#[must_use]
pub fn decode_status(message: impl Into<String>) -> Status {
    GrpcError::decode(message).into_status()
}

/// Creates a tonic `invalid_argument` status for protobuf validation failures.
#[must_use]
pub fn validation_status(message: impl Into<String>) -> Status {
    GrpcError::validation(message).into_status()
}

/// Creates a tonic status for bounded stream failures.
#[must_use]
pub fn stream_status(error: StreamError) -> Status {
    GrpcError::from_stream_error(error).into_status()
}

/// Creates a tonic `internal` status for service handler failures.
#[must_use]
pub fn service_status(message: impl Into<String>) -> Status {
    GrpcError::service(message).into_status()
}
