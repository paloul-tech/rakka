//! Typed errors for remote envelopes and serialization.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{MetricsRecorder, RakkaError, Subsystem, METRIC_REMOTE_FAILURES};

/// Convenient result alias for remote operations.
pub type RemoteResult<T> = Result<T, RemoteError>;

/// Remote transport, envelope, or serialization failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteError {
    /// A codec key was already registered.
    DuplicateCodec {
        /// Codec id.
        codec_id: String,
        /// Stable message type id.
        message_type_id: String,
        /// Message schema version.
        schema_version: u32,
    },
    /// A schema compatibility policy was invalid.
    InvalidSchemaCompatibilityPolicy {
        /// Minimum supported schema version.
        min_supported: u32,
        /// Maximum supported schema version.
        max_supported: u32,
        /// Current schema version used for encoding.
        current: u32,
    },
    /// No codec was registered for the requested key.
    UnknownCodec {
        /// Codec id.
        codec_id: String,
        /// Stable message type id.
        message_type_id: String,
        /// Message schema version.
        schema_version: u32,
    },
    /// No default codec was registered for the Rust message type.
    UnknownMessageType {
        /// Rust type name.
        rust_type: &'static str,
    },
    /// The requested Rust type did not match the registered codec type.
    CodecTypeMismatch {
        /// Stable message type id.
        message_type_id: String,
        /// Expected Rust type name.
        expected: &'static str,
    },
    /// Payload encoding failed.
    Encode {
        /// Codec id.
        codec_id: String,
        /// Failure detail.
        message: String,
    },
    /// Payload or envelope decoding failed.
    Decode {
        /// Codec id.
        codec_id: String,
        /// Failure detail.
        message: String,
    },
    /// Envelope was structurally invalid.
    InvalidEnvelope {
        /// Failure detail.
        message: String,
    },
}

impl RemoteError {
    /// Converts this error to a core framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Remote, self.code(), self.to_string())
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DuplicateCodec { .. } => "duplicate-codec",
            Self::InvalidSchemaCompatibilityPolicy { .. } => "invalid-schema-compatibility-policy",
            Self::UnknownCodec { .. } => "unknown-codec",
            Self::UnknownMessageType { .. } => "unknown-message-type",
            Self::CodecTypeMismatch { .. } => "codec-type-mismatch",
            Self::Encode { .. } => "encode-error",
            Self::Decode { .. } => "decode-error",
            Self::InvalidEnvelope { .. } => "invalid-envelope",
        }
    }

    /// Records this remote failure with a stable operation and error label.
    pub fn record_metrics(&self, recorder: &dyn MetricsRecorder, operation: &str) {
        recorder.increment_counter(
            METRIC_REMOTE_FAILURES,
            1,
            &[("operation", operation), ("error", self.code())],
        );
    }
}

impl Display for RemoteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateCodec {
                codec_id,
                message_type_id,
                schema_version,
            } => write!(
                f,
                "codec {codec_id}/{message_type_id}@{schema_version} is already registered"
            ),
            Self::InvalidSchemaCompatibilityPolicy {
                min_supported,
                max_supported,
                current,
            } => write!(
                f,
                "schema compatibility policy {min_supported}..={max_supported} does not support current schema {current}"
            ),
            Self::UnknownCodec {
                codec_id,
                message_type_id,
                schema_version,
            } => write!(
                f,
                "unknown codec {codec_id}/{message_type_id}@{schema_version}"
            ),
            Self::UnknownMessageType { rust_type } => {
                write!(f, "no default remote codec registered for {rust_type}")
            }
            Self::CodecTypeMismatch {
                message_type_id,
                expected,
            } => write!(
                f,
                "remote message {message_type_id} is not registered for Rust type {expected}"
            ),
            Self::Encode { codec_id, message } => {
                write!(f, "{codec_id} payload encode error: {message}")
            }
            Self::Decode { codec_id, message } => {
                write!(f, "{codec_id} payload decode error: {message}")
            }
            Self::InvalidEnvelope { message } => write!(f, "invalid remote envelope: {message}"),
        }
    }
}

impl Error for RemoteError {}
