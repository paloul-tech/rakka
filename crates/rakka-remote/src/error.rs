//! Typed errors for remote envelopes and serialization.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{RakkaError, Subsystem};

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

    fn code(&self) -> &'static str {
        match self {
            Self::DuplicateCodec { .. } => "duplicate-codec",
            Self::UnknownCodec { .. } => "unknown-codec",
            Self::UnknownMessageType { .. } => "unknown-message-type",
            Self::CodecTypeMismatch { .. } => "codec-type-mismatch",
            Self::Encode { .. } => "encode-error",
            Self::Decode { .. } => "decode-error",
            Self::InvalidEnvelope { .. } => "invalid-envelope",
        }
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
