//! Durable state error types.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{RakkaError, Subsystem};

use crate::store::{PersistenceId, Revision};

/// Convenient result alias for durable state operations.
pub type DurableResult<T> = Result<T, DurableError>;

/// Durable state operation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableError {
    /// Store revision differed from the caller's expected revision.
    RevisionConflict {
        /// Durable identity being written.
        persistence_id: PersistenceId,
        /// Revision expected by the caller.
        expected: Revision,
        /// Revision observed in the store.
        actual: Revision,
    },
    /// Underlying store failed or was unavailable.
    Store {
        /// Store backend name.
        backend: &'static str,
        /// Human-readable failure detail.
        message: String,
    },
    /// State encoding or decoding failed.
    Codec {
        /// Human-readable failure detail.
        message: String,
    },
    /// Durable actor received a command before recovery completed.
    NotRecovered {
        /// Durable identity that was not recovered.
        persistence_id: PersistenceId,
    },
}

impl DurableError {
    /// Creates a revision conflict error.
    #[must_use]
    pub fn revision_conflict(
        persistence_id: PersistenceId,
        expected: Revision,
        actual: Revision,
    ) -> Self {
        Self::RevisionConflict {
            persistence_id,
            expected,
            actual,
        }
    }

    /// Creates a store error.
    #[must_use]
    pub fn store(backend: &'static str, message: impl Into<String>) -> Self {
        Self::Store {
            backend,
            message: message.into(),
        }
    }

    /// Creates a codec error.
    #[must_use]
    pub fn codec(message: impl Into<String>) -> Self {
        Self::Codec {
            message: message.into(),
        }
    }

    /// Converts this error to a core framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Persistence, self.code(), self.to_string())
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::RevisionConflict { .. } => "revision-conflict",
            Self::Store { .. } => "store-error",
            Self::Codec { .. } => "codec-error",
            Self::NotRecovered { .. } => "not-recovered",
        }
    }
}

impl Display for DurableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RevisionConflict {
                persistence_id,
                expected,
                actual,
            } => write!(
                f,
                "revision conflict for {persistence_id}: expected {expected}, actual {actual}"
            ),
            Self::Store { backend, message } => write!(f, "{backend} store error: {message}"),
            Self::Codec { message } => write!(f, "state codec error: {message}"),
            Self::NotRecovered { persistence_id } => {
                write!(f, "durable actor {persistence_id} has not recovered")
            }
        }
    }
}

impl Error for DurableError {}
