//! Workflow reliability error types.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{RakkaError, Subsystem};
use rakka_persistence::{DurableError, Revision};

use crate::{WorkflowId, WorkflowMessageId};

/// Convenient result alias for workflow operations.
pub type WorkflowResult<T> = Result<T, WorkflowError>;

/// Durable workflow operation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowError {
    /// Durable inbox was used before recovery.
    NotRecovered {
        /// Workflow id.
        workflow_id: WorkflowId,
    },
    /// Store revision differed from the caller's expected revision.
    RevisionConflict {
        /// Workflow id.
        workflow_id: WorkflowId,
        /// Revision expected by the caller.
        expected: Revision,
        /// Revision observed in the store.
        actual: Revision,
    },
    /// Underlying persistence store failed.
    Persistence {
        /// Persistence failure.
        error: DurableError,
    },
    /// Inbox entry was not found.
    InboxEntryNotFound {
        /// Workflow id.
        workflow_id: WorkflowId,
        /// Message id.
        message_id: WorkflowMessageId,
    },
}

impl WorkflowError {
    /// Converts this error to a framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Workflow, self.code(), self.to_string())
    }

    fn code(&self) -> &'static str {
        match self {
            Self::NotRecovered { .. } => "not-recovered",
            Self::RevisionConflict { .. } => "revision-conflict",
            Self::Persistence { .. } => "persistence-error",
            Self::InboxEntryNotFound { .. } => "inbox-entry-not-found",
        }
    }
}

impl Display for WorkflowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRecovered { workflow_id } => {
                write!(f, "workflow {workflow_id} has not recovered")
            }
            Self::RevisionConflict {
                workflow_id,
                expected,
                actual,
            } => write!(
                f,
                "workflow {workflow_id} revision conflict: expected {expected}, actual {actual}"
            ),
            Self::Persistence { error } => Display::fmt(error, f),
            Self::InboxEntryNotFound {
                workflow_id,
                message_id,
            } => write!(
                f,
                "workflow {workflow_id} inbox entry {message_id} was not found"
            ),
        }
    }
}

impl Error for WorkflowError {}

pub(crate) fn map_durable_error(workflow_id: &WorkflowId, error: DurableError) -> WorkflowError {
    match error {
        DurableError::RevisionConflict {
            expected, actual, ..
        } => WorkflowError::RevisionConflict {
            workflow_id: workflow_id.clone(),
            expected,
            actual,
        },
        error => WorkflowError::Persistence { error },
    }
}
