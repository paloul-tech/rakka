//! Stable adapter-local failures mapped to A2A protocol errors.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use a2a::A2AError;
use rakka_agent_workflow::{AgentInboxError, AgentRunEngineError, AgentRunRuntimeError};
use rakka_persistence::DurableError;

use crate::mapping::A2AMappingError;
use crate::push::A2APushConfigError;
use crate::task::TaskProjectionError;

/// Stable adapter-local failures mapped to A2A protocol errors.
///
/// The [`RakkaA2AHandlerError::code`] strings are compatibility commitments:
/// they surface in A2A error messages and bounded adapter metrics labels.
#[derive(Debug, Clone)]
pub enum RakkaA2AHandlerError {
    /// A2A input could not be mapped to a Rakka command.
    Mapping(A2AMappingError),
    /// Task projection read or write failed.
    Projection(TaskProjectionError),
    /// Durable inbox acceptance failed.
    Inbox(AgentInboxError),
    /// Durable run-state transition failed.
    RunEngine(AgentRunEngineError),
    /// Actor-backed owner runtime failed.
    RunActor(AgentRunRuntimeError),
    /// Durable-state store query failed.
    Persistence(DurableError),
    /// Durable push configuration or push outbox scheduling failed.
    Push(A2APushConfigError),
    /// The owning entity or peer was temporarily unavailable.
    Unavailable {
        /// Stable retryable summary.
        message: String,
    },
    /// A stream was rejected by bounded stream limits.
    StreamLimit {
        /// Stable retryable summary.
        message: String,
    },
    /// Local owner actor ask failed before the command reached durable state.
    OwnerAsk {
        /// Stable failure summary.
        message: String,
    },
    /// The requested run was not found before accepting a continuation command.
    MissingRun {
        /// Missing public task id.
        task_id: String,
    },
    /// The task is already in a terminal state and cannot be cancelled.
    TaskNotCancelable {
        /// Public task id.
        task_id: String,
    },
    /// The node is draining and no longer accepts new public commands.
    Draining,
    /// The caller is not authorized for the operation.
    ///
    /// For operations that target an existing task this is surfaced as
    /// task-not-found, so authorization failures stay indistinguishable from
    /// missing tasks.
    NotAuthorized {
        /// Task id when the operation targets one.
        task_id: Option<String>,
    },
    /// The command kind is not valid for the normalized task lifecycle intent.
    InvalidLifecycle {
        /// Public task id.
        task_id: String,
        /// Stable reason.
        reason: &'static str,
    },
}

impl RakkaA2AHandlerError {
    /// Stable machine-readable adapter error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Mapping(error) => error.code(),
            Self::Projection(error) => error.code(),
            Self::Inbox(error) => error.code(),
            Self::RunEngine(error) => error.code(),
            Self::RunActor(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::Push(error) => error.code(),
            Self::Unavailable { .. } => "a2a-run-owner-unavailable",
            Self::StreamLimit { .. } => "a2a-stream-limit",
            Self::OwnerAsk { .. } => "a2a-run-owner-ask",
            Self::MissingRun { .. } => "task-not-found",
            Self::TaskNotCancelable { .. } => "task-not-cancelable",
            Self::Draining => "a2a-agent-draining",
            Self::NotAuthorized { .. } => "not-authorized",
            Self::InvalidLifecycle { .. } => "invalid-command-lifecycle",
        }
    }

    /// Converts this failure into the public A2A protocol error.
    #[must_use]
    pub fn into_a2a_error(self) -> A2AError {
        let code = self.code();
        match self {
            Self::Projection(TaskProjectionError::TaskNotFound { task_id })
            | Self::MissingRun { task_id }
            | Self::NotAuthorized {
                task_id: Some(task_id),
            } => A2AError::task_not_found(&task_id),
            Self::NotAuthorized { task_id: None } => {
                A2AError::invalid_params("not-authorized: operation was denied")
            }
            Self::TaskNotCancelable { task_id } => A2AError::task_not_cancelable(&task_id),
            Self::Mapping(error) => A2AError::invalid_params(format!("{code}: {error}")),
            Self::Projection(error) => A2AError::invalid_params(format!("{code}: {error}")),
            Self::InvalidLifecycle { reason, .. } => {
                A2AError::invalid_params(format!("{code}: {reason}"))
            }
            Self::RunEngine(AgentRunEngineError::MissingRunState { run_id }) => {
                A2AError::task_not_found(run_id.as_str())
            }
            Self::RunActor(AgentRunRuntimeError::RunEngine {
                error: AgentRunEngineError::MissingRunState { run_id },
            }) => A2AError::task_not_found(run_id.as_str()),
            Self::Inbox(error) => A2AError::internal(format!("{code}: {error}")),
            Self::RunEngine(error) => A2AError::internal(format!("{code}: {error}")),
            Self::RunActor(error) => A2AError::internal(format!("{code}: {error}")),
            Self::Persistence(error) => A2AError::internal(format!("{code}: {error}")),
            Self::Push(error) => A2AError::internal(format!("{code}: {error}")),
            Self::Unavailable { message } => A2AError::internal(format!("{code}: {message}")),
            Self::StreamLimit { message } => A2AError::internal(format!("{code}: {message}")),
            Self::OwnerAsk { message } => A2AError::internal(format!("{code}: {message}")),
            Self::Draining => {
                A2AError::internal("a2a-agent-draining: node is draining; retry another endpoint")
            }
        }
    }
}

impl Display for RakkaA2AHandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mapping(error) => Display::fmt(error, f),
            Self::Projection(error) => Display::fmt(error, f),
            Self::Inbox(error) => Display::fmt(error, f),
            Self::RunEngine(error) => Display::fmt(error, f),
            Self::RunActor(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::Push(error) => Display::fmt(error, f),
            Self::Unavailable { message }
            | Self::StreamLimit { message }
            | Self::OwnerAsk { message } => f.write_str(message),
            Self::MissingRun { task_id } => write!(f, "task not found: {task_id}"),
            Self::TaskNotCancelable { task_id } => {
                write!(f, "task {task_id} is terminal and cannot be cancelled")
            }
            Self::Draining => f.write_str("node is draining"),
            Self::NotAuthorized { task_id } => match task_id {
                Some(task_id) => write!(f, "task not found: {task_id}"),
                None => f.write_str("operation was denied"),
            },
            Self::InvalidLifecycle { task_id, reason } => {
                write!(f, "task {task_id} has invalid command lifecycle: {reason}")
            }
        }
    }
}

impl Error for RakkaA2AHandlerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mapping(error) => Some(error),
            Self::Projection(error) => Some(error),
            Self::Inbox(error) => Some(error),
            Self::RunEngine(error) => Some(error),
            Self::RunActor(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::Push(error) => Some(error),
            Self::Unavailable { .. }
            | Self::StreamLimit { .. }
            | Self::OwnerAsk { .. }
            | Self::Draining
            | Self::NotAuthorized { .. }
            | Self::MissingRun { .. }
            | Self::TaskNotCancelable { .. }
            | Self::InvalidLifecycle { .. } => None,
        }
    }
}

impl From<A2AMappingError> for RakkaA2AHandlerError {
    fn from(error: A2AMappingError) -> Self {
        Self::Mapping(error)
    }
}

impl From<TaskProjectionError> for RakkaA2AHandlerError {
    fn from(error: TaskProjectionError) -> Self {
        Self::Projection(error)
    }
}

impl From<AgentInboxError> for RakkaA2AHandlerError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox(error)
    }
}

impl From<AgentRunEngineError> for RakkaA2AHandlerError {
    fn from(error: AgentRunEngineError) -> Self {
        Self::RunEngine(error)
    }
}

impl From<AgentRunRuntimeError> for RakkaA2AHandlerError {
    fn from(error: AgentRunRuntimeError) -> Self {
        Self::RunActor(error)
    }
}

impl From<DurableError> for RakkaA2AHandlerError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

impl From<A2APushConfigError> for RakkaA2AHandlerError {
    fn from(error: A2APushConfigError) -> Self {
        Self::Push(error)
    }
}
