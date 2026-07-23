//! Error surface of the typed agent A2A adaptation.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent::{AgentEntityError, AgentIdentityError, AgentRunError, AgentTaskError};

use crate::mapping::A2AMappingError;
use crate::task::TaskProjectionError;

/// Result alias for the agents surface.
pub type RakkaAgentA2AResult<T> = Result<T, RakkaAgentA2AError>;

/// One failure of the typed agent A2A surface.
///
/// Every variant carries a stable machine-readable [`code`](Self::code);
/// codes are part of the compatibility surface like the mapping and entity
/// codes they wrap.
#[derive(Debug)]
#[non_exhaustive]
pub enum RakkaAgentA2AError {
    /// Request normalization failed before any durable acceptance.
    Mapping(A2AMappingError),
    /// An identity segment could not key a durable scope.
    Identity(AgentIdentityError),
    /// The task entity refused or failed the command.
    Task(AgentTaskError),
    /// The agent entity refused or failed the command.
    Entity(AgentEntityError),
    /// Reading run state for the projection failed.
    Run(AgentRunError),
    /// The public projection read model failed.
    Projection(TaskProjectionError),
    /// The entity refused the command with a stable domain code.
    Refused {
        /// Stable refusal code from the entity reply.
        code: String,
        /// Bounded refusal message.
        message: String,
    },
    /// The authorizer denied the operation.
    Unauthorized,
    /// No hosted agent target matched the request's selection.
    UnknownAgent {
        /// Requested agent id, when one was named.
        agent: Option<String>,
        /// Requested task-definition id, when one was named.
        task_definition: Option<String>,
    },
    /// The referenced public task does not exist in this tenant.
    TaskNotFound {
        /// Public task id, equal to the `AgentTaskId` value.
        task_id: String,
    },
    /// The operation is defined by the specification but not served yet.
    Unsupported {
        /// Bounded operation label.
        operation: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
}

impl RakkaAgentA2AError {
    /// Stable machine-readable code for A2A error payloads and bounded
    /// metrics labels.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Mapping(error) => error.code(),
            Self::Identity(_) => "invalid-identity",
            Self::Task(error) => error.code(),
            Self::Entity(error) => error.code(),
            Self::Run(error) => error.code(),
            Self::Projection(_) => "projection",
            Self::Refused { .. } => "refused",
            Self::Unauthorized => "unauthorized",
            Self::UnknownAgent { .. } => "unknown-agent",
            Self::TaskNotFound { .. } => "task-not-found",
            Self::Unsupported { .. } => "unsupported-operation",
        }
    }
}

impl Display for RakkaAgentA2AError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mapping(error) => Display::fmt(error, f),
            Self::Identity(error) => Display::fmt(error, f),
            Self::Task(error) => Display::fmt(error, f),
            Self::Entity(error) => Display::fmt(error, f),
            Self::Run(error) => Display::fmt(error, f),
            Self::Projection(error) => Display::fmt(error, f),
            Self::Refused { code, message } => write!(f, "refused ({code}): {message}"),
            Self::Unauthorized => write!(f, "the operation was not authorized"),
            Self::UnknownAgent {
                agent,
                task_definition,
            } => write!(
                f,
                "no hosted agent target matches agent {agent:?} / task definition {task_definition:?}"
            ),
            Self::TaskNotFound { task_id } => write!(f, "task {task_id} does not exist"),
            Self::Unsupported { operation, reason } => {
                write!(f, "{operation} is not supported: {reason}")
            }
        }
    }
}

impl Error for RakkaAgentA2AError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mapping(error) => Some(error),
            Self::Identity(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::Entity(error) => Some(error),
            Self::Run(error) => Some(error),
            Self::Projection(error) => Some(error),
            _ => None,
        }
    }
}

impl From<A2AMappingError> for RakkaAgentA2AError {
    fn from(error: A2AMappingError) -> Self {
        Self::Mapping(error)
    }
}

impl From<AgentIdentityError> for RakkaAgentA2AError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentTaskError> for RakkaAgentA2AError {
    fn from(error: AgentTaskError) -> Self {
        Self::Task(error)
    }
}

impl From<AgentEntityError> for RakkaAgentA2AError {
    fn from(error: AgentEntityError) -> Self {
        Self::Entity(error)
    }
}

impl From<AgentRunError> for RakkaAgentA2AError {
    fn from(error: AgentRunError) -> Self {
        Self::Run(error)
    }
}

impl From<TaskProjectionError> for RakkaAgentA2AError {
    fn from(error: TaskProjectionError) -> Self {
        Self::Projection(error)
    }
}
