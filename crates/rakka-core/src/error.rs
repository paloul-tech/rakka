//! Shared error conventions for Rakka crates.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

/// Convenient result alias used across Rakka crates.
pub type RakkaResult<T> = Result<T, RakkaError>;

/// Logical subsystem that produced an error or telemetry event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Subsystem {
    /// Core actor runtime and shared primitives.
    Core,
    /// Durable state API.
    Persistence,
    /// PostgreSQL durable state plugin.
    PersistencePostgres,
    /// Remote transport and serialization.
    Remote,
    /// Cluster membership and node lifecycle.
    Cluster,
    /// Cluster sharding and entity routing.
    Sharding,
    /// Durable workflow reliability patterns.
    Workflow,
    /// Bounded stream adapters.
    Stream,
    /// External child-process actors.
    Process,
    /// HTTP integration.
    Http,
    /// gRPC integration.
    Grpc,
    /// Kubernetes integration.
    K8s,
    /// Testkit helpers.
    Testkit,
}

impl Subsystem {
    /// Stable subsystem identifier for logs, metrics, and error codes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Persistence => "persistence",
            Self::PersistencePostgres => "persistence-postgres",
            Self::Remote => "remote",
            Self::Cluster => "cluster",
            Self::Sharding => "sharding",
            Self::Workflow => "workflow",
            Self::Stream => "stream",
            Self::Process => "process",
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::K8s => "k8s",
            Self::Testkit => "testkit",
        }
    }
}

/// Framework error with stable subsystem and code fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RakkaError {
    subsystem: Subsystem,
    code: String,
    message: String,
}

impl RakkaError {
    /// Creates a new framework error.
    #[must_use]
    pub fn new(subsystem: Subsystem, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            subsystem,
            code: code.into(),
            message: message.into(),
        }
    }

    /// Creates a new core runtime error.
    #[must_use]
    pub fn core(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(Subsystem::Core, code, message)
    }

    /// Subsystem that produced the error.
    #[must_use]
    pub const fn subsystem(&self) -> Subsystem {
        self.subsystem
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for RakkaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: {}",
            self.subsystem.as_str(),
            self.code,
            self.message
        )
    }
}

impl Error for RakkaError {}
