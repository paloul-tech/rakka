//! Actor path types.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

/// Logical path for a local Rakka actor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPath(String);

impl ActorPath {
    /// Creates a new actor path from an already formatted path string.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates a user actor path for a system.
    #[must_use]
    pub fn user(system_name: &str, actor_name: &str, incarnation: u64) -> Self {
        Self(format!(
            "rakka://local/{system_name}/user/{actor_name}#{incarnation}"
        ))
    }

    /// Creates a child actor path below this actor path.
    #[must_use]
    pub fn child(&self, child_name: &str, incarnation: u64) -> Self {
        Self(format!("{}/{child_name}#{incarnation}", self.0))
    }

    /// Returns the path as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
