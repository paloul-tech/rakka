//! Actor path and incarnation identity types.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{RakkaError, RakkaResult};

/// Logical path for a local Rakka actor.
///
/// `ActorPath` intentionally does not include an incarnation id. Reusing the
/// same logical name after an actor stops produces the same path with a new
/// [`ActorUid`].
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
    pub fn user(system_name: &str, actor_name: &str) -> Self {
        Self(format!("rakka://local/{system_name}/user/{actor_name}"))
    }

    /// Creates a system actor path for a system.
    #[must_use]
    pub fn system(system_name: &str, actor_name: &str) -> Self {
        Self(format!("rakka://local/{system_name}/system/{actor_name}"))
    }

    /// Creates a child actor path below this actor path.
    #[must_use]
    pub fn child(&self, child_name: &str) -> Self {
        Self(format!("{}/{child_name}", self.0))
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

/// Unique incarnation id assigned to one live actor cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorUid(u64);

impl ActorUid {
    /// Creates an actor uid from a numeric value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric uid value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl Display for ActorUid {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Validates one actor path segment before it is appended to a path.
///
/// Segments are kept conservative because they are part of serialized actor
/// references and operational metrics.
pub fn validate_actor_path_segment(segment: &str) -> RakkaResult<()> {
    if segment.is_empty() {
        return Err(RakkaError::core(
            "invalid-actor-name",
            "actor name must not be empty",
        ));
    }

    if segment.contains('/') || segment.contains('#') || segment.contains('?') {
        return Err(RakkaError::core(
            "invalid-actor-name",
            "actor name must not contain '/', '#', or '?'",
        ));
    }

    Ok(())
}
