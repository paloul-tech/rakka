//! Cluster node identity, addressing, roles, and protocol compatibility.

use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

/// Stable process incarnation identity for a Rakka cluster node.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId {
    logical_id: String,
    incarnation: String,
}

impl NodeId {
    /// Creates a node id from a stable logical id and an incarnation uid.
    #[must_use]
    pub fn new(logical_id: impl Into<String>, incarnation: impl Into<String>) -> Self {
        Self {
            logical_id: logical_id.into(),
            incarnation: incarnation.into(),
        }
    }

    /// Stable logical id, such as a StatefulSet pod name.
    #[must_use]
    pub fn logical_id(&self) -> &str {
        &self.logical_id
    }

    /// Incarnation uid that changes when the process or pod restarts.
    #[must_use]
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.logical_id, self.incarnation)
    }
}

/// Direct remoting address for a Rakka node.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeAddress {
    host: String,
    port: u16,
}

impl NodeAddress {
    /// Creates a direct node address.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Host name or IP address used for pod-to-pod remoting.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Remoting port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the host:port endpoint string.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Named node capability or placement role.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeRole(String);

impl NodeRole {
    /// Creates a node role.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the role name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for NodeRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Cluster protocol version used for rolling-update compatibility checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProtocolVersion {
    major: u16,
    minor: u16,
}

impl ProtocolVersion {
    /// Creates a protocol version.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Major version.
    #[must_use]
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Minor version.
    #[must_use]
    pub const fn minor(self) -> u16 {
        self.minor
    }
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Inclusive compatibility range advertised by a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompatibilityRange {
    min: ProtocolVersion,
    max: ProtocolVersion,
}

impl CompatibilityRange {
    /// Creates an inclusive compatibility range.
    #[must_use]
    pub const fn new(min: ProtocolVersion, max: ProtocolVersion) -> Self {
        Self { min, max }
    }

    /// Minimum compatible protocol version.
    #[must_use]
    pub const fn min(self) -> ProtocolVersion {
        self.min
    }

    /// Maximum compatible protocol version.
    #[must_use]
    pub const fn max(self) -> ProtocolVersion {
        self.max
    }

    /// Returns true when the version is inside this inclusive range.
    #[must_use]
    pub fn contains(self, version: ProtocolVersion) -> bool {
        self.min <= version && version <= self.max
    }
}

impl Display for CompatibilityRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}..={}", self.min, self.max)
    }
}

/// Node protocol and rolling-update compatibility advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterProtocol {
    version: ProtocolVersion,
    compatible_with: CompatibilityRange,
}

impl ClusterProtocol {
    /// Creates a cluster protocol advertisement.
    #[must_use]
    pub const fn new(version: ProtocolVersion, compatible_with: CompatibilityRange) -> Self {
        Self {
            version,
            compatible_with,
        }
    }

    /// Protocol version used by this node.
    #[must_use]
    pub const fn version(self) -> ProtocolVersion {
        self.version
    }

    /// Inclusive version range this node can communicate with.
    #[must_use]
    pub const fn compatible_with(self) -> CompatibilityRange {
        self.compatible_with
    }

    /// Returns true when both nodes advertise mutual compatibility.
    #[must_use]
    pub fn is_compatible_with(self, other: Self) -> bool {
        self.compatible_with.contains(other.version) && other.compatible_with.contains(self.version)
    }

    /// Default v1 protocol policy, allowing N/N+1 minor-version coexistence.
    #[must_use]
    pub const fn v1() -> Self {
        Self::new(
            ProtocolVersion::new(1, 0),
            CompatibilityRange::new(ProtocolVersion::new(1, 0), ProtocolVersion::new(1, 1)),
        )
    }
}

impl Default for ClusterProtocol {
    fn default() -> Self {
        Self::v1()
    }
}

impl Display for ClusterProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "version {}, compatible {}",
            self.version, self.compatible_with
        )
    }
}

/// Cluster node descriptor advertised through discovery and membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    id: NodeId,
    address: NodeAddress,
    roles: BTreeSet<NodeRole>,
    protocol: ClusterProtocol,
}

impl ClusterNode {
    /// Creates a cluster node with the default v1 protocol policy.
    #[must_use]
    pub fn new(id: NodeId, address: NodeAddress) -> Self {
        Self {
            id,
            address,
            roles: BTreeSet::new(),
            protocol: ClusterProtocol::default(),
        }
    }

    /// Returns the node id.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    /// Returns the direct remoting address.
    #[must_use]
    pub fn address(&self) -> &NodeAddress {
        &self.address
    }

    /// Returns the configured node roles.
    #[must_use]
    pub fn roles(&self) -> &BTreeSet<NodeRole> {
        &self.roles
    }

    /// Returns the advertised cluster protocol compatibility.
    #[must_use]
    pub const fn protocol(&self) -> ClusterProtocol {
        self.protocol
    }

    /// Adds a role to this node descriptor.
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.insert(NodeRole::new(role));
        self
    }

    /// Sets the cluster protocol compatibility advertisement.
    #[must_use]
    pub const fn with_protocol(mut self, protocol: ClusterProtocol) -> Self {
        self.protocol = protocol;
        self
    }
}
