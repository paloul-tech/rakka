//! Compatibility fixtures for Rakka v1 rolling-update tests.

use rakka_cluster::{ClusterProtocol, CompatibilityRange, ProtocolVersion};
use rakka_remote::{SchemaCompatibilityPolicy, DEFAULT_REMOTE_ENVELOPE_VERSION};

/// Compatibility dimensions covered by the v1 hardening matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompatibilityDimension {
    /// Cargo crate/package version.
    CrateVersion,
    /// Cluster protocol version and compatibility range.
    ClusterProtocol,
    /// Remote envelope wire version.
    RemoteEnvelope,
    /// Application message schema version.
    MessageSchema,
    /// Kubernetes manifest compatibility metadata.
    KubernetesManifest,
    /// Generated public API contract version.
    GeneratedApi,
}

impl CompatibilityDimension {
    /// Stable label used in docs and test diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CrateVersion => "crate-version",
            Self::ClusterProtocol => "cluster-protocol",
            Self::RemoteEnvelope => "remote-envelope",
            Self::MessageSchema => "message-schema",
            Self::KubernetesManifest => "kubernetes-manifest",
            Self::GeneratedApi => "generated-api",
        }
    }
}

/// Compatibility case names used across v1 hardening tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompatibilityCaseKind {
    /// Current N release.
    Current,
    /// Next N+1 release allowed during rolling update.
    Next,
    /// Too old to join or decode without an explicit bridge.
    IncompatibleOld,
    /// Too new or different major version to join or decode.
    IncompatibleNew,
    /// Additive schema evolution inside the accepted window.
    AdditiveSchema,
    /// Exact schema policy for intentionally incompatible migrations.
    ExactSchema,
}

impl CompatibilityCaseKind {
    /// Stable label used in docs and test diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "n-current",
            Self::Next => "n-plus-one",
            Self::IncompatibleOld => "incompatible-old",
            Self::IncompatibleNew => "incompatible-new",
            Self::AdditiveSchema => "additive-schema",
            Self::ExactSchema => "exact-schema",
        }
    }
}

/// One protocol-admission compatibility case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolCompatibilityCase {
    kind: CompatibilityCaseKind,
    protocol: ClusterProtocol,
    expected_compatible: bool,
}

impl ProtocolCompatibilityCase {
    /// Creates a protocol compatibility case.
    #[must_use]
    pub const fn new(
        kind: CompatibilityCaseKind,
        protocol: ClusterProtocol,
        expected_compatible: bool,
    ) -> Self {
        Self {
            kind,
            protocol,
            expected_compatible,
        }
    }

    /// Case kind.
    #[must_use]
    pub const fn kind(self) -> CompatibilityCaseKind {
        self.kind
    }

    /// Remote protocol advertised by this case.
    #[must_use]
    pub const fn protocol(self) -> ClusterProtocol {
        self.protocol
    }

    /// Whether the case should be admitted by the v1 rolling window.
    #[must_use]
    pub const fn expected_compatible(self) -> bool {
        self.expected_compatible
    }
}

/// One schema-compatibility case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaCompatibilityCase {
    kind: CompatibilityCaseKind,
    schema_version: u32,
    expected_supported: bool,
}

impl SchemaCompatibilityCase {
    /// Creates a schema compatibility case.
    #[must_use]
    pub const fn new(
        kind: CompatibilityCaseKind,
        schema_version: u32,
        expected_supported: bool,
    ) -> Self {
        Self {
            kind,
            schema_version,
            expected_supported,
        }
    }

    /// Case kind.
    #[must_use]
    pub const fn kind(self) -> CompatibilityCaseKind {
        self.kind
    }

    /// Schema version carried by the remote envelope.
    #[must_use]
    pub const fn schema_version(self) -> u32 {
        self.schema_version
    }

    /// Whether the case should be accepted by the additive schema window.
    #[must_use]
    pub const fn expected_supported(self) -> bool {
        self.expected_supported
    }
}

/// V1 compatibility fixture shared by matrix tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1CompatibilityFixture {
    crate_version: &'static str,
    manifest_version: &'static str,
    generated_api_version: &'static str,
    current_protocol_version: ProtocolVersion,
    next_protocol_version: ProtocolVersion,
    incompatible_old_protocol_version: ProtocolVersion,
    incompatible_new_protocol_version: ProtocolVersion,
    envelope_version: u32,
    current_schema_version: u32,
}

impl V1CompatibilityFixture {
    /// Creates the default v1 compatibility fixture.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            crate_version: env!("CARGO_PKG_VERSION"),
            manifest_version: "1.0",
            generated_api_version: "1.0",
            current_protocol_version: ProtocolVersion::new(1, 0),
            next_protocol_version: ProtocolVersion::new(1, 1),
            incompatible_old_protocol_version: ProtocolVersion::new(0, 9),
            incompatible_new_protocol_version: ProtocolVersion::new(2, 0),
            envelope_version: DEFAULT_REMOTE_ENVELOPE_VERSION,
            current_schema_version: 2,
        }
    }

    /// Cargo crate version used by the fixture.
    #[must_use]
    pub const fn crate_version(&self) -> &'static str {
        self.crate_version
    }

    /// Kubernetes manifest compatibility metadata version.
    #[must_use]
    pub const fn manifest_version(&self) -> &'static str {
        self.manifest_version
    }

    /// Generated public API contract version.
    #[must_use]
    pub const fn generated_api_version(&self) -> &'static str {
        self.generated_api_version
    }

    /// Current N cluster protocol version.
    #[must_use]
    pub const fn current_protocol_version(&self) -> ProtocolVersion {
        self.current_protocol_version
    }

    /// Next N+1 cluster protocol version.
    #[must_use]
    pub const fn next_protocol_version(&self) -> ProtocolVersion {
        self.next_protocol_version
    }

    /// Remote envelope wire version.
    #[must_use]
    pub const fn envelope_version(&self) -> u32 {
        self.envelope_version
    }

    /// Current schema version used for encoding in rolling-update tests.
    #[must_use]
    pub const fn current_schema_version(&self) -> u32 {
        self.current_schema_version
    }

    /// Standard v1 N/N+1 protocol advertisement for the current node.
    #[must_use]
    pub const fn current_protocol(&self) -> ClusterProtocol {
        ClusterProtocol::new(
            self.current_protocol_version,
            CompatibilityRange::new(self.current_protocol_version, self.next_protocol_version),
        )
    }

    /// Standard v1 N/N+1 protocol advertisement for the next node.
    #[must_use]
    pub const fn next_protocol(&self) -> ClusterProtocol {
        ClusterProtocol::new(
            self.next_protocol_version,
            CompatibilityRange::new(self.current_protocol_version, self.next_protocol_version),
        )
    }

    /// Protocol cases used by admission and handshake matrix tests.
    #[must_use]
    pub fn protocol_cases(&self) -> Vec<ProtocolCompatibilityCase> {
        vec![
            ProtocolCompatibilityCase::new(
                CompatibilityCaseKind::Current,
                self.current_protocol(),
                true,
            ),
            ProtocolCompatibilityCase::new(CompatibilityCaseKind::Next, self.next_protocol(), true),
            ProtocolCompatibilityCase::new(
                CompatibilityCaseKind::IncompatibleOld,
                ClusterProtocol::exact(self.incompatible_old_protocol_version),
                false,
            ),
            ProtocolCompatibilityCase::new(
                CompatibilityCaseKind::IncompatibleNew,
                ClusterProtocol::exact(self.incompatible_new_protocol_version),
                false,
            ),
        ]
    }

    /// Schema cases used by registry and remote-envelope matrix tests.
    #[must_use]
    pub fn schema_cases(&self) -> Vec<SchemaCompatibilityCase> {
        vec![
            SchemaCompatibilityCase::new(
                CompatibilityCaseKind::AdditiveSchema,
                self.current_schema_version.saturating_sub(1),
                true,
            ),
            SchemaCompatibilityCase::new(
                CompatibilityCaseKind::Current,
                self.current_schema_version,
                true,
            ),
            SchemaCompatibilityCase::new(
                CompatibilityCaseKind::IncompatibleOld,
                self.current_schema_version.saturating_sub(2),
                false,
            ),
            SchemaCompatibilityCase::new(
                CompatibilityCaseKind::IncompatibleNew,
                self.current_schema_version.saturating_add(1),
                false,
            ),
        ]
    }

    /// Standard additive schema policy for the fixture.
    #[must_use]
    pub fn additive_schema_policy(&self) -> SchemaCompatibilityPolicy {
        SchemaCompatibilityPolicy::n_plus_one(self.current_schema_version)
    }

    /// Exact schema policy for incompatible migrations.
    #[must_use]
    pub fn exact_schema_policy(&self) -> SchemaCompatibilityPolicy {
        SchemaCompatibilityPolicy::exact(self.current_schema_version)
    }
}

impl Default for V1CompatibilityFixture {
    fn default() -> Self {
        Self::new()
    }
}

/// Dimensions covered by the v1 compatibility matrix.
#[must_use]
pub const fn v1_compatibility_dimensions() -> &'static [CompatibilityDimension] {
    &[
        CompatibilityDimension::CrateVersion,
        CompatibilityDimension::ClusterProtocol,
        CompatibilityDimension::RemoteEnvelope,
        CompatibilityDimension::MessageSchema,
        CompatibilityDimension::KubernetesManifest,
        CompatibilityDimension::GeneratedApi,
    ]
}

/// Case kinds covered by the v1 compatibility matrix.
#[must_use]
pub const fn v1_compatibility_case_kinds() -> &'static [CompatibilityCaseKind] {
    &[
        CompatibilityCaseKind::Current,
        CompatibilityCaseKind::Next,
        CompatibilityCaseKind::IncompatibleOld,
        CompatibilityCaseKind::IncompatibleNew,
        CompatibilityCaseKind::AdditiveSchema,
        CompatibilityCaseKind::ExactSchema,
    ]
}
