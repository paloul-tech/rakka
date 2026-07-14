//! Schema versions and fail-closed compatibility for persisted agent records.
//!
//! Every durable record in this crate carries a [`StateSchemaVersion`], and a
//! version this binary does not understand is rejected instead of being
//! interpreted with guessed semantics. That rule is cross-cutting: it binds from
//! M1 for every record any later milestone persists, so a rolling update never
//! reads a newer record optimistically and never silently reinterprets an older
//! one ([specification section 20](../../../docs/plans/rakka-agent/spec.md)).
//!
//! The default policy is N/N+1: a binary reads the version it writes and the
//! version immediately before it, which is what a Kubernetes rolling update
//! needs while both generations share a cluster. Anything older must be
//! backfilled before the next deployment; anything newer means an older binary
//! is reading a record written by a newer peer, and it fails closed.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::StateSchemaVersion;
use serde::{Deserialize, Serialize};

/// Current schema version of the durable [`crate::agent::AgentEntityState`].
pub const CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::definition::AgentDefinitionRevision`].
pub const CURRENT_AGENT_DEFINITION_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::definition::SettingsRevision`].
pub const CURRENT_AGENT_SETTINGS_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::definition::AgentSetupRevision`].
pub const CURRENT_AGENT_SETUP_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a durable [`crate::choreography::AgentExchangeJournal`].
pub const CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of an [`crate::choreography::AgentExchangeEnvelope`].
pub const CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of an [`crate::choreography::AgentExchangeReply`].
pub const CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Result type for schema compatibility checks.
pub type AgentSchemaResult<T> = Result<T, AgentSchemaError>;

/// Durable agent record kinds that carry an independent schema version.
///
/// Each kind versions separately: a settings-revision change must not force a
/// rewrite of every stored definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentRecordKind {
    /// Durable state of the sharded agent entity.
    EntityState,
    /// Versioned agent definition revision.
    DefinitionRevision,
    /// Versioned agent settings revision.
    SettingsRevision,
    /// Versioned per-run agent setup revision.
    SetupRevision,
    /// Durable inter-entity exchange journal carried inside a participant's
    /// own state.
    ExchangeJournal,
    /// Inter-entity exchange envelope, which is both persisted by the initiator
    /// and sent across a node boundary.
    ExchangeEnvelope,
    /// Reply to an inter-entity exchange.
    ExchangeReply,
}

impl AgentRecordKind {
    /// Every record kind this binary versions.
    pub const ALL: [Self; 7] = [
        Self::EntityState,
        Self::DefinitionRevision,
        Self::SettingsRevision,
        Self::SetupRevision,
        Self::ExchangeJournal,
        Self::ExchangeEnvelope,
        Self::ExchangeReply,
    ];

    /// Stable kebab-case label for errors, logs, and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::EntityState => "agent-entity-state",
            Self::DefinitionRevision => "agent-definition-revision",
            Self::SettingsRevision => "agent-settings-revision",
            Self::SetupRevision => "agent-setup-revision",
            Self::ExchangeJournal => "agent-exchange-journal",
            Self::ExchangeEnvelope => "agent-exchange-envelope",
            Self::ExchangeReply => "agent-exchange-reply",
        }
    }

    /// Version of this record kind that the running binary writes.
    #[must_use]
    pub const fn current_schema_version(self) -> StateSchemaVersion {
        match self {
            Self::EntityState => CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION,
            Self::DefinitionRevision => CURRENT_AGENT_DEFINITION_SCHEMA_VERSION,
            Self::SettingsRevision => CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
            Self::SetupRevision => CURRENT_AGENT_SETUP_SCHEMA_VERSION,
            Self::ExchangeJournal => CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION,
            Self::ExchangeEnvelope => CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION,
            Self::ExchangeReply => CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::EntityState => 0,
            Self::DefinitionRevision => 1,
            Self::SettingsRevision => 2,
            Self::SetupRevision => 3,
            Self::ExchangeJournal => 4,
            Self::ExchangeEnvelope => 5,
            Self::ExchangeReply => 6,
        }
    }
}

impl Display for AgentRecordKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A durable record that declares its own schema version.
pub trait VersionedAgentRecord {
    /// Record kind this type persists as.
    const RECORD_KIND: AgentRecordKind;

    /// Schema version carried by this record instance.
    fn schema_version(&self) -> StateSchemaVersion;
}

/// Accepted schema-version window for one record kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSchemaCompatibility {
    current: StateSchemaVersion,
    minimum_supported: StateSchemaVersion,
}

impl AgentSchemaCompatibility {
    /// Creates a compatibility window.
    ///
    /// A minimum above the current version is not a legal window, so it is
    /// clamped to the current version: the constructor cannot produce a policy
    /// that rejects the version this binary writes.
    #[must_use]
    pub const fn new(current: StateSchemaVersion, minimum_supported: StateSchemaVersion) -> Self {
        let minimum_supported = if minimum_supported.get() > current.get() {
            current
        } else {
            minimum_supported
        };
        Self {
            current,
            minimum_supported,
        }
    }

    /// Creates the default N/N+1 window: this version and the one before it.
    #[must_use]
    pub const fn n_plus_one(current: StateSchemaVersion) -> Self {
        Self::new(current, previous_schema_version(current))
    }

    /// Version this binary writes.
    #[must_use]
    pub const fn current(self) -> StateSchemaVersion {
        self.current
    }

    /// Oldest version this binary reads.
    #[must_use]
    pub const fn minimum_supported(self) -> StateSchemaVersion {
        self.minimum_supported
    }

    /// Accepts a persisted version, or fails closed.
    pub const fn check(
        self,
        record: AgentRecordKind,
        version: StateSchemaVersion,
    ) -> AgentSchemaResult<()> {
        if version.get() > self.current.get() {
            return Err(AgentSchemaError::VersionAhead {
                record,
                version,
                current: self.current,
            });
        }
        if version.get() < self.minimum_supported.get() {
            return Err(AgentSchemaError::VersionTooOld {
                record,
                version,
                minimum_supported: self.minimum_supported,
            });
        }
        Ok(())
    }
}

/// Schema-compatibility policy for every durable record this crate writes.
///
/// The policy carries one window per [`AgentRecordKind`], so a milestone that
/// introduces a record kind widens this type without disturbing the windows
/// already in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSchemaPolicy {
    windows: [AgentSchemaCompatibility; AgentRecordKind::ALL.len()],
}

impl AgentSchemaPolicy {
    /// Creates the default N/N+1 policy: for every record kind, the version this
    /// binary writes and the version immediately before it.
    #[must_use]
    pub const fn n_plus_one() -> Self {
        let mut windows = [AgentSchemaCompatibility::n_plus_one(StateSchemaVersion::new(1));
            AgentRecordKind::ALL.len()];
        let mut index = 0;
        while index < AgentRecordKind::ALL.len() {
            let record = AgentRecordKind::ALL[index];
            windows[index] = AgentSchemaCompatibility::n_plus_one(record.current_schema_version());
            index += 1;
        }
        Self { windows }
    }

    /// Replaces the accepted window for one record kind.
    #[must_use]
    pub const fn with_compatibility(
        mut self,
        record: AgentRecordKind,
        compatibility: AgentSchemaCompatibility,
    ) -> Self {
        self.windows[record.index()] = compatibility;
        self
    }

    /// Accepted window for one record kind.
    #[must_use]
    pub const fn compatibility(&self, record: AgentRecordKind) -> AgentSchemaCompatibility {
        self.windows[record.index()]
    }

    /// Accepts a persisted version for one record kind, or fails closed.
    pub const fn check(
        &self,
        record: AgentRecordKind,
        version: StateSchemaVersion,
    ) -> AgentSchemaResult<()> {
        self.compatibility(record).check(record, version)
    }

    /// Accepts a loaded record, or fails closed.
    pub fn check_record<R>(&self, record: &R) -> AgentSchemaResult<()>
    where
        R: VersionedAgentRecord,
    {
        self.check(R::RECORD_KIND, record.schema_version())
    }
}

impl Default for AgentSchemaPolicy {
    fn default() -> Self {
        Self::n_plus_one()
    }
}

/// Schema version immediately before `version`, saturating at the first version.
#[must_use]
pub const fn previous_schema_version(version: StateSchemaVersion) -> StateSchemaVersion {
    if version.get() <= 1 {
        StateSchemaVersion::new(1)
    } else {
        StateSchemaVersion::new(version.get() - 1)
    }
}

/// Fail-closed schema rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSchemaError {
    /// The record was written by a newer binary than this one.
    VersionAhead {
        /// Record kind that failed the check.
        record: AgentRecordKind,
        /// Version carried by the record.
        version: StateSchemaVersion,
        /// Newest version this binary understands.
        current: StateSchemaVersion,
    },
    /// The record predates the oldest version this binary reads.
    VersionTooOld {
        /// Record kind that failed the check.
        record: AgentRecordKind,
        /// Version carried by the record.
        version: StateSchemaVersion,
        /// Oldest version this binary understands.
        minimum_supported: StateSchemaVersion,
    },
}

impl AgentSchemaError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::VersionAhead { .. } => "schema-version-ahead",
            Self::VersionTooOld { .. } => "schema-version-too-old",
        }
    }

    /// Record kind that failed the check.
    #[must_use]
    pub const fn record(&self) -> AgentRecordKind {
        match self {
            Self::VersionAhead { record, .. } | Self::VersionTooOld { record, .. } => *record,
        }
    }
}

impl Display for AgentSchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionAhead {
                record,
                version,
                current,
            } => write!(
                f,
                "{record} schema version {} was written by a newer binary; this binary understands up to {}",
                version.get(),
                current.get()
            ),
            Self::VersionTooOld {
                record,
                version,
                minimum_supported,
            } => write!(
                f,
                "{record} schema version {} is older than the minimum supported version {}",
                version.get(),
                minimum_supported.get()
            ),
        }
    }
}

impl Error for AgentSchemaError {}
