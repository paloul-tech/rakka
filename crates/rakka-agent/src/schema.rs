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

/// Current schema version of the durable [`crate::task::AgentTaskState`].
pub const CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::task::AgentTaskDefinition`].
pub const CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::task::AgentTaskHistoryEntry`].
pub const CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of the durable [`crate::run::AgentRunState`].
pub const CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::loop_runtime::AgentLoopState`].
///
/// The loop state versions separately from the run state that carries it,
/// because the loop is the part that evolves: a new phase or a new pending
/// reference must not force every run record to be rewritten
/// ([specification 9.4](../../../docs/plans/rakka-agent/spec.md)).
pub const CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::effect::AgentRunEffect`].
pub const CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::model::AgentModelTurn`].
///
/// The Rakka-owned turn is the only durable format for what a model produced, so
/// it carries its own version: an adapter upgrade migrates *this* record, and no
/// provider type is ever the compatibility contract
/// ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
pub const CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::budget::AgentEscrowLedger`].
///
/// The ledger versions separately from the entity state that carries it: the
/// escrow hierarchy grows a scope — a goal, an epoch — without any of that
/// touching the records of the entities it hangs under
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
pub const CURRENT_AGENT_ESCROW_LEDGER_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted
/// [`crate::admission::AutonomyAdmissionDecision`].
///
/// An admission decision is immutable and outlives the definition revision it
/// admitted, so it carries its own version rather than the agent state's: a
/// decision recorded by an earlier binary must still be interpretable, or fail
/// closed ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
pub const CURRENT_AGENT_ADMISSION_DECISION_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a durable [`crate::choreography::AgentExchangeJournal`].
pub const CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of an [`crate::choreography::AgentExchangeEnvelope`].
pub const CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of an [`crate::choreography::AgentExchangeReply`].
pub const CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted
/// [`crate::memory::SessionMemoryEntry`].
///
/// A session entry outlives the run that wrote it — terminal-run retention is
/// tenant policy ([specification 13.2](../../../docs/plans/rakka-agent/spec.md))
/// — and it is persisted in a store independent of the run's own state, so it
/// carries its own version and fails closed on one this binary cannot read.
pub const CURRENT_AGENT_SESSION_MEMORY_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted
/// [`crate::memory::MemoryContextSnapshot`].
///
/// A snapshot is immutable and content-addressed, and a model-effect retry reads
/// it back long after it was assembled
/// ([specification 13.5](../../../docs/plans/rakka-agent/spec.md)), so it carries
/// its own version rather than the run state's. Slice 2.2 reshaped the
/// private-selection field from bare identities to content-embedding
/// selections without bumping this version, under the unreleased-branch rule
/// the slice 1.7 amendment recorded: no released writer has ever persisted
/// the earlier shape, and every record written so far carries an empty
/// selection, which the reshaped field still loads. The first reshape after a
/// release must bump it.
pub const CURRENT_AGENT_MEMORY_CONTEXT_SNAPSHOT_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted
/// [`crate::observability::AgentDecisionEvent`].
///
/// A decision event is a projection record, never the correctness source, but
/// it is persisted with bounded retention and read back by the session view,
/// so it evolves under the same fail-closed rule as every other record.
pub const CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted
/// [`crate::memory::AgentPrivateMemory`].
///
/// A private memory outlives every run that touched it — it is the agent's
/// long-term record, scoped `(TenantId, AgentId)` and persisted in a store
/// independent of any run's state
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)) — so it
/// carries its own version and fails closed on one this binary cannot read.
/// Slice 2.1 reshaped the record declared by slice 1.11 without bumping this
/// version, under the unreleased-branch rule the slice 1.7 amendment recorded:
/// no released writer has ever persisted the earlier shape. The first reshape
/// after a release must bump it.
pub const CURRENT_AGENT_PRIVATE_MEMORY_SCHEMA_VERSION: StateSchemaVersion =
    StateSchemaVersion::new(1);

/// Current schema version of a persisted [`crate::checkpoints::AgentCheckpoint`].
///
/// A checkpoint outlives the effect generation it gates — it is resolved by a
/// human or authorization service after the run has passivated — so it carries
/// its own version rather than the run state's: a checkpoint opened by an
/// earlier binary must still be interpretable on resolution, or fail closed
/// ([specification 12.2](../../../docs/plans/rakka-agent/spec.md)).
pub const CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of a persisted
/// [`crate::wake::AgentWakePolicyRevision`].
///
/// A wake-policy revision outlives every wake constructed under it — a wake
/// binds the policy revision in force at construction, and an operator reads
/// that contract back long after the policy moved on — so it carries its own
/// version rather than the goal or task state's, and fails closed on one this
/// binary cannot read.
pub const CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Current schema version of the persisted
/// [`crate::wake_timers::AgentWakeTimerStoreState`].
///
/// The wake-timer store is the shared scanner's durable index of parked
/// occurrences. It is scanned by whichever pod hosts a scanner, so it
/// versions independently of any entity's state and fails closed on a record
/// this binary cannot read.
pub const CURRENT_AGENT_WAKE_TIMER_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

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
    /// Durable state of the sharded typed-task entity.
    TaskState,
    /// Versioned typed-task definition.
    TaskDefinition,
    /// One append-only typed-task history entry.
    TaskHistoryEntry,
    /// Durable state of the sharded run entity.
    RunState,
    /// The versioned durable loop state one run carries.
    LoopState,
    /// One durable effect a run committed and is waiting on.
    RunEffect,
    /// The Rakka-owned versioned record of one model turn.
    ModelTurn,
    /// The escrow ledger one scope owns, carried inside its own state.
    EscrowLedger,
    /// One immutable autonomy admission decision.
    AdmissionDecision,
    /// Durable inter-entity exchange journal carried inside a participant's
    /// own state.
    ExchangeJournal,
    /// Inter-entity exchange envelope, which is both persisted by the initiator
    /// and sent across a node boundary.
    ExchangeEnvelope,
    /// Reply to an inter-entity exchange.
    ExchangeReply,
    /// One durable HITL checkpoint a run is waiting on.
    Checkpoint,
    /// One ordered short-term session-memory entry.
    SessionMemoryEntry,
    /// One immutable memory context snapshot a model effect was prepared
    /// against.
    MemoryContextSnapshot,
    /// One structured loop-decision event, retained by a bounded sink.
    DecisionEvent,
    /// One agent-private long-term memory, scoped `(TenantId, AgentId)`.
    PrivateMemory,
    /// One accepted revision of a continuous goal's wake policy.
    WakePolicyRevision,
    /// The shared scanner's durable index of parked wake occurrences.
    WakeTimerState,
}

impl AgentRecordKind {
    /// Every record kind this binary versions.
    pub const ALL: [Self; 23] = [
        Self::EntityState,
        Self::DefinitionRevision,
        Self::SettingsRevision,
        Self::SetupRevision,
        Self::TaskState,
        Self::TaskDefinition,
        Self::TaskHistoryEntry,
        Self::RunState,
        Self::LoopState,
        Self::RunEffect,
        Self::ModelTurn,
        Self::EscrowLedger,
        Self::AdmissionDecision,
        Self::ExchangeJournal,
        Self::ExchangeEnvelope,
        Self::ExchangeReply,
        Self::Checkpoint,
        Self::SessionMemoryEntry,
        Self::MemoryContextSnapshot,
        Self::DecisionEvent,
        Self::PrivateMemory,
        Self::WakePolicyRevision,
        Self::WakeTimerState,
    ];

    /// Stable kebab-case label for errors, logs, and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::EntityState => "agent-entity-state",
            Self::DefinitionRevision => "agent-definition-revision",
            Self::SettingsRevision => "agent-settings-revision",
            Self::SetupRevision => "agent-setup-revision",
            Self::TaskState => "agent-task-state",
            Self::TaskDefinition => "agent-task-definition",
            Self::TaskHistoryEntry => "agent-task-history-entry",
            Self::RunState => "agent-run-state",
            Self::LoopState => "agent-loop-state",
            Self::RunEffect => "agent-run-effect",
            Self::ModelTurn => "agent-model-turn",
            Self::EscrowLedger => "agent-escrow-ledger",
            Self::AdmissionDecision => "agent-admission-decision",
            Self::ExchangeJournal => "agent-exchange-journal",
            Self::ExchangeEnvelope => "agent-exchange-envelope",
            Self::ExchangeReply => "agent-exchange-reply",
            Self::Checkpoint => "agent-checkpoint",
            Self::SessionMemoryEntry => "agent-session-memory-entry",
            Self::MemoryContextSnapshot => "agent-memory-context-snapshot",
            Self::DecisionEvent => "agent-decision-event",
            Self::PrivateMemory => "agent-private-memory",
            Self::WakePolicyRevision => "agent-wake-policy-revision",
            Self::WakeTimerState => "agent-wake-timer-state",
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
            Self::TaskState => CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
            Self::TaskDefinition => CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION,
            Self::TaskHistoryEntry => CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION,
            Self::RunState => CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION,
            Self::LoopState => CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
            Self::RunEffect => CURRENT_AGENT_RUN_EFFECT_SCHEMA_VERSION,
            Self::ModelTurn => CURRENT_AGENT_MODEL_TURN_SCHEMA_VERSION,
            Self::EscrowLedger => CURRENT_AGENT_ESCROW_LEDGER_SCHEMA_VERSION,
            Self::AdmissionDecision => CURRENT_AGENT_ADMISSION_DECISION_SCHEMA_VERSION,
            Self::ExchangeJournal => CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION,
            Self::ExchangeEnvelope => CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION,
            Self::ExchangeReply => CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION,
            Self::Checkpoint => CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION,
            Self::SessionMemoryEntry => CURRENT_AGENT_SESSION_MEMORY_SCHEMA_VERSION,
            Self::MemoryContextSnapshot => CURRENT_AGENT_MEMORY_CONTEXT_SNAPSHOT_SCHEMA_VERSION,
            Self::DecisionEvent => CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION,
            Self::PrivateMemory => CURRENT_AGENT_PRIVATE_MEMORY_SCHEMA_VERSION,
            Self::WakePolicyRevision => CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION,
            Self::WakeTimerState => CURRENT_AGENT_WAKE_TIMER_SCHEMA_VERSION,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::EntityState => 0,
            Self::DefinitionRevision => 1,
            Self::SettingsRevision => 2,
            Self::SetupRevision => 3,
            Self::TaskState => 4,
            Self::TaskDefinition => 5,
            Self::TaskHistoryEntry => 6,
            Self::RunState => 7,
            Self::LoopState => 8,
            Self::RunEffect => 9,
            Self::ModelTurn => 10,
            Self::EscrowLedger => 11,
            Self::AdmissionDecision => 12,
            Self::ExchangeJournal => 13,
            Self::ExchangeEnvelope => 14,
            Self::ExchangeReply => 15,
            Self::Checkpoint => 16,
            Self::SessionMemoryEntry => 17,
            Self::MemoryContextSnapshot => 18,
            Self::DecisionEvent => 19,
            Self::PrivateMemory => 20,
            Self::WakePolicyRevision => 21,
            Self::WakeTimerState => 22,
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
