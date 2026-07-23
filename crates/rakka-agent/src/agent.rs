//! The sharded agent entity.
//!
//! Owns [`AgentEntity`], keyed by `(TenantId, AgentId)`, together with its
//! serializable command protocol. The entity holds the durable definition and
//! lifecycle status, the current settings revision, policy and logical
//! credential-binding references, the agent-private memory namespace, and the
//! administrative suspend, resume, and terminate commands.
//!
//! Specification: sections 6.2 and 6.11.
//!
//! # The entity is not in the hot path
//!
//! Routine run creation never round-trips through this entity
//! ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)). A popular
//! agent would otherwise serialize every one of its own runs through a single
//! writer. Assignment instead *reads* the agent's durable definition and
//! admission state — that read is [`load_agent_entity_state`], and slice 1.4's
//! assignment flow uses it directly against the store. Commands to this entity
//! are administrative: publish a definition, accept a settings update, suspend,
//! resume, terminate.
//!
//! # Serializable protocol
//!
//! [`AgentEntityCommand`] and [`AgentEntityReply`] are the wire types: they
//! serialize, carry no `Arc` payloads and no in-process reply channels, and box
//! their large payloads. [`AgentEntityMessage`] is the process-local envelope
//! that pairs a command with its reply channel, and it is what
//! [`init_agent_entity_remote_sharding`] reconstructs on the owning node. The
//! protocol is serializable from this first commit so that a later slice never
//! has to retrofit remoting into an entity whose commands cannot cross a node
//! boundary.
//!
//! # Suspension
//!
//! Suspension is a lifecycle status rather than a settings field, so there is
//! exactly one durable answer to "may this agent dispatch". It is nonetheless an
//! *immediate safety* control in the sense of
//! [specification 7.2](../../../docs/plans/rakka-agent/spec.md): slice 1.8
//! rechecks [`AgentEntityState::is_dispatch_permitted`] before every dispatch,
//! alongside the immediate-safety settings of the current revision.
//!
//! Because suspension is a safety control, lifecycle transitions are fenced on
//! a monotonic lifecycle revision, exactly as settings updates are fenced on
//! theirs: a stale resume that has aged out of the deduplication window cannot
//! reorder over a later suspension and silently lift it.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::time::Duration;

use rakka_agent_workflow::{AgentTimestampMillis, StateSchemaVersion};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ReplyTo,
};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision, StateRecord};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeResult, ClusterSharding, ClusterShardingResult, Entity,
    EntityContext, EntityId, EntityTypeKey, EntityTypeRegistration, ShardBufferConfig,
    ShardedEntityRef,
};
use serde::{Deserialize, Serialize};

use crate::admission::{
    AgentAdmissionError, AutonomyAdmissionDecision, AGENT_ADMISSION_DETAIL_MAX_LENGTH,
};
use crate::definition::{
    AgentDefinition, AgentDefinitionError, AgentDefinitionId, AgentDefinitionRevision,
    AgentPolicyRefs, AgentRevisionNumber, AgentRevisionProvenance, AgentSettings,
    AgentSettingsChange, SettingsRevision,
};
use crate::identity::{
    AgentIdentityError, AgentMemoryNamespace, AgentOperationId, AgentScope, TenantId,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION,
};

/// Default sharded entity type of the agent entity.
pub const DEFAULT_AGENT_ENTITY_TYPE: &str = "RakkaAgent";

/// How many resolved operation ids the entity remembers for deduplication.
///
/// The log makes a replayed administrative command return its original outcome
/// instead of transitioning twice
/// ([specification 6.10](../../../docs/plans/rakka-agent/spec.md)). It is bounded
/// because durable state must stay bounded; a replay older than the window is
/// still safe, because every transition is additionally fenced — a settings
/// update carries the settings revision it expects to succeed, and a lifecycle
/// transition carries the lifecycle revision it expects to advance.
pub const AGENT_ENTITY_OPERATION_LOG_CAPACITY: usize = 64;

const DEFAULT_AGENT_ENTITY_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

/// Result type for agent entity operations.
pub type AgentEntityResult<T> = Result<T, AgentEntityError>;

/// Administrative lifecycle status of one agent
/// ([specification 6.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The status is durable and says nothing about runtime residency: an `Active`
/// agent usually has no actor instance on any pod
/// ([specification 6.11](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentLifecycleStatus {
    /// The agent may accept work and dispatch effects.
    Active,
    /// The agent stays addressable and recoverable, but no further effect may be
    /// dispatched until it is resumed.
    Suspended,
    /// The agent is permanently retired. No further transition is accepted.
    Terminated,
}

impl AgentLifecycleStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Terminated => "terminated",
        }
    }

    /// Whether the status permits dispatching further effects.
    #[must_use]
    pub const fn permits_dispatch(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether the status is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Terminated)
    }
}

impl Display for AgentLifecycleStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Bounded log of resolved operation ids and the outcome each produced.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentOperationLog {
    entries: VecDeque<AgentOperationLogEntry>,
}

impl AgentOperationLog {
    /// Outcome recorded for a previously applied operation, if it is still in
    /// the window.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<AgentEntityOutcome> {
        self.entries
            .iter()
            .find(|entry| &entry.operation_id == operation_id)
            .map(|entry| entry.outcome)
    }

    /// Number of remembered operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no operation is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn record(&mut self, operation_id: AgentOperationId, outcome: AgentEntityOutcome) {
        self.entries.push_back(AgentOperationLogEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_ENTITY_OPERATION_LOG_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentOperationLogEntry {
    operation_id: AgentOperationId,
    outcome: AgentEntityOutcome,
}

/// The compact result of one accepted entity transition.
///
/// This is what a replayed operation returns, so a re-driven exchange converges
/// on the original logical result rather than transitioning a second time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEntityOutcome {
    /// Lifecycle status after the transition.
    pub status: AgentLifecycleStatus,
    /// Lifecycle revision after the transition.
    pub lifecycle_revision: AgentRevisionNumber,
    /// Definition revision after the transition.
    pub definition_revision: AgentRevisionNumber,
    /// Settings revision after the transition.
    pub settings_revision: AgentRevisionNumber,
}

/// The durable record of the retraction that returned an agent to the
/// fail-closed default
/// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// It answers "why is this agent not admitted" from the entity's own state,
/// bounded like every admission detail. It survives only while the agent stays
/// unadmitted: the next accepted admission replaces the fail-closed default
/// this record explains, so it clears the record with it — the full trail
/// lives in the audit stream the provenance references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAdmissionRetraction {
    /// The bounded, stable reason the retracting principal gave.
    pub reason: String,
    /// Who retracted the admission, when, and under which audit reference.
    pub provenance: AgentRevisionProvenance,
}

/// Durable state of one agent entity.
///
/// It carries no credential material: `credential_bindings` are logical
/// references the application resolves at dispatch time and never persists
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEntityState {
    schema_version: StateSchemaVersion,
    scope: AgentScope,
    status: AgentLifecycleStatus,
    lifecycle_revision: AgentRevisionNumber,
    definition: AgentDefinitionRevision,
    settings: SettingsRevision,
    admission: Option<AutonomyAdmissionDecision>,
    #[serde(default)]
    admission_retraction: Option<Box<AgentAdmissionRetraction>>,
    applied_operations: AgentOperationLog,
    updated_at: AgentTimestampMillis,
}

impl AgentEntityState {
    /// Instantiates an agent's durable state from its first definition and
    /// settings revision.
    #[must_use]
    pub fn new(
        scope: AgentScope,
        definition: AgentDefinitionRevision,
        settings: SettingsRevision,
        updated_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION,
            scope,
            status: AgentLifecycleStatus::Active,
            lifecycle_revision: AgentRevisionNumber::INITIAL,
            definition,
            settings,
            // An agent is unadmitted until an authorized evaluator says
            // otherwise ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
            // Instantiation is not admission: the two are separate decisions
            // precisely so that creating an agent cannot be the act that
            // authorizes it to run unattended.
            admission: None,
            admission_retraction: None,
            applied_operations: AgentOperationLog::default(),
            updated_at,
        }
    }

    /// Scope this state belongs to.
    #[must_use]
    pub const fn scope(&self) -> &AgentScope {
        &self.scope
    }

    /// Tenant boundary of this agent.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        self.scope.tenant()
    }

    /// Current lifecycle status.
    #[must_use]
    pub const fn status(&self) -> AgentLifecycleStatus {
        self.status
    }

    /// Monotonic revision of the lifecycle status.
    ///
    /// Every accepted suspend, resume, or terminate advances it, and each
    /// lifecycle command carries the revision it expects to advance. Statuses
    /// recur — an agent can be suspended, resumed, and suspended again — so the
    /// status alone cannot fence a stale replay; the revision can: a resume
    /// issued before a later suspension is rejected rather than silently
    /// lifting it, even after its operation id has aged out of the
    /// deduplication window.
    #[must_use]
    pub const fn lifecycle_revision(&self) -> AgentRevisionNumber {
        self.lifecycle_revision
    }

    /// Current definition revision.
    #[must_use]
    pub const fn definition(&self) -> &AgentDefinitionRevision {
        &self.definition
    }

    /// Current settings revision.
    #[must_use]
    pub const fn settings(&self) -> &SettingsRevision {
        &self.settings
    }

    /// The autonomy admission decision on record, when there is one
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// `None` is the fail-closed default and not a transient one: an agent that
    /// has never been admitted, and an agent whose admission was retired, are
    /// the same thing to every enforcement point.
    #[must_use]
    pub const fn admission(&self) -> Option<&AutonomyAdmissionDecision> {
        self.admission.as_ref()
    }

    /// Why the admission on record was retired, when the agent is unadmitted
    /// because someone retired it
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Enforcement never reads this — a retracted admission and a never-granted
    /// one are the same fail-closed default — but an operator asking why an
    /// agent stopped running unattended deserves the answer from the entity
    /// itself. The next accepted admission clears it.
    #[must_use]
    pub fn admission_retraction(&self) -> Option<&AgentAdmissionRetraction> {
        self.admission_retraction.as_deref()
    }

    /// Application-owned policy references this agent runs under.
    #[must_use]
    pub const fn policies(&self) -> &AgentPolicyRefs {
        &self.definition.definition().policies
    }

    /// Namespace of this agent's private long-term memory.
    ///
    /// Derived from the scope rather than stored, so it cannot drift from the
    /// tenant and agent that own it.
    #[must_use]
    pub fn memory_namespace(&self) -> AgentMemoryNamespace {
        self.scope.memory_namespace()
    }

    /// Time of the last accepted transition.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// Bounded log of resolved operations.
    #[must_use]
    pub const fn applied_operations(&self) -> &AgentOperationLog {
        &self.applied_operations
    }

    /// Whether the agent's durable state currently permits dispatching effects.
    ///
    /// This is the entity's half of the immediate-safety recheck slice 1.8 runs
    /// before every dispatch. It is deliberately a pure function of durable
    /// state, so a dispatcher can evaluate it from a store read without waking
    /// the entity.
    #[must_use]
    pub const fn is_dispatch_permitted(&self) -> bool {
        self.status.permits_dispatch()
    }

    /// Compact outcome describing the current state.
    #[must_use]
    pub const fn outcome(&self) -> AgentEntityOutcome {
        AgentEntityOutcome {
            status: self.status,
            lifecycle_revision: self.lifecycle_revision,
            definition_revision: self.definition.revision(),
            settings_revision: self.settings.revision(),
        }
    }

    /// Bounded, credential-free projection of this state.
    #[must_use]
    pub fn snapshot(&self) -> AgentEntitySnapshot {
        let definition = self.definition.definition();
        AgentEntitySnapshot {
            scope: self.scope.clone(),
            status: self.status,
            lifecycle_revision: self.lifecycle_revision,
            definition_id: definition.definition_id.clone(),
            description: definition.description.clone(),
            definition_revision: self.definition.revision(),
            settings_revision: self.settings.revision(),
            memory_namespace: self.memory_namespace(),
            policies: definition.policies.clone(),
            updated_at: self.updated_at,
        }
    }
}

impl VersionedAgentRecord for AgentEntityState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::EntityState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Bounded, credential-free projection of an agent's durable state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEntitySnapshot {
    /// Scope of the agent.
    pub scope: AgentScope,
    /// Current lifecycle status.
    pub status: AgentLifecycleStatus,
    /// Current lifecycle revision, which the next lifecycle command must
    /// expect.
    pub lifecycle_revision: AgentRevisionNumber,
    /// Identity of the published definition.
    pub definition_id: AgentDefinitionId,
    /// Bounded outcome-oriented description.
    pub description: String,
    /// Current definition revision.
    pub definition_revision: AgentRevisionNumber,
    /// Current settings revision.
    pub settings_revision: AgentRevisionNumber,
    /// Namespace of the agent's private long-term memory.
    pub memory_namespace: AgentMemoryNamespace,
    /// Application-owned policy references.
    pub policies: AgentPolicyRefs,
    /// Time of the last accepted transition.
    pub updated_at: AgentTimestampMillis,
}

/// Serializable administrative command protocol of the agent entity.
///
/// Large payloads are boxed so the enum stays small enough to move cheaply
/// through mailboxes and remote envelopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEntityCommand {
    /// Instantiate the agent from its first definition and settings.
    Instantiate {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// Content of the first definition. The entity publishes the initial
        /// revision itself, so a foreign revision number or schema version can
        /// never enter the first durable record.
        definition: Box<AgentDefinition>,
        /// Initial settings.
        settings: Box<AgentSettings>,
        /// Who accepted the instantiation, when, and under which audit
        /// reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Publish the next definition revision.
    PublishDefinition {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// Definition content of the new revision.
        definition: Box<AgentDefinition>,
        /// Who published the revision, when, and under which audit reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Accept a settings update against an expected current revision.
    UpdateSettings {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// Settings revision the caller believes is current. A mismatch is
        /// rejected rather than merged.
        expected_revision: AgentRevisionNumber,
        /// Field-level changes to apply.
        changes: Vec<AgentSettingsChange>,
        /// Who accepted the update, when, and under which audit reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Record an autonomy admission decision an authorized evaluator made
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Rakka owns the durable decision and the enforcement points; the
    /// application owns the policy that authored it. The entity is not a rubber
    /// stamp for what it is handed: it verifies the decision against the
    /// definition it claims to admit, and refuses one that names revisions this
    /// agent is not currently on — an admission of a definition that has since
    /// been replaced would otherwise admit work nobody evaluated.
    Admit {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The decision the evaluator made.
        decision: Box<AutonomyAdmissionDecision>,
    },
    /// Retire the autonomy admission decision on record, returning the agent to
    /// the fail-closed default.
    Retract {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// A bounded, stable reason, recorded on the entity as
        /// [`AgentAdmissionRetraction`] while the agent stays unadmitted.
        reason: String,
        /// Who retracted it, when, and under which audit reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Suspend the agent. No further effect may be dispatched until it resumes.
    Suspend {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// Lifecycle revision the caller believes is current. A mismatch is
        /// rejected rather than reordered over a decision the caller never saw.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who suspended the agent, when, and under which audit reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Resume a suspended agent.
    Resume {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// Lifecycle revision the caller believes is current. A mismatch is
        /// rejected rather than reordered over a decision the caller never saw.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who resumed the agent, when, and under which audit reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Permanently retire the agent.
    Terminate {
        /// Stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// Lifecycle revision the caller believes is current. A mismatch is
        /// rejected rather than reordered over a decision the caller never saw.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who terminated the agent, when, and under which audit reference.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Read the agent's bounded durable projection.
    Describe,
}

impl AgentEntityCommand {
    /// Operation id this command deduplicates on, when it mutates state.
    #[must_use]
    pub const fn operation_id(&self) -> Option<&AgentOperationId> {
        match self {
            Self::Instantiate { operation_id, .. }
            | Self::PublishDefinition { operation_id, .. }
            | Self::UpdateSettings { operation_id, .. }
            | Self::Admit { operation_id, .. }
            | Self::Retract { operation_id, .. }
            | Self::Suspend { operation_id, .. }
            | Self::Resume { operation_id, .. }
            | Self::Terminate { operation_id, .. } => Some(operation_id),
            Self::Describe => None,
        }
    }
}

/// Serializable reply protocol of the agent entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEntityReply {
    /// The command transitioned the entity.
    Applied {
        /// Outcome of the transition.
        outcome: AgentEntityOutcome,
    },
    /// The operation id was already applied; this is the original outcome, and
    /// no second transition happened.
    Duplicate {
        /// Outcome the original application produced.
        outcome: AgentEntityOutcome,
    },
    /// The agent's bounded durable projection, absent if it was never
    /// instantiated.
    Snapshot(Option<Box<AgentEntitySnapshot>>),
    /// The command was rejected.
    Rejected {
        /// Stable machine-readable error code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentEntityReply {
    fn rejected(error: &AgentEntityError) -> Self {
        Self::Rejected {
            code: error.code().to_string(),
            message: error.to_string(),
        }
    }
}

/// Process-local envelope pairing a serializable command with its reply channel.
///
/// The reply channel never crosses a node boundary:
/// [`init_agent_entity_remote_sharding`] reconstructs this envelope on the owning
/// node from the [`AgentEntityCommand`] that arrived over `rakka-remote`.
pub struct AgentEntityMessage {
    /// Command to apply.
    pub command: AgentEntityCommand,
    /// Where the reply goes.
    pub reply_to: ReplyTo<AgentEntityReply>,
}

impl Debug for AgentEntityMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEntityMessage")
            .field("command", &self.command)
            .finish_non_exhaustive()
    }
}

/// Durable persistence id of one agent entity's state.
#[must_use]
pub fn agent_entity_persistence_id(scope: &AgentScope) -> PersistenceId {
    scope.persistence_id()
}

/// Loads one agent's durable state without waking its entity.
///
/// This is the read path the assignment flow of
/// [specification 9.8](../../../docs/plans/rakka-agent/spec.md) uses: a task
/// entity deciding an assignment reads the agent's durable definition and
/// admission state directly, so a popular agent never becomes a serialization
/// bottleneck for its own runs. The schema check is applied here too, so a stale
/// reader fails closed rather than assigning against a record it cannot
/// interpret.
pub async fn load_agent_entity_state<Store>(
    store: &Store,
    scope: &AgentScope,
    policy: &AgentSchemaPolicy,
) -> AgentEntityResult<Option<AgentEntityState>>
where
    Store: DurableStateStore<AgentEntityState>,
{
    let persistence_id = scope.persistence_id();
    let record = store.load(&persistence_id).await?;
    let Some(record) = record else {
        return Ok(None);
    };
    validate_loaded_state(&record.state, policy)?;
    Ok(Some(record.state))
}

fn validate_loaded_state(
    state: &AgentEntityState,
    policy: &AgentSchemaPolicy,
) -> AgentEntityResult<()> {
    policy.check_record(state)?;
    policy.check_record(&state.definition)?;
    policy.check_record(&state.settings)?;
    Ok(())
}

/// Durable facade over one agent entity's state.
///
/// It owns recovery, the fail-closed schema check, operation deduplication, and
/// compare-and-set persistence. The actor is a thin shell over it, and tests can
/// drive it without an actor system.
pub struct AgentEntityStore<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    scope: AgentScope,
    persistence_id: PersistenceId,
    store: Store,
    policy: AgentSchemaPolicy,
    recovered: bool,
    record: Option<StateRecord<AgentEntityState>>,
}

impl<Store> Debug for AgentEntityStore<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEntityStore")
            .field("scope", &self.scope)
            .field("backend", &self.store.backend_name())
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}

impl<Store> AgentEntityStore<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    /// Creates a durable facade for one agent scope.
    #[must_use]
    pub fn new(scope: AgentScope, store: Store) -> Self {
        let persistence_id = scope.persistence_id();
        Self {
            scope,
            persistence_id,
            store,
            policy: AgentSchemaPolicy::default(),
            recovered: false,
            record: None,
        }
    }

    /// Uses an explicit schema-compatibility policy.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Scope this facade addresses.
    #[must_use]
    pub const fn scope(&self) -> &AgentScope {
        &self.scope
    }

    /// Durable persistence id of this agent's state.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Loads the agent's durable state, failing closed on an unsupported schema
    /// version.
    pub async fn recover(&mut self) -> AgentEntityResult<Option<&AgentEntityState>> {
        let record = self.store.load(&self.persistence_id).await?;
        if let Some(record) = &record {
            validate_loaded_state(&record.state, &self.policy)?;
        }
        self.record = record;
        self.recovered = true;
        Ok(self.record.as_ref().map(|record| &record.state))
    }

    /// Currently recovered state, if the agent has been instantiated.
    pub fn state(&self) -> AgentEntityResult<Option<&AgentEntityState>> {
        self.ensure_recovered()?;
        Ok(self.record.as_ref().map(|record| &record.state))
    }

    /// Applies one administrative command.
    pub async fn apply(
        &mut self,
        command: AgentEntityCommand,
    ) -> AgentEntityResult<AgentEntityReply> {
        self.ensure_recovered()?;

        if let Some(operation_id) = command.operation_id() {
            if let Some(outcome) = self
                .record
                .as_ref()
                .and_then(|record| record.state.applied_operations.outcome(operation_id))
            {
                return Ok(AgentEntityReply::Duplicate { outcome });
            }
        }

        match command {
            AgentEntityCommand::Describe => Ok(AgentEntityReply::Snapshot(
                self.record
                    .as_ref()
                    .map(|record| Box::new(record.state.snapshot())),
            )),
            AgentEntityCommand::Instantiate {
                operation_id,
                definition,
                settings,
                provenance,
            } => {
                self.instantiate(operation_id, *definition, *settings, *provenance)
                    .await
            }
            AgentEntityCommand::PublishDefinition {
                operation_id,
                definition,
                provenance,
            } => {
                self.publish_definition(operation_id, *definition, *provenance)
                    .await
            }
            AgentEntityCommand::UpdateSettings {
                operation_id,
                expected_revision,
                changes,
                provenance,
            } => {
                self.update_settings(operation_id, expected_revision, changes, *provenance)
                    .await
            }
            AgentEntityCommand::Admit {
                operation_id,
                decision,
            } => self.admit(operation_id, *decision).await,
            AgentEntityCommand::Retract {
                operation_id,
                reason,
                provenance,
            } => self.retract(operation_id, reason, *provenance).await,
            AgentEntityCommand::Suspend {
                operation_id,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition_lifecycle(
                    operation_id,
                    expected_lifecycle_revision,
                    AgentLifecycleStatus::Suspended,
                    *provenance,
                )
                .await
            }
            AgentEntityCommand::Resume {
                operation_id,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition_lifecycle(
                    operation_id,
                    expected_lifecycle_revision,
                    AgentLifecycleStatus::Active,
                    *provenance,
                )
                .await
            }
            AgentEntityCommand::Terminate {
                operation_id,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition_lifecycle(
                    operation_id,
                    expected_lifecycle_revision,
                    AgentLifecycleStatus::Terminated,
                    *provenance,
                )
                .await
            }
        }
    }

    async fn instantiate(
        &mut self,
        operation_id: AgentOperationId,
        definition: AgentDefinition,
        settings: AgentSettings,
        provenance: AgentRevisionProvenance,
    ) -> AgentEntityResult<AgentEntityReply> {
        if self.record.is_some() {
            return Err(AgentEntityError::AlreadyInstantiated {
                scope: self.scope.clone(),
            });
        }

        // The definition's fields are public, so its bounded invariants cannot
        // be assumed from construction; the entity re-checks them before
        // anything is persisted.
        definition.validate()?;

        let accepted_at = provenance.accepted_at;
        // The entity publishes the initial revision itself, so the first durable
        // record always carries revision 1 and the schema version this binary
        // writes — a peer binary cannot hand it one it may later fail to read.
        let definition = AgentDefinitionRevision::initial(definition, provenance.clone());
        let settings = SettingsRevision::initial(&definition, settings, provenance)?;
        let mut state =
            AgentEntityState::new(self.scope.clone(), definition, settings, accepted_at);
        let outcome = state.outcome();
        state.applied_operations.record(operation_id, outcome);
        self.persist(state, Revision::INITIAL).await
    }

    async fn publish_definition(
        &mut self,
        operation_id: AgentOperationId,
        definition: AgentDefinition,
        provenance: AgentRevisionProvenance,
    ) -> AgentEntityResult<AgentEntityReply> {
        // The definition's fields are public, so its bounded invariants cannot
        // be assumed from construction; the entity re-checks them before
        // anything is persisted.
        definition.validate()?;

        let accepted_at = provenance.accepted_at;
        self.mutate(operation_id, accepted_at, |state| {
            let definition = state.definition.succeed(definition, provenance);
            // A published definition may narrow its envelope, so the settings
            // already in force must still be legal under the new one. If they
            // are not, the publication is rejected rather than leaving the agent
            // running under settings its definition no longer authorizes.
            state.settings.validate_against_definition(&definition)?;
            state.definition = definition;
            Ok(())
        })
        .await
    }

    async fn admit(
        &mut self,
        operation_id: AgentOperationId,
        decision: AutonomyAdmissionDecision,
    ) -> AgentEntityResult<AgentEntityReply> {
        let accepted_at = decision.created_at();
        self.mutate(operation_id, accepted_at, |state| {
            // The decision names the revisions it evaluated. If the agent has
            // moved on, this decision evaluated something else — accepting it
            // would admit a definition or settings nobody assessed — so it is
            // refused and the evaluator re-runs against what is current.
            let definition = state.definition.revision();
            if decision.definition_revision() != definition {
                return Err(AgentEntityError::StaleAdmissionRevision {
                    record: "definition",
                    admitted: decision.definition_revision(),
                    current: definition,
                });
            }
            let settings = state.settings.revision();
            if decision.settings_revision() != settings {
                return Err(AgentEntityError::StaleAdmissionRevision {
                    record: "settings",
                    admitted: decision.settings_revision(),
                    current: settings,
                });
            }
            // Rakka's half of the split: an attestation is taken on trust, and
            // everything the definition itself answers is not.
            decision.verify(state.definition.definition())?;
            state.admission = Some(decision);
            // A new admission replaces the fail-closed default the retraction
            // record explained, so the record retires with it.
            state.admission_retraction = None;
            Ok(())
        })
        .await
    }

    async fn retract(
        &mut self,
        operation_id: AgentOperationId,
        reason: String,
        provenance: AgentRevisionProvenance,
    ) -> AgentEntityResult<AgentEntityReply> {
        // The reason becomes part of a durable record, so it is held to the
        // same bound every admission detail is.
        if reason.len() > AGENT_ADMISSION_DETAIL_MAX_LENGTH {
            return Err(AgentEntityError::Admission(
                AgentAdmissionError::DetailTooLong {
                    field: "retraction reason",
                    length: reason.len(),
                    maximum: AGENT_ADMISSION_DETAIL_MAX_LENGTH,
                },
            ));
        }
        let accepted_at = provenance.accepted_at;
        self.mutate(operation_id, accepted_at, |state| {
            state.admission = None;
            state.admission_retraction =
                Some(Box::new(AgentAdmissionRetraction { reason, provenance }));
            Ok(())
        })
        .await
    }

    async fn update_settings(
        &mut self,
        operation_id: AgentOperationId,
        expected_revision: AgentRevisionNumber,
        changes: Vec<AgentSettingsChange>,
        provenance: AgentRevisionProvenance,
    ) -> AgentEntityResult<AgentEntityReply> {
        let accepted_at = provenance.accepted_at;
        self.mutate(operation_id, accepted_at, |state| {
            let current = state.settings.revision();
            if current != expected_revision {
                return Err(AgentEntityError::StaleSettingsRevision {
                    expected: expected_revision,
                    current,
                });
            }
            state.settings = state
                .settings
                .apply(&state.definition, changes, provenance)?;
            Ok(())
        })
        .await
    }

    async fn transition_lifecycle(
        &mut self,
        operation_id: AgentOperationId,
        expected_lifecycle_revision: AgentRevisionNumber,
        target: AgentLifecycleStatus,
        provenance: AgentRevisionProvenance,
    ) -> AgentEntityResult<AgentEntityReply> {
        self.mutate(operation_id, provenance.accepted_at, |state| {
            let current = state.lifecycle_revision;
            if current != expected_lifecycle_revision {
                return Err(AgentEntityError::StaleLifecycleRevision {
                    expected: expected_lifecycle_revision,
                    current,
                });
            }
            state.status = target;
            state.lifecycle_revision = current.next();
            Ok(())
        })
        .await
    }

    // Records the durable transition and its resolved operation id. A rejected
    // transition never reaches the store, so it leaves no trace in the operation
    // log and a corrected retry with the same operation id is still accepted.
    async fn mutate<F>(
        &mut self,
        operation_id: AgentOperationId,
        now: AgentTimestampMillis,
        transition: F,
    ) -> AgentEntityResult<AgentEntityReply>
    where
        F: FnOnce(&mut AgentEntityState) -> AgentEntityResult<()>,
    {
        let record = self
            .record
            .as_ref()
            .ok_or_else(|| AgentEntityError::NotInstantiated {
                scope: self.scope.clone(),
            })?;

        if record.state.status.is_terminal() {
            return Err(AgentEntityError::Terminated {
                scope: self.scope.clone(),
            });
        }

        let expected_revision = record.revision;
        let mut state = record.state.clone();
        transition(&mut state)?;
        state.updated_at = now;
        let outcome = state.outcome();
        state.applied_operations.record(operation_id, outcome);
        self.persist(state, expected_revision).await
    }

    async fn persist(
        &mut self,
        state: AgentEntityState,
        expected_revision: Revision,
    ) -> AgentEntityResult<AgentEntityReply> {
        let persisted = match self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, state)
            .await
        {
            Ok(persisted) => persisted,
            Err(error) => {
                if matches!(error, DurableError::RevisionConflict { .. }) {
                    // Someone else wrote this agent's state, so the cached record
                    // is stale and every further transition computed from it would
                    // be wrong. Drop it: the next command reloads the
                    // authoritative record instead of failing forever against a
                    // revision that no longer exists.
                    self.record = None;
                    self.recovered = false;
                }
                return Err(error.into());
            }
        };
        let outcome = persisted.state.outcome();
        self.record = Some(persisted);
        Ok(AgentEntityReply::Applied { outcome })
    }

    fn ensure_recovered(&self) -> AgentEntityResult<()> {
        if self.recovered {
            Ok(())
        } else {
            Err(AgentEntityError::NotRecovered {
                scope: self.scope.clone(),
            })
        }
    }
}

/// Actor-backed host of one sharded agent entity.
///
/// The actor is a routing and recovery shell: every decision lives in
/// [`AgentEntityStore`] and every durable fact lives in the state store, so the
/// entity can passivate after any command and recover on another pod
/// ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
///
/// An entity id that does not parse into an [`AgentScope`] cannot address a
/// durable record, so such an entity rejects every command instead of guessing a
/// scope.
pub struct AgentEntity<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    entity: Result<AgentEntityStore<Store>, AgentIdentityError>,
}

impl<Store> AgentEntity<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    /// Creates an entity for one sharded entity id.
    #[must_use]
    pub fn new(entity_id: &EntityId, store: Store, policy: AgentSchemaPolicy) -> Self {
        let entity = AgentScope::from_entity_id(entity_id)
            .map(|scope| AgentEntityStore::new(scope, store).with_schema_policy(policy));
        Self { entity }
    }
}

impl<Store> Actor for AgentEntity<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    type Msg = AgentEntityMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            let AgentEntityMessage { command, reply_to } = msg;
            let reply = match &mut self.entity {
                Err(error) => {
                    AgentEntityReply::rejected(&AgentEntityError::Identity(error.clone()))
                }
                Ok(entity) => match apply_recovered(entity, command).await {
                    Ok(reply) => reply,
                    Err(error) => AgentEntityReply::rejected(&error),
                },
            };
            let _reply_dropped = reply_to.reply(reply);
            Ok(ActorAction::Continue)
        })
    }
}

async fn apply_recovered<Store>(
    entity: &mut AgentEntityStore<Store>,
    command: AgentEntityCommand,
) -> AgentEntityResult<AgentEntityReply>
where
    Store: DurableStateStore<AgentEntityState>,
{
    // Recovery is lazy and idempotent: the first command after activation loads
    // the authoritative state, which is exactly what an entity re-materialized on
    // a new shard owner must do before it transitions.
    if entity.state().is_err() {
        entity.recover().await?;
    }
    entity.apply(command).await
}

/// Entity type key of the agent entity.
pub type AgentEntityTypeKey = EntityTypeKey<AgentEntityMessage>;

/// Registration returned after initializing sharded agent entities.
pub type AgentEntityRegistration = EntityTypeRegistration<AgentEntityMessage>;

/// Sharded reference to one agent entity.
pub type AgentEntityRef = ShardedEntityRef<AgentEntityMessage>;

/// Sharding settings for agent entities.
#[derive(Clone)]
pub struct AgentEntityShardingSettings {
    key: AgentEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    schema_policy: AgentSchemaPolicy,
}

impl Debug for AgentEntityShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEntityShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("actor_options", &self.actor_options)
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("buffer_config", &self.buffer_config)
            .field(
                "passivation_buffer_duration",
                &self.passivation_buffer_duration,
            )
            .field("schema_policy", &self.schema_policy)
            .finish()
    }
}

impl AgentEntityShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_ENTITY_PASSIVATION_BUFFER_DURATION,
            schema_policy: AgentSchemaPolicy::default(),
        }
    }

    /// Entity type key used for agent entities.
    #[must_use]
    pub const fn key(&self) -> &AgentEntityTypeKey {
        &self.key
    }

    /// Sets options used when each agent entity actor is spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for quiescent agent entities.
    #[must_use]
    pub const fn with_idle_passivation(mut self, timeout: Duration) -> Self {
        self.idle_passivation_timeout = Some(timeout);
        self
    }

    /// Disables idle passivation.
    #[must_use]
    pub const fn without_idle_passivation(mut self) -> Self {
        self.idle_passivation_timeout = None;
        self
    }

    /// Configures bounded buffering during shard handoff and passivation.
    #[must_use]
    pub fn with_buffering(mut self, config: ShardBufferConfig) -> Self {
        self.buffer_config = Some(config);
        self
    }

    /// Disables shard-level buffering.
    #[must_use]
    pub const fn without_buffering(mut self) -> Self {
        self.buffer_config = None;
        self
    }

    /// Sets how long explicit passivation buffers incoming messages.
    #[must_use]
    pub const fn with_passivation_buffer_duration(mut self, duration: Duration) -> Self {
        self.passivation_buffer_duration = duration;
        self
    }

    /// Uses an explicit schema-compatibility policy for hosted entities.
    #[must_use]
    pub const fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }
}

impl Default for AgentEntityShardingSettings {
    fn default() -> Self {
        Self::new(agent_entity_type_key())
    }
}

/// Creates the default sharded entity type key for agent entities.
#[must_use]
pub fn agent_entity_type_key() -> AgentEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_ENTITY_TYPE)
}

/// Maps an agent scope to its sharded entity id.
#[must_use]
pub fn agent_entity_id(scope: &AgentScope) -> EntityId {
    scope.entity_id()
}

/// Initializes node-local sharded agent entities.
pub fn init_agent_entity_sharding<Store>(
    sharding: &ClusterSharding,
    store: Store,
    settings: AgentEntityShardingSettings,
) -> ClusterShardingResult<AgentEntityRegistration>
where
    Store: DurableStateStore<AgentEntityState>,
{
    sharding.init(agent_entity(store, &settings))
}

/// Initializes sharded agent entities that a non-owning node can command over
/// `rakka-remote`.
///
/// The serializable [`AgentEntityCommand`] crosses the wire and is paired with a
/// node-local reply channel on the owner. The application registers the payload
/// codecs for [`AgentEntityCommand`] and [`AgentEntityReply`] with the node
/// runtime's serialization registry.
pub fn init_agent_entity_remote_sharding<Store>(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    store: Store,
    settings: AgentEntityShardingSettings,
) -> ClusterNodeRuntimeResult<AgentEntityRegistration>
where
    Store: DurableStateStore<AgentEntityState>,
{
    let entity = agent_entity(store, &settings);
    sharding.init_remote_with_ask(
        runtime,
        entity,
        |command: AgentEntityCommand, reply_to: ReplyTo<AgentEntityReply>| AgentEntityMessage {
            command,
            reply_to,
        },
    )
}

fn agent_entity<Store>(
    store: Store,
    settings: &AgentEntityShardingSettings,
) -> Entity<
    AgentEntityMessage,
    AgentEntity<Store>,
    impl Fn(EntityContext<AgentEntityMessage>) -> AgentEntity<Store> + Send + Sync + 'static,
>
where
    Store: DurableStateStore<AgentEntityState>,
{
    let schema_policy = settings.schema_policy;
    let mut entity = Entity::of(settings.key.clone(), move |context| {
        AgentEntity::new(context.entity_id(), store.clone(), schema_policy)
    })
    .with_actor_options(settings.actor_options.clone())
    .with_passivation_buffer_duration(settings.passivation_buffer_duration);

    if let Some(timeout) = settings.idle_passivation_timeout {
        entity = entity.with_idle_passivation(timeout);
    }
    if let Some(buffer_config) = settings.buffer_config.clone() {
        entity = entity.with_buffering(buffer_config);
    } else {
        entity = entity.without_buffering();
    }
    entity
}

/// Returns a sharded reference to one agent entity.
pub fn agent_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentEntityTypeKey,
    scope: &AgentScope,
) -> ClusterShardingResult<AgentEntityRef> {
    sharding.entity_ref_for(key, scope.key())
}

/// Returns a sharded reference to one agent entity from an entity registration.
#[must_use]
pub fn registered_agent_entity_ref(
    registration: &AgentEntityRegistration,
    scope: &AgentScope,
) -> AgentEntityRef {
    registration.entity_ref_for(scope.key())
}

/// Explicitly passivates one local agent entity.
pub fn passivate_agent_entity(
    sharding: &ClusterSharding,
    key: &AgentEntityTypeKey,
    scope: &AgentScope,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &scope.entity_id())
}

/// Rejection of an agent entity command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEntityError {
    /// The entity id or a scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// A definition, settings, or setup revision was rejected.
    Definition(AgentDefinitionError),
    /// An autonomy admission decision was rejected.
    Admission(AgentAdmissionError),
    /// The durable store rejected a load or write.
    Persistence(DurableError),
    /// A command reached the entity before its state was recovered.
    NotRecovered {
        /// Scope of the entity.
        scope: AgentScope,
    },
    /// The command requires an instantiated agent.
    NotInstantiated {
        /// Scope of the entity.
        scope: AgentScope,
    },
    /// The agent is already instantiated.
    AlreadyInstantiated {
        /// Scope of the entity.
        scope: AgentScope,
    },
    /// The agent is permanently retired and accepts no further transition.
    Terminated {
        /// Scope of the entity.
        scope: AgentScope,
    },
    /// The settings update expected a revision that is no longer current.
    StaleSettingsRevision {
        /// Revision the caller expected to be current.
        expected: AgentRevisionNumber,
        /// Revision that is actually current.
        current: AgentRevisionNumber,
    },
    /// The lifecycle command expected a lifecycle revision that is no longer
    /// current, so applying it would reorder over a decision the caller never
    /// saw.
    StaleLifecycleRevision {
        /// Lifecycle revision the caller expected to be current.
        expected: AgentRevisionNumber,
        /// Lifecycle revision that is actually current.
        current: AgentRevisionNumber,
    },
    /// The admission decision evaluated a revision the agent is no longer on,
    /// so it admits something other than what would run.
    StaleAdmissionRevision {
        /// Which record the decision named a stale revision of.
        record: &'static str,
        /// Revision the decision evaluated.
        admitted: AgentRevisionNumber,
        /// Revision that is actually current.
        current: AgentRevisionNumber,
    },
}

impl AgentEntityError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Definition(error) => error.code(),
            Self::Admission(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::NotRecovered { .. } => "agent-not-recovered",
            Self::NotInstantiated { .. } => "agent-not-instantiated",
            Self::AlreadyInstantiated { .. } => "agent-already-instantiated",
            Self::Terminated { .. } => "agent-terminated",
            Self::StaleSettingsRevision { .. } => "stale-settings-revision",
            Self::StaleLifecycleRevision { .. } => "stale-lifecycle-revision",
            Self::StaleAdmissionRevision { .. } => "stale-admission-revision",
        }
    }
}

impl Display for AgentEntityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Definition(error) => Display::fmt(error, f),
            Self::Admission(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::NotRecovered { scope } => {
                write!(f, "agent {scope} was commanded before its state recovered")
            }
            Self::NotInstantiated { scope } => write!(f, "agent {scope} is not instantiated"),
            Self::AlreadyInstantiated { scope } => {
                write!(f, "agent {scope} is already instantiated")
            }
            Self::Terminated { scope } => {
                write!(
                    f,
                    "agent {scope} is terminated and accepts no further transition"
                )
            }
            Self::StaleSettingsRevision { expected, current } => write!(
                f,
                "the settings update expected revision {expected} but revision {current} is current"
            ),
            Self::StaleLifecycleRevision { expected, current } => write!(
                f,
                "the lifecycle command expected lifecycle revision {expected} but revision {current} is current"
            ),
            Self::StaleAdmissionRevision {
                record,
                admitted,
                current,
            } => write!(
                f,
                "the admission decision evaluated {record} revision {admitted} but revision {current} is current"
            ),
        }
    }
}

impl Error for AgentEntityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Definition(error) => Some(error),
            Self::Admission(error) => Some(error),
            Self::Persistence(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentEntityError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentEntityError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentAdmissionError> for AgentEntityError {
    fn from(error: AgentAdmissionError) -> Self {
        Self::Admission(error)
    }
}

impl From<AgentDefinitionError> for AgentEntityError {
    fn from(error: AgentDefinitionError) -> Self {
        Self::Definition(error)
    }
}

impl From<DurableError> for AgentEntityError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}
