//! Tenant-scoped agent identities and stable operation identifiers.
//!
//! Owns the newtype identities of the agent domain — [`AgentId`],
//! [`AgentGoalId`], [`AgentTaskId`], [`AgentRunId`], [`AgentDelegationId`],
//! [`AgentWakeId`], [`AgentEnvironmentRef`], and [`KnowledgeSpaceId`] — which
//! stay distinct types even where their initial values coincide, the composite
//! scope keys that address the sharded entities, and the construction helpers
//! for the stable operation and deduplication identifiers every durable exchange
//! keys on.
//!
//! Specification: sections 6.1 through 6.10. The wake and knowledge-space scopes
//! are fixed here even though no runtime uses them yet, so the memory and
//! continuous-goal milestones cannot bake in an incompatible scope later.
//!
//! # Why identifiers are validated
//!
//! Every scope key is a composite: `(TenantId, AgentId)` addresses the agent
//! entity, `(TenantId, AgentId, AgentRunId)` addresses a run, and both are
//! flattened into a sharding [`EntityId`] and a durable [`PersistenceId`]. A
//! composite key is only sound if the flattening is injective, so identifier
//! values may not contain the scope separator, the persistence-id separator, or
//! control characters, and they are bounded in length. Construction and
//! deserialization both enforce this: a malformed identifier fails closed rather
//! than aliasing two tenants onto one durable record.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentCommandId, AgentDeduplicationKey, AgentTenantId};
use rakka_persistence::PersistenceId;
use rakka_sharding::EntityId;
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

/// Security and data-isolation boundary every durable agent record is scoped by
/// ([specification 6.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The durable substrate already tags its records with
/// [`AgentTenantId`], so the agent domain reuses that identity rather than
/// minting a parallel one. Its value is validated wherever it enters a composite
/// scope key.
pub type TenantId = AgentTenantId;

/// Separator between the segments of a composite scope key.
pub const AGENT_SCOPE_SEPARATOR: char = '/';

/// Separator reserved by [`PersistenceId`] between an entity type and entity id.
///
/// Identifier values may not contain it, so a scope key can always be flattened
/// into a durable persistence id. `rakka-persistence` exposes it as a string
/// constant; the crate-level test `persistence_id_separator_is_reserved` keeps
/// this character in step with it.
pub const AGENT_PERSISTENCE_SEPARATOR: char = '|';

/// Maximum length, in bytes, of one identifier value.
pub const AGENT_IDENTITY_MAX_LENGTH: usize = 256;

/// Result type for identity construction and scope-key parsing.
pub type AgentIdentityResult<T> = Result<T, AgentIdentityError>;

/// Rejects an identifier value that cannot safely key a durable composite scope.
///
/// The value must be non-empty, at most [`AGENT_IDENTITY_MAX_LENGTH`] bytes, and
/// free of control characters, the [`AGENT_SCOPE_SEPARATOR`], and the
/// persistence-id separator.
pub fn validate_identity_segment(field: &'static str, value: &str) -> AgentIdentityResult<()> {
    if value.is_empty() {
        return Err(AgentIdentityError::Empty { field });
    }
    if value.len() > AGENT_IDENTITY_MAX_LENGTH {
        return Err(AgentIdentityError::TooLong {
            field,
            length: value.len(),
            maximum: AGENT_IDENTITY_MAX_LENGTH,
        });
    }
    if let Some(character) = value.chars().find(|character| character.is_control()) {
        return Err(AgentIdentityError::ControlCharacter { field, character });
    }
    for reserved in [AGENT_SCOPE_SEPARATOR, AGENT_PERSISTENCE_SEPARATOR] {
        if value.contains(reserved) {
            return Err(AgentIdentityError::ReservedCharacter {
                field,
                character: reserved,
            });
        }
    }
    Ok(())
}

/// Rejects a tenant value that cannot safely key a durable composite scope.
///
/// [`TenantId`] is owned by the durable substrate and constructed infallibly, so
/// its value is validated where it enters an agent scope key instead.
pub fn validate_tenant(tenant: &TenantId) -> AgentIdentityResult<()> {
    validate_identity_segment("tenant_id", tenant.as_str())
}

macro_rules! validated_id {
    ($(#[$meta:meta])* $vis:vis $name:ident, $field:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
        #[serde(transparent)]
        $vis struct $name(String);

        impl $name {
            /// Field name reported by identity validation errors.
            pub const FIELD: &'static str = $field;

            /// Creates the identifier, rejecting a value that cannot key a
            /// durable composite scope.
            pub fn new(
                value: impl Into<String>,
            ) -> $crate::identity::AgentIdentityResult<Self> {
                let value = value.into();
                $crate::identity::validate_identity_segment(Self::FIELD, &value)?;
                Ok(Self(value))
            }

            /// Returns the identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identifier and returns its owned string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = $crate::identity::AgentIdentityError;

            fn try_from(value: String) -> $crate::identity::AgentIdentityResult<Self> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $crate::identity::AgentIdentityError;

            fn try_from(value: &str) -> $crate::identity::AgentIdentityResult<Self> {
                Self::new(value)
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(<D::Error as serde::de::Error>::custom)
            }
        }
    };
}

pub(crate) use validated_id;

validated_id! {
    /// Stable identity of one configured logical agent
    /// ([specification 6.2](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Stable across runs, activation, passivation, owner changes, pod loss, and
    /// shard movement.
    pub AgentId, "agent_id"
}

validated_id! {
    /// Identity of one top-level collaborative goal
    /// ([specification 6.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A goal is distinct from the agents and runs that contribute to it. Its
    /// value may be generated from the root [`AgentTaskId`], but the types stay
    /// distinct so goal coordination can move to a dedicated entity without
    /// changing the public contract.
    pub AgentGoalId, "agent_goal_id"
}

impl AgentGoalId {
    /// Derives the goal identity from the root task that coordinates it
    /// ([specification 6.3](../../../docs/plans/rakka-agent/spec.md), open
    /// decision 14's resolved default).
    ///
    /// Infallible by construction: both identities validate under the same
    /// [`validate_identity_segment`] rules, and the task id already passed
    /// them. The value coincides; the types and semantics stay distinct, so
    /// goal coordination can later move to a dedicated entity without changing
    /// the public contract.
    #[must_use]
    pub fn for_root_task(task: &AgentTaskId) -> Self {
        Self(task.as_str().to_owned())
    }
}

validated_id! {
    /// Identity of one durable, typed unit of work and its eventual public
    /// result ([specification 6.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Stable across assignment, handoff, reassignment, agent restart, and
    /// changes in the [`AgentRunId`] currently executing it. It maps one-to-one
    /// to an A2A `Task.id`.
    pub AgentTaskId, "agent_task_id"
}

validated_id! {
    /// Identity of one autonomous execution session
    /// ([specification 6.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A run belongs to exactly one `(TenantId, AgentId, AgentTaskId)` and is
    /// independently recoverable. The task binding is fixed at construction by
    /// [`AgentRunBinding`]; handoff and reassignment create a new run rather than
    /// re-targeting an existing one.
    ///
    /// This is the agent-domain run identity. It is deliberately distinct from
    /// the workflow-substrate run id of the same name, which identifies one
    /// durable workflow run.
    pub AgentRunId, "agent_run_id"
}

validated_id! {
    /// Identity of one durable assignment of work from a parent run to a
    /// specialist agent
    /// ([specification 6.6](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Fixed at M1 so the delegation graph of M4 cannot change the scope keys
    /// that earlier records already persist. Replaying one delegation resolves to
    /// the same logical child or to an explicit conflict.
    pub AgentDelegationId, "agent_delegation_id"
}

validated_id! {
    /// Identity of one durable handoff of a task from a source run to a
    /// target agent
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Derived by [`crate::coordination::handoff_id_for`] as a pure function
    /// of the source run's `(turn, slot)` coordinate. It doubles verbatim as
    /// the A2A message id and deduplication key of the handoff send, so
    /// replaying one handoff resolves to the same recorded transfer or to an
    /// explicit conflict, never to a second one.
    pub AgentHandoffId, "agent_handoff_id"
}

validated_id! {
    /// Identity of one durable workflow-tool invocation
    /// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Derived by [`crate::workflow_tool::workflow_invocation_id_for`] as a
    /// pure function of the parent run's `(turn, slot)` coordinate. It doubles
    /// verbatim as the child workflow run id and the `StartRun` deduplication
    /// key, so replaying one invocation creates or adopts the same durable
    /// child run rather than a second one.
    pub AgentWorkflowInvocationId, "agent_workflow_invocation_id"
}

validated_id! {
    /// Identity of one durable logical wake occurrence that may admit a
    /// continuous-goal epoch
    /// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Fixed at M1 so the continuous controller of M3 inherits a stable
    /// occurrence identity. It must be stable across scanner restart, pod loss,
    /// passivation, duplicate trigger delivery, and shard movement.
    pub AgentWakeId, "agent_wake_id"
}

validated_id! {
    /// Authorized reference to an application-owned shared resource, workspace,
    /// event stream, or other changing world state
    /// ([specification 6.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a logical reference: it never contains resolved credentials. Access
    /// and mutation happen through declared tools and effects.
    pub AgentEnvironmentRef, "agent_environment_ref"
}

validated_id! {
    /// Identity of one communal knowledge-graph boundary
    /// ([specification 6.8](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Fixed at M1 so the communal memory of M2 inherits a stable scope. The
    /// default space is tenant- or organization-scoped; cross-tenant sharing
    /// requires an explicit federation design.
    pub KnowledgeSpaceId, "knowledge_space_id"
}

validated_id! {
    /// Identity of one appended communal claim, as the graph store recorded
    /// it ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A mirror newtype: the graph crate owns the derived `ClaimId` and
    /// depends on this crate, so the append receipt carries the id in this
    /// crate's own validated form rather than importing the graph's.
    pub AgentCommunalClaimId, "agent_communal_claim_id"
}

/// Durable scope of one agent entity: `(TenantId, AgentId)`
/// ([specification 6.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the sharding key of [`crate::agent::AgentEntity`] and the namespace
/// of the agent's private long-term memory. It serializes as its flattened key
/// string, so a persisted scope is re-validated and re-parsed on load rather than
/// trusted field by field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentScope {
    tenant: TenantId,
    agent: AgentId,
}

impl AgentScope {
    /// Creates an agent scope, validating the tenant value.
    pub fn new(tenant: TenantId, agent: AgentId) -> AgentIdentityResult<Self> {
        validate_tenant(&tenant)?;
        Ok(Self { tenant, agent })
    }

    /// Tenant boundary of this agent.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Agent identity within the tenant.
    #[must_use]
    pub const fn agent(&self) -> &AgentId {
        &self.agent
    }

    /// Flattened, injective key string for this scope.
    #[must_use]
    pub fn key(&self) -> String {
        join_segments(&[self.tenant.as_str(), self.agent.as_str()])
    }

    /// Sharded entity id addressing this agent.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        EntityId::new(self.key())
    }

    /// Durable persistence id of this agent's entity state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new(format!("{AGENT_ENTITY_PERSISTENCE_PREFIX}:{}", self.key()))
    }

    /// Namespace of this agent's private long-term memory
    /// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The namespace is derived from the scope rather than stored independently,
    /// so private memory can never be addressed outside its owning tenant and
    /// agent.
    #[must_use]
    pub fn memory_namespace(&self) -> AgentMemoryNamespace {
        AgentMemoryNamespace(format!(
            "{AGENT_MEMORY_NAMESPACE_PREFIX}{AGENT_SCOPE_SEPARATOR}{}",
            self.key()
        ))
    }

    /// Parses a flattened scope key, failing closed on a malformed value.
    pub fn parse(key: &str) -> AgentIdentityResult<Self> {
        let [tenant, agent] = split_segments(SCOPE_FIELD_AGENT, key)?;
        Self::new(TenantId::new(tenant), AgentId::new(agent)?)
    }

    /// Parses the scope back out of a sharded entity id.
    pub fn from_entity_id(entity_id: &EntityId) -> AgentIdentityResult<Self> {
        Self::parse(entity_id.as_str())
    }
}

impl Display for AgentScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key())
    }
}

/// Durable scope of one typed task: `(TenantId, AgentTaskId)`
/// ([specification 6.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the sharding key of the task entity delivered by slice 1.4. It is
/// defined here because the agent domain's operation identifiers already key on
/// it, and its scope must not change once records exist.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentTaskScope {
    tenant: TenantId,
    task: AgentTaskId,
}

impl AgentTaskScope {
    /// Creates a task scope, validating the tenant value.
    pub fn new(tenant: TenantId, task: AgentTaskId) -> AgentIdentityResult<Self> {
        validate_tenant(&tenant)?;
        Ok(Self { tenant, task })
    }

    /// Tenant boundary of this task.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Task identity within the tenant.
    #[must_use]
    pub const fn task(&self) -> &AgentTaskId {
        &self.task
    }

    /// Flattened, injective key string for this scope.
    #[must_use]
    pub fn key(&self) -> String {
        join_segments(&[self.tenant.as_str(), self.task.as_str()])
    }

    /// Sharded entity id addressing this task.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        EntityId::new(self.key())
    }

    /// Durable persistence id of this task's entity state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new(format!(
            "{AGENT_TASK_ENTITY_PERSISTENCE_PREFIX}:{}",
            self.key()
        ))
    }

    /// Parses a flattened scope key, failing closed on a malformed value.
    pub fn parse(key: &str) -> AgentIdentityResult<Self> {
        let [tenant, task] = split_segments(SCOPE_FIELD_TASK, key)?;
        Self::new(TenantId::new(tenant), AgentTaskId::new(task)?)
    }

    /// Parses the scope back out of a sharded entity id.
    pub fn from_entity_id(entity_id: &EntityId) -> AgentIdentityResult<Self> {
        Self::parse(entity_id.as_str())
    }
}

impl Display for AgentTaskScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key())
    }
}

/// Durable scope of one run: `(TenantId, AgentId, AgentRunId)`
/// ([specification 6.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the sharding key of the run entity delivered by slice 1.5 and the
/// namespace of that run's short-term session memory. The task a run serves is
/// not part of its address; it is fixed by [`AgentRunBinding`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentRunScope {
    tenant: TenantId,
    agent: AgentId,
    run: AgentRunId,
}

impl AgentRunScope {
    /// Creates a run scope, validating the tenant value.
    pub fn new(tenant: TenantId, agent: AgentId, run: AgentRunId) -> AgentIdentityResult<Self> {
        validate_tenant(&tenant)?;
        Ok(Self { tenant, agent, run })
    }

    /// Tenant boundary of this run.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Agent executing this run.
    #[must_use]
    pub const fn agent(&self) -> &AgentId {
        &self.agent
    }

    /// Run identity within the agent.
    #[must_use]
    pub const fn run(&self) -> &AgentRunId {
        &self.run
    }

    /// Scope of the agent that owns this run.
    #[must_use]
    pub fn agent_scope(&self) -> AgentScope {
        AgentScope {
            tenant: self.tenant.clone(),
            agent: self.agent.clone(),
        }
    }

    /// Flattened, injective key string for this scope.
    #[must_use]
    pub fn key(&self) -> String {
        join_segments(&[self.tenant.as_str(), self.agent.as_str(), self.run.as_str()])
    }

    /// Sharded entity id addressing this run.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        EntityId::new(self.key())
    }

    /// Durable persistence id of this run's entity state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new(format!(
            "{AGENT_RUN_ENTITY_PERSISTENCE_PREFIX}:{}",
            self.key()
        ))
    }

    /// Parses a flattened scope key, failing closed on a malformed value.
    pub fn parse(key: &str) -> AgentIdentityResult<Self> {
        let [tenant, agent, run] = split_segments(SCOPE_FIELD_RUN, key)?;
        Self::new(
            TenantId::new(tenant),
            AgentId::new(agent)?,
            AgentRunId::new(run)?,
        )
    }

    /// Parses the scope back out of a sharded entity id.
    pub fn from_entity_id(entity_id: &EntityId) -> AgentIdentityResult<Self> {
        Self::parse(entity_id.as_str())
    }
}

impl Display for AgentRunScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key())
    }
}

/// The immutable binding of one run to the single task it serves
/// ([specification 6.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// A run stays bound to one [`AgentTaskId`] for its entire lifetime, so the task
/// is a constructor argument and there is no setter to re-target it. Handoff and
/// reassignment create a new run; parallel work uses multiple independently
/// sharded runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentRunBinding {
    scope: AgentRunScope,
    task: AgentTaskId,
    goal: Option<AgentGoalId>,
}

impl AgentRunBinding {
    /// Binds a run to the one task it will serve for its whole lifetime.
    #[must_use]
    pub const fn new(scope: AgentRunScope, task: AgentTaskId) -> Self {
        Self {
            scope,
            task,
            goal: None,
        }
    }

    /// Records the collaborative goal this run contributes to.
    #[must_use]
    pub fn with_goal(mut self, goal: AgentGoalId) -> Self {
        self.goal = Some(goal);
        self
    }

    /// Sharding scope of the bound run.
    #[must_use]
    pub const fn scope(&self) -> &AgentRunScope {
        &self.scope
    }

    /// Task this run serves. Fixed for the run's lifetime.
    #[must_use]
    pub const fn task(&self) -> &AgentTaskId {
        &self.task
    }

    /// Collaborative goal this run contributes to, when it has one.
    #[must_use]
    pub const fn goal(&self) -> Option<&AgentGoalId> {
        self.goal.as_ref()
    }

    /// Task scope this run reports its result proposals to.
    pub fn task_scope(&self) -> AgentIdentityResult<AgentTaskScope> {
        AgentTaskScope::new(self.scope.tenant().clone(), self.task.clone())
    }
}

/// Namespace of one agent's private long-term memory, derived from its scope.
///
/// Constructed only by [`AgentScope::memory_namespace`], so a memory namespace
/// cannot be minted outside the tenant and agent that own it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentMemoryNamespace(String);

impl AgentMemoryNamespace {
    /// Returns the namespace as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for AgentMemoryNamespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Prefix of the durable persistence id of an agent entity's state.
pub const AGENT_ENTITY_PERSISTENCE_PREFIX: &str = "agent-entity";

/// Prefix of the durable persistence id of a task entity's state.
pub const AGENT_TASK_ENTITY_PERSISTENCE_PREFIX: &str = "agent-task-entity";

/// Prefix of the durable persistence id of a run entity's state.
pub const AGENT_RUN_ENTITY_PERSISTENCE_PREFIX: &str = "agent-run-entity";

/// Prefix of an agent-private memory namespace.
pub const AGENT_MEMORY_NAMESPACE_PREFIX: &str = "agent-memory";

const SCOPE_FIELD_AGENT: &str = "agent scope";
const SCOPE_FIELD_TASK: &str = "task scope";
const SCOPE_FIELD_RUN: &str = "run scope";

fn join_segments(segments: &[&str]) -> String {
    let mut key = String::new();
    for (index, segment) in segments.iter().enumerate() {
        if index > 0 {
            key.push(AGENT_SCOPE_SEPARATOR);
        }
        key.push_str(segment);
    }
    key
}

fn split_segments<'a, const N: usize>(
    field: &'static str,
    key: &'a str,
) -> AgentIdentityResult<[&'a str; N]> {
    let segments: Vec<&str> = key.split(AGENT_SCOPE_SEPARATOR).collect();
    <[&str; N]>::try_from(segments.as_slice()).map_err(|_| AgentIdentityError::MalformedScopeKey {
        field,
        expected_segments: N,
        actual_segments: segments.len(),
    })
}

/// Serializes a composite scope as its flattened key, so a persisted scope is
/// re-parsed and re-validated on load instead of being trusted field by field.
macro_rules! scope_serde {
    ($name:ident) => {
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.key())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let key = String::deserialize(deserializer)?;
                Self::parse(&key).map_err(DeserializeError::custom)
            }
        }
    };
}

scope_serde!(AgentScope);
scope_serde!(AgentTaskScope);
scope_serde!(AgentRunScope);

/// Class of durable operation a stable operation id names
/// ([specification 6.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// The kind is the first segment of every [`AgentOperationId`], so two
/// operations of different classes can never collide even when their remaining
/// segments coincide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentOperationKind {
    /// An accepted command entering an entity's durable inbox.
    Command,
    /// Creation of a typed task.
    TaskCreation,
    /// Assignment of a task to an agent for one assignment generation.
    Assignment,
    /// Creation of a run for one assignment generation.
    RunCreation,
    /// A run's durable acceptance of its assignment.
    RunAcceptance,
    /// A run's proposal of a typed task result.
    ResultProposal,
    /// A task's validation decision on a result proposal.
    ResultDecision,
    /// A parent-local escrow allocation debit.
    BudgetAllocation,
    /// A run-local dispatch-time budget reservation.
    BudgetReservation,
    /// Settlement or return of budget to a parent scope.
    BudgetSettlement,
    /// Dispatch of one durable effect generation.
    EffectDispatch,
    /// Issue of a dispatch grant for one effect intent.
    DispatchGrant,
    /// An idempotent memory write.
    MemoryWrite,
    /// Resolution of a durable checkpoint.
    CheckpointResolution,
    /// An outbound A2A send.
    A2aSend,
    /// Admission of one continuous-goal wake occurrence.
    WakeAdmission,
    /// Admission of one continuous-goal epoch.
    EpochAdmission,
    /// A completed epoch returning its result to the controller.
    EpochResult,
    /// One goal evaluation: the committed effect, its record, and the
    /// exchange that carries the record to the coordinating task.
    GoalEvaluation,
    /// Append of one communal knowledge-graph claim.
    ClaimAppend,
    /// Durable delegation of work to a specialist agent.
    Delegation,
    /// A delegated child task returning its terminal outcome to the parent
    /// run that created it.
    DelegationResult,
    /// A child workflow run returning its terminal outcome to the parent run
    /// that invoked it.
    WorkflowResult,
    /// Durable handoff of a task to another agent.
    Handoff,
    /// A team member's claim on a shared task-board item.
    TeamClaim,
    /// One moderated conversation turn.
    ConversationTurn,
    /// Publication of a new agent definition revision.
    DefinitionUpdate,
    /// Acceptance of a settings update.
    SettingsUpdate,
    /// An administrative lifecycle command over an agent.
    LifecycleCommand,
    /// Acceptance of an agent-suspension lifecycle command.
    LifecycleSuspend,
    /// Acceptance of an agent-resumption lifecycle command.
    LifecycleResume,
    /// Acceptance of an agent-termination lifecycle command.
    LifecycleTerminate,
    /// A cancellation request or propagation step.
    Cancellation,
}

impl AgentOperationKind {
    /// Stable kebab-case label used as the first segment of an operation id.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::TaskCreation => "task-creation",
            Self::Assignment => "assignment",
            Self::RunCreation => "run-creation",
            Self::RunAcceptance => "run-acceptance",
            Self::ResultProposal => "result-proposal",
            Self::ResultDecision => "result-decision",
            Self::BudgetAllocation => "budget-allocation",
            Self::BudgetReservation => "budget-reservation",
            Self::BudgetSettlement => "budget-settlement",
            Self::EffectDispatch => "effect-dispatch",
            Self::DispatchGrant => "dispatch-grant",
            Self::MemoryWrite => "memory-write",
            Self::CheckpointResolution => "checkpoint-resolution",
            Self::A2aSend => "a2a-send",
            Self::WakeAdmission => "wake-admission",
            Self::EpochAdmission => "epoch-admission",
            Self::EpochResult => "epoch-result",
            Self::GoalEvaluation => "goal-evaluation",
            Self::ClaimAppend => "claim-append",
            Self::Delegation => "delegation",
            Self::DelegationResult => "delegation-result",
            Self::WorkflowResult => "workflow-result",
            Self::Handoff => "handoff",
            Self::TeamClaim => "team-claim",
            Self::ConversationTurn => "conversation-turn",
            Self::DefinitionUpdate => "definition-update",
            Self::SettingsUpdate => "settings-update",
            Self::LifecycleCommand => "lifecycle-command",
            Self::LifecycleSuspend => "lifecycle-suspend",
            Self::LifecycleResume => "lifecycle-resume",
            Self::LifecycleTerminate => "lifecycle-terminate",
            Self::Cancellation => "cancellation",
        }
    }
}

impl Display for AgentOperationKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Stable identifier of one durable operation
/// ([specification 6.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Replaying an accepted operation must not produce a second state transition or
/// logical write, which requires the identifier to be *derived*, not generated:
/// the same logical operation reconstructed on any node, after any restart, from
/// any trigger path, must yield the same value. The construction is therefore a
/// pure function of the operation kind and an ordered list of validated segments,
/// and it is injective because no segment may contain the separator.
///
/// The identifier converts to the substrate's [`AgentDeduplicationKey`] and
/// [`AgentCommandId`], so the durable inbox and outbox deduplicate on the very
/// same value the agent domain reasons about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AgentOperationId(String);

impl AgentOperationId {
    /// Derives an operation id from its kind and ordered segments.
    ///
    /// At least one segment is required, and every segment is validated as an
    /// identity segment so the flattening stays injective.
    pub fn new<I, S>(kind: AgentOperationKind, segments: I) -> AgentIdentityResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut value = String::from(kind.as_label());
        let mut count = 0_usize;
        for segment in segments {
            let segment = segment.as_ref();
            validate_identity_segment(OPERATION_SEGMENT_FIELD, segment)?;
            value.push(AGENT_SCOPE_SEPARATOR);
            value.push_str(segment);
            count += 1;
        }
        if count == 0 {
            return Err(AgentIdentityError::Empty {
                field: OPERATION_SEGMENT_FIELD,
            });
        }
        Ok(Self(value))
    }

    /// Derives an operation id scoped to one agent.
    ///
    /// The discriminator distinguishes operations of the same kind against the
    /// same agent — a settings revision number, a causation id, or another value
    /// that the initiator can reconstruct after a crash.
    pub fn for_agent(
        kind: AgentOperationKind,
        scope: &AgentScope,
        discriminator: impl AsRef<str>,
    ) -> AgentIdentityResult<Self> {
        Self::new(
            kind,
            [
                scope.tenant().as_str(),
                scope.agent().as_str(),
                discriminator.as_ref(),
            ],
        )
    }

    /// Returns the operation id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the operation id and returns its owned string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }

    /// Deduplication key used by the durable inbox and outbox for this
    /// operation.
    #[must_use]
    pub fn deduplication_key(&self) -> AgentDeduplicationKey {
        AgentDeduplicationKey::new(self.0.clone())
    }

    /// Command id used when this operation enters an entity's durable inbox.
    #[must_use]
    pub fn command_id(&self) -> AgentCommandId {
        AgentCommandId::new(self.0.clone())
    }
}

impl Display for AgentOperationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AgentOperationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let mut segments = value.split(AGENT_SCOPE_SEPARATOR);
        let kind = segments
            .next()
            .filter(|kind| !kind.is_empty())
            .ok_or_else(|| DeserializeError::custom("operation id is missing its kind segment"))?;
        validate_identity_segment(OPERATION_SEGMENT_FIELD, kind)
            .map_err(DeserializeError::custom)?;

        let mut count = 0_usize;
        for segment in segments {
            validate_identity_segment(OPERATION_SEGMENT_FIELD, segment)
                .map_err(DeserializeError::custom)?;
            count += 1;
        }
        if count == 0 {
            return Err(DeserializeError::custom(
                "operation id carries no discriminating segments",
            ));
        }
        Ok(Self(value))
    }
}

const OPERATION_SEGMENT_FIELD: &str = "operation_segment";

/// Rejection of an identifier or composite scope key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentIdentityError {
    /// The value was empty.
    Empty {
        /// Field that carried the value.
        field: &'static str,
    },
    /// The value exceeded the maximum identifier length.
    TooLong {
        /// Field that carried the value.
        field: &'static str,
        /// Length of the rejected value, in bytes.
        length: usize,
        /// Maximum accepted length, in bytes.
        maximum: usize,
    },
    /// The value contained a character reserved for composing scope keys.
    ReservedCharacter {
        /// Field that carried the value.
        field: &'static str,
        /// Reserved character found in the value.
        character: char,
    },
    /// The value contained a control character.
    ControlCharacter {
        /// Field that carried the value.
        field: &'static str,
        /// Control character found in the value.
        character: char,
    },
    /// A flattened scope key did not have the expected number of segments.
    MalformedScopeKey {
        /// Scope that failed to parse.
        field: &'static str,
        /// Number of segments the scope requires.
        expected_segments: usize,
        /// Number of segments the key actually carried.
        actual_segments: usize,
    },
}

impl AgentIdentityError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Empty { .. } => "empty-identifier",
            Self::TooLong { .. } => "identifier-too-long",
            Self::ReservedCharacter { .. } => "reserved-identifier-character",
            Self::ControlCharacter { .. } => "control-character-in-identifier",
            Self::MalformedScopeKey { .. } => "malformed-scope-key",
        }
    }

    /// Field that carried the rejected value.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::Empty { field }
            | Self::TooLong { field, .. }
            | Self::ReservedCharacter { field, .. }
            | Self::ControlCharacter { field, .. }
            | Self::MalformedScopeKey { field, .. } => field,
        }
    }
}

impl Display for AgentIdentityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(f, "{field} must not be empty"),
            Self::TooLong {
                field,
                length,
                maximum,
            } => write!(
                f,
                "{field} is {length} bytes, which exceeds the {maximum} byte identifier limit"
            ),
            Self::ReservedCharacter { field, character } => write!(
                f,
                "{field} must not contain the reserved scope character {character:?}"
            ),
            Self::ControlCharacter { field, character } => write!(
                f,
                "{field} must not contain the control character {character:?}"
            ),
            Self::MalformedScopeKey {
                field,
                expected_segments,
                actual_segments,
            } => write!(
                f,
                "{field} key must have {expected_segments} segments but had {actual_segments}"
            ),
        }
    }
}

impl Error for AgentIdentityError {}
