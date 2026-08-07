//! Durable delegation and the collaboration graph.
//!
//! Owns the [`AgentDelegationRecord`]: one durable assignment of work from a
//! parent run to a specialist agent, persisted in the same compare-and-set
//! that commits the outbound send effect — strictly before any dispatch — so
//! that a replay resolves to the same child or to an explicit conflict rather
//! than a second child ([specification 6.6]). The record's identities are pure
//! derivations of the parent run's `(turn, slot)` coordinate: the delegation
//! id doubles as the A2A message id and deduplication key, and the receiving
//! surface derives the child task id from that key, so every retry of one
//! logical delegation converges on one logical child.
//!
//! A peer is reached only through the outbox and `rakka-a2a`, carrying the
//! versioned collaboration metadata. The model cannot reach a peer through a
//! generic tool: it may request a *skill* through the one declared
//! coordination tool the loop intercepts, and the application-owned
//! [`AgentDelegationCatalog`] resolves the concrete agent, endpoint, and
//! contract inside the transition that persists the record. Replays reuse the
//! recorded resolution verbatim and never re-resolve, which is what makes
//! catalog drift a visible conflict instead of a silent second child.
//!
//! [`crate::identity::AgentOperationKind::Delegation`] and
//! [`crate::identity::AgentOperationKind::A2aSend`] remain reserved rather
//! than consumed here: the receiving surface keeps its single task-creation
//! ingress path, so a delegated creation and a plain A2A creation cannot
//! diverge, and the delegation's own convergence rests on the derived
//! deduplication key instead of a second operation class.
//!
//! Slice 4.4 enforces the coordinator limits against this shape: depth,
//! fan-out, and concurrency ceilings check the run's envelope and durable
//! cells at the delegation door; the descendants ceiling is the conserved
//! [`crate::AgentBudgetDimension::Descendants`] escrow dimension, spent one
//! plus [`AgentDelegationRecord::granted_descendants`] per committed
//! delegation; and cycle rejection compares a resolved target against the
//! validated ancestor-agent chain ([`AgentDelegationRecord::ancestors`]) at
//! agent-identity granularity — the escape hatch for deliberately bounded
//! iterative protocols ([specification 8.4]) is deferred with a refusal-only
//! default. Unused child sub-quota is *not* credited back on a child's
//! terminal result yet: the spend is the grant, conservatively, until a
//! later slice turns crediting on inside the result-ingestion transition.
//! Durable cancellation propagation to children rides the in-fabric
//! `DelegationCancel` exchange: the parent run owes one request per created,
//! unsettled child when its own scope is cancelled — or when a resolved
//! fan-in group left the child behind — and the child's terminal outcome
//! still returns as its `DelegationResult`
//! ([specification 8.7](../../docs/plans/rakka-agent/spec.md)).
//!
//! Specification: sections 8.4, 6.6, 8.7, and 14.4. Filled by slices 4.3
//! (this shape), 4.4 (fan-in and limits), and 4.6.
//!
//! [specification 8.4]: ../../../docs/plans/rakka-agent/spec.md
//!
//! [specification 6.6]: ../../../docs/plans/rakka-agent/spec.md

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;

use rakka_agent_workflow::{AgentEffectId, AgentTelemetryContext, AgentTimestampMillis};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::definition::{
    AgentCapabilityId, AgentCoordinationCapabilityKind, AgentCredentialBindingRef,
    AgentRevisionNumber, AgentTaskDefinitionId, AgentToolId,
};
use crate::goal::AgentGoalDelegationBudget;
use crate::identity::{
    validate_tenant, AgentDelegationId, AgentGoalId, AgentId, AgentIdentityError, AgentOperationId,
    AgentOperationKind, AgentRunId, AgentRunScope, AgentTaskId, TenantId,
};
use crate::task::{AgentContentDigest, AgentSchemaRef, AgentTaskContent, AgentTaskStatus};

/// Result type for delegation construction and validation.
pub type AgentDelegationResult<T> = Result<T, AgentDelegationError>;

/// Prefix of every derived [`AgentDelegationId`].
///
/// The suffix is a fixed-length digest, so the id always satisfies the
/// identity bounds whatever the parent scope contains, and an id without this
/// prefix was not derived by [`delegation_id_for`].
pub const AGENT_DELEGATION_ID_PREFIX: &str = "delegation-";

/// Maximum delegation cells one run retains.
///
/// A structural bound on the run's durable state, not a policy ceiling: the
/// goal's [`AgentGoalDelegationBudget`] fan-out and descendant dimensions are
/// enforced by a later slice, and this constant only keeps the cell map from
/// growing without limit until then.
pub const AGENT_RUN_MAX_DELEGATIONS: usize = 16;

/// Maximum serialized bytes of one [`AgentDelegationRecord`].
///
/// The record rides the parent run's bounded durable state, so an input that
/// does not fit inline belongs behind an artifact reference — the same rule
/// the task record applies to its own content.
pub const AGENT_DELEGATION_RECORD_MAX_BYTES: usize = 8 * 1024;

/// Maximum ancestors one delegation's lineage carries.
///
/// Lineage exists so a later slice can reject cycles and enforce depth
/// ceilings; a chain deeper than this bound is refused at record validation
/// long before any policy ceiling could see it.
pub const AGENT_DELEGATION_MAX_LINEAGE: usize = 16;

/// Maximum serialized bytes of one [`AgentTaskDelegationProvenance`].
///
/// The provenance arrives over the network and rides the child task's
/// bounded durable record, so the receiving surface holds it to the same
/// discipline the parent's own record obeys: a peer cannot inflate a child's
/// durable state with scope or binding collections the parent-side byte
/// bound would never have admitted.
pub const AGENT_DELEGATION_PROVENANCE_MAX_BYTES: usize = 8 * 1024;

/// Maximum bytes of one resolved logical endpoint reference.
pub const AGENT_DELEGATION_ENDPOINT_MAX_BYTES: usize = 256;

/// Default attempt ceiling of the outbound A2A send effect.
///
/// The send is idempotent by construction — the derived deduplication key
/// makes every retry converge on the same logical child — so retrying a
/// transient transport failure is safe and cheap.
pub const AGENT_A2A_SEND_DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Derives the identity of the delegation one run's turn commits in one slot.
///
/// The derivation is pure — the same digest construction as the wake
/// identity: every segment is length-prefixed so the encoding is injective —
/// and shares the `(turn, slot)` coordinate with the effect that carries the
/// send, so replaying the transition that decided the delegation resolves to
/// the same identity, the same A2A message id, and the same deduplication
/// key.
pub fn delegation_id_for(
    scope: &AgentRunScope,
    turn: u64,
    slot: usize,
) -> AgentDelegationResult<AgentDelegationId> {
    validate_tenant(scope.tenant())?;
    let digest = AgentContentDigest::sha256_of_segments([
        scope.tenant().as_str(),
        scope.agent().as_str(),
        scope.run().as_str(),
        &turn.to_string(),
        &slot.to_string(),
    ]);
    Ok(AgentDelegationId::new(format!(
        "{AGENT_DELEGATION_ID_PREFIX}{}",
        digest.value
    ))?)
}

/// Derives the stable operation id of the one delegation-result exchange a
/// child ever owes its parent.
///
/// Pure over `(tenant, delegation)`: the child's terminal status is
/// absorbing, so one logical result exists per delegation, ever, and every
/// re-drive after any loss owes the identical operation
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
pub fn delegation_result_operation_id(
    tenant: &TenantId,
    delegation: &AgentDelegationId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::DelegationResult,
        [tenant.as_str(), delegation.as_str(), "result"],
    )
}

/// Derives the stable operation id of the one delegation-cancel exchange a
/// parent run ever owes one delegated child.
///
/// Pure over `(tenant, delegation)`: a cancellation request is absorbing —
/// the child records it once and answers every replay from its marker — so
/// one logical request exists per delegation, ever, and every re-drive after
/// any loss owes the identical operation
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
pub fn delegation_cancel_operation_id(
    tenant: &TenantId,
    delegation: &AgentDelegationId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::Cancellation,
        [tenant.as_str(), delegation.as_str(), "delegation-cancel"],
    )
}

/// Rejects a depth that does not agree with the lineage.
///
/// A coherent chain records the full lineage above the delegation, so the
/// depth is always the ancestor count plus one. The check keeps the depth
/// the enforcement slices ceiling against non-forgeable: a sender cannot
/// claim an arbitrary depth while presenting a shorter chain.
fn check_depth_coherence(depth: u32, lineage: &[AgentDelegationId]) -> AgentDelegationResult<()> {
    let expected = u32::try_from(lineage.len())
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    if depth != expected {
        return Err(AgentDelegationError::DepthIncoherent {
            depth,
            ancestors: lineage.len(),
        });
    }
    Ok(())
}

/// Rejects an ancestor-agent chain that does not agree with the lineage.
///
/// The two chains are parallel: `ancestors[i]` is the agent that committed
/// `lineage[i]`, so a non-empty ancestry must match the lineage entry for
/// entry. An empty ancestry is tolerated for compatibility — a chain recorded
/// before the field existed — and the cycle check then refuses to *extend*
/// such a chain ([`AgentDelegationError::AncestryUnknown`]) rather than
/// trusting a gap a peer could hide an ancestor in.
fn check_ancestry_coherence(
    ancestors: &[AgentId],
    lineage: &[AgentDelegationId],
) -> AgentDelegationResult<()> {
    if !ancestors.is_empty() && ancestors.len() != lineage.len() {
        return Err(AgentDelegationError::AncestryIncoherent {
            ancestors: ancestors.len(),
            lineage: lineage.len(),
        });
    }
    Ok(())
}

/// The concrete target an application-owned catalog resolved for a requested
/// skill ([specification 8.4](../../../docs/plans/rakka-agent/spec.md), open
/// decision 15).
///
/// A target carries logical identity and the typed contract only. The
/// endpoint is a logical reference the A2A adapter understands — never a
/// resolved credential — and the credential bindings are references resolved
/// at dispatch time by the executor's own boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDelegationTarget {
    /// The specialist agent that serves the skill.
    pub agent: AgentId,
    /// Logical endpoint reference, when the target is not served by the
    /// deployment's default peer surface.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// The typed task definition the child is created under.
    pub task_definition: AgentTaskDefinitionId,
    /// Capability scopes delegated to the child.
    #[serde(default)]
    pub capability_scopes: BTreeSet<AgentCapabilityId>,
    /// Logical credential-binding references the child's tools may resolve.
    #[serde(default)]
    pub credential_bindings: Vec<AgentCredentialBindingRef>,
    /// The versioned output schema the child's result must satisfy.
    #[serde(default)]
    pub result_schema: Option<AgentSchemaRef>,
    /// The target contract revision the catalog resolved as compatible.
    #[serde(default)]
    pub compatibility: Option<AgentRevisionNumber>,
    /// The communal knowledge spaces the catalog explicitly delegates to
    /// this specialist ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The application's durable statement of "explicitly delegated": the
    /// interception intersects it with the parent's own effective grant, so a
    /// catalog can never widen what the parent holds. Empty means the
    /// specialist is delegated no communal access.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub knowledge_spaces: BTreeSet<crate::identity::KnowledgeSpaceId>,
}

impl AgentDelegationTarget {
    /// Creates a resolved target for an agent serving a task definition.
    #[must_use]
    pub fn new(agent: AgentId, task_definition: AgentTaskDefinitionId) -> Self {
        Self {
            agent,
            endpoint: None,
            task_definition,
            capability_scopes: BTreeSet::new(),
            credential_bindings: Vec::new(),
            result_schema: None,
            compatibility: None,
            knowledge_spaces: BTreeSet::new(),
        }
    }

    /// Sets the logical endpoint reference.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Explicitly delegates one communal knowledge space to the specialist
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn with_knowledge_space(mut self, space: crate::identity::KnowledgeSpaceId) -> Self {
        self.knowledge_spaces.insert(space);
        self
    }

    /// Sets the versioned output schema.
    #[must_use]
    pub fn with_result_schema(mut self, schema: AgentSchemaRef) -> Self {
        self.result_schema = Some(schema);
        self
    }

    /// Rejects a target whose endpoint exceeds its bound.
    pub fn validate(&self) -> AgentDelegationResult<()> {
        if let Some(endpoint) = &self.endpoint {
            if endpoint.is_empty() || endpoint.len() > AGENT_DELEGATION_ENDPOINT_MAX_BYTES {
                return Err(AgentDelegationError::TargetInvalid {
                    message: format!(
                        "the endpoint reference is {} bytes; it must be non-empty and at most {} \
                         bytes",
                        endpoint.len(),
                        AGENT_DELEGATION_ENDPOINT_MAX_BYTES
                    ),
                });
            }
        }
        Ok(())
    }
}

/// Why a catalog could not resolve a requested skill.
///
/// Every variant is definitive for the request that produced it: the
/// interception records a failed tool result under the variant's stable code
/// and the run continues, so the model can correct course.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentDelegationResolutionError {
    /// No target serves the requested skill.
    UnknownSkill {
        /// The skill the model requested.
        skill: AgentCapabilityId,
    },
    /// A target exists but the caller is not authorized to reach it.
    NotAuthorized {
        /// The skill the model requested.
        skill: AgentCapabilityId,
    },
    /// More than one target serves the skill and the catalog refuses to guess.
    Ambiguous {
        /// The skill the model requested.
        skill: AgentCapabilityId,
    },
    /// The catalog could not answer for an application-defined reason.
    Unavailable {
        /// Stable machine-readable code.
        code: String,
        /// Bounded human-readable detail.
        message: String,
    },
}

impl AgentDelegationResolutionError {
    /// Stable machine-readable code.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::UnknownSkill { .. } => "delegation-skill-unknown",
            Self::NotAuthorized { .. } => "delegation-skill-not-authorized",
            Self::Ambiguous { .. } => "delegation-target-ambiguous",
            Self::Unavailable { code, .. } => code,
        }
    }
}

impl Display for AgentDelegationResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSkill { skill } => {
                write!(f, "no delegation target serves the skill {skill}")
            }
            Self::NotAuthorized { skill } => {
                write!(
                    f,
                    "the caller is not authorized to delegate the skill {skill}"
                )
            }
            Self::Ambiguous { skill } => write!(
                f,
                "more than one delegation target serves the skill {skill}; the catalog refuses \
                 to guess"
            ),
            Self::Unavailable { code, message } => {
                write!(
                    f,
                    "the delegation catalog could not answer ({code}): {message}"
                )
            }
        }
    }
}

impl Error for AgentDelegationResolutionError {}

/// Application-owned resolution of a requested skill to a concrete target
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md), open
/// decision 15).
///
/// Resolution is synchronous because it happens inside the compare-and-set
/// that persists the delegation record: the record stores both the requested
/// skill and the resolved target, and replays reuse the recorded resolution
/// verbatim. An application whose lookup is asynchronous materializes its
/// catalog ahead of the loop.
pub trait AgentDelegationCatalog: Send + Sync {
    /// Resolves a requested skill to a concrete delegation target.
    fn resolve(
        &self,
        tenant: &TenantId,
        skill: &AgentCapabilityId,
    ) -> Result<AgentDelegationTarget, AgentDelegationResolutionError>;
}

/// A fixed skill-to-target catalog for tests and simple deployments.
#[derive(Debug, Clone, Default)]
pub struct StaticAgentDelegationCatalog {
    targets: BTreeMap<AgentCapabilityId, AgentDelegationTarget>,
}

impl StaticAgentDelegationCatalog {
    /// Creates an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a target serving a skill, replacing any previous target for it.
    #[must_use]
    pub fn with_target(mut self, skill: AgentCapabilityId, target: AgentDelegationTarget) -> Self {
        self.targets.insert(skill, target);
        self
    }
}

impl AgentDelegationCatalog for StaticAgentDelegationCatalog {
    fn resolve(
        &self,
        _tenant: &TenantId,
        skill: &AgentCapabilityId,
    ) -> Result<AgentDelegationTarget, AgentDelegationResolutionError> {
        self.targets.get(skill).cloned().ok_or_else(|| {
            AgentDelegationResolutionError::UnknownSkill {
                skill: skill.clone(),
            }
        })
    }
}

/// The bounded request the model may make through the declared coordination
/// tool.
///
/// This is the *entire* vocabulary model output has over delegation: a skill,
/// a bounded input, and an optional deadline. Unknown fields fail the parse,
/// so an agent id, endpoint, budget, or scope in model output is refused
/// rather than ignored — the catalog and the goal's own envelope decide
/// those.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDelegationToolCall {
    /// The skill the model requests.
    pub skill: AgentCapabilityId,
    /// The bounded input the child task is created with.
    pub input: Value,
    /// An optional deadline for the child, in epoch milliseconds.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
}

impl AgentDelegationToolCall {
    /// Parses the coordination tool's arguments, failing closed on anything
    /// beyond the declared vocabulary.
    pub fn parse(arguments: &Value) -> AgentDelegationResult<Self> {
        serde_json::from_value(arguments.clone()).map_err(|error| {
            AgentDelegationError::InvalidArguments {
                message: error.to_string(),
            }
        })
    }
}

/// One durable assignment of work from a parent run to a specialist agent
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Persisted in the same compare-and-set that commits the send effect,
/// strictly before any dispatch. Every identity below is either a pure
/// derivation of the parent's `(turn, slot)` coordinate or the recorded
/// output of the one catalog resolution this delegation ever performs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDelegationRecord {
    /// The delegation identity, derived by [`delegation_id_for`].
    pub delegation: AgentDelegationId,
    /// The collaborative goal the parent serves, when it serves one.
    #[serde(default)]
    pub goal: Option<AgentGoalId>,
    /// The parent task whose run delegates.
    pub parent_task: AgentTaskId,
    /// The delegating run.
    pub parent_run: AgentRunScope,
    /// Ancestor delegations, oldest first. Empty for a root's own children.
    #[serde(default)]
    pub lineage: Vec<AgentDelegationId>,
    /// The agent that committed each lineage entry, oldest first — parallel to
    /// [`Self::lineage`], entry for entry. The delegating run's own agent is
    /// not repeated here: it rides as [`Self::parent_run`], which is what
    /// closes the chain for cycle rejection. Empty on a record committed
    /// before the field existed; validation refuses a non-empty ancestry
    /// whose length disagrees with the lineage.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ancestors: Vec<AgentId>,
    /// Depth of the child below the root: the parent's own depth plus one,
    /// which is always the lineage length plus one — validation refuses a
    /// depth that does not agree with the recorded chain.
    pub depth: u32,
    /// The skill the model requested.
    pub requested_skill: AgentCapabilityId,
    /// The target the catalog resolved, recorded so a replay never
    /// re-resolves.
    pub resolved: AgentDelegationTarget,
    /// The A2A message id of the send: the delegation id verbatim.
    pub a2a_message_id: String,
    /// The deduplication key the receiving surface derives the child task
    /// from: the delegation id verbatim.
    pub deduplication_key: String,
    /// The parent turn that committed the delegation.
    pub turn: u64,
    /// The effect slot within the turn.
    pub slot: usize,
    /// The send effect's derived identity.
    pub effect: AgentEffectId,
    /// The model tool call this delegation answers — its causation. The
    /// outcome transition records the send's bounded confirmation as this
    /// call's tool result, which is how the turn completes.
    pub call_id: crate::model::AgentToolCallId,
    /// The bounded input the child task is created with.
    pub input: AgentTaskContent,
    /// The versioned output schema the child's result must satisfy, when the
    /// resolution carries one.
    #[serde(default)]
    pub result_schema: Option<AgentSchemaRef>,
    /// The narrowed delegation budget the child runs under: the parent's own
    /// ceilings min-narrowed per field, with `max_descendants` replaced by the
    /// escrowed sub-quota ([`Self::granted_descendants`]). It crosses A2A as
    /// validated provenance the child's own admission caps against — never a
    /// conserved escrow grant, which cannot ride A2A.
    #[serde(default)]
    pub budget: Option<AgentGoalDelegationBudget>,
    /// The conserved descendant sub-quota this delegation debited from the
    /// parent run's ledger *beyond* the child itself: the child's own subtree
    /// allowance, carried to the child as the wire budget's `max_descendants`.
    /// The delegation's total descendant cost is therefore one plus this.
    ///
    /// `None` on a record committed before the dimension existed. Under a
    /// bounded parent an untagged live cell makes the remaining headroom
    /// unknowable, and further delegation refuses — deny-when-unknown.
    #[serde(default)]
    pub granted_descendants: Option<u64>,
    /// The child's deadline, in epoch milliseconds.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
    /// Environments the parent's goal scope narrowed tool use to, carried to
    /// the child verbatim ([specification 8.5](../../../docs/plans/rakka-agent/spec.md));
    /// empty means no narrowing was carried.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub environments: BTreeSet<crate::identity::AgentEnvironmentRef>,
    /// The communal knowledge spaces explicitly delegated to the child: the
    /// catalog's statement intersected with the parent's own effective grant
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)); empty
    /// means none.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub knowledge_spaces: BTreeSet<crate::identity::KnowledgeSpaceId>,
    /// The agent definition revision the parent decided under.
    pub definition_revision: AgentRevisionNumber,
    /// The agent settings revision the parent decided under.
    pub settings_revision: AgentRevisionNumber,
    /// Trace propagation for the send.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
    /// When the record was committed.
    pub created_at: AgentTimestampMillis,
}

impl AgentDelegationRecord {
    /// Rejects a record that exceeds its structural bounds, or whose depth
    /// does not agree with its lineage.
    ///
    /// The whole record refuses rather than truncating: a delegation whose
    /// input does not fit inline belongs behind an artifact reference, and a
    /// lineage deeper than the structural bound is a graph no policy ceiling
    /// should ever have admitted.
    pub fn validate(&self) -> AgentDelegationResult<()> {
        self.resolved.validate()?;
        if self.lineage.len() > AGENT_DELEGATION_MAX_LINEAGE {
            return Err(AgentDelegationError::LineageTooDeep {
                length: self.lineage.len(),
                maximum: AGENT_DELEGATION_MAX_LINEAGE,
            });
        }
        check_depth_coherence(self.depth, &self.lineage)?;
        check_ancestry_coherence(&self.ancestors, &self.lineage)?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| AgentDelegationError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_DELEGATION_RECORD_MAX_BYTES {
            return Err(AgentDelegationError::RecordTooLarge {
                bytes,
                maximum: AGENT_DELEGATION_RECORD_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// Where one delegation stands.
///
/// `Pending` is the only unsettled state: the record is durable and the send
/// effect is committed, but no outcome has returned. Every other variant is
/// settled and absorbing for this delegation identity — recovery after
/// ambiguity uses a *new* delegation, never a resurrected one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDelegationStatus {
    /// The record is persisted and the send effect is committed or in flight.
    Pending,
    /// The send returned the durably created child.
    ChildCreated {
        /// The child task the delegation resolved to.
        child_task: AgentTaskId,
        /// The child's initial run, when the receiving surface reported one.
        #[serde(default)]
        child_run: Option<AgentRunId>,
    },
    /// The send resolved to an explicit conflict: a child exists that this
    /// delegation's identity does not own
    /// ([specification 6.6](../../../docs/plans/rakka-agent/spec.md)).
    Conflicted {
        /// Stable machine-readable conflict code.
        code: String,
    },
    /// The send failed definitively without creating a child.
    Failed {
        /// Stable machine-readable failure code.
        code: String,
    },
}

impl AgentDelegationStatus {
    /// Whether the delegation reached a settled state.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self, Self::Pending)
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ChildCreated { .. } => "child-created",
            Self::Conflicted { .. } => "conflicted",
            Self::Failed { .. } => "failed",
        }
    }
}

/// The bounded terminal outcome one child returned through the durable
/// delegation-result exchange ([specification 8.4]: the record carries the
/// child's status, result/evidence references, and terminal reason).
///
/// References only, never content: the digest fingerprints the child's
/// accepted result and the child task id — already on the cell's
/// `ChildCreated` status — is the authorized-query handle for anything more
/// ([`crate::query::authorized_agent_goal_view`] assembles the goal-wide
/// view those handles key into).
///
/// [specification 8.4]: ../../../docs/plans/rakka-agent/spec.md
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDelegationChildResult {
    /// The child task's terminal status.
    pub status: AgentTaskStatus,
    /// The child's stable terminal-reason code, when it recorded one.
    #[serde(default)]
    pub terminal_reason: Option<String>,
    /// Content digest of the child's accepted result, when one was accepted.
    #[serde(default)]
    pub result_digest: Option<AgentContentDigest>,
    /// The child run that served the terminal assignment, when known.
    #[serde(default)]
    pub child_run: Option<AgentRunId>,
    /// Descendant tasks the child's own subtree created, excluding the child
    /// itself. Recorded for a later slice to credit unused sub-quota back;
    /// slice 4.4 never credits — the spend stays the grant, conservatively.
    #[serde(default)]
    pub descendants_created: u64,
    /// When the parent recorded the result.
    pub recorded_at: AgentTimestampMillis,
}

/// The settled outcome of the one delegation-cancel exchange a parent ever
/// owes one child ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is the durable once-guard past the journal's bounded deduplication
/// window — the reason [`AgentDelegationCell::cancel`] exists at all — and
/// the observable outcome the propagation request records. Acceptance means
/// the child durably recorded the request, never that its started effects
/// stopped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDelegationCancelOutcome {
    /// The child durably recorded the request, or was already terminal.
    Accepted {
        /// When the receipt settled on the parent.
        settled_at: AgentTimestampMillis,
    },
    /// The child refused definitively under this stable code.
    Refused {
        /// The child's refusal code.
        code: String,
        /// When the refusal settled on the parent.
        settled_at: AgentTimestampMillis,
    },
}

/// One delegation's durable home on the parent run's loop state.
///
/// The cell commits with the send effect and settles in the same
/// compare-and-set that applies the effect's outcome, so the record, the
/// effect, and the status can never disagree about what happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDelegationCell {
    /// The durable record, persisted before the send.
    pub record: Box<AgentDelegationRecord>,
    /// Where the delegation stands.
    pub status: AgentDelegationStatus,
    /// When the status settled, when it has.
    #[serde(default)]
    pub settled_at: Option<AgentTimestampMillis>,
    /// The child's terminal outcome, once its result returned. First writer
    /// wins: one logical result per delegation, ever.
    #[serde(default)]
    pub result: Option<AgentDelegationChildResult>,
    /// The settled outcome of the delegation-cancel exchange this cell's
    /// child was chased with, once one settled
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    /// Records persisted before this field load without one.
    #[serde(default)]
    pub cancel: Option<AgentDelegationCancelOutcome>,
}

impl AgentDelegationCell {
    /// Creates the pending cell committed alongside the send effect.
    #[must_use]
    pub fn pending(record: Box<AgentDelegationRecord>) -> Self {
        Self {
            record,
            status: AgentDelegationStatus::Pending,
            settled_at: None,
            result: None,
            cancel: None,
        }
    }

    /// Records the settled outcome of the child's delegation-cancel exchange,
    /// first-writer-wins: one logical request per delegation, ever.
    pub fn record_cancel_outcome(&mut self, outcome: AgentDelegationCancelOutcome) {
        if self.cancel.is_none() {
            self.cancel = Some(outcome);
        }
    }

    /// Whether this cell's delegation-cancel was definitively refused.
    ///
    /// The child's settle rule accepts only the two forged/not-delegated
    /// codes as definitive, and both prove the addressed task carries no
    /// provenance naming this delegation — so it will never return a
    /// delegation result either, and a winding-down parent must not wait on
    /// it ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn cancel_refused(&self) -> bool {
        matches!(
            self.cancel,
            Some(AgentDelegationCancelOutcome::Refused { .. })
        )
    }

    /// Whether the child this cell created has recorded its terminal outcome.
    ///
    /// A cell that never created a child — settled `Conflicted` or `Failed` —
    /// answers `false`; its settlement, not a child result, is what released
    /// its debits.
    #[must_use]
    pub const fn child_settled(&self) -> bool {
        self.result.is_some()
    }

    /// Records the child's terminal outcome, first-writer-wins: a duplicate
    /// delivery of one logical result cannot rewrite history.
    pub fn record_child_result(&mut self, result: AgentDelegationChildResult) {
        if self.result.is_none() {
            self.result = Some(result);
        }
    }

    /// Settles the cell with the durably created child.
    ///
    /// Settlement is first-writer-wins: a cell that already settled keeps its
    /// original outcome, so a duplicate outcome delivery cannot rewrite
    /// history.
    pub fn settle_child_created(
        &mut self,
        child_task: AgentTaskId,
        child_run: Option<AgentRunId>,
        now: AgentTimestampMillis,
    ) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentDelegationStatus::ChildCreated {
            child_task,
            child_run,
        };
        self.settled_at = Some(now);
    }

    /// Settles the cell with an explicit conflict.
    pub fn settle_conflicted(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentDelegationStatus::Conflicted { code: code.into() };
        self.settled_at = Some(now);
    }

    /// Settles the cell with a definitive failure.
    pub fn settle_failed(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentDelegationStatus::Failed { code: code.into() };
        self.settled_at = Some(now);
    }
}

/// The delegation provenance a child task records at creation
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is the receiving side of the collaboration metadata: which delegation
/// created this task, under which parent, at what depth, with which delegated
/// scopes and advisory budget. Recorded and bounded in this slice; the
/// ceiling, cycle, and cancellation slices enforce against it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskDelegationProvenance {
    /// The delegation that created this task.
    pub delegation: AgentDelegationId,
    /// The parent task whose run delegated.
    pub parent_task: AgentTaskId,
    /// The delegating run.
    pub parent_run: AgentRunScope,
    /// The parent's ancestor delegations, oldest first — the chain *above*
    /// [`Self::delegation`], which is not repeated here.
    #[serde(default)]
    pub lineage: Vec<AgentDelegationId>,
    /// The agent that committed each lineage entry, oldest first — parallel
    /// to [`Self::lineage`]. The delegating agent itself rides as
    /// [`Self::parent_run`], which closes the chain: this task's own runs
    /// reject a delegation that resolves back to any agent in
    /// `ancestors ∪ {parent_run.agent()}` ∪ their own. Empty when the parent
    /// predates the field; validation refuses a non-empty ancestry whose
    /// length disagrees with the lineage.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ancestors: Vec<AgentId>,
    /// Depth of this task below the root: always the lineage length plus
    /// one — validation refuses a depth that does not agree with the
    /// recorded chain.
    pub depth: u32,
    /// The skill the parent requested.
    pub requested_skill: AgentCapabilityId,
    /// Capability scopes the parent delegated.
    #[serde(default)]
    pub capability_scopes: BTreeSet<AgentCapabilityId>,
    /// Logical credential-binding references the parent delegated.
    #[serde(default)]
    pub credential_bindings: Vec<AgentCredentialBindingRef>,
    /// The versioned output schema the parent expects.
    #[serde(default)]
    pub result_schema: Option<AgentSchemaRef>,
    /// The narrowed delegation budget the parent granted, validated at the
    /// creation door. Its `max_descendants` is the parent-escrowed sub-quota
    /// this task's own delegation authority is capped to — a cap the child's
    /// admission min-narrows below its own definition ceilings, never a
    /// conserved escrow grant: budgets cannot ride A2A.
    #[serde(default)]
    pub budget: Option<AgentGoalDelegationBudget>,
    /// The deadline the parent set for this task.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
    /// Environments the delegating scope narrowed tool use to
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)); empty
    /// means no narrowing was carried.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub environments: BTreeSet<crate::identity::AgentEnvironmentRef>,
    /// The communal knowledge spaces explicitly delegated to this task
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)). Empty
    /// means none were — including a provenance recorded before the field
    /// existed, which is exactly the fail-closed reading: nothing that old
    /// could have appended communally either.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub knowledge_spaces: BTreeSet<crate::identity::KnowledgeSpaceId>,
}

impl AgentTaskDelegationProvenance {
    /// Rejects a provenance whose lineage or serialized size exceeds its
    /// structural bounds, or whose depth does not agree with its lineage.
    ///
    /// The size bound is the receiving side of the parent's own record
    /// bound: every field here arrived from a peer, so the whole provenance
    /// refuses rather than truncating, exactly as the record does. Depth
    /// coherence is the enforcement slices' input hygiene: a peer cannot
    /// claim an arbitrary depth while presenting a shorter chain.
    pub fn validate(&self) -> AgentDelegationResult<()> {
        if self.lineage.len() > AGENT_DELEGATION_MAX_LINEAGE {
            return Err(AgentDelegationError::LineageTooDeep {
                length: self.lineage.len(),
                maximum: AGENT_DELEGATION_MAX_LINEAGE,
            });
        }
        check_depth_coherence(self.depth, &self.lineage)?;
        check_ancestry_coherence(&self.ancestors, &self.lineage)?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| AgentDelegationError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_DELEGATION_PROVENANCE_MAX_BYTES {
            return Err(AgentDelegationError::ProvenanceTooLarge {
                bytes,
                maximum: AGENT_DELEGATION_PROVENANCE_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// Maximum bytes of the peer status label one send receipt carries.
pub const AGENT_A2A_SEND_STATUS_MAX_BYTES: usize = 64;

/// The bounded receipt one completed outbound A2A send returns
/// ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identities and a status label only — never the child's content, and never
/// evidence that the root goal advanced: a child's terminal A2A state is
/// evidence returned to the parent, not a goal decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentA2aSendReceipt {
    /// The delegation the send carried.
    pub delegation: AgentDelegationId,
    /// The child task the receiving surface durably created or replayed.
    pub child_task: AgentTaskId,
    /// The child's initial run, when the receiving surface reported one.
    #[serde(default)]
    pub child_run: Option<AgentRunId>,
    /// The peer's bounded task-state label at the time of the send.
    pub peer_status: String,
}

impl AgentA2aSendReceipt {
    /// Rejects a receipt whose status label exceeds its bound.
    pub fn validate(&self) -> AgentDelegationResult<()> {
        if self.peer_status.len() > AGENT_A2A_SEND_STATUS_MAX_BYTES {
            return Err(AgentDelegationError::TargetInvalid {
                message: format!(
                    "the peer status label is {} bytes, which exceeds the {} byte bound",
                    self.peer_status.len(),
                    AGENT_A2A_SEND_STATUS_MAX_BYTES
                ),
            });
        }
        Ok(())
    }
}

/// The delegation authority one run's assignment carries
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run never reads the goal spec: the task's assignment decision copies
/// the goal's skill and tool sets, its delegation budget, and the task's own
/// provenance into this envelope, so the loop enforces goal-scope narrowing
/// from durable state it owns. Absent envelope means absent goal narrowing,
/// and an empty set inside the envelope means the same: the goal spec's sets
/// carry no declaredness, so narrowing is enforced only when a set is
/// non-empty — an empty set never fails closed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunDelegationEnvelope {
    /// Skills the goal may delegate to. A non-empty set closes delegation to
    /// exactly these skills; an empty set means the goal declared no skill
    /// narrowing, and any skill the catalog resolves may be requested.
    #[serde(default)]
    pub allowed_skills: BTreeSet<AgentCapabilityId>,
    /// Tools the goal may use. Empty means no goal-scope tool narrowing.
    #[serde(default)]
    pub allowed_tools: BTreeSet<AgentToolId>,
    /// Workflow tools the goal may invoke
    /// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)). Empty
    /// means no goal-scope workflow narrowing. An envelope persisted before
    /// the field decodes to the empty set — the same no-narrowing meaning.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub allowed_workflows: BTreeSet<crate::definition::AgentWorkflowToolId>,
    /// The delegation ceilings this run enforces at the delegation door:
    /// the goal spec's (root) or the task's granted provenance budget
    /// (delegated child), min-narrowed by the task definition's own ceilings
    /// either way.
    #[serde(default)]
    pub budget: Option<AgentGoalDelegationBudget>,
    /// Ancestor delegations of the task this run serves, oldest first.
    #[serde(default)]
    pub lineage: Vec<AgentDelegationId>,
    /// The agent that committed each lineage entry, oldest first — parallel
    /// to [`Self::lineage`]. For a delegated child this is the provenance's
    /// ancestry plus the delegating parent's own agent, so the cycle check
    /// compares a resolved target against `ancestors` plus the run's own
    /// agent and covers the whole chain. Empty at the root, and empty when
    /// the chain predates the field — in which case sub-delegation refuses
    /// rather than trusting a gap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ancestors: Vec<AgentId>,
    /// Depth of the task this run serves below the root. Zero for a root.
    #[serde(default)]
    pub depth: u32,
    /// The goal's own deadline, carried so children never outlive it.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
    /// The fan-in policy the goal declares for this run's fan-out groups,
    /// when it declares one; the wiring's default applies otherwise. Trusted
    /// state fixed at group open — never model output.
    #[serde(default)]
    pub fan_in: Option<crate::fan_in::AgentFanInPolicy>,
    /// Environments the goal scope narrows tool use to
    /// ([specification 8.1 and 8.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A **narrowing** dimension exactly like the allowed sets: empty means
    /// no goal-scope narrowing, and the definition/setup envelope still
    /// authorizes every access. An envelope persisted before the field
    /// decodes to the empty set — the same no-narrowing meaning.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub environments: BTreeSet<crate::identity::AgentEnvironmentRef>,
    /// The communal knowledge spaces explicitly delegated to this run
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md):
    /// children inherit only explicitly delegated access).
    ///
    /// A **grant**, deliberately diverging from the absent-means-no-narrowing
    /// rule above: `None` means no grant statement was recorded — a chain
    /// persisted before the field, or a task shape carrying none — and a
    /// claim append then fails closed when lineage exists, the
    /// deny-when-unknown posture the ancestry gap takes; `Some(set)` is the
    /// exact set appends may target, still inside the definition and setup
    /// envelopes; `Some(empty)` is explicitly nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge_spaces: Option<BTreeSet<crate::identity::KnowledgeSpaceId>>,
}

/// The wiring one run entity needs to serve delegation
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Construction refuses a configuration whose coordination set does not
/// declare [`AgentCoordinationCapabilityKind::Delegation`]: the capability is
/// trusted definition data, and requiring it here means a deployment cannot
/// wire the delegation tool while forgetting the capability that authorizes
/// it — the same construction-time obligation the guardrail chains carry.
#[derive(Clone)]
pub struct AgentRunDelegationConfig {
    /// The one declared coordination tool the loop intercepts.
    pub tool: AgentToolId,
    /// The declared await verb the loop intercepts into a fan-in close, when
    /// the deployment wires one. Unwired, the run delegates but never waits:
    /// the pre-fan-in behavior.
    pub fan_in_tool: Option<AgentToolId>,
    /// The fan-in policy a group opens under when the goal's envelope
    /// declares none. Trusted wiring, never model output.
    pub default_fan_in: crate::fan_in::AgentFanInPolicy,
    /// The application-owned catalog that resolves requested skills.
    pub catalog: Arc<dyn AgentDelegationCatalog>,
    /// The coordination capabilities the agent definition declares.
    pub coordination: BTreeSet<AgentCoordinationCapabilityKind>,
}

impl AgentRunDelegationConfig {
    /// Creates the wiring, refusing a coordination set without
    /// [`AgentCoordinationCapabilityKind::Delegation`].
    pub fn new(
        tool: AgentToolId,
        catalog: Arc<dyn AgentDelegationCatalog>,
        coordination: BTreeSet<AgentCoordinationCapabilityKind>,
    ) -> AgentDelegationResult<Self> {
        if !coordination.contains(&AgentCoordinationCapabilityKind::Delegation) {
            return Err(AgentDelegationError::CapabilityMissing);
        }
        Ok(Self {
            tool,
            fan_in_tool: None,
            default_fan_in: crate::fan_in::AgentFanInPolicy::default(),
            catalog,
            coordination,
        })
    }

    /// Declares the await verb the loop intercepts into a fan-in close.
    #[must_use]
    pub fn with_fan_in_tool(mut self, tool: AgentToolId) -> Self {
        self.fan_in_tool = Some(tool);
        self
    }

    /// Sets the fan-in policy used when the goal's envelope declares none,
    /// refusing one that can never resolve.
    pub fn with_default_fan_in(
        mut self,
        policy: crate::fan_in::AgentFanInPolicy,
    ) -> AgentDelegationResult<Self> {
        policy.validate()?;
        self.default_fan_in = policy;
        Ok(self)
    }
}

impl Debug for AgentRunDelegationConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentRunDelegationConfig")
            .field("tool", &self.tool)
            .field("coordination", &self.coordination)
            .finish_non_exhaustive()
    }
}

/// Errors of delegation construction, validation, and interception.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentDelegationError {
    /// The configuration's coordination set does not declare the delegation
    /// capability.
    CapabilityMissing,
    /// The requested skill is outside the goal's allowed set.
    SkillNotAllowed {
        /// The skill the model requested.
        skill: AgentCapabilityId,
    },
    /// The tool call's arguments do not parse as the declared vocabulary.
    InvalidArguments {
        /// The parse failure detail.
        message: String,
    },
    /// The run already retains its maximum delegation cells.
    LimitExceeded {
        /// The bound.
        maximum: usize,
    },
    /// The lineage exceeds its structural bound.
    LineageTooDeep {
        /// Actual ancestor count.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// The declared depth does not agree with the lineage.
    DepthIncoherent {
        /// The declared depth.
        depth: u32,
        /// The recorded ancestor count.
        ancestors: usize,
    },
    /// The ancestor-agent chain does not agree with the lineage.
    AncestryIncoherent {
        /// The presented ancestor-agent count.
        ancestors: usize,
        /// The presented lineage length.
        lineage: usize,
    },
    /// The chain carries lineage without ancestor agents, so cycle rejection
    /// cannot see who is above — sub-delegation refuses rather than trusting
    /// the gap.
    AncestryUnknown {
        /// The lineage length whose ancestry is missing.
        lineage: usize,
    },
    /// The child would exceed the maximum delegation depth
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
    DepthExceeded {
        /// The depth the child would take.
        depth: u32,
        /// The ceiling in force.
        maximum: u32,
    },
    /// The run would exceed its maximum direct children.
    FanOutExceeded {
        /// Direct children already committed, the planned one included.
        committed: u64,
        /// The ceiling in force.
        maximum: u32,
    },
    /// The run would exceed its maximum concurrently unsettled children.
    ConcurrencyExceeded {
        /// Children currently unsettled, the planned one included.
        active: u64,
        /// The ceiling in force.
        maximum: u32,
    },
    /// The run's escrowed descendant allocation cannot cover another child
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    DescendantsExhausted {
        /// The descendants allocation the run holds.
        limit: u64,
        /// What is already spent or held by live children, or `None` when a
        /// pre-slice cell makes the spend unaccountable — refused all the
        /// same: deny-when-unknown.
        spent: Option<u64>,
    },
    /// The resolved target already appears in the delegation chain
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md):
    /// repeated delegation lineage).
    CycleDetected {
        /// The agent the resolution cycled back to.
        agent: AgentId,
    },
    /// A quorum fan-in policy that can never resolve.
    QuorumInvalid {
        /// The declared quorum.
        n: u32,
        /// The structural membership bound.
        maximum: u32,
    },
    /// The record exceeds its serialized-size bound.
    RecordTooLarge {
        /// Actual serialized bytes.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The provenance exceeds its serialized-size bound.
    ProvenanceTooLarge {
        /// Actual serialized bytes.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The resolved target is structurally invalid.
    TargetInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// The record could not be encoded.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
    /// An identity derivation failed.
    Identity(AgentIdentityError),
}

impl AgentDelegationError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::CapabilityMissing => "delegation-capability-missing",
            Self::SkillNotAllowed { .. } => "delegation-skill-not-allowed",
            Self::InvalidArguments { .. } => "delegation-invalid-arguments",
            Self::LimitExceeded { .. } => "delegation-limit-exceeded",
            Self::LineageTooDeep { .. } => "delegation-lineage-too-deep",
            Self::DepthIncoherent { .. } => "delegation-depth-incoherent",
            Self::AncestryIncoherent { .. } => "delegation-ancestry-incoherent",
            Self::AncestryUnknown { .. } => "delegation-ancestry-unknown",
            Self::DepthExceeded { .. } => "delegation-depth-exceeded",
            Self::FanOutExceeded { .. } => "delegation-fan-out-exceeded",
            Self::ConcurrencyExceeded { .. } => "delegation-concurrency-exceeded",
            Self::DescendantsExhausted { .. } => "delegation-descendants-exhausted",
            Self::CycleDetected { .. } => "delegation-cycle-detected",
            Self::QuorumInvalid { .. } => "fan-in-quorum-invalid",
            Self::RecordTooLarge { .. } => "delegation-record-too-large",
            Self::ProvenanceTooLarge { .. } => "delegation-provenance-too-large",
            Self::TargetInvalid { .. } => "delegation-target-invalid",
            Self::Encoding { .. } => "delegation-record-unencodable",
            Self::Identity(_) => "delegation-identity-invalid",
        }
    }
}

impl Display for AgentDelegationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapabilityMissing => f.write_str(
                "the delegation configuration's coordination set does not declare the delegation \
                 capability",
            ),
            Self::SkillNotAllowed { skill } => {
                write!(f, "the goal does not allow delegating the skill {skill}")
            }
            Self::InvalidArguments { message } => write!(
                f,
                "the delegation tool call's arguments do not parse as the declared vocabulary: \
                 {message}"
            ),
            Self::LimitExceeded { maximum } => write!(
                f,
                "the run already retains its maximum of {maximum} delegations"
            ),
            Self::LineageTooDeep { length, maximum } => write!(
                f,
                "the delegation lineage carries {length} ancestors, which exceeds the {maximum} \
                 bound"
            ),
            Self::DepthIncoherent { depth, ancestors } => write!(
                f,
                "the declared depth {depth} does not agree with the {ancestors} recorded \
                 ancestors; a coherent chain declares depth {}",
                ancestors + 1
            ),
            Self::AncestryIncoherent { ancestors, lineage } => write!(
                f,
                "the chain presents {ancestors} ancestor agents against {lineage} lineage \
                 entries; a coherent chain records one agent per entry"
            ),
            Self::AncestryUnknown { lineage } => write!(
                f,
                "the chain carries {lineage} lineage entries without their ancestor agents, so \
                 cycle rejection cannot see the chain; a run under an unaccounted chain may \
                 finish its own work but not delegate further"
            ),
            Self::DepthExceeded { depth, maximum } => write!(
                f,
                "the child would sit at delegation depth {depth}, which exceeds the maximum \
                 depth {maximum}"
            ),
            Self::FanOutExceeded { committed, maximum } => write!(
                f,
                "the delegation would be this run's direct child {committed}, which exceeds the \
                 maximum fan-out {maximum}"
            ),
            Self::ConcurrencyExceeded { active, maximum } => write!(
                f,
                "the delegation would make {active} concurrently unsettled children, which \
                 exceeds the maximum {maximum}; a child counts until its send settles or its \
                 terminal result is recorded"
            ),
            Self::DescendantsExhausted { limit, spent } => match spent {
                Some(spent) => write!(
                    f,
                    "the run's descendants allocation of {limit} cannot cover another child: \
                     {spent} already spent or held by live children"
                ),
                None => write!(
                    f,
                    "the run's descendants allocation of {limit} cannot admit another child: a \
                     delegation committed before the descendants dimension existed makes the \
                     spend unaccountable, and unknown headroom is refused"
                ),
            },
            Self::CycleDetected { agent } => write!(
                f,
                "the resolved target {agent} already appears in the delegation chain; repeated \
                 delegation lineage is refused"
            ),
            Self::QuorumInvalid { n, maximum } => write!(
                f,
                "a fan-in quorum of {n} can never resolve: the quorum must be between 1 and the \
                 {maximum}-member structural bound"
            ),
            Self::RecordTooLarge { bytes, maximum } => write!(
                f,
                "the delegation record is {bytes} serialized bytes, which exceeds the {maximum} \
                 byte bound"
            ),
            Self::ProvenanceTooLarge { bytes, maximum } => write!(
                f,
                "the delegation provenance is {bytes} serialized bytes, which exceeds the \
                 {maximum} byte bound"
            ),
            Self::TargetInvalid { message } => {
                write!(f, "the resolved delegation target is invalid: {message}")
            }
            Self::Encoding { message } => {
                write!(f, "the delegation record could not be encoded: {message}")
            }
            Self::Identity(error) => write!(f, "a delegation identity derivation failed: {error}"),
        }
    }
}

impl Error for AgentDelegationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentDelegationError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("coordinator").expect("agent id"),
            AgentRunId::new("run-1").expect("run id"),
        )
        .expect("run scope")
    }

    #[test]
    fn delegation_ids_are_pure_and_distinct_per_slot() {
        let first = delegation_id_for(&scope(), 3, 0).expect("delegation id");
        let again = delegation_id_for(&scope(), 3, 0).expect("delegation id");
        let sibling = delegation_id_for(&scope(), 3, 1).expect("delegation id");
        assert_eq!(first, again);
        assert_ne!(first, sibling);
        assert!(first.as_str().starts_with(AGENT_DELEGATION_ID_PREFIX));
    }

    #[test]
    fn the_tool_call_vocabulary_fails_closed_on_unknown_fields() {
        let arguments = serde_json::json!({
            "skill": "translation",
            "input": {"text": "hello"},
            "agent": "attacker-chosen"
        });
        let error = AgentDelegationToolCall::parse(&arguments).expect_err("unknown field");
        assert_eq!(error.code(), "delegation-invalid-arguments");
    }

    #[test]
    fn an_oversized_provenance_fails_closed() {
        // Every collection below is individually valid; only their combined
        // serialized weight crosses the bound — the abuse shape a hostile
        // peer would send, refused whole rather than truncated.
        let provenance = AgentTaskDelegationProvenance {
            environments: Default::default(),
            knowledge_spaces: Default::default(),
            delegation: delegation_id_for(&scope(), 1, 0).expect("delegation id"),
            parent_task: AgentTaskId::new("ticket-1").expect("task id"),
            parent_run: scope(),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 1,
            requested_skill: AgentCapabilityId::new("translation").expect("capability"),
            capability_scopes: (0..256)
                .map(|index| {
                    AgentCapabilityId::new(format!("scope-{index:03}-{}", "x".repeat(32)))
                        .expect("capability")
                })
                .collect(),
            credential_bindings: Vec::new(),
            result_schema: None,
            budget: None,
            deadline: None,
        };
        let error = provenance
            .validate()
            .expect_err("the provenance is oversized");
        assert_eq!(error.code(), "delegation-provenance-too-large");
    }

    #[test]
    fn an_incoherent_depth_fails_closed() {
        // Depth is a pure function of the chain: a claimed depth with no
        // lineage behind it is a forgery the enforcement slices must never
        // ceiling against.
        let provenance = AgentTaskDelegationProvenance {
            environments: Default::default(),
            knowledge_spaces: Default::default(),
            delegation: delegation_id_for(&scope(), 1, 0).expect("delegation id"),
            parent_task: AgentTaskId::new("ticket-1").expect("task id"),
            parent_run: scope(),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 5,
            requested_skill: AgentCapabilityId::new("translation").expect("capability"),
            capability_scopes: BTreeSet::new(),
            credential_bindings: Vec::new(),
            result_schema: None,
            budget: None,
            deadline: None,
        };
        let error = provenance
            .validate()
            .expect_err("the depth does not agree with the lineage");
        assert_eq!(error.code(), "delegation-depth-incoherent");
    }

    #[test]
    fn construction_requires_the_delegation_capability() {
        let error = AgentRunDelegationConfig::new(
            AgentToolId::new("delegate").expect("tool id"),
            Arc::new(StaticAgentDelegationCatalog::new()),
            BTreeSet::from([AgentCoordinationCapabilityKind::Handoff]),
        )
        .expect_err("capability missing");
        assert_eq!(error.code(), "delegation-capability-missing");
    }

    #[test]
    fn a_settled_cell_keeps_its_first_outcome() {
        let record = AgentDelegationRecord {
            environments: Default::default(),
            knowledge_spaces: Default::default(),
            delegation: delegation_id_for(&scope(), 1, 0).expect("delegation id"),
            goal: None,
            parent_task: AgentTaskId::new("ticket-1").expect("task id"),
            parent_run: scope(),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 1,
            requested_skill: AgentCapabilityId::new("translation").expect("capability"),
            resolved: AgentDelegationTarget::new(
                AgentId::new("translator").expect("agent id"),
                AgentTaskDefinitionId::new("translate-document").expect("definition id"),
            ),
            a2a_message_id: "delegation-x".into(),
            deduplication_key: "delegation-x".into(),
            turn: 1,
            slot: 0,
            effect: AgentEffectId::new("effect-x"),
            call_id: crate::model::AgentToolCallId::new("call-1").expect("call id"),
            input: AgentTaskContent::inline(serde_json::json!({"text": "hello"}))
                .expect("bounded input"),
            result_schema: None,
            budget: None,
            granted_descendants: None,
            deadline: None,
            definition_revision: AgentRevisionNumber::new(1),
            settings_revision: AgentRevisionNumber::new(1),
            telemetry: AgentTelemetryContext::default(),
            created_at: AgentTimestampMillis::new(1),
        };
        record.validate().expect("bounded record");
        let mut cell = AgentDelegationCell::pending(Box::new(record));
        cell.settle_conflicted("delegation-child-conflict", AgentTimestampMillis::new(2));
        cell.settle_child_created(
            AgentTaskId::new("late").expect("task id"),
            None,
            AgentTimestampMillis::new(3),
        );
        assert_eq!(
            cell.status,
            AgentDelegationStatus::Conflicted {
                code: "delegation-child-conflict".into()
            }
        );
        assert_eq!(cell.settled_at, Some(AgentTimestampMillis::new(2)));
    }
}
