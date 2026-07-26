//! Communal claim records: identities, statement shape, provenance, trust.
//!
//! A claim is a provenance-bearing assertion appended to a knowledge space —
//! never an overwrite of an unqualified canonical fact
//! ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)). Conflicting
//! claims coexist for policy-aware resolution, which is why the claim's
//! identity derives from its append operation, not from its statement: two
//! agents asserting the same triple are two claims with distinct provenance.
//!
//! Open decision 3 is resolved structurally here
//! ([specification 21.3](../../../docs/plans/rakka-agent/spec.md)):
//! [`Claim::new`] takes no trust parameter and stamps `Proposed` with zero
//! transitions, so an agent-written claim cannot be born anything else. The
//! only path to a non-`Proposed` claim is [`Claim::restore`] from a persisted
//! record, and the store contract refuses to *append* such a record.
//!
//! Claim identity follows the same shape. [`Claim::new`] takes no claim id
//! either — it derives one from the scope and the append operation id — so a
//! constructed claim cannot carry an identity its own operation does not
//! produce. [`Claim::restore`] must accept any persisted id to load a record
//! at all, so the store's append door re-derives and refuses a mismatch
//! (`claim-append-id-not-derived`): otherwise a writer could squat the id
//! another writer's operation will derive and deny that append forever.

use std::collections::BTreeSet;

use rakka_agent::{
    validate_identity_segment, AgentCapabilityId, AgentContentDigest, AgentDelegationId,
    AgentGoalId, AgentId, AgentIdentityResult, AgentPolicyRef, AgentRevisionNumber, AgentRunId,
    AgentTaskContent, AgentTaskId, MemoryClassification, MemoryEmbeddingRef,
    AGENT_IDENTITY_MAX_LENGTH, AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH,
};
use rakka_agent_workflow::{AgentEffectId, AgentTimestampMillis, ArtifactRef, StateSchemaVersion};
use serde::{Deserialize, Serialize};

use crate::error::{check_schema_window, ClaimError, ClaimRecordKind, ClaimResult};
use crate::scope::KnowledgeSpaceScope;

/// Schema version this binary writes on every [`Claim`] record.
pub const CURRENT_CLAIM_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);

/// Largest inline size a [`ClaimObject::Value`] may carry, in bytes.
///
/// Deliberately tighter than the private-memory inline bound: a communal fact
/// is a statement, not a document. Larger content belongs behind an immutable
/// [`ArtifactRef`].
pub const CLAIM_OBJECT_INLINE_MAX_BYTES: usize = 4096;

/// Largest number of evidence artifact references one claim or transition may
/// carry.
pub const CLAIM_MAX_EVIDENCE_ARTIFACTS: usize = 16;

/// Largest number of read capabilities one claim's access set may require.
pub const CLAIM_MAX_ACL_CAPABILITIES: usize = 16;

/// Largest number of trust transitions one claim may accumulate.
///
/// The bound is enforced with an explicit refusal
/// (`claim-transition-history-full`): a claim oscillating this often is a
/// policy incident to surface, never a history to truncate silently.
pub const CLAIM_MAX_TRUST_TRANSITIONS: u32 = 32;

/// Declares a validated identifier newtype following the `rakka-agent`
/// identity rules (bounded, no control characters, no scope or persistence
/// separators), with fail-closed deserialization.
///
/// Hand-rolled equivalent of `rakka-agent`'s crate-private `validated_id!`
/// macro, built on its public [`validate_identity_segment`].
macro_rules! claim_id_type {
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
            pub fn new(value: impl Into<String>) -> AgentIdentityResult<Self> {
                let value = value.into();
                validate_identity_segment(Self::FIELD, &value)?;
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
            type Error = rakka_agent::AgentIdentityError;

            fn try_from(value: String) -> AgentIdentityResult<Self> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = rakka_agent::AgentIdentityError;

            fn try_from(value: &str) -> AgentIdentityResult<Self> {
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

claim_id_type! {
    /// Stable identity of one communal claim
    /// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Derived from the append operation id — see [`ClaimId::derive_appended`]
    /// — so a replayed append converges on the same claim (scenario 16) while
    /// two distinct appends of the same statement never collide.
    pub ClaimId, "claim_id"
}

claim_id_type! {
    /// Identity of one node in a knowledge space's graph.
    ///
    /// A validated identifier rather than free text, so every backend — a
    /// relational table, a property graph, a triplestore — can key on it
    /// without a vendor identifier entering the public types
    /// ([specification 13.6](../../../docs/plans/rakka-agent/spec.md)).
    pub ClaimNodeId, "claim_node_id"
}

claim_id_type! {
    /// Identity of one predicate (edge or attribute label) in a knowledge
    /// space's graph.
    pub ClaimPredicate, "claim_predicate"
}

claim_id_type! {
    /// The idempotent key of one claim append or trust transition.
    ///
    /// A replay under the same operation id returns the original logical
    /// result without a second write (scenario 16). It is derived, never
    /// generated: the writer reconstructs the same value on any node, after
    /// any crash.
    pub ClaimOperationId, "claim_operation_id"
}

/// Digests one salted derivation input into an identity value.
///
/// Cryptographic ([`AgentContentDigest::sha256_of_bytes`]) rather than the
/// default FNV fingerprint, because these derivations decide *identity*, and in
/// a communal space identity decides who a write belongs to. An operation id is
/// the idempotency key: a caller who can steer a colliding derivation makes a
/// distinct logical write replay to someone else's stored result, and the claim
/// id derived from it names whose claim that is. Salted domain separation stops
/// one derivation's input from being spelled as another's; only a
/// collision-resistant digest stops a *chosen* collision within one domain, and
/// FNV — which `rakka-agent` documents as explicitly not a security boundary —
/// does not.
///
/// The derivation is part of the durable contract: because the store's append
/// door re-derives the claim id, changing this algorithm changes claim identity
/// for every already-stored claim. It is a breaking change to a persisted
/// graph, never a transparent strengthening.
fn derivation_digest(input: &str) -> AgentContentDigest {
    AgentContentDigest::sha256_of_bytes(input.as_bytes())
}

impl ClaimOperationId {
    /// Derives the append key of one claim write.
    ///
    /// The scope and the discriminator both enter through a digest, so the
    /// result stays within the identity bound however long the discriminator
    /// is, and two tenants naming their spaces alike never collide because the
    /// scope's injective key is part of the digest input. The `claim-append`
    /// salt keeps this derivation domain disjoint from the transition domain.
    pub fn derive_append(
        scope: &KnowledgeSpaceScope,
        discriminator: impl AsRef<str>,
    ) -> AgentIdentityResult<Self> {
        let input = format!("claim-append|{}|{}", scope.key(), discriminator.as_ref());
        Self::new(format!("claim-op-{}", derivation_digest(&input).value))
    }

    /// Derives the key of one trust transition on one claim.
    ///
    /// The `claim-transition` salt keeps this domain disjoint from the append
    /// domain, so no discriminator can be spelled to land in the other
    /// domain's input, and the derivation digests with
    /// [`AgentContentDigest::sha256_of_bytes`], so no discriminator can be
    /// *searched* for a collision within this one either.
    pub fn derive_transition(
        scope: &KnowledgeSpaceScope,
        claim: &ClaimId,
        discriminator: impl AsRef<str>,
    ) -> AgentIdentityResult<Self> {
        let input = format!(
            "claim-transition|{}|{}|{}",
            scope.key(),
            claim,
            discriminator.as_ref()
        );
        Self::new(format!("claim-op-{}", derivation_digest(&input).value))
    }
}

impl ClaimId {
    /// Derives the claim identity of one append operation.
    ///
    /// The identity derives from the operation, not the statement, because
    /// conflicting claims must coexist
    /// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)): two
    /// agents asserting the same triple are two claims with distinct
    /// provenance. The append operation id is the one value that is
    /// reconstructable by the writer after any crash and unique per logical
    /// write, so a replay converges on the same claim and two distinct
    /// operations never collide — the latter resting on the collision
    /// resistance of [`AgentContentDigest::sha256_of_bytes`], since a collision
    /// here would deny one of the two writers its append.
    pub fn derive_appended(
        scope: &KnowledgeSpaceScope,
        operation_id: &ClaimOperationId,
    ) -> AgentIdentityResult<Self> {
        let input = format!("claim|{}|{}", scope.key(), operation_id);
        Self::new(format!("claim-{}", derivation_digest(&input).value))
    }
}

/// The object half of a claim statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ClaimObject {
    /// An edge to another node: the representation bounded traversal follows.
    Node(ClaimNodeId),
    /// A bounded literal or artifact-referenced value: an attribute assertion.
    /// Never extends traversal.
    Value(AgentTaskContent),
}

impl ClaimObject {
    /// The node this object points at, when it is an edge.
    #[must_use]
    pub const fn node(&self) -> Option<&ClaimNodeId> {
        match self {
            Self::Node(node) => Some(node),
            Self::Value(_) => None,
        }
    }
}

/// Provenance of one claim: who asserted it, and in service of what
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md),
/// [specification 8.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// Provenance only, never authority: recording a run or delegation does not
/// widen access to anything, exactly as the private-memory source rule states.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimProvenance {
    /// The agent that asserted the claim. Required: claims are agent-written.
    pub agent: AgentId,
    /// The goal the assertion served, when one did.
    #[serde(default)]
    pub goal: Option<AgentGoalId>,
    /// The task the assertion served, when one did.
    #[serde(default)]
    pub task: Option<AgentTaskId>,
    /// The run that produced the assertion, when one did.
    #[serde(default)]
    pub run: Option<AgentRunId>,
    /// The delegation under which the assertion was produced, when any.
    #[serde(default)]
    pub delegation: Option<AgentDelegationId>,
    /// The durable effect that carried the assertion, when one did.
    #[serde(default)]
    pub effect: Option<AgentEffectId>,
}

impl ClaimProvenance {
    /// Provenance naming only the asserting agent.
    #[must_use]
    pub const fn for_agent(agent: AgentId) -> Self {
        Self {
            agent,
            goal: None,
            task: None,
            run: None,
            delegation: None,
            effect: None,
        }
    }

    /// Sets the goal the assertion served.
    #[must_use]
    pub fn with_goal(mut self, goal: AgentGoalId) -> Self {
        self.goal = Some(goal);
        self
    }

    /// Sets the task the assertion served.
    #[must_use]
    pub fn with_task(mut self, task: AgentTaskId) -> Self {
        self.task = Some(task);
        self
    }

    /// Sets the run that produced the assertion.
    #[must_use]
    pub fn with_run(mut self, run: AgentRunId) -> Self {
        self.run = Some(run);
        self
    }

    /// Sets the delegation under which the assertion was produced.
    #[must_use]
    pub fn with_delegation(mut self, delegation: AgentDelegationId) -> Self {
        self.delegation = Some(delegation);
        self
    }

    /// Sets the durable effect that carried the assertion.
    #[must_use]
    pub fn with_effect(mut self, effect: AgentEffectId) -> Self {
        self.effect = Some(effect);
        self
    }

    fn validate(&self) -> ClaimResult<()> {
        if let Some(effect) = &self.effect {
            if effect.as_str().len() > AGENT_IDENTITY_MAX_LENGTH {
                return Err(ClaimError::ReferenceTooLong {
                    field: "provenance.effect",
                    length: effect.as_str().len(),
                    maximum: AGENT_IDENTITY_MAX_LENGTH,
                });
            }
        }
        Ok(())
    }
}

/// Per-claim access requirements beyond knowledge-space authorization
/// (the "ACL" of [specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Capabilities are the existing within-space authorization vocabulary, so the
/// access set names the capabilities a reader must hold; an empty set means
/// the space default — readable by every agent authorized for the space. The
/// set is carried, queryable data: its enforcement point is the communal
/// retrieval and policy layer, a recorded seam of this slice, while the store
/// itself enforces scope isolation (scenario 18).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAccess {
    /// Capabilities a reader must hold to see the claim.
    #[serde(default)]
    pub required_read_capabilities: BTreeSet<AgentCapabilityId>,
}

impl ClaimAccess {
    fn validate(&self) -> ClaimResult<()> {
        if self.required_read_capabilities.len() > CLAIM_MAX_ACL_CAPABILITIES {
            return Err(ClaimError::AccessOverflow {
                count: self.required_read_capabilities.len(),
                maximum: CLAIM_MAX_ACL_CAPABILITIES,
            });
        }
        Ok(())
    }
}

/// Trust status of one claim
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// `#[non_exhaustive]` because the specification names these the *initial*
/// trust states. The legal transitions are:
///
/// ```text
/// Proposed  -> Verified (gated) | Disputed | Retracted
/// Verified  -> Disputed | Retracted
/// Disputed  -> Verified (gated) | Retracted
/// Retracted -> (terminal)
/// ```
///
/// `Verified -> Disputed` exists because verified facts get challenged;
/// `Disputed -> Verified` exists because a dispute is an investigation, not a
/// verdict — and re-promotion re-passes the gate at a new history ordinal, so
/// an old grant can never be replayed into it. `Retracted` is terminal: it is
/// the auditable withdrawal, and un-retracting is a *new claim* referencing
/// the old one, preserving both provenances. Nothing transitions *to*
/// `Proposed`, which would launder history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ClaimTrustStatus {
    /// Asserted by an agent; not yet evaluated. Every appended claim is born
    /// here (open decision 3).
    Proposed,
    /// Promoted by policy, a verifier service, or a human decision.
    Verified,
    /// Challenged; under policy-aware resolution.
    Disputed,
    /// Withdrawn, auditably. Terminal.
    Retracted,
}

impl ClaimTrustStatus {
    /// Every trust status, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::Proposed,
        Self::Verified,
        Self::Disputed,
        Self::Retracted,
    ];

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Verified => "verified",
            Self::Disputed => "disputed",
            Self::Retracted => "retracted",
        }
    }

    /// Whether no further transition may leave this status.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Retracted)
    }

    /// Whether the legal transition table admits `self -> to`.
    #[must_use]
    pub const fn may_transition_to(&self, to: Self) -> bool {
        matches!(
            (self, to),
            (Self::Proposed, Self::Verified)
                | (Self::Proposed, Self::Disputed)
                | (Self::Proposed, Self::Retracted)
                | (Self::Verified, Self::Disputed)
                | (Self::Verified, Self::Retracted)
                | (Self::Disputed, Self::Verified)
                | (Self::Disputed, Self::Retracted)
        )
    }
}

/// One communal claim
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// The statement, provenance, and evidence are immutable once appended; only
/// the trust status and its transition count move, and only through the
/// append-only transition path. The `trust` and `transition_count` fields are
/// private and coherent by invariant — `Proposed` if and only if zero
/// transitions — so a freshly constructed claim is provably born `Proposed`
/// and a restored one provably carries its history.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Claim {
    schema_version: StateSchemaVersion,
    /// Stable claim identity.
    pub claim_id: ClaimId,
    /// The idempotent append key that created the claim.
    pub operation_id: ClaimOperationId,
    /// The statement's subject node.
    pub subject: ClaimNodeId,
    /// The statement's predicate.
    pub predicate: ClaimPredicate,
    /// The statement's object: an edge or a bounded value.
    pub object: ClaimObject,
    /// Fingerprint of the canonical subject/predicate/object statement,
    /// stamped at construction. Audit identity only, never authorization —
    /// the promotion gate recomputes a cryptographic digest of its own.
    pub content_digest: AgentContentDigest,
    /// Who asserted the claim, and in service of what.
    pub provenance: ClaimProvenance,
    /// Immutable evidence artifact references.
    #[serde(default)]
    pub evidence: Vec<ArtifactRef>,
    /// When the claim was appended.
    pub created_at: AgentTimestampMillis,
    /// Asserting confidence in basis points (0..=10_000).
    pub confidence_bps: u16,
    /// Classification of the claim's content.
    pub classification: MemoryClassification,
    /// Per-claim access requirements beyond space authorization.
    #[serde(default)]
    pub access: ClaimAccess,
    trust: ClaimTrustStatus,
    transition_count: u32,
    /// Embedding metadata, when the claim's content was embedded.
    #[serde(default)]
    pub embedding: Option<MemoryEmbeddingRef>,
    /// The policy in force when the claim was appended, when any.
    #[serde(default)]
    pub policy: Option<AgentPolicyRef>,
    /// The revision of that policy, when known.
    #[serde(default)]
    pub policy_revision: Option<AgentRevisionNumber>,
}

impl Claim {
    /// Creates a claim, born `Proposed` with zero transitions, under the
    /// identity its append operation derives.
    ///
    /// There is no trust parameter and no claim-id parameter: both invariants
    /// are the constructor's signature rather than a runtime check. The claim
    /// id is [derived](ClaimId::derive_appended) from `scope` and
    /// `operation_id` here, so a constructed claim cannot carry an identity
    /// its own append operation does not produce — the property that makes a
    /// replayed append converge on one claim. The scope is *used*, never
    /// stored: a claim record carries no tenant or space, so no layer above
    /// the store can re-check the scope it was answered for.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scope: &KnowledgeSpaceScope,
        operation_id: ClaimOperationId,
        subject: ClaimNodeId,
        predicate: ClaimPredicate,
        object: ClaimObject,
        provenance: ClaimProvenance,
        confidence_bps: u16,
        classification: MemoryClassification,
        created_at: AgentTimestampMillis,
    ) -> ClaimResult<Self> {
        let claim_id = ClaimId::derive_appended(scope, &operation_id)?;
        let content_digest = statement_digest(&subject, &predicate, &object)?;
        let claim = Self {
            schema_version: CURRENT_CLAIM_SCHEMA_VERSION,
            claim_id,
            operation_id,
            subject,
            predicate,
            object,
            content_digest,
            provenance,
            evidence: Vec::new(),
            created_at,
            confidence_bps,
            classification,
            access: ClaimAccess::default(),
            trust: ClaimTrustStatus::Proposed,
            transition_count: 0,
            embedding: None,
            policy: None,
            policy_revision: None,
        };
        claim.validate()?;
        Ok(claim)
    }

    /// Attaches evidence artifact references, re-validating the bound.
    pub fn with_evidence(mut self, evidence: Vec<ArtifactRef>) -> ClaimResult<Self> {
        self.evidence = evidence;
        self.validate()?;
        Ok(self)
    }

    /// Attaches per-claim access requirements, re-validating the bound.
    pub fn with_access(mut self, access: ClaimAccess) -> ClaimResult<Self> {
        self.access = access;
        self.validate()?;
        Ok(self)
    }

    /// Attaches embedding metadata, re-validating its bounds.
    pub fn with_embedding(mut self, embedding: MemoryEmbeddingRef) -> ClaimResult<Self> {
        self.embedding = Some(embedding);
        self.validate()?;
        Ok(self)
    }

    /// Attaches the policy the claim was appended under.
    #[must_use]
    pub fn with_policy(
        mut self,
        policy: AgentPolicyRef,
        revision: Option<AgentRevisionNumber>,
    ) -> Self {
        self.policy = Some(policy);
        self.policy_revision = revision;
        self
    }

    /// Current trust status.
    #[must_use]
    pub const fn trust(&self) -> ClaimTrustStatus {
        self.trust
    }

    /// Number of trust transitions the claim has accumulated.
    #[must_use]
    pub const fn transition_count(&self) -> u32 {
        self.transition_count
    }

    /// Schema version the record carries.
    #[must_use]
    pub const fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }

    /// Returns the claim with one legal trust transition applied.
    ///
    /// This is the single legality enforcement point every backend shares: it
    /// refuses an illegal `from -> to` pair and a full transition history, and
    /// it is the only public way trust moves — the fields themselves stay
    /// private, so no path can skip the table.
    pub fn apply_transition(&self, to: ClaimTrustStatus) -> ClaimResult<Self> {
        if !self.trust.may_transition_to(to) {
            return Err(ClaimError::IllegalTransition {
                claim_id: self.claim_id.clone(),
                from: self.trust,
                to,
            });
        }
        if self.transition_count >= CLAIM_MAX_TRUST_TRANSITIONS {
            return Err(ClaimError::TransitionHistoryFull {
                claim_id: self.claim_id.clone(),
                maximum: CLAIM_MAX_TRUST_TRANSITIONS,
            });
        }
        let mut applied = self.clone();
        applied.trust = to;
        applied.transition_count += 1;
        Ok(applied)
    }

    /// The wire/durable mirror of this claim, for adapters that persist
    /// records field by field.
    #[must_use]
    pub fn to_record(&self) -> ClaimRecord {
        ClaimRecord {
            schema_version: self.schema_version,
            claim_id: self.claim_id.clone(),
            operation_id: self.operation_id.clone(),
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            object: self.object.clone(),
            content_digest: self.content_digest.clone(),
            provenance: self.provenance.clone(),
            evidence: self.evidence.clone(),
            created_at: self.created_at,
            confidence_bps: self.confidence_bps,
            classification: self.classification,
            access: self.access.clone(),
            trust: self.trust,
            transition_count: self.transition_count,
            embedding: self.embedding.clone(),
            policy: self.policy.clone(),
            policy_revision: self.policy_revision,
        }
    }

    /// Rebuilds a claim from a persisted record, failing closed on an
    /// unsupported schema version, an out-of-bounds field, or an incoherent
    /// trust/transition pair.
    ///
    /// This is the only public path to a non-`Proposed` claim, and it is also
    /// how deserialization is implemented — a record that cannot be restored
    /// cannot cross the wire.
    pub fn restore(record: ClaimRecord) -> ClaimResult<Self> {
        check_schema_window(
            ClaimRecordKind::Claim,
            record.schema_version,
            CURRENT_CLAIM_SCHEMA_VERSION,
        )?;
        let claim = Self {
            schema_version: record.schema_version,
            claim_id: record.claim_id,
            operation_id: record.operation_id,
            subject: record.subject,
            predicate: record.predicate,
            object: record.object,
            content_digest: record.content_digest,
            provenance: record.provenance,
            evidence: record.evidence,
            created_at: record.created_at,
            confidence_bps: record.confidence_bps,
            classification: record.classification,
            access: record.access,
            trust: record.trust,
            transition_count: record.transition_count,
            embedding: record.embedding,
            policy: record.policy,
            policy_revision: record.policy_revision,
        };
        claim.validate()?;
        Ok(claim)
    }

    /// Validates every bound and the trust coherence invariant.
    pub fn validate(&self) -> ClaimResult<()> {
        if let ClaimObject::Value(content) = &self.object {
            let bytes = content.size_bytes();
            if bytes > CLAIM_OBJECT_INLINE_MAX_BYTES {
                return Err(ClaimError::ContentTooLarge {
                    bytes,
                    maximum: CLAIM_OBJECT_INLINE_MAX_BYTES,
                });
            }
        }
        if self.evidence.len() > CLAIM_MAX_EVIDENCE_ARTIFACTS {
            return Err(ClaimError::EvidenceOverflow {
                count: self.evidence.len(),
                maximum: CLAIM_MAX_EVIDENCE_ARTIFACTS,
            });
        }
        if self.confidence_bps > 10_000 {
            return Err(ClaimError::ConfidenceOutOfRange {
                confidence_bps: self.confidence_bps,
            });
        }
        self.provenance.validate()?;
        self.access.validate()?;
        if let Some(embedding) = &self.embedding {
            if embedding.model.is_empty()
                || embedding.model.len() > AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH
                || embedding.dimensions == 0
            {
                return Err(ClaimError::InvalidEmbeddingRef {
                    message: format!(
                        "the embedding model must be non-empty and at most \
                         {AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH} bytes, and the dimension \
                         count at least one; got model of {} bytes and {} dimensions",
                        embedding.model.len(),
                        embedding.dimensions
                    ),
                });
            }
        }
        // Coherence: nothing transitions *to* Proposed, so the pair is a
        // bijection — a Proposed claim has no history and a claim with
        // history is not Proposed.
        if (self.trust == ClaimTrustStatus::Proposed) != (self.transition_count == 0) {
            return Err(ClaimError::TrustIncoherent {
                claim_id: self.claim_id.clone(),
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for Claim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let record = ClaimRecord::deserialize(deserializer)?;
        Self::restore(record).map_err(serde::de::Error::custom)
    }
}

/// The wire/durable mirror of a [`Claim`], every field public.
///
/// Adapters rebuild claims through [`Claim::restore`], which re-validates
/// everything — including the trust coherence invariant — so an adapter can
/// carry the record without being able to forge an invalid claim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimRecord {
    /// Schema version the record carries.
    pub schema_version: StateSchemaVersion,
    /// Stable claim identity.
    pub claim_id: ClaimId,
    /// The idempotent append key that created the claim.
    pub operation_id: ClaimOperationId,
    /// The statement's subject node.
    pub subject: ClaimNodeId,
    /// The statement's predicate.
    pub predicate: ClaimPredicate,
    /// The statement's object.
    pub object: ClaimObject,
    /// Fingerprint of the canonical statement.
    pub content_digest: AgentContentDigest,
    /// Who asserted the claim, and in service of what.
    pub provenance: ClaimProvenance,
    /// Immutable evidence artifact references.
    #[serde(default)]
    pub evidence: Vec<ArtifactRef>,
    /// When the claim was appended.
    pub created_at: AgentTimestampMillis,
    /// Asserting confidence in basis points.
    pub confidence_bps: u16,
    /// Classification of the claim's content.
    pub classification: MemoryClassification,
    /// Per-claim access requirements.
    #[serde(default)]
    pub access: ClaimAccess,
    /// Current trust status.
    pub trust: ClaimTrustStatus,
    /// Number of trust transitions applied.
    pub transition_count: u32,
    /// Embedding metadata, when any.
    #[serde(default)]
    pub embedding: Option<MemoryEmbeddingRef>,
    /// The policy in force at append, when any.
    #[serde(default)]
    pub policy: Option<AgentPolicyRef>,
    /// The revision of that policy, when known.
    #[serde(default)]
    pub policy_revision: Option<AgentRevisionNumber>,
}

/// Fingerprints the canonical statement (sorted keys, so structurally equal
/// statements digest alike).
///
/// Deliberately the FNV fingerprint, not the cryptographic digest the identity
/// derivations use: nothing decides on this value. It tells an operator one
/// statement from another, while the promotion gate — the one place a statement
/// gates a decision — recomputes sha2-256 over the statement itself and never
/// reads this field.
fn statement_digest(
    subject: &ClaimNodeId,
    predicate: &ClaimPredicate,
    object: &ClaimObject,
) -> ClaimResult<AgentContentDigest> {
    let object = serde_json::to_value(object).map_err(|error| ClaimError::Encoding {
        message: format!("the claim object could not be encoded: {error}"),
    })?;
    Ok(AgentContentDigest::of_json(&serde_json::json!({
        "subject": subject.as_str(),
        "predicate": predicate.as_str(),
        "object": object,
    })))
}

#[cfg(test)]
mod tests {
    use rakka_agent::{KnowledgeSpaceId, TenantId};

    use super::*;

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            TenantId::new("acme"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    fn provenance() -> ClaimProvenance {
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid"))
    }

    fn artifact(id: &str) -> ArtifactRef {
        use rakka_agent_workflow::{AgentAttributes, ArtifactKind, RedactionStatus};
        ArtifactRef {
            artifact_id: id.to_string(),
            kind: ArtifactKind::File,
            uri: format!("s3://evidence/{id}"),
            checksum: Some(format!("sha256:{id}")),
            content_type: Some("application/json".to_string()),
            byte_len: Some(64),
            retention_class: None,
            encryption: None,
            redaction: RedactionStatus::Unredacted,
            created_at: AgentTimestampMillis::new(1),
            metadata: AgentAttributes::default(),
        }
    }

    fn claim() -> Claim {
        let operation_id =
            ClaimOperationId::derive_append(&scope(), "op-1").expect("the operation id derives");
        Claim::new(
            &scope(),
            operation_id,
            ClaimNodeId::new("customer-1").expect("the node id is valid"),
            ClaimPredicate::new("prefers").expect("the predicate is valid"),
            ClaimObject::Node(ClaimNodeId::new("channel-email").expect("the node id is valid")),
            provenance(),
            7_500,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid")
    }

    #[test]
    fn a_claim_is_born_proposed_with_zero_transitions() {
        let claim = claim();
        assert_eq!(claim.trust(), ClaimTrustStatus::Proposed);
        assert_eq!(claim.transition_count(), 0);
        assert_eq!(claim.schema_version(), CURRENT_CLAIM_SCHEMA_VERSION);
    }

    #[test]
    fn a_constructed_claim_carries_the_identity_its_operation_derives() {
        // The constructor takes no claim id, so the only assertion available is
        // that the derived one is what the claim carries — which is the point:
        // a mismatch is unrepresentable rather than refused.
        let claim = claim();
        assert_eq!(
            claim.claim_id,
            ClaimId::derive_appended(&scope(), &claim.operation_id).expect("the claim id derives")
        );
        // A distinct operation in the same scope is a distinct claim; the same
        // operation in a distinct scope is too.
        let other_operation =
            ClaimOperationId::derive_append(&scope(), "op-other").expect("the operation derives");
        assert_ne!(
            claim.claim_id,
            ClaimId::derive_appended(&scope(), &other_operation).expect("the claim id derives")
        );
        let other_scope = KnowledgeSpaceScope::new(
            TenantId::new("other"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid");
        assert_ne!(
            claim.claim_id,
            ClaimId::derive_appended(&other_scope, &claim.operation_id)
                .expect("the claim id derives")
        );
    }

    #[test]
    fn derivations_are_stable_and_their_salt_domains_are_disjoint() {
        let scope = scope();
        let append_a =
            ClaimOperationId::derive_append(&scope, "op-1").expect("the operation id derives");
        let append_b =
            ClaimOperationId::derive_append(&scope, "op-1").expect("the operation id derives");
        assert_eq!(append_a, append_b);

        // The transition domain never collides with the append domain, even
        // over an adversarial discriminator engineered to mimic its input.
        let claim_id = ClaimId::derive_appended(&scope, &append_a).expect("the claim id derives");
        let transition = ClaimOperationId::derive_transition(&scope, &claim_id, "op-1")
            .expect("the operation id derives");
        assert_ne!(append_a, transition);
        let mimic = ClaimOperationId::derive_append(&scope, format!("{claim_id}|op-1"))
            .expect("the operation id derives");
        assert_ne!(mimic, transition);

        // Distinct scopes never collide for the same discriminator.
        let other = KnowledgeSpaceScope::new(
            TenantId::new("other"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid");
        assert_ne!(
            ClaimOperationId::derive_append(&other, "op-1").expect("the operation id derives"),
            append_a
        );
    }

    #[test]
    fn identity_derivations_are_backed_by_a_cryptographic_digest() {
        // Salted domains stop a derivation input from being *spelled* as
        // another's; only a collision-resistant digest stops one from being
        // searched for. Pin the algorithm, not just the shape: identity decides
        // whose write a replay answers, so an FNV fingerprint here is a
        // steerable collision.
        let digest = derivation_digest("any derivation input");
        assert_eq!(digest.algorithm, rakka_agent::AgentDigestAlgorithm::Sha256);
        assert!(digest.algorithm.is_cryptographic());

        // The identities carry that digest's full width, and stay inside the
        // identity bound the newtypes validate against.
        let scope = scope();
        let operation =
            ClaimOperationId::derive_append(&scope, "op-1").expect("the operation id derives");
        let claim_id = ClaimId::derive_appended(&scope, &operation).expect("the claim id derives");
        let transition = ClaimOperationId::derive_transition(&scope, &claim_id, "t-1")
            .expect("the operation id derives");
        for (label, value) in [
            ("claim-op-", operation.as_str()),
            ("claim-", claim_id.as_str()),
            ("claim-op-", transition.as_str()),
        ] {
            let hex = value
                .strip_prefix(label)
                .expect("the identity carries its derivation prefix");
            assert_eq!(hex.len(), 64, "a sha2-256 identity carries 64 hex digits");
            assert!(hex
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
            assert!(value.len() <= AGENT_IDENTITY_MAX_LENGTH);
        }
    }

    #[test]
    fn the_transition_table_is_exactly_the_documented_lattice() {
        use ClaimTrustStatus::{Disputed, Proposed, Retracted, Verified};
        let legal = [
            (Proposed, Verified),
            (Proposed, Disputed),
            (Proposed, Retracted),
            (Verified, Disputed),
            (Verified, Retracted),
            (Disputed, Verified),
            (Disputed, Retracted),
        ];
        for from in ClaimTrustStatus::ALL {
            for to in ClaimTrustStatus::ALL {
                assert_eq!(
                    from.may_transition_to(to),
                    legal.contains(&(from, to)),
                    "transition {} -> {} disagrees with the documented table",
                    from.as_label(),
                    to.as_label()
                );
            }
        }
        assert!(Retracted.is_terminal());
        assert!(!Proposed.is_terminal());
    }

    #[test]
    fn apply_transition_enforces_the_table_and_the_history_bound() {
        let claim = claim();
        let verified = claim
            .apply_transition(ClaimTrustStatus::Verified)
            .expect("the promotion applies");
        assert_eq!(verified.trust(), ClaimTrustStatus::Verified);
        assert_eq!(verified.transition_count(), 1);

        assert_eq!(
            verified
                .apply_transition(ClaimTrustStatus::Proposed)
                .expect_err("nothing transitions to proposed")
                .code(),
            "claim-transition-illegal"
        );

        // Walk the lattice to the cap: alternate Disputed/Verified.
        let mut walked = verified;
        while walked.transition_count() < CLAIM_MAX_TRUST_TRANSITIONS {
            let to = if walked.trust() == ClaimTrustStatus::Verified {
                ClaimTrustStatus::Disputed
            } else {
                ClaimTrustStatus::Verified
            };
            walked = walked.apply_transition(to).expect("the transition applies");
        }
        assert_eq!(
            walked
                .apply_transition(ClaimTrustStatus::Retracted)
                .expect_err("a full history refuses")
                .code(),
            "claim-transition-history-full"
        );
    }

    #[test]
    fn validate_rejects_every_out_of_bounds_field() {
        let base = claim();

        let mut oversized = base.to_record();
        oversized.object = ClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!({
                "text": "x".repeat(CLAIM_OBJECT_INLINE_MAX_BYTES + 1)
            }))
            .expect("the content is within the task bound"),
        );
        assert_eq!(
            Claim::restore(oversized)
                .expect_err("oversized inline content is refused")
                .code(),
            "claim-content-too-large"
        );

        assert_eq!(
            base.clone()
                .with_evidence(vec![artifact("a"); CLAIM_MAX_EVIDENCE_ARTIFACTS + 1])
                .expect_err("an evidence overflow is refused")
                .code(),
            "claim-evidence-overflow"
        );

        let mut access = ClaimAccess::default();
        for index in 0..=CLAIM_MAX_ACL_CAPABILITIES {
            access.required_read_capabilities.insert(
                AgentCapabilityId::new(format!("cap-{index}")).expect("the capability is valid"),
            );
        }
        assert_eq!(
            base.clone()
                .with_access(access)
                .expect_err("an access overflow is refused")
                .code(),
            "claim-access-overflow"
        );

        let mut overconfident = base.to_record();
        overconfident.confidence_bps = 10_001;
        assert_eq!(
            Claim::restore(overconfident)
                .expect_err("an out-of-range confidence is refused")
                .code(),
            "claim-confidence-out-of-range"
        );

        assert_eq!(
            base.clone()
                .with_embedding(MemoryEmbeddingRef {
                    model: String::new(),
                    dimensions: 3,
                    version: AgentRevisionNumber::INITIAL,
                })
                .expect_err("an empty embedding model is refused")
                .code(),
            "claim-embedding-invalid"
        );
    }

    #[test]
    fn restore_fails_closed_on_incoherence_and_foreign_schema_versions() {
        let base = claim();

        // Trust incoherence in both directions.
        let mut forged = base.to_record();
        forged.trust = ClaimTrustStatus::Verified;
        assert_eq!(
            Claim::restore(forged)
                .expect_err("verified with zero transitions is incoherent")
                .code(),
            "claim-trust-incoherent"
        );
        let mut forged = base.to_record();
        forged.transition_count = 1;
        assert_eq!(
            Claim::restore(forged)
                .expect_err("proposed with history is incoherent")
                .code(),
            "claim-trust-incoherent"
        );

        // A version ahead of this binary fails closed, through restore and
        // through serde alike.
        let mut ahead = base.to_record();
        ahead.schema_version = StateSchemaVersion::new(CURRENT_CLAIM_SCHEMA_VERSION.get() + 1);
        assert_eq!(
            Claim::restore(ahead.clone())
                .expect_err("a newer schema version is refused")
                .code(),
            "schema-version-ahead"
        );
        let json = serde_json::to_string(&ahead).expect("the record serializes");
        assert!(serde_json::from_str::<Claim>(&json).is_err());

        // The round trip through the mirror is lossless for a valid claim.
        let json = serde_json::to_string(&base).expect("the claim serializes");
        let restored: Claim = serde_json::from_str(&json).expect("the claim deserializes");
        assert_eq!(restored, base);
    }

    #[test]
    fn the_statement_digest_is_structural() {
        let a = claim();
        let b = claim();
        assert_eq!(a.content_digest, b.content_digest);

        let operation_id =
            ClaimOperationId::derive_append(&scope(), "op-2").expect("the operation id derives");
        let different = Claim::new(
            &scope(),
            operation_id,
            ClaimNodeId::new("customer-1").expect("the node id is valid"),
            ClaimPredicate::new("prefers").expect("the predicate is valid"),
            ClaimObject::Node(ClaimNodeId::new("channel-phone").expect("the node id is valid")),
            provenance(),
            7_500,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid");
        assert_ne!(a.content_digest, different.content_digest);
    }
}
