//! The communal read path: how the communal claims a run may read enter the
//! context a model turn is computed from
//! ([specification 13.4, 13.5 and 16](../../../docs/plans/rakka-agent/spec.md)).
//!
//! # The private path is the model
//!
//! Everything here follows [`crate::retrieval`], tier for tier. A retriever
//! answers a **ranking**: identities and scores, never content. Assembly then
//! resolves every ranked identity through an **authority**, re-checks the
//! record it answers, fail closed, evaluates the statement at the
//! memory-ingress guardrail boundary, and embeds what survives into the
//! immutable context snapshot. The selection is a prefix of the ranking, a
//! failure degrades the turn to a smaller selection and never fails it, and a
//! retried turn reads the snapshot back instead of searching again.
//!
//! # What this crate may name
//!
//! `rakka-agent` cannot depend on the knowledge-graph crate: the graph depends
//! on it. So this module declares **ports** the graph crate implements and
//! **mirror types** that say what a claim is without naming the graph's own:
//! [`CommunalClaimRecord`] for a claim, [`AgentCommunalClaimTrust`] for its
//! trust state. A deployment wires the graph crate's adapters into an
//! [`AgentCommunalRetrieval`] bundle and hands it to
//! [`AgentRunMemory::with_communal_retrieval`](crate::memory::AgentRunMemory::with_communal_retrieval).
//!
//! # What a run may read
//!
//! Three things bound a communal read, and all three are checked on the
//! authoritative record, not on the ranking:
//!
//! - the **space**: a run searches only the spaces of its effective read
//!   grant ([`AgentCommunalReadGrant`]);
//! - the **claim's own requirements**: a claim may require capabilities of
//!   its reader, and a run reads it only when its grant holds every one;
//! - the **policy**: which trust states and classifications a deployment
//!   admits into a model context at all ([`CommunalRetrievalPolicy`]). By
//!   default only `Verified` claims are admitted, and no policy can admit a
//!   `Retracted` one.

use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use rakka_agent_workflow::{AgentTimestampMillis, ArtifactRef};
use serde::{Deserialize, Serialize};

use crate::definition::{AgentCapabilityId, AgentRevisionNumber};
use crate::guardrails::AgentGuardrailChain;
use crate::identity::{
    validate_identity_segment, AgentCommunalClaimId, AgentDelegationId, AgentGoalId, AgentId,
    AgentRunId, AgentRunScope, AgentTaskId, KnowledgeSpaceId, TenantId,
};
use crate::memory::{MemoryClassification, MemoryError, MemoryFuture};
use crate::retrieval::{
    AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS, AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES,
    AGENT_MEMORY_RETRIEVAL_RESOLUTION_FACTOR,
};
use crate::task::{AgentContentDigest, AgentTaskContent};

/// Most communal claims one context snapshot may select.
pub const AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_ENTRIES: usize = 16;

/// Largest total serialized statement content the communal claims of one
/// snapshot may embed, in bytes.
pub const AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES: usize = 64 * 1024;

/// Most knowledge spaces one assembly searches.
///
/// Each searched space costs one retriever call inside the run's settle pass,
/// so the number is bounded by policy however many spaces a grant names.
pub const AGENT_COMMUNAL_RETRIEVAL_MAX_SPACES: usize = 16;

/// Largest serialized object value a [`CommunalClaimRecord`] may carry, in
/// bytes. Equal to the knowledge graph's own bound, which a test in the graph
/// crate holds.
pub const AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES: usize = 4096;

/// Most evidence references a [`CommunalClaimRecord`] may carry. Equal to the
/// knowledge graph's own bound.
pub const AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE: usize = 16;

/// Most read capabilities a [`CommunalClaimRecord`] may require. Equal to the
/// knowledge graph's own bound.
pub const AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES: usize = 16;

/// The trust state of a communal claim, as this crate names it
/// ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// A mirror of the knowledge graph's own trust status, with the same labels;
/// the graph crate converts between the two exhaustively, so a state added
/// there fails to compile until it is mapped here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCommunalClaimTrust {
    /// Asserted by an agent and not yet evaluated.
    Proposed,
    /// Promoted by policy, a verifier or a human decision.
    Verified,
    /// Challenged, and under resolution.
    Disputed,
    /// Withdrawn. Never admitted into a model context.
    Retracted,
}

impl AgentCommunalClaimTrust {
    /// Every trust state, in declaration order.
    pub const ALL: [Self; 4] = [
        Self::Proposed,
        Self::Verified,
        Self::Disputed,
        Self::Retracted,
    ];

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Verified => "verified",
            Self::Disputed => "disputed",
            Self::Retracted => "retracted",
        }
    }
}

/// The object half of a communal claim's statement.
///
/// Encoded as the knowledge graph encodes its own claim object, so a
/// statement here and a statement there have one JSON shape and one digest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum CommunalClaimObject {
    /// An edge to another node, by its id.
    Node(String),
    /// A bounded literal, or a value held by artifact reference.
    Value(AgentTaskContent),
}

/// What a communal claim says: a subject, a predicate and an object.
///
/// This is the value a memory-ingress guardrail stage evaluates, as JSON:
/// `{"subject", "predicate", "object"}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommunalClaimStatement {
    /// The node the statement is about.
    pub subject: String,
    /// What is asserted of the subject.
    pub predicate: String,
    /// The asserted object.
    pub object: CommunalClaimObject,
}

impl CommunalClaimStatement {
    /// Rejects a statement that could not be a claim's: an empty, over-long or
    /// separator-carrying subject, predicate or node object, or an object
    /// value over [`AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES`].
    ///
    /// # Errors
    ///
    /// Returns `memory-entry-too-large` for an oversized object value and
    /// `memory-encoding-failed` for a subject, predicate or node id that is
    /// not a legal identifier. There is no dedicated code: assembly counts a
    /// statement that fails here and never surfaces the error.
    pub fn validate(&self) -> Result<(), MemoryError> {
        for (field, value) in [("subject", &self.subject), ("predicate", &self.predicate)] {
            validate_identity_segment(field, value).map_err(|error| invalid(&error))?;
        }
        match &self.object {
            CommunalClaimObject::Node(node) => {
                validate_identity_segment("object", node).map_err(|error| invalid(&error))?;
            }
            CommunalClaimObject::Value(content) => {
                content.validate().map_err(|error| invalid(&error))?;
                let bytes = content.size_bytes();
                if bytes > AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES {
                    return Err(MemoryError::EntryTooLarge {
                        bytes,
                        maximum: AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES,
                    });
                }
            }
        }
        Ok(())
    }

    /// The fingerprint of the statement: the digest of its JSON encoding.
    ///
    /// The knowledge graph fingerprints its own statements over the same
    /// value, so a claim's digest there and its record's digest here agree.
    #[must_use]
    pub fn digest(&self) -> AgentContentDigest {
        AgentContentDigest::of_json(&self.to_json())
    }

    /// The statement as the JSON value a guardrail stage evaluates.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// The serialized size of the statement, in bytes: what the snapshot's
    /// byte budget counts.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |bytes| bytes.len())
    }
}

/// A record or statement that is structurally not a claim's.
fn invalid(detail: &dyn fmt::Display) -> MemoryError {
    MemoryError::Encoding {
        message: format!("the communal claim record is invalid: {detail}"),
    }
}

/// Who asserted a communal claim, and in service of what.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommunalClaimProvenance {
    /// The agent that asserted the claim.
    pub agent: AgentId,
    /// The goal it was asserted under, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<AgentGoalId>,
    /// The task it was asserted under, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<AgentTaskId>,
    /// The run that asserted it, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<AgentRunId>,
    /// The delegation it was asserted under, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<AgentDelegationId>,
}

/// One communal claim as its authoritative store holds it: what
/// [`AgentCommunalClaimAuthority::resolve`] answers.
///
/// Assembly embeds this record, never anything a retriever said, so every
/// field a model context or an audit reads comes from the authority.
#[derive(Debug, Clone, PartialEq)]
pub struct CommunalClaimRecord {
    /// The claim's identity.
    pub claim_id: AgentCommunalClaimId,
    /// The knowledge space that holds it.
    pub space: KnowledgeSpaceId,
    /// What the claim says.
    pub statement: CommunalClaimStatement,
    /// The fingerprint of [`Self::statement`].
    pub statement_digest: AgentContentDigest,
    /// The claim's trust state.
    pub trust: AgentCommunalClaimTrust,
    /// Who asserted it.
    pub provenance: CommunalClaimProvenance,
    /// Immutable evidence artifact references.
    pub evidence: Vec<ArtifactRef>,
    /// When the claim was appended.
    pub created_at: AgentTimestampMillis,
    /// The asserting confidence, in basis points.
    pub confidence_bps: u16,
    /// The classification of the claim's content.
    pub classification: MemoryClassification,
    /// Every capability a reader must hold to read the claim.
    pub required_read_capabilities: BTreeSet<AgentCapabilityId>,
}

impl CommunalClaimRecord {
    /// Rejects a record outside the bounds a claim has: a statement that does
    /// not validate, a digest that does not describe the statement beside it,
    /// more than [`AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE`] evidence references,
    /// more than [`AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES`] required
    /// capabilities, or a confidence over 10000 basis points.
    ///
    /// An authority is an adapter over a store this crate cannot see. The
    /// bounds are checked here so that nothing an adapter answers can put an
    /// unbounded value into a snapshot.
    ///
    /// # Errors
    ///
    /// As [`CommunalClaimStatement::validate`], plus
    /// `memory-confidence-out-of-range`.
    pub fn validate(&self) -> Result<(), MemoryError> {
        self.statement.validate()?;
        if self.statement_digest != self.statement.digest() {
            return Err(invalid(
                &"its statement digest does not describe its statement",
            ));
        }
        if self.evidence.len() > AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE {
            return Err(invalid(&format!(
                "it carries {} evidence references; at most {AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE} \
                 are allowed",
                self.evidence.len()
            )));
        }
        if self.required_read_capabilities.len() > AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES {
            return Err(invalid(&format!(
                "it requires {} read capabilities; at most \
                 {AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES} are allowed",
                self.required_read_capabilities.len()
            )));
        }
        if self.confidence_bps > 10_000 {
            return Err(MemoryError::ConfidenceOutOfRange {
                confidence_bps: self.confidence_bps,
            });
        }
        Ok(())
    }
}

/// One search of one knowledge space: the text and everything that restricts
/// what it may find.
///
/// A retriever applies every restriction before it ranks
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)). Assembly
/// applies [`Self::admits`] again to the authoritative record, because a
/// ranking is not authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommunalRetrievalQuery {
    /// The query text.
    pub text: String,
    /// The most identities the ranking should carry.
    pub limit: usize,
    /// The confidence floor, in basis points.
    pub min_confidence_bps: u16,
    /// The admitted classifications.
    pub classifications: BTreeSet<MemoryClassification>,
    /// The admitted trust states.
    pub trust: BTreeSet<AgentCommunalClaimTrust>,
    /// The capabilities the reader holds.
    pub reader: BTreeSet<AgentCapabilityId>,
}

impl CommunalRetrievalQuery {
    /// Whether this query may be answered with `record`: the fail-closed
    /// re-check assembly runs on every record the authority answers.
    ///
    /// True when the record's trust state is admitted and is not `Retracted`,
    /// its classification is admitted, its confidence meets the floor, and
    /// every capability it requires is one the reader holds. `Retracted` is
    /// refused whatever the trust set says: a withdrawn claim never enters a
    /// model context.
    #[must_use]
    pub fn admits(&self, record: &CommunalClaimRecord) -> bool {
        record.trust != AgentCommunalClaimTrust::Retracted
            && self.trust.contains(&record.trust)
            && self.classifications.contains(&record.classification)
            && record.confidence_bps >= self.min_confidence_bps
            && record.required_read_capabilities.is_subset(&self.reader)
    }
}

/// One entry of a ranking: an identity and how well it matched. No content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedCommunalClaim {
    /// The claim.
    pub claim: AgentCommunalClaimId,
    /// The retriever's relevance score, in basis points.
    pub relevance_bps: u16,
}

/// What one retrieval of one space answered.
///
/// A ranking carries identities and scores only. It carries no content, so
/// there is nothing in it to trust.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommunalRetrievalOutcome {
    /// The ranking, best first.
    pub ranked: Vec<RankedCommunalClaim>,
    /// The backend's index watermark, when it reports one.
    pub index_watermark: Option<String>,
    /// Whether the backend said the space holds claims its index does not
    /// cover, so that this ranking could not include them.
    pub index_incomplete: bool,
}

/// Ranks the communal claims of one knowledge space against a query.
///
/// A ranking is advisory. Whatever an implementation answers, assembly
/// resolves each identity through an [`AgentCommunalClaimAuthority`] and
/// re-checks the record, so a wrong ranking can cost a selection slot and can
/// never put a claim in front of a run that may not read it.
pub trait AgentCommunalClaimRetriever: Send + Sync + 'static {
    /// Stable name of the backing implementation, for diagnostics and the
    /// `backend` metric label.
    fn backend_name(&self) -> &'static str;

    /// The retriever's version, recorded on the snapshot.
    fn retriever_version(&self) -> AgentRevisionNumber;

    /// Ranks the claims of `space` that `query` admits. A space the backend
    /// does not hold answers an empty ranking.
    fn retrieve<'a>(
        &'a self,
        tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        query: &'a CommunalRetrievalQuery,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, CommunalRetrievalOutcome>;
}

/// The authoritative source of communal claims: what a ranked identity is
/// resolved through before anything enters a snapshot.
pub trait AgentCommunalClaimAuthority: Send + Sync + 'static {
    /// Stable name of the backing implementation, for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// Reads one claim of one space. `None` for a claim the store does not
    /// hold in that scope, exactly as for an absent claim, and `None` for a
    /// tombstoned stub: a stub has no statement to give.
    fn resolve<'a>(
        &'a self,
        tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        claim: &'a AgentCommunalClaimId,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<CommunalClaimRecord>>;
}

/// What an agent may read communally, as its definition and setup grant it.
///
/// This is the grant before a run's delegation narrows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommunalReadGrant {
    /// The knowledge spaces the agent may search.
    pub spaces: BTreeSet<KnowledgeSpaceId>,
    /// The capabilities the agent holds for reading claims.
    pub read_capabilities: BTreeSet<AgentCapabilityId>,
    /// The definition revision the grant was read under.
    pub definition_revision: AgentRevisionNumber,
    /// The setup revision the grant was read under, when the run has a setup.
    pub setup_revision: Option<AgentRevisionNumber>,
}

/// Answers what one run's agent may read communally.
pub trait AgentCommunalReadGrantSource: Send + Sync + 'static {
    /// Stable name of the backing implementation, for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// The grant of the agent `scope` runs as, at `now`. `None` when there is
    /// nothing to grant: no such agent, or an agent that may not act.
    fn read_grant<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentCommunalReadGrant>>;
}

/// What a deployment admits into a model context from the communal tier, and
/// how much of it.
///
/// Every knob clamps to its bound rather than erroring, like every policy in
/// the crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommunalRetrievalPolicy {
    max_results: usize,
    max_bytes: usize,
    max_spaces: usize,
    query_entries: usize,
    min_confidence_bps: u16,
    classifications: BTreeSet<MemoryClassification>,
    trust: BTreeSet<AgentCommunalClaimTrust>,
}

impl CommunalRetrievalPolicy {
    /// The default policy: up to 8 claims within
    /// [`AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES`], from up to 4 spaces,
    /// queried from the last
    /// [`AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES`] window entries, any
    /// confidence, unclassified content, and **only `Verified` claims**.
    ///
    /// A `Proposed` claim is one agent's unreviewed assertion. Admitting it
    /// is a deployment's decision to make with [`Self::with_trust`], not a
    /// default.
    #[must_use]
    pub fn verified_only() -> Self {
        Self {
            max_results: 8,
            max_bytes: AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES,
            max_spaces: 4,
            query_entries: AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES,
            min_confidence_bps: 0,
            classifications: BTreeSet::from([MemoryClassification::Unclassified]),
            trust: BTreeSet::from([AgentCommunalClaimTrust::Verified]),
        }
    }

    /// Sets how many claims an assembly may select, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_ENTRIES`].
    #[must_use]
    pub fn with_max_results(mut self, max_results: usize) -> Self {
        self.max_results = max_results.clamp(1, AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_ENTRIES);
        self
    }

    /// Sets the selection's statement byte budget, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES`]. Selection stops at
    /// the first claim that would exceed it, so the selection stays a prefix
    /// of the ranking.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes.clamp(1, AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES);
        self
    }

    /// Sets how many knowledge spaces an assembly searches, clamped to
    /// `1..=`[`AGENT_COMMUNAL_RETRIEVAL_MAX_SPACES`].
    #[must_use]
    pub fn with_max_spaces(mut self, max_spaces: usize) -> Self {
        self.max_spaces = max_spaces.clamp(1, AGENT_COMMUNAL_RETRIEVAL_MAX_SPACES);
        self
    }

    /// Sets how many recent window entries the query derivation reads,
    /// clamped to `1..=`[`AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES`].
    #[must_use]
    pub fn with_query_entries(mut self, query_entries: usize) -> Self {
        self.query_entries = query_entries.clamp(1, AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES);
        self
    }

    /// Sets the confidence floor, clamped to 10000 basis points.
    #[must_use]
    pub fn with_min_confidence_bps(mut self, min_confidence_bps: u16) -> Self {
        self.min_confidence_bps = min_confidence_bps.min(10_000);
        self
    }

    /// Sets the admitted classifications. Any set is accepted; an empty one
    /// admits nothing.
    #[must_use]
    pub fn with_classifications(mut self, classifications: BTreeSet<MemoryClassification>) -> Self {
        self.classifications = classifications;
        self
    }

    /// Sets the admitted trust states. `Retracted` is removed: no policy can
    /// admit a withdrawn claim. A set left empty admits nothing.
    #[must_use]
    pub fn with_trust(mut self, trust: BTreeSet<AgentCommunalClaimTrust>) -> Self {
        let mut trust = trust;
        trust.remove(&AgentCommunalClaimTrust::Retracted);
        self.trust = trust;
        self
    }

    /// How many claims an assembly may select.
    #[must_use]
    pub const fn max_results(&self) -> usize {
        self.max_results
    }

    /// The selection's statement byte budget.
    #[must_use]
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// How many knowledge spaces an assembly searches.
    #[must_use]
    pub const fn max_spaces(&self) -> usize {
        self.max_spaces
    }

    /// How many recent window entries the query derivation reads.
    #[must_use]
    pub const fn query_entries(&self) -> usize {
        self.query_entries
    }

    /// The confidence floor, in basis points.
    #[must_use]
    pub const fn min_confidence_bps(&self) -> u16 {
        self.min_confidence_bps
    }

    /// The admitted classifications.
    #[must_use]
    pub const fn classifications(&self) -> &BTreeSet<MemoryClassification> {
        &self.classifications
    }

    /// The admitted trust states. Never holds `Retracted`.
    #[must_use]
    pub const fn trust(&self) -> &BTreeSet<AgentCommunalClaimTrust> {
        &self.trust
    }

    /// The most ranked identities one assembly resolves through the
    /// authority: [`Self::max_results`] times
    /// [`AGENT_MEMORY_RETRIEVAL_RESOLUTION_FACTOR`], capped at
    /// [`AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS`].
    ///
    /// Derived, so it cannot disagree with [`Self::max_results`], and bounded
    /// by this deployment's configuration rather than by the length of an
    /// adapter's answer, as the private tier's is.
    #[must_use]
    pub const fn max_resolutions(&self) -> usize {
        let allowance = self
            .max_results
            .saturating_mul(AGENT_MEMORY_RETRIEVAL_RESOLUTION_FACTOR);
        if allowance > AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS {
            AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS
        } else {
            allowance
        }
    }
}

impl Default for CommunalRetrievalPolicy {
    fn default() -> Self {
        Self::verified_only()
    }
}

/// The communal collaborators one deployment wires into
/// [`AgentRunMemory::with_communal_retrieval`](crate::memory::AgentRunMemory::with_communal_retrieval).
///
/// All five are required, each for the reason its private counterpart is.
/// The **authority**, because a retriever whose answer nothing checks is a
/// fail-open. The **grant source**, because a search with no stated limit on
/// what the run may read is one too. The **guardrail chain**, because a wired
/// retriever always evaluates whatever chain it holds at the memory-ingress
/// boundary: a deployment with no ingress stages says so with an empty chain,
/// and an optional chain would let a claim reach a model no stage ever saw.
/// The chain must be the one the deployment's dispatch authority carries;
/// [`AgentToolAuthority::with_memory_ingress`](crate::tools::AgentToolAuthority::with_memory_ingress)
/// attests it.
#[derive(Clone)]
pub struct AgentCommunalRetrieval {
    retriever: Arc<dyn AgentCommunalClaimRetriever>,
    authority: Arc<dyn AgentCommunalClaimAuthority>,
    grants: Arc<dyn AgentCommunalReadGrantSource>,
    guardrails: AgentGuardrailChain,
    policy: CommunalRetrievalPolicy,
}

impl AgentCommunalRetrieval {
    /// Bundles a retriever with the authority its rankings resolve through,
    /// the source of the run's read grant, the memory-ingress guardrail chain
    /// claims are evaluated under, and the policy.
    #[must_use]
    pub fn new(
        retriever: Arc<dyn AgentCommunalClaimRetriever>,
        authority: Arc<dyn AgentCommunalClaimAuthority>,
        grants: Arc<dyn AgentCommunalReadGrantSource>,
        guardrails: AgentGuardrailChain,
        policy: CommunalRetrievalPolicy,
    ) -> Self {
        Self {
            retriever,
            authority,
            grants,
            guardrails,
            policy,
        }
    }

    /// The retriever.
    #[must_use]
    pub fn retriever(&self) -> &dyn AgentCommunalClaimRetriever {
        self.retriever.as_ref()
    }

    /// The authority every ranked identity is resolved through.
    #[must_use]
    pub fn authority(&self) -> &dyn AgentCommunalClaimAuthority {
        self.authority.as_ref()
    }

    /// The source of the run's read grant.
    #[must_use]
    pub fn grants(&self) -> &dyn AgentCommunalReadGrantSource {
        self.grants.as_ref()
    }

    /// The memory-ingress guardrail chain.
    #[must_use]
    pub const fn guardrails(&self) -> &AgentGuardrailChain {
        &self.guardrails
    }

    /// The policy.
    #[must_use]
    pub const fn policy(&self) -> &CommunalRetrievalPolicy {
        &self.policy
    }
}

impl Debug for AgentCommunalRetrieval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentCommunalRetrieval")
            .field("retriever", &self.retriever.backend_name())
            .field("authority", &self.authority.backend_name())
            .field("grants", &self.grants.backend_name())
            .field("guardrails", &self.guardrails.revision())
            .field("policy", &self.policy)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent_workflow::{AgentAttributes, ArtifactKind, RedactionStatus};

    use super::*;

    fn capability(id: &str) -> AgentCapabilityId {
        AgentCapabilityId::new(id).expect("the capability id is valid")
    }

    fn statement(text: &str) -> CommunalClaimStatement {
        CommunalClaimStatement {
            subject: "order-42".to_string(),
            predicate: "status".to_string(),
            object: CommunalClaimObject::Value(
                AgentTaskContent::inline(serde_json::Value::String(text.to_string()))
                    .expect("the value is inline-bounded"),
            ),
        }
    }

    fn record() -> CommunalClaimRecord {
        let statement = statement("delayed at customs");
        CommunalClaimRecord {
            claim_id: AgentCommunalClaimId::new("claim-1").expect("the claim id is valid"),
            space: KnowledgeSpaceId::new("space-a").expect("the space id is valid"),
            statement_digest: statement.digest(),
            statement,
            trust: AgentCommunalClaimTrust::Verified,
            provenance: CommunalClaimProvenance {
                agent: AgentId::new("scout").expect("the agent id is valid"),
                goal: None,
                task: None,
                run: None,
                delegation: None,
            },
            evidence: Vec::new(),
            created_at: AgentTimestampMillis::new(1),
            confidence_bps: 5_000,
            classification: MemoryClassification::Unclassified,
            required_read_capabilities: BTreeSet::from([capability("cap-a")]),
        }
    }

    fn query() -> CommunalRetrievalQuery {
        CommunalRetrievalQuery {
            text: "customs".to_string(),
            limit: 8,
            min_confidence_bps: 0,
            classifications: BTreeSet::from([MemoryClassification::Unclassified]),
            trust: BTreeSet::from([AgentCommunalClaimTrust::Verified]),
            reader: BTreeSet::from([capability("cap-a")]),
        }
    }

    fn evidence(id: &str) -> ArtifactRef {
        ArtifactRef {
            artifact_id: id.to_string(),
            kind: ArtifactKind::File,
            uri: format!("s3://evidence/{id}"),
            checksum: None,
            content_type: None,
            byte_len: None,
            retention_class: None,
            encryption: None,
            redaction: RedactionStatus::Unredacted,
            created_at: AgentTimestampMillis::new(1),
            metadata: AgentAttributes::default(),
        }
    }

    #[test]
    fn the_default_policy_admits_only_verified() {
        let policy = CommunalRetrievalPolicy::default();
        assert_eq!(
            policy.trust(),
            &BTreeSet::from([AgentCommunalClaimTrust::Verified])
        );
        assert_eq!(
            policy.classifications(),
            &BTreeSet::from([MemoryClassification::Unclassified])
        );
        assert_eq!(policy.max_results(), 8);
        assert_eq!(policy.max_bytes(), 65_536);
        assert_eq!(policy.max_spaces(), 4);
        assert_eq!(policy.query_entries(), 4);
        assert_eq!(policy.min_confidence_bps(), 0);
        assert_eq!(policy.max_resolutions(), 32);
    }

    #[test]
    fn no_policy_can_admit_retracted() {
        let policy = CommunalRetrievalPolicy::default().with_trust(BTreeSet::from([
            AgentCommunalClaimTrust::Verified,
            AgentCommunalClaimTrust::Retracted,
        ]));
        assert_eq!(
            policy.trust(),
            &BTreeSet::from([AgentCommunalClaimTrust::Verified])
        );

        let proposed_too = CommunalRetrievalPolicy::default().with_trust(BTreeSet::from([
            AgentCommunalClaimTrust::Proposed,
            AgentCommunalClaimTrust::Verified,
            AgentCommunalClaimTrust::Disputed,
            AgentCommunalClaimTrust::Retracted,
        ]));
        assert_eq!(
            proposed_too.trust(),
            &BTreeSet::from([
                AgentCommunalClaimTrust::Proposed,
                AgentCommunalClaimTrust::Verified,
                AgentCommunalClaimTrust::Disputed,
            ])
        );

        // A set that held only `Retracted` admits nothing, rather than
        // falling back to something the deployment did not ask for.
        let nothing = CommunalRetrievalPolicy::default()
            .with_trust(BTreeSet::from([AgentCommunalClaimTrust::Retracted]));
        assert!(nothing.trust().is_empty());
    }

    #[test]
    fn policy_knobs_clamp() {
        let policy = CommunalRetrievalPolicy::default();
        assert_eq!(policy.clone().with_max_results(0).max_results(), 1);
        assert_eq!(policy.clone().with_max_results(99).max_results(), 16);
        assert_eq!(policy.clone().with_max_spaces(0).max_spaces(), 1);
        assert_eq!(policy.clone().with_max_spaces(99).max_spaces(), 16);
        assert_eq!(policy.clone().with_max_bytes(0).max_bytes(), 1);
        assert_eq!(
            policy.clone().with_max_bytes(usize::MAX).max_bytes(),
            65_536
        );
        assert_eq!(policy.clone().with_query_entries(0).query_entries(), 1);
        assert_eq!(policy.clone().with_query_entries(99).query_entries(), 4);
        assert_eq!(
            policy
                .clone()
                .with_min_confidence_bps(20_000)
                .min_confidence_bps(),
            10_000
        );
        // The resolution ceiling follows `max_results` and never exceeds 64.
        assert_eq!(policy.clone().with_max_results(1).max_resolutions(), 4);
        assert_eq!(policy.with_max_results(16).max_resolutions(), 64);
    }

    #[test]
    fn a_query_admits_only_what_the_reader_may_read() {
        let query = query();
        assert!(query.admits(&record()));

        let mut proposed = record();
        proposed.trust = AgentCommunalClaimTrust::Proposed;
        assert!(!query.admits(&proposed), "a trust state not admitted");

        let mut sensitive = record();
        sensitive.classification = MemoryClassification::Sensitive;
        assert!(!query.admits(&sensitive), "a classification not admitted");

        let mut floor = query.clone();
        floor.min_confidence_bps = 5_001;
        assert!(!floor.admits(&record()), "a confidence under the floor");
        floor.min_confidence_bps = 5_000;
        assert!(floor.admits(&record()));

        let mut two = record();
        two.required_read_capabilities.insert(capability("cap-b"));
        assert!(!query.admits(&two), "a capability the reader does not hold");
        let mut nobody = query.clone();
        nobody.reader = BTreeSet::new();
        assert!(!nobody.admits(&record()));
        let mut open = record();
        open.required_read_capabilities = BTreeSet::new();
        assert!(nobody.admits(&open), "a claim requiring nothing");

        // A trust set built by hand to admit `Retracted` still does not: the
        // policy's own clamp is not the only thing between a withdrawn claim
        // and a model.
        let mut hand_built = query.clone();
        hand_built.trust.insert(AgentCommunalClaimTrust::Retracted);
        let mut retracted = record();
        retracted.trust = AgentCommunalClaimTrust::Retracted;
        assert!(!hand_built.admits(&retracted));

        let mut empty = query;
        empty.trust = BTreeSet::new();
        assert!(!empty.admits(&record()), "an empty set admits nothing");
    }

    #[test]
    fn a_record_over_a_bound_fails_validation() {
        record().validate().expect("the base record is valid");

        let mut oversized = record();
        oversized.statement = statement(&"x".repeat(AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES));
        oversized.statement_digest = oversized.statement.digest();
        assert_eq!(
            oversized
                .validate()
                .expect_err("an inline object over the bound is refused")
                .code(),
            "memory-entry-too-large"
        );

        let mut evidenced = record();
        evidenced.evidence = vec![evidence("a"); AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE + 1];
        assert!(evidenced.validate().is_err());
        evidenced
            .evidence
            .truncate(AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE);
        evidenced.validate().expect("the bound itself is accepted");

        let mut guarded = record();
        guarded.required_read_capabilities = (0..=AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES)
            .map(|index| capability(&format!("cap-{index}")))
            .collect();
        assert!(guarded.validate().is_err());

        let mut confident = record();
        confident.confidence_bps = 10_001;
        assert_eq!(
            confident
                .validate()
                .expect_err("a confidence over 10000 is refused")
                .code(),
            "memory-confidence-out-of-range"
        );

        // The digest always describes the statement beside it.
        let mut mismatched = record();
        mismatched.statement = statement("something else entirely");
        assert!(mismatched.validate().is_err());

        // A statement that could not be a claim's is refused.
        let mut empty_subject = record();
        empty_subject.statement.subject = String::new();
        empty_subject.statement_digest = empty_subject.statement.digest();
        assert!(empty_subject.validate().is_err());
        let mut empty_node = record();
        empty_node.statement.object = CommunalClaimObject::Node(String::new());
        empty_node.statement_digest = empty_node.statement.digest();
        assert!(empty_node.validate().is_err());
    }

    #[test]
    fn the_trust_labels_match_the_graphs() {
        assert_eq!(
            AgentCommunalClaimTrust::ALL.map(AgentCommunalClaimTrust::as_label),
            ["proposed", "verified", "disputed", "retracted"]
        );
        // The labels are also the encoding.
        assert_eq!(
            serde_json::to_value(AgentCommunalClaimTrust::ALL).expect("the labels encode"),
            serde_json::json!(["proposed", "verified", "disputed", "retracted"])
        );
    }

    /// A statement encodes as `{"subject", "predicate", "object"}`, with the
    /// object tagged as the graph's own claim object is. The digest is taken
    /// over that value, which is what lets the graph's fingerprint and this
    /// one agree.
    #[test]
    fn a_statement_encodes_as_subject_predicate_object() {
        let value = statement("delayed");
        assert_eq!(
            serde_json::to_value(&value).expect("the statement encodes"),
            serde_json::json!({
                "subject": "order-42",
                "predicate": "status",
                "object": { "value": { "inline": "delayed" } }
            })
        );
        let edge = CommunalClaimStatement {
            subject: "order-42".to_string(),
            predicate: "shipped-by".to_string(),
            object: CommunalClaimObject::Node("carrier-north".to_string()),
        };
        assert_eq!(
            serde_json::to_value(&edge).expect("the statement encodes"),
            serde_json::json!({
                "subject": "order-42",
                "predicate": "shipped-by",
                "object": { "node": "carrier-north" }
            })
        );
        assert_eq!(
            edge.digest(),
            AgentContentDigest::of_json(&serde_json::json!({
                "subject": "order-42",
                "predicate": "shipped-by",
                "object": { "node": "carrier-north" }
            }))
        );
        assert_ne!(edge.digest(), value.digest());
    }
}
