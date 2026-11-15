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

use rakka_persistence::DurableStateStore;

use crate::agent::{load_agent_entity_state, AgentEntityState};
use crate::definition::{AgentCapabilityId, AgentRevisionNumber};
use crate::delegation::{delegated_grant, AgentRunDelegationEnvelope};
use crate::dispatch::AgentRunSetupResolver;
use crate::guardrails::{
    AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext, AgentGuardrailDisposition,
};
use crate::identity::{
    validate_identity_segment, AgentCommunalClaimId, AgentDelegationId, AgentGoalId, AgentId,
    AgentRunId, AgentRunScope, AgentScope, AgentTaskId, KnowledgeSpaceId, TenantId,
    AGENT_IDENTITY_MAX_LENGTH,
};
use crate::memory::{
    AgentRunMemory, MemoryClassification, MemoryContextSnapshot, MemoryError, MemoryFuture,
    SnapshotCommunalClaim, SnapshotCommunalRetrieval, SnapshotIngressRecord, SnapshotSessionEntry,
};
use crate::retrieval::{
    bounded_watermark, derive_query_text, AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS,
    AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES, AGENT_MEMORY_RETRIEVAL_RESOLUTION_FACTOR,
};
use crate::schema::AgentSchemaPolicy;
use crate::task::{AgentContentDigest, AgentTaskContent};

/// Most communal claims one context snapshot may select.
pub const AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_ENTRIES: usize = 16;

/// Largest total serialized statement and evidence-reference content the
/// communal claims of one snapshot may embed, in bytes.
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

/// Most serialized bytes one evidence reference of a [`CommunalClaimRecord`]
/// may take.
///
/// A reference names an artifact: an id, a URI, a checksum and a little
/// metadata. It is not content. The knowledge graph bounds how many
/// references a claim carries and not how large each is, and a snapshot
/// copies a selected claim's references into every turn that selects it, so
/// the read path bounds them itself: a claim carrying a larger reference is
/// not read.
pub const AGENT_COMMUNAL_CLAIM_EVIDENCE_REF_MAX_BYTES: usize = 2048;

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
    /// an evidence reference over
    /// [`AGENT_COMMUNAL_CLAIM_EVIDENCE_REF_MAX_BYTES`], more than
    /// [`AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES`] required capabilities,
    /// or a confidence over 10000 basis points.
    ///
    /// An authority is an adapter over a store this crate cannot see. The
    /// bounds are checked here so that every part of a record whose size an
    /// adapter chooses (the statement and the evidence references) is bounded
    /// before it can enter a snapshot. The identifiers are bounded by their
    /// own types.
    ///
    /// # Errors
    ///
    /// As [`CommunalClaimStatement::validate`], plus `memory-entry-too-large`
    /// for an oversized evidence reference and
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
        for reference in &self.evidence {
            let bytes = serde_json::to_vec(reference).map_or(usize::MAX, |bytes| bytes.len());
            if bytes > AGENT_COMMUNAL_CLAIM_EVIDENCE_REF_MAX_BYTES {
                return Err(MemoryError::EntryTooLarge {
                    bytes,
                    maximum: AGENT_COMMUNAL_CLAIM_EVIDENCE_REF_MAX_BYTES,
                });
            }
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

impl AgentCommunalReadGrant {
    /// The run's **effective** read grant: this grant narrowed by the run's
    /// delegation envelope.
    ///
    /// Each set follows the rule a communal append already follows, through
    /// the one function every delegated grant is derived by:
    ///
    /// - the envelope states a set: the two are intersected, so a delegation
    ///   can only ever narrow what the definition and setup grant;
    /// - the envelope has lineage and states nothing: nothing. A chain whose
    ///   statement predates the field cannot prove what it was delegated;
    /// - the envelope is a root's and states nothing, or there is no envelope
    ///   at all: the grant as answered.
    #[must_use]
    pub fn narrowed_by(&self, envelope: Option<&AgentRunDelegationEnvelope>) -> Self {
        let Some(envelope) = envelope else {
            return self.clone();
        };
        let has_lineage = !envelope.lineage.is_empty();
        Self {
            spaces: delegated_grant(
                &self.spaces,
                envelope.knowledge_spaces.as_ref(),
                has_lineage,
            ),
            read_capabilities: delegated_grant(
                &self.read_capabilities,
                envelope.read_capabilities.as_ref(),
                has_lineage,
            ),
            definition_revision: self.definition_revision,
            setup_revision: self.setup_revision,
        }
    }
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

/// The reference [`AgentCommunalReadGrantSource`]: the grant read from the
/// agent entity's durable state.
///
/// It reads what the dispatch gate reads
/// ([`AgentEntityAuthority`](crate::dispatch::AgentEntityAuthority)): the
/// agent's definition and lifecycle status through
/// [`load_agent_entity_state`], and the run's setup through an optional
/// [`AgentRunSetupResolver`]. So a suspension, a termination or a narrower
/// definition reaches the very next assembly, with the agent fully
/// passivated.
///
/// - No agent state, or an agent whose lifecycle status the dispatcher would
///   refuse to dispatch for: no grant.
/// - No setup: the definition envelope's knowledge spaces and read
///   capabilities.
/// - A setup: the intersection of the definition's sets and the setup's.
///   Intersecting, rather than taking the setup's, means a definition
///   narrowed after the setup was admitted still binds.
pub struct AgentEntityCommunalReadGrants<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    store: Arc<Store>,
    policy: AgentSchemaPolicy,
    setups: Option<Arc<dyn AgentRunSetupResolver>>,
}

impl<Store> AgentEntityCommunalReadGrants<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    /// A grant source over the agent entity store, reading agent states under
    /// the given schema-compatibility policy.
    #[must_use]
    pub fn new(store: Arc<Store>, policy: AgentSchemaPolicy) -> Self {
        Self {
            store,
            policy,
            setups: None,
        }
    }

    /// Resolves each run's setup through the given resolver, which must be
    /// the one the deployment's dispatch authority uses: a run reads under
    /// the setup it dispatches under.
    #[must_use]
    pub fn with_setups(mut self, setups: Arc<dyn AgentRunSetupResolver>) -> Self {
        self.setups = Some(setups);
        self
    }
}

impl<Store> Debug for AgentEntityCommunalReadGrants<Store>
where
    Store: DurableStateStore<AgentEntityState>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEntityCommunalReadGrants")
            .field("agents", &self.store.backend_name())
            .field("setups", &self.setups.is_some())
            .finish_non_exhaustive()
    }
}

/// The name [`AgentEntityCommunalReadGrants`] reports.
const AGENT_ENTITY_GRANT_BACKEND: &str = "agent-entity";

impl<Store> AgentCommunalReadGrantSource for AgentEntityCommunalReadGrants<Store>
where
    Store: DurableStateStore<AgentEntityState> + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        AGENT_ENTITY_GRANT_BACKEND
    }

    fn read_grant<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        _now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentCommunalReadGrant>> {
        Box::pin(async move {
            let backend = |error: &dyn fmt::Display| MemoryError::Backend {
                backend: AGENT_ENTITY_GRANT_BACKEND.to_string(),
                message: error.to_string(),
            };
            let agent = AgentScope::new(scope.tenant().clone(), scope.agent().clone())
                .map_err(|error| backend(&error))?;
            let state = load_agent_entity_state(self.store.as_ref(), &agent, &self.policy)
                .await
                .map_err(|error| backend(&error))?;
            let Some(state) = state else {
                return Ok(None);
            };
            // An agent that may not dispatch may not read either.
            if !state.status().permits_dispatch() {
                return Ok(None);
            }
            let definition = state.definition();
            let granted = definition.envelope();
            let setup = self
                .setups
                .as_ref()
                .and_then(|setups| setups.setup_for(scope));
            Ok(Some(match setup {
                None => AgentCommunalReadGrant {
                    spaces: granted.knowledge_spaces.clone(),
                    read_capabilities: granted.read_capabilities.clone(),
                    definition_revision: definition.revision(),
                    setup_revision: None,
                },
                Some(setup) => {
                    let narrowed = setup.envelope();
                    AgentCommunalReadGrant {
                        spaces: granted
                            .knowledge_spaces
                            .intersection(&narrowed.knowledge_spaces)
                            .cloned()
                            .collect(),
                        read_capabilities: granted
                            .read_capabilities
                            .intersection(&narrowed.read_capabilities)
                            .cloned()
                            .collect(),
                        definition_revision: definition.revision(),
                        setup_revision: Some(setup.revision()),
                    }
                }
            }))
        })
    }
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

    /// Sets the selection's byte budget, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES`]. A claim costs the
    /// serialized size of its statement and of its evidence references.
    /// Selection stops at the first claim that would exceed the budget, so
    /// the selection stays a prefix of the ranking.
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

    /// The selection's byte budget, over statements and evidence references.
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

// ===========================================================================
// Selecting communal claims into a context snapshot
// ([specification 13.4, 13.5 and 16](../../../docs/plans/rakka-agent/spec.md)).
// ===========================================================================

/// A bounded telemetry summary of one assembly's communal retrieval; never a
/// correctness input.
///
/// It has every field of the private tier's
/// [`RetrievalReport`](crate::retrieval::RetrievalReport), with the same
/// meaning, and three that only a search of several spaces has.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommunalRetrievalReport {
    /// Whether a communal retrieval was tried: a bundle was wired, a query
    /// was derived, and the run's grant was read or its read failed.
    ///
    /// A run that is granted no space is not a retrieval that found nothing.
    /// It is skipped, and this stays false.
    pub attempted: bool,
    /// Whether the retrieval was cut short by a backend failure: the grant
    /// could not be read, a space's search failed, or the authority failed
    /// part-way through the walk.
    ///
    /// A failed grant read selects nothing. A failed search costs that
    /// space's ranking and the others continue. An authority failure keeps
    /// the prefix already verified.
    pub degraded: bool,
    /// How many claims the snapshot selected.
    pub selected: usize,
    /// How many resolved records the fail-closed re-checks rejected, plus how
    /// many ranked identities repeated one already walked.
    pub rejected: usize,
    /// How many ranked identities the authority did not hold in the space
    /// that was searched.
    ///
    /// Named for what is observed, as the private tier's is: a claim of
    /// another space or tenant, an identity that never existed, and a claim
    /// tombstoned or deleted since it was indexed all land here. A non-zero
    /// value means the retriever and the authority disagree about what the
    /// space holds.
    pub unverified: usize,
    /// How many claims a memory-ingress stage blocked, including transforms
    /// the boundary cannot apply.
    pub blocked: usize,
    /// How many selections a memory-ingress stage transformed.
    pub transformed: usize,
    /// How many report-only findings the selections carry.
    pub reported: usize,
    /// How many claims a `require-checkpoint` outcome dropped fail-closed.
    pub checkpoint_refused: usize,
    /// Whether the walk stopped at
    /// [`CommunalRetrievalPolicy::max_resolutions`] with ranked identities
    /// left unresolved. Nothing failed; the rankings were longer than this
    /// deployment resolves.
    pub resolution_capped: bool,
    /// How many knowledge spaces were asked for a ranking, including one
    /// whose search failed.
    pub spaces_searched: usize,
    /// Whether the run's effective grant named more spaces than
    /// [`CommunalRetrievalPolicy::max_spaces`], so that some were not
    /// searched.
    pub spaces_capped: bool,
    /// Whether a searched space held claims its backend's index does not
    /// cover, so that this selection could not consider them.
    pub index_incomplete: bool,
}

impl CommunalRetrievalReport {
    /// The bounded outcome label for metrics: `skipped`, `degraded`, or
    /// `retrieved`.
    #[must_use]
    pub const fn outcome_label(&self) -> &'static str {
        if !self.attempted {
            "skipped"
        } else if self.degraded {
            "degraded"
        } else {
            "retrieved"
        }
    }
}

/// Derives the communal query one snapshot's assembly runs, from the session
/// window that same assembly selected.
///
/// The text is derived exactly as the private tier's is
/// ([`derive_retrieval_query`](crate::retrieval::derive_retrieval_query)):
/// the last [`CommunalRetrievalPolicy::query_entries`] non-summary entries'
/// inline text, oldest first, joined by newlines, truncated to
/// [`AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES`](crate::retrieval::AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES)
/// keeping the tail. Everything else is the policy's, and `reader` is the
/// capabilities of the run's effective read grant. `None` when the window
/// holds no text to search for.
#[must_use]
pub fn derive_communal_query(
    window: &[SnapshotSessionEntry],
    policy: &CommunalRetrievalPolicy,
    reader: BTreeSet<AgentCapabilityId>,
) -> Option<CommunalRetrievalQuery> {
    let text = derive_query_text(window, policy.query_entries())?;
    Some(CommunalRetrievalQuery {
        text,
        limit: policy.max_results(),
        min_confidence_bps: policy.min_confidence_bps(),
        classifications: policy.classifications().clone(),
        trust: policy.trust().clone(),
        reader,
    })
}

/// The ceiling on a statement a memory-ingress transform may answer, in
/// serialized bytes.
///
/// A statement is two identifiers and an object value, each bounded, so the
/// ceiling is their sum with room for the JSON around them and for escaping.
/// A transform over it is blocked by the chain itself, as
/// `guardrail-transform-oversized`. One under it is still decoded and
/// validated field by field.
const COMMUNAL_STATEMENT_TRANSFORM_MAX_BYTES: usize =
    AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES + 4 * AGENT_IDENTITY_MAX_LENGTH + 256;

/// The statement a memory-ingress transform turned `original` into, when the
/// transform is one the boundary can apply.
///
/// The output must decode as a statement that validates. A stage may rewrite
/// the subject, the predicate, or an inline value. It may not change what a
/// claim points at: a node object stays that node, an artifact reference
/// stays that reference, and an object never changes kind. Those are
/// identities, and a stage that rewrote one would put a statement in front of
/// a model that no claim makes.
fn applicable_transform(
    original: &CommunalClaimStatement,
    transformed: &serde_json::Value,
) -> Option<CommunalClaimStatement> {
    let transformed: CommunalClaimStatement = serde_json::from_value(transformed.clone()).ok()?;
    transformed.validate().ok()?;
    let applicable = match (&original.object, &transformed.object) {
        (CommunalClaimObject::Node(before), CommunalClaimObject::Node(after)) => before == after,
        (CommunalClaimObject::Value(before), CommunalClaimObject::Value(after)) => {
            match (before.inline_value(), after.inline_value()) {
                (Some(_), Some(_)) => true,
                (None, None) => before == after,
                _ => false,
            }
        }
        _ => false,
    };
    applicable.then_some(transformed)
}

/// One entry of the merged ranking: a claim, the space whose ranking named
/// it, and its clamped score.
struct MergedRanking {
    space: KnowledgeSpaceId,
    claim: AgentCommunalClaimId,
    relevance_bps: u16,
}

/// The communal half of
/// [`assemble_context`](crate::retrieval::assemble_context): searches the
/// spaces the run may read, resolves and re-checks every ranked claim,
/// evaluates each at the memory-ingress boundary, and embeds what survives
/// into `snapshot`.
///
/// Infallible, because memory is never the correctness source
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)): every
/// failure degrades to a smaller selection and is said on the report.
///
/// The snapshot is touched only when at least one space was searched. A run
/// with no bundle, no text to search for, no grant or no granted space leaves
/// it exactly as the session and private halves wrote it, which is a version
/// 1 record with the bytes it always had.
pub(crate) async fn select_communal_claims(
    memory: &AgentRunMemory,
    scope: &AgentRunScope,
    delegation: Option<&AgentRunDelegationEnvelope>,
    snapshot: &mut MemoryContextSnapshot,
    now: AgentTimestampMillis,
) -> CommunalRetrievalReport {
    let mut report = CommunalRetrievalReport::default();
    let Some(bundle) = memory.communal_retrieval() else {
        return report;
    };
    let policy = bundle.policy();

    // 1. The query, before anything is read: a turn with nothing to search
    //    for costs no grant read. The reader is filled in once it is known.
    let Some(mut query) = derive_communal_query(&snapshot.session, policy, BTreeSet::new()) else {
        return report;
    };

    // 2. The grant. A source that cannot answer degrades the turn to no
    //    communal selection: nothing may be searched on a grant nobody read.
    let grant = match bundle.grants().read_grant(scope, now).await {
        Ok(Some(grant)) => grant.narrowed_by(delegation),
        Ok(None) => return report,
        Err(_) => {
            report.attempted = true;
            report.degraded = true;
            return report;
        }
    };
    if grant.spaces.is_empty() {
        return report;
    }
    report.attempted = true;
    report.spaces_capped = grant.spaces.len() > policy.max_spaces();
    query.reader = grant.read_capabilities.clone();

    // 3. One search per granted space, in ascending id order. A space outside
    //    the effective grant is never named to the retriever at all.
    let tenant = scope.tenant();
    let retriever = bundle.retriever();
    let mut searched: Vec<KnowledgeSpaceId> = Vec::new();
    let mut merged: Vec<MergedRanking> = Vec::new();
    let mut watermarks: Vec<Option<String>> = Vec::new();
    for space in grant.spaces.iter().take(policy.max_spaces()) {
        searched.push(space.clone());
        match retriever.retrieve(tenant, space, &query, now).await {
            Ok(outcome) => {
                report.index_incomplete |= outcome.index_incomplete;
                watermarks.push(outcome.index_watermark);
                merged.extend(outcome.ranked.into_iter().map(|ranked| MergedRanking {
                    space: space.clone(),
                    claim: ranked.claim,
                    relevance_bps: ranked.relevance_bps.min(10_000),
                }));
            }
            // One space's outage costs that space's ranking. The others are
            // independent searches and continue.
            Err(_) => report.degraded = true,
        }
    }
    report.spaces_searched = searched.len();

    // 4. One ranking: best first, ties broken by space and then claim so the
    //    order is a function of the answers and not of their arrival.
    merged.sort_by(|a, b| {
        b.relevance_bps
            .cmp(&a.relevance_bps)
            .then_with(|| a.space.cmp(&b.space))
            .then_with(|| a.claim.cmp(&b.claim))
    });

    // 5. The walk.
    let chain = bundle.guardrails();
    let mut selections: Vec<SnapshotCommunalClaim> = Vec::new();
    let mut selected_bytes = 0usize;
    let mut seen: BTreeSet<(KnowledgeSpaceId, AgentCommunalClaimId)> = BTreeSet::new();
    let mut resolved = 0usize;

    for ranked in merged {
        if selections.len() >= policy.max_results() {
            break;
        }
        // A claim's identity is unique within its space, so the pair is what
        // repeats. Dedup precedes the read, so a ranking that names one claim
        // many times cannot multiply the reads.
        if !seen.insert((ranked.space.clone(), ranked.claim.clone())) {
            report.rejected += 1;
            continue;
        }
        // The resolution ceiling, checked immediately before the read it
        // bounds, so the length of an adapter's answer cannot decide how many
        // point reads the settle pass performs.
        if resolved >= policy.max_resolutions() {
            report.resolution_capped = true;
            break;
        }
        resolved += 1;

        // The ranking names; the authority answers. The claim is resolved in
        // the space whose ranking named it, so a claim of another space or
        // another tenant is simply not there.
        let record = match bundle
            .authority()
            .resolve(tenant, &ranked.space, &ranked.claim, now)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                report.unverified += 1;
                continue;
            }
            Err(_) => {
                // Stop and keep what is already verified, as the private
                // tier does: every selection held is a record this authority
                // answered, and the snapshot is immutable.
                report.degraded = true;
                break;
            }
        };

        // Fail-closed re-checks over the authoritative record. It must be the
        // claim that was asked for, in the space that was asked, within a
        // claim's bounds, and one this query may be answered with: trust,
        // classification, confidence and the reader's capabilities are all
        // decided here, on the record, whatever the ranking implied.
        if record.claim_id != ranked.claim
            || record.space != ranked.space
            || record.validate().is_err()
            || !query.admits(&record)
        {
            report.rejected += 1;
            continue;
        }

        // The memory-ingress boundary: one evaluation per claim, on the
        // statement, with the claim's identity in the context.
        let context = AgentGuardrailContext::new(AgentGuardrailBoundary::MemoryIngress, scope)
            .with_claim(&record.claim_id, &record.space);
        let decision = chain.evaluate_bounded(
            &context,
            &record.statement.to_json(),
            COMMUNAL_STATEMENT_TRANSFORM_MAX_BYTES,
        );
        match &decision.disposition {
            AgentGuardrailDisposition::Blocked { .. } => {
                report.blocked += 1;
                continue;
            }
            AgentGuardrailDisposition::CheckpointRequired { .. } => {
                // Fail-closed drop: there is no checkpoint plumbing at
                // snapshot assembly, and parking a model turn on a per-claim
                // grant would make memory a liveness gate.
                report.checkpoint_refused += 1;
                continue;
            }
            AgentGuardrailDisposition::Allowed => {}
        }

        let (statement, statement_digest) = if decision.transformed {
            let Some(transformed) = applicable_transform(&record.statement, &decision.content)
            else {
                report.blocked += 1;
                continue;
            };
            let digest = transformed.digest();
            (transformed, digest)
        } else {
            (record.statement, record.statement_digest)
        };

        // Stop at the first claim that does not fit, rather than skipping it
        // for a smaller one ranked below: the selection stays a prefix of the
        // ranking that survived the checks. A claim costs what the snapshot
        // will hold of it whose size the claim decides: its statement as
        // included and its evidence references.
        let claim_bytes = statement
            .size_bytes()
            .saturating_add(evidence_size_bytes(&record.evidence));
        if selected_bytes.saturating_add(claim_bytes) > policy.max_bytes() {
            break;
        }
        selected_bytes += claim_bytes;

        // Outcome accounting follows the budget check, so every count here
        // describes a claim the snapshot actually embeds.
        if decision.transformed {
            report.transformed += 1;
        }
        report.reported += decision.reports.len();

        selections.push(SnapshotCommunalClaim {
            claim_id: record.claim_id,
            space: record.space,
            trust: record.trust,
            statement,
            statement_digest,
            provenance: record.provenance,
            evidence: record.evidence,
            created_at: record.created_at,
            classification: record.classification,
            confidence_bps: record.confidence_bps,
            // The one value a ranking legitimately owns.
            relevance_bps: ranked.relevance_bps,
            transforms: decision
                .transforms
                .iter()
                .map(|transform| SnapshotIngressRecord {
                    stage: transform.stage.clone(),
                    revision: transform.revision,
                    reason_code: transform.reason_code.clone(),
                })
                .collect(),
            reports: decision
                .reports
                .iter()
                .map(|finding| SnapshotIngressRecord {
                    stage: finding.stage.clone(),
                    revision: finding.revision,
                    reason_code: finding.reason_code.clone(),
                })
                .collect(),
        });
    }

    report.selected = selections.len();
    snapshot.set_communal(
        SnapshotCommunalRetrieval {
            query: query.text,
            retriever: retriever.backend_name().to_string(),
            retriever_version: retriever.retriever_version(),
            index_watermark: common_watermark(watermarks),
            spaces: searched,
            trust: policy.trust().clone(),
            classifications: policy.classifications().clone(),
            min_confidence_bps: policy.min_confidence_bps(),
            index_incomplete: report.index_incomplete,
            ingress_revision: Some(chain.revision()),
            definition_revision: grant.definition_revision,
            setup_revision: grant.setup_revision,
        },
        selections,
        selected_bytes,
    );
    report
}

/// What a claim's evidence references cost a snapshot's byte budget: their
/// serialized size, and nothing for a claim that carries none.
fn evidence_size_bytes(evidence: &[ArtifactRef]) -> usize {
    if evidence.is_empty() {
        return 0;
    }
    serde_json::to_vec(evidence).map_or(usize::MAX, |bytes| bytes.len())
}

/// The index watermark one snapshot records for a search of several spaces:
/// the watermark every answering space reported, when they all reported the
/// same one, and nothing otherwise.
///
/// A snapshot has one watermark field. Spaces whose indexes stand at
/// different points have no single position to record, and recording one of
/// them would claim it for the others.
fn common_watermark(watermarks: Vec<Option<String>>) -> Option<String> {
    let mut watermarks = watermarks.into_iter();
    let first = watermarks.next()??;
    watermarks
        .all(|watermark| watermark.as_deref() == Some(first.as_str()))
        .then(|| bounded_watermark(first))
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
