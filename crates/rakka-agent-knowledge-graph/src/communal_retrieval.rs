//! The graph-backed communal read ports: what puts a knowledge space's claims
//! within reach of a model context
//! ([specification 13.4, 13.5 and 16](../../docs/plans/rakka-agent/spec.md)).
//!
//! `rakka-agent` declares the two ports, [`AgentCommunalClaimRetriever`] and
//! [`AgentCommunalClaimAuthority`], and cannot see this crate: the dependency
//! runs graph → agent. An application wires these implementations over its
//! [`KnowledgeGraphStore`] into an
//! [`AgentCommunalRetrieval`](rakka_agent::AgentCommunalRetrieval) bundle,
//! exactly as it wires the append executor and the goal-claim source.
//!
//! # Two adapters, two jobs
//!
//! The **retriever** ranks. It turns a query into a [`ClaimSearch`], and
//! answers the identities and scores of what the search found: no statement,
//! no provenance, nothing a caller could mistake for the claim.
//!
//! The **authority** answers the claim. Context assembly resolves every
//! ranked identity through it, in the space whose ranking named it, and
//! decides on the record it answers. That is where a claim's trust state,
//! classification and required read capabilities are enforced: on the record
//! the store holds at that moment, whatever an index said earlier.
//!
//! Both adapters may be wired over one store, and usually are. They stay two
//! types because the ranking is advisory and the record is not.
//!
//! # The mirror types
//!
//! `rakka-agent` names a claim with types of its own
//! ([`CommunalClaimRecord`], [`AgentCommunalClaimTrust`]). The conversions
//! live here and are exhaustive, so a trust status added to
//! [`ClaimTrustStatus`] fails to compile until it is mapped.

use std::sync::Arc;

use rakka_agent::{
    AgentCommunalClaimAuthority, AgentCommunalClaimId, AgentCommunalClaimRetriever,
    AgentCommunalClaimTrust, AgentRevisionNumber, CommunalClaimObject, CommunalClaimProvenance,
    CommunalClaimRecord, CommunalClaimStatement, CommunalRetrievalOutcome, CommunalRetrievalQuery,
    KnowledgeSpaceId, MemoryError, MemoryFuture, RankedCommunalClaim, TenantId,
};
use rakka_agent_workflow::AgentTimestampMillis;

use crate::claim::{Claim, ClaimId, ClaimObject, ClaimTrustStatus};
use crate::error::{ClaimError, ClaimResult};
use crate::scope::KnowledgeSpaceScope;
use crate::search::ClaimSearch;
use crate::store::{KnowledgeGraphCapability, KnowledgeGraphStore};

impl From<ClaimTrustStatus> for AgentCommunalClaimTrust {
    fn from(status: ClaimTrustStatus) -> Self {
        match status {
            ClaimTrustStatus::Proposed => Self::Proposed,
            ClaimTrustStatus::Verified => Self::Verified,
            ClaimTrustStatus::Disputed => Self::Disputed,
            ClaimTrustStatus::Retracted => Self::Retracted,
        }
    }
}

impl TryFrom<AgentCommunalClaimTrust> for ClaimTrustStatus {
    type Error = ClaimError;

    /// Maps a mirrored trust state back to the graph's.
    ///
    /// Fallible because the mirror is declared in another crate and may grow
    /// a state this crate does not know. Such a state names no trust status
    /// here, and a search must not be widened by guessing one.
    fn try_from(trust: AgentCommunalClaimTrust) -> ClaimResult<Self> {
        match trust {
            AgentCommunalClaimTrust::Proposed => Ok(Self::Proposed),
            AgentCommunalClaimTrust::Verified => Ok(Self::Verified),
            AgentCommunalClaimTrust::Disputed => Ok(Self::Disputed),
            AgentCommunalClaimTrust::Retracted => Ok(Self::Retracted),
            unknown => Err(ClaimError::Encoding {
                message: format!(
                    "the communal trust state `{}` names no claim trust status",
                    unknown.as_label()
                ),
            }),
        }
    }
}

/// A store failure as the ports report it. The claim error's stable code is
/// kept in the message, since a memory error has one backend variant.
fn backend_error(backend: &'static str, error: &ClaimError) -> MemoryError {
    MemoryError::Backend {
        backend: backend.to_string(),
        message: format!("{}: {error}", error.code()),
    }
}

/// The scope one port call addresses.
fn space_scope(
    backend: &'static str,
    tenant: &TenantId,
    space: &KnowledgeSpaceId,
) -> Result<KnowledgeSpaceScope, MemoryError> {
    KnowledgeSpaceScope::new(tenant.clone(), space.clone()).map_err(|error| MemoryError::Backend {
        backend: backend.to_string(),
        message: format!("{}: {error}", error.code()),
    })
}

/// Ranks the claims of a knowledge space by keyword search over a wired
/// [`KnowledgeGraphStore`].
///
/// The ranking carries identities and scores only. Every restriction the
/// query names is applied by the store before it ranks, so a claim the reader
/// may not read takes no place in the ranking; context assembly applies them
/// again to the record the authority answers.
#[derive(Clone)]
pub struct KnowledgeGraphCommunalRetriever {
    store: Arc<dyn KnowledgeGraphStore>,
}

impl KnowledgeGraphCommunalRetriever {
    /// Wires the retriever over the store it searches.
    ///
    /// # Errors
    ///
    /// Returns `claim-capability-unsupported` when the store does not report
    /// [`KnowledgeGraphCapability::KeywordSearch`]. A retriever over a store
    /// that cannot search would fail every turn's retrieval; refusing here
    /// makes that a wiring error instead of a permanently degraded run.
    pub fn new(store: Arc<dyn KnowledgeGraphStore>) -> ClaimResult<Self> {
        if !store
            .capabilities()
            .supports(KnowledgeGraphCapability::KeywordSearch)
        {
            return Err(ClaimError::CapabilityUnsupported {
                capability: KnowledgeGraphCapability::KeywordSearch
                    .as_label()
                    .to_string(),
            });
        }
        Ok(Self { store })
    }
}

impl std::fmt::Debug for KnowledgeGraphCommunalRetriever {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnowledgeGraphCommunalRetriever")
            .field("store", &self.store.backend_name())
            .finish()
    }
}

impl AgentCommunalClaimRetriever for KnowledgeGraphCommunalRetriever {
    fn backend_name(&self) -> &'static str {
        self.store.backend_name()
    }

    /// Version 1: keyword search scored by
    /// [`claim_search_relevance_bps`](crate::search::claim_search_relevance_bps).
    /// It moves when the meaning of a score does.
    fn retriever_version(&self) -> AgentRevisionNumber {
        AgentRevisionNumber::new(1)
    }

    fn retrieve<'a>(
        &'a self,
        tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        query: &'a CommunalRetrievalQuery,
        _now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, CommunalRetrievalOutcome> {
        Box::pin(async move {
            let backend = self.store.backend_name();
            let scope = space_scope(backend, tenant, space)?;
            // A withdrawn claim is never read, so no search asks for one. A
            // trust state this crate does not know names no claims.
            let trust = query
                .trust
                .iter()
                .filter(|trust| **trust != AgentCommunalClaimTrust::Retracted)
                .filter_map(|trust| ClaimTrustStatus::try_from(*trust).ok())
                .collect();
            let search = ClaimSearch::new(&query.text, query.limit)
                .with_trust(trust)
                .with_classifications(query.classifications.clone())
                .with_min_confidence_bps(query.min_confidence_bps)
                .with_reader(query.reader.clone());
            let report = self
                .store
                .search(&scope, &search)
                .await
                .map_err(|error| backend_error(backend, &error))?;

            let mut ranked = Vec::with_capacity(report.hits.len());
            for hit in report.hits {
                ranked.push(RankedCommunalClaim {
                    claim: communal_claim_id(backend, &hit.claim.claim_id)?,
                    relevance_bps: hit.relevance_bps,
                });
            }
            Ok(CommunalRetrievalOutcome {
                ranked,
                // The store SPI reports no index position, only whether the
                // index covers the space.
                index_watermark: None,
                index_incomplete: report.unindexed,
            })
        })
    }
}

/// Answers communal claims from a wired [`KnowledgeGraphStore`]: the record
/// context assembly decides on and embeds.
#[derive(Clone)]
pub struct KnowledgeGraphCommunalAuthority {
    store: Arc<dyn KnowledgeGraphStore>,
}

impl KnowledgeGraphCommunalAuthority {
    /// Wires the authority over the store it reads. Every store can serve it:
    /// it needs only `get`.
    #[must_use]
    pub fn new(store: Arc<dyn KnowledgeGraphStore>) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for KnowledgeGraphCommunalAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnowledgeGraphCommunalAuthority")
            .field("store", &self.store.backend_name())
            .finish()
    }
}

impl AgentCommunalClaimAuthority for KnowledgeGraphCommunalAuthority {
    fn backend_name(&self) -> &'static str {
        self.store.backend_name()
    }

    fn resolve<'a>(
        &'a self,
        tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        claim: &'a AgentCommunalClaimId,
        _now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<CommunalClaimRecord>> {
        Box::pin(async move {
            let backend = self.store.backend_name();
            let scope = space_scope(backend, tenant, space)?;
            // An identity that is not a legal claim id names no stored claim.
            let Ok(claim_id) = ClaimId::new(claim.as_str()) else {
                return Ok(None);
            };
            let held = self
                .store
                .get(&scope, &claim_id)
                .await
                .map_err(|error| backend_error(backend, &error))?;
            match held {
                // A tombstoned stub keeps its audit fields and has no
                // statement: there is nothing of it to read.
                Some(claim) if !claim.is_tombstoned() => {
                    communal_claim_record(backend, space, &claim).map(Some)
                }
                _ => Ok(None),
            }
        })
    }
}

fn communal_claim_id(
    backend: &'static str,
    claim_id: &ClaimId,
) -> Result<AgentCommunalClaimId, MemoryError> {
    AgentCommunalClaimId::new(claim_id.as_str()).map_err(|error| MemoryError::Backend {
        backend: backend.to_string(),
        message: format!("{}: {error}", error.code()),
    })
}

/// A live claim of `space` as the record `rakka-agent` reads.
///
/// A claim record carries no tenant or space, so the space is the scope the
/// claim was read under. Every other field is the claim's own. The statement
/// fingerprint is the graph's, carried over and not recomputed, so a record
/// whose statement and fingerprint disagree is caught by the reader.
fn communal_claim_record(
    backend: &'static str,
    space: &KnowledgeSpaceId,
    claim: &Claim,
) -> Result<CommunalClaimRecord, MemoryError> {
    Ok(CommunalClaimRecord {
        claim_id: communal_claim_id(backend, &claim.claim_id)?,
        space: space.clone(),
        statement: CommunalClaimStatement {
            subject: claim.subject.as_str().to_string(),
            predicate: claim.predicate.as_str().to_string(),
            object: match &claim.object {
                ClaimObject::Node(node) => CommunalClaimObject::Node(node.as_str().to_string()),
                ClaimObject::Value(value) => CommunalClaimObject::Value(value.clone()),
            },
        },
        statement_digest: claim.content_digest.clone(),
        trust: claim.trust().into(),
        provenance: CommunalClaimProvenance {
            agent: claim.provenance.agent.clone(),
            goal: claim.provenance.goal.clone(),
            task: claim.provenance.task.clone(),
            run: claim.provenance.run.clone(),
            delegation: claim.provenance.delegation.clone(),
        },
        evidence: claim.evidence.clone(),
        created_at: claim.created_at,
        confidence_bps: claim.confidence_bps,
        classification: claim.classification,
        required_read_capabilities: claim.access.required_read_capabilities.clone(),
    })
}
