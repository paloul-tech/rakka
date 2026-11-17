//! Shared fixtures of the meaning-based search tests: a store that serves no
//! optional capability, claims with chosen text, and the concept embedder's
//! vocabulary.

#![allow(dead_code)]

use std::collections::BTreeSet;

use rakka_agent::{AgentId, AgentTaskContent, MemoryClassification, MemoryEmbeddingRef};
use rakka_agent_knowledge_graph::conformance::{conformance_resolver, ConceptEmbedder};
use rakka_agent_knowledge_graph::store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalReport, KnowledgeGraphCapabilities,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimDeleteRequest, ClaimEmbedding, ClaimEmbeddingPage, ClaimError, ClaimFuture,
    ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimPromotionPolicy,
    ClaimProvenance, ClaimSearch, ClaimSearchReport, ClaimSemanticSearch, ClaimTombstoneRequest,
    ClaimTransitionOutcome, ClaimTrustStatus, ClaimTrustTransitionRequest,
    InMemoryKnowledgeGraphStore, KnowledgeGraphCapability, KnowledgeGraphStore,
    KnowledgeSpaceScope,
};
use rakka_agent_workflow::AgentTimestampMillis;

/// A conformant store that reports no optional capability: every mandatory
/// operation delegates to the in-memory store, and the optional ones are
/// left to the trait's refusing defaults.
#[derive(Default)]
pub struct CoreOnlyStore {
    pub inner: InMemoryKnowledgeGraphStore,
}

impl KnowledgeGraphStore for CoreOnlyStore {
    fn backend_name(&self) -> &'static str {
        "in-memory-core-only"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core()
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.append(scope, claim)
    }

    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        self.inner.get(scope, claim_id)
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        self.inner.query(scope, filter, cursor)
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        self.inner.traverse(scope, traversal)
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        self.inner.transition(scope, request, policy, now)
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        self.inner.transitions(scope, claim_id, cursor)
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTombstoneRequest,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.tombstone(scope, request)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimDeleteRequest,
    ) -> ClaimFuture<'a, ()> {
        self.inner.delete(scope, request)
    }
}

/// A store that serves meaning-based search badly, the way a backend with a
/// drifted index or a failing database would: its page of claims owed an
/// embedding lists **every** claim of the space, whatever its classification
/// or trust, and it can be told to fail every embedding write.
///
/// It exists to hold the callers to their own checks. What a store lists is
/// a nomination; the record decides what may be sent to a model.
#[derive(Default)]
pub struct MisbehavingSemanticStore {
    pub inner: InMemoryKnowledgeGraphStore,
    pub fail_embedding_writes: bool,
}

impl KnowledgeGraphStore for MisbehavingSemanticStore {
    fn backend_name(&self) -> &'static str {
        "in-memory-misbehaving"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core().with_capability(KnowledgeGraphCapability::SemanticSearch)
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.append(scope, claim)
    }

    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        self.inner.get(scope, claim_id)
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        self.inner.query(scope, filter, cursor)
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        self.inner.traverse(scope, traversal)
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        self.inner.transition(scope, request, policy, now)
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        self.inner.transitions(scope, claim_id, cursor)
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTombstoneRequest,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.tombstone(scope, request)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimDeleteRequest,
    ) -> ClaimFuture<'a, ()> {
        self.inner.delete(scope, request)
    }

    fn search_semantic<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        search: &'a ClaimSemanticSearch,
    ) -> ClaimFuture<'a, ClaimSearchReport> {
        self.inner.search_semantic(scope, search)
    }

    fn store_claim_embedding<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        embedding: &'a ClaimEmbedding,
    ) -> ClaimFuture<'a, bool> {
        if self.fail_embedding_writes {
            return Box::pin(async {
                Err(ClaimError::Backend {
                    backend: "in-memory-misbehaving".to_string(),
                    message: "the embedding columns do not exist".to_string(),
                })
            });
        }
        self.inner.store_claim_embedding(scope, claim_id, embedding)
    }

    fn unembedded_claims<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        _reference: &'a MemoryEmbeddingRef,
        _classifications: &'a BTreeSet<MemoryClassification>,
        _after: Option<&'a ClaimId>,
        _limit: usize,
    ) -> ClaimFuture<'a, ClaimEmbeddingPage> {
        Box::pin(async move {
            // Every claim the space holds, tombstoned stubs included.
            let page = self
                .inner
                .query(
                    scope,
                    &ClaimFilter::matching_all().including_tombstoned(),
                    ClaimCursor::start().with_limit(64),
                )
                .await?;
            Ok(ClaimEmbeddingPage {
                claims: page.claims,
                undecodable: 0,
                next: None,
            })
        })
    }
}

/// The vocabulary of the tests: two concepts, each said in two ways that
/// share no word.
pub fn embedder() -> ConceptEmbedder {
    ConceptEmbedder::new(&[&["car", "automobile"], &["boat", "ship"]])
}

/// A claim whose inline value is `text`. Its subject and predicate are words
/// outside the vocabulary, so the text alone decides the claim's direction.
pub fn stated(
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    text: &str,
    classification: MemoryClassification,
) -> Claim {
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, discriminator).expect("the operation id derives"),
        ClaimNodeId::new("fleetnote").expect("the node id is valid"),
        ClaimPredicate::new("reports").expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::Value::String(text.to_string()))
                .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        classification,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
}

/// Appends [`stated`].
pub async fn append_stated(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    text: &str,
    classification: MemoryClassification,
) -> Claim {
    store
        .append(scope, &stated(scope, discriminator, text, classification))
        .await
        .expect("the claim appends")
}

/// Moves a claim to `Retracted`.
pub async fn retract(store: &dyn KnowledgeGraphStore, scope: &KnowledgeSpaceScope, claim: &Claim) {
    store
        .transition(
            scope,
            &ClaimTrustTransitionRequest::new(
                claim.claim_id.clone(),
                ClaimOperationId::derive_transition(scope, &claim.claim_id, "retract")
                    .expect("the operation id derives"),
                ClaimTrustStatus::Retracted,
                conformance_resolver(),
                AgentTimestampMillis::new(10),
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the retraction applies");
}

/// The classifications a test deployment lets its embedder see.
pub fn unclassified() -> BTreeSet<MemoryClassification> {
    BTreeSet::from([MemoryClassification::Unclassified])
}

/// A search for the meaning of `text`, admitting fresh claims of every
/// classification: `Proposed`, read by a reader holding nothing.
pub async fn meaning_of(
    embedder: &ConceptEmbedder,
    text: &str,
    limit: usize,
) -> ClaimSemanticSearch {
    use rakka_agent::AgentMemoryEmbedder;
    let vector = embedder.embed(text).await.expect("the query embeds");
    ClaimSemanticSearch::new(
        ClaimSearch::new("", limit)
            .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
            .with_classifications(BTreeSet::from([
                MemoryClassification::Unclassified,
                MemoryClassification::Sensitive,
                MemoryClassification::Redacted,
            ])),
        ClaimEmbedding {
            reference: embedder.embedding_ref(),
            vector,
        },
    )
}
