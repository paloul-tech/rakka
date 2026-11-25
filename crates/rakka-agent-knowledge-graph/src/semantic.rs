//! Meaning-based search over the claims of one knowledge space.
//!
//! [Specification 13.6](../../../docs/plans/rakka-agent/spec.md) lets a
//! backend report optional query features; ranking by meaning is one
//! ([`KnowledgeGraphCapability::SemanticSearch`](crate::store::KnowledgeGraphCapability::SemanticSearch)).
//! This module is what every backend that reports it shares: the text a
//! claim is embedded from, the validated embedding, the request, and the one
//! mapping from cosine similarity to a relevance score.
//!
//! # A store never calls a model
//!
//! An embedding is a vector a *caller* computed: the communal retriever for a
//! query, the append executor and the embedding pass for a claim. A store
//! keeps one embedding per claim, beside the reference of the model that
//! produced it, and compares vectors. It never produces one and never
//! returns one.
//!
//! # Ranking is not authority
//!
//! [`ClaimSemanticSearch::admits`] is the keyword search's admission rule,
//! applied before anything is ranked, so a claim its reader may not see never
//! takes a hit slot
//! ([specification 16](../../../docs/plans/rakka-agent/spec.md)). A caller
//! that puts a hit in front of a model still resolves it through the store.
//!
//! # An embedding never outlives its claim's content
//!
//! A tombstone and a delete remove the claim's embedding in the same
//! operation, and no embedding can be stored for a tombstoned claim
//! afterwards.

use rakka_agent::{
    memory_embedding_text, MemoryEmbeddingRef, AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH,
};

use crate::claim::{Claim, ClaimId, ClaimObject};
use crate::error::{ClaimError, ClaimResult};
use crate::search::ClaimSearch;

/// Largest dimension count an embedding may declare. It is pgvector's own
/// ceiling for a stored vector, kept as the portable bound so that an
/// embedding one backend accepts is one every backend can hold.
pub const CLAIM_EMBEDDING_MAX_DIMENSIONS: u32 = 16_000;

/// Largest number of rows whose claim id this build reads that one page of
/// `KnowledgeGraphStore::unembedded_claims` reads. A row whose claim id this
/// build refuses is read past and does not count toward it.
pub const CLAIM_EMBEDDING_PAGE_MAX_CLAIMS: usize = 64;

/// The text a claim is embedded from: its subject, its predicate and its
/// object's text, joined by single spaces, in that order.
///
/// The object gives its node id when it is an edge and its text when it is an
/// inline value (a string as written, any other value as its canonical JSON).
/// A value held by artifact reference gives nothing, so such a claim is
/// embedded from its subject and predicate alone: this crate never loads
/// artifact bytes, and the reference itself is not content. The keyword
/// index takes a claim's words from the same three places.
///
/// A tombstoned stub has no text, because its statement was withdrawn.
///
/// The query side embeds the query text as it is.
#[must_use]
pub fn claim_embedding_text(claim: &Claim) -> Option<String> {
    if claim.is_tombstoned() {
        return None;
    }
    let mut text = format!("{} {}", claim.subject.as_str(), claim.predicate.as_str());
    let object = match &claim.object {
        ClaimObject::Node(node) => Some(node.as_str().to_string()),
        ClaimObject::Value(content) => memory_embedding_text(content),
    };
    if let Some(object) = object {
        text.push(' ');
        text.push_str(&object);
    }
    Some(text)
}

/// One vector, with the reference of the model that produced it.
///
/// The reference travels with the vector everywhere: a store keeps it beside
/// a claim's embedding and a search carries it beside the query's, and only
/// vectors under one reference are ever compared.
#[derive(Clone, PartialEq)]
pub struct ClaimEmbedding {
    /// The model, dimension count and pipeline version that produced the
    /// vector.
    pub reference: MemoryEmbeddingRef,
    /// The vector: exactly `reference.dimensions` finite values.
    pub vector: Vec<f32>,
}

impl ClaimEmbedding {
    /// Checks the embedding against its own reference.
    ///
    /// Every store operation that takes an embedding calls this first, so a
    /// malformed one is refused before anything is read or written.
    ///
    /// # Errors
    ///
    /// Returns `claim-embedding-invalid` when the model name is empty or
    /// longer than [`AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH`], when the
    /// reference declares zero dimensions or more than
    /// [`CLAIM_EMBEDDING_MAX_DIMENSIONS`], when the vector's length is not
    /// the declared dimension count, or when a component is not finite. The
    /// message names lengths, never a component.
    pub fn validate(&self) -> ClaimResult<()> {
        let reference = &self.reference;
        let refused = |message: String| Err(ClaimError::InvalidEmbeddingRef { message });
        if reference.model.is_empty()
            || reference.model.len() > AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH
        {
            return refused(format!(
                "the embedding model must be non-empty and at most \
                 {AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH} bytes, got {} bytes",
                reference.model.len()
            ));
        }
        if reference.dimensions == 0 || reference.dimensions > CLAIM_EMBEDDING_MAX_DIMENSIONS {
            return refused(format!(
                "the embedding model {} declares {} dimensions; a claim embedding has between 1 \
                 and {CLAIM_EMBEDDING_MAX_DIMENSIONS}",
                reference.model, reference.dimensions
            ));
        }
        if self.vector.len() != reference.dimensions as usize {
            return refused(format!(
                "the embedding model {} declares {} dimensions but the vector has {}",
                reference.model,
                reference.dimensions,
                self.vector.len()
            ));
        }
        if self.vector.iter().any(|value| !value.is_finite()) {
            return refused(format!(
                "the vector of the embedding model {} has a component that is not finite",
                reference.model
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for ClaimEmbedding {
    /// Prints the reference and the vector's length. A vector is derived from
    /// a claim's content, so it is never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimEmbedding")
            .field("reference", &self.reference)
            .field("vector_len", &self.vector.len())
            .finish()
    }
}

/// One meaning-based search of one knowledge space: who may be found, and the
/// query's embedding to rank them against.
///
/// The filters are a [`ClaimSearch`]: its trust set, classifications,
/// confidence floor, reader capabilities and limit mean here what they mean
/// in a keyword search. Its text and words are not used.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimSemanticSearch {
    filters: ClaimSearch,
    query: ClaimEmbedding,
}

impl ClaimSemanticSearch {
    /// A search that admits what `filters` admits and ranks by nearness to
    /// `query`.
    #[must_use]
    pub const fn new(filters: ClaimSearch, query: ClaimEmbedding) -> Self {
        Self { filters, query }
    }

    /// The admission filters and the limit.
    #[must_use]
    pub const fn filters(&self) -> &ClaimSearch {
        &self.filters
    }

    /// The query's embedding.
    #[must_use]
    pub const fn query(&self) -> &ClaimEmbedding {
        &self.query
    }

    /// The most hits the search answers: the filters' limit.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.filters.limit()
    }

    /// Whether the search may find `claim`: [`ClaimSearch::admits`] of its
    /// filters, the one admission rule every backend applies before it ranks.
    #[must_use]
    pub fn admits(&self, claim: &Claim) -> bool {
        self.filters.admits(claim)
    }
}

/// The one mapping from a cosine distance to a relevance, in basis points:
/// `(1 - distance / 2) * 10000`, rounded, within `0..=10000`.
///
/// It is the private memory tier's mapping, so a score means the same thing
/// in both tiers: identical directions score 10000, unrelated ones 5000 and
/// opposite ones 0. A distance that is not finite scores 0, which is what a
/// vector with no direction produces.
///
/// A backend whose database computes the distance maps it with this, so
/// every backend scores alike.
#[must_use]
pub fn claim_semantic_distance_relevance_bps(cosine_distance: f64) -> u16 {
    if !cosine_distance.is_finite() {
        return 0;
    }
    let bps = ((1.0 - cosine_distance / 2.0) * 10_000.0)
        .round()
        .clamp(0.0, 10_000.0);
    // The clamp bounds the value to what a `u16` holds, and it is finite.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let bps = bps as u16;
    bps
}

/// The relevance of a candidate vector to a query vector, in basis points:
/// [`claim_semantic_distance_relevance_bps`] of their cosine distance.
///
/// Zero when the two vectors differ in length or either has no direction
/// (zero magnitude). A backend ranks only vectors under one reference, so a
/// length mismatch is not expected; it scores zero rather than panicking.
#[must_use]
pub fn claim_semantic_relevance_bps(query: &[f32], candidate: &[f32]) -> u16 {
    if query.len() != candidate.len() {
        return 0;
    }
    let (mut dot, mut query_norm, mut candidate_norm) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (a, b) in query.iter().zip(candidate) {
        let (a, b) = (f64::from(*a), f64::from(*b));
        dot += a * b;
        query_norm += a * a;
        candidate_norm += b * b;
    }
    if query_norm == 0.0 || candidate_norm == 0.0 {
        return 0;
    }
    let cosine = dot / (query_norm.sqrt() * candidate_norm.sqrt());
    claim_semantic_distance_relevance_bps(1.0 - cosine)
}

/// One bounded page of the claims of a space that have no embedding under a
/// reference.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClaimEmbeddingPage {
    /// The claims, whole, in ascending claim-id order.
    pub claims: Vec<Claim>,
    /// How many records this page read and could not answer, because this
    /// build cannot decode them, cannot vouch for them, or refuses their
    /// claim id. The cursor moves past them; a non-zero count is the signal
    /// to look.
    pub undecodable: u64,
    /// The cursor for the next page: the last claim this page read, when the
    /// page was full. `None` is the end of the walk. Always a claim id: a
    /// row whose claim id this build refuses is read past and does not fill
    /// the page.
    pub next: Option<ClaimId>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rakka_agent::{
        AgentId, AgentRevisionNumber, AgentTaskContent, MemoryClassification, MemoryEmbeddingRef,
        AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH,
    };
    use rakka_agent_workflow::{
        AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
    };

    use super::*;
    use crate::claim::{
        Claim, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimProvenance,
        ClaimTrustStatus,
    };
    use crate::scope::KnowledgeSpaceScope;
    use crate::search::ClaimSearch;
    use crate::tombstone::{ClaimTombstone, ClaimTombstoneReason};

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            rakka_agent::TenantId::new("acme"),
            rakka_agent::KnowledgeSpaceId::new("space-a").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    fn claim(discriminator: &str, object: ClaimObject) -> Claim {
        let scope = scope();
        Claim::new(
            &scope,
            ClaimOperationId::derive_append(&scope, discriminator)
                .expect("the operation id derives"),
            ClaimNodeId::new("shipment").expect("the node id is valid"),
            ClaimPredicate::new("status").expect("the predicate is valid"),
            object,
            ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
            5_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid")
    }

    fn inline(value: serde_json::Value) -> ClaimObject {
        ClaimObject::Value(AgentTaskContent::inline(value).expect("the value is bounded"))
    }

    fn artifact() -> ClaimObject {
        ClaimObject::Value(AgentTaskContent::artifact(ArtifactRef {
            artifact_id: "secretartifactid".to_string(),
            kind: ArtifactKind::File,
            uri: "s3://evidence/secreturi".to_string(),
            checksum: Some("sha256:secretchecksum".to_string()),
            content_type: Some("application/json".to_string()),
            byte_len: Some(64),
            retention_class: None,
            encryption: None,
            redaction: RedactionStatus::Unredacted,
            created_at: AgentTimestampMillis::new(1),
            metadata: AgentAttributes::default(),
        }))
    }

    fn reference(model: &str, dimensions: u32) -> MemoryEmbeddingRef {
        MemoryEmbeddingRef {
            model: model.to_string(),
            dimensions,
            version: AgentRevisionNumber::new(1),
        }
    }

    fn embedding(model: &str, dimensions: u32, vector: Vec<f32>) -> ClaimEmbedding {
        ClaimEmbedding {
            reference: reference(model, dimensions),
            vector,
        }
    }

    #[test]
    fn a_claims_embedding_text_is_its_subject_predicate_and_object() {
        let node = claim(
            "node",
            ClaimObject::Node(ClaimNodeId::new("depot").expect("the node id is valid")),
        );
        assert_eq!(
            claim_embedding_text(&node).as_deref(),
            Some("shipment status depot")
        );
        let text = claim("text", inline(serde_json::json!("at the depot")));
        assert_eq!(
            claim_embedding_text(&text).as_deref(),
            Some("shipment status at the depot")
        );
        let value = claim("value", inline(serde_json::json!({"b": 1, "a": 2})));
        assert_eq!(
            claim_embedding_text(&value).as_deref(),
            Some(r#"shipment status {"a":2,"b":1}"#)
        );
        let held_by_reference = claim("artifact", artifact());
        assert_eq!(
            claim_embedding_text(&held_by_reference).as_deref(),
            Some("shipment status")
        );
        let stub = text
            .apply_tombstone(ClaimTombstone {
                operation_id: ClaimOperationId::derive_tombstone(&scope(), &text.claim_id, "t")
                    .expect("the operation id derives"),
                reason: ClaimTombstoneReason::Withdrawn,
                actor: crate::conformance::conformance_resolver(),
                tombstoned_at: AgentTimestampMillis::new(2),
            })
            .expect("the tombstone applies");
        assert_eq!(claim_embedding_text(&stub), None);
    }

    #[test]
    fn an_embedding_is_refused_unless_it_matches_its_reference() {
        let valid = embedding("model-a", 3, vec![0.25, 0.5, 1.0]);
        valid.validate().expect("a matching finite vector is valid");
        let rendered = format!("{valid:?}");
        assert!(rendered.contains("model-a") && rendered.contains('3'));
        assert!(
            !rendered.contains("0.25"),
            "a vector component is never printed: {rendered}"
        );

        let refused = [
            embedding("model-a", 3, vec![0.25, 0.5]),
            embedding("model-a", 3, vec![0.25, f32::NAN, 1.0]),
            embedding("model-a", 3, vec![0.25, f32::INFINITY, 1.0]),
            embedding("model-a", 0, Vec::new()),
            embedding(
                "model-a",
                CLAIM_EMBEDDING_MAX_DIMENSIONS + 1,
                vec![0.0; CLAIM_EMBEDDING_MAX_DIMENSIONS as usize + 1],
            ),
            embedding("", 3, vec![0.25, 0.5, 1.0]),
            embedding(
                &"m".repeat(AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH + 1),
                3,
                vec![0.25, 0.5, 1.0],
            ),
        ];
        for (index, embedding) in refused.iter().enumerate() {
            let error = embedding
                .validate()
                .expect_err("a malformed embedding is refused");
            assert_eq!(error.code(), "claim-embedding-invalid", "case {index}");
            assert!(
                !error.to_string().contains("0.25"),
                "the refusal never carries a component: {error}"
            );
        }
        embedding(
            "model-a",
            CLAIM_EMBEDDING_MAX_DIMENSIONS,
            vec![0.0; CLAIM_EMBEDDING_MAX_DIMENSIONS as usize],
        )
        .validate()
        .expect("the ceiling itself is allowed");
    }

    #[test]
    fn relevance_is_the_private_tiers_mapping_of_cosine() {
        assert_eq!(
            claim_semantic_relevance_bps(&[1.0, 0.0], &[1.0, 0.0]),
            10_000
        );
        assert_eq!(
            claim_semantic_relevance_bps(&[2.0, 0.0], &[1.0, 0.0]),
            10_000
        );
        assert_eq!(
            claim_semantic_relevance_bps(&[1.0, 0.0], &[0.6, 0.8]),
            8_000
        );
        assert_eq!(
            claim_semantic_relevance_bps(&[1.0, 0.0], &[0.0, 1.0]),
            5_000
        );
        assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[-1.0, 0.0]), 0);
        assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[0.0, 0.0]), 0);
        assert_eq!(
            claim_semantic_relevance_bps(&[1.0, 0.0], &[1.0, 0.0, 0.0]),
            0
        );
        assert_eq!(claim_semantic_relevance_bps(&[], &[]), 0);
        for (distance, bps) in [
            (0.0, 10_000),
            (0.4, 8_000),
            (1.0, 5_000),
            (2.0, 0),
            (2.5, 0),
            (-0.1, 10_000),
        ] {
            assert_eq!(
                claim_semantic_distance_relevance_bps(distance),
                bps,
                "distance {distance}"
            );
        }
        assert_eq!(claim_semantic_distance_relevance_bps(f64::NAN), 0);
        assert_eq!(claim_semantic_distance_relevance_bps(f64::INFINITY), 0);
    }

    #[test]
    fn a_semantic_search_admits_and_limits_as_its_filters_do() {
        let fresh = claim("fresh", inline(serde_json::json!("x")));
        let query = embedding("model-a", 2, vec![1.0, 0.0]);
        let verified_only = ClaimSemanticSearch::new(
            ClaimSearch::new("", 1000)
                .with_trust(BTreeSet::from([ClaimTrustStatus::Verified]))
                .with_classifications(BTreeSet::from([MemoryClassification::Unclassified])),
            query.clone(),
        );
        assert_eq!(verified_only.limit(), crate::search::CLAIM_SEARCH_MAX_HITS);
        assert!(!verified_only.admits(&fresh));
        assert_eq!(verified_only.query(), &query);

        let proposed = ClaimSemanticSearch::new(
            ClaimSearch::new("", 5)
                .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
                .with_classifications(BTreeSet::from([MemoryClassification::Unclassified])),
            query,
        );
        assert_eq!(proposed.limit(), 5);
        assert!(proposed.admits(&fresh));
        assert!(proposed.filters().admits(&fresh));
    }

    #[test]
    fn the_page_clamp_is_frozen() {
        assert_eq!(CLAIM_EMBEDDING_PAGE_MAX_CLAIMS, 64);
        assert_eq!(CLAIM_EMBEDDING_MAX_DIMENSIONS, 16_000);
        assert_eq!(ClaimEmbeddingPage::default().next, None);
    }
}
