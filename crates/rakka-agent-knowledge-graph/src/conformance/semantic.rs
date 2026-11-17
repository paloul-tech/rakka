//! Conformance clauses for meaning-based search
//! ([specification 13.6 and 16](../../../../docs/plans/rakka-agent/spec.md):
//! tenant, space, classification, trust and per-claim read capabilities are
//! enforced before results are ranked).
//!
//! [`semantic_search`] runs only against a backend that reports
//! [`KnowledgeGraphCapability::SemanticSearch`](crate::store::KnowledgeGraphCapability::SemanticSearch);
//! [`semantic_search_refused`] holds every other backend to the refusal. A
//! backend that reports the capability must answer the same hits in the same
//! order with the same scores, because all of them use one admission rule and
//! one mapping from cosine similarity to relevance.
//!
//! The clauses hand the store vectors directly. No model is involved: a
//! store never calls one.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use rakka_agent::{
    tokenize_search_text, AgentCapabilityId, AgentId, AgentMemoryEmbedder, AgentRevisionNumber,
    AgentTaskContent, KnowledgeSpaceId, MemoryClassification, MemoryEmbeddingRef, MemoryError,
    MemoryFuture,
};
use rakka_agent_workflow::AgentTimestampMillis;

use super::erasure::{delete_request, tombstone_request};
use super::{transition_request, ConformanceScopes};
use crate::claim::{
    Claim, ClaimAccess, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimProvenance, ClaimTrustStatus,
};
use crate::promotion::ClaimPromotionPolicy;
use crate::scope::KnowledgeSpaceScope;
use crate::search::{ClaimSearch, ClaimSearchReport};
use crate::semantic::{
    ClaimEmbedding, ClaimEmbeddingPage, ClaimSemanticSearch, CLAIM_EMBEDDING_PAGE_MAX_CLAIMS,
};
use crate::store::KnowledgeGraphStore;
use crate::tombstone::ClaimTombstoneReason;

/// The direction every query in these clauses points in.
const QUERY: [f32; 3] = [1.0, 0.0, 0.0];
/// A direction whose relevance to [`QUERY`] is exactly 8000.
const NEAR: [f32; 3] = [0.6, 0.8, 0.0];
/// A direction unrelated to [`QUERY`]: relevance exactly 5000.
const UNRELATED: [f32; 3] = [0.0, 1.0, 0.0];

/// The conformance embedding reference at one pipeline version.
fn reference(version: u64) -> MemoryEmbeddingRef {
    MemoryEmbeddingRef {
        model: "conformance-embedder".to_string(),
        dimensions: 3,
        version: AgentRevisionNumber::new(version),
    }
}

fn embedding(version: u64, vector: [f32; 3]) -> ClaimEmbedding {
    ClaimEmbedding {
        reference: reference(version),
        vector: vector.to_vec(),
    }
}

/// A space of the primary tenant that one sub-clause has to itself. Every
/// live claim of a space is a candidate of every search of it, so sub-clauses
/// that shared a space would see each other's claims.
fn space(scopes: &ConformanceScopes, name: &str) -> KnowledgeSpaceScope {
    KnowledgeSpaceScope::new(
        scopes.primary.tenant().clone(),
        KnowledgeSpaceId::new(name).expect("the space id is valid"),
    )
    .expect("the scope is valid")
}

/// What one conformance claim is, as far as admission is concerned.
struct Stated<'a> {
    discriminator: &'a str,
    confidence_bps: u16,
    classification: MemoryClassification,
    required: &'a [&'a str],
}

impl<'a> Stated<'a> {
    fn new(discriminator: &'a str) -> Self {
        Self {
            discriminator,
            confidence_bps: 5_000,
            classification: MemoryClassification::Unclassified,
            required: &[],
        }
    }
}

fn capability(id: &str) -> AgentCapabilityId {
    AgentCapabilityId::new(id).expect("the capability id is valid")
}

async fn append(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    stated: Stated<'_>,
) -> Claim {
    let operation_id = ClaimOperationId::derive_append(scope, stated.discriminator)
        .expect("the operation id derives");
    let claim = Claim::new(
        scope,
        operation_id,
        ClaimNodeId::new("subjectnode").expect("the node id is valid"),
        ClaimPredicate::new("predicateword").expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::Value::String(stated.discriminator.to_string()))
                .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        stated.confidence_bps,
        stated.classification,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
    .with_access(ClaimAccess {
        required_read_capabilities: stated.required.iter().map(|id| capability(id)).collect(),
    })
    .expect("the access set is bounded");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends")
}

/// Stores a version 1 embedding for a claim that must be there to take it.
async fn embed(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
    vector: [f32; 3],
) {
    assert!(
        store
            .store_claim_embedding(scope, &claim.claim_id, &embedding(1, vector))
            .await
            .expect("the embedding stores"),
        "a live claim of the scope takes an embedding"
    );
}

async fn append_embedded(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    vector: [f32; 3],
) -> Claim {
    let claim = append(store, scope, Stated::new(discriminator)).await;
    embed(store, scope, &claim, vector).await;
    claim
}

/// Filters admitting what a fresh conformance claim is: `Proposed` and
/// `Unclassified`, read by a reader holding nothing.
fn filters(limit: usize) -> ClaimSearch {
    ClaimSearch::new("", limit)
        .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
        .with_classifications(BTreeSet::from([MemoryClassification::Unclassified]))
}

/// A version 1 search toward [`QUERY`].
fn toward_query(filters: ClaimSearch) -> ClaimSemanticSearch {
    ClaimSemanticSearch::new(filters, embedding(1, QUERY))
}

async fn run(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    search: &ClaimSemanticSearch,
) -> ClaimSearchReport {
    store
        .search_semantic(scope, search)
        .await
        .expect("the search answers")
}

fn hit_ids(report: &ClaimSearchReport) -> Vec<ClaimId> {
    report
        .hits
        .iter()
        .map(|hit| hit.claim.claim_id.clone())
        .collect()
}

/// Ascending claim ids: the order ties are broken in, and a page is read in.
fn ascending(claims: &[&Claim]) -> Vec<ClaimId> {
    let mut ids: Vec<ClaimId> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    ids
}

async fn page(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    version: u64,
    classifications: &[MemoryClassification],
    after: Option<&ClaimId>,
    limit: usize,
) -> ClaimEmbeddingPage {
    store
        .unembedded_claims(
            scope,
            &reference(version),
            &classifications.iter().copied().collect(),
            after,
            limit,
        )
        .await
        .expect("the page answers")
}

fn page_ids(page: &ClaimEmbeddingPage) -> Vec<ClaimId> {
    page.claims
        .iter()
        .map(|claim| claim.claim_id.clone())
        .collect()
}

const UNCLASSIFIED: &[MemoryClassification] = &[MemoryClassification::Unclassified];

/// One excluded claim embedded at exactly the query, with exactly `limit`
/// admitted claims behind it: the excluded claim must take no slot.
async fn excluded_takes_no_slot(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    case: &str,
    excluded: &Claim,
    search: ClaimSemanticSearch,
) {
    let mut admitted = Vec::new();
    for index in 0..search.limit() {
        admitted
            .push(append_embedded(store, scope, &format!("{case}-admitted-{index}"), NEAR).await);
    }
    let report = run(store, scope, &search).await;
    assert_eq!(
        hit_ids(&report),
        ascending(&admitted.iter().collect::<Vec<_>>()),
        "the excluded claim took a slot an admitted claim was owed ({case})"
    );
    assert!(
        !hit_ids(&report).contains(&excluded.claim_id),
        "an excluded claim is never a hit ({case})"
    );
}

/// A store that does not report `semantic-search` refuses its three
/// operations, so a caller learns that the store cannot rank by meaning
/// instead of reading an empty answer as "nothing found".
pub async fn semantic_search_refused(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let claim = append(store, scope, Stated::new("refused")).await;
    let refusals = [
        store
            .search_semantic(scope, &toward_query(filters(8)))
            .await
            .map(|_| ())
            .expect_err("a store without the capability refuses the search"),
        store
            .store_claim_embedding(scope, &claim.claim_id, &embedding(1, QUERY))
            .await
            .map(|_| ())
            .expect_err("a store without the capability refuses an embedding"),
        store
            .unembedded_claims(scope, &reference(1), &BTreeSet::new(), None, 8)
            .await
            .map(|_| ())
            .expect_err("a store without the capability refuses the page"),
    ];
    for error in refusals {
        assert_eq!(error.code(), "claim-capability-unsupported");
        assert!(
            error.to_string().contains("semantic-search"),
            "the refusal names the capability: {error}"
        );
    }
}

/// Meaning-based search: admission precedes ranking, ranking is the shared
/// mapping's, one embedding per claim under one reference, erasure reaches
/// the embedding, and the page of unembedded claims is bounded and exact.
#[allow(clippy::too_many_lines)]
pub async fn semantic_search(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    // --- Ranking: descending relevance, ties by ascending claim id. --------
    let scope = &space(&scopes, "sem-ranking");
    let exact = append_embedded(store, scope, "rank-exact", QUERY).await;
    let near = append_embedded(store, scope, "rank-near", NEAR).await;
    let unrelated_a = append_embedded(store, scope, "rank-unrelated-a", UNRELATED).await;
    let unrelated_b = append_embedded(store, scope, "rank-unrelated-b", UNRELATED).await;
    let opposite = append_embedded(store, scope, "rank-opposite", [-1.0, 0.0, 0.0]).await;
    let directionless = append_embedded(store, scope, "rank-zero", [0.0, 0.0, 0.0]).await;

    let ranked = run(store, scope, &toward_query(filters(64))).await;
    let mut expected = vec![exact.claim_id.clone(), near.claim_id.clone()];
    expected.extend(ascending(&[&unrelated_a, &unrelated_b]));
    assert_eq!(hit_ids(&ranked), expected);
    assert_eq!(
        ranked
            .hits
            .iter()
            .map(|hit| hit.relevance_bps)
            .collect::<Vec<_>>(),
        vec![10_000, 8_000, 5_000, 5_000]
    );
    for scoreless in [&opposite, &directionless] {
        assert!(
            !hit_ids(&ranked).contains(&scoreless.claim_id),
            "a candidate that scores nothing is not a hit"
        );
    }
    assert_eq!(
        ranked.hits[0].claim, exact,
        "a hit is the stored claim, whole"
    );
    assert!(
        !ranked.unindexed,
        "every admitted claim of the space is embedded under the query's reference"
    );
    let cut = run(store, scope, &toward_query(filters(2))).await;
    assert_eq!(
        hit_ids(&cut),
        vec![exact.claim_id.clone(), near.claim_id.clone()],
        "the limit keeps the best"
    );

    // --- Admission precedes ranking. ---------------------------------------
    // Each excluded claim is embedded at exactly the query and would rank
    // first. Each case has a space of its own.
    let scope = &space(&scopes, "sem-trust");
    let disputed = append_embedded(store, scope, "trust-x", QUERY).await;
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &disputed.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies");
    excluded_takes_no_slot(store, scope, "trust", &disputed, toward_query(filters(3))).await;

    let scope = &space(&scopes, "sem-classification");
    let sensitive = append(
        store,
        scope,
        Stated {
            classification: MemoryClassification::Sensitive,
            ..Stated::new("class-x")
        },
    )
    .await;
    embed(store, scope, &sensitive, QUERY).await;
    excluded_takes_no_slot(
        store,
        scope,
        "classification",
        &sensitive,
        toward_query(filters(3)),
    )
    .await;

    let scope = &space(&scopes, "sem-confidence");
    let unsure = append(
        store,
        scope,
        Stated {
            confidence_bps: 1_000,
            ..Stated::new("confidence-x")
        },
    )
    .await;
    embed(store, scope, &unsure, QUERY).await;
    excluded_takes_no_slot(
        store,
        scope,
        "confidence",
        &unsure,
        toward_query(filters(3).with_min_confidence_bps(4_000)),
    )
    .await;

    let scope = &space(&scopes, "sem-capability");
    let guarded = append(
        store,
        scope,
        Stated {
            required: &["cap-guarded"],
            ..Stated::new("capability-x")
        },
    )
    .await;
    embed(store, scope, &guarded, QUERY).await;
    excluded_takes_no_slot(
        store,
        scope,
        "capability",
        &guarded,
        toward_query(filters(3).with_reader(BTreeSet::from([capability("cap-other")]))),
    )
    .await;
    assert!(
        hit_ids(
            &run(
                store,
                scope,
                &toward_query(filters(64).with_reader(BTreeSet::from([capability("cap-guarded")])))
            )
            .await
        )
        .contains(&guarded.claim_id),
        "a reader holding the capability finds the claim"
    );

    let scope = &space(&scopes, "sem-tombstone");
    let withdrawn = append_embedded(store, scope, "tombstone-x", QUERY).await;
    store
        .tombstone(
            scope,
            &tombstone_request(
                scope,
                &withdrawn.claim_id,
                "gone",
                ClaimTombstoneReason::Withdrawn,
            ),
        )
        .await
        .expect("the tombstone applies");
    excluded_takes_no_slot(
        store,
        scope,
        "tombstone",
        &withdrawn,
        toward_query(filters(3)),
    )
    .await;

    // A claim of another tenant, and one of another space of this tenant,
    // are no candidates here; nor is this space's claim a candidate there.
    let scope = &space(&scopes, "sem-scope");
    let other_space = space(&scopes, "sem-scope-other");
    let mine = append_embedded(store, scope, "scope-mine", NEAR).await;
    let foreign = append_embedded(store, &scopes.foreign, "scope-foreign", QUERY).await;
    let elsewhere = append_embedded(store, &other_space, "scope-elsewhere", QUERY).await;
    assert_eq!(
        hit_ids(&run(store, scope, &toward_query(filters(64))).await),
        vec![mine.claim_id.clone()]
    );
    assert_eq!(
        hit_ids(&run(store, &scopes.foreign, &toward_query(filters(64))).await),
        vec![foreign.claim_id.clone()]
    );
    assert_eq!(
        hit_ids(&run(store, &other_space, &toward_query(filters(64))).await),
        vec![elsewhere.claim_id.clone()]
    );

    // --- A claim with no embedding under the query's reference. ------------
    let scope = &space(&scopes, "sem-unembedded");
    let late = append(store, scope, Stated::new("late")).await;
    let before = run(store, scope, &toward_query(filters(64))).await;
    assert!(before.hits.is_empty(), "an unembedded claim is not a hit");
    assert!(
        before.unindexed,
        "an admitted claim with no embedding makes the answer incomplete"
    );
    assert!(store
        .store_claim_embedding(scope, &late.claim_id, &embedding(2, QUERY))
        .await
        .expect("the embedding stores"));
    let other_reference = run(store, scope, &toward_query(filters(64))).await;
    assert!(
        other_reference.hits.is_empty(),
        "a vector under another reference is never ranked"
    );
    assert!(other_reference.unindexed);
    embed(store, scope, &late, QUERY).await;
    let after = run(store, scope, &toward_query(filters(64))).await;
    assert_eq!(hit_ids(&after), vec![late.claim_id.clone()]);
    assert!(!after.unindexed);

    // --- `unindexed` follows the search's own filters. ----------------------
    let scope = &space(&scopes, "sem-unindexed-filters");
    append_embedded(store, scope, "filters-embedded", QUERY).await;
    append(
        store,
        scope,
        Stated {
            classification: MemoryClassification::Sensitive,
            ..Stated::new("filters-sensitive")
        },
    )
    .await;
    assert!(
        !run(store, scope, &toward_query(filters(64)))
            .await
            .unindexed,
        "an unembedded claim the search does not admit does not make it incomplete"
    );
    let admitting_sensitive = filters(64).with_classifications(BTreeSet::from([
        MemoryClassification::Unclassified,
        MemoryClassification::Sensitive,
    ]));
    assert!(
        run(store, scope, &toward_query(admitting_sensitive))
            .await
            .unindexed,
        "an unembedded claim the search admits does"
    );

    // The same for each of the other three filters, and for a retracted
    // claim. An unembedded claim one filter excludes must not make the answer
    // incomplete: for a claim the reader may not read, the flag would say
    // that the claim exists, and a retracted claim is never embedded, so it
    // would hold the flag up for good. A search that does admit the claim is
    // incomplete until the claim is embedded.
    let scope = &space(&scopes, "sem-unindexed-trust");
    append_embedded(store, scope, "trust-embedded", QUERY).await;
    let disputed = append(store, scope, Stated::new("trust-unembedded")).await;
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &disputed.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies");
    assert!(
        !run(store, scope, &toward_query(filters(64)))
            .await
            .unindexed,
        "an unembedded claim of a trust state the search does not admit"
    );
    assert!(
        run(
            store,
            scope,
            &toward_query(filters(64).with_trust(BTreeSet::from([
                ClaimTrustStatus::Proposed,
                ClaimTrustStatus::Disputed,
            ])))
        )
        .await
        .unindexed
    );

    let scope = &space(&scopes, "sem-unindexed-retracted");
    append_embedded(store, scope, "retracted-embedded", QUERY).await;
    let retracted = append(store, scope, Stated::new("retracted-unembedded")).await;
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &retracted.claim_id,
                "retract",
                ClaimTrustStatus::Retracted,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the retraction applies");
    assert!(
        !run(store, scope, &toward_query(filters(64)))
            .await
            .unindexed,
        "a retracted claim is never embedded and never makes a search incomplete"
    );

    let scope = &space(&scopes, "sem-unindexed-confidence");
    append_embedded(store, scope, "confidence-embedded", QUERY).await;
    append(
        store,
        scope,
        Stated {
            confidence_bps: 1_000,
            ..Stated::new("confidence-unembedded")
        },
    )
    .await;
    assert!(
        !run(
            store,
            scope,
            &toward_query(filters(64).with_min_confidence_bps(4_000))
        )
        .await
        .unindexed,
        "an unembedded claim below the search's confidence floor"
    );
    assert!(
        run(store, scope, &toward_query(filters(64)))
            .await
            .unindexed
    );

    let scope = &space(&scopes, "sem-unindexed-capability");
    append_embedded(store, scope, "capability-embedded", QUERY).await;
    append(
        store,
        scope,
        Stated {
            required: &["cap-guarded"],
            ..Stated::new("capability-unembedded")
        },
    )
    .await;
    assert!(
        !run(store, scope, &toward_query(filters(64)))
            .await
            .unindexed,
        "an unembedded claim the reader may not read must not show through the flag"
    );
    assert!(
        run(
            store,
            scope,
            &toward_query(filters(64).with_reader(BTreeSet::from([capability("cap-guarded")])))
        )
        .await
        .unindexed
    );

    // --- Storing an embedding. ---------------------------------------------
    let scope = &space(&scopes, "sem-storing");
    let stored = append(store, scope, Stated::new("stored")).await;
    let record_before = store
        .get(scope, &stored.claim_id)
        .await
        .expect("the claim reads");
    embed(store, scope, &stored, QUERY).await;
    embed(store, scope, &stored, QUERY).await;
    assert_eq!(
        hit_ids(&run(store, scope, &toward_query(filters(64))).await),
        vec![stored.claim_id.clone()],
        "storing the same embedding twice leaves one"
    );
    let record_after = store
        .get(scope, &stored.claim_id)
        .await
        .expect("the claim reads");
    assert_eq!(
        record_before, record_after,
        "storing an embedding changes nothing of the claim's record"
    );
    assert_eq!(
        record_after.expect("the claim is there").embedding,
        None,
        "the record's own embedding note is not written"
    );
    assert!(store
        .store_claim_embedding(scope, &stored.claim_id, &embedding(2, QUERY))
        .await
        .expect("the embedding stores"));
    assert!(
        run(store, scope, &toward_query(filters(64)))
            .await
            .hits
            .is_empty(),
        "a claim has one embedding: the new reference replaced the old"
    );
    assert_eq!(
        hit_ids(
            &run(
                store,
                scope,
                &ClaimSemanticSearch::new(filters(64), embedding(2, QUERY))
            )
            .await
        ),
        vec![stored.claim_id.clone()]
    );

    let absent = ClaimId::derive_appended(
        scope,
        &ClaimOperationId::derive_append(scope, "never-appended").expect("the id derives"),
    )
    .expect("the claim id derives");
    assert!(
        !store
            .store_claim_embedding(scope, &absent, &embedding(1, QUERY))
            .await
            .expect("an absent claim is an answer, not an error"),
        "nothing is stored for a claim the scope does not hold"
    );
    assert!(
        !store
            .store_claim_embedding(&scopes.foreign, &stored.claim_id, &embedding(1, QUERY))
            .await
            .expect("another scope's claim is an answer, not an error"),
        "nothing is stored through a scope that does not hold the claim"
    );
    let malformed = ClaimEmbedding {
        reference: reference(1),
        vector: vec![1.0, 0.0],
    };
    assert_eq!(
        store
            .store_claim_embedding(scope, &stored.claim_id, &malformed)
            .await
            .expect_err("a vector that does not match its reference is refused")
            .code(),
        "claim-embedding-invalid"
    );
    assert_eq!(
        store
            .search_semantic(scope, &ClaimSemanticSearch::new(filters(8), malformed))
            .await
            .expect_err("a malformed query embedding is refused")
            .code(),
        "claim-embedding-invalid"
    );

    // --- Erasure reaches the embedding. --------------------------------------
    let scope = &space(&scopes, "sem-erasure");
    let tombstoned = append_embedded(store, scope, "erase-tombstone", QUERY).await;
    let deleted = append_embedded(store, scope, "erase-delete", QUERY).await;
    assert_eq!(
        run(store, scope, &toward_query(filters(64)))
            .await
            .hits
            .len(),
        2
    );
    store
        .tombstone(
            scope,
            &tombstone_request(
                scope,
                &tombstoned.claim_id,
                "erase",
                ClaimTombstoneReason::Withdrawn,
            ),
        )
        .await
        .expect("the tombstone applies");
    store
        .delete(
            scope,
            &delete_request(scope, &deleted.claim_id, "erase", None),
        )
        .await
        .expect("the delete applies");
    let erased = run(store, scope, &toward_query(filters(64))).await;
    assert!(erased.hits.is_empty(), "an erased claim is never found");
    assert!(
        !erased.unindexed,
        "an erased claim is not a claim that still wants an embedding"
    );
    assert!(
        page(store, scope, 1, UNCLASSIFIED, None, 64)
            .await
            .claims
            .is_empty(),
        "an erased claim is never listed for embedding"
    );
    for gone in [&tombstoned, &deleted] {
        assert!(
            !store
                .store_claim_embedding(scope, &gone.claim_id, &embedding(1, QUERY))
                .await
                .expect("an erased claim is an answer, not an error"),
            "an erased claim cannot be given an embedding again"
        );
    }
    assert!(run(store, scope, &toward_query(filters(64)))
        .await
        .hits
        .is_empty());

    // --- The page of unembedded claims. --------------------------------------
    let scope = &space(&scopes, "sem-page");
    let mut unembedded = Vec::new();
    for index in 0..5 {
        unembedded.push(append(store, scope, Stated::new(&format!("page-{index}"))).await);
    }
    let in_order = ascending(&unembedded.iter().collect::<Vec<_>>());
    let first = page(store, scope, 1, UNCLASSIFIED, None, 2).await;
    assert_eq!(page_ids(&first), in_order[..2]);
    assert_eq!(first.undecodable, 0);
    assert_eq!(first.next.as_ref(), Some(&in_order[1]));
    let second = page(store, scope, 1, UNCLASSIFIED, first.next.as_ref(), 2).await;
    assert_eq!(page_ids(&second), in_order[2..4]);
    let third = page(store, scope, 1, UNCLASSIFIED, second.next.as_ref(), 2).await;
    assert_eq!(page_ids(&third), in_order[4..]);
    assert_eq!(third.next, None, "a page that was not full ends the walk");

    // What is never listed: a retracted claim, a claim outside the
    // classifications given, and a claim already embedded under the
    // reference. A claim embedded under another reference is listed.
    let retracted = append(store, scope, Stated::new("page-retracted")).await;
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &retracted.claim_id,
                "retract",
                ClaimTrustStatus::Retracted,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the retraction applies");
    let sensitive = append(
        store,
        scope,
        Stated {
            classification: MemoryClassification::Sensitive,
            ..Stated::new("page-sensitive")
        },
    )
    .await;
    embed(store, scope, &unembedded[0], QUERY).await;
    assert!(store
        .store_claim_embedding(scope, &unembedded[1].claim_id, &embedding(2, QUERY))
        .await
        .expect("the embedding stores"));
    let listed = page_ids(&page(store, scope, 1, UNCLASSIFIED, None, 64).await);
    assert_eq!(
        listed,
        ascending(&unembedded[1..].iter().collect::<Vec<_>>())
    );
    assert!(!listed.contains(&retracted.claim_id));
    assert!(!listed.contains(&sensitive.claim_id));
    assert!(
        page_ids(
            &page(
                store,
                scope,
                1,
                &[MemoryClassification::Sensitive],
                None,
                64
            )
            .await
        ) == vec![sensitive.claim_id.clone()],
        "the classifications given decide what is listed"
    );
    assert!(
        page(store, scope, 1, &[], None, 64).await.claims.is_empty(),
        "the empty set lists nothing"
    );

    // The clamp: at most the crate's page bound, at least one.
    let scope = &space(&scopes, "sem-page-clamp");
    for index in 0..=CLAIM_EMBEDDING_PAGE_MAX_CLAIMS {
        append(store, scope, Stated::new(&format!("clamp-{index}"))).await;
    }
    let widest = page(store, scope, 1, UNCLASSIFIED, None, 1_000).await;
    assert_eq!(widest.claims.len(), CLAIM_EMBEDDING_PAGE_MAX_CLAIMS);
    assert!(widest.next.is_some());
    let rest = page(store, scope, 1, UNCLASSIFIED, widest.next.as_ref(), 1_000).await;
    assert_eq!(rest.claims.len(), 1);
    assert_eq!(rest.next, None);
    let narrowest = page(store, scope, 1, UNCLASSIFIED, None, 0).await;
    assert_eq!(narrowest.claims.len(), 1, "a limit of zero reads one claim");
}

/// A deterministic embedder that needs no model, for tests of everything
/// that holds an embedder: the retriever, the append executor and the
/// embedding pass.
///
/// It knows a vocabulary of *concepts*, each a set of words that mean the
/// same thing. A text's vector has one component per concept, counting the
/// text's words that belong to it, and a last component that is 1 when the
/// text names no concept at all. Two texts that name the same concept in
/// different words therefore point the same way, which is what lets a test
/// find a claim by a query that shares no word with it; texts that name
/// different concepts are unrelated.
///
/// Clones share one record of the texts they were asked to embed.
#[derive(Debug, Clone)]
pub struct ConceptEmbedder {
    concepts: Arc<Vec<BTreeSet<String>>>,
    version: u64,
    failing: BTreeSet<String>,
    short: BTreeSet<String>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl ConceptEmbedder {
    /// The model name every concept embedder reports.
    pub const MODEL: &'static str = "concept-embedder";

    /// An embedder over `concepts`, at pipeline version 1. Each concept is
    /// the words that mean it; a word is matched as
    /// [`tokenize_search_text`] splits a text.
    #[must_use]
    pub fn new(concepts: &[&[&str]]) -> Self {
        Self {
            concepts: Arc::new(
                concepts
                    .iter()
                    .map(|words| words.iter().map(|word| word.to_lowercase()).collect())
                    .collect(),
            ),
            version: 1,
            failing: BTreeSet::new(),
            short: BTreeSet::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The same embedder at another pipeline version: another reference, as
    /// a deployment that changed its model or its pipeline has.
    #[must_use]
    pub const fn with_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }

    /// Makes `embed` fail for any text that holds `word`, as a provider that
    /// rejects one input does.
    #[must_use]
    pub fn failing_on(mut self, word: &str) -> Self {
        self.failing.insert(word.to_lowercase());
        self
    }

    /// Makes `embed` answer one component too few for any text that holds
    /// `word`, as a provider that answers the wrong dimension count does.
    #[must_use]
    pub fn answering_short_on(mut self, word: &str) -> Self {
        self.short.insert(word.to_lowercase());
        self
    }

    /// Every text this embedder, or a clone of it, was asked to embed, in
    /// order, whether or not the call succeeded.
    ///
    /// # Panics
    ///
    /// Panics if a thread panicked while recording a call.
    #[must_use]
    pub fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .expect("the call record lock is never poisoned")
            .clone()
    }
}

impl AgentMemoryEmbedder for ConceptEmbedder {
    fn embedding_ref(&self) -> MemoryEmbeddingRef {
        MemoryEmbeddingRef {
            model: Self::MODEL.to_string(),
            dimensions: u32::try_from(self.concepts.len() + 1).unwrap_or(u32::MAX),
            version: AgentRevisionNumber::new(self.version),
        }
    }

    fn embed<'a>(&'a self, text: &'a str) -> MemoryFuture<'a, Vec<f32>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("the call record lock is never poisoned")
                .push(text.to_string());
            let words = tokenize_search_text(text);
            if words.iter().any(|word| self.failing.contains(word)) {
                return Err(MemoryError::Backend {
                    backend: Self::MODEL.to_string(),
                    message: "the model refused this input".to_string(),
                });
            }
            let mut vector: Vec<f32> = self
                .concepts
                .iter()
                .map(|concept| {
                    let named = words.iter().filter(|word| concept.contains(*word)).count();
                    // A text names a concept a handful of times at most.
                    f32::from(u16::try_from(named).unwrap_or(u16::MAX))
                })
                .collect();
            let names_none = vector.iter().all(|component| *component == 0.0);
            vector.push(if names_none { 1.0 } else { 0.0 });
            if words.iter().any(|word| self.short.contains(word)) {
                vector.pop();
            }
            Ok(vector)
        })
    }
}
