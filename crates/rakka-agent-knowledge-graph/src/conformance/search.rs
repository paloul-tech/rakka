//! Conformance clause for keyword search
//! ([specification 13.6 and 16](../../../../docs/plans/rakka-agent/spec.md):
//! tenant, space, classification, trust and per-claim read capabilities are
//! enforced before results are ranked).
//!
//! The clause runs only against a backend that reports
//! [`KnowledgeGraphCapability::KeywordSearch`](crate::store::KnowledgeGraphCapability::KeywordSearch).
//! Every backend that does must answer the same hits in the same order,
//! because all of them use one admission rule, one word rule and one scorer.

use std::collections::BTreeSet;

use rakka_agent::{AgentCapabilityId, AgentId, KnowledgeSpaceId, MemoryClassification};
use rakka_agent_workflow::AgentTimestampMillis;

use super::erasure::tombstone_request;
use super::{transition_request, ConformanceScopes};
use crate::claim::{
    Claim, ClaimAccess, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimProvenance, ClaimTrustStatus,
};
use crate::promotion::ClaimPromotionPolicy;
use crate::scope::KnowledgeSpaceScope;
use crate::search::{
    claim_search_query_tokens, claim_search_relevance_bps, ClaimSearch, ClaimSearchReport,
};
use crate::store::KnowledgeGraphStore;
use crate::tombstone::ClaimTombstoneReason;

/// What one searchable conformance claim says and who may read it.
struct Worded<'a> {
    /// The append discriminator, unique within the clause.
    discriminator: &'a str,
    /// The text of the claim's inline value: the words a search can match.
    text: &'a str,
    confidence_bps: u16,
    classification: MemoryClassification,
    required: &'a [&'a str],
}

impl<'a> Worded<'a> {
    fn new(discriminator: &'a str, text: &'a str) -> Self {
        Self {
            discriminator,
            text,
            confidence_bps: 5_000,
            classification: MemoryClassification::Unclassified,
            required: &[],
        }
    }
}

fn capability(id: &str) -> AgentCapabilityId {
    AgentCapabilityId::new(id).expect("the capability id is valid")
}

/// Builds the claim. Its subject and predicate are words no query in this
/// clause uses, so a query matches a claim through its value alone.
fn worded_claim(scope: &KnowledgeSpaceScope, worded: &Worded<'_>) -> Claim {
    let operation_id = ClaimOperationId::derive_append(scope, worded.discriminator)
        .expect("the operation id derives");
    Claim::new(
        scope,
        operation_id,
        ClaimNodeId::new("subjectnode").expect("the node id is valid"),
        ClaimPredicate::new("predicateword").expect("the predicate is valid"),
        ClaimObject::Value(
            rakka_agent::AgentTaskContent::inline(serde_json::Value::String(
                worded.text.to_string(),
            ))
            .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        worded.confidence_bps,
        worded.classification,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
    .with_access(ClaimAccess {
        required_read_capabilities: worded.required.iter().map(|id| capability(id)).collect(),
    })
    .expect("the access set is bounded")
}

async fn append_worded(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    worded: Worded<'_>,
) -> Claim {
    let claim = worded_claim(scope, &worded);
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends")
}

/// A search admitting what a fresh conformance claim is: `Proposed` and
/// `Unclassified`, read by a reader holding nothing.
fn search(text: &str, limit: usize) -> ClaimSearch {
    ClaimSearch::new(text, limit)
        .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
        .with_classifications(BTreeSet::from([MemoryClassification::Unclassified]))
}

async fn run(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    search: &ClaimSearch,
) -> ClaimSearchReport {
    let report = store
        .search(scope, search)
        .await
        .expect("the search answers");
    assert!(
        !report.unindexed,
        "every claim appended through the store is covered by its search"
    );
    report
}

fn hit_ids(report: &ClaimSearchReport) -> Vec<ClaimId> {
    report
        .hits
        .iter()
        .map(|hit| hit.claim.claim_id.clone())
        .collect()
}

/// Ascending claim ids: the order ties are broken in.
fn ascending(claims: &[&Claim]) -> Vec<ClaimId> {
    let mut ids: Vec<ClaimId> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    ids
}

/// One excluded claim that would rank first, with exactly `limit` admitted
/// claims behind it: the excluded claim must take no slot.
async fn excluded_takes_no_slot(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    word: &str,
    excluded: &Claim,
    search: ClaimSearch,
) {
    let mut admitted = Vec::new();
    for index in 0..search.limit() {
        admitted.push(
            append_worded(
                store,
                scope,
                Worded::new(&format!("{word}-admitted-{index}"), word),
            )
            .await,
        );
    }
    let report = run(store, scope, &search).await;
    assert_eq!(
        hit_ids(&report),
        ascending(&admitted.iter().collect::<Vec<_>>()),
        "the excluded claim took a slot an admitted claim was owed ({word})"
    );
    assert!(
        !hit_ids(&report).contains(&excluded.claim_id),
        "an excluded claim is never a hit ({word})"
    );
}

/// Keyword search: admission precedes ranking, ranking is the shared scorer's,
/// erasure and scope are honored, and nothing bounds how far the search looks.
pub async fn keyword_search(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;

    // --- Ranking: descending relevance, ties by ascending claim id. --------
    let three = append_worded(store, scope, Worded::new("rank-3", "alpha beta gamma")).await;
    let two_a = append_worded(store, scope, Worded::new("rank-2a", "alpha beta")).await;
    let two_b = append_worded(store, scope, Worded::new("rank-2b", "beta alpha")).await;
    let one = append_worded(store, scope, Worded::new("rank-1", "alpha")).await;
    let _none = append_worded(store, scope, Worded::new("rank-0", "omega")).await;

    let ranked = run(store, scope, &search("alpha beta gamma delta", 64)).await;
    let mut expected = vec![three.claim_id.clone()];
    expected.extend(ascending(&[&two_a, &two_b]));
    expected.push(one.claim_id.clone());
    assert_eq!(hit_ids(&ranked), expected);
    assert_eq!(
        ranked
            .hits
            .iter()
            .map(|hit| hit.relevance_bps)
            .collect::<Vec<_>>(),
        vec![7_500, 5_000, 5_000, 2_500]
    );
    assert!(
        ranked.hits.iter().all(|hit| hit.relevance_bps > 0),
        "a claim sharing no word with the query is not a hit"
    );
    assert_eq!(
        ranked.hits[0].claim, three,
        "a hit is the stored claim, whole"
    );

    // The limit is honored, and what it keeps is the head of the ranking.
    let limited = run(store, scope, &search("alpha beta gamma delta", 2)).await;
    assert_eq!(hit_ids(&limited), expected[..2].to_vec());

    // --- Admission precedes ranking. ---------------------------------------
    // Each excluded claim holds both query words and would rank first. Every
    // case has words of its own, so that one case's excluded claim, which a
    // later case's search may well admit, shares no word with that search.
    let disputed =
        append_worded(store, scope, Worded::new("trust-x", "trustword trustbonus")).await;
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
    excluded_takes_no_slot(
        store,
        scope,
        "trustword",
        &disputed,
        search("trustword trustbonus", 3),
    )
    .await;
    // The same search that admits `Disputed` finds it first.
    let admitting = search("trustword trustbonus", 3).with_trust(BTreeSet::from([
        ClaimTrustStatus::Proposed,
        ClaimTrustStatus::Disputed,
    ]));
    assert_eq!(
        hit_ids(&run(store, scope, &admitting).await)[0],
        disputed.claim_id
    );

    let sensitive = append_worded(
        store,
        scope,
        Worded {
            classification: MemoryClassification::Sensitive,
            ..Worded::new("class-x", "classword classbonus")
        },
    )
    .await;
    excluded_takes_no_slot(
        store,
        scope,
        "classword",
        &sensitive,
        search("classword classbonus", 3),
    )
    .await;

    let unsure = append_worded(
        store,
        scope,
        Worded {
            confidence_bps: 1_000,
            ..Worded::new("conf-x", "confword confbonus")
        },
    )
    .await;
    excluded_takes_no_slot(
        store,
        scope,
        "confword",
        &unsure,
        search("confword confbonus", 3).with_min_confidence_bps(2_000),
    )
    .await;

    let guarded = append_worded(
        store,
        scope,
        Worded {
            required: &["cap-a"],
            ..Worded::new("cap-x", "capword capbonus")
        },
    )
    .await;
    excluded_takes_no_slot(
        store,
        scope,
        "capword",
        &guarded,
        search("capword capbonus", 3),
    )
    .await;

    // --- Per-claim read capabilities. --------------------------------------
    // A claim requiring nothing was found above by a reader holding nothing.
    let two_caps = append_worded(
        store,
        scope,
        Worded {
            required: &["cap-a", "cap-b"],
            ..Worded::new("caps-2", "twocapword")
        },
    )
    .await;
    let holding = |ids: &[&str]| {
        search("twocapword", 8).with_reader(ids.iter().map(|id| capability(id)).collect())
    };
    assert!(run(store, scope, &holding(&[])).await.hits.is_empty());
    assert!(
        run(store, scope, &holding(&["cap-a"]))
            .await
            .hits
            .is_empty(),
        "a reader holding one of two required capabilities does not read the claim"
    );
    assert_eq!(
        hit_ids(&run(store, scope, &holding(&["cap-a", "cap-b"])).await),
        vec![two_caps.claim_id.clone()]
    );
    assert_eq!(
        hit_ids(&run(store, scope, &holding(&["cap-a", "cap-b", "cap-c"])).await),
        vec![two_caps.claim_id.clone()],
        "holding more than a claim requires still reads it"
    );

    // --- An empty admitted set admits nothing. -----------------------------
    assert!(run(store, scope, &ClaimSearch::new("alpha", 8))
        .await
        .hits
        .is_empty());

    // --- Erasure. -----------------------------------------------------------
    let every_trust = |text: &str| {
        search(text, 8).with_trust(ClaimTrustStatus::ALL.into_iter().collect::<BTreeSet<_>>())
    };
    let withdrawn = append_worded(store, scope, Worded::new("gone", "withdrawnword")).await;
    assert_eq!(
        hit_ids(&run(store, scope, &every_trust("withdrawnword")).await),
        vec![withdrawn.claim_id.clone()]
    );
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
    assert!(
        run(store, scope, &every_trust("withdrawnword"))
            .await
            .hits
            .is_empty(),
        "a search for the words a withdrawn statement held finds nothing"
    );
    assert!(
        run(
            store,
            scope,
            &every_trust(crate::tombstone::CLAIM_TOMBSTONE_MARKER)
        )
        .await
        .hits
        .is_empty(),
        "a tombstoned stub is not a hit, even for its own markers"
    );

    // --- Scope. -------------------------------------------------------------
    let other_space = KnowledgeSpaceScope::new(
        scope.tenant().clone(),
        KnowledgeSpaceId::new("space-other").expect("the space id is valid"),
    )
    .expect("the scope is valid");
    for elsewhere in [&scopes.foreign, &other_space] {
        let report = store
            .search(elsewhere, &search("alpha beta gamma", 64))
            .await
            .expect("the search answers");
        assert!(
            report.hits.is_empty() && !report.unindexed,
            "another tenant and another space answer as an empty space does"
        );
    }

    // --- No scan ceiling. ---------------------------------------------------
    for index in 0..300 {
        append_worded(
            store,
            scope,
            Worded::new(&format!("filler-{index}"), "fillerword"),
        )
        .await;
    }
    let needle = append_worded(store, scope, Worded::new("needle", "needleword")).await;
    assert_eq!(
        hit_ids(&run(store, scope, &search("needleword", 8)).await),
        vec![needle.claim_id.clone()],
        "a matching claim is found however many others the space holds"
    );

    // --- A query with no words. --------------------------------------------
    for wordless in ["  ...  ", "", "-_-"] {
        assert!(
            run(store, scope, &search(wordless, 8))
                .await
                .hits
                .is_empty(),
            "a query with no words finds nothing and is not an error"
        );
    }

    // --- Text outside ASCII. -----------------------------------------------
    let wide = append_worded(store, scope, Worded::new("wide", "Straße ÉCOLE 東京")).await;
    let found = run(store, scope, &search("école", 8)).await;
    assert_eq!(hit_ids(&found), vec![wide.claim_id.clone()]);
    assert_eq!(
        found.hits[0].relevance_bps,
        claim_search_relevance_bps(&claim_search_query_tokens("école"), &wide)
    );
    assert_eq!(found.hits[0].relevance_bps, 10_000);
    let partial = run(store, scope, &search("ÉCOLE 東京 absent", 8)).await;
    assert_eq!(hit_ids(&partial), vec![wide.claim_id.clone()]);
    assert_eq!(partial.hits[0].relevance_bps, 6_666);
}
