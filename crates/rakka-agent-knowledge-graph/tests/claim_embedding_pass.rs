//! The embedding pass: what a deployment calls to give a space's claims the
//! embeddings a meaning-based search ranks by (issue #140).
//!
//! No loop in Rakka calls the pass. These tests hold the properties a
//! deployment's own loop rests on: the pass embeds exactly what is missing,
//! resumes from its cursor, is idempotent, re-embeds after a model change,
//! is not stopped by a claim the model refuses, and never sends a claim its
//! caller did not allow.

mod support;

use std::collections::BTreeSet;

use rakka_agent::{AgentMemoryEmbedder, MemoryClassification};
use rakka_agent_knowledge_graph::conformance::ConformanceScopes;
use rakka_agent_knowledge_graph::{
    embed_claims, Claim, ClaimEmbeddingPassPage, ClaimId, InMemoryKnowledgeGraphStore,
    KnowledgeGraphStore, KnowledgeSpaceScope,
};

use support::{
    append_stated, embedder, meaning_of, retract, unclassified, CoreOnlyStore,
    MisbehavingSemanticStore,
};

/// Walks the pass to its end, `limit` claims at a time, summing its pages.
async fn walk(
    store: &dyn KnowledgeGraphStore,
    embedder: &dyn AgentMemoryEmbedder,
    classifications: &BTreeSet<MemoryClassification>,
    scope: &KnowledgeSpaceScope,
    limit: usize,
) -> ClaimEmbeddingPassPage {
    let mut total = ClaimEmbeddingPassPage::default();
    let mut after: Option<ClaimId> = None;
    loop {
        let page = embed_claims(
            store,
            embedder,
            classifications,
            scope,
            after.as_ref(),
            limit,
        )
        .await
        .expect("the pass answers");
        total.embedded += page.embedded;
        total.failed += page.failed;
        total.undecodable += page.undecodable;
        match page.next {
            Some(next) => after = Some(next),
            None => return total,
        }
    }
}

async fn five_claims(store: &dyn KnowledgeGraphStore, scope: &KnowledgeSpaceScope) -> Vec<Claim> {
    let mut claims = Vec::new();
    for (index, text) in [
        "the car is at the depot",
        "the boat left at noon",
        "nothing of note",
        "a second car arrived",
        "the ship is late",
    ]
    .into_iter()
    .enumerate()
    {
        claims.push(
            append_stated(
                store,
                scope,
                &format!("claim-{index}"),
                text,
                MemoryClassification::Unclassified,
            )
            .await,
        );
    }
    claims
}

#[tokio::test]
async fn the_pass_embeds_what_is_missing_and_resumes_from_its_cursor() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scopes = ConformanceScopes::unique("pass-resume");
    let scope = &scopes.primary;
    let embedder = embedder();
    let claims = five_claims(&store, scope).await;

    let first = embed_claims(&store, &embedder, &unclassified(), scope, None, 2)
        .await
        .expect("the pass answers");
    assert_eq!((first.embedded, first.failed, first.undecodable), (2, 0, 0));
    let second = embed_claims(
        &store,
        &embedder,
        &unclassified(),
        scope,
        first.next.as_ref(),
        2,
    )
    .await
    .expect("the pass answers");
    assert_eq!(second.embedded, 2);
    let third = embed_claims(
        &store,
        &embedder,
        &unclassified(),
        scope,
        second.next.as_ref(),
        2,
    )
    .await
    .expect("the pass answers");
    assert_eq!(third.embedded, 1);
    assert_eq!(third.next, None, "a page that was not full ends the pass");

    // A query that shares no word with the claim finds it by meaning.
    let report = store
        .search_semantic(scope, &meaning_of(&embedder, "automobile", 2).await)
        .await
        .expect("the search answers");
    assert!(!report.unindexed);
    let mut found: Vec<&ClaimId> = report.hits.iter().map(|hit| &hit.claim.claim_id).collect();
    found.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut cars = vec![&claims[0].claim_id, &claims[3].claim_id];
    cars.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(found, cars);
    assert!(report.hits.iter().all(|hit| hit.relevance_bps == 10_000));
}

#[tokio::test]
async fn the_pass_is_idempotent() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scopes = ConformanceScopes::unique("pass-idempotent");
    let scope = &scopes.primary;
    let embedder = embedder();
    five_claims(&store, scope).await;

    assert_eq!(
        walk(&store, &embedder, &unclassified(), scope, 64)
            .await
            .embedded,
        5
    );
    let calls = embedder.calls().len();
    assert_eq!(calls, 5);
    let again = walk(&store, &embedder, &unclassified(), scope, 64).await;
    assert_eq!(again, ClaimEmbeddingPassPage::default());
    assert_eq!(
        embedder.calls().len(),
        calls,
        "a claim that is embedded is not sent to the model again"
    );
}

#[tokio::test]
async fn a_changed_reference_embeds_every_claim_again() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scopes = ConformanceScopes::unique("pass-reference");
    let scope = &scopes.primary;
    let first = embedder();
    five_claims(&store, scope).await;
    walk(&store, &first, &unclassified(), scope, 64).await;

    let second = embedder().with_version(2);
    let stale = store
        .search_semantic(scope, &meaning_of(&second, "automobile", 8).await)
        .await
        .expect("the search answers");
    assert!(stale.hits.is_empty() && stale.unindexed);
    assert_eq!(
        walk(&store, &second, &unclassified(), scope, 2)
            .await
            .embedded,
        5,
        "every claim is unembedded under the new reference"
    );
    let fresh = store
        .search_semantic(scope, &meaning_of(&second, "automobile", 2).await)
        .await
        .expect("the search answers");
    assert_eq!(fresh.hits.len(), 2);
    assert!(!fresh.unindexed);
}

#[tokio::test]
async fn a_claim_the_model_refuses_does_not_stop_the_claims_after_it() {
    for refusing in [
        embedder().failing_on("boat"),
        embedder().answering_short_on("boat"),
    ] {
        let store = InMemoryKnowledgeGraphStore::new();
        let scopes = ConformanceScopes::unique("pass-refused");
        let scope = &scopes.primary;
        let claims = five_claims(&store, scope).await;
        let refused = &claims[1];

        // One claim at a time, so the refused claim is alone on its page and
        // every claim after it depends on the cursor moving past it.
        let total = walk(&store, &refusing, &unclassified(), scope, 1).await;
        assert_eq!((total.embedded, total.failed), (4, 1));

        let left = store
            .unembedded_claims(scope, &refusing.embedding_ref(), &unclassified(), None, 64)
            .await
            .expect("the page answers");
        assert_eq!(
            left.claims
                .iter()
                .map(|claim| &claim.claim_id)
                .collect::<Vec<_>>(),
            vec![&refused.claim_id],
            "the refused claim is still owed an embedding"
        );
        assert!(!store.has_embedding(scope, &refused.claim_id));
    }
}

#[tokio::test]
async fn the_pass_sends_only_what_its_set_allows() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scopes = ConformanceScopes::unique("pass-set");
    let scope = &scopes.primary;
    let embedder = embedder();
    let plain = append_stated(
        &store,
        scope,
        "plain",
        "the car is here",
        MemoryClassification::Unclassified,
    )
    .await;
    let sensitive = append_stated(
        &store,
        scope,
        "sensitive",
        "the sensitive car",
        MemoryClassification::Sensitive,
    )
    .await;
    let redacted = append_stated(
        &store,
        scope,
        "redacted",
        "the redacted car",
        MemoryClassification::Redacted,
    )
    .await;
    let retracted = append_stated(
        &store,
        scope,
        "retracted",
        "the retracted car",
        MemoryClassification::Unclassified,
    )
    .await;
    retract(&store, scope, &retracted).await;

    // The caller names `Redacted` and the pass still never sends it.
    let allowed = BTreeSet::from([
        MemoryClassification::Unclassified,
        MemoryClassification::Redacted,
    ]);
    let total = walk(&store, &embedder, &allowed, scope, 64).await;
    assert_eq!((total.embedded, total.failed), (1, 0));
    assert_eq!(
        embedder.calls(),
        vec!["fleetnote reports the car is here".to_string()],
        "only the allowed, live, unretracted claim's text reaches the model"
    );
    assert!(store.has_embedding(scope, &plain.claim_id));
    for withheld in [&sensitive, &redacted, &retracted] {
        assert!(!store.has_embedding(scope, &withheld.claim_id));
    }

    // The empty set sends nothing at all.
    let nothing = walk(&store, &embedder, &BTreeSet::new(), scope, 64).await;
    assert_eq!(nothing, ClaimEmbeddingPassPage::default());
    assert_eq!(embedder.calls().len(), 1);
}

#[tokio::test]
async fn a_store_without_the_capability_ends_the_pass_with_its_refusal() {
    let store = CoreOnlyStore::default();
    let scopes = ConformanceScopes::unique("pass-unsupported");
    let scope = &scopes.primary;
    let embedder = embedder();
    five_claims(&store, scope).await;

    let error = embed_claims(&store, &embedder, &unclassified(), scope, None, 8)
        .await
        .expect_err("a store that cannot keep embeddings refuses the pass");
    assert_eq!(error.code(), "claim-capability-unsupported");
    assert!(
        embedder.calls().is_empty(),
        "no claim was sent to the model"
    );
}

/// What a store lists is a nomination. A backend whose listing has drifted
/// from its records, or one written elsewhere that ignores the set it was
/// given, must not get a claim's text sent to a model: the pass checks each
/// record itself, last, before the text leaves.
#[tokio::test]
async fn the_pass_checks_the_record_whatever_the_store_listed() {
    use rakka_agent_knowledge_graph::conformance::conformance_resolver;
    use rakka_agent_knowledge_graph::{
        ClaimOperationId, ClaimTombstoneReason, ClaimTombstoneRequest,
    };
    use rakka_agent_workflow::AgentTimestampMillis;

    let store = MisbehavingSemanticStore::default();
    let scopes = ConformanceScopes::unique("pass-drifted-listing");
    let scope = &scopes.primary;
    let embedder = embedder();
    let plain = append_stated(
        &store,
        scope,
        "plain",
        "the car is here",
        MemoryClassification::Unclassified,
    )
    .await;
    for (discriminator, classification) in [
        ("sensitive", MemoryClassification::Sensitive),
        ("redacted", MemoryClassification::Redacted),
    ] {
        append_stated(
            &store,
            scope,
            discriminator,
            "a withheld car",
            classification,
        )
        .await;
    }
    let retracted = append_stated(
        &store,
        scope,
        "retracted",
        "a retracted car",
        MemoryClassification::Unclassified,
    )
    .await;
    retract(&store, scope, &retracted).await;
    let withdrawn = append_stated(
        &store,
        scope,
        "withdrawn",
        "a withdrawn car",
        MemoryClassification::Unclassified,
    )
    .await;
    store
        .tombstone(
            scope,
            &ClaimTombstoneRequest {
                claim_id: withdrawn.claim_id.clone(),
                operation_id: ClaimOperationId::derive_tombstone(scope, &withdrawn.claim_id, "t")
                    .expect("the operation id derives"),
                reason: ClaimTombstoneReason::Withdrawn,
                actor: conformance_resolver(),
                tombstoned_at: AgentTimestampMillis::new(20),
            },
        )
        .await
        .expect("the tombstone applies");

    // The store lists all five. The caller even names `Redacted`.
    let allowed = BTreeSet::from([
        MemoryClassification::Unclassified,
        MemoryClassification::Redacted,
    ]);
    let page = embed_claims(&store, &embedder, &allowed, scope, None, 64)
        .await
        .expect("the pass answers");
    assert_eq!((page.embedded, page.failed), (1, 0));
    assert_eq!(
        embedder.calls(),
        vec!["fleetnote reports the car is here".to_string()],
        "only the claim its record allows reached the model"
    );
    assert!(store.inner.has_embedding(scope, &plain.claim_id));
}
