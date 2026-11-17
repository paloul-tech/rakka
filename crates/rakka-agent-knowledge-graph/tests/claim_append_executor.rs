//! The graph-backed claim-append executor, end to end against a real store
//! ([specification 8.5 and 13.4](../../docs/plans/rakka-agent/spec.md),
//! scenario 33's bridge half).
//!
//! `rakka-agent` commits the `ClaimAppend` effect and stamps its provenance;
//! this executor is the only thing that turns that effect into a durable
//! claim, so the properties the slice rests on are properties of *this* code:
//! the store operation derives from the intent's external idempotency key
//! (stable across a generation's attempts, fresh for a re-decided
//! generation), the stamped provenance reaches the record intact, and a
//! definitive store refusal comes back as a finding rather than a retryable
//! error.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;

use rakka_agent::{
    AgentClaimAppendExecutor, AgentClaimAppendFinding, AgentClaimAppendProvenance,
    AgentClaimAppendRequest, AgentClaimObjectRequest, AgentDelegationId, AgentEffectSpec,
    AgentGoalId, AgentId, AgentRevisionNumber, AgentRunEffect, AgentRunEffectRequest, AgentRunId,
    AgentRunScope, AgentTaskContent, AgentTaskId, KnowledgeSpaceId, MemoryClassification,
    AGENT_CLAIM_APPEND_DEFAULT_MAX_ATTEMPTS,
};
use rakka_agent_knowledge_graph::conformance::ConformanceScopes;
use rakka_agent_knowledge_graph::{
    ClaimCursor, ClaimDeleteRequest, ClaimFilter, ClaimId, ClaimOperationId,
    InMemoryKnowledgeGraphStore, KnowledgeGraphClaimAppendExecutor, KnowledgeGraphStore,
    KnowledgeSpaceScope,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use serde_json::json;

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

fn run_scope(scope: &KnowledgeSpaceScope) -> AgentRunScope {
    AgentRunScope::new(
        scope.tenant().clone(),
        AgentId::new("specialist").expect("the agent id is valid"),
        AgentRunId::new("task-1-gen-1").expect("the run id is valid"),
    )
    .expect("the run scope is valid")
}

fn provenance() -> AgentClaimAppendProvenance {
    AgentClaimAppendProvenance {
        agent: AgentId::new("specialist").expect("the agent id is valid"),
        goal: Some(AgentGoalId::new("goal-1").expect("the goal id is valid")),
        task: AgentTaskId::new("task-1").expect("the task id is valid"),
        run: AgentRunId::new("task-1-gen-1").expect("the run id is valid"),
        delegation: Some(AgentDelegationId::new("delegation-1").expect("the id is valid")),
    }
}

fn append_request(
    space: KnowledgeSpaceId,
    object: AgentClaimObjectRequest,
) -> AgentClaimAppendRequest {
    AgentClaimAppendRequest {
        space,
        subject: "finding".to_string(),
        predicate: "links".to_string(),
        object,
        confidence_bps: 5_000,
        classification: MemoryClassification::Unclassified,
        evidence: Vec::new(),
        requested_by: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "researcher".to_string(),
            display_name: None,
        },
    }
}

/// The committed effect exactly as the run's transition would hand it over:
/// idempotent, so it carries the derived external key the operation id
/// depends on.
fn intent(scope: &AgentRunScope, slot: usize, request: &AgentClaimAppendRequest) -> AgentRunEffect {
    let spec = AgentEffectSpec::idempotent(AGENT_CLAIM_APPEND_DEFAULT_MAX_ATTEMPTS)
        .expect("the spec is valid");
    AgentRunEffect::new(
        scope,
        1,
        slot,
        AgentRunEffectRequest::ClaimAppend {
            append: Box::new(request.clone()),
            provenance: Box::new(provenance()),
        },
        &spec,
        AgentRevisionNumber::INITIAL,
        NOW,
    )
    .expect("the effect derives")
}

#[tokio::test]
async fn the_executor_stamps_provenance_and_converges_every_attempt_of_a_generation() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone());
    let scopes = ConformanceScopes::unique("append-executor");
    let space = scopes.primary.space().clone();
    let scope = run_scope(&scopes.primary);
    let request = append_request(
        space,
        AgentClaimObjectRequest::Value(
            AgentTaskContent::inline(json!({ "note": "observed" }))
                .expect("the object is inline-bounded"),
        ),
    );
    let effect = intent(&scope, 0, &request);

    let claim = match executor
        .execute(&scope, &effect, &request, &provenance(), NOW)
        .await
        .expect("the append reaches the store")
    {
        AgentClaimAppendFinding::Appended { claim } => claim,
        other => panic!("expected an appended claim, got {other:?}"),
    };

    // Every provenance dimension the run stamped reaches the durable record,
    // including the carrying effect the executor adds itself.
    let stored = store
        .query(
            &scopes.primary,
            &ClaimFilter::matching_all(),
            ClaimCursor::start(),
        )
        .await
        .expect("the query answers");
    assert_eq!(stored.claims.len(), 1);
    let recorded = &stored.claims[0];
    assert_eq!(recorded.claim_id.as_str(), claim.as_str());
    assert_eq!(recorded.provenance.agent.as_str(), "specialist");
    assert_eq!(
        recorded.provenance.goal.as_ref().map(ToString::to_string),
        Some("goal-1".to_string())
    );
    assert_eq!(
        recorded.provenance.task.as_ref().map(ToString::to_string),
        Some("task-1".to_string())
    );
    assert_eq!(
        recorded.provenance.run.as_ref().map(ToString::to_string),
        Some("task-1-gen-1".to_string())
    );
    assert_eq!(
        recorded
            .provenance
            .delegation
            .as_ref()
            .map(ToString::to_string),
        Some("delegation-1".to_string())
    );
    assert_eq!(
        recorded.provenance.effect.as_ref().map(ToString::to_string),
        Some(effect.effect_id.as_str().to_string()),
        "the carrying effect is stamped by the executor"
    );

    // A second attempt of the same generation — a retry after an ambiguous
    // loss — derives the identical store operation, so the ledger answers
    // with the original claim rather than appending a second one.
    let replayed = match executor
        .execute(&scope, &effect, &request, &provenance(), NOW)
        .await
        .expect("the retry answers")
    {
        AgentClaimAppendFinding::Appended { claim } => claim,
        other => panic!("expected the original claim, got {other:?}"),
    };
    assert_eq!(replayed.as_str(), claim.as_str());
    let after = store
        .query(
            &scopes.primary,
            &ClaimFilter::matching_all(),
            ClaimCursor::start(),
        )
        .await
        .expect("the query answers");
    assert_eq!(
        after.claims.len(),
        1,
        "a retry never appends a second claim"
    );
}

/// A deliberately re-decided generation is a new logical claim, and a
/// different effect slot is a different claim: the operation follows the
/// external key rather than any constant.
#[tokio::test]
async fn a_new_generation_and_a_new_slot_each_derive_their_own_claim() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone());
    let scopes = ConformanceScopes::unique("append-executor-generations");
    let space = scopes.primary.space().clone();
    let scope = run_scope(&scopes.primary);
    let request = append_request(space, AgentClaimObjectRequest::Node("evidence".to_string()));

    let first = intent(&scope, 0, &request);
    let sibling = intent(&scope, 1, &request);
    let mut regenerated = intent(&scope, 0, &request);
    regenerated
        .begin_next_generation(&scope, NOW)
        .expect("the generation advances");

    let mut appended = Vec::new();
    for effect in [&first, &sibling, &regenerated] {
        match executor
            .execute(&scope, effect, &request, &provenance(), NOW)
            .await
            .expect("the append reaches the store")
        {
            AgentClaimAppendFinding::Appended { claim } => appended.push(claim),
            other => panic!("expected an appended claim, got {other:?}"),
        }
    }
    assert_ne!(appended[0].as_str(), appended[1].as_str());
    assert_ne!(
        appended[0].as_str(),
        appended[2].as_str(),
        "a re-decided generation appends its own claim"
    );
    let stored = store
        .query(
            &scopes.primary,
            &ClaimFilter::matching_all(),
            ClaimCursor::start(),
        )
        .await
        .expect("the query answers");
    assert_eq!(stored.claims.len(), 3);
}

/// A statement the store refuses on its own bounds comes back as a definitive
/// finding, not a retryable error: retrying it would burn the effect's whole
/// attempt bound on an answer that can never change.
#[tokio::test]
async fn a_store_refusal_is_definitive_rather_than_retryable() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone());
    let scopes = ConformanceScopes::unique("append-executor-refusal");
    let scope = run_scope(&scopes.primary);
    // A node id the graph's own identity rules refuse: the run-side door
    // bounds length and emptiness, but the node vocabulary is the store's.
    let request = append_request(
        scopes.primary.space().clone(),
        AgentClaimObjectRequest::Node("evidence/with/separators".to_string()),
    );
    let effect = intent(&scope, 0, &request);

    match executor
        .execute(&scope, &effect, &request, &provenance(), NOW)
        .await
        .expect("a definitive refusal is a finding, never a transport error")
    {
        AgentClaimAppendFinding::Refused { code, .. } => {
            assert!(!code.is_empty(), "the refusal carries the store's code");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    let stored = store
        .query(
            &scopes.primary,
            &ClaimFilter::matching_all(),
            ClaimCursor::start(),
        )
        .await
        .expect("the query answers");
    assert!(stored.claims.is_empty(), "a refused append writes nothing");
}

/// A retried append whose claim was since deleted is a definitive refusal, not
/// a retryable error and not a second append: the store's ledger says the
/// operation was erased, and retrying could never change that answer
/// ([specification 13.4](../../docs/plans/rakka-agent/spec.md): a definitively
/// failed append does not end the run).
#[tokio::test]
async fn a_replayed_append_of_an_erased_claim_is_a_definitive_refusal() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone());
    let scopes = ConformanceScopes::unique("append-executor-erased");
    let scope = run_scope(&scopes.primary);
    let request = append_request(
        scopes.primary.space().clone(),
        AgentClaimObjectRequest::Node("evidence".to_string()),
    );
    let effect = intent(&scope, 0, &request);

    let claim = match executor
        .execute(&scope, &effect, &request, &provenance(), NOW)
        .await
        .expect("the append reaches the store")
    {
        AgentClaimAppendFinding::Appended { claim } => claim,
        other => panic!("expected an appended claim, got {other:?}"),
    };

    // An application erases the claim.
    let claim_id = ClaimId::new(claim.as_str()).expect("the claim id is valid");
    store
        .delete(
            &scopes.primary,
            &ClaimDeleteRequest {
                operation_id: ClaimOperationId::derive_delete(&scopes.primary, &claim_id, "erase")
                    .expect("the operation id derives"),
                claim_id,
                expected: None,
            },
        )
        .await
        .expect("the delete applies");

    // The same intent again — a retry after an ambiguous loss.
    match executor
        .execute(&scope, &effect, &request, &provenance(), NOW)
        .await
        .expect("an erased operation is a finding, never a transport error")
    {
        AgentClaimAppendFinding::Refused { code, .. } => {
            assert_eq!(code, "claim-operation-erased");
        }
        other => panic!("expected a definitive refusal, got {other:?}"),
    }
    let stored = store
        .query(
            &scopes.primary,
            &ClaimFilter::matching_all().including_tombstoned(),
            ClaimCursor::start(),
        )
        .await
        .expect("the query answers");
    assert!(
        stored.claims.is_empty(),
        "the retry did not restore the deleted claim"
    );
}

// ---------------------------------------------------------------------------
// Embedding at append (issue #140).
// ---------------------------------------------------------------------------

use rakka_agent::AgentMemoryEmbedder;
use support::{embedder, meaning_of, unclassified, CoreOnlyStore};

/// An append of an inline text value, at one classification.
fn text_request(
    space: KnowledgeSpaceId,
    text: &str,
    classification: MemoryClassification,
) -> AgentClaimAppendRequest {
    let mut request = append_request(
        space,
        AgentClaimObjectRequest::Value(
            AgentTaskContent::inline(json!(text)).expect("the object is inline-bounded"),
        ),
    );
    request.classification = classification;
    request
}

/// Executes one append and returns the stored claim's id.
async fn appended(
    executor: &KnowledgeGraphClaimAppendExecutor,
    scope: &KnowledgeSpaceScope,
    slot: usize,
    request: &AgentClaimAppendRequest,
) -> ClaimId {
    let run = run_scope(scope);
    let effect = intent(&run, slot, request);
    match executor
        .execute(&run, &effect, request, &provenance(), NOW)
        .await
        .expect("the append reaches the store")
    {
        AgentClaimAppendFinding::Appended { claim } => {
            ClaimId::new(claim.as_str()).expect("the claim id is valid")
        }
        other => panic!("expected an appended claim, got {other:?}"),
    }
}

#[tokio::test]
async fn the_executor_embeds_a_fresh_claim() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let embedder = embedder();
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone())
        .with_embedder(Arc::new(embedder.clone()), unclassified());
    let scopes = ConformanceScopes::unique("append-embeds");
    let scope = &scopes.primary;
    let request = text_request(
        scope.space().clone(),
        "the car is at the depot",
        MemoryClassification::Unclassified,
    );

    let claim_id = appended(&executor, scope, 0, &request).await;
    assert_eq!(
        embedder.calls(),
        vec!["finding links the car is at the depot".to_string()],
        "the claim is embedded from its subject, predicate and object text"
    );

    // No pass ran: the claim is findable by meaning straight after its
    // append, by a query that shares no word with it.
    let report = store
        .search_semantic(scope, &meaning_of(&embedder, "automobile", 8).await)
        .await
        .expect("the search answers");
    assert_eq!(report.hits.len(), 1);
    assert_eq!(report.hits[0].claim.claim_id, claim_id);
    assert_eq!(report.hits[0].relevance_bps, 10_000);
    assert!(!report.unindexed);

    // The record is the claim as appended: its own embedding note is not
    // written by the executor.
    let stored = store
        .get(scope, &claim_id)
        .await
        .expect("the claim reads")
        .expect("the claim is there");
    assert_eq!(stored.embedding, None);

    // A replayed attempt converges on the same claim and the same single
    // embedding.
    assert_eq!(appended(&executor, scope, 0, &request).await, claim_id);
    assert_eq!(
        store
            .search_semantic(scope, &meaning_of(&embedder, "automobile", 8).await)
            .await
            .expect("the search answers")
            .hits
            .len(),
        1
    );
}

#[tokio::test]
async fn a_failed_embedding_does_not_fail_the_append() {
    for refusing in [
        embedder().failing_on("car"),
        embedder().answering_short_on("car"),
    ] {
        let store = Arc::new(InMemoryKnowledgeGraphStore::new());
        let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone())
            .with_embedder(Arc::new(refusing.clone()), unclassified());
        let scopes = ConformanceScopes::unique("append-embed-fails");
        let scope = &scopes.primary;
        let request = text_request(
            scope.space().clone(),
            "the car is at the depot",
            MemoryClassification::Unclassified,
        );

        // The finding is `Appended`, exactly as without an embedder.
        let claim_id = appended(&executor, scope, 0, &request).await;
        assert_eq!(refusing.calls().len(), 1, "the model was asked");
        assert!(store
            .get(scope, &claim_id)
            .await
            .expect("the claim reads")
            .is_some());
        assert!(!store.has_embedding(scope, &claim_id));
        // The claim is left for the pass.
        let owed = store
            .unembedded_claims(scope, &refusing.embedding_ref(), &unclassified(), None, 8)
            .await
            .expect("the page answers");
        assert_eq!(owed.claims.len(), 1);
        assert_eq!(owed.claims[0].claim_id, claim_id);
    }
}

#[tokio::test]
async fn the_executor_sends_only_the_classifications_it_was_given() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let scopes = ConformanceScopes::unique("append-embed-set");
    let scope = &scopes.primary;
    let embedder = embedder();
    let cases: [(BTreeSet<MemoryClassification>, MemoryClassification); 3] = [
        (unclassified(), MemoryClassification::Sensitive),
        (
            BTreeSet::from([
                MemoryClassification::Unclassified,
                MemoryClassification::Redacted,
            ]),
            MemoryClassification::Redacted,
        ),
        (BTreeSet::new(), MemoryClassification::Unclassified),
    ];
    for (slot, (allowed, classification)) in cases.into_iter().enumerate() {
        let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone())
            .with_embedder(Arc::new(embedder.clone()), allowed);
        let request = text_request(scope.space().clone(), "the car is here", classification);
        let claim_id = appended(&executor, scope, slot, &request).await;
        assert!(
            !store.has_embedding(scope, &claim_id),
            "a {classification:?} claim outside the executor's set is not embedded"
        );
    }
    assert!(
        embedder.calls().is_empty(),
        "no withheld claim's text reached the model: {:?}",
        embedder.calls()
    );
}

#[tokio::test]
async fn an_executor_without_an_embedder_embeds_nothing() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone());
    let scopes = ConformanceScopes::unique("append-no-embedder");
    let scope = &scopes.primary;
    let request = text_request(
        scope.space().clone(),
        "the car is at the depot",
        MemoryClassification::Unclassified,
    );
    let claim_id = appended(&executor, scope, 0, &request).await;
    assert!(!store.has_embedding(scope, &claim_id));
}

#[tokio::test]
async fn an_executor_over_a_store_without_the_capability_calls_no_model() {
    let store = Arc::new(CoreOnlyStore::default());
    let embedder = embedder();
    let executor = KnowledgeGraphClaimAppendExecutor::new(store.clone())
        .with_embedder(Arc::new(embedder.clone()), unclassified());
    let scopes = ConformanceScopes::unique("append-core-only");
    let scope = &scopes.primary;
    let request = text_request(
        scope.space().clone(),
        "the car is at the depot",
        MemoryClassification::Unclassified,
    );
    let claim_id = appended(&executor, scope, 0, &request).await;
    assert!(store
        .get(scope, &claim_id)
        .await
        .expect("the claim reads")
        .is_some());
    assert!(
        embedder.calls().is_empty(),
        "a store that cannot keep an embedding is not worth a model call"
    );
}
