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
    ClaimCursor, ClaimFilter, InMemoryKnowledgeGraphStore, KnowledgeGraphClaimAppendExecutor,
    KnowledgeGraphStore, KnowledgeSpaceScope,
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
