//! The graph-backed goal-claim source, end to end against a real store
//! ([specification 17.18](../../docs/plans/rakka-agent/spec.md): the goal
//! projection's shared-knowledge references).
//!
//! Settled claim receipts are pruned from durable run state, so this source
//! is the one complete enumeration the authorized goal view can join landed
//! claims through. What matters here: the join is goal-scoped, the stamped
//! provenance reaches the reference intact, the space each reference reports
//! is the scope it was queried under, and the bound is honoured across
//! paging.

use std::sync::Arc;

use rakka_agent::{
    AgentDelegationId, AgentGoalClaimSource, AgentGoalId, AgentId, AgentRunId, AgentTaskId,
    KnowledgeSpaceId, MemoryClassification, TenantId,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimProvenance,
    InMemoryKnowledgeGraphStore, KnowledgeGraphGoalClaimSource, KnowledgeGraphStore,
    KnowledgeSpaceScope,
};
use rakka_agent_workflow::AgentTimestampMillis;

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn space() -> KnowledgeSpaceId {
    KnowledgeSpaceId::new("mission-findings").expect("the space id is valid")
}

fn space_scope() -> KnowledgeSpaceScope {
    KnowledgeSpaceScope::new(tenant(), space()).expect("the scope is valid")
}

fn goal() -> AgentGoalId {
    AgentGoalId::new("goal-1").expect("the goal id is valid")
}

fn specialist() -> AgentId {
    AgentId::new("specialist").expect("the agent id is valid")
}

/// Appends one claim under `key`, asserted in service of `for_goal`.
async fn append_claim(
    store: &InMemoryKnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    key: &str,
    for_goal: &AgentGoalId,
    delegated: bool,
) -> Claim {
    let operation_id =
        ClaimOperationId::derive_append(scope, key).expect("the operation id derives");
    let mut provenance = ClaimProvenance::for_agent(specialist())
        .with_goal(for_goal.clone())
        .with_task(AgentTaskId::new("child-translation").expect("the task id is valid"))
        .with_run(AgentRunId::new("child-translation-gen-1").expect("the run id is valid"));
    if delegated {
        provenance = provenance.with_delegation(
            AgentDelegationId::new("delegation-1").expect("the delegation id is valid"),
        );
    }
    let claim = Claim::new(
        scope,
        operation_id,
        ClaimNodeId::new("finding").expect("the node id is valid"),
        ClaimPredicate::new("links").expect("the predicate is valid"),
        ClaimObject::Node(ClaimNodeId::new("source").expect("the node id is valid")),
        provenance,
        5_000,
        MemoryClassification::Unclassified,
        NOW,
    )
    .expect("the claim constructs");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends")
}

/// Goal-scoped claims join with their provenance intact — and only they do:
/// a claim under another goal never rides this goal's view.
#[tokio::test]
async fn goal_scoped_claims_join_with_their_provenance() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let scope = space_scope();
    let first = append_claim(&store, &scope, "append-1", &goal(), true).await;
    let second = append_claim(&store, &scope, "append-2", &goal(), false).await;
    let foreign_goal = AgentGoalId::new("goal-2").expect("the goal id is valid");
    append_claim(&store, &scope, "append-3", &foreign_goal, false).await;

    let source = KnowledgeGraphGoalClaimSource::new(store).with_space(space());
    let refs = source
        .claims_for_goal(&tenant(), &goal(), 16)
        .await
        .expect("the source answers");

    assert_eq!(refs.len(), 2, "the foreign goal's claim never joins");
    let mut expected: Vec<String> = vec![
        first.claim_id.as_str().to_string(),
        second.claim_id.as_str().to_string(),
    ];
    expected.sort();
    let answered: Vec<String> = refs
        .iter()
        .map(|reference| reference.claim.as_str().to_string())
        .collect();
    assert_eq!(answered, expected, "ascending claim-id order");
    for reference in &refs {
        assert_eq!(reference.space, space());
        assert_eq!(reference.agent, specialist());
        assert_eq!(
            reference.task.as_ref().map(|task| task.as_str()),
            Some("child-translation")
        );
        assert!(reference.run.is_some());
    }
    let delegated: Vec<bool> = refs
        .iter()
        .map(|reference| reference.delegation.is_some())
        .collect();
    assert!(
        delegated.contains(&true) && delegated.contains(&false),
        "the delegation provenance rides exactly where it was stamped"
    );
}

/// The bound holds across paging, and a space the source was never given is
/// simply not joined.
#[tokio::test]
async fn the_limit_and_the_named_spaces_bound_the_join() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let scope = space_scope();
    for index in 0..5 {
        append_claim(&store, &scope, &format!("append-{index}"), &goal(), false).await;
    }

    let source = KnowledgeGraphGoalClaimSource::new(store.clone()).with_space(space());
    let refs = source
        .claims_for_goal(&tenant(), &goal(), 3)
        .await
        .expect("the source answers");
    assert_eq!(refs.len(), 3, "the limit bounds the join");

    let elsewhere = KnowledgeGraphGoalClaimSource::new(store)
        .with_space(KnowledgeSpaceId::new("other-space").expect("the space id is valid"));
    let refs = elsewhere
        .claims_for_goal(&tenant(), &goal(), 16)
        .await
        .expect("the source answers");
    assert!(
        refs.is_empty(),
        "a space the source was never given is not joined"
    );
}
