//! The HITL/policy promotion gate, end to end against the in-memory store.
//!
//! Specification 13.4 (policy may require HITL or a verifier before a claim
//! becomes `Verified`) over the slice 1.10 checkpoint-grant machinery of
//! `rakka-agent`: grants are constructed directly (every field is public, as
//! a resolved checkpoint would populate them) and validated through the same
//! `validate_for_binding` path the dispatch gate uses.

use rakka_agent::AgentCheckpointKind;
use rakka_agent_knowledge_graph::conformance::{
    conformance_claim, conformance_resolver, promotion_grant_for, ConformanceScopes,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimError, ClaimId, ClaimOperationId, ClaimPromotionEvidence, ClaimPromotionPolicy,
    ClaimTransitionCursor, ClaimTrustStatus, ClaimTrustTransitionRequest,
    InMemoryKnowledgeGraphStore, KnowledgeGraphStore, KnowledgeSpaceScope,
};
use rakka_agent_workflow::AgentTimestampMillis;

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);
const GRANT_EXPIRY: AgentTimestampMillis = AgentTimestampMillis::new(1_000);

fn promotion(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    evidence: Option<ClaimPromotionEvidence>,
) -> ClaimTrustTransitionRequest {
    let operation_id = ClaimOperationId::derive_transition(scope, claim_id, discriminator)
        .expect("the operation id derives");
    let mut request = ClaimTrustTransitionRequest::new(
        claim_id.clone(),
        operation_id,
        ClaimTrustStatus::Verified,
        conformance_resolver(),
        NOW,
    );
    if let Some(evidence) = evidence {
        request = request.with_promotion(evidence);
    }
    request
}

fn dispute(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
) -> ClaimTrustTransitionRequest {
    let operation_id = ClaimOperationId::derive_transition(scope, claim_id, discriminator)
        .expect("the operation id derives");
    ClaimTrustTransitionRequest::new(
        claim_id.clone(),
        operation_id,
        ClaimTrustStatus::Disputed,
        conformance_resolver(),
        NOW,
    )
}

async fn appended(store: &InMemoryKnowledgeGraphStore, scope: &KnowledgeSpaceScope) -> Claim {
    let claim = conformance_claim(scope, "gate-claim", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    claim
}

fn rejection_reason(refusal: ClaimError) -> &'static str {
    match refusal {
        ClaimError::PromotionGrantRejected { reason, .. } => reason.code(),
        other => panic!("expected a grant rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn the_default_policy_refuses_an_ungated_promotion() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-required").primary;
    let claim = appended(&store, &scope).await;
    assert_eq!(
        store
            .transition(
                &scope,
                &promotion(&scope, &claim.claim_id, "bare", None),
                &ClaimPromotionPolicy::default(),
                NOW,
            )
            .await
            .expect_err("the default policy fails closed")
            .code(),
        "claim-promotion-grant-required"
    );
}

#[tokio::test]
async fn a_valid_grant_promotes_and_stamps_the_receipt() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-valid").primary;
    let claim = appended(&store, &scope).await;
    let grant = promotion_grant_for(&scope, &claim, GRANT_EXPIRY, 1);
    let outcome = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "granted",
                Some(ClaimPromotionEvidence { grant }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect("a granted promotion applies");

    assert_eq!(outcome.claim.trust(), ClaimTrustStatus::Verified);
    assert_eq!(
        outcome.claim.provenance, claim.provenance,
        "promotion never rewrites the original provenance"
    );
    let receipt = outcome.transition.gate.expect("the receipt is stamped");
    assert_eq!(receipt.resolver, conformance_resolver());
    assert!(receipt.argument_digest.algorithm.is_cryptographic());

    let read = store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .expect("the claim exists");
    assert_eq!(read.trust(), ClaimTrustStatus::Verified);
}

#[tokio::test]
async fn an_expired_grant_is_refused() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-expired").primary;
    let claim = appended(&store, &scope).await;
    let grant = promotion_grant_for(&scope, &claim, AgentTimestampMillis::new(5), 1);
    let refusal = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "expired",
                Some(ClaimPromotionEvidence { grant }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect_err("an expired grant is refused");
    assert_eq!(refusal.code(), "claim-promotion-grant-rejected");
    assert_eq!(rejection_reason(refusal), "checkpoint-grant-expired");
}

#[tokio::test]
async fn a_grant_bound_to_different_content_is_refused() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-digest").primary;
    let claim = appended(&store, &scope).await;

    // A grant minted for a *different* claim fails the identity check.
    let other = conformance_claim(&scope, "gate-other", "x", "y");
    store
        .append(&scope, &other)
        .await
        .expect("the claim appends");
    let foreign_grant = promotion_grant_for(&scope, &other, GRANT_EXPIRY, 1);
    let refusal = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "wrong-claim",
                Some(ClaimPromotionEvidence {
                    grant: foreign_grant,
                }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect_err("a grant for another claim is refused");
    assert_eq!(
        rejection_reason(refusal),
        "checkpoint-grant-intent-mismatch"
    );

    // A grant with the right identity but a tampered digest fails the digest
    // comparison — approved content is the only content a grant promotes.
    let mut tampered = promotion_grant_for(&scope, &claim, GRANT_EXPIRY, 1);
    tampered.argument_digest = promotion_grant_for(&scope, &other, GRANT_EXPIRY, 1).argument_digest;
    let refusal = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "tampered",
                Some(ClaimPromotionEvidence { grant: tampered }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect_err("a tampered digest is refused");
    assert_eq!(
        rejection_reason(refusal),
        "checkpoint-argument-digest-mismatch"
    );
}

#[tokio::test]
async fn a_reconciliation_kind_grant_cannot_authorize_a_promotion() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-kind").primary;
    let claim = appended(&store, &scope).await;
    let mut grant = promotion_grant_for(&scope, &claim, GRANT_EXPIRY, 1);
    grant.kind = AgentCheckpointKind::IndeterminateEffectReconciliation;
    assert_eq!(
        store
            .transition(
                &scope,
                &promotion(
                    &scope,
                    &claim.claim_id,
                    "wrong-kind",
                    Some(ClaimPromotionEvidence { grant }),
                ),
                &ClaimPromotionPolicy::default(),
                NOW,
            )
            .await
            .expect_err("a non-approval-family kind is refused")
            .code(),
        "claim-promotion-grant-kind"
    );
}

#[tokio::test]
async fn a_spent_grant_is_refused() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-spent").primary;
    let claim = appended(&store, &scope).await;
    let grant = promotion_grant_for(&scope, &claim, GRANT_EXPIRY, 0);
    let refusal = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "spent",
                Some(ClaimPromotionEvidence { grant }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect_err("a zero-use grant is refused");
    assert_eq!(rejection_reason(refusal), "checkpoint-grant-uses-exhausted");
}

#[tokio::test]
async fn a_replayed_promotion_answers_the_original_without_reevaluating_the_gate() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-replay").primary;
    let claim = appended(&store, &scope).await;
    let grant = promotion_grant_for(&scope, &claim, GRANT_EXPIRY, 1);
    let request = promotion(
        &scope,
        &claim.claim_id,
        "granted",
        Some(ClaimPromotionEvidence { grant }),
    );
    let first = store
        .transition(&scope, &request, &ClaimPromotionPolicy::default(), NOW)
        .await
        .expect("the promotion applies");

    // The replay arrives long after the grant expired; the decided promotion
    // is not re-litigated.
    let long_after = AgentTimestampMillis::new(GRANT_EXPIRY.as_millis() + 1_000);
    let replayed = store
        .transition(
            &scope,
            &request,
            &ClaimPromotionPolicy::default(),
            long_after,
        )
        .await
        .expect("a replay answers");
    assert_eq!(replayed, first);

    let history = store
        .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(
        history.transitions.len(),
        1,
        "a replay appends no transition"
    );
}

#[tokio::test]
async fn a_foreign_tenant_grant_is_refused() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scopes = ConformanceScopes::unique("gate-tenant");
    let claim = appended(&store, &scopes.primary).await;
    // Minted with the right binding but under the foreign tenant's authority.
    let mut grant = promotion_grant_for(&scopes.primary, &claim, GRANT_EXPIRY, 1);
    grant.scope = promotion_grant_for(&scopes.foreign, &claim, GRANT_EXPIRY, 1).scope;
    assert_eq!(
        store
            .transition(
                &scopes.primary,
                &promotion(
                    &scopes.primary,
                    &claim.claim_id,
                    "foreign",
                    Some(ClaimPromotionEvidence { grant }),
                ),
                &ClaimPromotionPolicy::default(),
                NOW,
            )
            .await
            .expect_err("a foreign tenant's grant is refused")
            .code(),
        "claim-promotion-grant-scope"
    );
}

#[tokio::test]
async fn generation_pinning_makes_a_stale_grant_useless_after_a_dispute() {
    let store = InMemoryKnowledgeGraphStore::new();
    let scope = ConformanceScopes::unique("gate-ordinal").primary;
    let claim = appended(&store, &scope).await;

    // Minted while the claim was Proposed at ordinal headroom 1.
    let stale = promotion_grant_for(&scope, &claim, GRANT_EXPIRY, 1);

    store
        .transition(
            &scope,
            &dispute(&scope, &claim.claim_id, "dispute"),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect("the dispute applies");

    // The re-promotion occupies ordinal 2; the stale grant binds generation 1.
    let refusal = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "stale-grant",
                Some(ClaimPromotionEvidence { grant: stale }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect_err("a stale-ordinal grant is refused");
    assert_eq!(
        rejection_reason(refusal),
        "checkpoint-grant-intent-mismatch"
    );

    // A grant minted for the claim as it now stands succeeds.
    let current = store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .expect("the claim exists");
    let fresh = promotion_grant_for(&scope, &current, GRANT_EXPIRY, 1);
    let outcome = store
        .transition(
            &scope,
            &promotion(
                &scope,
                &claim.claim_id,
                "fresh-grant",
                Some(ClaimPromotionEvidence { grant: fresh }),
            ),
            &ClaimPromotionPolicy::default(),
            NOW,
        )
        .await
        .expect("a fresh-ordinal grant promotes");
    assert_eq!(outcome.claim.trust(), ClaimTrustStatus::Verified);
    assert_eq!(outcome.transition.ordinal, 2);
}
