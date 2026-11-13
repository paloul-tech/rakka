//! Conformance clauses for the knowledge-space retention sweep.
//!
//! The sweep is written once, in [`crate::retention`], over the store's own
//! `query`, `transitions` and `delete`. These clauses run it against every
//! backend, so what they prove is the composition: that a backend's paging,
//! its transition listing and its fenced delete together age claims out
//! exactly when the policy says, and never a claim that moved while the sweep
//! was looking at it.
//!
//! [`conformance_claim`] stamps `created_at = 1`, [`transition_request`] and
//! the promotion request stamp `occurred_at = 10`, and a conformance tombstone
//! stamps `tombstoned_at = 20`; every `now` and window below is chosen around
//! those three instants.

use std::future::Future;
use std::pin::Pin;

use rakka_agent_workflow::AgentTimestampMillis;

use super::erasure::{delete_request, tombstone_request, CONFORMANCE_TOMBSTONED_AT};
use super::{
    conformance_claim, drained_query, promotion_request, transition_request, ConformanceScopes,
};
use crate::claim::{Claim, ClaimId, ClaimOperationId, ClaimTrustStatus};
use crate::promotion::ClaimPromotionPolicy;
use crate::retention::{
    discharge_knowledge_space_retention, discharge_knowledge_space_retention_with_hook,
    KnowledgeSpaceRetentionPolicy, KnowledgeSpaceRetentionReport,
    KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
};
use crate::scope::KnowledgeSpaceScope;
use crate::store::{ClaimFilter, KnowledgeGraphStore};
use crate::tombstone::ClaimTombstoneReason;

/// One full pass at the default batch bound, from the start of the space.
async fn full_pass(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    policy: &KnowledgeSpaceRetentionPolicy,
    now: u64,
) -> KnowledgeSpaceRetentionReport {
    let report = discharge_knowledge_space_retention(
        store,
        scope,
        policy,
        AgentTimestampMillis::new(now),
        None,
        KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
    )
    .await
    .expect("the sweep answers");
    assert!(
        report.next.is_none(),
        "a pass under the default bound exhausts a small space"
    );
    report
}

/// Appends one claim and walks it into `trust`.
async fn claim_resting_in(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    trust: ClaimTrustStatus,
) -> Claim {
    let claim = conformance_claim(scope, discriminator, "subject", discriminator);
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let ungated = ClaimPromotionPolicy::ungated();
    let now = AgentTimestampMillis::new(10);
    match trust {
        ClaimTrustStatus::Proposed => {}
        ClaimTrustStatus::Verified => {
            store
                .transition(
                    scope,
                    &promotion_request(scope, &claim, "promote", None),
                    &ungated,
                    now,
                )
                .await
                .expect("the ungated promotion applies");
        }
        other => {
            store
                .transition(
                    scope,
                    &transition_request(scope, &claim.claim_id, "walk", other),
                    &ungated,
                    now,
                )
                .await
                .expect("the transition applies");
        }
    }
    claim
}

/// Whether the claim is still stored, live or stub.
async fn exists(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    id: &ClaimId,
) -> bool {
    store
        .get(scope, id)
        .await
        .expect("the read answers")
        .is_some()
}

/// The ids of every claim in the space, stubs included.
async fn all_ids(store: &dyn KnowledgeGraphStore, scope: &KnowledgeSpaceScope) -> Vec<ClaimId> {
    drained_query(
        store,
        scope,
        &ClaimFilter::matching_all().including_tombstoned(),
    )
    .await
    .into_iter()
    .map(|claim| claim.claim_id)
    .collect()
}

/// A window per trust status, measured from when the claim entered it: a
/// status with no window is retained, the clock is the last transition, a
/// stub is judged by the tombstone window alone, and a legal hold stops the
/// sweep reading anything.
pub async fn retention_ages_claims_by_trust_status(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;

    let proposed = claim_resting_in(store, scope, "ra-proposed", ClaimTrustStatus::Proposed).await;
    let verified = claim_resting_in(store, scope, "ra-verified", ClaimTrustStatus::Verified).await;
    let disputed = claim_resting_in(store, scope, "ra-disputed", ClaimTrustStatus::Disputed).await;
    let retracted =
        claim_resting_in(store, scope, "ra-retracted", ClaimTrustStatus::Retracted).await;
    let proposed_stub =
        claim_resting_in(store, scope, "ra-stub-proposed", ClaimTrustStatus::Proposed).await;
    let verified_stub =
        claim_resting_in(store, scope, "ra-stub-verified", ClaimTrustStatus::Verified).await;
    for stub in [&proposed_stub, &verified_stub] {
        store
            .tombstone(
                scope,
                &tombstone_request(
                    scope,
                    &stub.claim_id,
                    "withdraw",
                    ClaimTombstoneReason::Policy,
                ),
            )
            .await
            .expect("the tombstone applies");
    }
    assert_eq!(CONFORMANCE_TOMBSTONED_AT, 20);

    let policy = KnowledgeSpaceRetentionPolicy::retain_all()
        .with_window(ClaimTrustStatus::Proposed, 100)
        .with_window(ClaimTrustStatus::Retracted, 100)
        .with_tombstone_window(100);

    // now = 100: nothing is due. The proposed claim entered at 1, the
    // retracted one at 10, the stubs at 20.
    let report = full_pass(store, scope, &policy, 100).await;
    assert_eq!(report.examined, 6);
    assert_eq!(report.deleted, 0, "nothing is due at 100");
    assert_eq!(report.not_yet_due, 4);
    assert_eq!(report.retained, 2, "verified and disputed have no window");
    assert_eq!(report.moved, 0);
    assert!(!report.held);
    assert_eq!(all_ids(store, scope).await.len(), 6);

    // now = 101: the live proposed claim (1 + 100) goes, and nothing else. The
    // proposed *stub* stays — a stub is judged by the tombstone window, never
    // by its trust status — and so does the retracted claim, whose clock is
    // its last transition (10), not its creation (1).
    let report = full_pass(store, scope, &policy, 101).await;
    assert_eq!(report.deleted, 1, "only the proposed claim is due at 101");
    assert_eq!(report.not_yet_due, 3);
    assert_eq!(report.retained, 2);
    assert!(!exists(store, scope, &proposed.claim_id).await);
    assert!(exists(store, scope, &proposed_stub.claim_id).await);
    assert!(
        exists(store, scope, &retracted.claim_id).await,
        "a retracted claim ages from its retraction, not from its creation"
    );

    // now = 109 / 110: the retracted claim (10 + 100) is due exactly at 110.
    let report = full_pass(store, scope, &policy, 109).await;
    assert_eq!(report.deleted, 0);
    let report = full_pass(store, scope, &policy, 110).await;
    assert_eq!(report.deleted, 1, "the retracted claim is due at 110");
    assert_eq!(report.not_yet_due, 2);
    assert!(!exists(store, scope, &retracted.claim_id).await);

    // now = 120: both stubs (20 + 100) go, including the verified one whose
    // trust status has no window at all.
    let report = full_pass(store, scope, &policy, 119).await;
    assert_eq!(report.deleted, 0);
    let report = full_pass(store, scope, &policy, 120).await;
    assert_eq!(report.deleted, 2, "both stubs are due at 120");
    assert_eq!(report.examined, 4);
    assert_eq!(report.retained, 2);
    assert!(!exists(store, scope, &proposed_stub.claim_id).await);
    assert!(!exists(store, scope, &verified_stub.claim_id).await);

    // The statuses nobody set a window for survived every pass.
    assert!(exists(store, scope, &verified.claim_id).await);
    assert!(exists(store, scope, &disputed.claim_id).await);

    // A legal hold: the sweep reads nothing and deletes nothing, however due
    // the space is.
    let eager = KnowledgeSpaceRetentionPolicy::retain_all()
        .with_window(ClaimTrustStatus::Verified, 0)
        .with_window(ClaimTrustStatus::Disputed, 0);
    let held = full_pass(store, scope, &eager.clone().with_legal_hold(true), u64::MAX).await;
    assert!(held.held);
    assert_eq!(held.examined, 0, "a held space is not read");
    assert_eq!(held.deleted, 0);
    assert!(exists(store, scope, &verified.claim_id).await);
    assert!(exists(store, scope, &disputed.claim_id).await);

    // Arithmetic at the edges. A window so long its due instant is past the
    // end of the clock never comes due, and does not overflow.
    let forever = KnowledgeSpaceRetentionPolicy::retain_all()
        .with_window(ClaimTrustStatus::Verified, u64::MAX)
        .with_window(ClaimTrustStatus::Disputed, u64::MAX);
    for now in [u64::MAX - 1, u64::MAX] {
        let report = full_pass(store, scope, &forever, now).await;
        assert_eq!(report.deleted, 0, "a u64::MAX window never comes due");
        assert_eq!(report.not_yet_due, 2);
    }
    // `now` earlier than the entry instant is not due, even with no window to
    // wait out; the boundary itself is inclusive.
    for now in [0, 9] {
        let report = full_pass(store, scope, &eager, now).await;
        assert_eq!(report.deleted, 0, "a claim is not due before it entered");
        assert_eq!(report.not_yet_due, 2);
    }
    let report = full_pass(store, scope, &eager, 10).await;
    assert_eq!(
        report.deleted, 2,
        "a zero window is due at the entry instant"
    );
    assert!(all_ids(store, scope).await.is_empty());
}

/// One call examines at most its batch bound and hands back a cursor; calls
/// fed their predecessor's cursor cover the space exactly once, even though
/// each cursor points at a claim the call just deleted.
pub async fn retention_is_bounded_and_resumable(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let policy =
        KnowledgeSpaceRetentionPolicy::retain_all().with_window(ClaimTrustStatus::Proposed, 0);
    let now = AgentTimestampMillis::new(5);

    for index in 0..5 {
        claim_resting_in(
            store,
            scope,
            &format!("rb-{index}"),
            ClaimTrustStatus::Proposed,
        )
        .await;
    }
    let ids = all_ids(store, scope).await;
    assert_eq!(ids.len(), 5);

    // A foreign space is swept as the empty space it is, and the sweep of it
    // touches nothing here.
    let foreign = full_pass(store, &scopes.foreign, &policy, 5).await;
    assert_eq!(foreign, KnowledgeSpaceRetentionReport::default());
    assert_eq!(all_ids(store, scope).await, ids);

    let mut cursor = None;
    let mut deleted = Vec::new();
    for (call, expected) in [2usize, 2, 1].into_iter().enumerate() {
        let report =
            discharge_knowledge_space_retention(store, scope, &policy, now, cursor.take(), 2)
                .await
                .expect("the sweep answers");
        assert_eq!(report.examined, expected, "call {call} examines its batch");
        assert_eq!(report.deleted, expected, "call {call} deletes what is due");
        deleted.push(report.deleted);
        let consumed: usize = deleted.iter().sum();
        if call < 2 {
            let next = report.next.expect("more claims remain after this call");
            assert_eq!(
                next.position(),
                Some(&ids[consumed - 1]),
                "the cursor rests on the last claim examined — one this call deleted"
            );
            assert!(!exists(store, scope, &ids[consumed - 1]).await);
            assert_eq!(
                all_ids(store, scope).await,
                ids[consumed..].to_vec(),
                "nothing past the cursor was touched, nothing before it survives"
            );
            cursor = Some(next);
        } else {
            assert!(report.next.is_none(), "the last call exhausts the space");
        }
    }
    assert!(all_ids(store, scope).await.is_empty());

    // A batch bound of zero behaves as one.
    for index in 0..2 {
        claim_resting_in(
            store,
            scope,
            &format!("rb-more-{index}"),
            ClaimTrustStatus::Proposed,
        )
        .await;
    }
    let report = discharge_knowledge_space_retention(store, scope, &policy, now, None, 0)
        .await
        .expect("the sweep answers");
    assert_eq!(report.examined, 1, "a zero batch bound examines one claim");
    assert_eq!(report.deleted, 1);
    assert!(report.next.is_some());

    // A full pass finishes the space, and a re-driven pass finds nothing.
    let report = full_pass(store, scope, &policy, 5).await;
    assert_eq!(report.deleted, 1);
    let again = full_pass(store, scope, &policy, 5).await;
    assert_eq!(again, KnowledgeSpaceRetentionReport::default());
}

/// A claim whose trust moves, or that is tombstoned, between the sweep's read
/// and its delete is counted as moved and kept; the refusal spends nothing, so
/// a later pass deletes it under the same derived operation id once it is due
/// in its new state.
pub async fn retention_leaves_a_claim_that_moved(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let disputed_under_sweep =
        claim_resting_in(store, scope, "rm-dispute", ClaimTrustStatus::Proposed).await;
    let tombstoned_under_sweep =
        claim_resting_in(store, scope, "rm-tombstone", ClaimTrustStatus::Proposed).await;

    // Proposed claims are due at once; a disputed claim ages 100 from its
    // dispute; stubs have no window.
    let policy = KnowledgeSpaceRetentionPolicy::retain_all()
        .with_window(ClaimTrustStatus::Proposed, 0)
        .with_window(ClaimTrustStatus::Disputed, 100);

    let dispute_id = disputed_under_sweep.claim_id.clone();
    let tombstone_id = tombstoned_under_sweep.claim_id.clone();
    let hook = |claim: &Claim| -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let claim_id = claim.claim_id.clone();
        let dispute_id = dispute_id.clone();
        let tombstone_id = tombstone_id.clone();
        Box::pin(async move {
            if claim_id == dispute_id {
                store
                    .transition(
                        scope,
                        &transition_request(
                            scope,
                            &claim_id,
                            "dispute",
                            ClaimTrustStatus::Disputed,
                        ),
                        &ClaimPromotionPolicy::ungated(),
                        AgentTimestampMillis::new(10),
                    )
                    .await
                    .expect("the dispute lands under the sweep");
            } else if claim_id == tombstone_id {
                store
                    .tombstone(
                        scope,
                        &tombstone_request(
                            scope,
                            &claim_id,
                            "withdraw",
                            ClaimTombstoneReason::Withdrawn,
                        ),
                    )
                    .await
                    .expect("the tombstone lands under the sweep");
            }
        })
    };
    let report = discharge_knowledge_space_retention_with_hook(
        store,
        scope,
        &policy,
        AgentTimestampMillis::new(5),
        None,
        KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
        &hook,
    )
    .await
    .expect("the sweep answers");
    assert_eq!(report.examined, 2);
    assert_eq!(
        report.deleted, 0,
        "neither claim is in the state the sweep read"
    );
    assert_eq!(report.moved, 2);

    let disputed = store
        .get(scope, &dispute_id)
        .await
        .expect("the read answers")
        .expect("a claim that moved is kept");
    assert_eq!(disputed.trust(), ClaimTrustStatus::Disputed);
    assert!(!disputed.is_tombstoned());
    let stub = store
        .get(scope, &tombstone_id)
        .await
        .expect("the read answers")
        .expect("the stub an operator made under the sweep is kept");
    assert!(stub.is_tombstoned());

    // Not yet due as a disputed claim (10 + 100).
    let report = full_pass(store, scope, &policy, 109).await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.not_yet_due, 1);
    assert_eq!(report.retained, 1, "the stub has no window");

    // Due at 110, and deleted under the very operation id the stale attempt
    // derived: the refusal spent nothing.
    let report = full_pass(store, scope, &policy, 110).await;
    assert_eq!(report.deleted, 1);
    assert_eq!(report.retained, 1);
    assert!(!exists(store, scope, &dispute_id).await);
    assert!(exists(store, scope, &tombstone_id).await);

    let mut replay = delete_request(scope, &dispute_id, "unused", None);
    replay.operation_id = ClaimOperationId::derive_delete(scope, &dispute_id, "retention")
        .expect("the operation id derives");
    store
        .delete(scope, &replay)
        .await
        .expect("the sweep's derived delete is the one that applied");
}
