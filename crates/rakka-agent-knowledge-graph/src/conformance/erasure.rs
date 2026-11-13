//! Conformance clauses for tombstone and delete
//! ([specification 13.1](../../../../docs/plans/rakka-agent/spec.md): every
//! memory API provides retention, tombstone, and deletion semantics).
//!
//! A tombstone withdraws one claim's content and keeps an auditable stub; a
//! delete removes the claim entirely. Both erase the replay ledger's payloads
//! for the claim's earlier operations, and that is the property these clauses
//! press hardest: no replayed write returns, or restores, what was withdrawn.

use rakka_agent::{AgentId, AgentPrivateMemory, AgentRevisionNumber, MemoryEmbeddingRef};
use rakka_agent_workflow::{
    AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
};

use super::{
    conformance_claim, conformance_resolver, drained_query, transition_request, ConformanceScopes,
};
use crate::claim::{
    Claim, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimTrustStatus,
    CLAIM_MAX_TRUST_TRANSITIONS, CURRENT_CLAIM_SCHEMA_VERSION,
};
use crate::promotion::ClaimPromotionPolicy;
use crate::scope::KnowledgeSpaceScope;
use crate::store::{ClaimFilter, ClaimTransitionCursor, ClaimTraversal, KnowledgeGraphStore};
use crate::tombstone::{
    ClaimDeleteExpectation, ClaimDeleteRequest, ClaimTombstoneReason, ClaimTombstoneRequest,
    CLAIM_TOMBSTONE_MARKER,
};
use crate::transition::ClaimTrustTransition;

/// The instant every conformance tombstone records.
pub(crate) const CONFORMANCE_TOMBSTONED_AT: u64 = 20;

/// A tombstone request for one claim, at discriminator `discriminator`.
pub(crate) fn tombstone_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    reason: ClaimTombstoneReason,
) -> ClaimTombstoneRequest {
    ClaimTombstoneRequest {
        claim_id: claim_id.clone(),
        operation_id: ClaimOperationId::derive_tombstone(scope, claim_id, discriminator)
            .expect("the operation id derives"),
        reason,
        actor: conformance_resolver(),
        tombstoned_at: AgentTimestampMillis::new(CONFORMANCE_TOMBSTONED_AT),
    }
}

/// A delete request for one claim, at discriminator `discriminator`.
pub(crate) fn delete_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    expected: Option<ClaimDeleteExpectation>,
) -> ClaimDeleteRequest {
    ClaimDeleteRequest {
        claim_id: claim_id.clone(),
        operation_id: ClaimOperationId::derive_delete(scope, claim_id, discriminator)
            .expect("the operation id derives"),
        expected,
    }
}

/// One evidence artifact reference.
fn evidence(id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind: ArtifactKind::File,
        uri: format!("s3://evidence/{id}"),
        checksum: Some(format!("sha256:{id}")),
        content_type: Some("application/json".to_string()),
        byte_len: Some(64),
        retention_class: None,
        encryption: None,
        redaction: RedactionStatus::Unredacted,
        created_at: AgentTimestampMillis::new(1),
        metadata: AgentAttributes::default(),
    }
}

/// An edge claim carrying evidence and an embedding: everything a tombstone
/// must clear beside the statement.
fn furnished_claim(
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    subject: &str,
    object: &str,
) -> Claim {
    conformance_claim(scope, discriminator, subject, object)
        .with_evidence(vec![evidence("e-1"), evidence("e-2")])
        .expect("the evidence is within bounds")
        .with_embedding(MemoryEmbeddingRef {
            model: "embedder-v1".to_string(),
            dimensions: 3,
            version: AgentRevisionNumber::INITIAL,
        })
        .expect("the embedding is valid")
}

/// Asserts `stub` is exactly `live` with its content withdrawn: the statement,
/// evidence and embedding cleared, every audit field kept.
fn assert_stub_of(stub: &Claim, live: &Claim) {
    assert!(stub.is_tombstoned(), "a tombstoned claim says so");
    assert_eq!(stub.subject.as_str(), CLAIM_TOMBSTONE_MARKER);
    assert_eq!(stub.predicate.as_str(), CLAIM_TOMBSTONE_MARKER);
    assert_eq!(
        stub.object,
        ClaimObject::Value(AgentPrivateMemory::tombstone_content()),
        "a stub's object is the null content marker"
    );
    assert!(stub.evidence.is_empty(), "a stub carries no evidence");
    assert!(stub.embedding.is_none(), "a stub carries no embedding");
    assert_eq!(stub.schema_version(), CURRENT_CLAIM_SCHEMA_VERSION);

    assert_eq!(stub.claim_id, live.claim_id);
    assert_eq!(stub.operation_id, live.operation_id);
    assert_eq!(stub.provenance, live.provenance);
    assert_eq!(stub.created_at, live.created_at);
    assert_eq!(stub.confidence_bps, live.confidence_bps);
    assert_eq!(stub.classification, live.classification);
    assert_eq!(stub.access, live.access);
    assert_eq!(stub.policy, live.policy);
    assert_eq!(stub.policy_revision, live.policy_revision);
    assert_eq!(
        stub.trust(),
        live.trust(),
        "a tombstone is not a trust verdict"
    );
    assert_eq!(stub.transition_count(), live.transition_count());
    assert_eq!(
        stub.content_digest, live.content_digest,
        "the fingerprint still describes the withdrawn statement"
    );
}

/// Every transition of one claim, drained across pages.
async fn drained_transitions(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
) -> Vec<ClaimTrustTransition> {
    let mut drained = Vec::new();
    let mut cursor = ClaimTransitionCursor::start();
    loop {
        let page = store
            .transitions(scope, claim_id, cursor)
            .await
            .expect("the history lists");
        drained.extend(page.transitions);
        match page.next {
            Some(next) => cursor = next,
            None => return drained,
        }
    }
}

/// The current stored claim, which must exist.
async fn stored(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    id: &ClaimId,
) -> Claim {
    store
        .get(scope, id)
        .await
        .expect("the read answers")
        .expect("the claim exists")
}

/// A tombstone withdraws the statement, evidence and embedding, and keeps the
/// claim's identity, provenance, classification, access, trust, fingerprint
/// and transition history.
pub async fn tombstone_withdraws_content(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let claim = furnished_claim(scope, "tw-1", "customer-1", "channel-email");
    let bystander = conformance_claim(scope, "tw-2", "customer-1", "channel-sms");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    store
        .append(scope, &bystander)
        .await
        .expect("the bystander appends");
    let disputed = store
        .transition(
            scope,
            &transition_request(
                scope,
                &claim.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies")
        .claim;

    let request = tombstone_request(
        scope,
        &claim.claim_id,
        "withdraw",
        ClaimTombstoneReason::Withdrawn,
    );
    let stub = store
        .tombstone(scope, &request)
        .await
        .expect("the tombstone applies");

    assert_stub_of(&stub, &disputed);
    assert_eq!(stub.trust(), ClaimTrustStatus::Disputed);
    let tombstone = stub.tombstone().expect("a stub records its withdrawal");
    assert_eq!(tombstone.operation_id, request.operation_id);
    assert_eq!(tombstone.reason, ClaimTombstoneReason::Withdrawn);
    assert_eq!(tombstone.actor, request.actor);
    assert_eq!(tombstone.tombstoned_at, request.tombstoned_at);

    // `get` returns the stub: the withdrawal stays visible to anyone
    // authorized for the space.
    assert_eq!(
        stored(store, scope, &claim.claim_id).await,
        stub,
        "the stored claim is the stub the operation returned"
    );

    // The history survives a tombstone whole.
    let history = drained_transitions(store, scope, &claim.claim_id).await;
    assert_eq!(history.len(), 1, "a tombstone keeps the transition history");
    assert_eq!(history[0].to, ClaimTrustStatus::Disputed);

    // Nothing else in the space moved.
    assert_eq!(
        serde_json::to_vec(&stored(store, scope, &bystander.claim_id).await)
            .expect("the claim serializes"),
        serde_json::to_vec(&bystander).expect("the claim serializes"),
        "a tombstone touches only the claim it addresses"
    );
}

/// A stub is hidden from discovery: `query` excludes it unless asked, no
/// statement filter ever matches it, and traversal never follows it.
pub async fn tombstoned_claims_are_hidden_from_discovery(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let live = conformance_claim(scope, "th-live", "hub", "n-live");
    let withdrawn = conformance_claim(scope, "th-gone", "hub", "n-gone");
    for claim in [&live, &withdrawn] {
        store.append(scope, claim).await.expect("the claim appends");
    }
    store
        .tombstone(
            scope,
            &tombstone_request(
                scope,
                &withdrawn.claim_id,
                "withdraw",
                ClaimTombstoneReason::Policy,
            ),
        )
        .await
        .expect("the tombstone applies");

    let ids = |claims: Vec<Claim>| -> Vec<ClaimId> {
        claims.into_iter().map(|claim| claim.claim_id).collect()
    };
    let mut both = vec![live.claim_id.clone(), withdrawn.claim_id.clone()];
    both.sort();

    // Excluded by default, included on opt-in.
    let default = drained_query(store, scope, &ClaimFilter::matching_all()).await;
    assert_eq!(
        ids(default),
        vec![live.claim_id.clone()],
        "a default query does not list a stub"
    );
    let opted_in = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all().including_tombstoned(),
    )
    .await;
    assert_eq!(ids(opted_in), both, "an opted-in query lists the stub too");

    // A statement filter never matches a stub — not by the statement it once
    // made, and not by the marker it holds now.
    let hub = ClaimNodeId::new("hub").expect("the node id is valid");
    let by_subject = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all()
            .with_subject(hub.clone())
            .including_tombstoned(),
    )
    .await;
    assert_eq!(
        ids(by_subject),
        vec![live.claim_id.clone()],
        "a stub no longer answers to its withdrawn subject"
    );
    let by_object = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all()
            .with_object_node(ClaimNodeId::new("n-gone").expect("the node id is valid"))
            .including_tombstoned(),
    )
    .await;
    assert!(
        by_object.is_empty(),
        "a stub no longer answers to its withdrawn object"
    );
    let by_marker = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all()
            .with_subject(ClaimNodeId::new(CLAIM_TOMBSTONE_MARKER).expect("the marker is valid"))
            .including_tombstoned(),
    )
    .await;
    assert!(
        by_marker.is_empty(),
        "the marker is not a subject a stub can be found by"
    );

    // The provenance axes still reach it.
    let by_agent = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all()
            .with_agent(AgentId::new("scout").expect("the agent id is valid"))
            .including_tombstoned(),
    )
    .await;
    assert_eq!(ids(by_agent), both, "a stub keeps its provenance axes");

    // Traversal reports the live edge only.
    let report = store
        .traverse(scope, &ClaimTraversal::from_node(hub))
        .await
        .expect("the traversal answers");
    let reached: Vec<&str> = report.nodes.iter().map(ClaimNodeId::as_str).collect();
    assert_eq!(
        reached,
        ["hub", "n-live"],
        "a tombstoned edge is not followed"
    );
    assert_eq!(report.edges.len(), 1);
    assert_eq!(report.edges[0].claim_id, live.claim_id);
}

/// A stub is frozen: its trust cannot move, it cannot be tombstoned again, and
/// it cannot be appended.
pub async fn a_stub_is_frozen(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let claim = conformance_claim(scope, "sf-1", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let stub = store
        .tombstone(
            scope,
            &tombstone_request(
                scope,
                &claim.claim_id,
                "withdraw",
                ClaimTombstoneReason::Withdrawn,
            ),
        )
        .await
        .expect("the tombstone applies");

    assert_eq!(
        store
            .transition(
                scope,
                &transition_request(
                    scope,
                    &claim.claim_id,
                    "retract",
                    ClaimTrustStatus::Retracted
                ),
                &ClaimPromotionPolicy::ungated(),
                AgentTimestampMillis::new(30),
            )
            .await
            .expect_err("a stub's trust is frozen")
            .code(),
        "claim-tombstoned"
    );
    assert_eq!(
        store
            .tombstone(
                scope,
                &tombstone_request(
                    scope,
                    &claim.claim_id,
                    "withdraw-again",
                    ClaimTombstoneReason::Policy,
                ),
            )
            .await
            .expect_err("a stub is not tombstoned twice")
            .code(),
        "claim-tombstoned"
    );
    assert_eq!(
        stored(store, scope, &claim.claim_id).await,
        stub,
        "a refused operation leaves the stub exactly as it was"
    );
    assert!(
        drained_transitions(store, scope, &claim.claim_id)
            .await
            .is_empty(),
        "a refused transition appends nothing"
    );

    // A stub is never appended: an append is a born-`Proposed` live claim.
    let never_stored = conformance_claim(scope, "sf-2", "a", "c");
    let born_stub = never_stored
        .apply_tombstone(
            stub.tombstone()
                .expect("a stub records its withdrawal")
                .clone(),
        )
        .expect("the stub is built");
    assert_eq!(
        store
            .append(scope, &born_stub)
            .await
            .expect_err("a stub is not appended")
            .code(),
        "claim-tombstoned"
    );
    assert!(
        store
            .get(scope, &never_stored.claim_id)
            .await
            .expect("the read answers")
            .is_none(),
        "a refused append stores nothing"
    );
}

/// Replay after a tombstone: the tombstone's own replay returns the stub, and
/// every earlier operation of the claim — its append and each transition, up
/// to a full history — answers `claim-operation-erased` and writes nothing.
pub async fn tombstone_replay_and_erased_operations(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let ungated = ClaimPromotionPolicy::ungated();
    let now = AgentTimestampMillis::new(10);

    // A claim driven to the history cap, every operation kept for replay.
    let claim = conformance_claim(scope, "tr-1", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let mut requests = Vec::new();
    for step in 0..CLAIM_MAX_TRUST_TRANSITIONS {
        let to = if step.is_multiple_of(2) {
            ClaimTrustStatus::Disputed
        } else {
            ClaimTrustStatus::Verified
        };
        let request = transition_request(scope, &claim.claim_id, &format!("osc-{step}"), to);
        store
            .transition(scope, &request, &ungated, now)
            .await
            .expect("the oscillation applies within the bound");
        requests.push(request);
    }
    let full = stored(store, scope, &claim.claim_id).await;
    assert_eq!(full.transition_count(), CLAIM_MAX_TRUST_TRANSITIONS);

    let request = tombstone_request(
        scope,
        &claim.claim_id,
        "withdraw",
        ClaimTombstoneReason::Withdrawn,
    );
    let stub = store
        .tombstone(scope, &request)
        .await
        .expect("a claim at the history cap is tombstoned");
    assert_stub_of(&stub, &full);

    // The tombstone's own replay answers the stub as first written.
    assert_eq!(
        store
            .tombstone(scope, &request)
            .await
            .expect("a replayed tombstone answers"),
        stub
    );

    // Every earlier operation is erased: no replay returns the statement.
    assert_eq!(
        store
            .append(scope, &claim)
            .await
            .expect_err("a replayed append of a tombstoned claim is refused")
            .code(),
        "claim-operation-erased"
    );
    for request in &requests {
        assert_eq!(
            store
                .transition(scope, request, &ungated, now)
                .await
                .expect_err("a replayed transition of a tombstoned claim is refused")
                .code(),
            "claim-operation-erased"
        );
    }
    assert_eq!(
        stored(store, scope, &claim.claim_id).await,
        stub,
        "an erased replay writes nothing"
    );
    let history = drained_transitions(store, scope, &claim.claim_id).await;
    assert_eq!(
        history.len(),
        usize::try_from(CLAIM_MAX_TRUST_TRANSITIONS).expect("the cap fits"),
        "a tombstone keeps the whole transition history"
    );

    // A tombstone under an operation id another kind already spent is a
    // conflict, and changes nothing.
    let other = conformance_claim(scope, "tr-other", "x", "y");
    store
        .append(scope, &other)
        .await
        .expect("the claim appends");
    let mut crossed = tombstone_request(
        scope,
        &other.claim_id,
        "unused",
        ClaimTombstoneReason::Withdrawn,
    );
    crossed.operation_id = other.operation_id.clone();
    assert_eq!(
        store
            .tombstone(scope, &crossed)
            .await
            .expect_err("a tombstone under a spent append operation id is refused")
            .code(),
        "claim-operation-conflict"
    );
    assert!(
        !stored(store, scope, &other.claim_id).await.is_tombstoned(),
        "a refused tombstone withdraws nothing"
    );
}

/// A tombstone under a foreign scope answers exactly as an absent claim does,
/// and withdraws nothing.
pub async fn tombstone_isolation(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let foreign = &scopes.foreign;
    let claim = conformance_claim(scope, "ti-1", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");

    let foreign_refusal = store
        .tombstone(
            foreign,
            &tombstone_request(
                foreign,
                &claim.claim_id,
                "foreign",
                ClaimTombstoneReason::Withdrawn,
            ),
        )
        .await
        .expect_err("a foreign tombstone is refused");
    let absent = conformance_claim(foreign, "ti-absent", "x", "y");
    let absent_refusal = store
        .tombstone(
            foreign,
            &tombstone_request(
                foreign,
                &absent.claim_id,
                "absent",
                ClaimTombstoneReason::Withdrawn,
            ),
        )
        .await
        .expect_err("an absent tombstone is refused");
    assert_eq!(foreign_refusal.code(), "claim-not-found");
    assert_eq!(foreign_refusal.code(), absent_refusal.code());

    assert_eq!(
        serde_json::to_vec(&stored(store, scope, &claim.claim_id).await)
            .expect("the claim serializes"),
        serde_json::to_vec(&claim).expect("the claim serializes"),
        "a foreign tombstone leaves the claim exactly as stored"
    );
}

/// A delete removes the claim and its whole transition history; a stub can be
/// deleted too, and nothing else in the space moves.
pub async fn delete_erases_a_claim(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let ungated = ClaimPromotionPolicy::ungated();
    let now = AgentTimestampMillis::new(10);

    let claim = furnished_claim(scope, "de-1", "customer-1", "channel-email");
    let bystander = conformance_claim(scope, "de-2", "customer-1", "channel-sms");
    for appended in [&claim, &bystander] {
        store
            .append(scope, appended)
            .await
            .expect("the claim appends");
    }
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &claim.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ungated,
            now,
        )
        .await
        .expect("the dispute applies");
    // An empty history, captured from a claim that never had one.
    let no_history = store
        .transitions(scope, &bystander.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");

    store
        .delete(
            scope,
            &delete_request(scope, &claim.claim_id, "erase", None),
        )
        .await
        .expect("the delete applies");

    assert!(
        store
            .get(scope, &claim.claim_id)
            .await
            .expect("the read answers")
            .is_none(),
        "a deleted claim is absent"
    );
    assert_eq!(
        store
            .transitions(scope, &claim.claim_id, ClaimTransitionCursor::start())
            .await
            .expect("the history lists"),
        no_history,
        "a deleted claim's history answers exactly as a claim with none"
    );
    for filter in [
        ClaimFilter::matching_all(),
        ClaimFilter::matching_all().including_tombstoned(),
    ] {
        let remaining = drained_query(store, scope, &filter).await;
        assert_eq!(remaining.len(), 1, "only the bystander remains");
        assert_eq!(remaining[0].claim_id, bystander.claim_id);
    }
    assert_eq!(
        serde_json::to_vec(&stored(store, scope, &bystander.claim_id).await)
            .expect("the claim serializes"),
        serde_json::to_vec(&bystander).expect("the claim serializes"),
        "a delete touches only the claim it addresses"
    );

    // A stub can be deleted, and its tombstone's own operation is erased with
    // it: the stub it once returned is content-free, but it is gone too.
    let withdrawn = conformance_claim(scope, "de-3", "customer-2", "channel-email");
    store
        .append(scope, &withdrawn)
        .await
        .expect("the claim appends");
    let tombstone = tombstone_request(
        scope,
        &withdrawn.claim_id,
        "withdraw",
        ClaimTombstoneReason::Withdrawn,
    );
    store
        .tombstone(scope, &tombstone)
        .await
        .expect("the tombstone applies");
    store
        .delete(
            scope,
            &delete_request(scope, &withdrawn.claim_id, "erase", None),
        )
        .await
        .expect("a stub is deleted");
    assert!(store
        .get(scope, &withdrawn.claim_id)
        .await
        .expect("the read answers")
        .is_none());
    assert_eq!(
        store
            .tombstone(scope, &tombstone)
            .await
            .expect_err("a replayed tombstone of a deleted claim is refused")
            .code(),
        "claim-operation-erased"
    );
    let remaining = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all().including_tombstoned(),
    )
    .await;
    assert_eq!(remaining.len(), 1, "the deleted stub is not listed");
}

/// A delete cannot be undone: a replayed append answers
/// `claim-operation-erased` and the claim stays absent, the delete's own
/// replay answers success, and an erased operation id answers the same
/// refusal whatever kind of operation it is replayed as.
pub async fn a_deleted_claim_cannot_be_restored_by_replay(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let ungated = ClaimPromotionPolicy::ungated();
    let now = AgentTimestampMillis::new(10);

    let claim = conformance_claim(scope, "dr-1", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let dispute = transition_request(
        scope,
        &claim.claim_id,
        "dispute",
        ClaimTrustStatus::Disputed,
    );
    store
        .transition(scope, &dispute, &ungated, now)
        .await
        .expect("the dispute applies");
    let delete = delete_request(scope, &claim.claim_id, "erase", None);
    store
        .delete(scope, &delete)
        .await
        .expect("the delete applies");

    let absent = || async {
        assert!(
            store
                .get(scope, &claim.claim_id)
                .await
                .expect("the read answers")
                .is_none(),
            "a deleted claim stays absent"
        );
        assert!(
            drained_query(
                store,
                scope,
                &ClaimFilter::matching_all().including_tombstoned()
            )
            .await
            .is_empty(),
            "nothing was restored"
        );
        assert!(
            drained_transitions(store, scope, &claim.claim_id)
                .await
                .is_empty(),
            "no history was restored"
        );
    };

    // The same `Claim` value the writer would re-send after a lost reply.
    assert_eq!(
        store
            .append(scope, &claim)
            .await
            .expect_err("a replayed append of a deleted claim is refused")
            .code(),
        "claim-operation-erased"
    );
    absent().await;
    assert_eq!(
        store
            .transition(scope, &dispute, &ungated, now)
            .await
            .expect_err("a replayed transition of a deleted claim is refused")
            .code(),
        "claim-operation-erased"
    );
    absent().await;

    // The delete's own replay answers success, and deletes nothing more.
    store
        .delete(scope, &delete)
        .await
        .expect("a replayed delete answers success");
    absent().await;

    // An erased operation id is erased for every kind of operation.
    let erased = claim.operation_id.clone();
    let mut as_transition = transition_request(
        scope,
        &claim.claim_id,
        "unused",
        ClaimTrustStatus::Retracted,
    );
    as_transition.operation_id = erased.clone();
    assert_eq!(
        store
            .transition(scope, &as_transition, &ungated, now)
            .await
            .expect_err("an erased operation id replayed as a transition is refused")
            .code(),
        "claim-operation-erased"
    );
    let mut as_tombstone = tombstone_request(
        scope,
        &claim.claim_id,
        "unused",
        ClaimTombstoneReason::Withdrawn,
    );
    as_tombstone.operation_id = erased.clone();
    assert_eq!(
        store
            .tombstone(scope, &as_tombstone)
            .await
            .expect_err("an erased operation id replayed as a tombstone is refused")
            .code(),
        "claim-operation-erased"
    );
    let mut as_delete = delete_request(scope, &claim.claim_id, "unused", None);
    as_delete.operation_id = erased;
    assert_eq!(
        store
            .delete(scope, &as_delete)
            .await
            .expect_err("an erased operation id replayed as a delete is refused")
            .code(),
        "claim-operation-erased"
    );
    absent().await;
}

/// A delete that expects a state the claim is no longer in changes nothing and
/// spends nothing: the same operation id succeeds once the expectation holds.
pub async fn a_stale_delete_changes_nothing(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let claim = conformance_claim(scope, "sd-1", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &claim.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies");
    let before = stored(store, scope, &claim.claim_id).await;
    assert_eq!(before.transition_count(), 1);

    let unchanged = || async {
        assert_eq!(
            stored(store, scope, &claim.claim_id).await,
            before,
            "a stale delete leaves the claim exactly as it was"
        );
        assert_eq!(
            drained_transitions(store, scope, &claim.claim_id)
                .await
                .len(),
            1,
            "a stale delete leaves the history exactly as it was"
        );
    };

    // The transition count moved on from what the caller read.
    let mut request = delete_request(
        scope,
        &claim.claim_id,
        "erase",
        Some(ClaimDeleteExpectation {
            transition_count: 2,
            tombstoned: false,
        }),
    );
    assert_eq!(
        store
            .delete(scope, &request)
            .await
            .expect_err("a delete expecting another transition count is stale")
            .code(),
        "claim-delete-stale"
    );
    unchanged().await;

    // The tombstone state differs from what the caller read.
    request.expected = Some(ClaimDeleteExpectation {
        transition_count: 1,
        tombstoned: true,
    });
    assert_eq!(
        store
            .delete(scope, &request)
            .await
            .expect_err("a delete expecting a stub is stale against a live claim")
            .code(),
        "claim-delete-stale"
    );
    unchanged().await;

    // The refusals spent nothing: the same operation id, now with the state
    // the claim is actually in, deletes it.
    request.expected = Some(ClaimDeleteExpectation {
        transition_count: 1,
        tombstoned: false,
    });
    store
        .delete(scope, &request)
        .await
        .expect("the same operation id succeeds once the expectation holds");
    assert!(store
        .get(scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .is_none());

    // And the other direction: a live expectation is stale against a stub.
    let withdrawn = conformance_claim(scope, "sd-2", "a", "c");
    store
        .append(scope, &withdrawn)
        .await
        .expect("the claim appends");
    let stub = store
        .tombstone(
            scope,
            &tombstone_request(
                scope,
                &withdrawn.claim_id,
                "withdraw",
                ClaimTombstoneReason::Withdrawn,
            ),
        )
        .await
        .expect("the tombstone applies");
    assert_eq!(
        store
            .delete(
                scope,
                &delete_request(
                    scope,
                    &withdrawn.claim_id,
                    "erase",
                    Some(ClaimDeleteExpectation {
                        transition_count: 0,
                        tombstoned: false,
                    }),
                ),
            )
            .await
            .expect_err("a delete expecting a live claim is stale against its stub")
            .code(),
        "claim-delete-stale"
    );
    assert_eq!(
        stored(store, scope, &withdrawn.claim_id).await,
        stub,
        "the stub an operator made survives a delete that read the live claim"
    );
}

/// A delete under a foreign scope answers exactly as an absent claim does, and
/// removes nothing.
pub async fn delete_isolation(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let foreign = &scopes.foreign;
    let claim = conformance_claim(scope, "di-1", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");

    let foreign_refusal = store
        .delete(
            foreign,
            &delete_request(foreign, &claim.claim_id, "foreign", None),
        )
        .await
        .expect_err("a foreign delete is refused");
    let absent = conformance_claim(foreign, "di-absent", "x", "y");
    let absent_refusal = store
        .delete(
            foreign,
            &delete_request(foreign, &absent.claim_id, "absent", None),
        )
        .await
        .expect_err("an absent delete is refused");
    assert_eq!(foreign_refusal.code(), "claim-not-found");
    assert_eq!(foreign_refusal.code(), absent_refusal.code());

    assert_eq!(
        serde_json::to_vec(&stored(store, scope, &claim.claim_id).await)
            .expect("the claim serializes"),
        serde_json::to_vec(&claim).expect("the claim serializes"),
        "a foreign delete leaves the claim exactly as stored"
    );
}

/// An operation id reused across append, transition, tombstone and delete is a
/// conflict in every direction the erasure kinds add, and changes nothing.
pub async fn erasure_operation_kinds(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let ungated = ClaimPromotionPolicy::ungated();
    let now = AgentTimestampMillis::new(10);
    let conflict = "claim-operation-conflict";

    // A live claim with one transition: an append id and a transition id.
    let live = conformance_claim(scope, "ok-live", "a", "b");
    store.append(scope, &live).await.expect("the claim appends");
    let dispute = transition_request(scope, &live.claim_id, "dispute", ClaimTrustStatus::Disputed);
    store
        .transition(scope, &dispute, &ungated, now)
        .await
        .expect("the dispute applies");
    let live_before = stored(store, scope, &live.claim_id).await;

    // An append's id spent as a delete.
    let mut crossed = delete_request(scope, &live.claim_id, "unused", None);
    crossed.operation_id = live.operation_id.clone();
    assert_eq!(
        store
            .delete(scope, &crossed)
            .await
            .expect_err("a delete under a spent append operation id is refused")
            .code(),
        conflict
    );
    // A transition's id spent as a tombstone.
    let mut crossed = tombstone_request(
        scope,
        &live.claim_id,
        "unused",
        ClaimTombstoneReason::Withdrawn,
    );
    crossed.operation_id = dispute.operation_id.clone();
    assert_eq!(
        store
            .tombstone(scope, &crossed)
            .await
            .expect_err("a tombstone under a spent transition operation id is refused")
            .code(),
        conflict
    );
    assert_eq!(stored(store, scope, &live.claim_id).await, live_before);

    // A tombstone's id spent as a delete.
    let withdrawn = conformance_claim(scope, "ok-stub", "a", "c");
    store
        .append(scope, &withdrawn)
        .await
        .expect("the claim appends");
    let tombstone = tombstone_request(
        scope,
        &withdrawn.claim_id,
        "withdraw",
        ClaimTombstoneReason::Withdrawn,
    );
    let stub = store
        .tombstone(scope, &tombstone)
        .await
        .expect("the tombstone applies");
    let mut crossed = delete_request(scope, &withdrawn.claim_id, "unused", None);
    crossed.operation_id = tombstone.operation_id.clone();
    assert_eq!(
        store
            .delete(scope, &crossed)
            .await
            .expect_err("a delete under a spent tombstone operation id is refused")
            .code(),
        conflict
    );
    assert_eq!(stored(store, scope, &withdrawn.claim_id).await, stub);

    // A delete's id spent as an append, a transition and a tombstone.
    let gone = conformance_claim(scope, "ok-gone", "a", "d");
    store.append(scope, &gone).await.expect("the claim appends");
    let delete = delete_request(scope, &gone.claim_id, "erase", None);
    store
        .delete(scope, &delete)
        .await
        .expect("the delete applies");

    let squatter = Claim::new(
        scope,
        delete.operation_id.clone(),
        ClaimNodeId::new("x").expect("the node id is valid"),
        crate::claim::ClaimPredicate::new("links").expect("the predicate is valid"),
        ClaimObject::Node(ClaimNodeId::new("y").expect("the node id is valid")),
        crate::claim::ClaimProvenance::for_agent(
            AgentId::new("scout").expect("the agent id is valid"),
        ),
        5_000,
        rakka_agent::MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid");
    assert_eq!(
        store
            .append(scope, &squatter)
            .await
            .expect_err("an append under a spent delete operation id is refused")
            .code(),
        conflict
    );
    assert!(
        store
            .get(scope, &squatter.claim_id)
            .await
            .expect("the read answers")
            .is_none(),
        "a refused append stores nothing"
    );
    let mut crossed =
        transition_request(scope, &live.claim_id, "unused", ClaimTrustStatus::Retracted);
    crossed.operation_id = delete.operation_id.clone();
    assert_eq!(
        store
            .transition(scope, &crossed, &ungated, now)
            .await
            .expect_err("a transition under a spent delete operation id is refused")
            .code(),
        conflict
    );
    let mut crossed = tombstone_request(
        scope,
        &live.claim_id,
        "unused",
        ClaimTombstoneReason::Withdrawn,
    );
    crossed.operation_id = delete.operation_id.clone();
    assert_eq!(
        store
            .tombstone(scope, &crossed)
            .await
            .expect_err("a tombstone under a spent delete operation id is refused")
            .code(),
        conflict
    );
    assert_eq!(
        stored(store, scope, &live.claim_id).await,
        live_before,
        "no refused cross-kind operation moved the live claim"
    );
    assert_eq!(
        drained_transitions(store, scope, &live.claim_id)
            .await
            .len(),
        1
    );
}
