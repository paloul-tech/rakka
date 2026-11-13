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
use crate::tombstone::{ClaimTombstoneReason, ClaimTombstoneRequest, CLAIM_TOMBSTONE_MARKER};
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
