//! The backend conformance harness: one suite, every implementation.
//!
//! [Specification 18 scenario 20](../../../docs/plans/rakka-agent/spec.md):
//! every communal graph backend passes the same claim-identity, idempotent
//! append, provenance, trust-filtering, authorization, and bounded-query
//! conformance suite without changing agent-domain code. This module *is*
//! that suite: the in-memory reference implementation runs it in this crate's
//! integration tests, and a backend crate runs
//! [`check_knowledge_graph_contract`] against its own store, unchanged.
//!
//! Like `rakka_agent::testkit`, the module is an ordinary ungated `pub mod`:
//! test support is part of the crate's contract surface, not a feature.
//!
//! Every clause takes the store under test plus a [`ConformanceScopes`] value
//! of fresh scopes. Fresh *scopes*, not fresh stores, are what clause
//! isolation needs — a live-database backend cannot cheaply construct stores,
//! but every backend can serve one more tenant. Scopes are unique per clause,
//! per process, and per run against a persistent backend
//! ([`ConformanceScopes::unique`]); a suite that wants to own its own
//! namespacing uses [`ConformanceScopes::unique_in`] or pins
//! [`CONFORMANCE_RUN_ENV`]. Clauses panic on violation, so each runs inside a
//! test.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use rakka_agent::{
    AgentCheckpointGrant, AgentCheckpointKind, AgentContentDigest, AgentId, AgentRevisionNumber,
    AgentRunScope, KnowledgeSpaceId, MemoryClassification, TenantId,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

use crate::claim::{
    Claim, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimProvenance,
    ClaimTrustStatus, CLAIM_MAX_TRUST_TRANSITIONS,
};
use crate::error::ClaimError;
use crate::promotion::{claim_promotion_binding, ClaimPromotionEvidence, ClaimPromotionPolicy};
use crate::scope::KnowledgeSpaceScope;
use crate::store::{
    ClaimCursor, ClaimFilter, ClaimTransitionCursor, ClaimTraversal, KnowledgeGraphStore,
    CLAIM_PAGE_MAX_ENTRIES, CLAIM_TRAVERSAL_MAX_DEPTH,
};
use crate::transition::ClaimTrustTransitionRequest;

/// The two scopes a conformance clause works with: the scope under test, and
/// a foreign scope (different tenant *and* different space id) proving
/// isolation.
#[derive(Debug, Clone)]
pub struct ConformanceScopes {
    /// The scope the clause exercises.
    pub primary: KnowledgeSpaceScope,
    /// A scope that must never observe the primary's data.
    pub foreign: KnowledgeSpaceScope,
}

static CONFORMANCE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Number of hex digits of the run nonce that enter a tenant id. Sixty-four
/// bits of a cryptographic digest: two runs colliding is not a scenario a suite
/// needs to defend against, and the tenant stays readable.
const RUN_NONCE_HEX_DIGITS: usize = 16;

/// Environment variable a deployment may set to pin the run namespace, for a
/// suite that wants to inspect the rows a failing run left behind.
pub const CONFORMANCE_RUN_ENV: &str = "RAKKA_KNOWLEDGE_GRAPH_CONFORMANCE_RUN";

/// The namespace distinguishing this run's scopes from every other run's.
///
/// A sequence counter alone cannot do this: it is process-local and starts at
/// zero, so two test binaries — which `cargo test` runs concurrently — and two
/// runs against the same live database all mint identical tenants. Neither is
/// hypothetical for slice 2.4's database-backed suite.
///
/// The process id alone is not enough either (an operating system recycles it,
/// so a later run can inherit it), and a coarse clock alone can repeat, so both
/// enter one digest. [`CONFORMANCE_RUN_ENV`] overrides it when a deployment
/// wants a namespace it can find again.
fn run_namespace() -> &'static str {
    static RUN_NAMESPACE: OnceLock<String> = OnceLock::new();
    RUN_NAMESPACE.get_or_init(|| {
        if let Some(pinned) = std::env::var(CONFORMANCE_RUN_ENV)
            .ok()
            .filter(|pinned| !pinned.is_empty())
        {
            return pinned;
        }
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let seed = format!("{}|{elapsed}", std::process::id());
        AgentContentDigest::sha256_of_bytes(seed.as_bytes())
            .value
            .chars()
            .take(RUN_NONCE_HEX_DIGITS)
            .collect()
    })
}

impl ConformanceScopes {
    /// Fresh scopes for one clause run, in this run's own namespace.
    ///
    /// Distinct from every other clause in this process (a sequence counter),
    /// and from every other process and every earlier run against the same
    /// database (a per-run namespace digested from the process id and the
    /// wall clock, which [`CONFORMANCE_RUN_ENV`] pins when a deployment wants
    /// one it can find again). The label enters both tenants so a failure names
    /// the clause that produced it; it must satisfy the identity rules (no `/`,
    /// `|`, or control characters).
    #[must_use]
    pub fn unique(label: &str) -> Self {
        Self::unique_in(run_namespace(), label)
    }

    /// Fresh scopes for one clause run in an explicitly named namespace.
    ///
    /// For a suite that manages its own namespacing — a database-backed run
    /// that wants to drop everything it wrote afterwards, say. Successive calls
    /// still differ within a namespace: clause isolation is the sequence
    /// counter's job, and the namespace only separates runs from each other.
    #[must_use]
    pub fn unique_in(namespace: &str, label: &str) -> Self {
        let sequence = CONFORMANCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let tenant =
            |suffix: char| TenantId::new(format!("conf-{namespace}-{label}-{sequence}-{suffix}"));
        let primary = KnowledgeSpaceScope::new(
            tenant('a'),
            KnowledgeSpaceId::new("space-a").expect("the space id is valid"),
        )
        .expect("the conformance namespace and label satisfy the identity rules");
        let foreign = KnowledgeSpaceScope::new(
            tenant('b'),
            KnowledgeSpaceId::new("space-b").expect("the space id is valid"),
        )
        .expect("the conformance namespace and label satisfy the identity rules");
        Self { primary, foreign }
    }
}

/// A distinct valid claim for one scope, keyed by a per-clause discriminator.
///
/// The subject/object pair makes the claim an edge, so traversal clauses can
/// reuse it.
#[must_use]
pub fn conformance_claim(
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    subject: &str,
    object: &str,
) -> Claim {
    let operation_id =
        ClaimOperationId::derive_append(scope, discriminator).expect("the operation id derives");
    Claim::new(
        scope,
        operation_id,
        ClaimNodeId::new(subject).expect("the node id is valid"),
        ClaimPredicate::new("links").expect("the predicate is valid"),
        ClaimObject::Node(ClaimNodeId::new(object).expect("the node id is valid")),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
}

/// The resolving principal every conformance grant records.
#[must_use]
pub fn conformance_resolver() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "conformance-approver".to_string(),
        display_name: None,
    }
}

/// A checkpoint grant covering exactly this claim's next promotion, as a
/// resolved slice 1.10 checkpoint would issue it.
///
/// Test support for the promotion gate: the binding is derived through
/// [`claim_promotion_binding`], so the grant binds the same effect id,
/// generation (the claim's next history ordinal), target, and cryptographic
/// digest the gate recomputes at validation.
#[must_use]
pub fn promotion_grant_for(
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
    expires_at: AgentTimestampMillis,
    allowed_use_count: u32,
) -> AgentCheckpointGrant {
    let binding = claim_promotion_binding(scope, claim).expect("the binding derives");
    AgentCheckpointGrant {
        checkpoint_id: HumanCheckpointId::new(format!("ck-{}", claim.claim_id)),
        kind: AgentCheckpointKind::SecurityAuthorization,
        scope: AgentRunScope::new(
            scope.tenant().clone(),
            AgentId::new("verifier").expect("the agent id is valid"),
            rakka_agent::AgentRunId::new("verify-run").expect("the run id is valid"),
        )
        .expect("the run scope is valid"),
        task: None,
        goal: None,
        effect_id: binding.effect_id,
        generation: binding.generation,
        target: binding.target,
        argument_digest: binding.argument_digest,
        safety_class: binding.safety_class,
        settings_revision: AgentRevisionNumber::INITIAL,
        policy: None,
        credential_binding: None,
        resolver: conformance_resolver(),
        issued_at: AgentTimestampMillis::new(1),
        expires_at,
        allowed_use_count,
    }
}

/// A promotion request carrying the given evidence, at transition
/// discriminator `discriminator`.
fn promotion_request(
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
    discriminator: &str,
    evidence: Option<ClaimPromotionEvidence>,
) -> ClaimTrustTransitionRequest {
    let operation_id = ClaimOperationId::derive_transition(scope, &claim.claim_id, discriminator)
        .expect("the operation id derives");
    let mut request = ClaimTrustTransitionRequest::new(
        claim.claim_id.clone(),
        operation_id,
        ClaimTrustStatus::Verified,
        conformance_resolver(),
        AgentTimestampMillis::new(10),
    );
    if let Some(evidence) = evidence {
        request = request.with_promotion(evidence);
    }
    request
}

/// A non-promotion transition request.
fn transition_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    to: ClaimTrustStatus,
) -> ClaimTrustTransitionRequest {
    let operation_id = ClaimOperationId::derive_transition(scope, claim_id, discriminator)
        .expect("the operation id derives");
    ClaimTrustTransitionRequest::new(
        claim_id.clone(),
        operation_id,
        to,
        conformance_resolver(),
        AgentTimestampMillis::new(10),
    )
}

/// Every claim a filter admits, drained across pages.
///
/// A clause asserting on a *complete* answer must walk the cursor rather than
/// read one page: a backend may declare a page bound tighter than the crate cap
/// and serve it, so one page is not guaranteed to hold everything a filter
/// admits. Only [`bounded_queries`] — the clause about paging itself — inspects
/// pages directly.
async fn drained_query(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    filter: &ClaimFilter,
) -> Vec<Claim> {
    let mut drained = Vec::new();
    let mut cursor = ClaimCursor::start();
    loop {
        let page = store
            .query(scope, filter, cursor)
            .await
            .expect("the query answers");
        drained.extend(page.claims);
        match page.next {
            Some(next) => cursor = next,
            None => return drained,
        }
    }
}

/// Claim identity: derivation is stable, distinct operations yield distinct
/// claims, and a stored claim reads back exactly.
pub async fn claim_identity(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let first = conformance_claim(scope, "id-1", "a", "b");
    let again = conformance_claim(scope, "id-1", "a", "b");
    assert_eq!(
        first.claim_id, again.claim_id,
        "the same operation must derive the same claim id on any node"
    );
    let second = conformance_claim(scope, "id-2", "a", "b");
    assert_ne!(
        first.claim_id, second.claim_id,
        "distinct operations must derive distinct claims even for one statement"
    );

    store
        .append(scope, &first)
        .await
        .expect("the claim appends");
    store
        .append(scope, &second)
        .await
        .expect("the same statement under a distinct operation appends");
    let read = store
        .get(scope, &first.claim_id)
        .await
        .expect("the read answers")
        .expect("the stored claim exists");
    assert_eq!(
        serde_json::to_vec(&read).expect("the claim serializes"),
        serde_json::to_vec(&first).expect("the claim serializes"),
        "a stored claim must read back serialized-identical"
    );
}

/// Scenario 16 (graph half): a replayed append returns the original claim and
/// creates nothing, even after later operations moved the record.
pub async fn idempotent_append(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let claim = conformance_claim(scope, "replay-1", "a", "b");
    let stored = store
        .append(scope, &claim)
        .await
        .expect("the claim appends");

    let replayed = store.append(scope, &claim).await.expect("a replay answers");
    assert_eq!(
        serde_json::to_vec(&replayed).expect("the claim serializes"),
        serde_json::to_vec(&stored).expect("the claim serializes"),
        "a replayed append must return the original stored claim"
    );

    // An intervening transition moves the record; the replay still answers
    // the original.
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
    let replayed = store.append(scope, &claim).await.expect("a replay answers");
    assert_eq!(
        replayed.trust(),
        ClaimTrustStatus::Proposed,
        "a replay answers the original result, not the current record"
    );
    let current = store
        .get(scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .expect("the claim exists");
    assert_eq!(
        current.trust(),
        ClaimTrustStatus::Disputed,
        "the replay must not have rewound the current record"
    );

    let all = drained_query(store, scope, &ClaimFilter::matching_all()).await;
    assert_eq!(all.len(), 1, "a replay must not create a second claim");
}

/// Provenance: every dimension round-trips, and transitions preserve the
/// original provenance in an append-only, ordinal-ordered history.
pub async fn provenance_preservation(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    use rakka_agent::{AgentDelegationId, AgentGoalId, AgentTaskId};
    let scope = &scopes.primary;

    let operation_id =
        ClaimOperationId::derive_append(scope, "prov-1").expect("the operation id derives");
    let provenance =
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid"))
            .with_goal(AgentGoalId::new("goal-1").expect("the goal id is valid"))
            .with_task(AgentTaskId::new("task-1").expect("the task id is valid"))
            .with_run(rakka_agent::AgentRunId::new("run-1").expect("the run id is valid"))
            .with_delegation(
                AgentDelegationId::new("delegation-1").expect("the delegation id is valid"),
            )
            .with_effect(rakka_agent_workflow::AgentEffectId::new("effect-1"));
    let claim = Claim::new(
        scope,
        operation_id,
        ClaimNodeId::new("a").expect("the node id is valid"),
        ClaimPredicate::new("links").expect("the predicate is valid"),
        ClaimObject::Node(ClaimNodeId::new("b").expect("the node id is valid")),
        provenance.clone(),
        5_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");

    for (discriminator, to) in [
        ("dispute", ClaimTrustStatus::Disputed),
        ("retract", ClaimTrustStatus::Retracted),
    ] {
        store
            .transition(
                scope,
                &transition_request(scope, &claim.claim_id, discriminator, to),
                &ClaimPromotionPolicy::ungated(),
                AgentTimestampMillis::new(10),
            )
            .await
            .expect("the transition applies");
    }

    let read = store
        .get(scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .expect("a retracted claim stays readable");
    assert_eq!(
        read.provenance, provenance,
        "transitions must preserve the original provenance untouched"
    );
    assert_eq!(read.trust(), ClaimTrustStatus::Retracted);

    let history = store
        .transitions(scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(
        history.transitions.len(),
        2,
        "each transition appends exactly once"
    );
    assert_eq!(history.transitions[0].ordinal, 1);
    assert_eq!(history.transitions[0].from, ClaimTrustStatus::Proposed);
    assert_eq!(history.transitions[0].to, ClaimTrustStatus::Disputed);
    assert_eq!(history.transitions[1].ordinal, 2);
    assert_eq!(history.transitions[1].to, ClaimTrustStatus::Retracted);
}

/// Trust filtering: queries filter by trust state and provenance dimensions,
/// and default traversal excludes retracted edges.
pub async fn trust_filtering(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;

    // Four edge claims from one hub, walked into the four trust states.
    let mut by_state = Vec::new();
    for (index, target) in [
        (ClaimTrustStatus::Proposed, "n-proposed"),
        (ClaimTrustStatus::Verified, "n-verified"),
        (ClaimTrustStatus::Disputed, "n-disputed"),
        (ClaimTrustStatus::Retracted, "n-retracted"),
    ]
    .into_iter()
    .enumerate()
    {
        let (state, node) = target;
        let claim = conformance_claim(scope, &format!("tf-{index}"), "hub", node);
        store
            .append(scope, &claim)
            .await
            .expect("the claim appends");
        match state {
            ClaimTrustStatus::Proposed => {}
            ClaimTrustStatus::Verified => {
                store
                    .transition(
                        scope,
                        &promotion_request(scope, &claim, "promote", None),
                        &ClaimPromotionPolicy::ungated(),
                        AgentTimestampMillis::new(10),
                    )
                    .await
                    .expect("the ungated promotion applies");
            }
            ClaimTrustStatus::Disputed => {
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
            }
            ClaimTrustStatus::Retracted => {
                store
                    .transition(
                        scope,
                        &transition_request(
                            scope,
                            &claim.claim_id,
                            "retract",
                            ClaimTrustStatus::Retracted,
                        ),
                        &ClaimPromotionPolicy::ungated(),
                        AgentTimestampMillis::new(10),
                    )
                    .await
                    .expect("the retraction applies");
            }
        }
        by_state.push((state, claim.claim_id.clone()));
    }

    for (state, claim_id) in &by_state {
        let matching = drained_query(
            store,
            scope,
            &ClaimFilter::matching_all().with_trust(BTreeSet::from([*state])),
        )
        .await;
        assert_eq!(
            matching.len(),
            1,
            "exactly one claim holds trust state {}",
            state.as_label()
        );
        assert_eq!(&matching[0].claim_id, claim_id);
    }

    // Provenance filtering: everything here was asserted by "scout"; a
    // different agent matches nothing.
    let by_agent = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all()
            .with_agent(AgentId::new("scout").expect("the agent id is valid")),
    )
    .await;
    assert_eq!(by_agent.len(), 4);
    let by_other = drained_query(
        store,
        scope,
        &ClaimFilter::matching_all()
            .with_agent(AgentId::new("someone-else").expect("the agent id is valid")),
    )
    .await;
    assert!(by_other.is_empty());

    // Default traversal follows every state but Retracted.
    let report = store
        .traverse(
            scope,
            &ClaimTraversal::from_node(ClaimNodeId::new("hub").expect("the node id is valid")),
        )
        .await
        .expect("the traversal answers");
    let reached: Vec<&str> = report.nodes.iter().map(ClaimNodeId::as_str).collect();
    assert!(reached.contains(&"n-proposed"));
    assert!(reached.contains(&"n-verified"));
    assert!(reached.contains(&"n-disputed"));
    assert!(
        !reached.contains(&"n-retracted"),
        "a retracted edge must not extend default traversal"
    );
}

/// Open decision 3: an append is refused unless the claim is born `Proposed`
/// with zero transitions.
pub async fn born_proposed(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let claim = conformance_claim(scope, "bp-1", "a", "b");

    // A coherent — but non-Proposed — record forged through the mirror is
    // exactly what the append door must refuse.
    let forged = claim
        .apply_transition(ClaimTrustStatus::Verified)
        .expect("the forge walks the legal table");
    assert_eq!(
        store
            .append(scope, &forged)
            .await
            .expect_err("a non-proposed append is refused")
            .code(),
        "claim-append-not-proposed"
    );
}

/// Derived identity: an append is refused unless the claim carries the id its
/// own operation derives in the addressed scope, so no writer can squat the id
/// another writer's operation will produce.
pub async fn appended_identity_is_derived(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;

    // The id a later writer's "victim" operation will derive.
    let victim = conformance_claim(scope, "victim", "a", "b");
    // A squatter's own operation, wearing the victim's id — restorable through
    // the mirror, which is exactly why the append door must re-derive.
    let squatter = conformance_claim(scope, "squatter", "x", "y");
    let mut record = squatter.to_record();
    record.claim_id = victim.claim_id.clone();
    let squat = Claim::restore(record).expect("the mirror restores a foreign id");

    let refusal = store
        .append(scope, &squat)
        .await
        .expect_err("an underived claim id is refused");
    assert_eq!(refusal.code(), "claim-append-id-not-derived");

    // The refusal is not a partial write: the victim's own append still works,
    // and the squatter's operation id is still spendable by its own claim.
    store
        .append(scope, &victim)
        .await
        .expect("the squat left the victim's identity free");
    store
        .append(scope, &squatter)
        .await
        .expect("the squat spent nothing of its own operation");
}

/// Scenario 18 (graph half): an unauthorized scope learns nothing — reads
/// answer byte-identically to reading a genuinely empty space, and a foreign
/// write fails exactly as an absent claim does.
pub async fn authorization_isolation(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let foreign = &scopes.foreign;
    // A third scope nothing ever wrote to: the reference answer for "empty".
    let empty = ConformanceScopes::unique("isolation-empty").primary;

    let claim = conformance_claim(scope, "iso-1", "a", "b");
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

    // get: absent, not an error, not a hint.
    assert!(store
        .get(foreign, &claim.claim_id)
        .await
        .expect("the read answers")
        .is_none());

    // query, transitions, traverse: identical to the empty space, compared on
    // the whole answer value.
    let foreign_query = store
        .query(foreign, &ClaimFilter::matching_all(), ClaimCursor::start())
        .await
        .expect("the query answers");
    let empty_query = store
        .query(&empty, &ClaimFilter::matching_all(), ClaimCursor::start())
        .await
        .expect("the query answers");
    assert_eq!(
        foreign_query, empty_query,
        "a foreign query must answer exactly as an empty space does"
    );
    assert_eq!(format!("{foreign_query:?}"), format!("{empty_query:?}"));

    let foreign_history = store
        .transitions(foreign, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the listing answers");
    let empty_history = store
        .transitions(&empty, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the listing answers");
    assert_eq!(foreign_history, empty_history);

    let traversal = ClaimTraversal::from_node(ClaimNodeId::new("a").expect("the node id is valid"))
        .with_depth(CLAIM_TRAVERSAL_MAX_DEPTH);
    let foreign_report = store
        .traverse(foreign, &traversal)
        .await
        .expect("the traversal answers");
    let empty_report = store
        .traverse(&empty, &traversal)
        .await
        .expect("the traversal answers");
    assert_eq!(foreign_report, empty_report);
    assert!(
        foreign_report.nodes.is_empty(),
        "a foreign start node reaches nothing"
    );

    // A foreign write is refused with the exact refusal an absent claim
    // produces — code and shape.
    let foreign_refusal = store
        .transition(
            foreign,
            &transition_request(
                foreign,
                &claim.claim_id,
                "foreign",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect_err("a foreign transition is refused");
    let absent = conformance_claim(foreign, "iso-absent", "x", "y");
    let absent_refusal = store
        .transition(
            foreign,
            &transition_request(
                foreign,
                &absent.claim_id,
                "absent",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect_err("an absent transition is refused");
    assert_eq!(foreign_refusal.code(), "claim-not-found");
    assert_eq!(foreign_refusal.code(), absent_refusal.code());
}

/// Bounded queries: pages hold no more than their effective limit, and a full
/// cursor walk covers every claim exactly once.
///
/// The page expectations are stated against the *effective* limit — the smaller
/// of the request and the backend's declared
/// [`KnowledgeGraphCapabilities::max_page_entries`], which is the bound the SPI
/// obliges an implementation to serve. A backend declaring a tighter page bound
/// passes by honouring its declaration; one that serves more than it declares
/// fails.
///
/// [`KnowledgeGraphCapabilities::max_page_entries`]: crate::store::KnowledgeGraphCapabilities::max_page_entries
pub async fn bounded_queries(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let total = 7usize;
    for index in 0..total {
        store
            .append(
                scope,
                &conformance_claim(scope, &format!("bq-{index}"), "a", &format!("n-{index}")),
            )
            .await
            .expect("the claim appends");
    }
    let declared = store.capabilities().max_page_entries();

    let requested = 3;
    let effective = requested.min(declared);
    let mut seen = BTreeSet::new();
    let mut cursor = ClaimCursor::start().with_limit(requested);
    let mut pages = 0;
    loop {
        let page = store
            .query(scope, &ClaimFilter::matching_all(), cursor)
            .await
            .expect("the query answers");
        assert!(
            page.claims.len() <= effective,
            "a page never exceeds its effective limit of {effective}"
        );
        for claim in &page.claims {
            assert!(
                seen.insert(claim.claim_id.as_str().to_string()),
                "a cursor walk must never repeat a claim"
            );
        }
        pages += 1;
        assert!(pages <= total + 1, "a cursor walk must terminate");
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    assert_eq!(seen.len(), total, "a cursor walk must omit nothing");

    // An oversized request is clamped, not refused — to the declaration when
    // the backend has one tighter than the crate cap, to the cap otherwise.
    let clamped = store
        .query(
            scope,
            &ClaimFilter::matching_all(),
            ClaimCursor::start().with_limit(CLAIM_PAGE_MAX_ENTRIES + 100),
        )
        .await
        .expect("the query answers");
    assert!(
        clamped.claims.len() <= declared,
        "an oversized request is clamped to the declared bound of {declared}, not refused"
    );
    assert!(declared <= CLAIM_PAGE_MAX_ENTRIES);
}

/// Edges of the fixture chain reachable within *n* outbound hops of `root`,
/// indexed by *n*: nothing at zero, the two root edges at one, plus
/// `l1-a -> l2-a` at two, plus `l2-a -> l3-a` at three.
const FIXTURE_EDGES_WITHIN_DEPTH: [usize; 4] = [0, 2, 3, 4];

/// Outbound hops needed to exhaust the fixture chain from `root`. Past this
/// depth the reachable set no longer grows, so nothing is left to truncate.
const FIXTURE_CHAIN_DEPTH: u32 = 3;

/// Bounded traversal: depth, node, and edge budgets cut explicitly, and the
/// report is deterministic.
///
/// The depth expectations are stated against the *effective* depth — the
/// smaller of the request and the backend's declared
/// [`KnowledgeGraphCapabilities::max_traversal_depth`], which is the bound the
/// SPI obliges an implementation to serve. A backend declaring a tighter depth
/// must therefore pass this clause by honouring its declaration, and a backend
/// exceeding its own declaration fails it.
///
/// [`KnowledgeGraphCapabilities::max_traversal_depth`]: crate::store::KnowledgeGraphCapabilities::max_traversal_depth
pub async fn bounded_traversal(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    // A three-level chain with a fan-out at the root.
    for (discriminator, from, to) in [
        ("bt-1", "root", "l1-a"),
        ("bt-2", "root", "l1-b"),
        ("bt-3", "l1-a", "l2-a"),
        ("bt-4", "l2-a", "l3-a"),
    ] {
        store
            .append(scope, &conformance_claim(scope, discriminator, from, to))
            .await
            .expect("the edge appends");
    }
    let root = ClaimNodeId::new("root").expect("the node id is valid");

    // Depth one is always servable: the declared bound is floored at one.
    let shallow = store
        .traverse(scope, &ClaimTraversal::from_node(root.clone()))
        .await
        .expect("the traversal answers");
    assert_eq!(
        shallow.edges.len(),
        FIXTURE_EDGES_WITHIN_DEPTH[1],
        "depth one follows only the root's edges"
    );
    assert!(
        shallow.truncated,
        "a depth cut with work remaining is explicit"
    );

    // Ask for the crate cap; the backend's declaration is what it must serve.
    let effective_depth = store
        .capabilities()
        .max_traversal_depth()
        .min(CLAIM_TRAVERSAL_MAX_DEPTH);
    let reachable =
        FIXTURE_EDGES_WITHIN_DEPTH[usize::try_from(effective_depth.min(FIXTURE_CHAIN_DEPTH))
            .expect("a depth within the chain indexes the fixture table")];
    let full = store
        .traverse(
            scope,
            &ClaimTraversal::from_node(root.clone()).with_depth(CLAIM_TRAVERSAL_MAX_DEPTH),
        )
        .await
        .expect("the traversal answers");
    assert_eq!(
        full.edges.len(),
        reachable,
        "a traversal follows exactly the edges within its effective depth of {effective_depth}"
    );
    assert_eq!(
        full.truncated,
        effective_depth < FIXTURE_CHAIN_DEPTH,
        "truncation is set exactly when the effective depth of {effective_depth} leaves \
         reachable work"
    );
    let again = store
        .traverse(
            scope,
            &ClaimTraversal::from_node(root.clone()).with_depth(CLAIM_TRAVERSAL_MAX_DEPTH),
        )
        .await
        .expect("the traversal answers");
    assert_eq!(full, again, "a traversal is deterministic");

    let node_starved = store
        .traverse(
            scope,
            &ClaimTraversal::from_node(root.clone())
                .with_depth(CLAIM_TRAVERSAL_MAX_DEPTH)
                .with_node_budget(2),
        )
        .await
        .expect("the traversal answers");
    assert!(node_starved.truncated);
    assert_eq!(node_starved.nodes.len(), 2);

    let edge_starved = store
        .traverse(
            scope,
            &ClaimTraversal::from_node(root)
                .with_depth(CLAIM_TRAVERSAL_MAX_DEPTH)
                .with_edge_budget(1),
        )
        .await
        .expect("the traversal answers");
    assert!(edge_starved.truncated);
    assert_eq!(edge_starved.edges.len(), 1);
}

/// The transition table end to end: every legal pair applies, every illegal
/// pair refuses, the history bound refuses explicitly, and replays answer
/// their original outcome.
pub async fn transition_legality_and_replay(
    store: &dyn KnowledgeGraphStore,
    scopes: ConformanceScopes,
) {
    let scope = &scopes.primary;
    let ungated = ClaimPromotionPolicy::ungated();
    let now = AgentTimestampMillis::new(10);

    // Every (from, to) pair, from a claim walked into `from`.
    for (index, from) in ClaimTrustStatus::ALL.into_iter().enumerate() {
        for (jndex, to) in ClaimTrustStatus::ALL.into_iter().enumerate() {
            let claim = conformance_claim(scope, &format!("tl-{index}-{jndex}"), "a", "b");
            store
                .append(scope, &claim)
                .await
                .expect("the claim appends");
            if from != ClaimTrustStatus::Proposed {
                store
                    .transition(
                        scope,
                        &transition_request(scope, &claim.claim_id, "walk", from),
                        &ungated,
                        now,
                    )
                    .await
                    .expect("the walk into the source state applies");
            }
            let attempt = store
                .transition(
                    scope,
                    &transition_request(scope, &claim.claim_id, "attempt", to),
                    &ungated,
                    now,
                )
                .await;
            if from.may_transition_to(to) {
                let outcome = attempt.expect("a legal transition applies");
                assert_eq!(outcome.claim.trust(), to);
                assert_eq!(outcome.transition.from, from);
            } else {
                assert_eq!(
                    attempt.expect_err("an illegal transition refuses").code(),
                    "claim-transition-illegal"
                );
            }
        }
    }

    // A replayed transition answers its original outcome and appends nothing.
    let claim = conformance_claim(scope, "tl-replay", "a", "b");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let request = transition_request(
        scope,
        &claim.claim_id,
        "dispute",
        ClaimTrustStatus::Disputed,
    );
    let first = store
        .transition(scope, &request, &ungated, now)
        .await
        .expect("the dispute applies");
    let replayed = store
        .transition(scope, &request, &ungated, now)
        .await
        .expect("a replay answers");
    assert_eq!(replayed, first, "a replay answers the original outcome");
    let history = store
        .transitions(scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(history.transitions.len(), 1, "a replay appends nothing");

    // An operation id reused across kinds is a conflict, in both directions.
    let cross = store
        .append(scope, &{
            let operation_id = request.operation_id.clone();
            Claim::new(
                scope,
                operation_id,
                ClaimNodeId::new("x").expect("the node id is valid"),
                ClaimPredicate::new("links").expect("the predicate is valid"),
                ClaimObject::Node(ClaimNodeId::new("y").expect("the node id is valid")),
                ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
                5_000,
                MemoryClassification::Unclassified,
                AgentTimestampMillis::new(1),
            )
            .expect("the claim is valid")
        })
        .await
        .expect_err("an append under a spent transition operation id is refused");
    assert_eq!(cross.code(), "claim-operation-conflict");
    let mut cross_request = transition_request(
        scope,
        &claim.claim_id,
        "unused",
        ClaimTrustStatus::Retracted,
    );
    cross_request.operation_id = claim.operation_id.clone();
    assert_eq!(
        store
            .transition(scope, &cross_request, &ungated, now)
            .await
            .expect_err("a transition under a spent append operation id is refused")
            .code(),
        "claim-operation-conflict"
    );

    // The bounded history refuses explicitly at the cap.
    let oscillating = conformance_claim(scope, "tl-cap", "a", "b");
    store
        .append(scope, &oscillating)
        .await
        .expect("the claim appends");
    for step in 0..CLAIM_MAX_TRUST_TRANSITIONS {
        let to = if step % 2 == 0 {
            ClaimTrustStatus::Disputed
        } else {
            ClaimTrustStatus::Verified
        };
        store
            .transition(
                scope,
                &transition_request(scope, &oscillating.claim_id, &format!("osc-{step}"), to),
                &ungated,
                now,
            )
            .await
            .expect("the oscillation applies within the bound");
    }
    assert_eq!(
        store
            .transition(
                scope,
                &transition_request(
                    scope,
                    &oscillating.claim_id,
                    "over-cap",
                    ClaimTrustStatus::Retracted,
                ),
                &ungated,
                now,
            )
            .await
            .expect_err("a full history refuses explicitly")
            .code(),
        "claim-transition-history-full"
    );
}

/// The promotion gate: the default policy fails closed, a valid grant
/// promotes and stamps the audit receipt, and the refusal codes are stable.
pub async fn promotion_gate(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes) {
    let scope = &scopes.primary;
    let now = AgentTimestampMillis::new(10);

    // The default policy refuses an ungated promotion.
    let gated = conformance_claim(scope, "pg-1", "a", "b");
    store
        .append(scope, &gated)
        .await
        .expect("the claim appends");
    assert_eq!(
        store
            .transition(
                scope,
                &promotion_request(scope, &gated, "bare", None),
                &ClaimPromotionPolicy::default(),
                now,
            )
            .await
            .expect_err("an ungated promotion is refused")
            .code(),
        "claim-promotion-grant-required"
    );

    // A valid grant promotes, and the transition carries the receipt.
    let grant = promotion_grant_for(scope, &gated, AgentTimestampMillis::new(1_000), 1);
    let outcome = store
        .transition(
            scope,
            &promotion_request(
                scope,
                &gated,
                "granted",
                Some(ClaimPromotionEvidence { grant }),
            ),
            &ClaimPromotionPolicy::default(),
            now,
        )
        .await
        .expect("a granted promotion applies");
    assert_eq!(outcome.claim.trust(), ClaimTrustStatus::Verified);
    let receipt = outcome
        .transition
        .gate
        .as_ref()
        .expect("a gated promotion stamps its receipt");
    assert_eq!(receipt.resolver, conformance_resolver());
    assert!(receipt.argument_digest.algorithm.is_cryptographic());

    // An expired grant is refused with the checkpoint vocabulary preserved.
    let expiring = conformance_claim(scope, "pg-2", "a", "b");
    store
        .append(scope, &expiring)
        .await
        .expect("the claim appends");
    let stale = promotion_grant_for(scope, &expiring, AgentTimestampMillis::new(5), 1);
    let refusal = store
        .transition(
            scope,
            &promotion_request(
                scope,
                &expiring,
                "expired",
                Some(ClaimPromotionEvidence { grant: stale }),
            ),
            &ClaimPromotionPolicy::default(),
            now,
        )
        .await
        .expect_err("an expired grant is refused");
    assert_eq!(refusal.code(), "claim-promotion-grant-rejected");
    match refusal {
        ClaimError::PromotionGrantRejected { reason, .. } => {
            assert_eq!(reason.code(), "checkpoint-grant-expired");
        }
        other => panic!("unexpected refusal shape: {other:?}"),
    }
}

/// The capability report is coherent: declared limits stay within the crate
/// caps, and an implementation never reports a capability this crate does not
/// define.
pub async fn capability_report_coherence(store: &dyn KnowledgeGraphStore) {
    let capabilities = store.capabilities();
    assert!(capabilities.max_traversal_depth() >= 1);
    assert!(capabilities.max_traversal_depth() <= CLAIM_TRAVERSAL_MAX_DEPTH);
    assert!(capabilities.max_page_entries() >= 1);
    assert!(capabilities.max_page_entries() <= CLAIM_PAGE_MAX_ENTRIES);
    assert!(!store.backend_name().is_empty());
}

/// Runs every conformance clause with fresh unique scopes.
///
/// Slice 2.4 runs exactly this against a second, structurally different
/// backend — unchanged (scenario 20).
pub async fn check_knowledge_graph_contract(store: &dyn KnowledgeGraphStore) {
    claim_identity(store, ConformanceScopes::unique("identity")).await;
    idempotent_append(store, ConformanceScopes::unique("idempotent")).await;
    provenance_preservation(store, ConformanceScopes::unique("provenance")).await;
    trust_filtering(store, ConformanceScopes::unique("trust")).await;
    born_proposed(store, ConformanceScopes::unique("born-proposed")).await;
    appended_identity_is_derived(store, ConformanceScopes::unique("derived-identity")).await;
    authorization_isolation(store, ConformanceScopes::unique("isolation")).await;
    bounded_queries(store, ConformanceScopes::unique("bounded-queries")).await;
    bounded_traversal(store, ConformanceScopes::unique("bounded-traversal")).await;
    transition_legality_and_replay(store, ConformanceScopes::unique("legality")).await;
    promotion_gate(store, ConformanceScopes::unique("gate")).await;
    capability_report_coherence(store).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_are_distinct_per_clause_and_per_namespace() {
        // Within a namespace, successive calls differ: clause isolation.
        let first = ConformanceScopes::unique_in("run-1", "clause");
        let second = ConformanceScopes::unique_in("run-1", "clause");
        assert_ne!(first.primary, second.primary);
        // The primary and foreign scopes of one call differ in both segments.
        assert_ne!(first.primary.tenant(), first.foreign.tenant());
        assert_ne!(first.primary.space(), first.foreign.space());

        // Across namespaces, nothing is shared even at the same sequence
        // position — the property a second run against a live database needs.
        let other = ConformanceScopes::unique_in("run-2", "clause");
        assert!(!other.primary.tenant().as_str().contains("-run-1-"));
        assert_ne!(other.primary, first.primary);
    }

    #[test]
    fn the_run_namespace_is_stable_within_a_process_and_not_a_counter() {
        // Stable: every clause in one run shares it, so a suite can find its
        // own rows. Not a counter: it cannot be reproduced by a later run.
        assert_eq!(run_namespace(), run_namespace());
        assert!(!run_namespace().is_empty());
        // The default namespace is the digest nonce, not the process id alone.
        if std::env::var(CONFORMANCE_RUN_ENV).is_err() {
            assert_eq!(run_namespace().len(), RUN_NONCE_HEX_DIGITS);
            assert!(run_namespace().chars().all(|c| c.is_ascii_hexdigit()));
            assert_ne!(run_namespace(), std::process::id().to_string());
        }
        // The namespace reaches the scopes it names.
        let scopes = ConformanceScopes::unique("namespaced");
        assert!(scopes.primary.tenant().as_str().contains(run_namespace()));
    }
}
