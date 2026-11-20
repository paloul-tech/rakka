//! The retention sweep and a claim whose stored history is damaged
//! (issue #138).
//!
//! A store writes a claim and its history together, so a claim whose
//! transitions disagree with its record is damage from outside the store.
//! The in-memory store cannot be damaged that way. The store here wraps it
//! and answers as a damaged backend does: for one claim it lists no
//! transitions, for another it lists one more than the record counts, and
//! for a third it refuses the delete `claim-history-damaged`.
//!
//! The sweep counts such a claim as `damaged`, leaves it, and goes on to the
//! claims behind it. It used to count the first as `moved` on every pass and
//! never report it, and to end the pass on the second's error.

use rakka_agent_knowledge_graph::conformance::{conformance_claim, conformance_resolver};
use rakka_agent_knowledge_graph::store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalReport, KnowledgeGraphCapabilities,
};
use rakka_agent_knowledge_graph::{
    discharge_knowledge_space_retention, Claim, ClaimDeleteRequest, ClaimError, ClaimFuture,
    ClaimId, ClaimOperationId, ClaimPromotionPolicy, ClaimTombstoneRequest, ClaimTransitionOutcome,
    ClaimTrustStatus, ClaimTrustTransitionRequest, InMemoryKnowledgeGraphStore,
    KnowledgeGraphStore, KnowledgeSpaceRetentionPolicy, KnowledgeSpaceScope,
    KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
};
use rakka_agent_workflow::AgentTimestampMillis;

/// The in-memory store, answering for two chosen claims as a backend whose
/// rows were damaged would.
#[derive(Default)]
struct DamagedStore {
    inner: InMemoryKnowledgeGraphStore,
    /// The claim whose transitions the store no longer holds.
    history_lost: Option<ClaimId>,
    /// The claim the store holds one transition too many for.
    history_extended: Option<ClaimId>,
    /// The claim whose delete the store refuses as damaged.
    delete_refused: Option<ClaimId>,
}

impl KnowledgeGraphStore for DamagedStore {
    fn backend_name(&self) -> &'static str {
        "in-memory-damaged"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core()
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.append(scope, claim)
    }

    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        self.inner.get(scope, claim_id)
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        self.inner.query(scope, filter, cursor)
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        self.inner.traverse(scope, traversal)
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        self.inner.transition(scope, request, policy, now)
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        if self.history_lost.as_ref() == Some(claim_id) {
            return Box::pin(async {
                Ok(ClaimTransitionPage {
                    transitions: Vec::new(),
                    next: None,
                })
            });
        }
        if self.history_extended.as_ref() == Some(claim_id) {
            return Box::pin(async move {
                let mut page = self.inner.transitions(scope, claim_id, cursor).await?;
                // One more entry lies behind the last one the record counts.
                let last = page
                    .transitions
                    .last()
                    .expect("the claim has a transition")
                    .ordinal;
                page.next = Some(ClaimTransitionCursor::after_ordinal(last));
                Ok(page)
            });
        }
        self.inner.transitions(scope, claim_id, cursor)
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTombstoneRequest,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.tombstone(scope, request)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimDeleteRequest,
    ) -> ClaimFuture<'a, ()> {
        if self.delete_refused.as_ref() == Some(&request.claim_id) {
            let claim_id = request.claim_id.clone();
            return Box::pin(async move { Err(ClaimError::HistoryDamaged { claim_id }) });
        }
        self.inner.delete(scope, request)
    }
}

fn scope(space: &str) -> KnowledgeSpaceScope {
    rakka_agent_knowledge_graph::conformance::ConformanceScopes::unique(space).primary
}

/// Every proposed and disputed claim is due at once.
fn policy() -> KnowledgeSpaceRetentionPolicy {
    KnowledgeSpaceRetentionPolicy::retain_all()
        .with_window(ClaimTrustStatus::Proposed, 0)
        .with_window(ClaimTrustStatus::Disputed, 0)
}

async fn appended(store: &DamagedStore, scope: &KnowledgeSpaceScope, name: &str) -> Claim {
    let claim = conformance_claim(scope, name, "subject", name);
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends")
}

async fn disputed(store: &DamagedStore, scope: &KnowledgeSpaceScope, name: &str) -> Claim {
    let claim = appended(store, scope, name).await;
    store
        .transition(
            scope,
            &ClaimTrustTransitionRequest::new(
                claim.claim_id.clone(),
                ClaimOperationId::derive_transition(scope, &claim.claim_id, "dispute")
                    .expect("the operation id derives"),
                ClaimTrustStatus::Disputed,
                conformance_resolver(),
                AgentTimestampMillis::new(10),
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies");
    claim
}

async fn sweep(
    store: &DamagedStore,
    scope: &KnowledgeSpaceScope,
) -> rakka_agent_knowledge_graph::KnowledgeSpaceRetentionReport {
    discharge_knowledge_space_retention(
        store,
        scope,
        &policy(),
        AgentTimestampMillis::new(1_000),
        None,
        KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
    )
    .await
    .expect("the sweep answers, and does not end on a damaged claim")
}

/// A claim that is aged by its last transition, whose store no longer holds
/// that transition, is reported as damaged and kept. The sweep does not call
/// it moved: read again, the claim is still there and unchanged, so nothing
/// moved. And it goes on to delete the healthy claim beside it.
#[tokio::test]
async fn a_claim_whose_last_transition_is_gone_is_reported_damaged() {
    let scope = scope("damaged-history-lost");
    let mut store = DamagedStore::default();
    let lost = disputed(&store, &scope, "lost").await;
    let healthy = disputed(&store, &scope, "healthy").await;
    store.history_lost = Some(lost.claim_id.clone());

    let report = sweep(&store, &scope).await;

    assert_eq!(
        (
            report.examined,
            report.damaged,
            report.deleted,
            report.moved
        ),
        (2, 1, 1, 0)
    );
    assert!(
        store
            .get(&scope, &lost.claim_id)
            .await
            .expect("the read answers")
            .is_some(),
        "the damaged claim is left as it is"
    );
    assert!(
        store
            .get(&scope, &healthy.claim_id)
            .await
            .expect("the read answers")
            .is_none(),
        "the healthy claim behind it is deleted"
    );

    // It is reported again by every pass that reaches it.
    let again = sweep(&store, &scope).await;
    assert_eq!((again.examined, again.damaged), (1, 1));
}

/// A claim the store holds one transition too many for is reported as
/// damaged too. The sweep reads that as "a later transition lies behind the
/// last one", which is also what a claim that moved answers; read again, the
/// claim is at the count the query read, so nothing moved.
#[tokio::test]
async fn a_claim_with_a_transition_its_record_does_not_count_is_reported_damaged() {
    let scope = scope("damaged-history-extended");
    let mut store = DamagedStore::default();
    let extended = disputed(&store, &scope, "extended").await;
    let healthy = disputed(&store, &scope, "healthy").await;
    store.history_extended = Some(extended.claim_id.clone());

    let report = sweep(&store, &scope).await;

    assert_eq!(
        (
            report.examined,
            report.damaged,
            report.deleted,
            report.moved
        ),
        (2, 1, 1, 0)
    );
    assert!(store
        .get(&scope, &extended.claim_id)
        .await
        .expect("the read answers")
        .is_some());
    assert!(store
        .get(&scope, &healthy.claim_id)
        .await
        .expect("the read answers")
        .is_none());
}

/// A claim whose delete the store refuses as damaged is reported as damaged
/// and kept, and the pass goes on. The error used to end the pass at that
/// claim, on every pass.
#[tokio::test]
async fn a_claim_whose_delete_is_refused_as_damaged_does_not_end_the_pass() {
    let scope = scope("damaged-delete-refused");
    let mut store = DamagedStore::default();
    let refused = appended(&store, &scope, "refused").await;
    let healthy = appended(&store, &scope, "healthy").await;
    store.delete_refused = Some(refused.claim_id.clone());

    let report = sweep(&store, &scope).await;

    assert_eq!(
        (
            report.examined,
            report.damaged,
            report.deleted,
            report.moved
        ),
        (2, 1, 1, 0)
    );
    assert!(store
        .get(&scope, &refused.claim_id)
        .await
        .expect("the read answers")
        .is_some());
    assert!(store
        .get(&scope, &healthy.claim_id)
        .await
        .expect("the read answers")
        .is_none());
}

/// A claim deleted between the sweep's two reads has no last transition
/// either, and that is no damage: read again, the claim is gone. It is
/// counted as moved.
#[tokio::test]
async fn a_claim_that_is_gone_by_the_second_read_is_moved_not_damaged() {
    let scope = scope("damaged-or-gone");
    let mut store = DamagedStore::default();
    let gone = disputed(&store, &scope, "gone").await;
    // The store lists the claim and holds neither its history nor, by the
    // time the sweep reads again, the claim itself.
    let listed = store
        .query(
            &scope,
            &ClaimFilter::matching_all(),
            ClaimCursor::start().with_limit(1),
        )
        .await
        .expect("the query answers");
    assert_eq!(listed.claims.len(), 1);
    store.history_lost = Some(gone.claim_id.clone());
    let report = discharge_knowledge_space_retention(
        &GoneOnReread { inner: store },
        &scope,
        &policy(),
        AgentTimestampMillis::new(1_000),
        None,
        KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
    )
    .await
    .expect("the sweep answers");

    assert_eq!((report.examined, report.moved, report.damaged), (1, 1, 0));
}

/// [`DamagedStore`], answering every `get` as if the claim had been deleted
/// since the query listed it.
struct GoneOnReread {
    inner: DamagedStore,
}

impl KnowledgeGraphStore for GoneOnReread {
    fn backend_name(&self) -> &'static str {
        "in-memory-gone-on-reread"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core()
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.append(scope, claim)
    }

    fn get<'a>(
        &'a self,
        _scope: &'a KnowledgeSpaceScope,
        _claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        Box::pin(async { Ok(None) })
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        self.inner.query(scope, filter, cursor)
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        self.inner.traverse(scope, traversal)
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        self.inner.transition(scope, request, policy, now)
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        self.inner.transitions(scope, claim_id, cursor)
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTombstoneRequest,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.tombstone(scope, request)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimDeleteRequest,
    ) -> ClaimFuture<'a, ()> {
        self.inner.delete(scope, request)
    }
}
