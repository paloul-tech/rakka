//! The retention sweep and a claim whose stored history is damaged
//! (issue #138).
//!
//! A store writes a claim and its history together, so a claim whose
//! transitions disagree with its record is damage from outside the store.
//! The in-memory store cannot be damaged that way. The store here wraps it
//! and answers as a damaged backend does, for the claims it is told to.
//!
//! The sweep counts such a claim as `damaged`, leaves it, and goes on to the
//! claims behind it. It used to count a claim missing its last transition as
//! `moved` on every pass and never report it, and to end the pass on a
//! delete the store refused.
//!
//! The sweep walks a space in claim-id order, and a claim's id derives from
//! a scope that is new on every run. Each test therefore gives its roles out
//! by that order: the damaged claims are the ones the sweep reaches first.

use std::sync::Mutex;

use rakka_agent_knowledge_graph::conformance::{
    conformance_claim, conformance_resolver, ConformanceScopes,
};
use rakka_agent_knowledge_graph::store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalReport, KnowledgeGraphCapabilities,
};
use rakka_agent_knowledge_graph::{
    discharge_knowledge_space_retention, Claim, ClaimDeleteRequest, ClaimError, ClaimFuture,
    ClaimId, ClaimOperationId, ClaimPromotionPolicy, ClaimTombstoneReason, ClaimTombstoneRequest,
    ClaimTransitionOutcome, ClaimTrustStatus, ClaimTrustTransition, ClaimTrustTransitionRequest,
    InMemoryKnowledgeGraphStore, KnowledgeGraphStore, KnowledgeSpaceRetentionPolicy,
    KnowledgeSpaceRetentionReport, KnowledgeSpaceScope,
    KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
};
use rakka_agent_workflow::AgentTimestampMillis;

/// How the store answers for the history of one claim.
enum Damage {
    /// It holds none of the claim's transitions.
    Lost,
    /// It holds an entry behind the claim's last one.
    Extended,
    /// Where the claim's last transition should be, it holds one of the
    /// next ordinal: the last is gone and a later one is there.
    Shifted,
    /// It holds this transition for a claim whose record counts none.
    Stray(Box<ClaimTrustTransition>),
}

/// The in-memory store, answering for chosen claims as a backend whose rows
/// were damaged would.
#[derive(Default)]
struct DamagedStore {
    inner: InMemoryKnowledgeGraphStore,
    /// The claims whose history is damaged, and how.
    histories: Vec<(ClaimId, Damage)>,
    /// The claim whose delete the store refuses as damaged.
    delete_refused: Option<ClaimId>,
    /// Every claim a delete was asked for, refused or not.
    deletes_asked: Mutex<Vec<ClaimId>>,
}

impl DamagedStore {
    fn damage(&mut self, claim: &Claim, damage: Damage) {
        self.histories.push((claim.claim_id.clone(), damage));
    }

    fn delete_was_asked_for(&self, claim: &Claim) -> bool {
        self.deletes_asked
            .lock()
            .expect("the list is not poisoned")
            .contains(&claim.claim_id)
    }
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
        let Some((_, damage)) = self.histories.iter().find(|(id, _)| id == claim_id) else {
            return self.inner.transitions(scope, claim_id, cursor);
        };
        Box::pin(async move {
            let mut page = self.inner.transitions(scope, claim_id, cursor).await?;
            match damage {
                Damage::Lost => page.transitions.clear(),
                Damage::Extended => {
                    let last = page
                        .transitions
                        .last()
                        .expect("the claim has a transition")
                        .ordinal;
                    page.next = Some(ClaimTransitionCursor::after_ordinal(last));
                }
                Damage::Shifted => {
                    let last = page
                        .transitions
                        .last_mut()
                        .expect("the claim has a transition");
                    last.ordinal += 1;
                }
                Damage::Stray(transition) => page.transitions.push((**transition).clone()),
            }
            Ok(page)
        })
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
        self.deletes_asked
            .lock()
            .expect("the list is not poisoned")
            .push(request.claim_id.clone());
        if self.delete_refused.as_ref() == Some(&request.claim_id) {
            let claim_id = request.claim_id.clone();
            return Box::pin(async move { Err(ClaimError::HistoryDamaged { claim_id }) });
        }
        self.inner.delete(scope, request)
    }
}

fn scope(space: &str) -> KnowledgeSpaceScope {
    ConformanceScopes::unique(space).primary
}

/// Every proposed and disputed claim, and every stub, is due at once.
fn due_at_once() -> KnowledgeSpaceRetentionPolicy {
    KnowledgeSpaceRetentionPolicy::retain_all()
        .with_window(ClaimTrustStatus::Proposed, 0)
        .with_window(ClaimTrustStatus::Disputed, 0)
        .with_tombstone_window(0)
}

async fn appended(store: &DamagedStore, scope: &KnowledgeSpaceScope, name: &str) -> Claim {
    let claim = conformance_claim(scope, name, "subject", name);
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends")
}

async fn dispute(
    store: &DamagedStore,
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
) -> ClaimTrustTransition {
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
        .expect("the dispute applies")
        .transition
}

/// `count` claims, appended alike, in the order the sweep reaches them.
/// Each is disputed where `disputed` says so.
async fn claims_in_sweep_order(
    store: &DamagedStore,
    scope: &KnowledgeSpaceScope,
    count: usize,
    disputed: bool,
) -> Vec<Claim> {
    let mut claims = Vec::new();
    for index in 0..count {
        let claim = appended(store, scope, &format!("claim-{index}")).await;
        if disputed {
            dispute(store, scope, &claim).await;
        }
        claims.push(claim);
    }
    claims.sort_by(|a, b| a.claim_id.as_str().cmp(b.claim_id.as_str()));
    claims
}

async fn sweep_under(
    store: &DamagedStore,
    scope: &KnowledgeSpaceScope,
    policy: &KnowledgeSpaceRetentionPolicy,
) -> KnowledgeSpaceRetentionReport {
    discharge_knowledge_space_retention(
        store,
        scope,
        policy,
        AgentTimestampMillis::new(1_000),
        None,
        KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
    )
    .await
    .expect("the sweep answers, and does not end on a damaged claim")
}

async fn sweep(store: &DamagedStore, scope: &KnowledgeSpaceScope) -> KnowledgeSpaceRetentionReport {
    sweep_under(store, scope, &due_at_once()).await
}

/// What a pass counted: examined, damaged, deleted, moved.
fn counts(report: &KnowledgeSpaceRetentionReport) -> (usize, usize, usize, usize) {
    (
        report.examined,
        report.damaged,
        report.deleted,
        report.moved,
    )
}

async fn stored(store: &DamagedStore, scope: &KnowledgeSpaceScope, claim: &Claim) -> bool {
    store
        .get(scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .is_some()
}

/// Sweeps a space of two disputed claims, the first of which the sweep
/// reaches has its history damaged as `damage` says. The damaged claim is
/// counted and kept with no delete asked for it, the healthy claim behind it
/// is deleted, and the next pass counts the damaged one again.
async fn assert_the_sweep_counts_and_leaves(damage: Damage, space: &str) {
    let scope = scope(space);
    let mut store = DamagedStore::default();
    let claims = claims_in_sweep_order(&store, &scope, 2, true).await;
    let (damaged, healthy) = (&claims[0], &claims[1]);
    store.damage(damaged, damage);

    let report = sweep(&store, &scope).await;

    assert_eq!(counts(&report), (2, 1, 1, 0));
    assert!(
        stored(&store, &scope, damaged).await,
        "the damaged claim is left as it is"
    );
    assert!(
        !store.delete_was_asked_for(damaged),
        "the sweep attempts no delete for a claim whose history it cannot read cleanly"
    );
    assert!(
        !stored(&store, &scope, healthy).await,
        "the healthy claim behind it is deleted"
    );
    let again = sweep(&store, &scope).await;
    assert_eq!(counts(&again), (1, 1, 0, 0), "and it is counted again");
}

/// A claim that is aged by its last transition, whose store no longer holds
/// that transition, is reported as damaged and kept. The sweep does not call
/// it moved: read again, the claim is still at the count the query read.
#[tokio::test]
async fn a_claim_whose_last_transition_is_gone_is_reported_damaged() {
    assert_the_sweep_counts_and_leaves(Damage::Lost, "damaged-history-lost").await;
}

/// A claim the store holds one transition too many for is reported as
/// damaged too. "A later transition lies behind the last one" is also what a
/// claim that moved answers; read again, this one is at the count the query
/// read, so nothing moved.
#[tokio::test]
async fn a_claim_with_an_entry_behind_its_last_transition_is_reported_damaged() {
    assert_the_sweep_counts_and_leaves(Damage::Extended, "damaged-history-extended").await;
}

/// The entry the sweep reads must be the one of the claim's count. A claim
/// whose last transition is gone, with a later one stored, answers one entry
/// and nothing behind it, and that entry is not its last.
#[tokio::test]
async fn a_transition_of_another_ordinal_is_not_the_claims_last() {
    assert_the_sweep_counts_and_leaves(Damage::Shifted, "damaged-history-shifted").await;
}

/// A claim whose delete the store refuses as damaged is reported as damaged
/// and kept, and the pass goes on. The error used to end the pass at that
/// claim, on every pass.
#[tokio::test]
async fn a_claim_whose_delete_is_refused_as_damaged_does_not_end_the_pass() {
    let scope = scope("damaged-delete-refused");
    let mut store = DamagedStore::default();
    let claims = claims_in_sweep_order(&store, &scope, 2, false).await;
    let (refused, healthy) = (&claims[0], &claims[1]);
    store.delete_refused = Some(refused.claim_id.clone());

    let report = sweep(&store, &scope).await;

    assert_eq!(counts(&report), (2, 1, 1, 0));
    assert!(store.delete_was_asked_for(refused));
    assert!(stored(&store, &scope, refused).await);
    assert!(!stored(&store, &scope, healthy).await);
}

/// A proposed claim is aged by its own record, and a proposed claim counts
/// no transition. One the store holds a transition for is not deleted on its
/// record's word: that transition may be the claim's real, later history,
/// under a record restored to an earlier state. The sweep reads the end of
/// its history before the delete, counts it as damaged, and asks for no
/// delete, which a store that erases damaged claims would carry out.
#[tokio::test]
async fn a_due_proposed_claim_with_a_stored_transition_is_damaged_and_not_deleted() {
    let scope = scope("damaged-proposed-stray");
    let mut store = DamagedStore::default();
    let donor = appended(&store, &scope, "donor").await;
    let stray = dispute(&store, &scope, &donor).await;
    let claims = claims_in_sweep_order(&store, &scope, 2, false).await;
    let (damaged, healthy) = (&claims[0], &claims[1]);
    store.damage(damaged, Damage::Stray(Box::new(stray)));

    // Only proposed claims have a window here: the donor is retained.
    let policy =
        KnowledgeSpaceRetentionPolicy::retain_all().with_window(ClaimTrustStatus::Proposed, 0);
    let report = sweep_under(&store, &scope, &policy).await;

    assert_eq!(counts(&report), (3, 1, 1, 0));
    assert_eq!(report.retained, 1);
    assert!(stored(&store, &scope, damaged).await);
    assert!(
        !store.delete_was_asked_for(damaged),
        "no delete is asked for on the word of a record its history contradicts"
    );
    assert!(!stored(&store, &scope, healthy).await);
}

/// The same for a stub, which is aged by its tombstone: one with an entry
/// behind the history its record counts is damaged, and is not deleted.
#[tokio::test]
async fn a_due_stub_with_an_entry_behind_its_history_is_damaged_and_not_deleted() {
    let scope = scope("damaged-stub-extended");
    let mut store = DamagedStore::default();
    let claims = claims_in_sweep_order(&store, &scope, 2, true).await;
    let (damaged, healthy) = (&claims[0], &claims[1]);
    for claim in &claims {
        store
            .tombstone(
                &scope,
                &ClaimTombstoneRequest {
                    claim_id: claim.claim_id.clone(),
                    operation_id: ClaimOperationId::derive_tombstone(
                        &scope,
                        &claim.claim_id,
                        "withdraw",
                    )
                    .expect("the operation id derives"),
                    reason: ClaimTombstoneReason::Withdrawn,
                    actor: conformance_resolver(),
                    tombstoned_at: AgentTimestampMillis::new(20),
                },
            )
            .await
            .expect("the tombstone applies");
    }
    store.damage(damaged, Damage::Extended);

    let report = sweep(&store, &scope).await;

    assert_eq!(counts(&report), (2, 1, 1, 0));
    assert!(stored(&store, &scope, damaged).await);
    assert!(!store.delete_was_asked_for(damaged));
    assert!(!stored(&store, &scope, healthy).await);
}

/// The sweep is no scan for damage. It reports a damaged claim where it
/// reads the claim's history or has its delete refused, and nowhere else: a
/// claim in a status with no window is retained unread, and one whose delete
/// would be refused is not asked for a delete before it is due. A `damaged`
/// of zero does not say the space holds no damaged claim.
#[tokio::test]
async fn the_sweep_reports_damage_only_where_it_reads_or_deletes() {
    let scope = scope("damaged-unreported");
    let mut store = DamagedStore::default();
    let unread = appended(&store, &scope, "unread").await;
    dispute(&store, &scope, &unread).await;
    store.damage(&unread, Damage::Lost);
    let not_due = appended(&store, &scope, "not-due").await;
    store.delete_refused = Some(not_due.claim_id.clone());

    // Disputed claims have no window. Proposed ones are due at 2,001.
    let policy =
        KnowledgeSpaceRetentionPolicy::retain_all().with_window(ClaimTrustStatus::Proposed, 2_000);
    let report = sweep_under(&store, &scope, &policy).await;

    assert_eq!(
        (
            report.examined,
            report.retained,
            report.not_yet_due,
            report.damaged
        ),
        (2, 1, 1, 0)
    );
    assert!(!store.delete_was_asked_for(&not_due));
}

/// A pass bounded to one claim a call resumes past damaged claims: the
/// cursor moves over each, and the calls together examine every claim once.
#[tokio::test]
async fn a_bounded_pass_resumes_past_damaged_claims() {
    let scope = scope("damaged-bounded");
    let mut store = DamagedStore::default();
    let claims = claims_in_sweep_order(&store, &scope, 4, true).await;
    store.damage(&claims[0], Damage::Lost);
    store.damage(&claims[2], Damage::Extended);

    let mut cursor = None;
    let mut calls = 0;
    let mut total = (0, 0, 0, 0);
    loop {
        let report = discharge_knowledge_space_retention(
            &store,
            &scope,
            &due_at_once(),
            AgentTimestampMillis::new(1_000),
            cursor,
            1,
        )
        .await
        .expect("the call answers");
        calls += 1;
        let counted = counts(&report);
        assert_eq!(counted.0, 1, "call {calls} examines one claim");
        total = (
            total.0 + counted.0,
            total.1 + counted.1,
            total.2 + counted.2,
            total.3 + counted.3,
        );
        cursor = report.next;
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!((calls, total), (4, (4, 2, 2, 0)));
    assert!(stored(&store, &scope, &claims[0]).await);
    assert!(!stored(&store, &scope, &claims[1]).await);
    assert!(stored(&store, &scope, &claims[2]).await);
    assert!(!stored(&store, &scope, &claims[3]).await);
}
