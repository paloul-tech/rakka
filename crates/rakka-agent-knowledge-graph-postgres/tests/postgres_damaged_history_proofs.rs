//! What only a live database can prove about a claim whose stored history is
//! damaged (issue #138).
//!
//! The store writes a claim and its history in one statement, so it never
//! leaves the two disagreeing. Rows edited by hand, or tables restored to
//! different moments, do. Each proof here damages a claim's history with a
//! statement of its own and then asks the store to erase the claim.
//!
//! A store refuses, and writes nothing. A store built
//! [`with_damaged_history_erasure`](PostgresKnowledgeGraphStore::with_damaged_history_erasure)
//! erases the claim, and finds the stored results of the transitions it has
//! no row for by reading the scope's ledger.
//!
//! Every test is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently
//! without it.

use rakka_agent_knowledge_graph::conformance::{
    self, conformance_claim, conformance_resolver, ConformanceScopes,
};
use rakka_agent_knowledge_graph::{
    discharge_knowledge_space_retention, Claim, ClaimDeleteRequest, ClaimError, ClaimId,
    ClaimOperationId, ClaimPromotionPolicy, ClaimTombstoneReason, ClaimTombstoneRequest,
    ClaimTrustStatus, ClaimTrustTransitionRequest, KnowledgeGraphStore,
    KnowledgeSpaceRetentionPolicy, KnowledgeSpaceScope,
    KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
};
use rakka_agent_knowledge_graph_postgres::PostgresKnowledgeGraphStore;
use rakka_agent_workflow::AgentTimestampMillis;
use tokio_postgres::{Client, NoTls};

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

/// Removes one transition of the claim.
const LOSE_TRANSITION: &str = "DELETE FROM rakka_agent_knowledge_claim_transition \
     WHERE tenant = $1 AND space = $2 AND claim_id = $3 AND ordinal = $4";

/// Adds a transition behind the claim's last one, a copy of it.
const ADD_TRANSITION: &str = "INSERT INTO rakka_agent_knowledge_claim_transition \
         (tenant, space, claim_id, ordinal, record) \
     SELECT tenant, space, claim_id, ordinal + 1, record \
     FROM rakka_agent_knowledge_claim_transition \
     WHERE tenant = $1 AND space = $2 AND claim_id = $3 AND ordinal = $4";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .expect("the PostgreSQL test database should connect");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres test connection error: {error}");
        }
    });
    client
}

fn dsn() -> Option<String> {
    std::env::var("RAKKA_POSTGRES_TEST_DSN").ok()
}

/// A migrated store, and a second connection for the statements that damage
/// rows and read them back.
async fn store_and_raw() -> Option<(PostgresKnowledgeGraphStore, Client)> {
    let dsn = dsn()?;
    let store = PostgresKnowledgeGraphStore::new(connect(&dsn).await);
    store.migrate().await.expect("the schema applies");
    Some((store, connect(&dsn).await))
}

/// The same, for a store that erases a claim whose history is damaged.
async fn erasing_store_and_raw() -> Option<(PostgresKnowledgeGraphStore, Client)> {
    let (store, raw) = store_and_raw().await?;
    Some((store.with_damaged_history_erasure(), raw))
}

fn scope(label: &str) -> KnowledgeSpaceScope {
    ConformanceScopes::unique(label).primary
}

fn transition_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    to: ClaimTrustStatus,
) -> ClaimTrustTransitionRequest {
    ClaimTrustTransitionRequest::new(
        claim_id.clone(),
        ClaimOperationId::derive_transition(scope, claim_id, discriminator)
            .expect("the operation id derives"),
        to,
        conformance_resolver(),
        NOW,
    )
}

fn tombstone_request(scope: &KnowledgeSpaceScope, claim_id: &ClaimId) -> ClaimTombstoneRequest {
    ClaimTombstoneRequest {
        claim_id: claim_id.clone(),
        operation_id: ClaimOperationId::derive_tombstone(scope, claim_id, "withdraw")
            .expect("the operation id derives"),
        reason: ClaimTombstoneReason::Withdrawn,
        actor: conformance_resolver(),
        tombstoned_at: AgentTimestampMillis::new(20),
    }
}

fn delete_request(scope: &KnowledgeSpaceScope, claim_id: &ClaimId) -> ClaimDeleteRequest {
    ClaimDeleteRequest {
        claim_id: claim_id.clone(),
        operation_id: ClaimOperationId::derive_delete(scope, claim_id, "erase")
            .expect("the operation id derives"),
        expected: None,
    }
}

/// Appends a claim and walks it proposed, disputed, retracted: two
/// transitions, ordinals 1 and 2.
async fn retracted_claim(
    store: &PostgresKnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    name: &str,
) -> Claim {
    let claim = conformance_claim(scope, name, "subject", name);
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
                NOW,
            )
            .await
            .expect("the transition applies");
    }
    claim
}

/// Runs one damaging statement against one transition of the claim.
async fn damage(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    sql: &str,
    ordinal: i32,
) {
    let changed = raw
        .execute(
            sql,
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &claim_id.as_str(),
                &ordinal,
            ],
        )
        .await
        .expect("the damaging statement runs");
    assert_eq!(changed, 1, "exactly one transition row is damaged");
}

/// The kind of the ledger row of one operation, when there is one.
async fn ledger_kind(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    operation_id: &ClaimOperationId,
) -> Option<String> {
    raw.query_opt(
        "SELECT kind FROM rakka_agent_knowledge_claim_op \
         WHERE tenant = $1 AND space = $2 AND operation_id = $3",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &operation_id.as_str(),
        ],
    )
    .await
    .expect("the ledger row reads")
    .map(|row| row.get("kind"))
}

/// The ordinals of the claim's stored transitions.
async fn stored_ordinals(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
) -> Vec<i32> {
    raw.query(
        "SELECT ordinal FROM rakka_agent_knowledge_claim_transition \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3 ORDER BY ordinal",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &claim_id.as_str(),
        ],
    )
    .await
    .expect("the transitions read")
    .iter()
    .map(|row| row.get("ordinal"))
    .collect()
}

/// Asserts that neither erasure wrote anything: the claim is as it was, its
/// operations still answer from the ledger, and neither erasure's operation
/// id is spent.
async fn assert_nothing_was_written(
    store: &PostgresKnowledgeGraphStore,
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
    ordinals: &[i32],
) {
    let kept = store
        .get(scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .expect("the claim is still stored");
    assert!(!kept.is_tombstoned());
    assert_eq!(kept.trust(), ClaimTrustStatus::Retracted);
    assert_eq!(stored_ordinals(raw, scope, &claim.claim_id).await, ordinals);
    assert_eq!(
        ledger_kind(raw, scope, &claim.operation_id)
            .await
            .as_deref(),
        Some("append"),
        "the append still answers from the ledger"
    );
    for discriminator in ["dispute", "retract"] {
        let operation_id =
            ClaimOperationId::derive_transition(scope, &claim.claim_id, discriminator)
                .expect("the operation id derives");
        assert_eq!(
            ledger_kind(raw, scope, &operation_id).await.as_deref(),
            Some("transition"),
            "the {discriminator} still answers from the ledger"
        );
    }
    for spent in [
        tombstone_request(scope, &claim.claim_id).operation_id,
        delete_request(scope, &claim.claim_id).operation_id,
    ] {
        assert_eq!(
            ledger_kind(raw, scope, &spent).await,
            None,
            "a refused erasure spends no operation id"
        );
    }
}

/// Asks for a tombstone and a delete of a damaged claim, twice each, and
/// asserts that every answer is the damaged-history refusal.
async fn assert_both_erasures_are_refused_as_damaged(
    store: &PostgresKnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
) {
    for attempt in 1..=2 {
        let tombstone = store
            .tombstone(scope, &tombstone_request(scope, &claim.claim_id))
            .await
            .expect_err("a damaged claim is not tombstoned");
        assert_eq!(
            tombstone,
            ClaimError::HistoryDamaged {
                claim_id: claim.claim_id.clone()
            },
            "tombstone, attempt {attempt}"
        );
        assert_eq!(tombstone.code(), "claim-history-damaged");
        let delete = store
            .delete(scope, &delete_request(scope, &claim.claim_id))
            .await
            .expect_err("a damaged claim is not deleted");
        assert_eq!(
            delete,
            ClaimError::HistoryDamaged {
                claim_id: claim.claim_id.clone()
            },
            "delete, attempt {attempt}"
        );
    }
}

/// A claim whose last transition row is gone is refused its tombstone and
/// its delete as damaged, on every attempt, and nothing is written. It used
/// to answer that the erasure had lost eight races and to ask for a retry.
#[tokio::test]
async fn postgres_a_claim_missing_its_last_transition_is_refused_as_damaged_when_dsn_is_set() {
    let Some((store, raw)) = store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-last");
    let claim = retracted_claim(&store, &scope, "last").await;
    damage(&raw, &scope, &claim.claim_id, LOSE_TRANSITION, 2).await;

    assert_both_erasures_are_refused_as_damaged(&store, &scope, &claim).await;
    assert_nothing_was_written(&store, &raw, &scope, &claim, &[1]).await;
}

/// The same for a claim missing an earlier transition: the store holds as
/// many rows as a claim one transition younger, and not the first of them.
#[tokio::test]
async fn postgres_a_claim_missing_an_earlier_transition_is_refused_as_damaged_when_dsn_is_set() {
    let Some((store, raw)) = store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-earlier");
    let claim = retracted_claim(&store, &scope, "earlier").await;
    damage(&raw, &scope, &claim.claim_id, LOSE_TRANSITION, 1).await;

    assert_both_erasures_are_refused_as_damaged(&store, &scope, &claim).await;
    assert_nothing_was_written(&store, &raw, &scope, &claim, &[2]).await;
}

/// The same for a claim the store holds one transition too many for.
#[tokio::test]
async fn postgres_a_claim_with_a_transition_too_many_is_refused_as_damaged_when_dsn_is_set() {
    let Some((store, raw)) = store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-extra");
    let claim = retracted_claim(&store, &scope, "extra").await;
    damage(&raw, &scope, &claim.claim_id, ADD_TRANSITION, 2).await;

    assert_both_erasures_are_refused_as_damaged(&store, &scope, &claim).await;
    assert_nothing_was_written(&store, &raw, &scope, &claim, &[1, 2, 3]).await;
}

/// The right number of rows is not enough: a claim that lost one transition
/// and gained another holds as many rows as its record counts, and is
/// damaged. Naming the operations of the rows that are there would leave the
/// lost transition's stored result behind.
#[tokio::test]
async fn postgres_a_claim_with_the_right_number_of_wrong_transitions_is_damaged_when_dsn_is_set() {
    let Some((store, raw)) = store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-swapped");
    let claim = retracted_claim(&store, &scope, "swapped").await;
    damage(&raw, &scope, &claim.claim_id, ADD_TRANSITION, 2).await;
    damage(&raw, &scope, &claim.claim_id, LOSE_TRANSITION, 1).await;

    assert_both_erasures_are_refused_as_damaged(&store, &scope, &claim).await;
    assert_nothing_was_written(&store, &raw, &scope, &claim, &[2, 3]).await;
}

/// The retention sweep counts each damaged claim and goes on to the healthy
/// claims behind it. The pass used to end on the first refused delete, on
/// every pass, and to count a claim missing its last transition as moved.
#[tokio::test]
async fn postgres_the_sweep_counts_damaged_claims_and_goes_on_when_dsn_is_set() {
    let Some((store, raw)) = store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-sweep");
    // The sweep cannot age the first, and the store refuses to delete the
    // other two.
    let missing_last = retracted_claim(&store, &scope, "missing-last").await;
    let missing_earlier = retracted_claim(&store, &scope, "missing-earlier").await;
    let one_too_many = retracted_claim(&store, &scope, "one-too-many").await;
    let healthy = retracted_claim(&store, &scope, "healthy").await;
    damage(&raw, &scope, &missing_last.claim_id, LOSE_TRANSITION, 2).await;
    damage(&raw, &scope, &missing_earlier.claim_id, LOSE_TRANSITION, 1).await;
    damage(&raw, &scope, &one_too_many.claim_id, ADD_TRANSITION, 2).await;

    let policy =
        KnowledgeSpaceRetentionPolicy::retain_all().with_window(ClaimTrustStatus::Retracted, 0);
    for pass in 1..=2 {
        let report = discharge_knowledge_space_retention(
            &store,
            &scope,
            &policy,
            AgentTimestampMillis::new(1_000),
            None,
            KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
        )
        .await
        .expect("the pass answers, and does not end on a damaged claim");
        let healthy_deleted = usize::from(pass == 1);
        assert_eq!(
            (
                report.examined,
                report.damaged,
                report.deleted,
                report.moved
            ),
            (3 + healthy_deleted, 3, healthy_deleted, 0),
            "pass {pass}"
        );
    }
    for damaged in [&missing_last, &missing_earlier, &one_too_many] {
        assert!(
            store
                .get(&scope, &damaged.claim_id)
                .await
                .expect("the read answers")
                .is_some(),
            "a damaged claim is left as it is"
        );
    }
    assert!(store
        .get(&scope, &healthy.claim_id)
        .await
        .expect("the read answers")
        .is_none());
}

/// The kinds of the ledger rows of a claim's append and its two transitions.
async fn ledger_kinds_of(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
) -> [Option<String>; 3] {
    let dispute = ClaimOperationId::derive_transition(scope, &claim.claim_id, "dispute")
        .expect("the operation id derives");
    let retract = ClaimOperationId::derive_transition(scope, &claim.claim_id, "retract")
        .expect("the operation id derives");
    [
        ledger_kind(raw, scope, &claim.operation_id).await,
        ledger_kind(raw, scope, &dispute).await,
        ledger_kind(raw, scope, &retract).await,
    ]
}

fn kinds(kinds: [&str; 3]) -> [Option<String>; 3] {
    kinds.map(|kind| Some(kind.to_string()))
}

/// Every ledger payload of the scope that still holds the text.
async fn ledger_payloads_holding(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    text: &str,
) -> Vec<String> {
    raw.query(
        "SELECT operation_id FROM rakka_agent_knowledge_claim_op \
         WHERE tenant = $1 AND space = $2 AND position($3::BYTEA IN result) > 0",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &text.as_bytes(),
        ],
    )
    .await
    .expect("the ledger reads")
    .iter()
    .map(|row| row.get("operation_id"))
    .collect()
}

/// A store that opted in deletes a claim that lost a transition, and clears
/// the stored result of the transition it has no row for. That result holds
/// the whole claim as it stood after the transition, and no row names its
/// operation: the store finds it by reading the scope's ledger.
///
/// A neighbour's operations are left alone, among them one whose stored
/// result mentions the deleted claim's id and is not that claim's.
#[tokio::test]
async fn postgres_an_opted_in_store_deletes_a_damaged_claim_whole_when_dsn_is_set() {
    let Some((store, raw)) = erasing_store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-delete");
    let claim = retracted_claim(&store, &scope, "pg-damaged-delete-subject").await;
    // The neighbour's statement points at the damaged claim by its id.
    let neighbour = conformance_claim(&scope, "neighbour", "subject", claim.claim_id.as_str());
    store
        .append(&scope, &neighbour)
        .await
        .expect("the neighbour appends");
    store
        .transition(
            &scope,
            &transition_request(
                &scope,
                &neighbour.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            NOW,
        )
        .await
        .expect("the neighbour's transition applies");
    damage(&raw, &scope, &claim.claim_id, LOSE_TRANSITION, 1).await;
    assert_eq!(
        ledger_payloads_holding(&raw, &scope, "pg-damaged-delete-subject")
            .await
            .len(),
        3,
        "the append and both transitions hold the statement"
    );

    store
        .delete(&scope, &delete_request(&scope, &claim.claim_id))
        .await
        .expect("an opted-in store deletes a damaged claim");

    assert!(store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .is_none());
    assert!(stored_ordinals(&raw, &scope, &claim.claim_id)
        .await
        .is_empty());
    assert_eq!(
        ledger_kinds_of(&raw, &scope, &claim).await,
        kinds(["erased", "erased", "erased"]),
        "the append, the transition with no row, and the transition with one"
    );
    assert_eq!(
        ledger_payloads_holding(&raw, &scope, "pg-damaged-delete-subject").await,
        Vec::<String>::new(),
        "no stored result still holds the statement"
    );
    assert_eq!(
        ledger_kind(
            &raw,
            &scope,
            &delete_request(&scope, &claim.claim_id).operation_id
        )
        .await
        .as_deref(),
        Some("delete")
    );
    // The neighbour is untouched, and its operations still answer.
    let neighbour_dispute =
        ClaimOperationId::derive_transition(&scope, &neighbour.claim_id, "dispute")
            .expect("the operation id derives");
    assert_eq!(
        ledger_kind(&raw, &scope, &neighbour.operation_id)
            .await
            .as_deref(),
        Some("append")
    );
    assert_eq!(
        ledger_kind(&raw, &scope, &neighbour_dispute)
            .await
            .as_deref(),
        Some("transition"),
        "a stored result that mentions the claim and is not its own is kept"
    );
    // A replay of the lost transition is refused like any erased operation.
    let replay = store
        .transition(
            &scope,
            &transition_request(
                &scope,
                &claim.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            NOW,
        )
        .await
        .expect_err("the erased transition does not answer its stored result");
    assert_eq!(replay.code(), "claim-operation-erased");
}

/// A store that opted in tombstones a damaged claim, clearing the same
/// stored results, and then deletes the stub. A store that did not opt in
/// refuses the stub's delete: a stub's history is held to its record too.
#[tokio::test]
async fn postgres_an_opted_in_store_tombstones_a_damaged_claim_when_dsn_is_set() {
    let Some((store, raw)) = erasing_store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-tombstone");
    let claim = retracted_claim(&store, &scope, "pg-damaged-tombstone-subject").await;
    damage(&raw, &scope, &claim.claim_id, LOSE_TRANSITION, 2).await;

    let stub = store
        .tombstone(&scope, &tombstone_request(&scope, &claim.claim_id))
        .await
        .expect("an opted-in store tombstones a damaged claim");

    assert!(stub.is_tombstoned());
    assert_eq!(
        ledger_kinds_of(&raw, &scope, &claim).await,
        kinds(["erased", "erased", "erased"])
    );
    assert_eq!(
        ledger_payloads_holding(&raw, &scope, "pg-damaged-tombstone-subject").await,
        Vec::<String>::new()
    );
    assert_eq!(
        stored_ordinals(&raw, &scope, &claim.claim_id).await,
        [1],
        "a tombstone keeps the history the store holds"
    );

    let refusing =
        PostgresKnowledgeGraphStore::new(connect(&dsn().expect("the gate is set")).await);
    let refused = refusing
        .delete(&scope, &delete_request(&scope, &claim.claim_id))
        .await
        .expect_err("a store that did not opt in refuses the damaged stub");
    assert_eq!(refused.code(), "claim-history-damaged");

    store
        .delete(&scope, &delete_request(&scope, &claim.claim_id))
        .await
        .expect("the opted-in store deletes the damaged stub");
    assert!(store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .is_none());
    assert!(stored_ordinals(&raw, &scope, &claim.claim_id)
        .await
        .is_empty());
}

/// A stored result that mentions the damaged claim and cannot be parsed
/// stops an opted-in store's erasure: it cannot say whose the result is.
/// Nothing is written, and with that row gone the delete goes through.
#[tokio::test]
async fn postgres_an_unreadable_stored_result_stops_the_opted_in_erasure_when_dsn_is_set() {
    let Some((store, raw)) = erasing_store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-unreadable");
    let claim = retracted_claim(&store, &scope, "unreadable").await;
    damage(&raw, &scope, &claim.claim_id, LOSE_TRANSITION, 1).await;
    let unreadable = format!("not a stored outcome, and it mentions {}", claim.claim_id);
    raw.execute(
        "INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result) \
         VALUES ($1, $2, 'an-unreadable-operation', 'transition', $3)",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &unreadable.as_bytes(),
        ],
    )
    .await
    .expect("the unreadable row is stored");

    let refused = store
        .delete(&scope, &delete_request(&scope, &claim.claim_id))
        .await
        .expect_err("the erasure cannot tell whose the unreadable result is");
    assert_eq!(refused.code(), "claim-backend-failed");
    assert!(
        refused.to_string().contains("could not be parsed"),
        "{refused}"
    );
    assert_nothing_was_written(&store, &raw, &scope, &claim, &[2]).await;

    raw.execute(
        "DELETE FROM rakka_agent_knowledge_claim_op \
         WHERE tenant = $1 AND space = $2 AND operation_id = 'an-unreadable-operation'",
        &[&scope.tenant().as_str(), &scope.space().as_str()],
    )
    .await
    .expect("the unreadable row is removed");
    store
        .delete(&scope, &delete_request(&scope, &claim.claim_id))
        .await
        .expect("the erasure goes through without it");
    assert_eq!(
        ledger_kinds_of(&raw, &scope, &claim).await,
        kinds(["erased", "erased", "erased"])
    );
}

/// Run through an opted-in store, the sweep deletes a damaged claim whose
/// last transition it reads cleanly, and still counts the two it does not:
/// one whose last transition is gone, and one with an entry behind its last.
/// The sweep ages a claim only by what it can read without doubt, whatever
/// the store would then do with the delete.
#[tokio::test]
async fn postgres_the_sweep_through_an_opted_in_store_deletes_what_it_can_age_when_dsn_is_set() {
    let Some((store, raw)) = erasing_store_and_raw().await else {
        return;
    };
    let scope = scope("pg-damaged-sweep-erasing");
    let missing_last = retracted_claim(&store, &scope, "missing-last").await;
    let missing_earlier = retracted_claim(&store, &scope, "missing-earlier").await;
    let one_too_many = retracted_claim(&store, &scope, "one-too-many").await;
    damage(&raw, &scope, &missing_last.claim_id, LOSE_TRANSITION, 2).await;
    damage(&raw, &scope, &missing_earlier.claim_id, LOSE_TRANSITION, 1).await;
    damage(&raw, &scope, &one_too_many.claim_id, ADD_TRANSITION, 2).await;

    let policy =
        KnowledgeSpaceRetentionPolicy::retain_all().with_window(ClaimTrustStatus::Retracted, 0);
    let report = discharge_knowledge_space_retention(
        &store,
        &scope,
        &policy,
        AgentTimestampMillis::new(1_000),
        None,
        KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
    )
    .await
    .expect("the pass answers");

    assert_eq!(
        (
            report.examined,
            report.damaged,
            report.deleted,
            report.moved
        ),
        (3, 2, 1, 0)
    );
    assert!(store
        .get(&scope, &missing_earlier.claim_id)
        .await
        .expect("the read answers")
        .is_none());
    // The explicit delete is the way out for the other two.
    for left in [&missing_last, &one_too_many] {
        store
            .delete(&scope, &delete_request(&scope, &left.claim_id))
            .await
            .expect("an opted-in store deletes it when asked");
        assert!(stored_ordinals(&raw, &scope, &left.claim_id)
            .await
            .is_empty());
    }
}

/// Opting in changes nothing for a claim whose history is whole: the shared
/// delete and tombstone clauses pass through an opted-in store.
#[tokio::test]
async fn postgres_an_opted_in_store_passes_the_erasure_clauses_when_dsn_is_set() {
    let Some((store, _raw)) = erasing_store_and_raw().await else {
        return;
    };
    conformance::delete_erases_a_claim(&store, ConformanceScopes::unique("pg-erasing-delete"))
        .await;
    conformance::tombstone_withdraws_content(
        &store,
        ConformanceScopes::unique("pg-erasing-tombstone"),
    )
    .await;
    conformance::tombstone_replay_and_erased_operations(
        &store,
        ConformanceScopes::unique("pg-erasing-replay"),
    )
    .await;
}
