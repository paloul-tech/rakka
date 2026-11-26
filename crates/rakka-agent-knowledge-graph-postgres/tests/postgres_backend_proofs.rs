//! What only a live database can prove about the PostgreSQL backend.
//!
//! Specification 19: production durability claims are never based on the
//! in-memory implementation. The conformance suite (its own test file) proves
//! the shared semantics; this file proves the properties that need real
//! durability and real concurrency — replays answering across a reconnect,
//! genuinely concurrent compare-and-set races over two connections, the
//! concurrent-migrator race, fail-closed decoding of doctored rows, and the
//! bytes a live claim is stored as — including scenario 33's concurrent
//! specialist appends to one space.
//!
//! Every test is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently
//! without it.

use rakka_agent::{
    AgentCapabilityId, AgentDelegationId, AgentGoalId, AgentId, AgentPolicyRef,
    AgentRevisionNumber, AgentRunId, AgentTaskId, MemoryClassification, MemoryEmbeddingRef,
};
use rakka_agent_knowledge_graph::conformance::{
    conformance_claim, conformance_resolver, promotion_grant_for, ConformanceScopes,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimAccess, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimPromotionEvidence, ClaimPromotionPolicy, ClaimProvenance, ClaimTransitionCursor,
    ClaimTrustStatus, ClaimTrustTransitionRequest, KnowledgeGraphStore, KnowledgeSpaceScope,
    CURRENT_CLAIM_SCHEMA_VERSION,
};
use rakka_agent_knowledge_graph_postgres::PostgresKnowledgeGraphStore;
use rakka_agent_workflow::{
    AgentAttributes, AgentEffectId, AgentTimestampMillis, ArtifactEncryptionRef, ArtifactKind,
    ArtifactRef, RedactionStatus, StateSchemaVersion,
};
use tokio_postgres::{Client, NoTls};

async fn client() -> Option<Client> {
    let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
        Ok(dsn) => dsn,
        Err(_) => return None,
    };
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("the PostgreSQL test database should connect");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres test connection error: {error}");
        }
    });
    Some(client)
}

async fn store() -> Option<PostgresKnowledgeGraphStore> {
    let store = PostgresKnowledgeGraphStore::new(client().await?);
    store.migrate().await.expect("the schema applies");
    Some(store)
}

fn transition_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &rakka_agent_knowledge_graph::ClaimId,
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

#[tokio::test]
async fn postgres_replays_answer_original_results_across_reconnect_when_dsn_is_set() {
    let Some(store_a) = store().await else { return };
    let scopes = ConformanceScopes::unique("pg-reconnect");
    let scope = &scopes.primary;
    let claim = conformance_claim(scope, "reconnect", "node-a", "node-b");
    let appended = store_a
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let dispute = transition_request(
        scope,
        &claim.claim_id,
        "reconnect-dispute",
        ClaimTrustStatus::Disputed,
    );
    let outcome = store_a
        .transition(
            scope,
            &dispute,
            &ClaimPromotionPolicy::default(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies");

    // A brand-new connection answers both replays with the original results:
    // the ledger is durable state, not connection state.
    let store_b = store().await.expect("a second connection opens");
    let replayed_append = store_b
        .append(scope, &claim)
        .await
        .expect("the append replays");
    assert_eq!(
        serde_json::to_vec(&replayed_append).expect("the claim serializes"),
        serde_json::to_vec(&appended).expect("the claim serializes"),
        "a replayed append answers the originally stored claim byte-identically"
    );
    assert_eq!(replayed_append.trust(), ClaimTrustStatus::Proposed);
    let replayed_outcome = store_b
        .transition(
            scope,
            &dispute,
            &ClaimPromotionPolicy::default(),
            AgentTimestampMillis::new(99),
        )
        .await
        .expect("the transition replays");
    assert_eq!(replayed_outcome, outcome);

    // The replays moved nothing: one transition, and the live claim is still
    // the disputed one.
    let history = store_b
        .transitions(scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(history.transitions.len(), 1);
    let live = store_b
        .get(scope, &claim.claim_id)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert_eq!(live.trust(), ClaimTrustStatus::Disputed);
    assert_eq!(live.transition_count(), 1);
}

#[tokio::test]
async fn postgres_concurrent_distinct_transitions_do_not_lose_updates_when_dsn_is_set() {
    // Two connections race two *different* operations on one claim. Whatever
    // the interleaving: no update is lost, ordinals are gapless, and every
    // successful operation's replay answers its original outcome. A loser that
    // is refused is refused by the legality table re-run against the state
    // that beat it — never by a spurious backend failure.
    let Some(store_a) = store().await else { return };
    let store_b = store().await.expect("a second connection opens");
    let scopes = ConformanceScopes::unique("pg-race-distinct");
    let scope = scopes.primary.clone();
    let claim = conformance_claim(&scope, "race", "node-a", "node-b");
    store_a
        .append(&scope, &claim)
        .await
        .expect("the claim appends");

    let dispute = transition_request(
        &scope,
        &claim.claim_id,
        "race-dispute",
        ClaimTrustStatus::Disputed,
    );
    let retract = transition_request(
        &scope,
        &claim.claim_id,
        "race-retract",
        ClaimTrustStatus::Retracted,
    );

    let dispute_task = {
        let store = store_a.clone();
        let scope = scope.clone();
        let request = dispute.clone();
        tokio::spawn(async move {
            store
                .transition(
                    &scope,
                    &request,
                    &ClaimPromotionPolicy::default(),
                    AgentTimestampMillis::new(10),
                )
                .await
        })
    };
    let retract_task = {
        let store = store_b.clone();
        let scope = scope.clone();
        let request = retract.clone();
        tokio::spawn(async move {
            store
                .transition(
                    &scope,
                    &request,
                    &ClaimPromotionPolicy::default(),
                    AgentTimestampMillis::new(10),
                )
                .await
        })
    };
    let results = [
        (
            dispute.clone(),
            dispute_task.await.expect("the task completes"),
        ),
        (
            retract.clone(),
            retract_task.await.expect("the task completes"),
        ),
    ];

    let mut successes = Vec::new();
    for (request, result) in results {
        match result {
            Ok(outcome) => successes.push((request, outcome)),
            Err(error) => assert_eq!(
                error.code(),
                "claim-transition-illegal",
                "a lost race is refused by the legality table, not by the backend: {error}"
            ),
        }
    }
    assert!(!successes.is_empty(), "at least one racer commits");

    let history = store_a
        .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    let ordinals: Vec<u32> = history.transitions.iter().map(|t| t.ordinal).collect();
    let expected: Vec<u32> = (1..=u32::try_from(successes.len()).expect("bounded")).collect();
    assert_eq!(
        ordinals, expected,
        "ordinals are gapless and duplicate-free"
    );
    let live = store_a
        .get(&scope, &claim.claim_id)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert_eq!(live.transition_count() as usize, successes.len());
    for (request, outcome) in &successes {
        assert!(
            history
                .transitions
                .iter()
                .any(|t| t.operation_id == request.operation_id),
            "each successful operation appears in the history exactly once"
        );
        let replayed = store_b
            .transition(
                &scope,
                request,
                &ClaimPromotionPolicy::default(),
                AgentTimestampMillis::new(50),
            )
            .await
            .expect("the replay answers");
        assert_eq!(&replayed, outcome, "a replay answers its original outcome");
    }
}

#[tokio::test]
async fn postgres_same_operation_race_converges_when_dsn_is_set() {
    // Two connections race the *same* operation id: exactly one applies, the
    // other replays the winner's outcome, and both callers observe the same
    // logical result — the durable equivalent of the in-memory ledger.
    let Some(store_a) = store().await else { return };
    let store_b = store().await.expect("a second connection opens");
    let scopes = ConformanceScopes::unique("pg-race-same");
    let scope = scopes.primary.clone();
    let claim = conformance_claim(&scope, "race-same", "node-a", "node-b");
    store_a
        .append(&scope, &claim)
        .await
        .expect("the claim appends");

    let dispute = transition_request(
        &scope,
        &claim.claim_id,
        "race-same-dispute",
        ClaimTrustStatus::Disputed,
    );
    let task_a = {
        let store = store_a.clone();
        let scope = scope.clone();
        let request = dispute.clone();
        tokio::spawn(async move {
            store
                .transition(
                    &scope,
                    &request,
                    &ClaimPromotionPolicy::default(),
                    AgentTimestampMillis::new(10),
                )
                .await
        })
    };
    let task_b = {
        let store = store_b.clone();
        let scope = scope.clone();
        let request = dispute.clone();
        tokio::spawn(async move {
            store
                .transition(
                    &scope,
                    &request,
                    &ClaimPromotionPolicy::default(),
                    AgentTimestampMillis::new(10),
                )
                .await
        })
    };
    let outcome_a = task_a
        .await
        .expect("the task completes")
        .expect("the racer converges on the applied outcome");
    let outcome_b = task_b
        .await
        .expect("the task completes")
        .expect("the racer converges on the applied outcome");
    assert_eq!(outcome_a, outcome_b);

    let history = store_a
        .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(
        history.transitions.len(),
        1,
        "one operation, one transition"
    );
}

#[tokio::test]
async fn postgres_replayed_promotion_is_not_relitigated_after_grant_expiry_when_dsn_is_set() {
    // The decided gate is durable: replaying a granted promotion after its
    // grant expired — on a new connection — answers the original outcome with
    // its receipt, while a *fresh* promotion under an equally expired grant is
    // refused. The difference is the ledger, not the gate.
    let Some(store_a) = store().await else { return };
    let scopes = ConformanceScopes::unique("pg-gate-replay");
    let scope = &scopes.primary;
    let policy = ClaimPromotionPolicy::gate_all();
    let expiry = AgentTimestampMillis::new(1_000);

    let claim = conformance_claim(scope, "gated", "node-a", "node-b");
    let appended = store_a
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let grant = promotion_grant_for(scope, &appended, expiry, 1);
    let promote = transition_request(
        scope,
        &claim.claim_id,
        "gated-promotion",
        ClaimTrustStatus::Verified,
    )
    .with_promotion(ClaimPromotionEvidence { grant });
    let outcome = store_a
        .transition(scope, &promote, &policy, AgentTimestampMillis::new(10))
        .await
        .expect("the granted promotion applies");
    assert!(
        outcome.transition.gate.is_some(),
        "the gate stamped its receipt"
    );

    let store_b = store().await.expect("a second connection opens");
    let replayed = store_b
        .transition(scope, &promote, &policy, AgentTimestampMillis::new(5_000))
        .await
        .expect("the replay answers without re-evaluating the expired grant");
    assert_eq!(replayed, outcome);

    // Control: the same expired grant refuses a fresh promotion outright.
    let fresh = conformance_claim(scope, "gated-fresh", "node-a", "node-c");
    let fresh_appended = store_a
        .append(scope, &fresh)
        .await
        .expect("the claim appends");
    let fresh_grant = promotion_grant_for(scope, &fresh_appended, expiry, 1);
    let fresh_promote = transition_request(
        scope,
        &fresh.claim_id,
        "gated-fresh-promotion",
        ClaimTrustStatus::Verified,
    )
    .with_promotion(ClaimPromotionEvidence { grant: fresh_grant });
    let refused = store_a
        .transition(
            scope,
            &fresh_promote,
            &policy,
            AgentTimestampMillis::new(5_000),
        )
        .await
        .expect_err("a fresh promotion under an expired grant is refused");
    assert_eq!(refused.code(), "claim-promotion-grant-rejected");
}

#[tokio::test]
async fn postgres_migration_is_idempotent_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    // `store()` already migrated once; both repeats are no-ops.
    store
        .migrate()
        .await
        .expect("the second migration is a no-op");
    store
        .migrate()
        .await
        .expect("the third migration is a no-op");
}

#[tokio::test]
async fn postgres_concurrent_migrators_do_not_race_when_dsn_is_set() {
    // Two nodes starting at once against a fresh database both run the
    // migration, and `CREATE TABLE IF NOT EXISTS` is not atomic against a
    // concurrent creation: without the advisory lock the loser fails with a
    // `pg_type` unique violation instead of the no-op it reads like.
    //
    // The race only exists while the tables are absent, so this runs in a
    // private schema rather than dropping the shared ones out from under the
    // tests running beside it.
    let Some(setup) = client().await else { return };
    let dsn = std::env::var("RAKKA_POSTGRES_TEST_DSN").expect("the DSN is set");
    let schema = format!(
        "rakka_knowledge_graph_migration_race_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos()
    );
    setup
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("the private schema is created");

    // Each migrator needs its own session: an advisory lock is re-entrant
    // within one session, so racing on a shared connection would prove
    // nothing.
    let mut migrators = Vec::new();
    for _ in 0..4_u32 {
        let dsn = dsn.clone();
        let schema = schema.clone();
        migrators.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
                .await
                .expect("the migrator connects");
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    eprintln!("postgres migrator connection error: {error}");
                }
            });
            client
                .batch_execute(&format!("SET search_path TO {schema}"))
                .await
                .expect("the migrator targets the private schema");
            PostgresKnowledgeGraphStore::new(client).migrate().await
        }));
    }

    let mut failures = Vec::new();
    for migrator in migrators {
        if let Err(error) = migrator.await.expect("the migrator task completes") {
            failures.push(error.to_string());
        }
    }
    let cleanup = setup
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    assert!(
        failures.is_empty(),
        "concurrent migrators raced the system catalogs: {failures:?}"
    );
    cleanup.expect("the private schema is dropped");
}

#[tokio::test]
async fn postgres_doctored_rows_fail_closed_when_dsn_is_set() {
    // The BYTEA record is authoritative, and it is re-validated through the
    // domain crate's restore door on every load: a row rewritten by a newer
    // writer fails the schema window, and a statement edited under its stale
    // fingerprint fails the digest re-derivation — typed refusals, never a
    // silently decoded wrong claim.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scopes = ConformanceScopes::unique("pg-doctored");
    let scope = &scopes.primary;
    let doctor_sql = "UPDATE rakka_agent_knowledge_claim SET record = $4 \
                      WHERE tenant = $1 AND space = $2 AND claim_id = $3";

    let ahead = conformance_claim(scope, "ahead", "node-a", "node-b");
    store
        .append(scope, &ahead)
        .await
        .expect("the claim appends");
    let mut record = ahead.to_record();
    record.schema_version = StateSchemaVersion::new(CURRENT_CLAIM_SCHEMA_VERSION.get() + 1);
    let bytes = serde_json::to_vec(&record).expect("the record serializes");
    raw.execute(
        doctor_sql,
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &ahead.claim_id.as_str(),
            &bytes,
        ],
    )
    .await
    .expect("the row is doctored");
    let error = store
        .get(scope, &ahead.claim_id)
        .await
        .expect_err("a newer-schema record fails closed");
    assert_eq!(error.code(), "schema-version-ahead");

    let edited = conformance_claim(scope, "edited", "node-a", "node-b");
    store
        .append(scope, &edited)
        .await
        .expect("the claim appends");
    let mut record = edited.to_record();
    record.subject = ClaimNodeId::new("node-tampered").expect("the node id is valid");
    let bytes = serde_json::to_vec(&record).expect("the record serializes");
    raw.execute(
        doctor_sql,
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &edited.claim_id.as_str(),
            &bytes,
        ],
    )
    .await
    .expect("the row is doctored");
    let error = store
        .get(scope, &edited.claim_id)
        .await
        .expect_err("an edited statement under a stale fingerprint fails closed");
    assert_eq!(error.code(), "claim-statement-digest-mismatch");
}

fn attributes(key: &str, value: &str) -> AgentAttributes {
    let mut attributes = AgentAttributes::new();
    attributes.insert(key.to_string(), value.to_string());
    attributes
}

/// An artifact reference with every field set.
fn furnished_artifact(id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind: ArtifactKind::File,
        uri: format!("s3://evidence/{id}"),
        checksum: Some(format!("sha256:{id}")),
        content_type: Some("application/json".to_string()),
        byte_len: Some(64),
        retention_class: Some("legal-hold-7y".to_string()),
        encryption: Some(ArtifactEncryptionRef {
            algorithm: "aes-256-gcm".to_string(),
            key_ref: "kms://keys/evidence".to_string(),
            context: attributes("purpose", "evidence"),
        }),
        redaction: RedactionStatus::Redacted,
        created_at: AgentTimestampMillis::new(1_700_000_000_000),
        metadata: attributes("source", "ticket-system"),
    }
}

/// A `Proposed` edge claim in `scope` with every optional field set: the
/// shape `rakka-agent-knowledge-graph`'s `tests/live_claim_encoding.rs` pins,
/// less its transition (an append must be `Proposed`). It cannot be that
/// file's fixture, because the claim id derives from the scope and this
/// file's scopes are unique per run.
fn furnished_claim(scope: &KnowledgeSpaceScope) -> Claim {
    let provenance =
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid"))
            .with_goal(AgentGoalId::new("goal-1").expect("the goal id is valid"))
            .with_task(AgentTaskId::new("task-1").expect("the task id is valid"))
            .with_run(AgentRunId::new("task-1-gen-1").expect("the run id is valid"))
            .with_delegation(AgentDelegationId::new("delegation-1").expect("the id is valid"))
            .with_effect(AgentEffectId::new("effect-1"));
    let mut access = ClaimAccess::default();
    for capability in ["read-pii", "read-support"] {
        access
            .required_read_capabilities
            .insert(AgentCapabilityId::new(capability).expect("the capability is valid"));
    }
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, "live-bytes").expect("the operation id derives"),
        ClaimNodeId::new("customer-1").expect("the node id is valid"),
        ClaimPredicate::new("prefers").expect("the predicate is valid"),
        ClaimObject::Node(ClaimNodeId::new("channel-email").expect("the node id is valid")),
        provenance,
        7_500,
        MemoryClassification::Sensitive,
        AgentTimestampMillis::new(1_700_000_000_001),
    )
    .expect("the claim is valid")
    .with_evidence(vec![furnished_artifact("a"), furnished_artifact("b")])
    .expect("the evidence is within bounds")
    .with_embedding(MemoryEmbeddingRef {
        model: "embedder-v1".to_string(),
        dimensions: 3,
        version: AgentRevisionNumber::new(2),
    })
    .expect("the embedding is valid")
    .with_access(access)
    .expect("the access set is within bounds")
    .with_policy(
        AgentPolicyRef::new("retention-v1").expect("the policy ref is valid"),
        Some(AgentRevisionNumber::new(3)),
    )
}

/// The claim row's `record` bytes, read raw.
async fn stored_record(raw: &Client, scope: &KnowledgeSpaceScope, claim: &Claim) -> Vec<u8> {
    raw.query_one(
        "SELECT record FROM rakka_agent_knowledge_claim \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &claim.claim_id.as_str(),
        ],
    )
    .await
    .expect("the claim row reads")
    .get("record")
}

#[tokio::test]
async fn postgres_stores_a_live_claim_as_its_canonical_record_bytes_when_dsn_is_set() {
    // The backend stores a live claim as its record's `serde_json` bytes, in
    // the claim row and in the append's ledger row. `tests/live_claim_encoding.rs`
    // in `rakka-agent-knowledge-graph` holds that encoding to the bytes the
    // build before tombstones (`d12b0f8`) wrote, so together they hold what a
    // binary from before #68 reads off this table. Every expected value is
    // encoded from a claim this test built, never from a decoded row, which
    // would pass whatever the backend stored.
    const STORED: &str = "the backend stores a live claim as its record's serde_json bytes, \
        which tests/live_claim_encoding.rs in rakka-agent-knowledge-graph holds to the \
        bytes written before tombstones";
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scopes = ConformanceScopes::unique("pg-live-bytes");
    let scope = &scopes.primary;
    let claim = furnished_claim(scope);

    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    let expected = serde_json::to_vec(&claim.to_record()).expect("the record serializes");
    assert_eq!(
        stored_record(&raw, scope, &claim).await,
        expected,
        "the claim row: {STORED}"
    );
    let ledger = raw
        .query_one(
            "SELECT kind, result FROM rakka_agent_knowledge_claim_op \
             WHERE tenant = $1 AND space = $2 AND operation_id = $3",
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &claim.operation_id.as_str(),
            ],
        )
        .await
        .expect("the append's ledger row reads");
    assert_eq!(ledger.get::<_, String>("kind"), "append");
    assert_eq!(
        ledger.get::<_, Vec<u8>>("result"),
        expected,
        "the append ledger row: {STORED}"
    );

    // A transition rewrites the record, through its own statement.
    store
        .transition(
            scope,
            &transition_request(
                scope,
                &claim.claim_id,
                "live-bytes-dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::default(),
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("the dispute applies");
    let moved = claim
        .apply_transition(ClaimTrustStatus::Disputed)
        .expect("the dispute applies");
    assert_eq!(
        stored_record(&raw, scope, &claim).await,
        serde_json::to_vec(&moved.to_record()).expect("the record serializes"),
        "the claim row after a transition: {STORED}"
    );
    assert_eq!(
        store
            .get(scope, &claim.claim_id)
            .await
            .expect("the claim reads"),
        Some(moved)
    );
}

#[tokio::test]
async fn postgres_concurrent_specialist_appends_race_when_dsn_is_set() {
    // Two connections genuinely race two *distinct* specialist appends into
    // one space (specification 18 scenario 33). Both land, each retains
    // exactly its own provenance, and a replay of either — from either
    // connection — answers its original claim. This is the concurrency half
    // of the conformance suite's `concurrent_specialist_append_provenance`,
    // which pins the same semantics for any interleaving.
    let Some(store_a) = store().await else { return };
    let Some(store_b) = store().await else { return };
    let scopes = ConformanceScopes::unique("pg-specialist-race");
    let scope = scopes.primary.clone();

    let specialist_claim = |index: usize| {
        use rakka_agent::{AgentDelegationId, AgentId, AgentRunId, MemoryClassification};
        use rakka_agent_knowledge_graph::{Claim, ClaimObject, ClaimPredicate, ClaimProvenance};
        let operation_id =
            ClaimOperationId::derive_append(&scope, format!("pg-specialist-{index}"))
                .expect("the operation id derives");
        Claim::new(
            &scope,
            operation_id,
            ClaimNodeId::new("finding").expect("the node id is valid"),
            ClaimPredicate::new("links").expect("the predicate is valid"),
            ClaimObject::Node(
                ClaimNodeId::new(format!("evidence-{index}")).expect("the node id is valid"),
            ),
            ClaimProvenance::for_agent(
                AgentId::new(format!("specialist-{index}")).expect("the agent id is valid"),
            )
            .with_run(AgentRunId::new(format!("run-{index}")).expect("the run id is valid"))
            .with_delegation(
                AgentDelegationId::new(format!("delegation-{index}"))
                    .expect("the delegation id is valid"),
            ),
            5_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the claim is valid")
    };
    let first = specialist_claim(0);
    let second = specialist_claim(1);

    let (left, right) = tokio::join!(
        store_a.append(&scope, &first),
        store_b.append(&scope, &second),
    );
    let left = left.expect("the first specialist's claim appends");
    let right = right.expect("the second specialist's claim appends");
    assert_ne!(
        left.claim_id, right.claim_id,
        "distinct operations, distinct claims"
    );
    assert_eq!(left.provenance.agent.as_str(), "specialist-0");
    assert_eq!(right.provenance.agent.as_str(), "specialist-1");

    // Cross-connection replays converge on the original claims.
    let replayed_first = store_b
        .append(&scope, &first)
        .await
        .expect("the replay answers");
    assert_eq!(replayed_first.claim_id, left.claim_id);
    let replayed_second = store_a
        .append(&scope, &second)
        .await
        .expect("the replay answers");
    assert_eq!(replayed_second.claim_id, right.claim_id);
}
