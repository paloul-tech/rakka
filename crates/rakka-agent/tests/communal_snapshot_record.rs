//! The context snapshot's communal selection, and the rule that versions it.
//!
//! Issue #69, [specification 13.5](../../../docs/plans/rakka-agent/spec.md). A
//! snapshot is written as version 2 exactly when a communal search ran, and as
//! version 1 otherwise, with the bytes and digest it had before the read path
//! existed. Three shapes are refused on load, in the snapshot's own decoding,
//! so they are refused on every store.

use std::collections::BTreeSet;

use rakka_agent::{
    assemble_session_context, AgentCommunalClaimId, AgentCommunalClaimTrust,
    AgentContextSnapshotRef, AgentId, AgentRevisionNumber, AgentRunId, AgentRunScope,
    AgentSchemaPolicy, AgentTaskContent, CommunalClaimObject, CommunalClaimProvenance,
    CommunalClaimStatement, InMemorySessionMemoryStore, KnowledgeSpaceId, MemoryClassification,
    MemoryContextSnapshot, MemoryEntryId, MemoryEntryRole, MemoryOperationId, MemorySequence,
    SessionMemoryEntry, SessionMemoryStore, SessionWindowPolicy, SnapshotCommunalClaim,
    SnapshotCommunalRetrieval, TenantId, VersionedAgentRecord,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::{json, Value};

fn scope() -> AgentRunScope {
    AgentRunScope::new(
        TenantId::new("acme"),
        AgentId::new("support").expect("the agent id is valid"),
        AgentRunId::new("run-1").expect("the run id is valid"),
    )
    .expect("the scope is valid")
}

/// A snapshot assembled from one session entry, with no retrieval of any
/// kind: what a run wrote before the read path existed.
async fn session_snapshot() -> MemoryContextSnapshot {
    let scope = scope();
    let session = InMemorySessionMemoryStore::new();
    let entry = SessionMemoryEntry::new(
        MemoryEntryId::derive(&scope, "turn-1-user").expect("the entry id derives"),
        MemoryOperationId::derive(&scope, "turn-1-user").expect("the operation id derives"),
        MemorySequence::new(1),
        MemoryEntryRole::User,
        AgentTaskContent::inline(json!({ "text": "where is my order" }))
            .expect("the content is bounded"),
        1,
        None,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(5),
    )
    .expect("the entry is bounded");
    session
        .append(&scope, &entry)
        .await
        .expect("the entry appends");
    let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("the reference derives");
    assemble_session_context(
        &session,
        &scope,
        &reference,
        1,
        &SessionWindowPolicy::recent_window(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(10),
    )
    .await
    .expect("the snapshot assembles")
}

fn retrieval(query: &str) -> SnapshotCommunalRetrieval {
    SnapshotCommunalRetrieval {
        query: query.to_string(),
        retriever: "in-memory".to_string(),
        retriever_version: AgentRevisionNumber::INITIAL,
        index_watermark: None,
        spaces: vec![KnowledgeSpaceId::new("space-a").expect("the space id is valid")],
        trust: BTreeSet::from([AgentCommunalClaimTrust::Verified]),
        classifications: BTreeSet::from([MemoryClassification::Unclassified]),
        min_confidence_bps: 0,
        index_incomplete: false,
        ingress_revision: Some(AgentRevisionNumber::new(3)),
        definition_revision: AgentRevisionNumber::new(2),
        setup_revision: None,
    }
}

fn claim(relevance_bps: u16) -> SnapshotCommunalClaim {
    let statement = CommunalClaimStatement {
        subject: "order-42".to_string(),
        predicate: "status".to_string(),
        object: CommunalClaimObject::Value(
            AgentTaskContent::inline(json!("delayed at customs")).expect("the value is bounded"),
        ),
    };
    SnapshotCommunalClaim {
        claim_id: AgentCommunalClaimId::new("claim-1").expect("the claim id is valid"),
        space: KnowledgeSpaceId::new("space-a").expect("the space id is valid"),
        trust: AgentCommunalClaimTrust::Verified,
        statement_digest: statement.digest(),
        statement,
        provenance: CommunalClaimProvenance {
            agent: AgentId::new("scout").expect("the agent id is valid"),
            goal: None,
            task: None,
            run: None,
            delegation: None,
        },
        evidence: Vec::new(),
        created_at: AgentTimestampMillis::new(1),
        classification: MemoryClassification::Unclassified,
        confidence_bps: 5_000,
        relevance_bps,
        transforms: Vec::new(),
        reports: Vec::new(),
    }
}

fn version(snapshot: &MemoryContextSnapshot) -> u32 {
    snapshot.schema_version().get()
}

/// Encodes, lets the caller edit the JSON, and decodes the result.
fn decoded_after(
    snapshot: &MemoryContextSnapshot,
    edit: impl FnOnce(&mut serde_json::Map<String, Value>),
) -> Result<MemoryContextSnapshot, serde_json::Error> {
    let mut value = serde_json::to_value(snapshot).expect("the snapshot encodes");
    edit(value.as_object_mut().expect("a snapshot is an object"));
    serde_json::from_str(&value.to_string())
}

#[tokio::test]
async fn a_snapshot_without_a_communal_search_is_version_1() {
    let snapshot = session_snapshot().await;
    assert_eq!(version(&snapshot), 1);
    assert!(snapshot.communal_claims.is_empty());
    assert!(snapshot.communal.is_none());

    let value = serde_json::to_value(&snapshot).expect("the snapshot encodes");
    assert!(
        value.get("communal").is_none(),
        "a snapshot that searched nothing carries no communal record"
    );
    assert!(
        value["budget"].get("communal_claim_bytes").is_none(),
        "and no communal byte count"
    );
    assert_eq!(value["communal_claims"], json!([]));

    // It loads under the current policy and reads back equal.
    let decoded: MemoryContextSnapshot =
        serde_json::from_value(value).expect("a version 1 snapshot decodes");
    assert_eq!(decoded, snapshot);
    AgentSchemaPolicy::default()
        .check_record(&decoded)
        .expect("version 1 is inside the accepted window");
}

#[tokio::test]
async fn a_snapshot_with_a_communal_search_is_version_2() {
    let mut selected = session_snapshot().await;
    selected.set_communal(retrieval("where is my order"), vec![claim(7_500)], 64);
    assert_eq!(version(&selected), 2);
    assert_eq!(selected.communal_claims.len(), 1);
    assert_eq!(selected.budget.communal_claims, 1);
    assert_eq!(selected.budget.communal_claim_bytes, 64);
    assert_eq!(selected.content_digest, selected.compute_digest());
    let decoded: MemoryContextSnapshot =
        serde_json::from_str(&serde_json::to_string(&selected).expect("the snapshot encodes"))
            .expect("a version 2 snapshot decodes");
    assert_eq!(decoded, selected);
    AgentSchemaPolicy::default()
        .check_record(&decoded)
        .expect("version 2 is the current version");

    // A search that selected nothing is still a search: the record of it is
    // what makes the snapshot version 2.
    let mut searched = session_snapshot().await;
    searched.set_communal(retrieval("where is my order"), Vec::new(), 0);
    assert_eq!(version(&searched), 2);
    assert!(searched.communal_claims.is_empty());
    assert!(searched.communal.is_some());
    let decoded: MemoryContextSnapshot =
        serde_json::from_str(&serde_json::to_string(&searched).expect("the snapshot encodes"))
            .expect("a version 2 snapshot with no claim decodes");
    assert_eq!(decoded, searched);
}

#[tokio::test]
async fn the_version_2_digest_covers_the_communal_record() {
    let plain = session_snapshot().await;

    let mut first = session_snapshot().await;
    first.set_communal(retrieval("where is my order"), vec![claim(7_500)], 64);
    assert_ne!(
        first.content_digest, plain.content_digest,
        "a communal search changes what the snapshot assembled"
    );

    let mut other_query = session_snapshot().await;
    other_query.set_communal(retrieval("something else"), vec![claim(7_500)], 64);
    assert_ne!(
        other_query.compute_digest(),
        first.compute_digest(),
        "the digest covers the communal record"
    );

    let mut other_claim = session_snapshot().await;
    other_claim.set_communal(retrieval("where is my order"), vec![claim(2_500)], 64);
    assert_ne!(
        other_claim.compute_digest(),
        first.compute_digest(),
        "the digest covers the selected claims"
    );

    // The budget is an account of the content, not content.
    let mut rebudgeted = first.clone();
    rebudgeted.budget.session_bytes += 1;
    rebudgeted.budget.communal_claim_bytes += 1;
    assert_eq!(rebudgeted.compute_digest(), first.compute_digest());
}

#[tokio::test]
async fn a_version_1_snapshot_with_communal_claims_is_refused() {
    let mut selected = session_snapshot().await;
    selected.set_communal(retrieval("where is my order"), vec![claim(7_500)], 64);
    let claims = serde_json::to_value(&selected.communal_claims).expect("the claims encode");

    let error = decoded_after(&session_snapshot().await, |snapshot| {
        snapshot.insert("communal_claims".to_string(), claims);
    })
    .expect_err("a version 1 snapshot selects no communal claim");
    assert!(error.to_string().contains("version 1"), "{error}");
}

#[tokio::test]
async fn a_version_1_snapshot_with_a_communal_record_is_refused() {
    let communal =
        serde_json::to_value(retrieval("where is my order")).expect("the record encodes");
    let error = decoded_after(&session_snapshot().await, |snapshot| {
        snapshot.insert("communal".to_string(), communal);
    })
    .expect_err("a version 1 snapshot records no communal search");
    assert!(error.to_string().contains("version 1"), "{error}");
}

#[tokio::test]
async fn a_version_2_snapshot_without_a_communal_record_is_refused() {
    let mut selected = session_snapshot().await;
    selected.set_communal(retrieval("where is my order"), vec![claim(7_500)], 64);
    let error = decoded_after(&selected, |snapshot| {
        snapshot.remove("communal");
    })
    .expect_err("a version 2 snapshot always records its communal search");
    assert!(error.to_string().contains("version 2"), "{error}");

    // The same for a bare version bump of a version 1 record.
    let error = decoded_after(&session_snapshot().await, |snapshot| {
        snapshot.insert("schema_version".to_string(), json!(2));
    })
    .expect_err("a version 2 snapshot always records its communal search");
    assert!(error.to_string().contains("version 2"), "{error}");
}

/// The snapshot record's communal field as it stood at `86cf087`: a list of
/// memory entry ids, a placeholder no writer ever filled.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct MemoryContextSnapshotRecord86cf087 {
    schema_version: u32,
    #[serde(default)]
    communal_claims: Vec<MemoryEntryId>,
}

/// A binary from before the read path can never read a selected claim as
/// something else: the claim is an object where it expects an id, and the
/// whole snapshot fails to decode.
#[tokio::test]
async fn the_86cf087_shape_cannot_read_a_claim_as_a_memory_id() {
    let mut selected = session_snapshot().await;
    selected.set_communal(retrieval("where is my order"), vec![claim(7_500)], 64);
    let encoded = serde_json::to_string(&selected).expect("the snapshot encodes");
    serde_json::from_str::<MemoryContextSnapshotRecord86cf087>(&encoded)
        .expect_err("a selected claim is not a memory entry id");

    // With no claim selected the earlier shape decodes the record and drops
    // the communal search it does not know. The model input is the same in
    // that case; this is why the roll order upgrades every snapshot reader
    // before any run is wired.
    let mut searched = session_snapshot().await;
    searched.set_communal(retrieval("where is my order"), Vec::new(), 0);
    let encoded = serde_json::to_string(&searched).expect("the snapshot encodes");
    let earlier: MemoryContextSnapshotRecord86cf087 =
        serde_json::from_str(&encoded).expect("the earlier shape decodes an empty selection");
    assert_eq!(earlier.schema_version, 2);
    assert!(earlier.communal_claims.is_empty());

    // A version 1 snapshot from this build is one the earlier shape reads.
    let encoded = serde_json::to_string(&session_snapshot().await).expect("the snapshot encodes");
    let earlier: MemoryContextSnapshotRecord86cf087 =
        serde_json::from_str(&encoded).expect("the earlier shape decodes version 1");
    assert_eq!(earlier.schema_version, 1);
}

#[tokio::test]
async fn relevance_is_clamped_to_10000() {
    let mut snapshot = session_snapshot().await;
    snapshot.set_communal(
        retrieval("where is my order"),
        vec![claim(20_000), claim(10_000), claim(9_999)],
        64,
    );
    assert_eq!(
        snapshot
            .communal_claims
            .iter()
            .map(|claim| claim.relevance_bps)
            .collect::<Vec<_>>(),
        vec![10_000, 10_000, 9_999]
    );
}
