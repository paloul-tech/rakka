//! The knowledge graph's implementations of the communal read ports
//! ([specification 13.4, 13.5 and 16](../../docs/plans/rakka-agent/spec.md);
//! issue #69).
//!
//! `rakka-agent` declares the ports and mirror types and cannot see this
//! crate. What matters here is that the mapping loses nothing a model context
//! or an audit reads, that a retriever answers identities and scores and
//! nothing else, and that the mirrored bounds and labels cannot drift from
//! the graph's own.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use rakka_agent::{
    AgentCapabilityId, AgentCommunalClaimAuthority, AgentCommunalClaimId,
    AgentCommunalClaimRetriever, AgentCommunalClaimTrust, AgentDelegationId, AgentGoalId, AgentId,
    AgentRevisionNumber, AgentRunId, AgentTaskContent, AgentTaskId, CommunalClaimObject,
    CommunalRetrievalQuery, KnowledgeSpaceId, MemoryClassification, TenantId,
    AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE, AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES,
    AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimAccess, ClaimCursor, ClaimDeleteRequest, ClaimError, ClaimFilter, ClaimFuture,
    ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPage, ClaimPredicate,
    ClaimPromotionPolicy, ClaimProvenance, ClaimSearch, ClaimSearchReport, ClaimTombstoneReason,
    ClaimTombstoneRequest, ClaimTransitionCursor, ClaimTransitionOutcome, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalReport, ClaimTrustStatus, ClaimTrustTransitionRequest,
    InMemoryKnowledgeGraphStore, KnowledgeGraphCapabilities, KnowledgeGraphCapability,
    KnowledgeGraphCommunalAuthority, KnowledgeGraphCommunalRetriever, KnowledgeGraphStore,
    KnowledgeSpaceScope, CLAIM_MAX_ACL_CAPABILITIES, CLAIM_MAX_EVIDENCE_ARTIFACTS,
    CLAIM_OBJECT_INLINE_MAX_BYTES,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, PrincipalRef, RedactionStatus,
};

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn space(id: &str) -> KnowledgeSpaceId {
    KnowledgeSpaceId::new(id).expect("the space id is valid")
}

fn scope(space_id: &str) -> KnowledgeSpaceScope {
    KnowledgeSpaceScope::new(tenant(), space(space_id)).expect("the scope is valid")
}

fn capabilities(ids: &[&str]) -> BTreeSet<AgentCapabilityId> {
    ids.iter()
        .map(|id| AgentCapabilityId::new(*id).expect("the capability id is valid"))
        .collect()
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "reviewer".to_string(),
        display_name: None,
    }
}

fn evidence(id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind: ArtifactKind::File,
        uri: format!("s3://evidence/{id}"),
        checksum: None,
        content_type: None,
        byte_len: None,
        retention_class: None,
        encryption: None,
        redaction: RedactionStatus::Unredacted,
        created_at: AgentTimestampMillis::new(1),
        metadata: AgentAttributes::default(),
    }
}

/// A claim that says `text` about `order-42`, not yet appended.
fn worded(scope: &KnowledgeSpaceScope, key: &str, text: &str) -> Claim {
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, key).expect("the operation id derives"),
        ClaimNodeId::new("order-42").expect("the node id is valid"),
        ClaimPredicate::new("status").expect("the predicate is valid"),
        ClaimObject::Value(AgentTaskContent::inline(serde_json::json!(text)).expect("inline")),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        MemoryClassification::Unclassified,
        NOW,
    )
    .expect("the claim constructs")
}

async fn append(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    claim: Claim,
) -> Claim {
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends")
}

async fn move_to(
    store: &dyn KnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    claim: &Claim,
    to: ClaimTrustStatus,
) {
    let request = ClaimTrustTransitionRequest::new(
        claim.claim_id.clone(),
        ClaimOperationId::derive_transition(scope, &claim.claim_id, to.as_label())
            .expect("the operation id derives"),
        to,
        principal(),
        NOW,
    );
    store
        .transition(scope, &request, &ClaimPromotionPolicy::ungated(), NOW)
        .await
        .expect("the transition applies");
}

fn query(text: &str) -> CommunalRetrievalQuery {
    CommunalRetrievalQuery {
        text: text.to_string(),
        limit: 8,
        min_confidence_bps: 0,
        classifications: BTreeSet::from([MemoryClassification::Unclassified]),
        trust: BTreeSet::from([
            AgentCommunalClaimTrust::Proposed,
            AgentCommunalClaimTrust::Verified,
        ]),
        reader: BTreeSet::new(),
    }
}

fn communal_id(claim: &Claim) -> AgentCommunalClaimId {
    AgentCommunalClaimId::new(claim.claim_id.as_str()).expect("the claim id is valid")
}

/// A store that reports a chosen capability set, records each search it is
/// given and answers a chosen report. Every other operation is unreachable in
/// these tests.
struct RecordingStore {
    capabilities: KnowledgeGraphCapabilities,
    report: ClaimSearchReport,
    searches: Mutex<Vec<(KnowledgeSpaceScope, ClaimSearch)>>,
}

impl RecordingStore {
    fn searching(report: ClaimSearchReport) -> Self {
        Self {
            capabilities: KnowledgeGraphCapabilities::core()
                .with_capability(KnowledgeGraphCapability::KeywordSearch),
            report,
            searches: Mutex::new(Vec::new()),
        }
    }

    fn without_search() -> Self {
        Self {
            capabilities: KnowledgeGraphCapabilities::core(),
            report: ClaimSearchReport::default(),
            searches: Mutex::new(Vec::new()),
        }
    }
}

fn unreachable_operation<T>() -> ClaimFuture<'static, T> {
    Box::pin(async {
        Err(ClaimError::Backend {
            backend: "recording".to_string(),
            message: "not an operation these adapters call".to_string(),
        })
    })
}

impl KnowledgeGraphStore for RecordingStore {
    fn backend_name(&self) -> &'static str {
        "recording"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        self.capabilities.clone()
    }

    fn append<'a>(&'a self, _: &'a KnowledgeSpaceScope, _: &'a Claim) -> ClaimFuture<'a, Claim> {
        unreachable_operation()
    }

    fn get<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        unreachable_operation()
    }

    fn query<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimFilter,
        _: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        unreachable_operation()
    }

    fn traverse<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        unreachable_operation()
    }

    fn transition<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimTrustTransitionRequest,
        _: &'a ClaimPromotionPolicy,
        _: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        unreachable_operation()
    }

    fn transitions<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimId,
        _: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        unreachable_operation()
    }

    fn tombstone<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimTombstoneRequest,
    ) -> ClaimFuture<'a, Claim> {
        unreachable_operation()
    }

    fn delete<'a>(
        &'a self,
        _: &'a KnowledgeSpaceScope,
        _: &'a ClaimDeleteRequest,
    ) -> ClaimFuture<'a, ()> {
        unreachable_operation()
    }

    fn search<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        search: &'a ClaimSearch,
    ) -> ClaimFuture<'a, ClaimSearchReport> {
        Box::pin(async move {
            self.searches
                .lock()
                .expect("the double's lock is never poisoned")
                .push((scope.clone(), search.clone()));
            Ok(self.report.clone())
        })
    }
}

// ---------------------------------------------------------------------------
// The retriever.
// ---------------------------------------------------------------------------

#[test]
fn the_retriever_refuses_a_store_that_cannot_search() {
    let refused = KnowledgeGraphCommunalRetriever::new(Arc::new(RecordingStore::without_search()))
        .expect_err("a store that cannot search cannot back a retriever");
    assert_eq!(refused.code(), "claim-capability-unsupported");
    assert!(refused.to_string().contains("keyword-search"));

    let retriever =
        KnowledgeGraphCommunalRetriever::new(Arc::new(InMemoryKnowledgeGraphStore::new()))
            .expect("the in-memory store searches");
    assert_eq!(retriever.backend_name(), "in-memory");
    assert_eq!(retriever.retriever_version(), AgentRevisionNumber::new(1));
}

#[tokio::test]
async fn the_retriever_maps_the_query_onto_a_search() {
    let store = Arc::new(RecordingStore::searching(ClaimSearchReport::default()));
    let retriever =
        KnowledgeGraphCommunalRetriever::new(store.clone()).expect("the store searches");
    let asked = CommunalRetrievalQuery {
        text: "where is my order".to_string(),
        limit: 5,
        min_confidence_bps: 2_500,
        classifications: BTreeSet::from([
            MemoryClassification::Unclassified,
            MemoryClassification::Sensitive,
        ]),
        trust: BTreeSet::from([
            AgentCommunalClaimTrust::Verified,
            AgentCommunalClaimTrust::Disputed,
            // No search is ever asked for a withdrawn claim.
            AgentCommunalClaimTrust::Retracted,
        ]),
        reader: capabilities(&["cap-a", "cap-b"]),
    };

    let outcome = retriever
        .retrieve(&tenant(), &space("s1"), &asked, NOW)
        .await
        .expect("the search answers");
    assert!(outcome.ranked.is_empty());

    let searches = store
        .searches
        .lock()
        .expect("the double's lock is never poisoned");
    assert_eq!(searches.len(), 1);
    let (searched_scope, search) = &searches[0];
    assert_eq!(searched_scope, &scope("s1"));
    assert_eq!(search.text(), "where is my order");
    assert_eq!(search.limit(), 5);
    assert_eq!(search.min_confidence_bps(), 2_500);
    assert_eq!(search.classifications(), &asked.classifications);
    assert_eq!(
        search.trust(),
        &BTreeSet::from([ClaimTrustStatus::Verified, ClaimTrustStatus::Disputed])
    );
    assert_eq!(search.reader(), &asked.reader);
}

#[tokio::test]
async fn the_retriever_answers_identities_and_scores_only() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let s1 = scope("s1");
    let both = append(&*store, &s1, worded(&s1, "both", "delayed customs")).await;
    let one = append(&*store, &s1, worded(&s1, "one", "delayed elsewhere today")).await;
    append(&*store, &s1, worded(&s1, "none", "nothing in common")).await;
    // A claim of another space with the same words is not this space's.
    let s2 = scope("s2");
    append(&*store, &s2, worded(&s2, "other", "delayed customs")).await;
    // A claim the reader may not read takes no place in the ranking.
    let guarded = worded(&s1, "guarded", "delayed customs")
        .with_access(ClaimAccess {
            required_read_capabilities: capabilities(&["cap-secret"]),
        })
        .expect("the access is bounded");
    append(&*store, &s1, guarded).await;

    let retriever =
        KnowledgeGraphCommunalRetriever::new(store.clone()).expect("the store searches");
    let outcome = retriever
        .retrieve(&tenant(), &space("s1"), &query("delayed customs"), NOW)
        .await
        .expect("the search answers");

    assert_eq!(
        outcome
            .ranked
            .iter()
            .map(|ranked| ranked.claim.clone())
            .collect::<Vec<_>>(),
        vec![communal_id(&both), communal_id(&one)],
        "best first, and only what the query admits in the space asked"
    );
    assert!(outcome.ranked[0].relevance_bps > outcome.ranked[1].relevance_bps);
    assert!(outcome
        .ranked
        .iter()
        .all(|ranked| ranked.relevance_bps > 0 && ranked.relevance_bps <= 10_000));
    assert_eq!(outcome.index_watermark, None);
    assert!(!outcome.index_incomplete);

    // A space the store does not hold answers an empty ranking, not an error.
    let empty = retriever
        .retrieve(&tenant(), &space("nowhere"), &query("delayed customs"), NOW)
        .await
        .expect("an unknown space answers");
    assert!(empty.ranked.is_empty());
}

#[tokio::test]
async fn an_unindexed_report_becomes_index_incomplete() {
    let store = Arc::new(RecordingStore::searching(ClaimSearchReport {
        hits: Vec::new(),
        unindexed: true,
    }));
    let retriever = KnowledgeGraphCommunalRetriever::new(store).expect("the store searches");

    let outcome = retriever
        .retrieve(&tenant(), &space("s1"), &query("anything"), NOW)
        .await
        .expect("the search answers");
    assert!(outcome.index_incomplete);
}

// ---------------------------------------------------------------------------
// The authority.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_authority_maps_a_live_claim_field_by_field() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let s1 = scope("s1");
    let claim = Claim::new(
        &s1,
        ClaimOperationId::derive_append(&s1, "full").expect("the operation id derives"),
        ClaimNodeId::new("order-42").expect("the node id is valid"),
        ClaimPredicate::new("status").expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!({ "state": "delayed", "days": 3 }))
                .expect("inline"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid"))
            .with_goal(AgentGoalId::new("goal-1").expect("the goal id is valid"))
            .with_task(AgentTaskId::new("task-1").expect("the task id is valid"))
            .with_run(AgentRunId::new("run-1").expect("the run id is valid"))
            .with_delegation(
                AgentDelegationId::new("delegation-1").expect("the delegation id is valid"),
            ),
        7_300,
        MemoryClassification::Sensitive,
        AgentTimestampMillis::new(77),
    )
    .expect("the claim constructs")
    .with_evidence(vec![evidence("receipt-1"), evidence("receipt-2")])
    .expect("the evidence is bounded")
    .with_access(ClaimAccess {
        required_read_capabilities: capabilities(&["cap-a", "cap-b"]),
    })
    .expect("the access is bounded");
    let claim = append(&*store, &s1, claim).await;
    move_to(&*store, &s1, &claim, ClaimTrustStatus::Verified).await;

    let authority = KnowledgeGraphCommunalAuthority::new(store.clone());
    assert_eq!(authority.backend_name(), "in-memory");
    let record = authority
        .resolve(&tenant(), &space("s1"), &communal_id(&claim), NOW)
        .await
        .expect("the store answers")
        .expect("the claim is held");

    assert_eq!(record.claim_id, communal_id(&claim));
    assert_eq!(record.space, space("s1"));
    assert_eq!(record.statement.subject, "order-42");
    assert_eq!(record.statement.predicate, "status");
    assert_eq!(
        record.statement.object,
        CommunalClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!({ "state": "delayed", "days": 3 }))
                .expect("inline")
        )
    );
    assert_eq!(
        record.statement_digest, claim.content_digest,
        "the record carries the graph's own fingerprint"
    );
    assert_eq!(
        record.statement.digest(),
        claim.content_digest,
        "and the mirror statement fingerprints to the same value"
    );
    assert_eq!(record.trust, AgentCommunalClaimTrust::Verified);
    assert_eq!(record.provenance.agent.as_str(), "scout");
    assert_eq!(
        record.provenance.goal.as_ref().map(|goal| goal.as_str()),
        Some("goal-1")
    );
    assert_eq!(
        record.provenance.task.as_ref().map(|task| task.as_str()),
        Some("task-1")
    );
    assert_eq!(
        record.provenance.run.as_ref().map(|run| run.as_str()),
        Some("run-1")
    );
    assert_eq!(
        record
            .provenance
            .delegation
            .as_ref()
            .map(|delegation| delegation.as_str()),
        Some("delegation-1")
    );
    assert_eq!(record.evidence, claim.evidence);
    assert_eq!(record.evidence.len(), 2);
    assert_eq!(record.created_at, AgentTimestampMillis::new(77));
    assert_eq!(record.confidence_bps, 7_300);
    assert_eq!(record.classification, MemoryClassification::Sensitive);
    assert_eq!(
        record.required_read_capabilities,
        capabilities(&["cap-a", "cap-b"])
    );
    record
        .validate()
        .expect("a record mapped from a live claim is within a record's bounds");

    // An edge maps to a node object, with the same fingerprint.
    let edge = Claim::new(
        &s1,
        ClaimOperationId::derive_append(&s1, "edge").expect("the operation id derives"),
        ClaimNodeId::new("order-42").expect("the node id is valid"),
        ClaimPredicate::new("shipped-by").expect("the predicate is valid"),
        ClaimObject::Node(ClaimNodeId::new("carrier-north").expect("the node id is valid")),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        MemoryClassification::Unclassified,
        NOW,
    )
    .expect("the claim constructs");
    let edge = append(&*store, &s1, edge).await;
    let record = authority
        .resolve(&tenant(), &space("s1"), &communal_id(&edge), NOW)
        .await
        .expect("the store answers")
        .expect("the claim is held");
    assert_eq!(
        record.statement.object,
        CommunalClaimObject::Node("carrier-north".to_string())
    );
    assert_eq!(record.trust, AgentCommunalClaimTrust::Proposed);
    assert_eq!(record.statement.digest(), edge.content_digest);
    record.validate().expect("the edge record validates");
}

#[tokio::test]
async fn the_authority_answers_none_for_a_stub() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let s1 = scope("s1");
    let claim = append(&*store, &s1, worded(&s1, "withdrawn", "delayed customs")).await;
    store
        .tombstone(
            &s1,
            &ClaimTombstoneRequest {
                claim_id: claim.claim_id.clone(),
                operation_id: ClaimOperationId::derive_tombstone(&s1, &claim.claim_id, "withdraw")
                    .expect("the operation id derives"),
                reason: ClaimTombstoneReason::Withdrawn,
                actor: principal(),
                tombstoned_at: AgentTimestampMillis::new(20),
            },
        )
        .await
        .expect("the claim is tombstoned");

    let authority = KnowledgeGraphCommunalAuthority::new(store);
    let answered = authority
        .resolve(&tenant(), &space("s1"), &communal_id(&claim), NOW)
        .await
        .expect("the store answers");
    assert_eq!(answered, None, "a stub has no statement to give");
}

#[tokio::test]
async fn the_authority_answers_none_for_another_scope() {
    let store = Arc::new(InMemoryKnowledgeGraphStore::new());
    let s1 = scope("s1");
    let claim = append(&*store, &s1, worded(&s1, "held", "delayed customs")).await;
    let authority = KnowledgeGraphCommunalAuthority::new(store);

    // Another space of the same tenant.
    assert_eq!(
        authority
            .resolve(&tenant(), &space("s2"), &communal_id(&claim), NOW)
            .await
            .expect("the store answers"),
        None
    );
    // The same space name under another tenant.
    assert_eq!(
        authority
            .resolve(
                &TenantId::new("globex"),
                &space("s1"),
                &communal_id(&claim),
                NOW
            )
            .await
            .expect("the store answers"),
        None
    );
    // An identity the space never held.
    assert_eq!(
        authority
            .resolve(
                &tenant(),
                &space("s1"),
                &AgentCommunalClaimId::new("claim-never-appended").expect("the id is valid"),
                NOW
            )
            .await
            .expect("the store answers"),
        None
    );
    // And the claim itself, where it is held.
    assert!(authority
        .resolve(&tenant(), &space("s1"), &communal_id(&claim), NOW)
        .await
        .expect("the store answers")
        .is_some());
}

// ---------------------------------------------------------------------------
// What is mirrored cannot drift.
// ---------------------------------------------------------------------------

#[test]
fn the_mirrored_bounds_equal_the_graphs() {
    assert_eq!(
        AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES,
        CLAIM_OBJECT_INLINE_MAX_BYTES
    );
    assert_eq!(
        AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE,
        CLAIM_MAX_EVIDENCE_ARTIFACTS
    );
    assert_eq!(
        AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES,
        CLAIM_MAX_ACL_CAPABILITIES
    );
}

#[test]
fn the_trust_labels_round_trip() {
    assert_eq!(
        ClaimTrustStatus::ALL.len(),
        AgentCommunalClaimTrust::ALL.len()
    );
    for status in ClaimTrustStatus::ALL {
        let mirrored = AgentCommunalClaimTrust::from(status);
        assert_eq!(mirrored.as_label(), status.as_label());
        assert_eq!(
            serde_json::to_string(&mirrored).expect("the mirror encodes"),
            serde_json::to_string(&status).expect("the status encodes")
        );
        assert_eq!(
            ClaimTrustStatus::try_from(mirrored).expect("every known state maps back"),
            status
        );
    }
    for mirrored in AgentCommunalClaimTrust::ALL {
        let status = ClaimTrustStatus::try_from(mirrored).expect("every known state maps");
        assert_eq!(AgentCommunalClaimTrust::from(status), mirrored);
    }
}
