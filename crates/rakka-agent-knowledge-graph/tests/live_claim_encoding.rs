//! A live claim encodes to the bytes the build before tombstones wrote.
//!
//! `docs/rakka-compatibility.md` (the issue #68 entry) promises that a live
//! claim "is still written at version 1 and encodes to the same bytes as
//! before" the `tombstone` field existed. A binary from before #68 reads
//! every live claim only because that holds, which is what makes a rollback
//! to it clean while no stub exists.
//!
//! The literals below the fixtures were printed by running the fixture
//! section of this file, unchanged, at `d12b0f8` (the merge of #129, the last
//! commit before #68). Each claim is compared against them as `Claim` and as
//! `ClaimRecord` (the mirror the PostgreSQL backend stores), and decoded from
//! them through both doors. The furnished edge claim sets every optional
//! field; the two value claims leave them unset, so the `Some` and the empty
//! encodings are both held, and each object shape a live claim can take is
//! held once. The verified and retracted claims complete the spellings: with
//! the others, the five hold every trust status, every classification, every
//! artifact kind and every redaction status, so renaming any one variant
//! fails here. Inline JSON in a fixture keeps its keys in sorted order, so the
//! bytes do not depend on `serde_json`'s `preserve_order` feature.
//!
//! The PostgreSQL backend's side is
//! `postgres_stores_a_live_claim_as_its_canonical_record_bytes_when_dsn_is_set`
//! in `rakka-agent-knowledge-graph-postgres`: it holds the bytes that backend
//! stores to the encoding pinned here.

use rakka_agent::{
    AgentCapabilityId, AgentDelegationId, AgentGoalId, AgentId, AgentPolicyRef,
    AgentRevisionNumber, AgentRunId, AgentTaskContent, AgentTaskId, KnowledgeSpaceId,
    MemoryClassification, MemoryEmbeddingRef, TenantId,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimAccess, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimProvenance, ClaimTrustStatus, KnowledgeSpaceScope,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentEffectId, AgentTimestampMillis, ArtifactEncryptionRef, ArtifactKind,
    ArtifactRef, RedactionStatus,
};
use serde_json::json;

fn scope() -> KnowledgeSpaceScope {
    KnowledgeSpaceScope::new(
        TenantId::new("acme"),
        KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
    )
    .expect("the scope is valid")
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

fn provenance() -> ClaimProvenance {
    ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid"))
}

fn node(id: &str) -> ClaimNodeId {
    ClaimNodeId::new(id).expect("the node id is valid")
}

fn predicate(name: &str) -> ClaimPredicate {
    ClaimPredicate::new(name).expect("the predicate is valid")
}

fn append(discriminator: &str) -> ClaimOperationId {
    ClaimOperationId::derive_append(&scope(), discriminator).expect("the operation id derives")
}

/// An edge claim with every optional field set and one trust transition.
fn furnished_edge_claim() -> Claim {
    let provenance = provenance()
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
        &scope(),
        append("edge-claim"),
        node("customer-1"),
        predicate("prefers"),
        ClaimObject::Node(node("channel-email")),
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
    .apply_transition(ClaimTrustStatus::Disputed)
    .expect("the dispute applies")
}

/// A value claim whose object is inline JSON; every optional field unset.
fn inline_value_claim() -> Claim {
    let content = AgentTaskContent::inline(json!({
        "days": ["mon", "tue"],
        "since": 2024,
        "window": { "end": "17:00", "start": "09:00" }
    }))
    .expect("the inline content is within bounds");
    Claim::new(
        &scope(),
        append("inline-claim"),
        node("customer-1"),
        predicate("prefers-hours"),
        ClaimObject::Value(content),
        provenance(),
        6_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1_700_000_000_002),
    )
    .expect("the claim is valid")
}

/// A value claim whose object is artifact content; every optional field unset.
fn artifact_value_claim() -> Claim {
    Claim::new(
        &scope(),
        append("artifact-claim"),
        node("customer-1"),
        predicate("has-contract"),
        ClaimObject::Value(AgentTaskContent::artifact(furnished_artifact("contract"))),
        provenance(),
        9_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1_700_000_000_003),
    )
    .expect("the claim is valid")
}

/// An artifact reference with every optional field unset.
fn spelled_artifact(id: &str, kind: ArtifactKind, redaction: RedactionStatus) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind,
        uri: format!("s3://evidence/{id}"),
        checksum: None,
        content_type: None,
        byte_len: None,
        retention_class: None,
        encryption: None,
        redaction,
        created_at: AgentTimestampMillis::new(1_700_000_000_000),
        metadata: AgentAttributes::new(),
    }
}

/// A verified claim that spells, between its evidence, classification and
/// inline value, every artifact kind, every redaction status, the redacted
/// classification, and a float, a boolean, a null and escaped characters.
fn verified_claim() -> Claim {
    let kinds = [
        ArtifactKind::Input,
        ArtifactKind::Prompt,
        ArtifactKind::Completion,
        ArtifactKind::File,
        ArtifactKind::Embedding,
        ArtifactKind::ToolOutput,
        ArtifactKind::Screenshot,
        ArtifactKind::Log,
        ArtifactKind::State,
        ArtifactKind::Other,
    ];
    let redactions = [
        RedactionStatus::Unknown,
        RedactionStatus::Unredacted,
        RedactionStatus::Redacted,
        RedactionStatus::ReferenceOnly,
    ];
    let evidence = kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            spelled_artifact(
                &format!("e{index}"),
                kind,
                redactions[index % redactions.len()],
            )
        })
        .collect();
    let content = AgentTaskContent::inline(json!({
        "confidence": 0.875,
        "language": "fr \"Québec\" \\ x\n\u{7}",
        "native": true,
        "notes": null
    }))
    .expect("the inline content is within bounds");
    Claim::new(
        &scope(),
        append("verified-claim"),
        node("customer-2"),
        predicate("speaks"),
        ClaimObject::Value(content),
        provenance(),
        8_000,
        MemoryClassification::Redacted,
        AgentTimestampMillis::new(1_700_000_000_004),
    )
    .expect("the claim is valid")
    .with_evidence(evidence)
    .expect("the evidence is within bounds")
    .apply_transition(ClaimTrustStatus::Verified)
    .expect("the verification applies")
}

/// A retracted claim, the trust status's terminal spelling.
fn retracted_claim() -> Claim {
    Claim::new(
        &scope(),
        append("retracted-claim"),
        node("customer-3"),
        predicate("prefers"),
        ClaimObject::Node(node("channel-phone")),
        provenance(),
        5_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1_700_000_000_005),
    )
    .expect("the claim is valid")
    .apply_transition(ClaimTrustStatus::Disputed)
    .expect("the dispute applies")
    .apply_transition(ClaimTrustStatus::Retracted)
    .expect("the retraction applies")
}

// ---- The bytes the build at d12b0f8 wrote ----
//
// Printed by running everything above this line, unchanged, at `d12b0f8`.
// Never regenerate them from a later build: a literal printed by the build
// under test proves only that the build agrees with itself.

const FURNISHED_EDGE_CLAIM: &str = r#"{"schema_version":1,"claim_id":"claim-d14f9e1d1e4d581453ec500e3079c9304187c3287e65fb9b26ed1b942a39e31b","operation_id":"claim-op-41769795f4bb17f2d2258f958a0e8817a8632cd73c5cb0a8480f8c54fefb7cfe","subject":"customer-1","predicate":"prefers","object":{"node":"channel-email"},"content_digest":{"algorithm":"fnv1a128","value":"a8cf971c9e8377182ca8b4781becb100"},"provenance":{"agent":"scout","goal":"goal-1","task":"task-1","run":"task-1-gen-1","delegation":"delegation-1","effect":"effect-1"},"evidence":[{"artifact_id":"a","kind":"file","uri":"s3://evidence/a","checksum":"sha256:a","content_type":"application/json","byte_len":64,"retention_class":"legal-hold-7y","encryption":{"algorithm":"aes-256-gcm","key_ref":"kms://keys/evidence","context":{"purpose":"evidence"}},"redaction":"redacted","created_at":1700000000000,"metadata":{"source":"ticket-system"}},{"artifact_id":"b","kind":"file","uri":"s3://evidence/b","checksum":"sha256:b","content_type":"application/json","byte_len":64,"retention_class":"legal-hold-7y","encryption":{"algorithm":"aes-256-gcm","key_ref":"kms://keys/evidence","context":{"purpose":"evidence"}},"redaction":"redacted","created_at":1700000000000,"metadata":{"source":"ticket-system"}}],"created_at":1700000000001,"confidence_bps":7500,"classification":"sensitive","access":{"required_read_capabilities":["read-pii","read-support"]},"trust":"disputed","transition_count":1,"embedding":{"model":"embedder-v1","dimensions":3,"version":2},"policy":"retention-v1","policy_revision":3}"#;
const INLINE_VALUE_CLAIM: &str = r#"{"schema_version":1,"claim_id":"claim-b131eabd63380ba7f856f58bf4c12aed893215ad4367aeabf932a3eb32cbd66d","operation_id":"claim-op-3e7b090c3c34d05693ace4bd38b085733616cbb4de95322150d10f28efa71a9e","subject":"customer-1","predicate":"prefers-hours","object":{"value":{"inline":{"days":["mon","tue"],"since":2024,"window":{"end":"17:00","start":"09:00"}}}},"content_digest":{"algorithm":"fnv1a128","value":"905bb49f5ba28a5764ec1462043fdb23"},"provenance":{"agent":"scout","goal":null,"task":null,"run":null,"delegation":null,"effect":null},"evidence":[],"created_at":1700000000002,"confidence_bps":6000,"classification":"unclassified","access":{"required_read_capabilities":[]},"trust":"proposed","transition_count":0,"embedding":null,"policy":null,"policy_revision":null}"#;
const ARTIFACT_VALUE_CLAIM: &str = r#"{"schema_version":1,"claim_id":"claim-7a41cf1ac04ef3e4f8f156a30864961980cbeb06f2a33e09984013c540eb06d5","operation_id":"claim-op-2f0e6a3090df5756f48f52d2b35ddac6a4e6da7bf013837b4c69dac44659f067","subject":"customer-1","predicate":"has-contract","object":{"value":{"artifact":{"artifact_id":"contract","kind":"file","uri":"s3://evidence/contract","checksum":"sha256:contract","content_type":"application/json","byte_len":64,"retention_class":"legal-hold-7y","encryption":{"algorithm":"aes-256-gcm","key_ref":"kms://keys/evidence","context":{"purpose":"evidence"}},"redaction":"redacted","created_at":1700000000000,"metadata":{"source":"ticket-system"}}}},"content_digest":{"algorithm":"fnv1a128","value":"867753198b79780574dc3c9d6fdf3e97"},"provenance":{"agent":"scout","goal":null,"task":null,"run":null,"delegation":null,"effect":null},"evidence":[],"created_at":1700000000003,"confidence_bps":9000,"classification":"unclassified","access":{"required_read_capabilities":[]},"trust":"proposed","transition_count":0,"embedding":null,"policy":null,"policy_revision":null}"#;
const VERIFIED_CLAIM: &str = r#"{"schema_version":1,"claim_id":"claim-450653030100c2dc50ea4ddc0a36c8337206becca8b96c51d201edfc46fc533d","operation_id":"claim-op-bda60d93cf11e2c32c0ba48fa3a9ad91ae87ab03236f888c489b2e012b6be592","subject":"customer-2","predicate":"speaks","object":{"value":{"inline":{"confidence":0.875,"language":"fr \"Québec\" \\ x\n\u0007","native":true,"notes":null}}},"content_digest":{"algorithm":"fnv1a128","value":"e52f1569ca61a215de3d0e7012d685b2"},"provenance":{"agent":"scout","goal":null,"task":null,"run":null,"delegation":null,"effect":null},"evidence":[{"artifact_id":"e0","kind":"input","uri":"s3://evidence/e0","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"unknown","created_at":1700000000000,"metadata":{}},{"artifact_id":"e1","kind":"prompt","uri":"s3://evidence/e1","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"unredacted","created_at":1700000000000,"metadata":{}},{"artifact_id":"e2","kind":"completion","uri":"s3://evidence/e2","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"redacted","created_at":1700000000000,"metadata":{}},{"artifact_id":"e3","kind":"file","uri":"s3://evidence/e3","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"reference-only","created_at":1700000000000,"metadata":{}},{"artifact_id":"e4","kind":"embedding","uri":"s3://evidence/e4","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"unknown","created_at":1700000000000,"metadata":{}},{"artifact_id":"e5","kind":"tool-output","uri":"s3://evidence/e5","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"unredacted","created_at":1700000000000,"metadata":{}},{"artifact_id":"e6","kind":"screenshot","uri":"s3://evidence/e6","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"redacted","created_at":1700000000000,"metadata":{}},{"artifact_id":"e7","kind":"log","uri":"s3://evidence/e7","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"reference-only","created_at":1700000000000,"metadata":{}},{"artifact_id":"e8","kind":"state","uri":"s3://evidence/e8","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"unknown","created_at":1700000000000,"metadata":{}},{"artifact_id":"e9","kind":"other","uri":"s3://evidence/e9","checksum":null,"content_type":null,"byte_len":null,"retention_class":null,"encryption":null,"redaction":"unredacted","created_at":1700000000000,"metadata":{}}],"created_at":1700000000004,"confidence_bps":8000,"classification":"redacted","access":{"required_read_capabilities":[]},"trust":"verified","transition_count":1,"embedding":null,"policy":null,"policy_revision":null}"#;
const RETRACTED_CLAIM: &str = r#"{"schema_version":1,"claim_id":"claim-f81093ba4666d85d2347cda56e095231e97f570afaca1d4868365ad12383f7b8","operation_id":"claim-op-595e4bb71fddb5c00912e051eb5d89d75cb5191381a79e2a45abf7277e256ab4","subject":"customer-3","predicate":"prefers","object":{"node":"channel-phone"},"content_digest":{"algorithm":"fnv1a128","value":"04af76daad525acf31a41fbbab6aa54a"},"provenance":{"agent":"scout","goal":null,"task":null,"run":null,"delegation":null,"effect":null},"evidence":[],"created_at":1700000000005,"confidence_bps":5000,"classification":"unclassified","access":{"required_read_capabilities":[]},"trust":"retracted","transition_count":2,"embedding":null,"policy":null,"policy_revision":null}"#;

use rakka_agent_knowledge_graph::{ClaimRecord, CLAIM_LIVE_SCHEMA_VERSION};

/// What a failure here means, for whoever meets one.
const BROKEN_PROMISE: &str = "a live claim no longer encodes to the bytes the build \
    before tombstones (d12b0f8) wrote. docs/rakka-compatibility.md (the issue #68 \
    entry) promises it does, and a binary from before #68 reads live claims only \
    because it holds. Do not update the literal: restore the encoding, or make the \
    change a schema bump with its own rollout";

/// Holds one claim to its literal: encoded as `Claim` and as `ClaimRecord`,
/// and decoded through `Claim`'s own serde and through `Claim::restore` (the
/// PostgreSQL backend's decode path).
fn assert_pinned(name: &str, claim: &Claim, literal: &str) {
    assert_eq!(
        serde_json::to_string(claim).expect("the claim serializes"),
        literal,
        "{name}, encoded as Claim: {BROKEN_PROMISE}"
    );
    assert_eq!(
        serde_json::to_string(&claim.to_record()).expect("the record serializes"),
        literal,
        "{name}, encoded as ClaimRecord: {BROKEN_PROMISE}"
    );

    let decoded: Claim = serde_json::from_str(literal)
        .unwrap_or_else(|error| panic!("{name}, decoded as Claim: {error}: {BROKEN_PROMISE}"));
    assert_eq!(
        &decoded, claim,
        "{name}, decoded as Claim: {BROKEN_PROMISE}"
    );
    let record: ClaimRecord = serde_json::from_str(literal).unwrap_or_else(|error| {
        panic!("{name}, decoded as ClaimRecord: {error}: {BROKEN_PROMISE}")
    });
    let restored = Claim::restore(record)
        .unwrap_or_else(|error| panic!("{name}, restored: {error}: {BROKEN_PROMISE}"));
    assert_eq!(&restored, claim, "{name}, restored: {BROKEN_PROMISE}");

    assert_eq!(
        decoded.schema_version(),
        CLAIM_LIVE_SCHEMA_VERSION,
        "{name}"
    );
    assert_eq!(decoded.schema_version().get(), 1, "{name}");
    assert_eq!(decoded.tombstone(), None, "{name}");
}

#[test]
fn a_furnished_edge_claim_encodes_to_the_bytes_written_before_tombstones() {
    let claim = furnished_edge_claim();
    // The fixture keeps testing what it says it does: a moved trust status.
    assert_eq!(claim.trust(), ClaimTrustStatus::Disputed);
    assert_eq!(claim.transition_count(), 1);
    assert_pinned("furnished edge claim", &claim, FURNISHED_EDGE_CLAIM);
}

#[test]
fn an_inline_value_claim_encodes_to_the_bytes_written_before_tombstones() {
    assert_pinned(
        "inline value claim",
        &inline_value_claim(),
        INLINE_VALUE_CLAIM,
    );
}

#[test]
fn an_artifact_value_claim_encodes_to_the_bytes_written_before_tombstones() {
    assert_pinned(
        "artifact value claim",
        &artifact_value_claim(),
        ARTIFACT_VALUE_CLAIM,
    );
}

#[test]
fn a_verified_claim_encodes_to_the_bytes_written_before_tombstones() {
    let claim = verified_claim();
    // The fixture keeps spelling what it says it does.
    assert_eq!(claim.trust(), ClaimTrustStatus::Verified);
    assert_eq!(claim.classification, MemoryClassification::Redacted);
    assert_eq!(claim.evidence.len(), 10, "one artifact of every kind");
    assert_pinned("verified claim", &claim, VERIFIED_CLAIM);
}

#[test]
fn a_retracted_claim_encodes_to_the_bytes_written_before_tombstones() {
    let claim = retracted_claim();
    assert_eq!(claim.trust(), ClaimTrustStatus::Retracted);
    assert_eq!(claim.transition_count(), 2);
    assert_pinned("retracted claim", &claim, RETRACTED_CLAIM);
}
