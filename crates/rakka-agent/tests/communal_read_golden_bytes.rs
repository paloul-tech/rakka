//! The bytes a record had before the communal read path, pinned.
//!
//! Issue #69 adds read capabilities to the authority envelope and to every
//! delegation record, and a communal selection to the context snapshot. Each
//! addition is omitted when empty, so a record written without the feature in
//! use keeps the bytes it had at `86cf087`. These literals were captured from
//! that commit and must never be edited: a failure here means a record that
//! does not use the read path changed shape.
//!
//! The delegation record and provenance are decoded from their literal and
//! encoded again rather than built from a struct literal, so this file needs no
//! edit when those records gain a field.

use rakka_agent::{
    assemble_session_context, AgentAuthorityEnvelope, AgentCapabilityId, AgentContextSnapshotRef,
    AgentDelegationRecord, AgentEffectSafetyClass, AgentId, AgentRevisionNumber, AgentRunId,
    AgentRunScope, AgentTaskContent, AgentTaskDelegationProvenance, AgentToolDeclaration,
    AgentToolId, InMemorySessionMemoryStore, KnowledgeSpaceId, MemoryClassification, MemoryEntryId,
    MemoryEntryRole, MemoryOperationId, MemorySequence, SessionMemoryEntry, SessionMemoryStore,
    SessionWindowPolicy, TenantId,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::json;

const SNAPSHOT_86CF087: &str = r#"{"schema_version":1,"reference":{"snapshot_id":"run-d2abeded3ee1ea765af33b450618e893-turn-1","version":1},"scope":"acme/support/run-1","turn":1,"trust":"untrusted","session":[{"entry_id":"mem-entry-5e525e83e3de8612274af7a53a613378","sequence":1,"role":"user","content":{"inline":{"text":"where is my order"}},"content_digest":{"algorithm":"fnv1a128","value":"0ea3f86118927dc787a4bf4c6b3838bd"},"classification":"unclassified"}],"retrievals":[{"query":"session-window:turn-1","retriever":"session-window","retriever_version":1,"index_watermark":null}],"private_memory":[],"communal_claims":[],"policy_revision":1,"ingress_revision":null,"budget":{"session_entries":1,"session_bytes":39,"private_memories":0,"private_memory_bytes":0,"communal_claims":0},"content_digest":{"algorithm":"fnv1a128","value":"bc0d9ceea6e01314484cbe5ee53e9b7a"},"assembled_at":10}"#;

const SNAPSHOT_DIGEST_86CF087: &str =
    r#"{"algorithm":"fnv1a128","value":"bc0d9ceea6e01314484cbe5ee53e9b7a"}"#;

const ENVELOPE_86CF087: &str = r#"{"tools":{"search":{"safety":"read-only","capabilities":["net.read"],"credential_binding":null,"execution_policy":null,"environments":[]}},"model_profiles":[],"workflow_tools":[],"task_definitions":[],"collaborators":[],"knowledge_spaces":["support-kb"],"environments":[],"credential_bindings":[],"coordination_capabilities":[],"operation_classes":[],"mandatory_guardrails":[],"budgets":{"max_loop_iterations":null,"max_model_calls":null,"max_tool_calls":null,"max_effects":null,"max_effect_attempts":null,"max_tokens":null,"max_cost_micros":null,"max_wall_clock_millis":null,"max_concurrent_effects":null}}"#;

const DELEGATION_RECORD_86CF087: &str = r#"{"delegation":"delegation-cbc97344659bdaa3d6ff2e41e556895ee9f57e960a93c4245ea92ab0488ec14c","goal":null,"parent_task":"goal-root","parent_run":"acme/support/run-1","lineage":[],"depth":1,"requested_skill":"billing.refund","resolved":{"agent":"billing","endpoint":null,"task_definition":"refund","capability_scopes":[],"credential_bindings":[],"result_schema":null,"compatibility":null,"knowledge_spaces":["support-kb"]},"a2a_message_id":"delegation-cbc97344659bdaa3d6ff2e41e556895ee9f57e960a93c4245ea92ab0488ec14c","deduplication_key":"delegation-cbc97344659bdaa3d6ff2e41e556895ee9f57e960a93c4245ea92ab0488ec14c","turn":1,"slot":0,"effect":"effect-1","call_id":"call-1","input":{"inline":{"text":"hello"}},"result_schema":null,"budget":null,"granted_descendants":3,"deadline":null,"knowledge_spaces":["support-kb"],"definition_revision":1,"settings_revision":1,"telemetry":{"trace_parent":null,"trace_state":null,"baggage":{},"span_links":[]},"created_at":1}"#;

const DELEGATION_PROVENANCE_86CF087: &str = r#"{"delegation":"delegation-cbc97344659bdaa3d6ff2e41e556895ee9f57e960a93c4245ea92ab0488ec14c","parent_task":"goal-root","parent_run":"acme/support/run-1","lineage":[],"depth":1,"requested_skill":"billing.refund","capability_scopes":[],"credential_bindings":[],"result_schema":null,"budget":null,"deadline":null,"knowledge_spaces":["support-kb"]}"#;

fn scope() -> AgentRunScope {
    AgentRunScope::new(
        TenantId::new("acme"),
        AgentId::new("support").expect("the agent id is valid"),
        AgentRunId::new("run-1").expect("the run id is valid"),
    )
    .expect("the scope is valid")
}

/// A snapshot assembled with no communal search is a version 1 record with the
/// bytes and the digest it had before the read path existed.
#[tokio::test]
async fn a_version_1_snapshot_keeps_its_bytes_and_digest() {
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

    let snapshot = assemble_session_context(
        &session,
        &scope,
        &reference,
        1,
        &SessionWindowPolicy::recent_window(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(10),
    )
    .await
    .expect("the snapshot assembles");

    assert_eq!(
        serde_json::to_string(&snapshot).expect("the snapshot encodes"),
        SNAPSHOT_86CF087
    );
    assert_eq!(
        serde_json::to_string(&snapshot.content_digest).expect("the digest encodes"),
        SNAPSHOT_DIGEST_86CF087
    );
}

/// An authority envelope that grants no read capability has the bytes it had
/// before the grant existed.
#[test]
fn an_authority_envelope_keeps_its_bytes() {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.tools.insert(
        AgentToolId::new("search").expect("the tool id is valid"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)
            .with_capability(AgentCapabilityId::new("net.read").expect("the capability is valid")),
    );
    envelope
        .knowledge_spaces
        .insert(KnowledgeSpaceId::new("support-kb").expect("the space id is valid"));

    assert_eq!(
        serde_json::to_string(&envelope).expect("the envelope encodes"),
        ENVELOPE_86CF087
    );
}

/// A delegation record and a task's delegation provenance written before read
/// capabilities existed decode, and encode again to the same bytes.
#[test]
fn a_delegation_record_and_provenance_keep_their_bytes() {
    let record: AgentDelegationRecord =
        serde_json::from_str(DELEGATION_RECORD_86CF087).expect("the earlier record decodes");
    assert_eq!(
        serde_json::to_string(&record).expect("the record encodes"),
        DELEGATION_RECORD_86CF087
    );

    let provenance: AgentTaskDelegationProvenance =
        serde_json::from_str(DELEGATION_PROVENANCE_86CF087)
            .expect("the earlier provenance decodes");
    assert_eq!(
        serde_json::to_string(&provenance).expect("the provenance encodes"),
        DELEGATION_PROVENANCE_86CF087
    );
}
