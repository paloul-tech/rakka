//! Fail-closed schema compatibility for persisted agent records.
//!
//! Specification: section 20. A durable record written by a binary this one does
//! not understand is rejected rather than interpreted with guessed semantics, and
//! that check runs on the load path — not only where a record is constructed —
//! because that is where an N/N+1 rolling update actually meets a foreign record.

use rakka_agent::{
    load_agent_entity_state, AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId,
    AgentDefinitionRevision, AgentEntityState, AgentEntityStore, AgentId, AgentRecordKind,
    AgentRevisionProvenance, AgentSchemaCompatibility, AgentSchemaPolicy, AgentScope,
    AgentSettings, SettingsRevision, TenantId, CURRENT_AGENT_DEFINITION_SCHEMA_VERSION,
    CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION, CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
    CURRENT_AGENT_SETUP_SCHEMA_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef, StateSchemaVersion,
};
use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore, Revision};
use serde_json::{json, Value};

type Store = InMemoryDurableStateStore<AgentEntityState>;

fn scope() -> AgentScope {
    AgentScope::new(
        TenantId::new("acme"),
        AgentId::new("support-agent").expect("agent id should be valid"),
    )
    .expect("agent scope should be valid")
}

fn provenance() -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "control-plane".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(1),
        causation_id: AgentCausationId::new("cause-1"),
        audit_ref: AgentAuditEventId::new("audit-1"),
    }
}

fn state() -> AgentEntityState {
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
        "Resolves customer support tickets end to end.",
        AgentAuthorityEnvelope::empty(),
    )
    .expect("definition should be valid");
    let definition = AgentDefinitionRevision::initial(definition, provenance());
    let settings = SettingsRevision::initial(&definition, AgentSettings::default(), provenance())
        .expect("initial settings should be accepted");
    AgentEntityState::new(scope(), definition, settings, AgentTimestampMillis::new(1))
}

/// Rewrites one schema version inside a persisted record, the way a newer or
/// older peer binary would have written it.
fn state_with_schema_version(pointer: &[&str], version: u64) -> AgentEntityState {
    let mut value: Value = serde_json::to_value(state()).expect("state should serialize");
    let mut cursor = &mut value;
    for key in pointer {
        cursor = cursor
            .get_mut(*key)
            .unwrap_or_else(|| panic!("the persisted record should carry a {key} field"));
    }
    *cursor = json!(version);
    serde_json::from_value(value).expect("the tampered record should still deserialize")
}

async fn store_with(state: AgentEntityState) -> Store {
    let store = Store::new();
    store
        .compare_and_set(&scope().persistence_id(), Revision::INITIAL, state)
        .await
        .expect("the record should persist");
    store
}

#[test]
fn the_default_policy_reads_the_version_it_writes_and_the_one_before_it() {
    let policy = AgentSchemaPolicy::default();

    for (kind, current) in [
        (
            AgentRecordKind::EntityState,
            CURRENT_AGENT_ENTITY_STATE_SCHEMA_VERSION,
        ),
        (
            AgentRecordKind::DefinitionRevision,
            CURRENT_AGENT_DEFINITION_SCHEMA_VERSION,
        ),
        (
            AgentRecordKind::SettingsRevision,
            CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
        ),
        (
            AgentRecordKind::SetupRevision,
            CURRENT_AGENT_SETUP_SCHEMA_VERSION,
        ),
    ] {
        assert!(
            policy.check(kind, current).is_ok(),
            "{kind} must read the version it writes"
        );
        assert_eq!(
            policy
                .check(kind, StateSchemaVersion::new(current.get() + 1))
                .expect_err("a record from a newer peer must fail closed")
                .code(),
            "schema-version-ahead"
        );
    }
}

#[test]
fn a_record_older_than_the_supported_window_fails_closed() {
    // A binary at version 3 reads 3 and 2. A record still at version 1 must be
    // backfilled before this deployment rolls, not read with guessed semantics.
    let policy = AgentSchemaPolicy::new(
        AgentSchemaCompatibility::n_plus_one(StateSchemaVersion::new(3)),
        AgentSchemaCompatibility::n_plus_one(StateSchemaVersion::new(3)),
        AgentSchemaCompatibility::n_plus_one(StateSchemaVersion::new(3)),
        AgentSchemaCompatibility::n_plus_one(StateSchemaVersion::new(3)),
    );
    let window = policy.compatibility(AgentRecordKind::EntityState);
    assert_eq!(window.current(), StateSchemaVersion::new(3));
    assert_eq!(window.minimum_supported(), StateSchemaVersion::new(2));

    assert!(policy
        .check(AgentRecordKind::EntityState, StateSchemaVersion::new(2))
        .is_ok());
    assert_eq!(
        policy
            .check(AgentRecordKind::EntityState, StateSchemaVersion::new(1))
            .expect_err("a record below the window must fail closed")
            .code(),
        "schema-version-too-old"
    );
}

#[tokio::test]
async fn loading_an_entity_state_from_a_newer_binary_fails_closed() {
    let store = store_with(state_with_schema_version(&["schema_version"], 2)).await;

    let error = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect_err("a state record from a newer binary must not be interpreted");
    assert_eq!(error.code(), "schema-version-ahead");
}

#[tokio::test]
async fn a_nested_definition_or_settings_version_is_checked_too() {
    // The entity state, its definition revision, and its settings revision version
    // independently. A state whose envelope is readable but whose nested definition
    // is not must still fail closed.
    let store = store_with(state_with_schema_version(
        &["definition", "schema_version"],
        7,
    ))
    .await;
    let error = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect_err("a definition revision from a newer binary must not be interpreted");
    assert_eq!(error.code(), "schema-version-ahead");

    let store = store_with(state_with_schema_version(
        &["settings", "schema_version"],
        7,
    ))
    .await;
    let error = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect_err("a settings revision from a newer binary must not be interpreted");
    assert_eq!(error.code(), "schema-version-ahead");
}

#[tokio::test]
async fn the_entity_store_recovery_path_fails_closed_as_well() {
    // The read path used by assignment and the recovery path used by the entity
    // actor apply the same check, so neither can advance against a record it
    // cannot interpret.
    let store = store_with(state_with_schema_version(&["schema_version"], 2)).await;

    let mut entity = AgentEntityStore::new(scope(), store);
    let error = entity
        .recover()
        .await
        .expect_err("recovery must not interpret a record from a newer binary");
    assert_eq!(error.code(), "schema-version-ahead");
}

#[tokio::test]
async fn a_supported_record_loads() {
    let store = store_with(state()).await;

    let loaded = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("a current record should load")
        .expect("the record exists");
    assert_eq!(loaded.scope(), &scope());

    let missing = load_agent_entity_state(&Store::new(), &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("an absent record is not an error");
    assert!(missing.is_none());
}
