//! Identity, scope-key, and stable-operation-id contracts.
//!
//! Specification: sections 6.1 through 6.10. These are contract tests: they pin
//! the properties later slices depend on — that a composite scope key is
//! injective and round-trips through a sharding entity id, that a run's task
//! binding cannot be re-targeted, and that the same logical operation always
//! derives the same deduplication identity.

use rakka_agent::{
    AgentDelegationId, AgentEnvironmentRef, AgentGoalId, AgentId, AgentOperationId,
    AgentOperationKind, AgentRunBinding, AgentRunId, AgentRunScope, AgentScope, AgentTaskId,
    AgentTaskScope, AgentWakeId, KnowledgeSpaceId, TenantId, AGENT_IDENTITY_MAX_LENGTH,
    AGENT_PERSISTENCE_SEPARATOR,
};
use rakka_persistence::PERSISTENCE_ID_SEPARATOR;

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn agent() -> AgentId {
    AgentId::new("support-agent").expect("agent id should be valid")
}

fn scope() -> AgentScope {
    AgentScope::new(tenant(), agent()).expect("agent scope should be valid")
}

#[test]
fn persistence_id_separator_is_reserved() {
    // The reserved character list is only correct as long as it matches the
    // separator `rakka-persistence` actually uses to compose a persistence id.
    assert_eq!(
        PERSISTENCE_ID_SEPARATOR,
        AGENT_PERSISTENCE_SEPARATOR.to_string(),
        "the reserved persistence separator must track rakka-persistence"
    );
}

#[test]
fn identifiers_reject_values_that_cannot_key_a_composite_scope() {
    assert_eq!(
        AgentId::new("")
            .expect_err("an empty agent id is not addressable")
            .code(),
        "empty-identifier"
    );
    assert_eq!(
        AgentId::new("tenant/agent")
            .expect_err("the scope separator would alias two scope keys")
            .code(),
        "reserved-identifier-character"
    );
    assert_eq!(
        AgentId::new("agent|state")
            .expect_err("the persistence separator would alias two durable records")
            .code(),
        "reserved-identifier-character"
    );
    assert_eq!(
        AgentId::new("agent\nid")
            .expect_err("a control character is not a legal identifier")
            .code(),
        "control-character-in-identifier"
    );
    assert_eq!(
        AgentId::new("a".repeat(AGENT_IDENTITY_MAX_LENGTH + 1))
            .expect_err("an unbounded identifier is not a bounded durable key")
            .code(),
        "identifier-too-long"
    );

    assert!(AgentId::new("a".repeat(AGENT_IDENTITY_MAX_LENGTH)).is_ok());
    assert!(AgentId::new("support-agent.v2:eu-west-1").is_ok());
}

#[test]
fn identifiers_fail_closed_on_deserialization() {
    // A value that construction rejects must not sneak in through a durable
    // record or a remote envelope.
    let error = serde_json::from_str::<AgentId>("\"tenant/agent\"")
        .expect_err("deserialization must reject a value construction rejects");
    assert!(
        error.to_string().contains("reserved scope character"),
        "unexpected error: {error}"
    );

    let agent: AgentId = serde_json::from_str("\"support-agent\"").expect("valid id should load");
    assert_eq!(agent.as_str(), "support-agent");
}

#[test]
fn distinct_identity_types_do_not_interchange_on_equal_values() {
    // Section 6.3 and 6.4 permit a goal id and a task id to be generated from the
    // same value. The types must still not be substitutable, which is what keeps
    // goal coordination free to move to its own entity later.
    let task = AgentTaskId::new("work-1").expect("task id should be valid");
    let goal = AgentGoalId::new("work-1").expect("goal id should be valid");
    assert_eq!(task.as_str(), goal.as_str());

    // The remaining identities exist from M1 so that M2 through M5 inherit stable
    // scope keys rather than migrating durable records.
    assert!(AgentRunId::new("run-1").is_ok());
    assert!(AgentDelegationId::new("delegation-1").is_ok());
    assert!(AgentWakeId::new("wake-1").is_ok());
    assert!(AgentEnvironmentRef::new("workspace-1").is_ok());
    assert!(KnowledgeSpaceId::new("space-1").is_ok());
}

#[test]
fn agent_scope_round_trips_through_its_sharding_entity_id() {
    let scope = scope();
    assert_eq!(scope.key(), "acme/support-agent");

    let parsed = AgentScope::from_entity_id(&scope.entity_id())
        .expect("an entity id this crate minted must parse back");
    assert_eq!(parsed, scope);

    assert_eq!(
        scope.persistence_id().as_str(),
        "agent-entity:acme/support-agent"
    );
    assert_eq!(
        scope.memory_namespace().as_str(),
        "agent-memory/acme/support-agent"
    );
}

#[test]
fn scope_keys_are_injective_across_tenants() {
    // Without validated segments, ("acme/support", "agent") and ("acme",
    // "support/agent") would flatten to the same durable key and one tenant would
    // read another's state.
    let first = AgentScope::new(TenantId::new("acme"), agent()).expect("scope should be valid");
    let second = AgentScope::new(TenantId::new("acme-eu"), agent()).expect("scope should be valid");
    assert_ne!(first.key(), second.key());

    let colliding = AgentScope::new(TenantId::new("acme/support"), agent())
        .expect_err("a tenant carrying the scope separator must be rejected");
    assert_eq!(colliding.code(), "reserved-identifier-character");
    assert_eq!(colliding.field(), "tenant_id");
}

#[test]
fn scope_keys_fail_closed_on_a_malformed_entity_id() {
    let error = AgentScope::parse("acme").expect_err("a one-segment key is not an agent scope");
    assert_eq!(error.code(), "malformed-scope-key");

    let error = AgentScope::parse("acme/support-agent/extra")
        .expect_err("a three-segment key is not an agent scope");
    assert_eq!(error.code(), "malformed-scope-key");
}

#[test]
fn run_scope_and_task_scope_key_their_own_entities() {
    let run_scope = AgentRunScope::new(
        tenant(),
        agent(),
        AgentRunId::new("run-1").expect("run id should be valid"),
    )
    .expect("run scope should be valid");
    assert_eq!(run_scope.key(), "acme/support-agent/run-1");
    assert_eq!(
        AgentRunScope::from_entity_id(&run_scope.entity_id()).expect("run scope should parse"),
        run_scope
    );
    assert_eq!(run_scope.agent_scope(), scope());

    let task_scope = AgentTaskScope::new(
        tenant(),
        AgentTaskId::new("task-1").expect("task id should be valid"),
    )
    .expect("task scope should be valid");
    assert_eq!(task_scope.key(), "acme/task-1");
}

#[test]
fn a_run_is_bound_to_one_task_for_its_lifetime() {
    let run_scope = AgentRunScope::new(
        tenant(),
        agent(),
        AgentRunId::new("run-1").expect("run id should be valid"),
    )
    .expect("run scope should be valid");
    let task = AgentTaskId::new("task-1").expect("task id should be valid");
    let binding = AgentRunBinding::new(run_scope.clone(), task.clone())
        .with_goal(AgentGoalId::new("goal-1").expect("goal id should be valid"));

    assert_eq!(binding.task(), &task);
    assert_eq!(binding.scope(), &run_scope);
    assert_eq!(
        binding
            .task_scope()
            .expect("the run's task scope should be derivable")
            .key(),
        "acme/task-1"
    );

    // The binding is constructor-only: re-targeting a run at another task would
    // require building a different binding, which in practice means a different
    // run. There is deliberately no setter to reach for.
    let rebound = AgentRunBinding::new(
        run_scope,
        AgentTaskId::new("task-2").expect("task id should be valid"),
    );
    assert_ne!(rebound.task(), binding.task());
}

#[test]
fn operation_ids_are_derived_and_therefore_stable_under_replay() {
    // The same logical operation, reconstructed on any node after any restart,
    // must derive the same id, or a re-driven exchange would transition twice.
    let first = AgentOperationId::for_agent(AgentOperationKind::SettingsUpdate, &scope(), "7")
        .expect("operation id should be derivable");
    let replayed = AgentOperationId::for_agent(AgentOperationKind::SettingsUpdate, &scope(), "7")
        .expect("operation id should be derivable");

    assert_eq!(first, replayed);
    assert_eq!(first.as_str(), "settings-update/acme/support-agent/7");
    assert_eq!(
        first.deduplication_key().as_str(),
        first.command_id().as_str(),
        "the inbox and the outbox must deduplicate on the same value"
    );
}

#[test]
fn operation_ids_do_not_collide_across_kinds_or_segments() {
    let settings = AgentOperationId::for_agent(AgentOperationKind::SettingsUpdate, &scope(), "1")
        .expect("operation id should be derivable");
    let lifecycle =
        AgentOperationId::for_agent(AgentOperationKind::LifecycleCommand, &scope(), "1")
            .expect("operation id should be derivable");
    assert_ne!(settings, lifecycle, "the kind discriminates an operation");

    let next = AgentOperationId::for_agent(AgentOperationKind::SettingsUpdate, &scope(), "2")
        .expect("operation id should be derivable");
    assert_ne!(
        settings, next,
        "the discriminator discriminates an operation"
    );

    // Segment boundaries cannot be forged, because no segment may contain the
    // separator: ("a/b", "c") and ("a", "b/c") cannot both exist.
    let error = AgentOperationId::new(AgentOperationKind::Assignment, ["acme/support", "1"])
        .expect_err("a segment carrying the separator must be rejected");
    assert_eq!(error.code(), "reserved-identifier-character");

    let error = AgentOperationId::new(AgentOperationKind::Assignment, Vec::<String>::new())
        .expect_err("an operation id needs at least one discriminating segment");
    assert_eq!(error.code(), "empty-identifier");
}

#[test]
fn operation_ids_fail_closed_on_deserialization() {
    let error = serde_json::from_str::<AgentOperationId>("\"settings-update\"")
        .expect_err("an operation id with no segments carries no identity");
    assert!(
        error.to_string().contains("no discriminating segments"),
        "unexpected error: {error}"
    );

    let operation: AgentOperationId = serde_json::from_str("\"assignment/acme/task-1/3\"")
        .expect("a well-formed operation id should load");
    assert_eq!(operation.as_str(), "assignment/acme/task-1/3");
}
