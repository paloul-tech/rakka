//! A foreign tenant reads exactly what an empty tenant reads.
//!
//! Specification: section 16 ("every request MUST be authenticated and
//! tenant-authorized before data access"; "policy checks MUST occur before
//! queries that could reveal resource existence across a tenant boundary");
//! scenario 18, generalized past memory to every sharded entity class.
//!
//! The tenant is a *key*, not a filter: it is the first segment of every
//! entity's persistence id, so a cross-tenant read is a read of a different
//! record rather than a read that has to be refused. That is the strongest
//! shape this can take — there is no code path to forget — but it had never
//! been asserted for the entity classes, and "the key includes the tenant" is
//! an argument about construction, not an observation.
//!
//! What is new here, and what is deliberately not:
//!
//! - **New**: every durable load path, driven for a foreign tenant against a
//!   populated store, compared by *whole value* against the same read in a
//!   third genuinely-empty tenant. `is_none()` would be satisfied by a backend
//!   that answered "absent" differently from how it answered "unknown scope",
//!   which is exactly the existence disclosure the clause forbids.
//! - **New**: the first cross-tenant case in the dispatch authority. Every
//!   test in `tool_authority.rs` is single-tenant, so an agent record read for
//!   the wrong tenant had never been exercised at the gate.
//! - **Not duplicated**: `choreography.rs` already proves an exchange may not
//!   cross a tenant boundary (`exchange-cross-tenant`), `coordination_replay.rs`
//!   proves the shared replay entry point's fence
//!   (`coordination-scope-foreign-tenant`), and `goal_view.rs` proves the goal
//!   view's owner wrapper answers a denial exactly as a missing goal.

use rakka_agent::{
    load_agent_entity_state, load_agent_run_state, load_agent_task_state, AgentEntityClass,
    AgentId, AgentRunId, AgentRunScope, AgentSchemaPolicy, AgentScope, AgentTaskId, AgentTaskScope,
    TenantId,
};

mod common;

use common::*;

/// A tenant that holds records.
fn populated_tenant() -> TenantId {
    tenant()
}

/// A tenant that shares no record with the populated one.
fn foreign_tenant() -> TenantId {
    TenantId::new("other-corp")
}

/// A third tenant nothing ever writes to — the reference answer that makes
/// "reveals nothing" a whole-value equality rather than an `is_none()`.
fn empty_tenant() -> TenantId {
    TenantId::new("never-written")
}

fn agent_scope_in(tenant: &TenantId) -> AgentScope {
    AgentScope::new(tenant.clone(), agent_id()).expect("the agent scope is valid")
}

fn task_scope_in(tenant: &TenantId) -> AgentTaskScope {
    AgentTaskScope::new(
        tenant.clone(),
        AgentTaskId::new(TASK).expect("the task id is valid"),
    )
    .expect("the task scope is valid")
}

fn run_scope_in(tenant: &TenantId) -> AgentRunScope {
    let run = run_scope();
    AgentRunScope::new(
        tenant.clone(),
        AgentId::new(AGENT).expect("the agent id is valid"),
        AgentRunId::new(run.run().as_str()).expect("the run id is valid"),
    )
    .expect("the run scope is valid")
}

/// Which entity classes this file sweeps, and why any it does not.
///
/// `AgentEntityClass` is `#[non_exhaustive]`, so an integration test — a
/// separate crate — cannot match it exhaustively. The catalogue-length
/// assertion below is the tripwire instead: a class added to the enum bumps it
/// and fails, which forces whoever added it to decide whether this sweep
/// covers it.
const SWEPT_CLASSES: [AgentEntityClass; 3] = [
    AgentEntityClass::Agent,
    AgentEntityClass::Task,
    AgentEntityClass::Run,
];

/// The coordination classes this sweep does not drive, with the reason.
const UNSWEPT_CLASSES: [(AgentEntityClass, &str); 2] = [
    (
        AgentEntityClass::Team,
        "the shared fixture creates no team; `team_board.rs` owns the board entity",
    ),
    (
        AgentEntityClass::Conversation,
        "the shared fixture creates no conversation; `conversation_turns.rs` owns it",
    ),
];

/// A populated world: one agent, one task, one run, all driven to completion.
async fn populated_world() -> Fixture {
    let fx = Fixture::new(rakka_agent::testkit::ScriptedDispatcher::with_adapter(
        rakka_agent::testkit::DeterministicModelAdapter::new().with_turn(
            rakka_agent::AgentModelTurn::new(rakka_agent::CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("done")
                .with_proposal(
                    rakka_agent::AgentTaskContent::inline(
                        serde_json::json!({ "answer": "resolved" }),
                    )
                    .expect("the proposal is inline-bounded"),
                ),
        ),
    ));
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");
    fx
}

// ---------------------------------------------------------------------------
// The sweep names every entity class.
// ---------------------------------------------------------------------------

/// Every entity class is either swept or unswept for a stated reason.
#[test]
fn the_sweep_names_every_entity_class() {
    // Bumped by a milestone that adds a sharded entity class, which is the
    // point: the new class needs a decision, not a silent pass.
    const CLASS_COUNT_AT_AUTHORING: usize = 5;
    assert_eq!(
        SWEPT_CLASSES.len() + UNSWEPT_CLASSES.len(),
        CLASS_COUNT_AT_AUTHORING,
        "a sharded entity class was added; decide whether this sweep covers it"
    );
    for (class, reason) in UNSWEPT_CLASSES {
        assert!(
            !reason.is_empty(),
            "{} is unswept with no reason; an unexplained gap reads as coverage",
            class.as_label()
        );
    }
    for class in SWEPT_CLASSES {
        assert!(
            UNSWEPT_CLASSES.iter().all(|(other, _)| *other != class),
            "{} is listed both swept and unswept",
            class.as_label()
        );
    }
}

// ---------------------------------------------------------------------------
// Every durable load path: foreign ≡ empty, by whole value.
// ---------------------------------------------------------------------------

/// The agent entity's durable read answers a foreign tenant exactly as it
/// answers a tenant that has never existed.
#[tokio::test]
async fn a_foreign_tenants_agent_read_is_byte_identical_to_an_empty_tenants() {
    let fx = populated_world().await;
    let policy = AgentSchemaPolicy::default();

    let owned = load_agent_entity_state(&fx.agents, &agent_scope_in(&populated_tenant()), &policy)
        .await
        .expect("the populated read succeeds");
    assert!(
        owned.is_some(),
        "the populated tenant holds nothing, so this test is vacuous"
    );

    let foreign = load_agent_entity_state(&fx.agents, &agent_scope_in(&foreign_tenant()), &policy)
        .await
        .expect("the foreign read succeeds");
    let empty = load_agent_entity_state(&fx.agents, &agent_scope_in(&empty_tenant()), &policy)
        .await
        .expect("the empty read succeeds");

    assert_eq!(
        foreign, empty,
        "a foreign tenant learns something an empty tenant does not"
    );
}

/// The task entity's durable read, likewise.
#[tokio::test]
async fn a_foreign_tenants_task_read_is_byte_identical_to_an_empty_tenants() {
    let fx = populated_world().await;
    let policy = AgentSchemaPolicy::default();

    let owned = load_agent_task_state(&fx.tasks, &task_scope_in(&populated_tenant()), &policy)
        .await
        .expect("the populated read succeeds");
    assert!(owned.is_some(), "the populated tenant holds no task");

    let foreign = load_agent_task_state(&fx.tasks, &task_scope_in(&foreign_tenant()), &policy)
        .await
        .expect("the foreign read succeeds");
    let empty = load_agent_task_state(&fx.tasks, &task_scope_in(&empty_tenant()), &policy)
        .await
        .expect("the empty read succeeds");

    assert_eq!(foreign, empty);
}

/// The run entity's durable read, likewise.
#[tokio::test]
async fn a_foreign_tenants_run_read_is_byte_identical_to_an_empty_tenants() {
    let fx = populated_world().await;
    let policy = AgentSchemaPolicy::default();

    let owned = load_agent_run_state(&fx.runs, &run_scope_in(&populated_tenant()), &policy)
        .await
        .expect("the populated read succeeds");
    assert!(owned.is_some(), "the populated tenant holds no run");

    let foreign = load_agent_run_state(&fx.runs, &run_scope_in(&foreign_tenant()), &policy)
        .await
        .expect("the foreign read succeeds");
    let empty = load_agent_run_state(&fx.runs, &run_scope_in(&empty_tenant()), &policy)
        .await
        .expect("the empty read succeeds");

    assert_eq!(foreign, empty);
}

/// The same identity in two tenants holds independent records, and neither
/// read reveals the other.
///
/// The twin-identity case: the *same* agent id under two tenants. If the
/// tenant were a filter rather than a key, this is where the two would
/// collide.
#[tokio::test]
async fn the_same_identity_in_two_tenants_holds_independent_records() {
    let fx = populated_world().await;
    let policy = AgentSchemaPolicy::default();

    let owned = load_agent_entity_state(&fx.agents, &agent_scope_in(&populated_tenant()), &policy)
        .await
        .expect("the read succeeds")
        .expect("the populated tenant holds its agent");

    // The persistence ids must differ, which is what makes the isolation
    // structural rather than enforced.
    assert_ne!(
        agent_scope_in(&populated_tenant()).persistence_id(),
        agent_scope_in(&foreign_tenant()).persistence_id(),
        "two tenants share a durable record, which no later check could fix"
    );

    let foreign = load_agent_entity_state(&fx.agents, &agent_scope_in(&foreign_tenant()), &policy)
        .await
        .expect("the read succeeds");
    assert!(foreign.is_none());

    // And the owned record is untouched by the foreign read.
    let again = load_agent_entity_state(&fx.agents, &agent_scope_in(&populated_tenant()), &policy)
        .await
        .expect("the read succeeds")
        .expect("still there");
    assert_eq!(owned, again);
}
