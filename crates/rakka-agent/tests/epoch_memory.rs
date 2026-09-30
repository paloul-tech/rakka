//! Continuous epochs use distinct finite task/run short-term-memory scopes,
//! and cross-epoch continuity comes only from authorized shared state.
//!
//! Specification: sections 6.5 ("Cross-epoch continuity belongs in the stable
//! goal/controller state, agent-private memory, and explicit artifacts; one
//! unbounded run and short-term-memory session MUST NOT be the default
//! continuous-execution model") and 13.2 (session memory is scoped
//! `(TenantId, AgentId, AgentRunId)`; one run's entries are never visible
//! through another run's session API); scenario 51 of section 18. Two epochs
//! of one goal run to completion here: each records its turns under its own
//! derived run scope, each opens with its own occurrence — reconstructed from
//! the controller's durable state, never from the other epoch's session — and
//! what carries across epochs is exactly the controller state's counters and
//! ledger.

use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, AgentModelTurn, AgentRunMemory, AgentSchemaPolicy, AgentTaskContent,
    AgentWakeDisposition, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    ScheduleRevision, SessionMemoryCursor, SessionMemoryStore, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::{continuous_goal_mode, epoch_scopes_for, task_scope, wake_policy, Fixture};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Observed.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

#[tokio::test]
async fn epochs_use_distinct_session_scopes_and_share_only_authorized_state() {
    let session = Arc::new(InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(proposing_turn())
            .with_turn(proposing_turn()),
    )
    .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone()));
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    // Epoch one: admitted, executed, completed, released.
    let first = fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    let (first_task, first_run) = epoch_scopes_for(first.wake_id());
    fx.clock
        .fetch_add(1_000, std::sync::atomic::Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    fx.pump_epoch(&first_task, &first_run)
        .await
        .expect("the first epoch converges");

    let first_session = session
        .read(&first_run, SessionMemoryCursor::start())
        .await
        .expect("the first epoch's session reads");
    assert!(
        !first_session.entries.is_empty(),
        "the first epoch recorded its turns"
    );
    let first_len = session.len(&first_run);

    // Epoch two: a fresh derived run scope, a fresh session.
    let second = fx.schedule_wake(50_000, ScheduleRevision::INITIAL).await;
    let (second_task, second_run) = epoch_scopes_for(second.wake_id());
    fx.clock.store(60_000, std::sync::atomic::Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Admitted { .. },
            ..
        }
    ));
    fx.pump_epoch(&second_task, &second_run)
        .await
        .expect("the second epoch converges");

    // Distinct finite scopes: each epoch's session holds its own turns, and
    // running the second changed nothing under the first's scope.
    assert_ne!(first_run, second_run);
    let second_session = session
        .read(&second_run, SessionMemoryCursor::start())
        .await
        .expect("the second epoch's session reads");
    assert!(!second_session.entries.is_empty());
    assert_eq!(
        session.len(&first_run),
        first_len,
        "the second epoch never wrote through the first's session scope"
    );
    let first_ids: Vec<_> = first_session
        .entries
        .iter()
        .map(|entry| entry.entry_id.clone())
        .collect();
    assert!(
        second_session
            .entries
            .iter()
            .all(|entry| !first_ids.contains(&entry.entry_id)),
        "no entry is visible through both sessions"
    );

    // Each epoch opened with its own occurrence, reconstructed from the
    // controller's durable state — not from the other epoch's session.
    let opening = serde_json::to_value(&second_session.entries[0].content)
        .expect("the opening entry serializes")
        .to_string();
    assert!(
        opening.contains(second.wake_id().as_str()),
        "the second epoch observes its own occurrence"
    );
    assert!(
        !opening.contains(first.wake_id().as_str()),
        "the second epoch's input carries nothing of the first's"
    );

    // What crosses epochs is the authorized shared state: the controller's
    // durable counters and the goal ledger the epochs settled into.
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let root_task = root.task().expect("the root is created");
    let controller = root_task
        .wake_controller
        .as_ref()
        .expect("the controller exists");
    assert_eq!(controller.counters().admitted, 2);
    assert_eq!(controller.counters().released, 2);
    assert_eq!(root_task.escrow.outstanding().count(), 0);
}
