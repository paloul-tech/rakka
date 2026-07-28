//! The goal-window ceiling is enforced at epoch admission, and refill is a
//! persisted logical-time transition — never a restart's side effect.
//!
//! Specification: section 9.7 ("Continuous goals MUST combine per-epoch
//! allocation with a durable rolling or calendar-window goal ceiling. Refill
//! MUST be a persisted logical-time policy transition and MUST NOT occur
//! because an actor/pod restarted, a shard moved, or an entity was
//! activated") and the window clauses of 8.2. Every entity here is rebuilt
//! from durable state on every call, so each step is already the restart the
//! refill must ignore.

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, wake_admission_command, AgentBudgetDimension, AgentBudgetWindow,
    AgentGoalWindowCeiling, AgentOperationId, AgentOperationKind, AgentSchemaPolicy,
    AgentTaskEntityCommand, AgentTaskEntityReply, AgentWakeControllerState, AgentWakeDisposition,
    AgentWakeOutcome, ScheduleRevision,
};

mod common;

use common::{
    continuous_goal_mode, scheduled_wake_binding, task_scope, wake_policy, Fixture, TASK, TENANT,
};

const WINDOW_MILLIS: u64 = 3_600_000;

fn windowed_mode() -> rakka_agent::AgentGoalMode {
    let mut ceiling = rakka_agent::AgentBudgetAllocation::unbounded();
    ceiling.set(AgentBudgetDimension::ModelCalls, Some(20));
    continuous_goal_mode(
        wake_policy()
            .with_goal_window(AgentGoalWindowCeiling {
                window: AgentBudgetWindow::Rolling {
                    length_millis: WINDOW_MILLIS,
                },
                ceiling,
            })
            .expect("the windowed policy is valid"),
    )
}

async fn controller(fx: &Fixture) -> AgentWakeControllerState {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    state
        .task()
        .expect("the root is created")
        .wake_controller
        .clone()
        .expect("the controller exists")
}

fn complete(wake: &rakka_agent::AgentWakeId, discriminator: &str) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::CompleteWakeOccurrence {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Command,
            [TENANT, TASK, discriminator],
        )
        .expect("the operation id derives"),
        wake: wake.clone(),
    }
}

fn disposition_of(reply: AgentTaskEntityReply) -> AgentWakeDisposition {
    let AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("the admission applies, got {reply:?}");
    };
    let Some(AgentWakeOutcome::Disposition(disposition)) = outcome.wake else {
        panic!("the outcome records a disposition, got {:?}", outcome.wake);
    };
    disposition
}

#[tokio::test]
async fn the_window_ceiling_defers_admissions_and_refills_only_by_logical_time() {
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(windowed_mode()).await;

    // Two epochs of 8 model calls fit the 20-call window; each is completed
    // so the next admission finds a free slot.
    for (due_at, discriminator) in [(5, "c1"), (10, "c2")] {
        let binding = scheduled_wake_binding(due_at, ScheduleRevision::INITIAL);
        let disposition = disposition_of(
            fx.apply_task_command(
                wake_admission_command(binding.clone()).expect("the command derives"),
            )
            .await
            .expect("the admission applies"),
        );
        assert!(
            matches!(disposition, AgentWakeDisposition::Admitted { .. }),
            "epoch at {due_at} fits the window, got {disposition:?}"
        );
        fx.apply_task_command(complete(binding.wake_id(), discriminator))
            .await
            .expect("the release applies");
    }

    // The third would need 24 of 20: deferred, parked, and recorded — a
    // replay answers from the record.
    let third = scheduled_wake_binding(15, ScheduleRevision::INITIAL);
    let command = wake_admission_command(third.clone()).expect("the command derives");
    let disposition = disposition_of(
        fx.apply_task_command(command.clone())
            .await
            .expect("the deferral applies"),
    );
    assert!(matches!(disposition, AgentWakeDisposition::Deferred { .. }));
    let state = controller(&fx).await;
    assert_eq!(state.counters().deferred, 1);
    assert_eq!(state.pending().len(), 1);
    let ledger = state.window().expect("the window ledger exists");
    assert_eq!(ledger.consumed().get(AgentBudgetDimension::ModelCalls), 16);
    let replay = fx
        .apply_task_command(command)
        .await
        .expect("the replayed deferral is answered");
    assert!(matches!(replay, AgentTaskEntityReply::Duplicate { .. }));

    // Every step above already rebuilt the entity from durable state — the
    // restart the refill must ignore. Read it back the way a fresh pod would
    // and prove nothing refilled.
    let state = controller(&fx).await;
    assert_eq!(
        state
            .window()
            .expect("the ledger survives restart")
            .consumed()
            .get(AgentBudgetDimension::ModelCalls),
        16,
        "activation neither refills nor consumes"
    );

    // Logical time crosses the window boundary: the next delivery's own
    // recorded transition refills — and the *deferred* occurrence takes the
    // turned window first, oldest parked ahead of the fresh delivery, which
    // parks behind it instead of leapfrogging it.
    fx.clock
        .store(WINDOW_MILLIS + 100, std::sync::atomic::Ordering::SeqCst);
    let fourth = scheduled_wake_binding(WINDOW_MILLIS + 50, ScheduleRevision::INITIAL);
    let disposition = disposition_of(
        fx.apply_task_command(wake_admission_command(fourth.clone()).expect("the command derives"))
            .await
            .expect("the post-refill delivery applies"),
    );
    assert!(
        matches!(disposition, AgentWakeDisposition::Coalesced { .. }),
        "the fresh delivery parks behind the promoted deferred occurrence, got {disposition:?}"
    );
    let state = controller(&fx).await;
    assert_eq!(
        state.active()[0].binding().wake_id(),
        third.wake_id(),
        "the deferred occurrence was promoted first"
    );
    assert_eq!(state.pending().len(), 1);
    let ledger = state.window().expect("the refilled ledger exists");
    assert_eq!(
        ledger.consumed().get(AgentBudgetDimension::ModelCalls),
        8,
        "the turned window holds exactly the promoted epoch's charge"
    );

    // Releasing the promoted epoch promotes the fresh occurrence, charged
    // against the same turned window in the same transition.
    let release = fx
        .apply_task_command(complete(third.wake_id(), "c4"))
        .await
        .expect("the release applies");
    let AgentTaskEntityReply::Applied { outcome } = release else {
        panic!("the release applies, got {release:?}");
    };
    assert!(matches!(
        outcome.wake,
        Some(AgentWakeOutcome::Release(release))
            if release.admitted_next.as_ref() == Some(fourth.wake_id())
    ));
    let state = controller(&fx).await;
    assert_eq!(state.counters().admitted, 4);
    assert!(state.pending().is_empty());
    assert_eq!(
        state
            .window()
            .expect("the ledger exists")
            .consumed()
            .get(AgentBudgetDimension::ModelCalls),
        16
    );
}
