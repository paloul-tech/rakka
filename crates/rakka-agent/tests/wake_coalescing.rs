//! One `AgentWakeId`, one execution owner: overlap coalescing and downtime.
//!
//! Specification: section 8.2 ("Duplicate timer scans, events, callbacks, A2A
//! commands, or scanner restarts MUST produce one logical `AgentWakeId` and at
//! most one admitted child epoch"; "The default overlap policy MUST forbid a
//! second active epoch and durably coalesce triggers received while one is
//! active. The default missed-occurrence policy after downtime MUST admit at
//! most one coalesced epoch"); scenarios 48 and 50 of section 18.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, wake_admission_command, AgentBudgetAllocation, AgentBudgetDimension,
    AgentMissedOccurrencePolicy, AgentOperationId, AgentOperationKind, AgentSchemaPolicy,
    AgentTaskEntityCommand, AgentTaskEntityReply, AgentWakeBinding, AgentWakeControllerState,
    AgentWakeDisposition, AgentWakeOccurrence, AgentWakeOutcome, AgentWakePolicy, AgentWakeRelease,
    AgentWakeTriggerKind, ScheduleRevision,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::{
    continuous_goal_mode, goal_id, scheduled_wake_binding, task_scope, tenant, Fixture, TASK,
    TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

async fn controller(fx: &Fixture) -> AgentWakeControllerState {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    state
        .task()
        .expect("the task is created")
        .wake_controller
        .clone()
        .expect("a continuous task carries its controller")
}

fn admit(binding: &AgentWakeBinding) -> AgentTaskEntityCommand {
    wake_admission_command(binding.clone()).expect("the admission command derives")
}

fn complete(binding: &AgentWakeBinding, discriminator: &str) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::CompleteWakeOccurrence {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Command,
            [TENANT, TASK, discriminator],
        )
        .expect("the operation id derives"),
        wake: binding.wake_id().clone(),
    }
}

fn applied_disposition(reply: AgentTaskEntityReply) -> AgentWakeDisposition {
    let AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("the command applies, got {reply:?}");
    };
    let Some(AgentWakeOutcome::Disposition(disposition)) = outcome.wake else {
        panic!(
            "the outcome records a wake disposition, got {:?}",
            outcome.wake
        );
    };
    disposition
}

fn applied_release(reply: AgentTaskEntityReply) -> AgentWakeRelease {
    let AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("the command applies, got {reply:?}");
    };
    let Some(AgentWakeOutcome::Release(release)) = outcome.wake else {
        panic!("the outcome records a release, got {:?}", outcome.wake);
    };
    release
}

/// A hybrid policy so the same occurrence can arrive both from the scanner
/// and as an authenticated A2A command.
fn hybrid_policy() -> AgentWakePolicy {
    let mut budget = AgentBudgetAllocation::unbounded();
    budget.set(AgentBudgetDimension::ModelCalls, Some(8));
    AgentWakePolicy::new(
        [
            AgentWakeTriggerKind::DurableTimer,
            AgentWakeTriggerKind::A2aCommand,
        ],
        budget,
        Some(60_000),
    )
    .expect("the hybrid policy is valid")
}

#[tokio::test]
async fn every_trigger_path_resolves_to_one_wake_and_one_admission() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task_with_mode(continuous_goal_mode(hybrid_policy()))
        .await;

    // The occurrence arrives first from the shared scanner.
    fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);

    // The same logical occurrence arrives again as an A2A command: a
    // different trigger path, a different accepted time — and the identical
    // derived wake and operation id, so the controller answers from its
    // record instead of admitting twice.
    let a2a = AgentWakeBinding::new(
        tenant(),
        goal_id(),
        ScheduleRevision::INITIAL,
        AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(5),
        },
        AgentWakeTriggerKind::A2aCommand,
        AgentTimestampMillis::new(9_999),
        rakka_agent::AgentRevisionNumber::INITIAL,
    )
    .expect("the A2A binding is valid");
    let reply = fx
        .apply_task_command(admit(&a2a))
        .await
        .expect("the duplicate delivery is answered");
    assert!(
        matches!(reply, AgentTaskEntityReply::Duplicate { .. }),
        "the A2A path replays the scanner's admission, got {reply:?}"
    );

    let controller = controller(&fx).await;
    assert_eq!(controller.counters().admitted, 1);
    assert_eq!(controller.active().len(), 1);
}

#[tokio::test]
async fn concurrent_triggers_coalesce_while_exactly_one_owns_execution() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    let first = scheduled_wake_binding(1_000, ScheduleRevision::INITIAL);
    let second = scheduled_wake_binding(2_000, ScheduleRevision::INITIAL);
    let third = scheduled_wake_binding(3_000, ScheduleRevision::INITIAL);

    let admitted = applied_disposition(
        fx.apply_task_command(admit(&first))
            .await
            .expect("the first occurrence is dispositioned"),
    );
    assert!(matches!(admitted, AgentWakeDisposition::Admitted { .. }));

    // Two more triggers while the first owns execution: both coalesce, and
    // the single default slot keeps the latest.
    let coalesced = applied_disposition(
        fx.apply_task_command(admit(&second))
            .await
            .expect("the second occurrence is dispositioned"),
    );
    assert!(matches!(
        coalesced,
        AgentWakeDisposition::Coalesced { replaced: None, .. }
    ));
    let superseding = applied_disposition(
        fx.apply_task_command(admit(&third))
            .await
            .expect("the third occurrence is dispositioned"),
    );
    let AgentWakeDisposition::Coalesced {
        replaced: Some(replaced),
        ..
    } = superseding
    else {
        panic!("the third occurrence supersedes the parked one, got {superseding:?}");
    };
    assert_eq!(&replaced, second.wake_id());

    let state = controller(&fx).await;
    assert_eq!(
        state.active().len(),
        1,
        "exactly one occurrence owns execution"
    );
    assert_eq!(state.active()[0].binding().wake_id(), first.wake_id());
    assert_eq!(state.pending().len(), 1);
    assert_eq!(state.pending()[0].wake_id(), third.wake_id());
    assert_eq!(state.counters().coalesced, 2);
    assert_eq!(state.counters().superseded, 1);

    // A replay of the superseded trigger is answered from the record, not
    // re-parked: its redelivery cannot resurrect it past the newer one.
    let replayed = fx
        .apply_task_command(admit(&second))
        .await
        .expect("the replayed trigger is answered");
    assert!(matches!(replayed, AgentTaskEntityReply::Duplicate { .. }));

    // Releasing the active occurrence promotes the parked one — in the same
    // durable transition, exactly once.
    let release = applied_release(
        fx.apply_task_command(complete(&first, "complete-1"))
            .await
            .expect("the release applies"),
    );
    assert_eq!(&release.released, first.wake_id());
    assert_eq!(release.admitted_next.as_ref(), Some(third.wake_id()));

    let state = controller(&fx).await;
    assert_eq!(state.active().len(), 1);
    assert_eq!(state.active()[0].binding().wake_id(), third.wake_id());
    assert!(state.pending().is_empty());
    assert_eq!(state.counters().admitted, 2);
    assert_eq!(state.counters().released, 1);

    // Releasing again finds nothing pending, and a replayed release is a
    // duplicate rather than a second promotion.
    let release = applied_release(
        fx.apply_task_command(complete(&third, "complete-2"))
            .await
            .expect("the second release applies"),
    );
    assert!(release.admitted_next.is_none());
    let replayed = fx
        .apply_task_command(complete(&third, "complete-2"))
        .await
        .expect("the replayed release is answered");
    assert!(matches!(replayed, AgentTaskEntityReply::Duplicate { .. }));
    assert_eq!(controller(&fx).await.counters().released, 2);
}

#[tokio::test]
async fn downtime_admits_at_most_one_coalesced_occurrence() {
    let policy = hybrid_policy()
        .with_admission_window(60_000)
        .expect("the window is accepted")
        .with_maximum_lateness(120_000)
        .expect("the lateness is accepted");
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task_with_mode(continuous_goal_mode(policy))
        .await;

    // Three occurrences became due while no scanner could deliver them, all
    // now far past the maximum lateness. Recovery admits exactly one
    // coalesced representative; the rest of the backlog is absorbed by it —
    // counted missed, never parked — so one downtime yields one epoch, never
    // a representative plus an echo.
    for due_at in [1_000, 2_000, 3_000] {
        fx.schedule_wake(due_at, ScheduleRevision::INITIAL).await;
    }
    fx.clock.store(1_000_000, Ordering::SeqCst);
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the recovery pass runs");
    assert_eq!(scan.outcomes.len(), 3);
    for outcome in &scan.outcomes[1..] {
        assert!(
            matches!(
                outcome,
                rakka_agent::AgentWakeScanOutcome::Dispositioned {
                    disposition: AgentWakeDisposition::Skipped { .. },
                    ..
                }
            ),
            "the backlog behind the representative is absorbed, got {outcome:?}"
        );
    }

    let state = controller(&fx).await;
    assert_eq!(
        state.counters().admitted,
        1,
        "downtime admits exactly one coalesced occurrence"
    );
    assert_eq!(state.counters().missed, 2);
    assert_eq!(state.active().len(), 1);
    assert!(state.active()[0].is_representative());
    assert!(
        state.pending().is_empty(),
        "the absorbed backlog parks nothing behind its representative"
    );

    // A second recovery pass finds every entry terminal: nothing replays.
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the second pass runs");
    assert!(scan.outcomes.is_empty());
    assert_eq!(controller(&fx).await.counters().admitted, 1);
}

#[tokio::test]
async fn a_skip_policy_skips_missed_occurrences_outright() {
    let mut policy = hybrid_policy()
        .with_maximum_lateness(120_000)
        .expect("the lateness is accepted");
    policy.missed_occurrence = AgentMissedOccurrencePolicy::Skip;
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task_with_mode(continuous_goal_mode(policy))
        .await;

    for due_at in [1_000, 2_000] {
        fx.schedule_wake(due_at, ScheduleRevision::INITIAL).await;
    }
    fx.clock.store(1_000_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 2);
    for outcome in &scan.outcomes {
        assert!(matches!(
            outcome,
            rakka_agent::AgentWakeScanOutcome::Dispositioned {
                disposition: AgentWakeDisposition::Skipped { .. },
                ..
            }
        ));
    }

    let state = controller(&fx).await;
    assert_eq!(state.counters().missed, 2);
    assert_eq!(state.counters().admitted, 0);
    assert!(state.active().is_empty());
}
