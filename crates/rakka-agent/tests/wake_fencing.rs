//! An obsolete schedule revision cannot admit an epoch, and a restart resets
//! nothing.
//!
//! Specification: sections 6.9 ("A schedule update MUST create a monotonic
//! revision and fence pending wakes from obsolete revisions") and 8.2 ("A
//! schedule update MUST fence obsolete occurrences"); scenario 49 of section
//! 18, including the obsolete-revision injection of the slice's done-when.
//! Restart is structural throughout — every entity, store facade, and scanner
//! is rebuilt from durable state alone — and the stale-owner arm proves the
//! compare-and-set fence: a controller whose shard moved cannot split its
//! schedule between two owners.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, wake_admission_command, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentSchemaPolicy, AgentTaskEntityCommand, AgentTaskEntityReply,
    AgentTaskEntityStore, AgentWakeControllerState, AgentWakeDisposition, AgentWakeOutcome,
    AgentWakePolicyRevision, AgentWakeTimerStatus, ScheduleRevision,
};

mod common;

use common::{provenance, scheduled_wake_binding, task_scope, wake_policy, Fixture, TASK, TENANT};

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
        .unwrap_or_default()
}

async fn schedule_revision_in_force(fx: &Fixture) -> ScheduleRevision {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    state
        .task()
        .expect("the task is created")
        .goal_mode
        .continuous()
        .expect("the task is continuous")
        .schedule_revision
}

fn update(
    discriminator: &str,
    schedule_revision: ScheduleRevision,
    wake_policy: Option<AgentWakePolicyRevision>,
) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::UpdateContinuousSchedule {
        operation_id: AgentOperationId::new(
            AgentOperationKind::SettingsUpdate,
            [TENANT, TASK, discriminator],
        )
        .expect("the operation id derives"),
        schedule_revision,
        wake_policy: wake_policy.map(Box::new),
    }
}

#[tokio::test]
async fn an_obsolete_schedule_revision_cannot_admit_after_an_update() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    // An occurrence parked under revision 1, still pending when the schedule
    // moves to revision 2.
    fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    let updated = fx
        .apply_task_command(update("rev-2", ScheduleRevision::new(2), None))
        .await
        .expect("the schedule update applies");
    let AgentTaskEntityReply::Applied { outcome } = updated else {
        panic!("the update applies, got {updated:?}");
    };
    assert!(matches!(
        outcome.wake,
        Some(AgentWakeOutcome::ScheduleUpdated {
            schedule_revision, fenced: 0, ..
        }) if schedule_revision == ScheduleRevision::new(2)
    ));

    // The scanner delivers the obsolete occurrence; the controller fences it
    // as a recorded transition, and the entry goes terminal so no later scan
    // ever replays it.
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    let rakka_agent::AgentWakeScanOutcome::Dispositioned {
        disposition,
        marked,
        ..
    } = &scan.outcomes[0]
    else {
        panic!("the injection dispositions, got {:?}", scan.outcomes[0]);
    };
    assert!(matches!(disposition, AgentWakeDisposition::Fenced { .. }));
    assert_eq!(*marked, AgentWakeTimerStatus::Fenced);
    let state = controller(&fx).await;
    assert_eq!(state.counters().fenced, 1);
    assert_eq!(state.counters().admitted, 0);
    assert!(state.active().is_empty());

    let scan = fx.wake_scanner().scan_due().await.expect("the rescan runs");
    assert!(scan.outcomes.is_empty(), "a fenced entry never rescans");

    // The current revision still admits: fencing is a comparison, not a halt.
    fx.schedule_wake(2_000, ScheduleRevision::new(2)).await;
    fx.clock.fetch_add(10_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Admitted { .. },
            ..
        }
    ));
    assert_eq!(controller(&fx).await.counters().admitted, 1);

    // The revision moves strictly forward: an equal or older revision is
    // refused, and a replay of the accepted update answers as a duplicate.
    let stale = fx
        .apply_task_command(update("rev-2-again", ScheduleRevision::new(2), None))
        .await
        .expect_err("a non-monotonic update is refused");
    assert_eq!(stale.code(), "task-schedule-not-monotonic");
    let replay = fx
        .apply_task_command(update("rev-2", ScheduleRevision::new(2), None))
        .await
        .expect("the replayed update is answered");
    assert!(matches!(replay, AgentTaskEntityReply::Duplicate { .. }));
}

#[tokio::test]
async fn a_restart_resets_neither_the_revision_nor_the_policy() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    // Move the schedule and the policy forward together, then fence one
    // obsolete injection so the counters are non-trivial.
    let newer_policy = AgentWakePolicyRevision::initial(wake_policy(), provenance(1))
        .expect("the initial revision is valid")
        .updated(wake_policy(), provenance(2))
        .expect("the updated revision is valid");
    fx.apply_task_command(update(
        "rev-2",
        ScheduleRevision::new(2),
        Some(newer_policy),
    ))
    .await
    .expect("the update applies");
    let obsolete = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let fenced = fx
        .apply_task_command(
            wake_admission_command(obsolete).expect("the admission command derives"),
        )
        .await
        .expect("the obsolete injection is dispositioned");
    let AgentTaskEntityReply::Applied { outcome } = fenced else {
        panic!("the injection applies, got {fenced:?}");
    };
    assert!(matches!(
        outcome.wake,
        Some(AgentWakeOutcome::Disposition(
            AgentWakeDisposition::Fenced { .. }
        ))
    ));

    // Everything the fixture drives is already rebuilt from durable state on
    // every call; read the record back the way a fresh pod would.
    assert_eq!(
        schedule_revision_in_force(&fx).await,
        ScheduleRevision::new(2)
    );
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let spec = state
        .task()
        .expect("the task is created")
        .goal_mode
        .continuous()
        .expect("the task is continuous");
    assert_eq!(spec.wake_policy.revision(), AgentRevisionNumber::new(2));
    assert_eq!(controller(&fx).await.counters().fenced, 1);

    // And the same obsolete occurrence injected after the "restart" replays
    // the recorded fence rather than dispositioning twice.
    let obsolete = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let replay = fx
        .apply_task_command(
            wake_admission_command(obsolete).expect("the admission command derives"),
        )
        .await
        .expect("the replayed injection is answered");
    assert!(matches!(replay, AgentTaskEntityReply::Duplicate { .. }));
    assert_eq!(controller(&fx).await.counters().fenced, 1);
}

#[tokio::test]
async fn a_schedule_update_fences_the_parked_occurrence_but_not_the_active_one() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    let active = scheduled_wake_binding(1_000, ScheduleRevision::INITIAL);
    let parked = scheduled_wake_binding(2_000, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(active.clone()).expect("the command derives"))
        .await
        .expect("the first occurrence admits");
    fx.apply_task_command(wake_admission_command(parked).expect("the command derives"))
        .await
        .expect("the second occurrence coalesces");

    let updated = fx
        .apply_task_command(update("rev-2", ScheduleRevision::new(2), None))
        .await
        .expect("the update applies");
    let AgentTaskEntityReply::Applied { outcome } = updated else {
        panic!("the update applies, got {updated:?}");
    };
    assert!(matches!(
        outcome.wake,
        Some(AgentWakeOutcome::ScheduleUpdated { fenced: 1, .. })
    ));

    let state = controller(&fx).await;
    assert!(
        state.pending().is_empty(),
        "the parked occurrence is fenced"
    );
    assert_eq!(
        state.active().len(),
        1,
        "an already-admitted occurrence is not fenced by a schedule update"
    );
    assert_eq!(state.active()[0].binding().wake_id(), active.wake_id());

    // Releasing the survivor finds nothing to promote: the fenced occurrence
    // is gone, not waiting.
    let release = fx
        .apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Command,
                [TENANT, TASK, "complete-1"],
            )
            .expect("the operation id derives"),
            wake: active.wake_id().clone(),
        })
        .await
        .expect("the release applies");
    let AgentTaskEntityReply::Applied { outcome } = release else {
        panic!("the release applies, got {release:?}");
    };
    assert!(matches!(
        outcome.wake,
        Some(AgentWakeOutcome::Release(release)) if release.admitted_next.is_none()
    ));
}

#[tokio::test]
async fn a_revision_ahead_of_the_controller_fails_closed() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    // A binding claiming revision 2 while the controller is at revision 1:
    // no schedule the controller accepted ever issued it, so it is refused —
    // not fenced, not coalesced, not admitted.
    let ahead = scheduled_wake_binding(5, ScheduleRevision::new(2));
    let error = fx
        .apply_task_command(wake_admission_command(ahead).expect("the command derives"))
        .await
        .expect_err("a revision ahead fails closed");
    assert_eq!(error.code(), "wake-revision-ahead");
    assert_eq!(controller(&fx).await, AgentWakeControllerState::default());

    // Through the scanner, the refusal leaves the entry pending: an operator
    // can still cancel it, and the schedule update that legitimizes it would
    // let a later pass deliver it.
    fx.schedule_wake(5, ScheduleRevision::new(2)).await;
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Rejected { code, .. } if code == "wake-revision-ahead"
    ));
    let entry_status = fx
        .wake_scanner()
        .timers_mut()
        .recover(rakka_agent_workflow::AgentTimestampMillis::new(0))
        .await
        .expect("the store recovers")
        .entries()
        .values()
        .next()
        .expect("the entry exists")
        .status();
    assert_eq!(entry_status, AgentWakeTimerStatus::Pending);
}

#[tokio::test]
async fn a_stale_owner_cannot_split_the_controller() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    // Two materializations of the same task — the old owner and the new one
    // after a shard move. Both recover the same durable record.
    let mut stale = AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    stale
        .recover(fx.now())
        .await
        .expect("the stale owner recovers");

    // The new owner commits an admission.
    let first = scheduled_wake_binding(1_000, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(first).expect("the command derives"))
        .await
        .expect("the new owner's admission applies");

    // The stale owner, still holding its old cached revision, tries its own
    // admission: the compare-and-set fence refuses the write.
    let second = scheduled_wake_binding(2_000, ScheduleRevision::INITIAL);
    let command = wake_admission_command(second).expect("the command derives");
    let error = stale
        .apply(command.clone(), &fx.router, fx.now())
        .await
        .expect_err("the stale write is fenced");
    assert_eq!(error.code(), "revision-conflict");

    // Recovery reacquires the latest revision, and the same command then
    // applies cleanly — as a coalescing, because the new owner's admission is
    // authoritative.
    stale
        .recover(fx.now())
        .await
        .expect("the stale owner re-recovers");
    let reply = stale
        .apply(command, &fx.router, fx.now())
        .await
        .expect("the recovered owner applies");
    let AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("the recovered command applies, got {reply:?}");
    };
    assert!(matches!(
        outcome.wake,
        Some(AgentWakeOutcome::Disposition(
            AgentWakeDisposition::Coalesced { .. }
        ))
    ));

    let state = controller(&fx).await;
    assert_eq!(state.counters().admitted, 1);
    assert_eq!(state.counters().coalesced, 1);
    assert_eq!(state.active().len(), 1);
}
