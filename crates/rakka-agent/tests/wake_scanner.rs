//! Scanner and pod uptime never create an occurrence; only durable logical
//! time does.
//!
//! Specification: section 8.2 ("A pod/actor/dispatcher start or restart MUST
//! NOT itself create a wake, epoch, schedule reset, or budget refill") and
//! the section 15 scanner clauses; scenario 47 of section 18, plus the
//! duplicate-scan half of scenario 48. The scanner here is the real
//! [`rakka_agent::AgentWakeScanner`] over the real durable wake-timer store;
//! restarts are structural — every entity and scanner is rebuilt from durable
//! state alone — and the crash windows are injected at exact durable
//! boundaries: a reply lost after the controller dispositioned the wake, a
//! double delivery of the same derived operation id, an owner killed on
//! either side of every task-store write of the admission transition.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::{sweep_crash_points, ExchangeFault, ScriptedDispatcher};
use rakka_agent::{
    load_agent_task_state, AgentSchemaPolicy, AgentTaskEntityReply, AgentWakeCounters,
    AgentWakeDisposition, AgentWakeTimerStatus, ScheduleRevision,
};

mod common;

use common::{task_scope, Fixture};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

async fn wake_counters(fx: &Fixture) -> AgentWakeCounters {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let task = state.task().expect("the task is created");
    task.wake_controller
        .as_ref()
        .map(|controller| *controller.counters())
        .unwrap_or_default()
}

async fn active_wakes(fx: &Fixture) -> usize {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let task = state.task().expect("the task is created");
    task.wake_controller
        .as_ref()
        .map(|controller| controller.active().len())
        .unwrap_or_default()
}

#[tokio::test]
async fn scanner_restarts_create_no_occurrence_until_a_durable_wake_is_due() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    // Scanner start, restart, and rescan over a store with nothing due: three
    // fresh scanners — three "pod starts" — and not one occurrence.
    for _restart in 0..3 {
        let scan = fx
            .wake_scanner()
            .scan_due()
            .await
            .expect("an empty pass succeeds");
        assert_eq!(scan.due_count, 0);
        assert!(scan.outcomes.is_empty());
    }
    assert_eq!(wake_counters(&fx).await, AgentWakeCounters::default());

    // A wake scheduled for the future is durable but not due: still nothing.
    fx.schedule_wake(1_000_000, ScheduleRevision::INITIAL).await;
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("a not-yet-due pass succeeds");
    assert!(scan.outcomes.is_empty());
    assert_eq!(wake_counters(&fx).await, AgentWakeCounters::default());

    // Only logical time makes it due. The clock advancing past the due time —
    // not any restart before it — is what admits the occurrence.
    fx.clock.fetch_add(2_000_000, Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    let counters = wake_counters(&fx).await;
    assert_eq!(counters.admitted, 1);
    assert_eq!(active_wakes(&fx).await, 1);
}

#[tokio::test]
async fn entity_activation_alone_creates_no_wake() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;

    // Recover the root control task over and over — activation after
    // passivation, materialization after shard movement. State moves only
    // under a delivered wake, never under residency.
    for _activation in 0..3 {
        let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the task state loads")
            .expect("the task exists");
        assert!(state
            .task()
            .expect("the task is created")
            .goal_mode
            .is_continuous());
    }
    assert_eq!(wake_counters(&fx).await, AgentWakeCounters::default());
}

#[tokio::test]
async fn a_lost_reply_leaves_the_entry_pending_and_the_rescan_converges() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;
    fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    fx.clock.fetch_add(1_000, Ordering::SeqCst);

    // The reply is lost *after* the controller dispositioned the wake: the
    // admission is durable, the timer entry is still pending.
    fx.wake_delivery.inject(ExchangeFault::LoseReply);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    assert!(matches!(
        scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Failed { .. }
    ));
    assert_eq!(wake_counters(&fx).await.admitted, 1);

    // The rescan redelivers the same derived operation id, the controller
    // answers from its record — the *original* admitted disposition — and the
    // entry finally marks fired: one admission however many scans it took.
    let scan = fx.wake_scanner().scan_due().await.expect("the rescan runs");
    assert_eq!(scan.outcomes.len(), 1);
    let rakka_agent::AgentWakeScanOutcome::Dispositioned {
        disposition,
        redelivery,
        marked,
        ..
    } = &scan.outcomes[0]
    else {
        panic!(
            "the rescan dispositions the entry, got {:?}",
            scan.outcomes[0]
        );
    };
    assert!(matches!(disposition, AgentWakeDisposition::Admitted { .. }));
    assert!(redelivery);
    assert_eq!(*marked, AgentWakeTimerStatus::Fired);
    assert_eq!(wake_counters(&fx).await.admitted, 1);
    assert_eq!(active_wakes(&fx).await, 1);

    // Nothing is left to scan.
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the final pass runs");
    assert!(scan.outcomes.is_empty());
}

#[tokio::test]
async fn a_double_delivery_is_answered_from_the_record() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;
    fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    fx.clock.fetch_add(1_000, Ordering::SeqCst);

    // The same admission command reaches the controller twice; the reply the
    // scanner sees is the second one, which must carry the first's result.
    fx.wake_delivery.inject(ExchangeFault::DeliverTwice);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    let rakka_agent::AgentWakeScanOutcome::Dispositioned {
        disposition,
        redelivery,
        ..
    } = &scan.outcomes[0]
    else {
        panic!(
            "the delivery dispositions the entry, got {:?}",
            scan.outcomes[0]
        );
    };
    assert!(matches!(disposition, AgentWakeDisposition::Admitted { .. }));
    assert!(redelivery);
    assert_eq!(fx.wake_delivery.deliveries(), 2);
    assert_eq!(wake_counters(&fx).await.admitted, 1);
    assert_eq!(active_wakes(&fx).await, 1);
}

#[tokio::test]
async fn a_crash_between_admission_and_mark_fired_converges_on_rescan() {
    // Reference run: count the wake-store writes of a schedule-then-scan flow.
    let reference = fixture();
    reference.instantiate_agent().await;
    reference.create_continuous_task().await;
    reference.schedule_wake(5, ScheduleRevision::INITIAL).await;
    reference.clock.fetch_add(1_000, Ordering::SeqCst);
    reference
        .wake_scanner()
        .scan_due()
        .await
        .expect("the reference pass runs");
    let writes = reference.wakes.writes();
    assert_eq!(writes, 2, "one schedule write, one mark-fired write");

    // Kill the scanner on the mark-fired write: the admission committed on
    // the task store, the timer entry stayed pending.
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;
    fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    // Arming resets the write counter, so the mark-fired write is the first
    // write from here.
    fx.wakes
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    fx.wake_scanner()
        .scan_due()
        .await
        .expect_err("the armed crash fires on the mark-fired write");
    fx.wakes
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    assert_eq!(wake_counters(&fx).await.admitted, 1);
    fx.wakes.survive();

    // A fresh scanner over the same durable state converges: the duplicate
    // admission is answered from the record with the original disposition,
    // and the entry marks fired.
    let scan = fx.wake_scanner().scan_due().await.expect("the rescan runs");
    assert_eq!(scan.outcomes.len(), 1);
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Admitted { .. },
            redelivery: true,
            ..
        }
    ));
    assert_eq!(wake_counters(&fx).await.admitted, 1);
}

#[tokio::test]
async fn the_wake_admission_survives_any_owner_loss() {
    // Reference run: how many task-store writes the create-then-admit flow
    // takes in total.
    let reference = fixture();
    reference.instantiate_agent().await;
    reference.create_continuous_task().await;
    let binding = common::scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let command = rakka_agent::wake_admission_command(binding.clone())
        .expect("the admission command derives");
    reference
        .apply_task_command(command.clone())
        .await
        .expect("the reference admission applies");
    let writes = reference.tasks.writes();
    assert!(writes >= 2, "the flow writes the task store at least twice");

    // Kill the owner on both sides of every task-store write of the whole
    // flow. Whatever committed is what the next owner finds; re-driving the
    // same commands under the same derived operation ids must converge on one
    // task and exactly one admission.
    sweep_crash_points(writes, |nth, point| {
        let command = command.clone();
        async move {
            let fx = fixture();
            fx.instantiate_agent().await;
            fx.tasks.crash_at(nth, point);
            fx.create_continuous_task().await;
            let _ = fx.apply_task_command(command.clone()).await;
            fx.tasks.assert_crash_fired(nth, point);
            fx.tasks.survive();

            // The next owner re-drives the identical sequence.
            fx.create_continuous_task().await;
            let reply = fx
                .apply_task_command(command)
                .await
                .unwrap_or_else(|error| {
                    panic!("the re-driven admission applies at write {nth} {point:?}: {error}")
                });
            assert!(
                matches!(
                    reply,
                    AgentTaskEntityReply::Applied { .. } | AgentTaskEntityReply::Duplicate { .. }
                ),
                "the re-driven admission converges at write {nth} {point:?}, got {reply:?}"
            );
            let counters = wake_counters(&fx).await;
            assert_eq!(
                counters.admitted, 1,
                "exactly one admission at write {nth} {point:?}"
            );
            assert_eq!(
                active_wakes(&fx).await,
                1,
                "exactly one active occurrence at write {nth} {point:?}"
            );
        }
    })
    .await;
}
