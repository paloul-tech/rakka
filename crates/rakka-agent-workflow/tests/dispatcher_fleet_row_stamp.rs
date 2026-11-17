//! A fleet entry remembers which outbox row it was registered from (issues
//! #153 and #154).
//!
//! A fleet entry outlives its outbox row when a run's workflow record is
//! purged, or one store is restored without the other. The owner of the
//! outbox can then schedule the same ticket again, as a new row with no
//! attempt history, and nothing told that row from the one that was lost.
//!
//! An outbox row's `scheduled_at` is written once, when the row is created,
//! and no retry changes it. Registration records it on the entry, and
//! [`AgentDispatchEntry::indexes_row`] answers whether a row is the one the
//! entry was registered from. A registration that reads another row leaves
//! the entry as it was: what the entry holds is the last the fleet learned
//! from the row that is gone.
//!
//! The stamp is the instant the row was created only for an effect that
//! names no due time, which is every ticket the agent tier schedules. An
//! effect that names one is stamped with that time, whenever its row is
//! written, and the last test here pins what follows.

use rakka_agent_workflow::{
    agent_run_workflow_id, AgentDispatchStatus, AgentDispatcherFleetState, AgentRunId,
    AgentTimestampMillis,
};
use rakka_persistence::DurableStateStore;
use rakka_workflow::{OutboxMessageId, OutboxStatus, WorkflowTelemetryEvent, WorkflowTimestamp};

mod fleet_support;

use fleet_support::*;

const RUN: &str = "run-a";
const EFFECT: &str = "effect";

fn run() -> AgentRunId {
    AgentRunId::new(RUN)
}

/// Schedules [`EFFECT`] with no due time of its own, so its row is stamped
/// with the instant it is written.
async fn schedule_due_when_scheduled(world: &World) {
    schedule_effect(
        &world.workflow_store,
        &world.clock,
        &run(),
        tool_effect_due_when_scheduled(EFFECT),
    )
    .await;
}

/// Schedules [`EFFECT`] with no due time of its own and registers it.
async fn register(world: &World, worker: &mut TestWorker) {
    schedule_due_when_scheduled(world).await;
    worker.recover().await.expect("the fleet recovers");
    worker
        .refresh_run(run(), None)
        .await
        .expect("the run registers");
}

/// Deletes the run's whole workflow record, and with it every outbox row.
async fn lose_the_workflow_record(world: &World) {
    let id = agent_run_workflow_id(&run()).persistence_id();
    let record = world
        .workflow_store
        .load(&id)
        .await
        .expect("the workflow store loads")
        .expect("the run has a workflow record");
    world
        .workflow_store
        .delete(&id, record.revision)
        .await
        .expect("the workflow record is deleted");
}

#[tokio::test]
async fn a_registration_records_when_its_row_was_scheduled() {
    let world = World::new();
    let mut worker = world.worker("worker-a");

    register(&world, &mut worker).await;

    let row = outbox_row(&world.workflow_store, &world.clock, &run(), EFFECT)
        .await
        .expect("the row exists");
    let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(
        entry.row_scheduled_at,
        Some(AgentTimestampMillis::new(row.scheduled_at().as_millis()))
    );
    assert!(entry.indexes_row(&row));
}

/// The same row, after a failed attempt: its stamp did not move, so the
/// entry still indexes it and follows it.
#[tokio::test]
async fn a_row_that_was_retried_is_still_the_row_the_entry_indexes() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    register(&world, &mut worker).await;
    world.clock.advance_millis(50);
    let row = move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        EFFECT,
        OutboxStatus::Failed,
    )
    .await;
    assert_eq!(row.attempts().attempts(), 1);
    // Past the retry backoff, so the row is due again.
    world.clock.advance_millis(60_000);

    let registration = worker
        .refresh_run(run(), None)
        .await
        .expect("the run registers");

    assert_eq!(registration.registered_effects, 1);
    let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert!(entry.indexes_row(&row));
    assert_eq!(entry.attempts, 1, "the entry follows its own row");
    assert_eq!(entry.row_scheduled_at, Some(AgentTimestampMillis::new(100)));
}

/// The row is lost after an attempt failed, and the same ticket is scheduled
/// again. The entry does not index the new row, and registering from it
/// changes nothing: the attempt the fleet counted is not overwritten by a
/// row that never saw it.
#[tokio::test]
async fn a_registration_from_another_row_leaves_the_entry_as_it_was() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    register(&world, &mut worker).await;
    let claim = worker
        .claim_due()
        .await
        .expect("the claim pass runs")
        .claims
        .remove(0);
    worker
        .fleet_mut()
        .record_claim_failure(
            &claim,
            &WorkflowTelemetryEvent::OutboxDispatchRetried {
                message_id: OutboxMessageId::new(EFFECT),
                attempt: 1,
                next_retry_at: WorkflowTimestamp::from_millis(200),
                message: "tool-timeout".to_string(),
            },
        )
        .await
        .expect("the claim reschedules");
    let before = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(
        (before.status, before.attempts),
        (AgentDispatchStatus::RetryScheduled, 1)
    );
    lose_the_workflow_record(&world).await;
    world.clock.advance_millis(400);
    schedule_due_when_scheduled(&world).await;
    let recreated = outbox_row(&world.workflow_store, &world.clock, &run(), EFFECT)
        .await
        .expect("the row was scheduled again");
    assert_eq!(recreated.attempts().attempts(), 0);
    let revision = world.fleet_revision().await;

    let registration = worker
        .refresh_run(run(), None)
        .await
        .expect("the run registers");

    assert_eq!(registration.observed_due_effects, 1);
    assert_eq!(registration.registered_effects, 0);
    assert_eq!(
        world.fleet_revision().await,
        revision,
        "nothing was written"
    );
    let after = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(after, before);
    assert!(!after.indexes_row(&recreated));
}

/// An entry written by a build that recorded no stamp: it decodes, it
/// indexes whatever row it is shown, and a registration updates it as it
/// always did, without giving it a stamp.
#[tokio::test]
async fn an_entry_with_no_stamp_indexes_any_row_and_registers_as_before() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    register(&world, &mut worker).await;
    let record = fleet_record(&world.fleet_store)
        .await
        .expect("the record exists");
    let mut state = serde_json::to_value(&record.state).expect("the state encodes");
    let dispatch_id = format!("{RUN}:{EFFECT}");
    assert!(
        state["entries"][&dispatch_id]
            .as_object_mut()
            .expect("the entry is an object")
            .remove("row_scheduled_at")
            .is_some(),
        "the stamp was there to remove"
    );
    let earlier: AgentDispatcherFleetState =
        serde_json::from_value(state).expect("a record with no stamp decodes");
    world
        .fleet_store
        .inner()
        .compare_and_set(
            &rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
            record.revision,
            earlier,
        )
        .await
        .expect("the earlier record is written");
    lose_the_workflow_record(&world).await;
    world.clock.advance_millis(400);
    schedule_due_when_scheduled(&world).await;
    let recreated = move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        EFFECT,
        OutboxStatus::Failed,
    )
    .await;
    // Past the retry backoff, so the row is due again.
    world.clock.advance_millis(60_000);

    let mut worker = world.worker("worker-b");
    worker.recover().await.expect("the fleet recovers");
    let registration = worker
        .refresh_run(run(), None)
        .await
        .expect("the run registers");

    assert_eq!(registration.registered_effects, 1);
    let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(entry.row_scheduled_at, None, "no stamp is adopted later");
    assert!(entry.indexes_row(&recreated));
    assert_eq!(entry.attempts, 1, "the entry follows the row it was shown");
}

/// An effect that names its own due time is stamped with that time, not
/// with the instant its row is written. Scheduled again after its row was
/// lost, it carries the same stamp, and the entry cannot tell the two rows
/// apart. The agent tier's tickets name no due time; this is the workflow
/// tier's shape, whose dispatcher does not read the stamp.
#[tokio::test]
async fn a_row_scheduled_for_a_stated_time_is_not_told_from_its_replacement() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    world.register_one(&mut worker, RUN, EFFECT).await;
    lose_the_workflow_record(&world).await;
    world.clock.advance_millis(400);
    schedule(&world.workflow_store, &world.clock, &run(), EFFECT).await;
    let recreated = outbox_row(&world.workflow_store, &world.clock, &run(), EFFECT)
        .await
        .expect("the row was scheduled again");

    let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(entry.row_scheduled_at, Some(AgentTimestampMillis::new(100)));
    assert!(entry.indexes_row(&recreated));
}
