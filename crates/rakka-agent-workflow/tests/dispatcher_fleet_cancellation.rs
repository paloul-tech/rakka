//! Cancelling a run's dispatches in the order fence, settle, finish
//! (issue #145).
//!
//! The rule the fleet's prune relies on is that a fleet entry is finished
//! only after its outbox row is. The workflow-tier worker's cancellation
//! used to go the other way: it marked entries `Cancelled` and then settled
//! their rows, and repaired a crash between the two by reading the
//! `Cancelled` entries back. A prune that removed such an entry first left a
//! due row with nothing to stop it being registered and dispatched.
//!
//! It now fences the run's unfinished entries with the flag that already
//! means "never claimable again", settles their rows, and only then
//! finishes them. A crash at any point leaves fenced, unfinished entries,
//! which no prune removes and no worker claims.

use rakka_agent_workflow::{AgentDispatchStatus, AgentEffectId, AgentRunId, AgentTimestampMillis};
use rakka_workflow::{OutboxMessageId, OutboxStatus, WorkflowTelemetryEvent, WorkflowTimestamp};

mod fleet_support;

use fleet_support::*;

const RUN: &str = "run-a";
const FUTURE: AgentTimestampMillis = AgentTimestampMillis::new(u64::MAX);

fn run() -> AgentRunId {
    AgentRunId::new(RUN)
}

fn ids(effects: &[&str]) -> Vec<AgentEffectId> {
    effects
        .iter()
        .map(|effect| AgentEffectId::new(*effect))
        .collect()
}

async fn entry_of(world: &World, effect: &str) -> rakka_agent_workflow::AgentDispatchEntry {
    fleet_entry(&world.fleet_store, &run(), effect)
        .await
        .expect("the entry is in the record")
}

async fn row_status(world: &World, effect: &str) -> Option<OutboxStatus> {
    outbox_status(&world.workflow_store, &world.clock, &run(), effect).await
}

/// One completed effect, one in flight and one registered and unclaimed, all
/// of [`RUN`].
async fn done_flight_waiting(world: &World, worker: &mut TestWorker) {
    world.complete_one(worker, RUN, "effect-done").await;
    world.claim_one(worker, RUN, "effect-flight").await;
    world.register_one(worker, RUN, "effect-waiting").await;
}

#[tokio::test]
async fn a_request_fences_and_finishes_nothing() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    done_flight_waiting(&world, &mut worker).await;

    let request = worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");

    assert_eq!(request.run_id, run());
    assert_eq!(request.awaiting_settlement, ids(&["effect-waiting"]));
    assert_eq!(request.in_flight_entries, 1);
    assert_eq!(request.already_terminal_entries, 1);
    let flight = entry_of(&world, "effect-flight").await;
    assert_eq!(flight.status, AgentDispatchStatus::Claimed);
    assert!(flight.cancellation_requested);
    assert_eq!(
        flight.last_error_code.as_deref(),
        Some("cancellation-requested")
    );
    let waiting = entry_of(&world, "effect-waiting").await;
    assert_eq!(waiting.status, AgentDispatchStatus::Pending);
    assert!(waiting.cancellation_requested);
    assert_eq!(waiting.last_error_code, None, "it keeps the code it had");
    let done = entry_of(&world, "effect-done").await;
    assert_eq!(done.status, AgentDispatchStatus::Completed);
    assert!(!done.cancellation_requested);
    // The state read answers the same list.
    let record = fleet_record(&world.fleet_store)
        .await
        .expect("the record exists");
    assert_eq!(
        record
            .state
            .cancellations_awaiting_settlement(&run(), AgentTimestampMillis::new(100)),
        ids(&["effect-waiting"])
    );
}

#[tokio::test]
async fn a_second_request_writes_nothing_and_lists_what_still_waits() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    done_flight_waiting(&world, &mut worker).await;
    worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");
    let before = world.fleet_revision().await;

    let again = worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs again");

    assert_eq!(again.awaiting_settlement, ids(&["effect-waiting"]));
    assert_eq!(world.fleet_revision().await, before);
}

#[tokio::test]
async fn a_request_for_a_run_with_no_entries_writes_nothing() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    world.complete_one(&mut worker, "run-other", "effect").await;
    let before = world.fleet_revision().await;

    let request = worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");

    assert!(request.awaiting_settlement.is_empty());
    assert_eq!(request.in_flight_entries, 0);
    assert_eq!(request.already_terminal_entries, 0);
    assert_eq!(world.fleet_revision().await, before);
}

#[tokio::test]
async fn a_crash_after_the_fence_leaves_nothing_a_prune_can_remove() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    world.register_one(&mut worker, RUN, "effect").await;
    // The fence is written, and the worker dies before it settles the row.
    worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");
    assert_eq!(
        row_status(&world, "effect").await,
        Some(OutboxStatus::Pending)
    );

    // A prune at any horizon leaves the fenced entry, and no worker claims it.
    let mut survivor = world.worker("worker-b");
    let prune = survivor
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");
    assert_eq!(prune.pruned, 0);
    assert_eq!(prune.remaining, 1);
    let batch = survivor.claim_due().await.expect("the claim pass runs");
    assert!(batch.claims.is_empty(), "a fenced entry is never claimed");

    // The next cycle over the run settles the row and finishes the entry.
    let mut dispatcher = RecordingDispatcher::succeeding();
    let cycle = survivor
        .run_once([run()], &mut dispatcher)
        .await
        .expect("the cycle runs");

    assert!(cycle.claims.claims.is_empty());
    assert!(dispatcher.seen.is_empty(), "the cancelled effect never ran");
    assert_eq!(
        row_status(&world, "effect").await,
        Some(OutboxStatus::Cancelled)
    );
    let entry = entry_of(&world, "effect").await;
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
}

#[tokio::test]
async fn a_crash_after_the_settle_is_finished_by_the_next_refresh() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    world.register_one(&mut worker, RUN, "effect").await;
    worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");
    // The row is settled, and the worker dies before it finishes the entry.
    move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        "effect",
        OutboxStatus::Cancelled,
    )
    .await;
    assert_eq!(
        entry_of(&world, "effect").await.status,
        AgentDispatchStatus::Pending
    );

    let mut survivor = world.worker("worker-b");
    survivor.recover().await.expect("the fleet recovers");
    survivor
        .refresh_run(run(), None)
        .await
        .expect("the run refreshes");

    let entry = entry_of(&world, "effect").await;
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(
        row_status(&world, "effect").await,
        Some(OutboxStatus::Cancelled)
    );
}

#[tokio::test]
async fn a_lease_that_lapses_between_the_two_writes_is_not_finalized() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    done_flight_waiting(&world, &mut worker).await;
    let request = worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");
    // The in-flight entry's lease lapses after the request listed what it
    // would settle. Its row was not settled, so it must not be finished.
    world.clock.advance_millis(LEASE_MS + 1);

    let none = worker
        .fleet_mut()
        .finalize_run_cancellations(&run(), &[])
        .await
        .expect("the finalize runs");
    let finished = worker
        .fleet_mut()
        .finalize_run_cancellations(&run(), &request.awaiting_settlement)
        .await
        .expect("the finalize runs");

    assert!(none.is_empty());
    assert_eq!(finished, ids(&["effect-waiting"]));
    assert_eq!(
        entry_of(&world, "effect-waiting").await.status,
        AgentDispatchStatus::Cancelled
    );
    let flight = entry_of(&world, "effect-flight").await;
    assert_eq!(flight.status, AgentDispatchStatus::Claimed);
    assert!(flight.cancellation_requested);
}

#[tokio::test]
async fn a_finalize_finishes_only_an_entry_that_is_fenced_and_not_in_flight() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    done_flight_waiting(&world, &mut worker).await;
    let before = world.fleet_revision().await;

    // Nothing was fenced: naming the entries finishes none of them.
    let unfenced = worker
        .fleet_mut()
        .finalize_run_cancellations(&run(), &ids(&["effect-waiting", "effect-flight"]))
        .await
        .expect("the finalize runs");
    assert!(unfenced.is_empty());
    assert_eq!(world.fleet_revision().await, before);

    worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");
    // Fenced and still in flight: named, and still not finished.
    let in_flight = worker
        .fleet_mut()
        .finalize_run_cancellations(&run(), &ids(&["effect-flight", "effect-done"]))
        .await
        .expect("the finalize runs");
    assert!(in_flight.is_empty());
    assert_eq!(
        entry_of(&world, "effect-flight").await.status,
        AgentDispatchStatus::Claimed
    );
    assert_eq!(
        entry_of(&world, "effect-done").await.status,
        AgentDispatchStatus::Completed
    );
}

#[tokio::test]
async fn an_entry_an_earlier_binary_cancelled_first_is_still_settled() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    // What an earlier binary's fleet-first cancellation leaves after a
    // crash: the entry `Cancelled`, the row never settled.
    let claim = world.claim_one(&mut worker, RUN, "effect").await;
    worker
        .fleet_mut()
        .record_claim_failure(
            &claim,
            &WorkflowTelemetryEvent::OutboxDispatchCancelled {
                message_id: OutboxMessageId::new("effect"),
                at: WorkflowTimestamp::from_millis(100),
                message: "run-cancelled".to_string(),
            },
        )
        .await
        .expect("the entry is cancelled at the fleet alone");
    assert_eq!(
        row_status(&world, "effect").await,
        Some(OutboxStatus::Pending)
    );

    let registration = worker
        .refresh_run(run(), None)
        .await
        .expect("the run refreshes");

    assert_eq!(
        row_status(&world, "effect").await,
        Some(OutboxStatus::Cancelled)
    );
    assert_eq!(registration.registered_effects, 0);
    assert_eq!(
        entry_of(&world, "effect").await.status,
        AgentDispatchStatus::Cancelled
    );
}

#[tokio::test]
async fn a_fresh_handles_refresh_reads_the_fleet_before_the_inbox() {
    let world = World::new();
    schedule(&world.workflow_store, &world.clock, &run(), "effect").await;
    // Before the fresh handle's first read of the fleet, another worker
    // registers the effect, runs it, finishes it, and prunes its entry.
    let mut other = world.worker("worker-other");
    let clock = world.clock.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    world.fleet_store.before_next_load(Box::new(move || {
        Box::pin(async move {
            let mut dispatcher = RecordingDispatcher::succeeding();
            other
                .run_once([run()], &mut dispatcher)
                .await
                .expect("the other worker's cycle runs");
            clock.advance_millis(1);
            let prune = other
                .fleet_mut()
                .prune_terminal(FUTURE)
                .await
                .expect("the prune runs");
            sender
                .send((dispatcher.seen.len(), prune.pruned))
                .expect("the test is waiting");
        })
    }));

    let mut fresh = world.worker("worker-fresh");
    let registration = fresh
        .refresh_run(run(), None)
        .await
        .expect("the run refreshes");

    let (dispatched, pruned) = receiver.await.expect("the other worker ran");
    assert_eq!(dispatched, 1);
    assert_eq!(pruned, 1);
    assert_eq!(
        registration.observed_due_effects, 0,
        "the due list was read after the fleet"
    );
    assert_eq!(registration.registered_effects, 0);
    assert!(
        fleet_entry(&world.fleet_store, &run(), "effect")
            .await
            .is_none(),
        "a finished effect is not registered again"
    );
}

#[tokio::test]
async fn the_workers_cancellation_reports_what_it_finished() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    done_flight_waiting(&world, &mut worker).await;

    let cancellation = worker
        .cancel_run_dispatches(&run())
        .await
        .expect("the cancellation runs");

    assert_eq!(cancellation.run_id, run());
    assert_eq!(cancellation.cancelled_entries, 1);
    assert_eq!(cancellation.cancelled_effect_ids, ids(&["effect-waiting"]));
    assert_eq!(cancellation.in_flight_entries, 1);
    assert_eq!(cancellation.already_terminal_entries, 1);
    assert_eq!(
        row_status(&world, "effect-waiting").await,
        Some(OutboxStatus::Cancelled)
    );
    assert_eq!(
        entry_of(&world, "effect-waiting").await.status,
        AgentDispatchStatus::Cancelled
    );
    let flight = entry_of(&world, "effect-flight").await;
    assert_eq!(flight.status, AgentDispatchStatus::Claimed);
    assert!(flight.cancellation_requested);
}

// ---------------------------------------------------------------------------
// A lost attempt survives the cancellation (issue #209). A `Dispatching` row
// is an attempt whose worker was lost after `Started`: its outcome is
// unknown. The cancellation counts it with a line every reader can tell from
// a ticket that never started.
// ---------------------------------------------------------------------------

/// `effect`'s row: its status, attempt count and last failure line.
async fn row_of(world: &World, effect: &str) -> (OutboxStatus, u32, Option<String>) {
    let row = outbox_row(&world.workflow_store, &world.clock, &run(), effect)
        .await
        .expect("the row exists");
    (
        row.status(),
        row.attempts().attempts(),
        row.attempts().last_error().map(str::to_string),
    )
}

/// A `Dispatching` row is cancelled with the witness. `effect-flight`'s
/// worker marked its row and was lost; its lease has lapsed. The worker's
/// cancellation counts that attempt with the witness line, and cancels a
/// waiting row as before, counting nothing.
#[tokio::test]
async fn a_dispatching_row_is_cancelled_with_the_witness() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let _claim = world.claim_one(&mut worker, RUN, "effect-flight").await;
    move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        "effect-flight",
        OutboxStatus::Dispatching,
    )
    .await;
    world.register_one(&mut worker, RUN, "effect-waiting").await;
    world.clock.advance_millis(LEASE_MS + 1);

    worker
        .cancel_run_dispatches(&run())
        .await
        .expect("the cancellation runs");

    assert_eq!(
        row_of(&world, "effect-flight").await,
        (
            OutboxStatus::Cancelled,
            1,
            Some(rakka_agent_workflow::DISPATCH_CANCELLED_IN_FLIGHT_LINE.to_string())
        )
    );
    assert_eq!(
        row_of(&world, "effect-waiting").await,
        (OutboxStatus::Cancelled, 0, None)
    );
    for effect in ["effect-flight", "effect-waiting"] {
        assert_eq!(
            entry_of(&world, effect).await.status,
            AgentDispatchStatus::Cancelled,
            "{effect}"
        );
    }
}

/// A refresh settles a fenced `Dispatching` row with the witness. The
/// cancellation request lands while `effect-flight` is in flight; its
/// worker marks the row and is lost, and the lease lapses. The next refresh
/// counts the attempt with the witness line and finishes the entry.
#[tokio::test]
async fn a_refresh_settles_a_fenced_dispatching_row_with_the_witness() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let _claim = world.claim_one(&mut worker, RUN, "effect-flight").await;
    let request = worker
        .fleet_mut()
        .request_run_cancellation(&run())
        .await
        .expect("the request runs");
    assert_eq!(request.in_flight_entries, 1);
    move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        "effect-flight",
        OutboxStatus::Dispatching,
    )
    .await;
    world.clock.advance_millis(LEASE_MS + 1);

    worker
        .refresh_run(run(), None)
        .await
        .expect("the refresh runs");

    assert_eq!(
        row_of(&world, "effect-flight").await,
        (
            OutboxStatus::Cancelled,
            1,
            Some(rakka_agent_workflow::DISPATCH_CANCELLED_IN_FLIGHT_LINE.to_string())
        )
    );
    assert_eq!(
        entry_of(&world, "effect-flight").await.status,
        AgentDispatchStatus::Cancelled
    );
}
