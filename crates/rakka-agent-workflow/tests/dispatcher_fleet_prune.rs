//! Removing finished entries from the dispatcher fleet record (issue #145).
//!
//! The record is one durable record that every worker reads and writes
//! whole, and nothing used to remove an entry from it.
//! [`AgentDispatcherFleet::prune_terminal`] removes the entries that finished
//! before a horizon. It never touches an entry that is not finished, it
//! writes nothing when it finds nothing, and it is one more writer among the
//! workers: it reads the record fresh and re-applies a write it loses.

use rakka_agent_workflow::{
    AgentDispatchClaim, AgentDispatchStatus, AgentDispatcherFleetState, AgentRunId,
    AgentTimestampMillis, METRIC_AGENT_DISPATCHER_FLEET,
};
use rakka_persistence::{DurableStateStore, Revision};
use rakka_workflow::{OutboxMessageId, WorkflowTelemetryEvent, WorkflowTimestamp};

mod fleet_support;

use fleet_support::*;

/// A horizon after every instant a test reaches.
const FUTURE: AgentTimestampMillis = AgentTimestampMillis::new(u64::MAX);

#[tokio::test]
async fn a_one_entry_record_keeps_the_bytes_it_had() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, "run-a", "effect-a").await;
    worker
        .fleet_mut()
        .complete_claim(&claim)
        .await
        .expect("the claim completes");
    let record = fleet_record(&world.fleet_store)
        .await
        .expect("the record exists");

    let bytes = serde_json::to_string(&record.state).expect("the state encodes");

    assert_eq!(bytes, PINNED_ONE_ENTRY_RECORD);
}

/// The bytes this build writes for the record the test above builds: what
/// `ff9a495` wrote, and `row_scheduled_at` (issues #153 and #154).
const PINNED_ONE_ENTRY_RECORD: &str = r#"{"entries":{"run-a:effect-a":{"dispatch_id":"run-a:effect-a","workflow_id":null,"run_id":"run-a","effect_id":"effect-a","effect_kind":"tool-call","target":{"target_type":"tool","name":"research-tool","address":"tool://research-tool","attributes":{}},"target_class":"tool","due_at":100,"row_scheduled_at":100,"status":"completed","lease":null,"last_fencing_token":1,"attempts":0,"cancellation_requested":false,"last_error_code":null,"created_at":100,"updated_at":100,"completed_at":100,"exhausted_at":null,"attributes":{}}},"updated_at":100}"#;

/// A record written before `row_scheduled_at` existed is read, and written
/// back byte for byte: an entry with no stamp is not given one, and the
/// field is absent rather than null.
#[test]
fn a_record_written_before_the_row_stamp_keeps_the_bytes_it_had() {
    let state: AgentDispatcherFleetState =
        serde_json::from_str(PINNED_RECORD_BEFORE_THE_ROW_STAMP).expect("the record decodes");

    let bytes = serde_json::to_string(&state).expect("the state encodes");

    assert_eq!(bytes, PINNED_RECORD_BEFORE_THE_ROW_STAMP);
}

/// The bytes `ff9a495` wrote for the same record.
const PINNED_RECORD_BEFORE_THE_ROW_STAMP: &str = r#"{"entries":{"run-a:effect-a":{"dispatch_id":"run-a:effect-a","workflow_id":null,"run_id":"run-a","effect_id":"effect-a","effect_kind":"tool-call","target":{"target_type":"tool","name":"research-tool","address":"tool://research-tool","attributes":{}},"target_class":"tool","due_at":100,"status":"completed","lease":null,"last_fencing_token":1,"attempts":0,"cancellation_requested":false,"last_error_code":null,"created_at":100,"updated_at":100,"completed_at":100,"exhausted_at":null,"attributes":{}}},"updated_at":100}"#;

/// Settles a claim as exhausted, with an event built by hand: the fleet
/// follows whatever outbox transition it is told about.
async fn exhaust(worker: &mut TestWorker, claim: &AgentDispatchClaim) {
    worker
        .fleet_mut()
        .record_claim_failure(
            claim,
            &WorkflowTelemetryEvent::OutboxDispatchExhausted {
                message_id: OutboxMessageId::new(claim.effect_id.as_str()),
                attempts: 3,
                message: "exhausted".to_string(),
            },
        )
        .await
        .expect("the claim settles exhausted");
}

/// Settles a claim as cancelled, with an event built by hand.
async fn cancel(worker: &mut TestWorker, claim: &AgentDispatchClaim, at: u64) {
    worker
        .fleet_mut()
        .record_claim_failure(
            claim,
            &WorkflowTelemetryEvent::OutboxDispatchCancelled {
                message_id: OutboxMessageId::new(claim.effect_id.as_str()),
                at: WorkflowTimestamp::from_millis(at),
                message: "ticket-stale".to_string(),
            },
        )
        .await
        .expect("the claim settles cancelled");
}

/// Reschedules a claim far in the future, with an event built by hand.
async fn retry_later(worker: &mut TestWorker, claim: &AgentDispatchClaim) {
    worker
        .fleet_mut()
        .record_claim_failure(
            claim,
            &WorkflowTelemetryEvent::OutboxDispatchRetried {
                message_id: OutboxMessageId::new(claim.effect_id.as_str()),
                attempt: 1,
                next_retry_at: WorkflowTimestamp::from_millis(1_000_000),
                message: "later".to_string(),
            },
        )
        .await
        .expect("the claim reschedules");
}

async fn status_of(world: &World, run: &str, effect: &str) -> Option<AgentDispatchStatus> {
    fleet_entry(&world.fleet_store, &AgentRunId::new(run), effect)
        .await
        .map(|entry| entry.status)
}

/// A world holding three entries that finished at 100, one of each finished
/// status, and one that finished at 200. The clock is left at 200.
async fn three_old_and_one_new(world: &World, worker: &mut TestWorker) {
    let completed = world.claim_one(worker, "run-completed", "effect").await;
    worker
        .fleet_mut()
        .complete_claim(&completed)
        .await
        .expect("the claim completes");
    let exhausted = world.claim_one(worker, "run-exhausted", "effect").await;
    exhaust(worker, &exhausted).await;
    let cancelled = world.claim_one(worker, "run-cancelled", "effect").await;
    cancel(worker, &cancelled, 100).await;
    world.clock.advance_millis(100);
    let newer = world.claim_one(worker, "run-newer", "effect").await;
    worker
        .fleet_mut()
        .complete_claim(&newer)
        .await
        .expect("the claim completes");
}

#[tokio::test]
async fn a_prune_removes_finished_entries_older_than_the_horizon() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    three_old_and_one_new(&world, &mut worker).await;

    let prune = worker
        .fleet_mut()
        .prune_terminal(AgentTimestampMillis::new(200))
        .await
        .expect("the prune runs");

    assert_eq!(prune.pruned, 3);
    assert_eq!(prune.remaining, 1);
    assert_eq!(status_of(&world, "run-completed", "effect").await, None);
    assert_eq!(status_of(&world, "run-exhausted", "effect").await, None);
    assert_eq!(status_of(&world, "run-cancelled", "effect").await, None);
    // An entry that finished at the horizon itself is kept: the horizon is
    // "before", not "at or before".
    assert_eq!(
        status_of(&world, "run-newer", "effect").await,
        Some(AgentDispatchStatus::Completed)
    );
}

#[tokio::test]
async fn a_prune_keeps_every_live_entry_at_a_future_horizon() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let mut brief = world.worker_with_lease("worker-brief", 10);

    let retried = world.claim_one(&mut worker, "run-retry", "effect").await;
    retry_later(&mut worker, &retried).await;
    // In flight when its run is cancelled: fenced, and not finished.
    world.claim_one(&mut worker, "run-fenced", "effect").await;
    worker
        .cancel_run_dispatches(&AgentRunId::new("run-fenced"))
        .await
        .expect("the cancellation is requested");
    // Claimed under a ten-millisecond lease, which the clock then passes.
    world.claim_one(&mut brief, "run-lapsed", "effect").await;
    world.claim_one(&mut worker, "run-live", "effect").await;
    world.clock.advance_millis(50);
    world
        .register_one(&mut worker, "run-pending", "effect")
        .await;
    let before = world.fleet_revision().await;

    let prune = worker
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");

    assert_eq!(prune.pruned, 0);
    assert_eq!(prune.remaining, 5);
    assert_eq!(world.fleet_revision().await, before, "nothing was written");
    assert_eq!(
        status_of(&world, "run-retry", "effect").await,
        Some(AgentDispatchStatus::RetryScheduled)
    );
    assert_eq!(
        status_of(&world, "run-pending", "effect").await,
        Some(AgentDispatchStatus::Pending)
    );
    for run in ["run-fenced", "run-lapsed", "run-live"] {
        assert_eq!(
            status_of(&world, run, "effect").await,
            Some(AgentDispatchStatus::Claimed),
            "{run} is claimed and stays"
        );
    }
    let fenced = fleet_entry(&world.fleet_store, &AgentRunId::new("run-fenced"), "effect")
        .await
        .expect("the fenced entry stays");
    assert!(fenced.cancellation_requested);
    let now = AgentTimestampMillis::new(150);
    let lapsed = fleet_entry(&world.fleet_store, &AgentRunId::new("run-lapsed"), "effect")
        .await
        .expect("the lapsed entry stays");
    assert!(!lapsed.is_in_flight_at(now), "its lease has lapsed");
    let live = fleet_entry(&world.fleet_store, &AgentRunId::new("run-live"), "effect")
        .await
        .expect("the live entry stays");
    assert!(live.is_in_flight_at(now), "its lease is live");
}

#[tokio::test]
async fn a_prune_with_nothing_eligible_writes_nothing() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    three_old_and_one_new(&world, &mut worker).await;
    let before = world.fleet_revision().await;
    let writes = world.fleet_store.writes();

    // Nothing finished before 100.
    let prune = worker
        .fleet_mut()
        .prune_terminal(AgentTimestampMillis::new(100))
        .await
        .expect("the prune runs");

    assert_eq!(prune.pruned, 0);
    assert_eq!(prune.remaining, 4);
    assert_eq!(world.fleet_revision().await, before);
    assert_eq!(world.fleet_store.writes(), writes);
}

#[tokio::test]
async fn a_prune_on_a_store_with_no_record_creates_none() {
    let world = World::new();
    let mut worker = world.worker("worker-a");

    let prune = worker
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");

    assert_eq!(prune.pruned, 0);
    assert_eq!(prune.remaining, 0);
    assert!(fleet_record(&world.fleet_store).await.is_none());
}

#[tokio::test]
async fn a_prune_retries_a_lost_write_and_succeeds() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    three_old_and_one_new(&world, &mut worker).await;
    world.fleet_store.contend(3);

    let prune = worker
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("three lost writes are inside the bound");

    assert_eq!(prune.pruned, 4);
    assert_eq!(prune.remaining, 0);
}

#[tokio::test]
async fn a_prune_that_loses_more_than_the_bound_returns_the_conflict() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    three_old_and_one_new(&world, &mut worker).await;
    world.fleet_store.contend(4);

    let error = worker
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect_err("a fourth lost write is past the bound");

    assert_eq!(error.code(), "revision-conflict");
    let record = fleet_record(&world.fleet_store)
        .await
        .expect("the record exists");
    assert_eq!(record.state.entries().len(), 4, "nothing was removed");
}

#[tokio::test]
async fn two_prunes_from_two_handles_remove_the_eligible_entries_once() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    three_old_and_one_new(&world, &mut worker_a).await;
    // The second prune lands between the first one's read and its write.
    let mut worker_b = world.worker("worker-b");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            let prune = worker_b
                .fleet_mut()
                .prune_terminal(FUTURE)
                .await
                .expect("the second prune runs");
            sender.send(prune).expect("the first prune is waiting");
        })
    }));

    let first = worker_a
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the first prune runs");
    let second = receiver.await.expect("the second prune ran");

    assert_eq!(first.pruned + second.pruned, 4);
    assert_eq!(first.remaining, 0);
    assert_eq!(second.remaining, 0);
    let record = fleet_record(&world.fleet_store)
        .await
        .expect("the record exists");
    assert!(record.state.entries().is_empty());
}

#[tokio::test]
async fn a_prune_removes_ten_thousand_entries_in_one_write() {
    let world = World::new();
    // Ten thousand finished entries, built from the pinned one-entry record.
    let template: serde_json::Value =
        serde_json::from_str(PINNED_ONE_ENTRY_RECORD).expect("the pinned record parses");
    let entry = template["entries"]["run-a:effect-a"].clone();
    let mut entries = serde_json::Map::new();
    for index in 0..10_000 {
        let mut entry = entry.clone();
        let run = format!("run-{index}");
        entry["dispatch_id"] = serde_json::json!(format!("{run}:effect-a"));
        entry["run_id"] = serde_json::json!(run);
        entries.insert(format!("{run}:effect-a"), entry);
    }
    let state: AgentDispatcherFleetState =
        serde_json::from_value(serde_json::json!({ "entries": entries, "updated_at": 100 }))
            .expect("the built record decodes");
    assert_eq!(state.entries().len(), 10_000);
    let seeded = world
        .fleet_store
        .inner()
        .compare_and_set(
            &rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
            Revision::INITIAL,
            state,
        )
        .await
        .expect("the record is seeded");
    let mut worker = world.worker("worker-a");

    let prune = worker
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");

    assert_eq!(prune.pruned, 10_000);
    assert_eq!(prune.remaining, 0);
    assert_eq!(
        world.fleet_revision().await,
        Some(seeded.revision.next()),
        "one write removed them all"
    );
}

#[tokio::test]
async fn a_lapsed_claim_on_a_pruned_entry_is_fenced() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let stale = world.claim_one(&mut worker_a, "run-a", "effect").await;
    // The lease lapses, another worker takes the entry over, finishes it,
    // and a prune removes it.
    world.clock.advance_millis(LEASE_MS + 1);
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("B re-claims");
    worker_b
        .fleet_mut()
        .complete_claim(&batch.claims[0])
        .await
        .expect("B completes");
    world.clock.advance_millis(1);
    let prune = worker_b
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");
    assert_eq!(prune.pruned, 1);
    let before = world.fleet_revision().await;

    // A reads the record again and finds no entry for its claim.
    worker_a.recover().await.expect("the fleet recovers");
    let current = worker_a
        .fleet_mut()
        .is_current_claim(&stale)
        .expect("an absent entry is an answer, not an error");
    let covers = worker_a
        .fleet_mut()
        .is_current_claim_for(&stale, 0)
        .expect("an absent entry is an answer, not an error");
    let error = worker_a
        .fleet_mut()
        .complete_claim(&stale)
        .await
        .expect_err("a claim on a pruned entry is not settled");

    assert!(!current);
    assert!(!covers, "a claim whose entry is gone covers nothing");
    assert_eq!(error.code(), "claim-fenced");
    assert_eq!(world.fleet_revision().await, before, "nothing was written");
}

#[tokio::test]
async fn a_prune_from_another_handle_does_not_cost_a_claim_its_settle() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    world.complete_one(&mut worker_a, "run-old", "effect").await;
    world.clock.advance_millis(1);
    let claim = world.claim_one(&mut worker_a, "run-live", "effect").await;
    // Another handle prunes while A's claim is in flight.
    let mut worker_b = world.worker("worker-b");
    let prune = worker_b
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");
    assert_eq!(prune.pruned, 1);
    assert_eq!(prune.remaining, 1);

    let settled = worker_a
        .fleet_mut()
        .complete_claim(&claim)
        .await
        .expect("the settle re-reads past the prune");

    assert_eq!(settled.status, AgentDispatchStatus::Completed);
    assert_eq!(
        status_of(&world, "run-live", "effect").await,
        Some(AgentDispatchStatus::Completed)
    );
    assert_eq!(status_of(&world, "run-old", "effect").await, None);
}

#[tokio::test]
async fn a_prune_counts_what_it_removed() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    three_old_and_one_new(&world, &mut worker).await;

    worker
        .fleet_mut()
        .prune_terminal(AgentTimestampMillis::new(200))
        .await
        .expect("the prune runs");

    let snapshot = world.metrics.snapshot();
    let pruned: f64 = snapshot
        .observations_named(METRIC_AGENT_DISPATCHER_FLEET)
        .into_iter()
        .filter(|observation| {
            observation.attribute("operation") == Some("prune")
                && observation.attribute("outcome") == Some("pruned")
                && observation.attribute("detail") == Some("none")
        })
        .map(|observation| observation.value())
        .sum();
    assert_eq!(pruned, 3.0);
}

#[tokio::test]
async fn registering_a_run_again_after_a_prune_inserts_nothing() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    world.complete_one(&mut worker, "run-a", "effect").await;
    world.clock.advance_millis(1);
    worker
        .fleet_mut()
        .prune_terminal(FUTURE)
        .await
        .expect("the prune runs");

    let registration = worker
        .refresh_run(AgentRunId::new("run-a"), None)
        .await
        .expect("the run refreshes");

    assert_eq!(registration.observed_due_effects, 0);
    assert_eq!(registration.registered_effects, 0);
    assert_eq!(status_of(&world, "run-a", "effect").await, None);
}
