//! A fleet claim follows an outbox row that is already finished (issue #145).
//!
//! The outbox row is the authority on whether an effect is done. A worker
//! can find its claim's row already finished: another worker finished it
//! after this one's lease lapsed, or a crash fell between the outbox write
//! and the fleet write. [`AgentDispatcherFleet::settle_claim_from_row`]
//! brings the fleet entry in line with the row, and the workflow-tier worker
//! never dispatches a row that is finished or gone.

use rakka_agent_workflow::{AgentDispatchStatus, AgentRunId};
use rakka_workflow::OutboxStatus;

mod fleet_support;

use fleet_support::*;

const RUN: &str = "run-a";
const EFFECT: &str = "effect";

fn run() -> AgentRunId {
    AgentRunId::new(RUN)
}

#[tokio::test]
async fn a_dispatched_row_completes_the_entry() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, RUN, EFFECT).await;
    let row = move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        EFFECT,
        OutboxStatus::Dispatched,
    )
    .await;

    let entry = worker
        .fleet_mut()
        .settle_claim_from_row(&claim, Some(&row))
        .await
        .expect("the claim settles");

    assert_eq!(entry.status, AgentDispatchStatus::Completed);
    assert_eq!(entry.last_error_code, None, "a completion carries no code");
    assert_eq!(entry.lease, None);
    let stored = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(stored, entry);
}

#[tokio::test]
async fn an_exhausted_row_exhausts_the_entry() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, RUN, EFFECT).await;
    let row = move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        EFFECT,
        OutboxStatus::Exhausted,
    )
    .await;

    let entry = worker
        .fleet_mut()
        .settle_claim_from_row(&claim, Some(&row))
        .await
        .expect("the claim settles");

    assert_eq!(entry.status, AgentDispatchStatus::Exhausted);
    assert_eq!(entry.attempts, row.attempts().attempts());
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-settled"));
    assert!(entry.exhausted_at.is_some());
}

#[tokio::test]
async fn a_cancelled_row_cancels_the_entry() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, RUN, EFFECT).await;
    let row = move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        EFFECT,
        OutboxStatus::Cancelled,
    )
    .await;

    let entry = worker
        .fleet_mut()
        .settle_claim_from_row(&claim, Some(&row))
        .await
        .expect("the claim settles");

    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-settled"));
}

#[tokio::test]
async fn an_absent_row_cancels_the_entry() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, RUN, EFFECT).await;

    let entry = worker
        .fleet_mut()
        .settle_claim_from_row(&claim, None)
        .await
        .expect("the claim settles");

    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-missing"));
}

#[tokio::test]
async fn an_unfinished_row_is_refused() {
    for status in [
        OutboxStatus::Pending,
        OutboxStatus::Dispatching,
        OutboxStatus::Failed,
    ] {
        let world = World::new();
        let mut worker = world.worker("worker-a");
        let claim = world.claim_one(&mut worker, RUN, EFFECT).await;
        let row = move_row_to(&world.workflow_store, &world.clock, &run(), EFFECT, status).await;
        let before = world.fleet_revision().await;

        let error = worker
            .fleet_mut()
            .settle_claim_from_row(&claim, Some(&row))
            .await
            .expect_err("a row that is not finished settles nothing");

        assert_eq!(error.code(), "invalid-dispatch-entry", "{status:?}");
        assert_eq!(world.fleet_revision().await, before, "{status:?}");
        let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
            .await
            .expect("the entry is in the record");
        assert_eq!(entry.status, AgentDispatchStatus::Claimed, "{status:?}");
    }
}

#[tokio::test]
async fn a_lapsed_claim_is_fenced_and_writes_nothing() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, RUN, EFFECT).await;
    let row = move_row_to(
        &world.workflow_store,
        &world.clock,
        &run(),
        EFFECT,
        OutboxStatus::Dispatched,
    )
    .await;
    world.clock.advance_millis(LEASE_MS + 1);
    let before = world.fleet_revision().await;

    let error = worker
        .fleet_mut()
        .settle_claim_from_row(&claim, Some(&row))
        .await
        .expect_err("a lapsed claim settles nothing");

    assert_eq!(error.code(), "claim-fenced");
    assert_eq!(world.fleet_revision().await, before);
}

// ---------------------------------------------------------------------------
// The workflow-tier worker never dispatches a row that is finished or gone.
// ---------------------------------------------------------------------------

/// How many dispatches the fleet counted as skipped under `detail`.
fn skipped(world: &World, detail: &str) -> f64 {
    world
        .metrics
        .snapshot()
        .observations_named(rakka_agent_workflow::METRIC_AGENT_DISPATCHER_FLEET)
        .into_iter()
        .filter(|observation| {
            observation.attribute("operation") == Some("dispatch")
                && observation.attribute("outcome") == Some("skipped")
                && observation.attribute("detail") == Some(detail)
        })
        .map(|observation| observation.value())
        .sum()
}

#[tokio::test]
async fn a_claim_on_a_row_already_dispatched_dispatches_nothing() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim_a = world.claim_one(&mut worker_a, RUN, EFFECT).await;
    // A dispatches and records the success at the outbox, and then loses
    // its fleet write for good: the row says done and the entry does not.
    world.fleet_store.contend(4);
    let mut first = RecordingDispatcher::succeeding();
    let error = worker_a
        .dispatch_claim(claim_a, &mut first)
        .await
        .expect_err("the fleet write is lost past the bound");
    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(first.seen.len(), 1);
    assert_eq!(
        outbox_status(&world.workflow_store, &world.clock, &run(), EFFECT).await,
        Some(OutboxStatus::Dispatched)
    );
    // The lease lapses and another worker takes the entry.
    world.clock.advance_millis(LEASE_MS + 1);
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("B re-claims");
    assert_eq!(batch.claims.len(), 1);

    let mut second = RecordingDispatcher::succeeding();
    let error = worker_b
        .dispatch_claim(batch.claims[0].clone(), &mut second)
        .await
        .expect_err("a finished row is not dispatched");

    assert_eq!(error.code(), "claim-unavailable");
    assert!(second.seen.is_empty(), "the effect ran once");
    assert_eq!(
        outbox_status(&world.workflow_store, &world.clock, &run(), EFFECT).await,
        Some(OutboxStatus::Dispatched),
        "the row is as it was"
    );
    let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(entry.status, AgentDispatchStatus::Completed);
    assert_eq!(skipped(&world, "ticket-row-settled"), 1.0);
}

#[tokio::test]
async fn a_claim_on_an_absent_row_settles_the_entry() {
    use rakka_persistence::DurableStateStore;

    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, RUN, EFFECT).await;
    // The run's outbox is gone: its record is deleted from the store.
    let persistence_id = rakka_agent_workflow::agent_run_workflow_id(&run()).persistence_id();
    let record = world
        .workflow_store
        .load(&persistence_id)
        .await
        .expect("the workflow store loads")
        .expect("the run's record exists");
    world
        .workflow_store
        .delete(&persistence_id, record.revision)
        .await
        .expect("the run's record is deleted");

    let mut dispatcher = RecordingDispatcher::succeeding();
    let error = worker
        .dispatch_claim(claim, &mut dispatcher)
        .await
        .expect_err("an absent row is not dispatched");

    assert_eq!(error.code(), "claim-unavailable");
    assert!(dispatcher.seen.is_empty());
    let entry = fleet_entry(&world.fleet_store, &run(), EFFECT)
        .await
        .expect("the entry is in the record");
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-missing"));
    assert_eq!(skipped(&world, "ticket-row-missing"), 1.0);
}

/// Two runs with one registered effect each, the first one's row already
/// finished behind the fleet's back.
async fn one_finished_row_and_one_due(world: &World, worker: &mut TestWorker) {
    world.register_one(worker, "run-1", EFFECT).await;
    world.register_one(worker, "run-2", EFFECT).await;
    move_row_to(
        &world.workflow_store,
        &world.clock,
        &AgentRunId::new("run-1"),
        EFFECT,
        OutboxStatus::Dispatched,
    )
    .await;
}

#[tokio::test]
async fn a_cycle_goes_on_past_a_claim_that_needed_no_dispatch() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    one_finished_row_and_one_due(&world, &mut worker).await;

    let mut dispatcher = RecordingDispatcher::succeeding();
    let cycle = worker
        .run_once(
            [AgentRunId::new("run-1"), AgentRunId::new("run-2")],
            &mut dispatcher,
        )
        .await
        .expect("a claim that needed no dispatch does not fail the cycle");

    assert_eq!(cycle.claims.claims.len(), 2);
    assert_eq!(cycle.completions.len(), 1);
    assert_eq!(cycle.completions[0].claim.run_id, AgentRunId::new("run-2"));
    assert_eq!(dispatcher.seen.len(), 1);
    for run in ["run-1", "run-2"] {
        let entry = fleet_entry(&world.fleet_store, &AgentRunId::new(run), EFFECT)
            .await
            .expect("the entry is in the record");
        assert_eq!(entry.status, AgentDispatchStatus::Completed, "{run}");
    }
}

#[tokio::test]
async fn a_skipped_dispatch_is_counted() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    one_finished_row_and_one_due(&world, &mut worker).await;

    let mut dispatcher = RecordingDispatcher::succeeding();
    worker
        .run_once(
            [AgentRunId::new("run-1"), AgentRunId::new("run-2")],
            &mut dispatcher,
        )
        .await
        .expect("the cycle runs");

    assert_eq!(skipped(&world, "ticket-row-settled"), 1.0);
    assert_eq!(skipped(&world, "ticket-row-missing"), 0.0);
}
