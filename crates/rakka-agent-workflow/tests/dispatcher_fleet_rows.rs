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
