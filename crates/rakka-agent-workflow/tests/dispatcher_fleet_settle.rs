//! A settle that loses its write re-reads the fleet record (issue #145).
//!
//! The fleet record is one durable record that every worker writes. A settle
//! used to write against the revision its handle read at claim time, so any
//! write from another handle in between (another worker's claim, a prune)
//! made it lose, after the outbox row was already written. It now reads the
//! record again and re-applies under the claim's own fence: this worker, this
//! fencing token, a lease still active. The fence is in the record, so a
//! re-read can never settle a claim another worker took over.

use std::sync::Arc;

use rakka_agent_workflow::{AgentDispatchClaim, AgentDispatchStatus, AgentRunId};
use rakka_core::InMemoryMetricsRecorder;
use rakka_workflow::ManualWorkflowClock;

mod fleet_support;

use fleet_support::*;

struct World {
    fleet_store: FleetStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
}

impl World {
    fn new() -> Self {
        Self {
            fleet_store: FleetStore::new(),
            workflow_store: WorkflowStore::new(),
            clock: clock(),
            metrics: Arc::new(InMemoryMetricsRecorder::new()),
        }
    }

    fn worker(&self, worker_id: &str) -> TestWorker {
        worker(
            worker_id,
            &self.fleet_store,
            &self.workflow_store,
            &self.clock,
            &self.metrics,
        )
    }

    /// Schedules one effect into `run`, registers it through `worker`, and
    /// claims it.
    async fn claim_one(
        &self,
        worker: &mut TestWorker,
        run: &str,
        effect: &str,
    ) -> AgentDispatchClaim {
        let run_id = AgentRunId::new(run);
        schedule(&self.workflow_store, &self.clock, &run_id, effect).await;
        worker.recover().await.expect("the fleet recovers");
        worker
            .refresh_run(run_id, None)
            .await
            .expect("the run registers");
        let mut batch = worker.claim_due().await.expect("the claim pass runs");
        assert_eq!(batch.claims.len(), 1, "exactly the new effect is claimed");
        batch.claims.remove(0)
    }
}

#[tokio::test]
async fn a_settle_survives_another_workers_claim() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let mut worker_b = world.worker("worker-b");
    let claim_a = world.claim_one(&mut worker_a, "run-a", "effect-a").await;
    // Another handle writes the record after A read it.
    let claim_b = world.claim_one(&mut worker_b, "run-b", "effect-b").await;

    let settled = worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect("the settle re-reads and applies");

    assert_eq!(settled.status, AgentDispatchStatus::Completed);
    let entry_a = fleet_entry(&world.fleet_store, &AgentRunId::new("run-a"), "effect-a")
        .await
        .expect("A's entry is in the record");
    assert_eq!(entry_a.status, AgentDispatchStatus::Completed);
    let entry_b = fleet_entry(&world.fleet_store, &AgentRunId::new("run-b"), "effect-b")
        .await
        .expect("B's entry is in the record");
    assert_eq!(entry_b.status, AgentDispatchStatus::Claimed);
    assert_eq!(
        entry_b.lease.map(|lease| lease.fencing_token),
        Some(claim_b.fencing_token),
        "the re-read kept the other worker's claim as it was"
    );
}

#[tokio::test]
async fn a_settle_whose_claim_was_taken_over_is_fenced_after_the_re_read() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim_a = world.claim_one(&mut worker_a, "run-a", "effect-a").await;

    // Between A's check and A's write, the lease lapses and another worker
    // takes the entry over.
    let mut worker_b = world.worker("worker-b");
    let clock = world.clock.clone();
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            clock.advance_millis(LEASE_MS + 1);
            worker_b.recover().await.expect("the fleet recovers");
            let batch = worker_b.claim_due().await.expect("B re-claims");
            assert_eq!(batch.claims.len(), 1);
            assert_eq!(batch.claims[0].fencing_token, 2);
        })
    }));

    let error = worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect_err("a claim another worker took over is not settled");

    assert_eq!(error.code(), "claim-fenced");
    let entry = fleet_entry(&world.fleet_store, &AgentRunId::new("run-a"), "effect-a")
        .await
        .expect("the entry is in the record");
    assert_eq!(entry.status, AgentDispatchStatus::Claimed);
    assert_eq!(entry.lease.map(|lease| lease.fencing_token), Some(2));
}

#[tokio::test]
async fn a_settle_that_loses_three_times_succeeds_on_the_fourth_write() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim_a = world.claim_one(&mut worker_a, "run-a", "effect-a").await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(3);

    let settled = worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect("three lost writes are inside the bound");

    assert_eq!(settled.status, AgentDispatchStatus::Completed);
    assert_eq!(world.fleet_store.writes() - before, 4);
}

#[tokio::test]
async fn a_settle_that_loses_more_than_the_bound_returns_the_conflict() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim_a = world.claim_one(&mut worker_a, "run-a", "effect-a").await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(4);

    let error = worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect_err("a fourth lost write is past the bound");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(world.fleet_store.writes() - before, 4);
    let entry = fleet_entry(&world.fleet_store, &AgentRunId::new("run-a"), "effect-a")
        .await
        .expect("the entry is in the record");
    assert_eq!(entry.status, AgentDispatchStatus::Claimed);
}

#[tokio::test]
async fn a_failure_other_than_a_lost_write_is_not_retried() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim_a = world.claim_one(&mut worker_a, "run-a", "effect-a").await;
    let before = world.fleet_store.writes();
    world.fleet_store.refuse_next_write();

    let error = worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect_err("a store refusal is returned as it is");

    assert_eq!(error.code(), "store-error");
    assert_eq!(world.fleet_store.writes() - before, 1);
}
