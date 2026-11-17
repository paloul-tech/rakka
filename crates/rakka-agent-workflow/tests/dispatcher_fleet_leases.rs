//! A batch's leases: renewed by its worker's settles, and given back when
//! the worker stops short (issue #157).
//!
//! A claim pass leases every claim of its batch at one instant, and a worker
//! runs them one after another. So the last claim's real budget was the lease
//! less everything ahead of it, and a worker that failed part-way left the
//! claims it never started leased until that instant passed.
//!
//! Two things change that. A handle can be told which claims its worker
//! holds and has not started, and every settle it writes then extends their
//! leases in the same write. And a handle can release claims: each one that
//! is still current goes back to `Pending` with no lease, claimable at once,
//! under the fence every other write keeps.

use rakka_agent_workflow::{
    AgentDispatchClaim, AgentDispatchEntry, AgentDispatchJob, AgentDispatchLease,
    AgentDispatchStatus, AgentDispatcherWorkerId, AgentEffectDispatchFuture, AgentEffectDispatcher,
    AgentRunId, AgentTimestampMillis, METRIC_AGENT_DISPATCHER_FLEET,
};
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchResult, OutboxMessageId, WorkflowTelemetryEvent,
    WorkflowTimestamp,
};

mod fleet_support;

use fleet_support::*;

const EFFECT: &str = "effect";

/// Registers one effect for each run and claims them all in one batch, in
/// the order given, which must be the fleet's own: by run id.
async fn claim_batch(
    world: &World,
    worker: &mut TestWorker,
    runs: &[&str],
) -> Vec<AgentDispatchClaim> {
    for run in runs {
        world.register_one(worker, run, EFFECT).await;
    }
    let batch = worker.claim_due().await.expect("the claim pass runs");
    let claimed: Vec<&str> = batch
        .claims
        .iter()
        .map(|claim| claim.run_id.as_str())
        .collect();
    assert_eq!(claimed, runs, "one batch, in run order");
    batch.claims
}

/// One run's fleet entry, as the store holds it now.
async fn entry(world: &World, run: &str) -> AgentDispatchEntry {
    fleet_entry(&world.fleet_store, &AgentRunId::new(run), EFFECT)
        .await
        .expect("the entry is in the record")
}

/// When one run's entry's lease expires, if it holds one.
async fn expiry(world: &World, run: &str) -> Option<u64> {
    entry(world, run)
        .await
        .lease
        .map(|lease| lease.lease_expires_at.as_millis())
}

/// What the fleet counter holds under one operation and outcome.
fn counted(world: &World, operation: &str, outcome: &str) -> f64 {
    world
        .metrics
        .snapshot()
        .observations_named(METRIC_AGENT_DISPATCHER_FLEET)
        .into_iter()
        .filter(|observation| {
            observation.attribute("operation") == Some(operation)
                && observation.attribute("outcome") == Some(outcome)
                && observation.attribute("detail") == Some("none")
        })
        .map(|observation| observation.value())
        .sum()
}

// ---------------------------------------------------------------------------
// A settle renews what its handle holds.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_settle_renews_the_claims_its_handle_holds() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b", "run-c"]).await;
    assert_eq!(expiry(&world, "run-b").await, Some(1_100));
    worker_a
        .fleet_mut()
        .hold_claims(vec![claims[1].clone(), claims[2].clone()]);

    world.clock.advance_millis(600);
    worker_a
        .fleet_mut()
        .complete_claim(&claims[0])
        .await
        .expect("the first claim settles");

    // A full lease from the settle, at 700, on the same claim: the worker,
    // the token and the claim time it had.
    let renewed = AgentDispatchLease {
        worker_id: AgentDispatcherWorkerId::new("worker-a"),
        fencing_token: 1,
        claimed_at: AgentTimestampMillis::new(100),
        lease_expires_at: AgentTimestampMillis::new(1_700),
    };
    for run in ["run-b", "run-c"] {
        let held = entry(&world, run).await;
        assert_eq!(held.status, AgentDispatchStatus::Claimed);
        assert_eq!(held.lease, Some(renewed.clone()), "{run} is renewed");
    }

    // Past the instant the batch was first leased to, the held claims are
    // still this worker's: no other worker takes them, and their own
    // settles are not fenced.
    world.clock.advance_millis(600);
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("the claim pass runs");
    assert!(
        batch.claims.is_empty(),
        "nothing lapsed: {:?}",
        batch.claims
    );
    worker_a
        .fleet_mut()
        .complete_claim(&claims[1])
        .await
        .expect("the second claim settles under its renewed lease");
}

#[tokio::test]
async fn a_settle_renews_nothing_its_handle_was_not_told_to_hold() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b"]).await;

    world.clock.advance_millis(600);
    worker_a
        .fleet_mut()
        .complete_claim(&claims[0])
        .await
        .expect("the first claim settles");

    assert_eq!(
        expiry(&world, "run-b").await,
        Some(1_100),
        "a claim nobody named keeps the lease it was given"
    );
}

#[tokio::test]
async fn holding_replaces_what_was_held() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b", "run-c"]).await;
    worker_a
        .fleet_mut()
        .hold_claims(vec![claims[1].clone(), claims[2].clone()]);
    worker_a.fleet_mut().hold_claims(vec![claims[2].clone()]);

    world.clock.advance_millis(600);
    worker_a
        .fleet_mut()
        .complete_claim(&claims[0])
        .await
        .expect("the first claim settles");

    assert_eq!(expiry(&world, "run-b").await, Some(1_100));
    assert_eq!(expiry(&world, "run-c").await, Some(1_700));
}

#[tokio::test]
async fn a_held_claim_whose_lease_lapsed_is_not_renewed() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    // Two claims of one worker under two lease instants: run-b's to 1100,
    // run-a's to 1600.
    let claim_b = world.claim_one(&mut worker_a, "run-b", EFFECT).await;
    world.clock.advance_millis(500);
    let claim_a = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    worker_a.fleet_mut().hold_claims(vec![claim_b]);

    // At 1200 run-b's lease is over and run-a's is not.
    world.clock.advance_millis(600);
    worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect("the current claim settles");

    assert_eq!(
        expiry(&world, "run-b").await,
        Some(1_100),
        "a lapsed lease is not brought back"
    );
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("the claim pass runs");
    assert_eq!(batch.claims.len(), 1, "the lapsed claim is anyone's");
    assert_eq!(batch.claims[0].fencing_token, 2);
}

#[tokio::test]
async fn a_held_claim_another_worker_took_over_is_not_renewed() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim_b = world.claim_one(&mut worker_a, "run-b", EFFECT).await;
    world.clock.advance_millis(500);
    let claim_a = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    worker_a.fleet_mut().hold_claims(vec![claim_b]);

    // At 1200 another worker takes run-b over, to 2200.
    world.clock.advance_millis(600);
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("the claim pass runs");
    assert_eq!(batch.claims.len(), 1);

    // At 1400 the first worker settles run-a, still holding its old claim
    // on run-b. A renewal from here would read 2400.
    world.clock.advance_millis(200);
    worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect("the current claim settles");

    let taken = entry(&world, "run-b").await;
    assert_eq!(
        taken.lease,
        Some(AgentDispatchLease {
            worker_id: AgentDispatcherWorkerId::new("worker-b"),
            fencing_token: 2,
            claimed_at: AgentTimestampMillis::new(1_200),
            lease_expires_at: AgentTimestampMillis::new(2_200),
        }),
        "the other worker's lease is as it wrote it"
    );
}

#[tokio::test]
async fn a_renewal_never_shortens_a_lease() {
    let world = World::new();
    // One worker id over two handles with two lease lengths: run-b is
    // claimed to 5100 by the long one.
    let mut long = world.worker_with_lease("worker-a", 5_000);
    let claim_b = world.claim_one(&mut long, "run-b", EFFECT).await;
    let mut short = world.worker("worker-a");
    let claim_a = world.claim_one(&mut short, "run-a", EFFECT).await;
    short.fleet_mut().hold_claims(vec![claim_b]);

    // A full lease of the short handle's from 200 would be 1200.
    world.clock.advance_millis(100);
    short
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect("the claim settles");

    assert_eq!(expiry(&world, "run-b").await, Some(5_100));
}

#[tokio::test]
async fn a_fenced_settle_writes_nothing_and_so_renews_nothing() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    // run-a's lease runs to 1100 and run-b's to 1600.
    let claim_a = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    world.clock.advance_millis(500);
    let claim_b = world.claim_one(&mut worker_a, "run-b", EFFECT).await;
    worker_a.fleet_mut().hold_claims(vec![claim_b]);
    let before = world.fleet_store.writes();

    world.clock.advance_millis(600);
    let error = worker_a
        .fleet_mut()
        .complete_claim(&claim_a)
        .await
        .expect_err("a lapsed claim is not settled");

    assert_eq!(error.code(), "claim-fenced");
    assert_eq!(world.fleet_store.writes(), before);
    assert_eq!(expiry(&world, "run-b").await, Some(1_600));
}

#[tokio::test]
async fn a_settle_that_loses_its_write_renews_from_the_record_it_reads_again() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b"]).await;
    worker_a.fleet_mut().hold_claims(vec![claims[1].clone()]);
    world.clock.advance_millis(600);

    // The settle's first write, at 700, loses to another worker's
    // registration, which lands 100 later.
    let fleet_store = world.fleet_store.clone();
    let workflow_store = world.workflow_store.clone();
    let clock = world.clock.clone();
    let metrics = world.metrics.clone();
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            clock.advance_millis(100);
            let run_id = AgentRunId::new("run-other");
            schedule(&workflow_store, &clock, &run_id, EFFECT).await;
            let mut other = worker("worker-b", &fleet_store, &workflow_store, &clock, &metrics);
            other.recover().await.expect("the fleet recovers");
            other
                .refresh_run(run_id, None)
                .await
                .expect("the other run registers");
        })
    }));
    let before = world.fleet_store.writes();

    worker_a
        .fleet_mut()
        .complete_claim(&claims[0])
        .await
        .expect("the settle reads again and applies");

    // The registration's write, the lost settle and the settle that landed.
    assert_eq!(world.fleet_store.writes() - before, 3);
    assert_eq!(
        entry(&world, "run-a").await.status,
        AgentDispatchStatus::Completed
    );
    assert_eq!(
        expiry(&world, "run-b").await,
        Some(1_800),
        "a full lease from the write that landed, at 800"
    );
    assert_eq!(
        entry(&world, "run-other").await.status,
        AgentDispatchStatus::Pending,
        "the other worker's registration is in the record that was written"
    );
}

#[tokio::test]
async fn renewed_entries_are_counted() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b", "run-c"]).await;
    worker_a
        .fleet_mut()
        .hold_claims(vec![claims[1].clone(), claims[2].clone()]);
    assert_eq!(counted(&world, "renew", "renewed"), 0.0);

    world.clock.advance_millis(600);
    worker_a
        .fleet_mut()
        .complete_claim(&claims[0])
        .await
        .expect("the first claim settles");

    assert_eq!(counted(&world, "renew", "renewed"), 2.0);
}

#[tokio::test]
async fn a_settle_at_the_instant_of_the_claim_renews_nothing_and_counts_nothing() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b"]).await;
    worker_a.fleet_mut().hold_claims(vec![claims[1].clone()]);
    let claimed = entry(&world, "run-b").await;

    // No time has passed: a full lease from here is the lease it has.
    worker_a
        .fleet_mut()
        .complete_claim(&claims[0])
        .await
        .expect("the first claim settles");

    assert_eq!(entry(&world, "run-b").await, claimed);
    assert_eq!(counted(&world, "renew", "renewed"), 0.0);
}

// ---------------------------------------------------------------------------
// Releasing claims.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_released_claim_is_pending_and_claimable_at_once() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b"]).await;

    let released = worker_a
        .fleet_mut()
        .release_claims(&claims[1..])
        .await
        .expect("the release runs");

    assert_eq!(released, 1);
    let given_back = entry(&world, "run-b").await;
    assert_eq!(given_back.status, AgentDispatchStatus::Pending);
    assert_eq!(given_back.lease, None);
    assert_eq!(given_back.last_fencing_token, 1);
    assert_eq!(
        entry(&world, "run-a").await.status,
        AgentDispatchStatus::Claimed,
        "a claim that was not named keeps its lease"
    );
    assert_eq!(expiry(&world, "run-a").await, Some(1_100));

    // No clock has moved: another worker takes it under the next token.
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("the claim pass runs");
    assert_eq!(batch.claims.len(), 1);
    assert_eq!(batch.claims[0].run_id.as_str(), "run-b");
    assert_eq!(batch.claims[0].fencing_token, 2);
}

#[tokio::test]
async fn a_write_under_a_released_claim_is_fenced() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    worker_a
        .fleet_mut()
        .release_claims(std::slice::from_ref(&claim))
        .await
        .expect("the release runs");

    let error = worker_a
        .fleet_mut()
        .complete_claim(&claim)
        .await
        .expect_err("a released claim settles nothing");

    assert_eq!(error.code(), "claim-fenced");
    assert_eq!(
        entry(&world, "run-a").await.status,
        AgentDispatchStatus::Pending
    );
}

#[tokio::test]
async fn a_release_changes_the_status_and_the_lease_and_nothing_else() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    // An entry with a history: one failed attempt, a code, a retry time.
    let first = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    worker_a
        .fleet_mut()
        .record_claim_failure(
            &first,
            &WorkflowTelemetryEvent::OutboxDispatchRetried {
                message_id: OutboxMessageId::new(EFFECT),
                attempt: 1,
                next_retry_at: WorkflowTimestamp::from_millis(200),
                message: "tool-timeout".to_string(),
            },
        )
        .await
        .expect("the claim reschedules");
    world.clock.advance_millis(200);
    let mut batch = worker_a.claim_due().await.expect("the claim pass runs");
    let second = batch.claims.remove(0);
    let claimed = entry(&world, "run-a").await;
    assert_eq!(
        (claimed.status, claimed.attempts, claimed.last_fencing_token),
        (AgentDispatchStatus::Claimed, 1, 2)
    );

    world.clock.advance_millis(50);
    worker_a
        .fleet_mut()
        .release_claims(&[second])
        .await
        .expect("the release runs");

    let expected = AgentDispatchEntry {
        status: AgentDispatchStatus::Pending,
        lease: None,
        updated_at: AgentTimestampMillis::new(350),
        ..claimed
    };
    assert_eq!(entry(&world, "run-a").await, expected);
    assert_eq!(expected.due_at, AgentTimestampMillis::new(200));
    assert_eq!(expected.last_error_code.as_deref(), Some("tool-timeout"));
}

#[tokio::test]
async fn a_release_leaves_a_claim_whose_lease_lapsed() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    world.clock.advance_millis(LEASE_MS + 1);
    let before = world.fleet_store.writes();

    let released = worker_a
        .fleet_mut()
        .release_claims(&[claim])
        .await
        .expect("the release runs");

    assert_eq!(released, 0);
    assert_eq!(world.fleet_store.writes(), before, "nothing is written");
    let lapsed = entry(&world, "run-a").await;
    assert_eq!(lapsed.status, AgentDispatchStatus::Claimed);
    assert_eq!(expiry(&world, "run-a").await, Some(1_100));
}

#[tokio::test]
async fn a_release_leaves_a_claim_another_worker_took_over() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    world.clock.advance_millis(LEASE_MS + 1);
    let mut worker_b = world.worker("worker-b");
    worker_b.recover().await.expect("the fleet recovers");
    let batch = worker_b.claim_due().await.expect("the claim pass runs");
    assert_eq!(batch.claims.len(), 1);
    let before = world.fleet_store.writes();

    let released = worker_a
        .fleet_mut()
        .release_claims(&[claim])
        .await
        .expect("the release runs");

    assert_eq!(released, 0);
    assert_eq!(world.fleet_store.writes(), before, "nothing is written");
    let taken = entry(&world, "run-a").await;
    assert_eq!(taken.status, AgentDispatchStatus::Claimed);
    assert_eq!(
        taken
            .lease
            .map(|lease| (lease.worker_id, lease.fencing_token)),
        Some((AgentDispatcherWorkerId::new("worker-b"), 2))
    );
}

#[tokio::test]
async fn a_release_reads_the_record_before_it_writes() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim = world.claim_one(&mut worker_a, "run-a", EFFECT).await;
    // Another handle writes the record after this one last read it.
    let mut worker_b = world.worker("worker-b");
    world.register_one(&mut worker_b, "run-b", EFFECT).await;
    let before = world.fleet_store.writes();

    let released = worker_a
        .fleet_mut()
        .release_claims(&[claim])
        .await
        .expect("the release runs");

    assert_eq!(released, 1);
    assert_eq!(
        world.fleet_store.writes() - before,
        1,
        "one write, not one lost to a stale copy and one more"
    );
    assert_eq!(
        entry(&world, "run-b").await.status,
        AgentDispatchStatus::Pending,
        "the other handle's registration is still in the record"
    );
}

#[tokio::test]
async fn a_release_that_loses_three_times_lands_on_the_fourth_write() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b"]).await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(3);

    let released = worker_a
        .fleet_mut()
        .release_claims(&claims)
        .await
        .expect("three lost writes are inside the bound");

    assert_eq!(released, 2);
    assert_eq!(world.fleet_store.writes() - before, 4);
    for run in ["run-a", "run-b"] {
        assert_eq!(
            entry(&world, run).await.status,
            AgentDispatchStatus::Pending
        );
    }
}

#[tokio::test]
async fn a_release_that_loses_more_than_the_bound_returns_the_conflict() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b"]).await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(4);

    let error = worker_a
        .fleet_mut()
        .release_claims(&claims)
        .await
        .expect_err("a fourth lost write is past the bound");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(world.fleet_store.writes() - before, 4);
    for run in ["run-a", "run-b"] {
        assert_eq!(
            entry(&world, run).await.status,
            AgentDispatchStatus::Claimed
        );
    }
}

#[tokio::test]
async fn released_entries_are_counted() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claims = claim_batch(&world, &mut worker_a, &["run-a", "run-b", "run-c"]).await;

    worker_a
        .fleet_mut()
        .release_claims(&claims[1..])
        .await
        .expect("the release runs");

    assert_eq!(counted(&world, "release", "released"), 2.0);
}

/// What an older binary reads from a record a release wrote: an entry shape
/// it already knows. Nothing here is a field, a status or a value that a
/// registration of a lapsed claim did not already write.
#[tokio::test]
async fn a_released_entry_is_written_in_a_shape_the_record_already_had() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let claim = world.claim_one(&mut worker_a, "run-a", "effect-a").await;
    worker_a
        .fleet_mut()
        .release_claims(&[claim])
        .await
        .expect("the release runs");
    let record = fleet_record(&world.fleet_store)
        .await
        .expect("the record exists");

    let bytes = serde_json::to_string(&record.state).expect("the state encodes");

    assert_eq!(bytes, PINNED_RELEASED_RECORD);
}

/// The bytes of a one-entry record whose one claim was released.
const PINNED_RELEASED_RECORD: &str = r#"{"entries":{"run-a:effect-a":{"dispatch_id":"run-a:effect-a","workflow_id":null,"run_id":"run-a","effect_id":"effect-a","effect_kind":"tool-call","target":{"target_type":"tool","name":"research-tool","address":"tool://research-tool","attributes":{}},"target_class":"tool","due_at":100,"row_scheduled_at":100,"status":"pending","lease":null,"last_fencing_token":1,"attempts":0,"cancellation_requested":false,"last_error_code":null,"created_at":100,"updated_at":100,"completed_at":null,"exhausted_at":null,"attributes":{}}},"updated_at":100}"#;

// ---------------------------------------------------------------------------
// The workflow-tier worker's cycle.
// ---------------------------------------------------------------------------

/// Schedules one effect into each run's outbox, for a cycle to register.
async fn schedule_runs(world: &World, runs: &[&str]) -> Vec<AgentRunId> {
    let mut run_ids = Vec::new();
    for run in runs {
        let run_id = AgentRunId::new(*run);
        schedule(&world.workflow_store, &world.clock, &run_id, EFFECT).await;
        run_ids.push(run_id);
    }
    run_ids
}

/// A dispatcher that succeeds, and arms the fleet store to refuse its next
/// write during its first dispatch: the settle of the claim it is running.
struct FailsItsFirstSettle {
    fleet_store: FleetStore,
    seen: Vec<AgentRunId>,
}

impl AgentEffectDispatcher for FailsItsFirstSettle {
    fn dispatch<'a>(&'a mut self, job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a> {
        if self.seen.is_empty() {
            self.fleet_store.refuse_next_write();
        }
        self.seen.push(job.claim.run_id.clone());
        Box::pin(async move { OutboxDispatchResult::Success })
    }
}

#[tokio::test]
async fn a_cycle_that_fails_releases_the_claims_it_did_not_start() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let run_ids = schedule_runs(&world, &["run-a", "run-b", "run-c"]).await;
    let mut dispatcher = FailsItsFirstSettle {
        fleet_store: world.fleet_store.clone(),
        seen: Vec::new(),
    };

    let error = worker_a
        .run_once(run_ids, &mut dispatcher)
        .await
        .expect_err("the first claim's settle is refused by the store");

    assert_eq!(error.code(), "store-error");
    assert_eq!(dispatcher.seen, [AgentRunId::new("run-a")]);
    let failed = entry(&world, "run-a").await;
    assert_eq!(
        (failed.status, expiry(&world, "run-a").await),
        (AgentDispatchStatus::Claimed, Some(1_100)),
        "the claim that failed keeps its lease"
    );
    for run in ["run-b", "run-c"] {
        let given_back = entry(&world, run).await;
        assert_eq!(given_back.status, AgentDispatchStatus::Pending, "{run}");
        assert_eq!(given_back.lease, None, "{run}");
    }
}

/// A dispatcher that succeeds, and takes some time over each dispatch.
struct TakesItsTime {
    clock: ManualWorkflowClock,
    millis: u64,
}

impl AgentEffectDispatcher for TakesItsTime {
    fn dispatch<'a>(&'a mut self, _job: &'a AgentDispatchJob) -> AgentEffectDispatchFuture<'a> {
        self.clock.advance_millis(self.millis);
        Box::pin(async move { OutboxDispatchResult::Success })
    }
}

#[tokio::test]
async fn a_cycle_keeps_its_batch_past_the_instant_it_was_leased_to() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let run_ids = schedule_runs(&world, &["run-a", "run-b", "run-c"]).await;
    // Three dispatches of 600 under a lease of 1000: each fits a lease, and
    // the batch does not.
    let mut dispatcher = TakesItsTime {
        clock: world.clock.clone(),
        millis: 600,
    };

    let cycle = worker_a
        .run_once(run_ids, &mut dispatcher)
        .await
        .expect("each claim settles under a lease its worker renewed");

    assert_eq!(cycle.completions.len(), 3);
    for run in ["run-a", "run-b", "run-c"] {
        assert_eq!(
            entry(&world, run).await.status,
            AgentDispatchStatus::Completed,
            "{run}"
        );
    }
}
