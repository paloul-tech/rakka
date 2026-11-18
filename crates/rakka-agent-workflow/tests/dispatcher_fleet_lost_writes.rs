//! A write to the fleet record that loses its compare-and-set (issues #159
//! and #160).
//!
//! The record is one record, written whole, and every worker writes it. A
//! settle, a release and a prune that lost a write already read the record
//! again and re-applied. Three things are added here:
//!
//! - **A claim does the same.** Each attempt is made whole from the record
//!   it read: what is due, the limits, the batch and the fencing tokens.
//! - **A registration does too, from its caller.** Its lost write is a
//!   fence: the list of due effects must be no older than the record it is
//!   written against. So the caller reads the record and then the list
//!   again, and the fleet answers what follows the loss.
//! - **Every lost write of these five is counted, and a re-application
//!   waits first.** A short random time, longer each time, so that writers
//!   that lost to the same write do not read again together. Only a
//!   settle's first re-application follows at once: its copy dates from its
//!   claim, so a first loss says the copy was old. The wait is the
//!   handle's own, a thread by default, and never the fleet's clock.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use rakka_agent_workflow::{
    agent_dispatcher_fleet_persistence_id, AgentDispatchConcurrencyLimits, AgentDispatchEntry,
    AgentDispatchStatus, AgentDispatcherError, AgentDispatcherFleetSettings, AgentDispatcherWorker,
    AgentDispatcherWorkerId, AgentFleetWritePause, AgentRunId, AgentTimestampMillis,
    METRIC_AGENT_DISPATCHER_FLEET,
};
use rakka_persistence::{DurableError, Revision};
use rakka_workflow::{OutboxStatus, WorkflowClock};

mod fleet_support;

use fleet_support::*;

const EFFECT: &str = "effect";
const FUTURE: AgentTimestampMillis = AgentTimestampMillis::new(u64::MAX);

/// The five writes that are re-applied, as the counter names them.
const WRITES: [&str; 5] = ["register", "claim", "settle", "release", "prune"];

/// A worker over the world's stores under settings of its own.
fn worker_with(
    world: &World,
    worker_id: &str,
    settings: AgentDispatcherFleetSettings,
) -> TestWorker {
    AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new(worker_id),
        world.fleet_store.clone(),
        world.workflow_store.clone(),
        settings,
        world.clock.clone(),
        world.metrics.clone(),
    )
}

/// One run's fleet entry, as the store holds it now.
async fn entry(world: &World, run: &str) -> AgentDispatchEntry {
    fleet_entry(&world.fleet_store, &AgentRunId::new(run), EFFECT)
        .await
        .expect("the entry is in the record")
}

/// What the fleet counter holds under one operation, outcome and detail.
fn counted(world: &World, operation: &str, outcome: &str, detail: &str) -> f64 {
    world
        .metrics
        .snapshot()
        .observations_named(METRIC_AGENT_DISPATCHER_FLEET)
        .into_iter()
        .filter(|observation| {
            observation.attribute("operation") == Some(operation)
                && observation.attribute("outcome") == Some(outcome)
                && observation.attribute("detail") == Some(detail)
        })
        .map(|observation| observation.value())
        .sum()
}

/// The lost writes counted under one operation: the three re-applications,
/// and the loss that was returned.
fn lost(world: &World, operation: &str) -> [f64; 4] {
    [
        counted(world, operation, "reapplied", "1"),
        counted(world, operation, "reapplied", "2"),
        counted(world, operation, "reapplied", "3"),
        counted(world, operation, "revision-conflict", "none"),
    ]
}

/// A wait that ends at once and keeps the times it was asked for.
fn recorded_waits() -> (AgentFleetWritePause, Arc<Mutex<Vec<Duration>>>) {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let record = asked.clone();
    let pause = AgentFleetWritePause::with(move |wait| {
        record.lock().expect("not poisoned").push(wait);
        std::future::ready(())
    });
    (pause, asked)
}

/// A worker whose fleet handle records its waits and does not wait.
fn worker_recording_waits(world: &World) -> (TestWorker, Arc<Mutex<Vec<Duration>>>) {
    let (pause, asked) = recorded_waits();
    let mut worker = world.worker("worker-a");
    worker.fleet_mut().set_write_pause(pause);
    (worker, asked)
}

/// The milliseconds a recorded wait asked for.
fn millis(asked: &Arc<Mutex<Vec<Duration>>>) -> Vec<u128> {
    asked
        .lock()
        .expect("not poisoned")
        .iter()
        .map(Duration::as_millis)
        .collect()
}

/// Whether the waits asked for are the ones a write's re-applications ask
/// for: 5 to 10 ms before the first, 10 to 20 before the second, 20 to 40
/// before the third. A settle skips the first.
fn are_the_waits_of(operation: &str, reapplications: usize, asked: &[u128]) -> bool {
    let schedule = [5..10, 10..20, 20..40];
    let skipped = usize::from(operation == "settle");
    let expected = &schedule[skipped.min(reapplications)..reapplications];
    asked.len() == expected.len()
        && asked
            .iter()
            .zip(expected)
            .all(|(wait, range)| range.contains(wait))
}

/// Polls a future to its end with no runtime under it: no reactor and no
/// timer. The in-memory stores answer at once, so only a wait can be
/// pending, and it is polled until it is over.
fn drive<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::yield_now();
    }
}

/// A lost compare-and-set, as the fleet's store reports one.
fn conflict() -> AgentDispatcherError {
    AgentDispatcherError::from(DurableError::revision_conflict(
        agent_dispatcher_fleet_persistence_id(),
        Revision::new(1),
        Revision::new(2),
    ))
}

/// How a test arms the fleet's store before the write under test.
#[derive(Clone, Copy)]
enum Arm {
    /// The next `n` compare-and-sets lose.
    Lose(usize),
    /// The next compare-and-set is refused for another reason.
    Refuse,
}

/// Makes one write of the named kind through `worker`, with the store armed
/// just before it. Everything the write needs is set up first, unarmed.
async fn write(
    world: &World,
    worker: &mut TestWorker,
    operation: &str,
    arm: Arm,
) -> Result<(), AgentDispatcherError> {
    let arm_store = || match arm {
        Arm::Lose(times) => world.fleet_store.contend(times),
        Arm::Refuse => world.fleet_store.refuse_next_write(),
    };
    match operation {
        "register" => {
            let run_id = due_in_a_fleet_that_has_a_record(world, worker).await;
            arm_store();
            worker.refresh_run(run_id, None).await.map(|_| ())
        }
        "claim" => {
            world.register_one(worker, "run-a", EFFECT).await;
            arm_store();
            worker.claim_due().await.map(|_| ())
        }
        "settle" => {
            let claim = world.claim_one(worker, "run-a", EFFECT).await;
            arm_store();
            worker.fleet_mut().complete_claim(&claim).await.map(|_| ())
        }
        "release" => {
            let claim = world.claim_one(worker, "run-a", EFFECT).await;
            arm_store();
            worker
                .fleet_mut()
                .release_claims(&[claim])
                .await
                .map(|_| ())
        }
        "prune" => {
            world.complete_one(worker, "run-a", EFFECT).await;
            world.clock.advance_millis(1);
            arm_store();
            worker.fleet_mut().prune_terminal(FUTURE).await.map(|_| ())
        }
        other => panic!("no write is named {other}"),
    }
}

/// Schedules run-a's effect without registering it, in a fleet whose record
/// already exists and which `worker` has read. A registration of run-a is
/// then a write another writer can beat: there is a revision to move.
async fn due_in_a_fleet_that_has_a_record(world: &World, worker: &mut TestWorker) -> AgentRunId {
    world.register_one(worker, "run-0", EFFECT).await;
    let run_id = AgentRunId::new("run-a");
    schedule(&world.workflow_store, &world.clock, &run_id, EFFECT).await;
    run_id
}

// ---------------------------------------------------------------------------
// A claim that loses its write reads again and claims.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_claim_that_loses_its_write_reads_again_and_claims() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    world.register_one(&mut worker_a, "run-a", EFFECT).await;
    world.register_one(&mut worker_a, "run-b", EFFECT).await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(1);

    let batch = worker_a
        .claim_due()
        .await
        .expect("a claim that lost one write claims on the record it read again");

    assert_eq!(batch.claims.len(), 2);
    assert_eq!(world.fleet_store.writes() - before, 2, "one lost, one won");
    for run in ["run-a", "run-b"] {
        let entry = entry(&world, run).await;
        assert_eq!(entry.status, AgentDispatchStatus::Claimed);
        assert_eq!(
            entry.lease.expect("the entry is leased").worker_id,
            AgentDispatcherWorkerId::new("worker-a")
        );
    }
    // The batch that landed is the one that is counted, once.
    assert_eq!(counted(&world, "claim", "claimed", "none"), 2.0);
}

#[tokio::test]
async fn a_re_applied_claim_leaves_what_another_worker_claimed_meanwhile() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    world.register_one(&mut worker_a, "run-a", EFFECT).await;
    world.register_one(&mut worker_a, "run-b", EFFECT).await;
    // Between worker-a's read and its write, worker-b claims run-a.
    let mut worker_b = worker_with(
        &world,
        "worker-b",
        AgentDispatcherFleetSettings::new(1, LEASE_MS),
    );
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            let batch = worker_b.claim_due().await.expect("worker-b claims");
            assert_eq!(batch.claims.len(), 1);
            assert_eq!(batch.claims[0].run_id.as_str(), "run-a");
        })
    }));

    let batch = worker_a.claim_due().await.expect("worker-a claims");

    let claimed: Vec<&str> = batch
        .claims
        .iter()
        .map(|claim| claim.run_id.as_str())
        .collect();
    assert_eq!(claimed, ["run-b"], "run-a is worker-b's");
    let run_a = entry(&world, "run-a").await.lease.expect("run-a is leased");
    assert_eq!(run_a.worker_id, AgentDispatcherWorkerId::new("worker-b"));
    assert_eq!(run_a.fencing_token, 1);
    let run_b = entry(&world, "run-b").await.lease.expect("run-b is leased");
    assert_eq!(run_b.worker_id, AgentDispatcherWorkerId::new("worker-a"));
}

#[tokio::test]
async fn a_re_applied_claim_takes_its_fencing_token_from_the_record_it_read_again() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    world.register_one(&mut worker_a, "run-a", EFFECT).await;
    // Between worker-a's read and its write, worker-b claims run-a under
    // token 1 and gives it back. The entry is pending again and its last
    // token is 1, which worker-a's copy does not know.
    let mut worker_b = world.worker("worker-b");
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            let batch = worker_b.claim_due().await.expect("worker-b claims");
            assert_eq!(batch.claims[0].fencing_token, 1);
            worker_b
                .fleet_mut()
                .release_claims(&batch.claims)
                .await
                .expect("worker-b gives it back");
        })
    }));

    let batch = worker_a.claim_due().await.expect("worker-a claims");

    assert_eq!(batch.claims.len(), 1);
    assert_eq!(batch.claims[0].fencing_token, 2, "the token after the last");
    let lease = entry(&world, "run-a").await.lease.expect("run-a is leased");
    assert_eq!(lease.fencing_token, 2);
    assert_eq!(lease.worker_id, AgentDispatcherWorkerId::new("worker-a"));
}

#[tokio::test]
async fn a_re_applied_claim_counts_the_limit_from_the_record_it_read_again() {
    let world = World::new();
    let one_at_a_time = AgentDispatcherFleetSettings::new(8, LEASE_MS)
        .concurrency_limits(AgentDispatchConcurrencyLimits::new(1));
    let mut worker_a = worker_with(&world, "worker-a", one_at_a_time);
    world.register_one(&mut worker_a, "run-a", EFFECT).await;
    world.register_one(&mut worker_a, "run-b", EFFECT).await;
    // worker-a's copy has nothing in flight, so its first attempt takes
    // run-a. Meanwhile worker-b takes run-a, which is the one slot.
    let mut worker_b = worker_with(
        &world,
        "worker-b",
        AgentDispatcherFleetSettings::new(1, LEASE_MS),
    );
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            worker_b.claim_due().await.expect("worker-b claims");
        })
    }));
    let before = world.fleet_store.writes();

    let batch = worker_a.claim_due().await.expect("the claim pass runs");

    assert!(batch.claims.is_empty(), "the slot is taken");
    assert_eq!(batch.concurrency_limited, 1, "run-b was left at the limit");
    assert_eq!(
        entry(&world, "run-b").await.status,
        AgentDispatchStatus::Pending
    );
    assert_eq!(
        world.fleet_store.writes() - before,
        2,
        "worker-a's lost write and worker-b's: nothing to write after the re-read"
    );
}

#[tokio::test]
async fn a_re_applied_claim_is_leased_from_the_instant_it_landed() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    world.register_one(&mut worker_a, "run-a", EFFECT).await;
    world.fleet_store.contend(1);
    let clock = world.clock.clone();
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            clock.advance_millis(50);
        })
    }));

    let batch = worker_a.claim_due().await.expect("the claim pass runs");

    assert_eq!(batch.claimed_at.as_millis(), 150);
    assert_eq!(batch.claims[0].lease_expires_at.as_millis(), 150 + LEASE_MS);
    let lease = entry(&world, "run-a").await.lease.expect("run-a is leased");
    assert_eq!(lease.claimed_at.as_millis(), 150);
    assert_eq!(lease.lease_expires_at.as_millis(), 150 + LEASE_MS);
}

#[tokio::test]
async fn a_claim_that_loses_more_than_the_bound_returns_the_conflict() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    world.register_one(&mut worker_a, "run-a", EFFECT).await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(4);

    let error = worker_a
        .claim_due()
        .await
        .expect_err("a fourth lost write is past the bound");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(world.fleet_store.writes() - before, 4);
    let entry = entry(&world, "run-a").await;
    assert_eq!(entry.status, AgentDispatchStatus::Pending);
    assert!(entry.lease.is_none(), "no lost claim took a lease");
    assert_eq!(entry.last_fencing_token, 0, "and none took a token");
    assert_eq!(counted(&world, "claim", "claimed", "none"), 0.0);
}

// ---------------------------------------------------------------------------
// A registration that loses its write is re-applied by its caller.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refresh_whose_registration_loses_its_write_registers_on_the_record_it_read_again() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let run_id = due_in_a_fleet_that_has_a_record(&world, &mut worker_a).await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(1);

    let registration = worker_a
        .refresh_run(run_id, None)
        .await
        .expect("a registration that lost one write is made again");

    assert_eq!(registration.registered_effects, 1);
    assert_eq!(world.fleet_store.writes() - before, 2, "one lost, one won");
    assert_eq!(
        entry(&world, "run-a").await.status,
        AgentDispatchStatus::Pending
    );
}

#[tokio::test]
async fn a_refresh_reads_its_due_list_again_before_it_registers_again() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let run_id = AgentRunId::new("run-a");
    schedule(&world.workflow_store, &world.clock, &run_id, EFFECT).await;
    worker_a.recover().await.expect("the fleet recovers");
    // Between worker-a's reads and its write, worker-b registers the effect,
    // runs it and settles it, and the finished entry is pruned. The list
    // worker-a read still names the effect, and nothing in the record does.
    let mut worker_b = world.worker("worker-b");
    let clock = world.clock.clone();
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            worker_b
                .refresh_run(AgentRunId::new("run-a"), None)
                .await
                .expect("worker-b registers");
            let mut batch = worker_b.claim_due().await.expect("worker-b claims");
            worker_b
                .dispatch_claim(
                    batch.claims.remove(0),
                    &mut RecordingDispatcher::succeeding(),
                )
                .await
                .expect("worker-b runs the effect");
            clock.advance_millis(1);
            let prune = worker_b
                .fleet_mut()
                .prune_terminal(FUTURE)
                .await
                .expect("the prune runs");
            assert_eq!(prune.pruned, 1);
        })
    }));

    let registration = worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect("the refresh runs");

    assert_eq!(
        outbox_status(&world.workflow_store, &world.clock, &run_id, EFFECT).await,
        Some(OutboxStatus::Dispatched),
        "the effect ran once and is finished"
    );
    assert!(
        fleet_entry(&world.fleet_store, &run_id, EFFECT)
            .await
            .is_none(),
        "a finished effect is not registered again"
    );
    assert_eq!(registration.observed_due_effects, 0);
    assert_eq!(registration.registered_effects, 0);
}

#[tokio::test]
async fn a_refresh_made_again_reads_the_record_before_it_reads_its_list() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    // worker-a holds a copy of the record, so its first attempt loads
    // nothing from the fleet's store: the next load is the re-read.
    let run_id = due_in_a_fleet_that_has_a_record(&world, &mut worker_a).await;
    world.fleet_store.contend(1);
    // While that re-read is being answered, worker-b registers the effect,
    // runs it and settles it, and the finished entry is pruned. A list read
    // before the record would still name the effect.
    let mut worker_b = world.worker("worker-b");
    let clock = world.clock.clone();
    world.fleet_store.before_next_load(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            worker_b
                .refresh_run(AgentRunId::new("run-a"), None)
                .await
                .expect("worker-b registers");
            let mut batch = worker_b.claim_due().await.expect("worker-b claims");
            let run_a = batch
                .claims
                .iter()
                .position(|claim| claim.run_id.as_str() == "run-a")
                .expect("run-a is claimed");
            worker_b
                .dispatch_claim(
                    batch.claims.remove(run_a),
                    &mut RecordingDispatcher::succeeding(),
                )
                .await
                .expect("worker-b runs the effect");
            worker_b
                .fleet_mut()
                .release_claims(&batch.claims)
                .await
                .expect("worker-b gives the rest back");
            clock.advance_millis(1);
            let prune = worker_b
                .fleet_mut()
                .prune_terminal(FUTURE)
                .await
                .expect("the prune runs");
            assert_eq!(prune.pruned, 1);
        })
    }));

    let registration = worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect("the refresh runs");

    assert_eq!(
        outbox_status(&world.workflow_store, &world.clock, &run_id, EFFECT).await,
        Some(OutboxStatus::Dispatched),
        "the effect ran once and is finished"
    );
    assert!(
        fleet_entry(&world.fleet_store, &run_id, EFFECT)
            .await
            .is_none(),
        "a finished and pruned effect is not put back"
    );
    assert_eq!(registration.registered_effects, 0);
    assert_eq!(lost(&world, "register"), [1.0, 0.0, 0.0, 0.0]);
}

#[tokio::test]
async fn a_refresh_made_again_leaves_a_new_cancellation_to_the_next_refresh() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    // run-a has one effect registered and a second one due, unregistered.
    world.register_one(&mut worker_a, "run-a", "effect-1").await;
    let run_id = AgentRunId::new("run-a");
    schedule(&world.workflow_store, &world.clock, &run_id, "effect-2").await;
    // Between worker-a's reads and its write, the run's cancellation is
    // requested: effect-1 is fenced and not yet finished.
    let mut worker_b = world.worker("worker-b");
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            let request = worker_b
                .fleet_mut()
                .request_run_cancellation(&AgentRunId::new("run-a"))
                .await
                .expect("the request lands");
            assert_eq!(request.awaiting_settlement.len(), 1);
        })
    }));

    worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect("the refresh runs");

    // The re-read showed the fence. This refresh registered again and left
    // the fenced entry as it found it.
    let fenced = fleet_entry(&world.fleet_store, &run_id, "effect-1")
        .await
        .expect("effect-1 is indexed");
    assert!(fenced.cancellation_requested, "the fence is kept");
    assert_eq!(fenced.status, AgentDispatchStatus::Pending);

    // The next refresh finishes it: the row first, then the entry.
    worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect("the next refresh runs");
    assert_eq!(
        outbox_status(&world.workflow_store, &world.clock, &run_id, "effect-1").await,
        Some(OutboxStatus::Cancelled)
    );
    assert_eq!(
        fleet_entry(&world.fleet_store, &run_id, "effect-1")
            .await
            .expect("effect-1 is indexed")
            .status,
        AgentDispatchStatus::Cancelled
    );
}

#[tokio::test]
async fn a_re_applied_registration_counts_what_the_write_that_landed_changed() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let run_id = AgentRunId::new("run-a");
    schedule(&world.workflow_store, &world.clock, &run_id, EFFECT).await;
    worker_a.recover().await.expect("the fleet recovers");
    // Between worker-a's reads and its write, worker-b registers the same
    // effect. Read again, there is nothing left for worker-a to change.
    let mut worker_b = world.worker("worker-b");
    world.fleet_store.before_next_write(Box::new(move || {
        Box::pin(async move {
            worker_b.recover().await.expect("the fleet recovers");
            let registration = worker_b
                .refresh_run(AgentRunId::new("run-a"), None)
                .await
                .expect("worker-b registers");
            assert_eq!(registration.registered_effects, 1);
        })
    }));
    let before = world.fleet_store.writes();

    let registration = worker_a
        .refresh_run(run_id, None)
        .await
        .expect("the refresh runs");

    assert_eq!(registration.observed_due_effects, 1);
    assert_eq!(
        registration.registered_effects, 0,
        "the count is not carried from the try that lost"
    );
    assert_eq!(
        world.fleet_store.writes() - before,
        2,
        "worker-a's lost write and worker-b's: nothing to write after the re-read"
    );
}

#[tokio::test]
async fn a_refresh_whose_registration_loses_more_than_the_bound_returns_the_conflict() {
    let world = World::new();
    let mut worker_a = world.worker("worker-a");
    let run_id = due_in_a_fleet_that_has_a_record(&world, &mut worker_a).await;
    let before = world.fleet_store.writes();
    world.fleet_store.contend(4);

    let error = worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect_err("a fourth lost write is past the bound");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(world.fleet_store.writes() - before, 4);
    assert!(fleet_entry(&world.fleet_store, &run_id, EFFECT)
        .await
        .is_none());
}

// ---------------------------------------------------------------------------
// What follows a lost registration, as the fleet answers its caller.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_lost_registration_below_the_bound_goes_again() {
    let world = World::new();
    let mut worker = world.worker("worker-a");

    for reapplied in 0..3 {
        worker
            .fleet_mut()
            .registration_lost(reapplied, conflict())
            .await
            .expect("a registration re-applied fewer than three times goes again");
    }

    assert_eq!(lost(&world, "register"), [1.0, 1.0, 1.0, 0.0]);
}

#[tokio::test]
async fn a_lost_registration_at_the_bound_returns_its_conflict() {
    let world = World::new();
    let mut worker = world.worker("worker-a");

    let error = worker
        .fleet_mut()
        .registration_lost(3, conflict())
        .await
        .expect_err("three re-applications is the bound");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(lost(&world, "register"), [0.0, 0.0, 0.0, 1.0]);
}

#[tokio::test]
async fn a_registration_that_failed_another_way_is_returned_uncounted() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let refused = AgentDispatcherError::from(DurableError::store("a-store", "the store is down"));
    let code = refused.code().to_string();

    let error = worker
        .fleet_mut()
        .registration_lost(0, refused)
        .await
        .expect_err("only a lost compare-and-set is re-applied");

    assert_eq!(error.code(), code);
    assert_ne!(error.code(), "revision-conflict");
    assert_eq!(lost(&world, "register"), [0.0; 4]);
}

// ---------------------------------------------------------------------------
// Every lost write is counted.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_lost_write_is_counted_by_the_re_application_it_led_to() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        write(&world, &mut worker, operation, Arm::Lose(2))
            .await
            .unwrap_or_else(|error| panic!("{operation} lands on its third try: {error}"));

        assert_eq!(lost(&world, operation), [1.0, 1.0, 0.0, 0.0], "{operation}");
        for other in WRITES.into_iter().filter(|other| *other != operation) {
            assert_eq!(lost(&world, other), [0.0; 4], "{other} lost nothing");
        }
    }
}

#[tokio::test]
async fn every_write_that_ran_out_is_counted_under_the_code_it_returned() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        let error = write(&world, &mut worker, operation, Arm::Lose(4))
            .await
            .expect_err("a fourth lost write is past the bound");

        assert_eq!(error.code(), "revision-conflict", "{operation}");
        assert_eq!(lost(&world, operation), [1.0, 1.0, 1.0, 1.0], "{operation}");
    }
}

#[tokio::test]
async fn a_write_that_lands_the_first_time_counts_no_loss() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        write(&world, &mut worker, operation, Arm::Lose(0))
            .await
            .unwrap_or_else(|error| panic!("{operation} lands: {error}"));

        for any in WRITES {
            assert_eq!(lost(&world, any), [0.0; 4], "{operation}: {any}");
        }
    }
}

#[tokio::test]
async fn a_write_refused_for_another_reason_is_returned_uncounted() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        let error = write(&world, &mut worker, operation, Arm::Refuse)
            .await
            .expect_err("the store refused the write");

        assert_ne!(error.code(), "revision-conflict", "{operation}");
        assert_eq!(lost(&world, operation), [0.0; 4], "{operation}");
    }
}

// ---------------------------------------------------------------------------
// A re-application waits first, and what it waits on is the handle's own.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_settle_is_made_again_at_once_the_first_time() {
    let world = World::new();
    let (mut worker, asked) = worker_recording_waits(&world);

    write(&world, &mut worker, "settle", Arm::Lose(1))
        .await
        .expect("the settle lands on its second try");

    assert_eq!(millis(&asked), Vec::<u128>::new());
    assert_eq!(lost(&world, "settle"), [1.0, 0.0, 0.0, 0.0]);
}

#[tokio::test]
async fn every_other_write_waits_before_its_first_re_application() {
    for operation in WRITES.into_iter().filter(|write| *write != "settle") {
        let world = World::new();
        let (mut worker, asked) = worker_recording_waits(&world);

        write(&world, &mut worker, operation, Arm::Lose(1))
            .await
            .unwrap_or_else(|error| panic!("{operation} lands on its second try: {error}"));

        let asked = millis(&asked);
        assert_eq!(asked.len(), 1, "{operation} asked for {asked:?}");
        assert!(
            (5..10).contains(&asked[0]),
            "{operation} asked for {asked:?}"
        );
    }
}

#[tokio::test]
async fn each_later_re_application_waits_longer() {
    for operation in WRITES {
        for losses in [2, 3] {
            let world = World::new();
            let (mut worker, asked) = worker_recording_waits(&world);

            write(&world, &mut worker, operation, Arm::Lose(losses))
                .await
                .unwrap_or_else(|error| panic!("{operation} lands after {losses}: {error}"));

            let asked = millis(&asked);
            assert!(
                are_the_waits_of(operation, losses, &asked),
                "{operation} lost {losses} and asked for {asked:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_write_that_ran_out_does_not_wait_after_its_last_loss() {
    for operation in WRITES {
        let world = World::new();
        let (mut worker, asked) = worker_recording_waits(&world);

        write(&world, &mut worker, operation, Arm::Lose(4))
            .await
            .expect_err("a fourth lost write is past the bound");

        let asked = millis(&asked);
        assert!(
            are_the_waits_of(operation, 3, &asked),
            "{operation} asked for {asked:?}: three re-applications, and no wait after the last loss"
        );
        let total: u128 = asked.iter().sum();
        assert!(total < 70, "{operation} asked for {total} ms in all");
    }
}

#[tokio::test]
async fn the_wait_is_not_the_same_every_time() {
    let world = World::new();
    let (mut worker, asked) = worker_recording_waits(&world);

    for (reapplied, range) in [(0, 5..10), (1, 10..20), (2, 20..40)] {
        asked.lock().expect("not poisoned").clear();
        for _ in 0..32 {
            worker
                .fleet_mut()
                .registration_lost(reapplied, conflict())
                .await
                .expect("the re-application goes ahead");
        }
        let asked = millis(&asked);
        assert_eq!(asked.len(), 32);
        assert!(asked.iter().all(|wait| range.contains(wait)), "{asked:?}");
        let distinct: BTreeSet<_> = asked.iter().collect();
        assert!(distinct.len() > 1, "32 waits were all {distinct:?}");
    }
}

#[test]
fn the_default_wait_needs_no_runtime_and_lasts_the_time_asked() {
    // No Tokio runtime here at all: a bare loop polls the futures. A settle
    // that loses three writes waits twice, 10 ms and 20 ms at the least.
    drive(async {
        let world = World::new();
        let mut worker = world.worker("worker-a");
        let claim = world.claim_one(&mut worker, "run-a", EFFECT).await;
        world.fleet_store.contend(3);
        let started = std::time::Instant::now();

        worker
            .fleet_mut()
            .complete_claim(&claim)
            .await
            .expect("the settle lands on its fourth try");

        assert!(
            started.elapsed() >= Duration::from_millis(30),
            "the settle waited {:?}",
            started.elapsed()
        );
        assert_eq!(
            entry(&world, "run-a").await.status,
            AgentDispatchStatus::Completed
        );
    });
}

#[test]
fn every_write_can_run_out_with_no_runtime_under_it() {
    for operation in WRITES {
        drive(async {
            let world = World::new();
            let mut worker = world.worker("worker-a");

            let error = write(&world, &mut worker, operation, Arm::Lose(4))
                .await
                .expect_err("a fourth lost write is past the bound");

            assert_eq!(error.code(), "revision-conflict", "{operation}");
        });
    }
}

#[tokio::test]
async fn the_wait_ends_while_the_fleets_clock_stands_still() {
    // The fleet's clock here moves only when a test moves it, like a
    // deployment's logical clock. A wait that watched it would never end.
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, "run-a", EFFECT).await;
    let at = world.clock.now();
    world.fleet_store.contend(3);

    worker
        .fleet_mut()
        .complete_claim(&claim)
        .await
        .expect("the settle lands on its fourth try");

    assert_eq!(world.clock.now(), at, "the fleet's clock did not move");
    assert_eq!(
        entry(&world, "run-a").await.status,
        AgentDispatchStatus::Completed
    );
}

#[tokio::test]
async fn a_wait_that_ends_at_once_turns_the_waiting_off_and_still_counts() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    worker
        .fleet_mut()
        .set_write_pause(AgentFleetWritePause::with(|_| std::future::ready(())));

    let error = write(&world, &mut worker, "prune", Arm::Lose(4))
        .await
        .expect_err("a fourth lost write is past the bound");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(lost(&world, "prune"), [1.0, 1.0, 1.0, 1.0]);
}
