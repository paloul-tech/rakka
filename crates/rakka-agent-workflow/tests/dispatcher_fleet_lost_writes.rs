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
//! - **Every lost write is counted, and the later re-applications wait.**
//!   The first follows at once, because a handle's copy is usually old by
//!   the time it writes. The second and third wait a short random time on
//!   the runtime's timer, never on the fleet's clock.

use std::collections::BTreeSet;
use std::time::Duration;

use rakka_agent_workflow::{
    agent_dispatcher_fleet_persistence_id, AgentDispatchConcurrencyLimits, AgentDispatchEntry,
    AgentDispatchStatus, AgentDispatcherError, AgentDispatcherFleetSettings, AgentDispatcherWorker,
    AgentDispatcherWorkerId, AgentRunId, AgentTimestampMillis, METRIC_AGENT_DISPATCHER_FLEET,
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

/// The lost writes counted under one operation: the three re-applications
/// and the exhaustion.
fn lost(world: &World, operation: &str) -> [f64; 4] {
    [
        counted(world, operation, "reapplied", "1"),
        counted(world, operation, "reapplied", "2"),
        counted(world, operation, "reapplied", "3"),
        counted(world, operation, "exhausted", "none"),
    ]
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
async fn every_write_that_ran_out_is_counted_exhausted() {
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
        let before = world.fleet_store.writes();

        let error = write(&world, &mut worker, operation, Arm::Refuse)
            .await
            .expect_err("the store refused the write");

        assert_ne!(error.code(), "revision-conflict", "{operation}");
        assert_eq!(lost(&world, operation), [0.0; 4], "{operation}");
        // `write` sets its scene up through the same store, so only the
        // refused write is the last one asked.
        assert!(world.fleet_store.writes() > before);
    }
}

// ---------------------------------------------------------------------------
// The later re-applications wait, on the runtime's timer.
// ---------------------------------------------------------------------------

/// How long the runtime's clock moved while `work` ran. On a paused runtime
/// that is exactly what `work` slept.
async fn slept<T>(work: impl std::future::Future<Output = T>) -> (T, Duration) {
    let started = tokio::time::Instant::now();
    let output = work.await;
    (output, started.elapsed())
}

#[tokio::test(start_paused = true)]
async fn the_first_re_application_follows_at_once() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        let (written, waited) = slept(write(&world, &mut worker, operation, Arm::Lose(1))).await;

        written.unwrap_or_else(|error| panic!("{operation} lands on its second try: {error}"));
        assert_eq!(waited, Duration::ZERO, "{operation}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_second_re_application_waits_between_ten_and_twenty_milliseconds() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        let (written, waited) = slept(write(&world, &mut worker, operation, Arm::Lose(2))).await;

        written.unwrap_or_else(|error| panic!("{operation} lands on its third try: {error}"));
        assert!(
            (Duration::from_millis(10)..Duration::from_millis(20)).contains(&waited),
            "{operation} waited {waited:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_third_re_application_waits_between_twenty_and_forty_more() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        let (written, waited) = slept(write(&world, &mut worker, operation, Arm::Lose(3))).await;

        written.unwrap_or_else(|error| panic!("{operation} lands on its fourth try: {error}"));
        assert!(
            (Duration::from_millis(30)..Duration::from_millis(60)).contains(&waited),
            "{operation} waited {waited:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_that_ran_out_waited_under_sixty_milliseconds_and_not_before_returning() {
    for operation in WRITES {
        let world = World::new();
        let mut worker = world.worker("worker-a");

        let (written, waited) = slept(write(&world, &mut worker, operation, Arm::Lose(4))).await;

        written.expect_err("a fourth lost write is past the bound");
        assert!(
            (Duration::from_millis(30)..Duration::from_millis(60)).contains(&waited),
            "{operation} waited {waited:?}: two waits, and none after the last loss"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_wait_is_not_the_same_every_time() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let mut seconds = BTreeSet::new();
    let mut thirds = BTreeSet::new();

    for _ in 0..32 {
        let (answer, waited) = slept(worker.fleet_mut().registration_lost(1, conflict())).await;
        answer.expect("the second re-application goes ahead");
        assert!((Duration::from_millis(10)..Duration::from_millis(20)).contains(&waited));
        seconds.insert(waited);
        let (answer, waited) = slept(worker.fleet_mut().registration_lost(2, conflict())).await;
        answer.expect("the third re-application goes ahead");
        assert!((Duration::from_millis(20)..Duration::from_millis(40)).contains(&waited));
        thirds.insert(waited);
    }

    assert!(seconds.len() > 1, "32 waits were all {seconds:?}");
    assert!(thirds.len() > 1, "32 waits were all {thirds:?}");
}

#[tokio::test(start_paused = true)]
async fn the_wait_ends_while_the_fleets_clock_stands_still() {
    // The fleet's clock here moves only when a test moves it, like a
    // deployment's logical clock. A wait that watched it would never end.
    let world = World::new();
    let mut worker = world.worker("worker-a");
    let claim = world.claim_one(&mut worker, "run-a", EFFECT).await;
    let at = world.clock.now();
    world.fleet_store.contend(3);

    let (settled, waited) = slept(worker.fleet_mut().complete_claim(&claim)).await;

    settled.expect("the settle lands on its fourth try");
    assert!(waited >= Duration::from_millis(30));
    assert_eq!(world.clock.now(), at, "the fleet's clock did not move");
    assert_eq!(
        entry(&world, "run-a").await.status,
        AgentDispatchStatus::Completed
    );
}
