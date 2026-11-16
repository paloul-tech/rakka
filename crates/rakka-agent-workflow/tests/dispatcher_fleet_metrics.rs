//! The fleet record's entry count, by status (issue #145).
//!
//! Nothing showed the record growing while it grew: the fleet reported what
//! was in flight and what was due, never how much it held. The gauge
//! `rakka.agent_workflow.dispatcher.entries` reports the entries by dispatch
//! status, a zero included, wherever the fleet reports its other gauges.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    AgentDispatchStatus, AgentTimestampMillis, METRIC_AGENT_DISPATCHER_ENTRIES,
};

mod fleet_support;

use fleet_support::*;

const STATUSES: [AgentDispatchStatus; 6] = [
    AgentDispatchStatus::Pending,
    AgentDispatchStatus::Claimed,
    AgentDispatchStatus::Completed,
    AgentDispatchStatus::RetryScheduled,
    AgentDispatchStatus::Exhausted,
    AgentDispatchStatus::Cancelled,
];

/// The gauge's latest sample for each status label it has reported.
fn latest_entries(world: &World) -> BTreeMap<String, f64> {
    let snapshot = world.metrics.snapshot();
    let mut latest = BTreeMap::new();
    for observation in snapshot.observations_named(METRIC_AGENT_DISPATCHER_ENTRIES) {
        let attributes = observation.attributes();
        assert_eq!(attributes.len(), 1, "the gauge has one label");
        assert_eq!(attributes[0].key(), "status");
        latest.insert(attributes[0].value().to_string(), observation.value());
    }
    latest
}

/// Two completed entries, one pending and one claimed. The last write is a
/// settle, which is where the fleet reports its gauges.
async fn two_completed_one_pending_one_claimed(world: &World, worker: &mut TestWorker) {
    world.complete_one(worker, "run-done-1", "effect").await;
    world.claim_one(worker, "run-claimed", "effect").await;
    let finishing = world.claim_one(worker, "run-done-2", "effect").await;
    // Registered and never claimed.
    world.register_one(worker, "run-pending", "effect").await;
    worker
        .fleet_mut()
        .complete_claim(&finishing)
        .await
        .expect("the claim completes");
}

#[tokio::test]
async fn the_entries_gauge_reports_every_status_zeros_included() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    two_completed_one_pending_one_claimed(&world, &mut worker).await;

    let entries = latest_entries(&world);

    assert_eq!(entries.len(), STATUSES.len(), "one sample per status");
    for status in STATUSES {
        assert!(
            entries.contains_key(status.as_label()),
            "{} is reported",
            status.as_label()
        );
    }
    assert_eq!(entries["pending"], 1.0);
    assert_eq!(entries["claimed"], 1.0);
    assert_eq!(entries["completed"], 2.0);
    assert_eq!(entries["retry-scheduled"], 0.0);
    assert_eq!(entries["exhausted"], 0.0);
    assert_eq!(entries["cancelled"], 0.0);
}

#[tokio::test]
async fn the_entries_gauge_falls_after_a_prune() {
    let world = World::new();
    let mut worker = world.worker("worker-a");
    two_completed_one_pending_one_claimed(&world, &mut worker).await;
    assert_eq!(latest_entries(&world)["completed"], 2.0);

    worker
        .fleet_mut()
        .prune_terminal(AgentTimestampMillis::new(u64::MAX))
        .await
        .expect("the prune runs");

    let entries = latest_entries(&world);
    assert_eq!(entries["completed"], 0.0);
    assert_eq!(entries["pending"], 1.0);
    assert_eq!(entries["claimed"], 1.0);
}
