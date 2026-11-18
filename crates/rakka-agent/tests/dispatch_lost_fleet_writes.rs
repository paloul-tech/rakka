//! A pass whose registration or claim loses its write to the fleet record
//! (issues #159 and #160).
//!
//! A pass writes the shared fleet record as a registration, a claim and one
//! settle for each claim. Only the settle read the record again when its
//! compare-and-set lost. A registration or a claim that lost failed the
//! pass, with nothing stranded and the whole pass to do again.
//!
//! Both are now made again in the same pass. The claim is made again by the
//! fleet, from the record alone. The registration is made again here, by
//! the dispatcher, because its lost write is a fence: the list of due
//! tickets it registers must be no older than the record it writes against,
//! so the list is read again too, after the record.

use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter};
use rakka_agent::{
    AgentEffectSpec, AgentModelTurn, AgentRunEffect, AgentTaskContent, AgentToolAuthority,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentDispatchEntry, AgentDispatchStatus, AgentDispatcherFleet, AgentDispatcherFleetSettings,
    AgentDispatcherFleetState, AgentTimestampMillis, METRIC_AGENT_DISPATCHER_FLEET,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

const TOOL: &str = "charge-card";
const FUTURE: AgentTimestampMillis = AgentTimestampMillis::new(u64::MAX);

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_tool_call(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("call-1").expect("the call id is valid"),
                    AgentToolId::new(TOOL).expect("the tool id is valid"),
                    serde_json::json!({ "amount": 42 }),
                )
                .expect("the tool call is bounded"),
            ),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                    .expect("the proposal is inline-bounded"),
            ),
        )
}

/// A run whose tool ticket is in the outbox and not yet registered, in a
/// fleet whose record already exists, with every fleet series recorded.
async fn ready_tool_ticket() -> (
    AuthorityFixture,
    AgentRunEffect,
    Arc<InMemoryMetricsRecorder>,
) {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    )
    .with_metrics(metrics.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    assert!(
        fleet_state(&fx).await.is_some(),
        "the model turn before it left a fleet record to contend"
    );
    (fx, effect, metrics)
}

/// The fleet record as the store holds it, if it holds one.
async fn fleet_state(fx: &AuthorityFixture) -> Option<AgentDispatcherFleetState> {
    fx.fleet_store
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")
        .map(|record| record.state)
}

/// The fleet index entry for one effect's ticket, if it is registered.
async fn fleet_entry(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<AgentDispatchEntry> {
    fleet_state(fx)
        .await?
        .entry(&rakka_agent_workflow::agent_dispatch_id(
            &rakka_agent::workflow_run_id(&run_scope()),
            &effect.dispatch_ticket_id(),
        ))
        .cloned()
}

/// The lost writes the fleet counted under one operation: its three
/// re-applications and its exhaustion.
fn lost(metrics: &InMemoryMetricsRecorder, operation: &str) -> [f64; 4] {
    let counted = |outcome: &str, detail: &str| -> f64 {
        metrics
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
    };
    [
        counted("reapplied", "1"),
        counted("reapplied", "2"),
        counted("reapplied", "3"),
        counted("exhausted", "none"),
    ]
}

#[tokio::test]
async fn a_registration_that_loses_its_write_is_made_again_and_the_pass_goes_on() {
    let (fx, effect, metrics) = ready_tool_ticket().await;
    // The pass's first write to the fleet record is its registration.
    fx.fleet_store.crash_at(1, CrashPoint::ConflictBeforeWrite);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("a pass whose registration lost one write goes on");

    fx.fleet_store
        .assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    assert_eq!(pass.registered, 1);
    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (1, 1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(
        fleet_entry(&fx, &effect)
            .await
            .expect("the ticket is registered")
            .status,
        AgentDispatchStatus::Completed
    );
    assert_eq!(lost(&metrics, "register"), [1.0, 0.0, 0.0, 0.0]);
    assert_eq!(lost(&metrics, "claim"), [0.0; 4]);
}

#[tokio::test]
async fn a_claim_that_loses_its_write_is_made_again_in_the_same_pass() {
    let (fx, _effect, metrics) = ready_tool_ticket().await;
    // The registration is the pass's first fleet write and the claim its
    // second.
    fx.fleet_store.crash_at(2, CrashPoint::ConflictBeforeWrite);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("a pass whose claim lost one write goes on");

    fx.fleet_store
        .assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    assert_eq!(pass.registered, 1);
    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (1, 1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(lost(&metrics, "claim"), [1.0, 0.0, 0.0, 0.0]);
    assert_eq!(lost(&metrics, "register"), [0.0; 4]);
}

#[tokio::test]
async fn a_registration_made_again_reads_the_runs_due_tickets_again() {
    let (fx, effect, _metrics) = ready_tool_ticket().await;
    // This dispatcher's registration loses its write. Before it reads the
    // fleet record again, another dispatcher registers the ticket, runs it
    // and settles it, and an operator prunes the finished entry. The list
    // this dispatcher read still names the ticket, and nothing in the
    // record it reads next does.
    fx.fleet_store.crash_at(1, CrashPoint::ConflictBeforeWrite);
    let mut other = fx.worker("worker-other", fx.tools.clone(), None);
    let pruning = fx.fleet_store.clone();
    let clock = fx.wf_clock.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let second_read = fx.fleet_store.clone();
    fx.fleet_store.before_next_load(Box::new(move || {
        Box::pin(async move {
            // The first read is the one the lost write was made against.
            // What the other dispatcher does comes before the second.
            second_read.before_next_load(Box::new(move || {
                Box::pin(async move {
                    let pass = other
                        .pump_run(&run_scope())
                        .await
                        .expect("the other dispatcher's pass runs");
                    let prune = AgentDispatcherFleet::with_clock_and_metrics(
                        pruning,
                        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
                        AgentDispatcherFleetSettings::default(),
                        clock,
                        Arc::new(rakka_core::NoopMetricsRecorder),
                    )
                    .prune_terminal(FUTURE)
                    .await
                    .expect("the prune runs");
                    sender
                        .send((pass.invoked, prune.pruned))
                        .expect("the test is waiting");
                })
            }));
        })
    }));

    let pass = fx
        .worker("worker-late", fx.tools.clone(), None)
        .register_run(&run_scope())
        .await
        .expect("the registration is made again");

    let (invoked, pruned) = receiver.await.expect("the other dispatcher ran");
    assert_eq!(invoked, 1, "the other dispatcher ran the tool");
    assert_eq!(
        pruned, 2,
        "the first model turn's finished entry and the tool's were pruned"
    );
    assert!(
        fleet_entry(&fx, &effect).await.is_none(),
        "a finished effect is not registered again"
    );
    // What the second read found due is the run's next ticket, which the
    // tool's result scheduled: registered because it is due, and never run.
    let state = fleet_state(&fx).await.expect("the fleet record exists");
    assert_eq!(pass.registered, state.entries().len());
    assert!(state.entries().values().all(|entry| {
        entry.status == AgentDispatchStatus::Pending
            && entry.effect_id != effect.dispatch_ticket_id()
    }));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

#[tokio::test]
async fn a_registration_made_again_counts_what_the_write_that_landed_changed() {
    let (fx, effect, _metrics) = ready_tool_ticket().await;
    // This dispatcher's registration loses its write, and before it reads
    // the fleet record again another dispatcher registers the same ticket.
    // Read again, there is nothing left for this one to change.
    fx.fleet_store.crash_at(1, CrashPoint::ConflictBeforeWrite);
    let mut other = fx.worker("worker-other", fx.tools.clone(), None);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let second_read = fx.fleet_store.clone();
    fx.fleet_store.before_next_load(Box::new(move || {
        Box::pin(async move {
            second_read.before_next_load(Box::new(move || {
                Box::pin(async move {
                    let pass = other
                        .register_run(&run_scope())
                        .await
                        .expect("the other dispatcher registers");
                    sender.send(pass.registered).expect("the test is waiting");
                })
            }));
        })
    }));

    let pass = fx
        .worker("worker-late", fx.tools.clone(), None)
        .register_run(&run_scope())
        .await
        .expect("the registration is made again");

    assert_eq!(
        receiver.await.expect("the other dispatcher ran"),
        1,
        "the other dispatcher registered the ticket"
    );
    assert_eq!(
        pass.registered, 0,
        "the count is not carried from the try that lost"
    );
    assert_eq!(
        fx.fleet_store.writes(),
        2,
        "the lost write and the other dispatcher's: nothing to write after the re-read"
    );
    assert_eq!(
        fleet_entry(&fx, &effect)
            .await
            .expect("the ticket is registered")
            .status,
        AgentDispatchStatus::Pending
    );
}

#[tokio::test]
async fn a_registration_that_loses_every_write_fails_the_pass_at_the_bound() {
    let (fx, effect, metrics) = ready_tool_ticket().await;
    fx.fleet_store.conflict_from(1);

    let error = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("a registration that loses four writes fails its pass");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(fx.fleet_store.writes(), 4, "one try and three more");
    assert!(fleet_entry(&fx, &effect).await.is_none());
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(lost(&metrics, "register"), [1.0, 1.0, 1.0, 1.0]);

    // Nothing is left behind: the next pass runs the ticket.
    fx.fleet_store.survive();
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the next pass runs");
    assert_eq!((pass.registered, pass.invoked), (1, 1));
}

#[tokio::test]
async fn a_claim_that_loses_every_write_fails_the_pass_and_leaves_the_ticket_claimable() {
    let (fx, effect, metrics) = ready_tool_ticket().await;
    // From the pass's second fleet write on: its claim, and every time the
    // claim is made again.
    fx.fleet_store.conflict_from(2);

    let error = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("a claim that loses four writes fails its pass");

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(
        fx.fleet_store.writes(),
        5,
        "the registration, then one claim and three more"
    );
    let entry = fleet_entry(&fx, &effect)
        .await
        .expect("the registration landed");
    assert_eq!(entry.status, AgentDispatchStatus::Pending);
    assert!(entry.lease.is_none(), "no lost claim took a lease");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(lost(&metrics, "claim"), [1.0, 1.0, 1.0, 1.0]);
}
