//! A dispatch pass split into its per-run and per-worker halves (issue #96,
//! asks 3 and 4).
//!
//! `pump_run` does two kinds of work in one call. Registering the run's due
//! tickets and fencing a winding-down run are scoped to that run; claiming
//! and executing what the fleet has due is not. A node with several class
//! workers that each pumped every run did the per-run half once per worker,
//! and each recovered and re-persisted the shared fleet record per run per
//! tick. [`AgentRunEffectDispatcher::register_run`] and
//! [`AgentRunEffectDispatcher::claim_and_execute`] are the two halves, and
//! `pump_run` is still the two in sequence. Every pass that reads the run
//! reports the status it read, so a caller need not load the run again to
//! learn it has ended.

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentDispatchWindow, AgentEffectSpec, AgentModelTurn, AgentRunEffect, AgentRunStatus,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{AgentDispatchEntry, AgentDispatchStatus};

mod common;

use common::*;

const TOOL: &str = "charge-card";

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

/// A run whose tool ticket is in the outbox, not yet registered in the fleet.
async fn ready_tool_ticket() -> (AuthorityFixture, AgentRunEffect) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    (fx, effect)
}

/// The fleet index entry for one effect's ticket, if it is registered.
async fn fleet_entry(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<AgentDispatchEntry> {
    use rakka_persistence::DurableStateStore;
    let record = fx
        .fleet_store
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")?;
    record
        .state
        .entry(&rakka_agent_workflow::agent_dispatch_id(
            &rakka_agent::workflow_run_id(&run_scope()),
            &effect.dispatch_ticket_id(),
        ))
        .cloned()
}

async fn run_status(fx: &AuthorityFixture) -> AgentRunStatus {
    fx.fx.run_snapshot().await.expect("the run exists").status
}

#[tokio::test]
async fn register_run_registers_the_runs_ticket_and_reports_its_status_without_claiming() {
    let (fx, effect) = ready_tool_ticket().await;
    let status = run_status(&fx).await;

    let pass = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");

    assert_eq!(pass.registered, 1);
    assert_eq!(
        (pass.claimed, pass.invoked),
        (0, 0),
        "registration claims nothing"
    );
    assert_eq!(pass.run_status, Some(status), "the status the pass read");
    assert_eq!(
        fleet_entry(&fx, &effect)
            .await
            .expect("the ticket is registered")
            .status,
        AgentDispatchStatus::Pending
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

#[tokio::test]
async fn claim_and_execute_runs_what_the_fleet_has_registered_and_only_that() {
    let (fx, effect) = ready_tool_ticket().await;

    // Nothing is registered yet: the per-worker half claims from the fleet
    // alone and never reads a run's outbox.
    let unregistered = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the claim runs");
    assert_eq!(unregistered.claimed, 0);
    assert!(fleet_entry(&fx, &effect).await.is_none());

    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the claim runs");

    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (1, 1, 1));
    assert_eq!(pass.registered, 0, "claiming registers nothing");
    assert_eq!(
        pass.run_status, None,
        "a claim pass reads no run of its own"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

#[tokio::test]
async fn pump_run_reports_the_status_of_a_run_that_has_ended() {
    let (fx, _effect) = ready_tool_ticket().await;
    fx.pump().await;
    assert_eq!(run_status(&fx).await, AgentRunStatus::Completed);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(pass.run_status, Some(AgentRunStatus::Completed));
}

#[tokio::test]
async fn re_registering_a_call_in_flight_writes_nothing_to_the_fleet() {
    let (fx, effect) = ready_tool_ticket().await;
    // Kill the worker once its `Started` is durable: the ticket's outbox row
    // stays `Dispatching`, which is due on every registration, and its fleet
    // entry keeps a live lease.
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let died = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(died.died);
    assert_eq!(
        fleet_entry(&fx, &effect)
            .await
            .expect("the ticket is registered")
            .status,
        AgentDispatchStatus::Claimed
    );
    assert_eq!(
        fx.outbox_row(&effect)
            .await
            .expect("the ticket's row exists")
            .status(),
        OutboxStatus::Dispatching,
        "a dispatching row is due, so the registration below observes it"
    );

    fx.fleet_store.reset_writes();
    let pass = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");

    assert_eq!(pass.registered, 0, "an unchanged entry is not counted");
    assert_eq!(fx.fleet_store.writes(), 0, "nor written");
}
