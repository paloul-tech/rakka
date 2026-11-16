//! Pruning the fleet record under `AgentRunEffectDispatcher` (issue #145).
//!
//! A finished fleet entry used to be what stopped a finished effect being
//! registered again. Once a prune can remove it, that has to hold without
//! it: a registration must never carry a list of due effects older than the
//! fleet record it writes against, and a prune from one dispatcher must not
//! cost another one's claim its settle.

use std::sync::Arc;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentEffectSpec, AgentModelTurn, AgentRunEffect, AgentRunStatus, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentDispatchEntry, AgentDispatchStatus, AgentDispatcherFleet, AgentDispatcherFleetSettings,
    AgentDispatcherFleetState, AgentDispatcherPrune, AgentTimestampMillis,
};
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

/// Prunes the fleet record the way an operator command does: through a
/// fleet handle of its own over the fleet store, with no dispatcher.
async fn prune_as_an_operator(
    fleet_store: FleetStore,
    clock: rakka_agent::testkit::SharedAtomicWorkflowClock,
) -> AgentDispatcherPrune {
    AgentDispatcherFleet::with_clock_and_metrics(
        fleet_store,
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
        AgentDispatcherFleetSettings::default(),
        clock,
        Arc::new(rakka_core::NoopMetricsRecorder),
    )
    .prune_terminal(FUTURE)
    .await
    .expect("the prune runs")
}

#[tokio::test]
async fn a_registration_never_inserts_an_effect_finished_before_its_fleet_read() {
    let (fx, effect) = ready_tool_ticket().await;
    // Between this dispatcher's read of the run's outbox and its read of
    // the fleet record, another one registers the ticket, runs it, settles
    // it, and an operator prunes the finished entry.
    let mut other = fx.worker("worker-other", fx.tools.clone(), None);
    let fleet_store = fx.fleet_store.clone();
    let clock = fx.wf_clock.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    fx.fleet_store.before_next_load(Box::new(move || {
        Box::pin(async move {
            let pass = other
                .pump_run(&run_scope())
                .await
                .expect("the other dispatcher's pass runs");
            let prune = prune_as_an_operator(fleet_store, clock).await;
            sender
                .send((pass.invoked, prune.pruned))
                .expect("the test is waiting");
        })
    }));

    let pass = fx
        .worker("worker-late", fx.tools.clone(), None)
        .register_run(&run_scope())
        .await
        .expect("the registration runs");

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
async fn pumping_a_finished_run_after_a_prune_registers_nothing() {
    let (fx, _effect) = ready_tool_ticket().await;
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
    let held = fleet_state(&fx)
        .await
        .expect("the fleet record exists")
        .entries()
        .len();
    assert!(held >= 1, "the run left finished entries behind");

    let prune = prune_as_an_operator(fx.fleet_store.clone(), fx.wf_clock.clone()).await;
    assert_eq!(
        prune.pruned, held,
        "every entry of a finished run is finished"
    );
    assert_eq!(prune.remaining, 0);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!((pass.registered, pass.claimed, pass.invoked), (0, 0, 0));
    assert!(fleet_state(&fx)
        .await
        .expect("the fleet record exists")
        .entries()
        .is_empty());
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// The dispatcher's own prune, under another dispatcher's claim.
// ---------------------------------------------------------------------------

/// A tool during whose call another dispatcher prunes the fleet record.
struct PrunedMidCall {
    other: tokio::sync::Mutex<Option<Pipeline>>,
    pruned: std::sync::Mutex<Option<AgentDispatcherPrune>>,
}

impl rakka_agent::AgentDispatchToolExecutor for PrunedMidCall {
    fn execute<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a rakka_agent_workflow::AgentEphemeralCredential>,
    ) -> rakka_agent::AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            let mut other = self
                .other
                .lock()
                .await
                .take()
                .expect("the other dispatcher is wired before the call");
            let prune = other
                .prune_fleet(FUTURE)
                .await
                .expect("the other dispatcher's prune runs");
            *self.pruned.lock().expect("the slot is not poisoned") = Some(prune);
            Ok(
                AgentTaskContent::inline(serde_json::json!({ "paid": true }))
                    .expect("the result is inline-bounded"),
            )
        })
    }
}

#[tokio::test]
async fn a_prune_from_another_dispatcher_does_not_cost_a_claim_its_settle() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let tool = Arc::new(PrunedMidCall {
        other: tokio::sync::Mutex::new(None),
        pruned: std::sync::Mutex::new(None),
    });
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    )
    .with_tool_executor(tool.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    *tool.other.lock().await = Some(fx.worker("worker-pruner", fx.tools.clone(), None));

    // The prune lands while this dispatcher's claim is in flight: between
    // its claim, which read the fleet record, and its settle.
    let pass = fx
        .worker("worker-claimant", fx.tools.clone(), None)
        .pump_run(&run_scope())
        .await
        .expect("a prune from another handle does not fail the pass");

    let prune = tool
        .pruned
        .lock()
        .expect("the slot is not poisoned")
        .clone()
        .expect("the prune ran during the call");
    assert_eq!(
        prune.pruned, 1,
        "the first model turn's finished entry was pruned"
    );
    assert_eq!(prune.remaining, 1, "the claim in flight was left alone");
    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (1, 1, 1));
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Completed),
        "the claim settled as what it was, not as a cancelled ticket a lease later"
    );
}

#[tokio::test]
async fn prune_fleet_reports_what_it_removed() {
    let (fx, _effect) = ready_tool_ticket().await;
    fx.pump().await;
    let held = fleet_state(&fx)
        .await
        .expect("the fleet record exists")
        .entries()
        .len();
    assert!(held >= 2);

    let prune = fx
        .pipeline()
        .prune_fleet(FUTURE)
        .await
        .expect("the prune runs");

    assert_eq!(prune.pruned, held);
    assert_eq!(prune.remaining, 0);
    assert!(fleet_state(&fx)
        .await
        .expect("the fleet record exists")
        .entries()
        .is_empty());
}
