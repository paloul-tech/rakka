//! A ticket that is finished while its run was never told (issue #134).
//!
//! Reproductions, as the code stands before the fix. Each test here changes
//! with the fix.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, SharedAtomicWorkflowClock};
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor, AgentEffectSpec,
    AgentModelTurn, AgentRunEffect, AgentRunEffectStatus, AgentRunScope, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{OutboxMessageId, OutboxStatus, WorkflowTelemetryEvent};
use rakka_agent_workflow::{
    AgentDispatchEntry, AgentDispatchStatus, AgentDispatcherFleet, AgentDispatcherFleetSettings,
    AgentDispatcherWorkerId, AgentEphemeralCredential,
};
use rakka_persistence::DurableStateStore;

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

/// A run whose tool ticket is in the outbox, under `spec`, with `tools`.
async fn ready_tool_ticket(
    spec: &AgentEffectSpec,
    tools: Option<Arc<dyn AgentDispatchToolExecutor>>,
) -> (AuthorityFixture, AgentRunEffect) {
    let mut fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    );
    if let Some(tools) = tools {
        fx = fx.with_tool_executor(tools);
    }
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    (fx, effect)
}

/// The fleet index entry for one effect's ticket, if it is registered.
async fn fleet_entry(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<AgentDispatchEntry> {
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

/// A tool that always fails definitively, and counts its calls.
#[derive(Default)]
struct CountingRefusal {
    calls: AtomicUsize,
}

impl AgentDispatchToolExecutor for CountingRefusal {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Err(AgentDispatchError::Definitive {
                code: "tool-refused".to_string(),
                message: "the tool refused the call".to_string(),
            })
        })
    }
}

/// The worker is lost after the fleet write that finishes the entry and
/// before it tells the run. The row is `Exhausted`, the entry is
/// `Exhausted`, and nothing claims a finished entry: the run waits for good.
#[tokio::test]
async fn pinned_a_worker_lost_after_the_fleet_write_leaves_the_run_waiting() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let tool = Arc::new(CountingRefusal::default());
    let (fx, effect) = ready_tool_ticket(&spec, Some(tool.clone())).await;

    fx.fx.runs.crash_at(1, CrashPoint::BeforeWrite);
    fx.pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("the delivery is refused");
    fx.fx.runs.assert_crash_fired(1, CrashPoint::BeforeWrite);
    fx.fx.runs.survive();
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fx.outbox_row(&effect).await.map(|row| row.status()),
        Some(OutboxStatus::Exhausted)
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Exhausted)
    );

    for _round in 0..3 {
        fx.expire_lease();
        fx.settle().await;
        let pass = fx
            .pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the pass runs");
        assert_eq!((pass.claimed, pass.delivered), (0, 0));
    }
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    eprintln!(
        "run status: {:?}",
        fx.fx.run_snapshot().await.expect("the run exists").status
    );
}

/// A fleet handle of its own over the fixture's fleet record.
fn another_fleet_handle(
    fx: &AuthorityFixture,
) -> AgentDispatcherFleet<FleetStore, SharedAtomicWorkflowClock> {
    AgentDispatcherFleet::with_clock_and_metrics(
        fx.fleet_store.clone(),
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
        AgentDispatcherFleetSettings::new(16, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    )
}

/// A live run whose ticket is finished at the fleet and whose row was lost.
/// The run puts a row back, registration leaves a finished entry alone, and
/// nothing ever claims it: the run waits for good.
#[tokio::test]
async fn pinned_a_live_run_whose_finished_ticket_lost_its_row_waits_for_good() {
    let spec = AgentEffectSpec::non_idempotent();
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let mut fleet = another_fleet_handle(&fx);
    let claim = fleet
        .claim_due(AgentDispatcherWorkerId::new("worker-lost"))
        .await
        .expect("the claim pass runs")
        .claims
        .remove(0);
    fleet
        .record_claim_failure(
            &claim,
            &WorkflowTelemetryEvent::OutboxDispatchExhausted {
                message_id: OutboxMessageId::new(claim.effect_id.as_str()),
                attempts: 1,
                message: "tool-timeout".to_string(),
            },
        )
        .await
        .expect("the entry is exhausted");
    let id = workflow_persistence_id(&run_scope());
    let record = fx
        .workflow_store
        .load(&id)
        .await
        .expect("the workflow store loads")
        .expect("the run has a workflow record");
    fx.workflow_store
        .delete(&id, record.revision)
        .await
        .expect("the workflow record is deleted");

    for _round in 0..3 {
        fx.expire_lease();
        fx.settle().await;
        let pass = fx
            .pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the pass runs");
        assert_eq!((pass.claimed, pass.delivered, pass.invoked), (0, 0, 0));
    }
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    assert_eq!(
        fx.outbox_row(&effect).await.map(|row| row.status()),
        Some(OutboxStatus::Pending),
        "the run put a row back, and nothing will claim it"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}
