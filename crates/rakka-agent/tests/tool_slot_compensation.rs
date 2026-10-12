//! A tool slot in a real dispatch pipeline (issue #85, ask 12): the one value
//! handed to both of the dispatcher's slots sends a tool call through its
//! router and an operator-scheduled compensation to its compensation
//! executor, never to the tool route that served the compensated call.
//!
//! A non-idempotent `mcp.` tool whose outcome is unknown parks its run for
//! reconciliation; the operator's `Compensate` decision schedules the
//! compensation, and the next pass dispatches it through the slot.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentCheckpointDecision, AgentCompensationRef, AgentDispatchError, AgentDispatchFuture,
    AgentDispatchToolExecutor, AgentEffectSpec, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentReconciliationDecision, AgentRunEffect, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunScope, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolExecutorRouter, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEphemeralCredential, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "mcp.crm.charge";
const COMPENSATION: &str = "mcp.crm.refund";

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

/// The `mcp.` route: every call's outcome is unknown, and the calls are
/// counted.
struct UnknownOutcome {
    calls: Arc<AtomicUsize>,
}

impl AgentDispatchToolExecutor for UnknownOutcome {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(AgentDispatchError::outcome_unknown(
                "scripted-outcome-unknown",
                "the call was sent and no answer was read",
            ))
        })
    }
}

#[tokio::test]
async fn one_slot_in_both_dispatcher_slots_sends_a_scheduled_compensation_to_its_compensation_executor(
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let fallback = RecordingToolExecutor::new();
    let compensations = RecordingCompensations::default();
    let slot = Arc::new(
        AgentToolExecutorRouter::new(Arc::new(fallback.clone()))
            .with_prefix_route(
                "mcp.",
                Arc::new(UnknownOutcome {
                    calls: calls.clone(),
                }),
            )
            .with_compensations(Arc::new(compensations.clone())),
    );
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::non_idempotent(),
        )),
        None,
    )
    .with_tool_executor(slot.clone())
    .with_compensation_executor(slot);

    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let pass = fx.one_pass().await;
    assert_eq!(pass.parked_indeterminate, 1, "{pass:?}");
    assert_eq!(
        fx.effect_at(1).await.expect("the parked effect").status,
        AgentRunEffectStatus::Indeterminate
    );

    // The operator schedules the compensation through the open checkpoint.
    let now = fx.fx.now();
    let mut run = fx.fx.run();
    run.recover(now).await.expect("the run recovers");
    let checkpoint_id = {
        let state = run.state().expect("state reads");
        let open = state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        assert_eq!(open.len(), 1, "one checkpoint gates the ambiguity");
        open[0].checkpoint_id.clone()
    };
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "compensate",
            )
            .expect("the decision key derives"),
            checkpoint_id,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::Compensate {
                    compensation: AgentCompensationRef::new(COMPENSATION)
                        .expect("the reference is valid"),
                },
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
        &fx.fx.router,
        now,
    )
    .await
    .expect("the compensation decision applies");

    fx.expire_lease();
    fx.pump().await;

    assert_eq!(
        compensations.seen(),
        vec![COMPENSATION],
        "the slot's compensation executor performed the scheduled compensation"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the mcp route served its one call and never the compensation"
    );
    assert!(
        fallback.invocations().is_empty(),
        "nothing reached the router's fallback"
    );
    assert!(
        fx.fx
            .run_snapshot()
            .await
            .expect("the run")
            .status
            .is_terminal(),
        "the run settles once the compensation's outcome arrives: {:?}",
        fx.fx.run_snapshot().await.map(|run| run.status)
    );
}
