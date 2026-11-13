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

/// The one effect the run holds whose request matches `pick`, re-read from
/// durable state, with the tool results the loop holds beside it.
async fn held_effect(
    fx: &Fixture,
    pick: impl Fn(&AgentRunEffect) -> bool,
) -> (AgentRunEffect, Vec<rakka_agent::AgentToolResult>) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let state = run.state().expect("state reads");
    let loop_state = state.loop_state().expect("the loop exists");
    let effect = loop_state
        .effects()
        .iter()
        .find(|effect| pick(effect))
        .expect("the effect is held")
        .clone();
    (effect, loop_state.tool_results().to_vec())
}

/// A `RecordEffectResult` carrying a tool outcome under `call_id`.
fn tool_result_command(effect: &AgentRunEffect, call_id: AgentToolCallId) -> AgentRunEntityCommand {
    AgentRunEntityCommand::RecordEffectResult {
        operation_id: effect
            .result_operation_id(&run_scope())
            .expect("the result operation id derives"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        attempt: effect.attempts.saturating_add(1),
        fence: 0,
        outcome: Box::new(rakka_agent::AgentRunEffectOutcome::Tool {
            call_id,
            content: AgentTaskContent::inline(serde_json::json!({ "refunded": true }))
                .expect("the content is inline-bounded"),
        }),
    }
}

/// Issue #125: a compensation settles only under the call id derived from
/// its own effect. A tool outcome under any other id, here the compensated
/// call's own, is refused `effect-outcome-mismatch` before anything changes:
/// the compensation stays outstanding and no tool result is recorded. The
/// outcome under `compensation_call_id(effect)` settles it.
#[tokio::test]
async fn a_foreign_outcome_does_not_settle_a_compensation() {
    let policies = rakka_agent::AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new(TOOL).expect("the tool id is valid"),
            AgentEffectSpec::non_idempotent(),
        )
        .expect("the tool spec is valid");
    let fx = Fixture::with_sink(
        rakka_agent::testkit::ScriptedDispatcher::with_adapter(tool_then_proposal()),
        rakka_agent::InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(std::sync::atomic::AtomicU64::new(1)),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Answer only the model call, so the tool effect is dispatched and
    // outstanding, then report its outcome unknown: the run parks.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run settles");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");
    drop(run);
    let (tool, _) = held_effect(&fx, |effect| effect.request.tool_call().is_some()).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: tool
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: tool.effect_id.clone(),
            generation: tool.generation,
            attempt: 1,
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                "connection-lost",
                "the worker died mid-invocation",
            )),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");
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
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the compensation decision applies");
    drop(run);

    let is_compensation = |effect: &AgentRunEffect| {
        effect.kind() == rakka_agent::AgentRunEffectKind::CompensationCall
    };
    let (compensation, results_before) = held_effect(&fx, is_compensation).await;
    assert_eq!(compensation.status, AgentRunEffectStatus::Ready);
    let own_id = rakka_agent::compensation_call_id(&compensation);
    let compensated_call = AgentToolCallId::new("call-1").expect("the call id is valid");
    assert_ne!(own_id, compensated_call);

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let refusal = run
        .apply(
            tool_result_command(&compensation, compensated_call),
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("a result under another call id settles no compensation");
    assert_eq!(refusal.code(), "effect-outcome-mismatch", "{refusal}");
    drop(run);
    let (held, results) = held_effect(&fx, is_compensation).await;
    assert_eq!(held, compensation, "the compensation is unmoved");
    assert_eq!(results, results_before, "no tool result is recorded");

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        tool_result_command(&compensation, own_id.clone()),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the compensation's own result applies");
    drop(run);
    let (held, _) = held_effect(&fx, is_compensation).await;
    assert_eq!(held.status, AgentRunEffectStatus::Succeeded);
}
