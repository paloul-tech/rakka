//! The model adapter, end to end over the durable effect path.
//!
//! Specification: sections 10.1, 10.2, and 10.4; the durable execution rule of
//! 9.5. One scripted model turn must run end to end through the *same* durable
//! effect path a production provider uses — the run persists a model effect and
//! passivates, a dispatcher invokes the adapter and returns the turn as a durable
//! result command, and the run resumes from durable state alone
//! ([specification 10.4](../../../docs/plans/rakka-agent/spec.md): the test
//! adapter must not make tests pass by invoking the loop directly around
//! persistence).
//!
//! [`drive_one_turn`] is the shared body. It is exercised twice: by the
//! deterministic [`DeterministicModelAdapter`], which needs no `rig` feature, and
//! — under the `rig` feature — by the Rig-backed [`RigModelAdapter`] over a
//! scripted stub provider. Both must converge on the same completed run, because
//! the adapter is the only thing that differs; the durable path is identical.

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentLoopPhase, AgentModelAdapter, AgentModelTurn, AgentModelUsage, AgentRunStatus,
    AgentRunTerminalReason, AgentTaskContent, AgentTaskStatus, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

/// The turn the deterministic adapter scripts: it proposes the resolved answer,
/// with the same usage the scripted Rig provider reports.
fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 0,
        })
}

/// The shared body: one scripted turn drives a run to completion through the
/// durable effect path, and completes the public task only through the task
/// entity's decision — never by the run mutating its own state.
async fn drive_one_turn<A: AgentModelAdapter>(dispatcher: ScriptedDispatcher<A>) {
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;

    // The task assigned the run and the run durably accepted, all before a
    // single model call.
    let accepted = fx.run_snapshot().await.expect("the run accepted");
    assert_eq!(accepted.generation.get(), 1);

    fx.pump().await.expect("the loop should run to completion");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    assert_eq!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::ResultAccepted)
    );

    // The turn came back over the durable effect path: one model call, one
    // durable effect, one turn.
    assert_eq!(fx.dispatcher.model_calls(), 1);
    assert_eq!(fx.dispatched_effects(), 1);
    assert_eq!(run.turn, 1);

    // The run's own state records the consequence; the *task* is what made the
    // public task terminal ([specification 9.5]).
    let accepted_result = run.accepted_result.expect("the task accepted a result");
    assert_eq!(
        accepted_result.content.inline_value(),
        Some(&serde_json::json!({ "answer": "resolved" }))
    );

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);

    // The run charged what the turn billed, in its own ledger.
    assert_eq!(run.budget.tokens(), 15);
    assert_eq!(run.budget.model_calls(), 1);
}

#[tokio::test]
async fn the_deterministic_adapter_drives_one_turn_end_to_end() {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(proposing_turn()),
    );
    drive_one_turn(dispatcher.clone()).await;

    // The turn was produced by the adapter, over the effect path, exactly
    // once: a dispatcher that bypassed the adapter, or a broken memo that
    // produced twice, fails here rather than passing silently.
    assert_eq!(dispatcher.adapter().calls(), 1);
}

#[cfg(feature = "rig")]
#[tokio::test]
async fn the_rig_adapter_drives_the_same_turn_against_a_stub_provider() {
    use rakka_agent::rig::{RigModelAdapter, ScriptedCompletionModel};

    let provider = ScriptedCompletionModel::new()
        .returning_text("I have an answer.")
        .returning_result(serde_json::json!({ "answer": "resolved" }))
        .with_usage(10, 5);
    let dispatcher = ScriptedDispatcher::with_adapter(RigModelAdapter::new(provider));
    drive_one_turn(dispatcher).await;
}
