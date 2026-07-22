//! Terminal run recovery reschedules nothing.
//!
//! Specification: section 15 ("A recovered run MUST inspect pending effects
//! ... before advancing"); scenario 19 of section 18. A run that reached a
//! terminal status with its settlement drained is durably done: recovering
//! it — on any owner, however many times — must dispatch no completed
//! effect again and make *no durable write at all*. The proof arms a crash
//! point at the first write permanently, so "writes nothing" is a hard
//! assertion rather than an inference: any attempted write would surface as
//! an injected loss, and the recovery pump must instead return cleanly with
//! the write counter untouched.

use rakka_agent::testkit::{sweep_crash_points, CrashPoint, ScriptedDispatcher};
use rakka_agent::{
    AgentModelTurn, AgentModelUsage, AgentRunStatus, AgentTaskContent, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 3,
        })
}

fn tool_flow_fixture() -> Fixture {
    Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("lookup"))
            .with_turn(proposing_turn("resolved"))
            .with_tool_result(
                "lookup",
                AgentTaskContent::inline(serde_json::json!({ "found": true }))
                    .expect("the tool result is inline-bounded"),
            ),
    )
}

/// Proves the recovery of the fixture's terminal run is writeless and
/// dispatches nothing: the run store is armed to fail the *first* write, and
/// the pump must converge cleanly anyway with the counter at zero and the
/// effect sink unchanged.
async fn assert_writeless_terminal_recovery(fx: &Fixture, context: &str) {
    let effects_before = fx.dispatched_effects();
    let before = fx.run_snapshot().await.expect("the run exists");
    assert!(
        before.status.is_terminal(),
        "{context}: the run must already be terminal"
    );

    // Arm the tripwire: `crash_at` resets the counter, so any write from here
    // on both fails the pump and shows in `writes()`.
    fx.runs.crash_at(1, CrashPoint::BeforeWrite);
    fx.pump()
        .await
        .unwrap_or_else(|error| panic!("{context}: terminal recovery wrote durably: {error}"));
    assert_eq!(
        fx.runs.writes(),
        0,
        "{context}: terminal recovery attempted a durable write"
    );
    fx.runs.survive();

    assert_eq!(
        fx.dispatched_effects(),
        effects_before,
        "{context}: terminal recovery rescheduled a completed effect"
    );
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        (after.status, after.turn),
        (before.status, before.turn),
        "{context}: terminal recovery changed the record"
    );
}

#[tokio::test]
async fn terminal_run_recovery_dispatches_nothing_and_writes_nothing() {
    // Scenario 19, the direct proof: complete a two-turn tool flow — model,
    // tool, model, acceptance, settlement — then recover the terminal run and
    // demand a writeless, dispatchless pass.
    let fx = tool_flow_fixture();
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the flow completes");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2);

    assert_writeless_terminal_recovery(&fx, "clean completion").await;
}

#[tokio::test]
async fn every_crash_converges_to_a_terminal_run_that_recovers_writeless() {
    // Scenario 19 joined to the owner-kill sweep: whatever write the owner
    // died at — including the window where the run committed its terminal
    // transition but still owed its settlement hand-back — the converged run
    // is terminal, settled, and its recovery is writeless. The owed
    // settlement is driven exactly once by the convergence pump; the
    // writeless pass proves nothing else was left behind.
    let reference = tool_flow_fixture();
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(
        writes >= 6,
        "the tool flow should make several durable writes, saw {writes}"
    );
    let reference_effects = reference.dispatched_effects();

    sweep_crash_points(writes, |nth, point| async move {
        let fx = tool_flow_fixture();
        fx.instantiate_agent().await;

        fx.runs.crash_at(nth, point);
        fx.create_task().await;
        let _crashed = fx.pump().await;

        // A new owner activates and finds only what was durably committed.
        fx.runs.survive();
        fx.pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let run = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} should still complete"
        );
        assert_eq!(
            fx.dispatched_effects(),
            reference_effects,
            "crash {point:?} at write {nth} rescheduled a completed effect"
        );

        assert_writeless_terminal_recovery(&fx, &format!("crash {point:?} at write {nth}")).await;
    })
    .await;
}
