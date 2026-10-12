//! A timer firing that changes nothing writes nothing.
//!
//! `FireCheckpointTimers` and `FireFanInDeadline` are safe to deliver early,
//! late, or twice, so a scheduler may fire them blind. A firing with nothing
//! due used to commit anyway: one compare-and-set and one entry of the run's
//! 64-entry operation log, which then evicted the ids a client re-posts. Now
//! such a firing answers [`AgentRunEntityReply::Unchanged`], writes nothing,
//! and leaves its operation id free, so the same id still acts once something
//! is due.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    AgentCheckpointSla, AgentEffectPolicies, AgentEffectSpec, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRunEntityCommand, AgentRunEntityReply, AgentRunStatus,
    AgentRunTerminalReason, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me charge that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id is valid"),
                tool_id(),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

/// A fixture whose one tool parks the run on an approval that expires
/// `expire_after_ms` after it opens.
fn fixture(expire_after_ms: u64) -> Fixture {
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            tool_id(),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid")
        .with_checkpoint_sla(AgentCheckpointSla {
            due_after_ms: None,
            expire_after_ms: Some(expire_after_ms),
            escalation_target: None,
        });
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn())
            .with_tool_result(
                TOOL,
                AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                    .expect("the tool result is inline-bounded"),
            ),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

fn timer_id(discriminator: &str) -> AgentOperationId {
    AgentOperationId::for_agent(
        AgentOperationKind::CheckpointResolution,
        &agent_scope(),
        discriminator,
    )
    .expect("the operation id derives")
}

fn fire_checkpoint_timers(discriminator: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: timer_id(discriminator),
    }
}

async fn park(fx: &Fixture) {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::WaitingForApproval);
}

#[tokio::test]
async fn a_checkpoint_timer_with_nothing_due_answers_unchanged_and_writes_nothing() {
    let fx = fixture(100);
    park(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    fx.runs.reset_writes();
    let reply = run
        .apply(fire_checkpoint_timers("early"), &fx.router, fx.now())
        .await
        .expect("the early firing answers");

    let AgentRunEntityReply::Unchanged { outcome } = reply else {
        panic!("a firing with nothing due should answer Unchanged, got {reply:?}");
    };
    assert_eq!(outcome.status, AgentRunStatus::WaitingForApproval);
    assert_eq!(
        fx.runs.writes(),
        0,
        "a firing with nothing due writes nothing"
    );
    let mut fresh = fx.run();
    fresh.recover(fx.now()).await.expect("the run recovers");
    assert!(
        fresh
            .state()
            .expect("state reads")
            .applied_operations()
            .outcome(&timer_id("early"))
            .is_none(),
        "the firing's operation id is not recorded"
    );
}

#[tokio::test]
async fn a_fan_in_deadline_with_no_group_answers_unchanged_and_writes_nothing() {
    let fx = fixture(100);
    park(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    fx.runs.reset_writes();
    let reply = run
        .apply(
            AgentRunEntityCommand::FireFanInDeadline {
                operation_id: timer_id("fan-in"),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the deadline firing answers");

    assert!(
        matches!(reply, AgentRunEntityReply::Unchanged { .. }),
        "a deadline with no group should answer Unchanged, got {reply:?}"
    );
    assert_eq!(
        fx.runs.writes(),
        0,
        "a firing with nothing due writes nothing"
    );
}

#[tokio::test]
async fn an_unchanged_firing_leaves_its_id_free_to_act_once_something_is_due() {
    let fx = fixture(100);
    park(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let early = run
        .apply(fire_checkpoint_timers("expiry"), &fx.router, fx.now())
        .await
        .expect("the early firing answers");
    assert!(matches!(early, AgentRunEntityReply::Unchanged { .. }));

    // The same id, once the checkpoint's expiry has come due, acts: an
    // answer that wrote nothing consumed nothing.
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let due = run
        .apply(fire_checkpoint_timers("expiry"), &fx.router, fx.now())
        .await
        .expect("the due firing applies");
    assert!(
        matches!(due, AgentRunEntityReply::Applied { .. }),
        "the same id should act once the expiry is due, got {due:?}"
    );

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert!(matches!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::EffectFailed { ref code, .. }) if code == "checkpoint-expired"
    ));
}

#[tokio::test]
async fn an_unchanged_answer_is_never_given_from_a_stale_cache() {
    // An answer that writes nothing is never fenced by a compare-and-set, so
    // a resident facade must not give it from a cache another writer has
    // moved past: here the facade cached the run before it parked, and the
    // checkpoint its cache has never seen is the one now due.
    let fx = fixture(100);
    fx.instantiate_agent().await;
    fx.create_task().await;
    let mut resident = fx.run();
    resident.recover(fx.now()).await.expect("the run recovers");
    assert!(
        resident
            .state()
            .expect("state reads")
            .loop_state()
            .is_none_or(|loop_state| loop_state.open_checkpoints().is_empty()),
        "the resident facade cached the run before it parked"
    );

    fx.pump().await.expect("the loop parks on the checkpoint");
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let reply = resident
        .apply(fire_checkpoint_timers("stale"), &fx.router, fx.now())
        .await
        .expect("the firing answers");
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "the firing should act on the durable record, got {reply:?}"
    );
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
}
