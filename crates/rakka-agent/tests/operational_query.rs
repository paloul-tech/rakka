//! The authoritative operational snapshot and the session view.
//!
//! Specification: section 17.18; scenarios 21 and 56 of section 18. The
//! snapshot is derived from the durable run record alone and returns the
//! durable state revision it read, so it stays correct — lifecycle, waits,
//! budget, effects, cancellation — when telemetry is sampled, delayed,
//! dropped, or entirely unavailable, and when the entity is passivated
//! (scenario 56). The session view is a projection over it: it joins the
//! decision events a sink retained and the trace segments the durable records
//! carry, and it is explicit about its own freshness rather than degrading
//! the authoritative half (scenario 21).

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    agent_operational_snapshot, assemble_agent_session_view, AgentCancellationProgress,
    AgentCheckpointKind, AgentDecisionEventSink, AgentDecisionKind, AgentEffectPolicies,
    AgentEffectSpec, AgentModelTurn, AgentModelUsage, AgentObservabilityError,
    AgentObservabilityFuture, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunStatus,
    AgentSchemaPolicy, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentDecisionEventSink, InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

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

/// A fixture whose tool is checkpoint-required, so the run parks on an
/// approval before the tool may dispatch.
fn checkpointed_fixture() -> Fixture {
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new("charge-card").expect("tool id"),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("charge-card"))
            .with_turn(proposing_turn("charged")),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

/// A sink that always refuses: what an unavailable telemetry backend looks
/// like to the session view.
#[derive(Debug)]
struct UnavailableSink;

impl AgentDecisionEventSink for UnavailableSink {
    fn backend_name(&self) -> &'static str {
        "unavailable"
    }

    fn append<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _event: &'a rakka_agent::AgentDecisionEvent,
    ) -> AgentObservabilityFuture<'a, rakka_agent::AgentDecisionWriteStatus> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }

    fn read<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _after: u64,
        _limit: usize,
    ) -> AgentObservabilityFuture<'a, Vec<rakka_agent::AgentDecisionEvent>> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }
}

/// With no telemetry wired at all, the snapshot answers everything scenario 56
/// asks — lifecycle, waits, budget, effects, cancellation — from the durable
/// record, with the revision it read.
#[tokio::test]
async fn the_snapshot_answers_from_durable_state_with_telemetry_unavailable() {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");

    assert!(
        snapshot.revision.get() > 0,
        "an authoritative point read returns its durable revision"
    );
    assert_eq!(snapshot.observed_at, AgentTimestampMillis::new(9_999));
    let run = snapshot.run.as_ref().expect("the run accepted");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2);
    assert!(
        run.budget.tokens() > 0,
        "the budget consumption is the durable ledger's"
    );
    assert_eq!(snapshot.wait_reason, None);
    assert_eq!(snapshot.next_wake, None);
    assert!(
        snapshot.pending_effects.is_empty(),
        "a completed run holds no unresolved effect"
    );
    assert!(snapshot.open_checkpoints.is_empty());
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::NotRequested
    );
    assert_eq!(
        snapshot.decision_cursor, 0,
        "an unwired run sequenced no decision events, and the snapshot says so"
    );

    // Deriving again from the same revision yields the same answer: the
    // snapshot is pure over the durable record.
    let again = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(again, snapshot);
}

/// A run parked behind an approval checkpoint reports the wait, the gated
/// effect with its safety class and grant state, and the checkpoint's bounded
/// resolver requirements.
#[tokio::test]
async fn a_parked_run_reports_its_wait_effects_and_checkpoint() {
    let fx = checkpointed_fixture();
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the run parks on its checkpoint");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");

    let run = snapshot.run.as_ref().expect("the run accepted");
    assert_eq!(run.status, AgentRunStatus::WaitingForApproval);
    assert_eq!(
        snapshot.wait_reason.as_deref(),
        Some("waiting-for-approval"),
        "the wait reason is the bounded status label"
    );

    let gated = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.checkpoint_required)
        .expect("the gated tool effect is pending");
    assert_eq!(gated.status, AgentRunEffectStatus::Pending);
    assert_eq!(
        gated.attempts, 0,
        "a gated effect has never been dispatched"
    );
    assert!(!gated.granted, "no grant exists before the decision");

    assert_eq!(snapshot.open_checkpoints.len(), 1);
    assert_eq!(
        snapshot.open_checkpoints[0].kind,
        AgentCheckpointKind::Approval
    );
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::NotRequested
    );
}

/// Cancellation progress follows section 8.7: an ambiguous consequential
/// effect holds `WaitingForReconciliation` — terminal cancellation is never
/// inferred from acceptance of the request — and only the durably recorded
/// terminal state reports `Completed`.
#[tokio::test]
async fn cancellation_progress_follows_the_durable_record() {
    // Stage scenario 57's shape: a non-idempotent tool attempt reported
    // ambiguous, then a cancellation request.
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, tool_calling_turn("charge-card")),
    );
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;

    let now = fx.now();
    let mut run = fx.run();
    run.recover(now).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("the run settles");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");

    let (effect_id, generation, operation_id) = {
        let state = run.state().expect("state reads");
        let loop_state = state.loop_state().expect("the loop exists");
        let effect = loop_state
            .effects()
            .iter()
            .find(|effect| effect.request.tool_call().is_some())
            .expect("the tool effect exists")
            .clone();
        let operation_id = effect
            .result_operation_id(&run_scope())
            .expect("the operation id derives");
        (effect.effect_id, effect.generation, operation_id)
    };
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id,
            effect_id,
            generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::Indeterminate {
                code: "connection-lost".to_string(),
                message: "the worker died mid-invocation".to_string(),
            }),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: cancel_operation_id("cancel-1"),
            reason: "operator-requested".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the cancellation request is accepted");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::WaitingForReconciliation,
        "acceptance of the request is not terminal cancellation"
    );
    let ambiguous = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.status == AgentRunEffectStatus::Indeterminate)
        .expect("the ambiguous effect is reported as indeterminate work");
    assert_eq!(ambiguous.attempts, 1);
}

/// The session view joins the snapshot with the sink's decision events and is
/// explicit about freshness; a failing sink degrades only the projection half,
/// never the authoritative one.
#[tokio::test]
async fn the_session_view_joins_decisions_and_reports_its_own_lag() {
    let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher).with_decision_events(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let view = assemble_agent_session_view(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        Some(sink.as_ref()),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");

    assert!(view.decisions_available);
    assert_eq!(
        view.decision_lag, 0,
        "every sequenced decision is projected"
    );
    let kinds: Vec<AgentDecisionKind> = view.decisions.iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        vec![
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Continue,
            AgentDecisionKind::SubmitResult,
        ],
        "the session's decisions are reconstructable in order (scenario 21)"
    );
    assert_eq!(
        view.snapshot.decision_cursor, 4,
        "the durable cursor backs the lag computation"
    );

    // The same view over an unavailable sink: the authoritative half is
    // untouched, and the projection half says exactly what it is missing.
    let degraded = assemble_agent_session_view(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        Some(&UnavailableSink),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");
    assert!(!degraded.decisions_available);
    assert!(degraded.decisions.is_empty());
    assert_eq!(
        degraded.decision_lag, 4,
        "all four decisions are unprojected"
    );
    assert_eq!(degraded.snapshot, view.snapshot);
}

fn cancel_operation_id(label: &str) -> rakka_agent::AgentOperationId {
    rakka_agent::AgentOperationId::new(rakka_agent::AgentOperationKind::Command, ["acme", label])
        .expect("the operation id derives")
}
