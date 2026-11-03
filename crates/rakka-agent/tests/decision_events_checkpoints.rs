//! Checkpoint opens on the decision stream (issue #119, asks 1 and 2).
//!
//! Specification: sections 12 and 17.7. A run that parks on an approval or
//! authorization checkpoint, or on the reconciliation of an effect whose
//! outcome is unknown, records the park as a decision with the checkpoint's
//! own references, so an operator inbox can be fed from the run's decision
//! stream alone.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    agent_operational_snapshot, AgentCheckpointKind, AgentCheckpointSla, AgentDecisionEvent,
    AgentDecisionEventVocabulary, AgentDecisionKind, AgentDecisionSource, AgentEffectPolicies,
    AgentEffectSpec, AgentModelTurn, AgentRunEffectOutcome, AgentRunEntityCommand, AgentRunStatus,
    AgentSchemaPolicy, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

fn dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::new()
        .with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Let me charge that.")
                .with_tool_call(
                    AgentToolCallRequest::new(
                        AgentToolCallId::new("call-1").expect("call id is valid"),
                        tool_id(),
                        serde_json::json!({ "amount": 42 }),
                    )
                    .expect("the tool call is bounded"),
                ),
        )
        .with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Done.")
                .with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                        .expect("the proposal is inline-bounded"),
                ),
        )
        .with_tool_result(
            TOOL,
            AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                .expect("the tool result is inline-bounded"),
        )
}

fn sla() -> AgentCheckpointSla {
    AgentCheckpointSla {
        due_after_ms: Some(1_000),
        expire_after_ms: Some(5_000),
        escalation_target: Some("on-call".to_string()),
    }
}

fn fixture(
    spec: AgentEffectSpec,
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let sink = CountingDecisionSink::new();
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(tool_id(), spec)
        .expect("the tool spec is valid")
        .with_checkpoint_sla(sla());
    let fx = Fixture::with_sink(
        dispatcher(),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(vocabulary);
    (fx, sink)
}

/// Drives the run to its checkpoint wait.
async fn park(fx: &Fixture, status: AgentRunStatus) {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, status);
}

fn of_kind(events: &[AgentDecisionEvent], kind: AgentDecisionKind) -> Vec<&AgentDecisionEvent> {
    events.iter().filter(|event| event.kind == kind).collect()
}

async fn assert_open_matches_view(
    fx: &Fixture,
    sink: &CountingDecisionSink,
    kind: AgentDecisionKind,
    checkpoint_kind: AgentCheckpointKind,
) {
    let events = sink.events(&run_scope());
    let opens = of_kind(&events, kind);
    assert_eq!(opens.len(), 1, "exactly one open is recorded: {events:?}");
    let open = opens[0];
    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    let view = &snapshot.open_checkpoints[0];
    let checkpoint = open
        .checkpoint
        .as_ref()
        .expect("the open names its checkpoint");
    assert_eq!(open.source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(checkpoint.checkpoint_id, view.checkpoint_id);
    assert_eq!(checkpoint.kind, checkpoint_kind);
    assert!(view.due_at.is_some() && view.expires_at.is_some());
    assert_eq!(checkpoint.due_at, view.due_at);
    assert_eq!(checkpoint.expires_at, view.expires_at);
    assert_eq!(open.effect.as_ref(), Some(&view.bound_effect));
    assert_eq!(open.safety_class, Some(view.bound_effect.safety_class));
    let call_tools = of_kind(&events, AgentDecisionKind::CallTools);
    assert_eq!(
        call_tools.len(),
        1,
        "the model's tool selection precedes it"
    );
    assert_eq!(open.phase, call_tools[0].phase);
    assert!(open.sequence > call_tools[0].sequence);
}

#[tokio::test]
async fn an_approval_checkpoint_open_records_request_approval_with_its_references() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        AgentDecisionEventVocabulary::Version2,
    );
    park(&fx, AgentRunStatus::WaitingForApproval).await;
    assert_open_matches_view(
        &fx,
        &sink,
        AgentDecisionKind::RequestApproval,
        AgentCheckpointKind::Approval,
    )
    .await;

    // A re-driven pump re-enters the park without recording a second open.
    fx.pump().await.expect("the re-driven pump rests");
    assert_eq!(
        of_kind(
            &sink.events(&run_scope()),
            AgentDecisionKind::RequestApproval
        )
        .len(),
        1
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // Under the default vocabulary the same flow stays readable by bc0c224.
    let (fx1, sink1) = fixture(
        AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        AgentDecisionEventVocabulary::Version1,
    );
    park(&fx1, AgentRunStatus::WaitingForApproval).await;
    let events = sink1.events(&run_scope());
    assert!(!of_kind(&events, AgentDecisionKind::RequestApproval).is_empty());
    assert_bc0c224_decodes(&events);
    assert_decision_stream_whole(&sink1, &run_scope());
}

#[tokio::test]
async fn an_authorization_checkpoint_open_records_request_authorization() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent().with_authorization_required(),
        AgentDecisionEventVocabulary::Version2,
    );
    park(&fx, AgentRunStatus::WaitingForAuthorization).await;
    assert_open_matches_view(
        &fx,
        &sink,
        AgentDecisionKind::RequestAuthorization,
        AgentCheckpointKind::SecurityAuthorization,
    )
    .await;
    assert_decision_stream_whole(&sink, &run_scope());
}

/// Answers the model call, then reports the dispatched tool effect ambiguous.
async fn report_unknown(fx: &Fixture) {
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
        let effect = state
            .loop_state()
            .expect("the loop exists")
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
            outcome: Box::new(AgentRunEffectOutcome::indeterminate(
                "mcp-outcome-unknown",
                "the worker died mid-invocation",
            )),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, AgentRunStatus::WaitingForReconciliation);
    // Let the settle pass flush the committed decisions.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run settles");
}

#[tokio::test]
async fn an_unknown_outcome_on_a_live_run_records_request_reconciliation_with_its_code() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent(),
        AgentDecisionEventVocabulary::Version2,
    );
    report_unknown(&fx).await;
    let events = sink.events(&run_scope());
    let opens = of_kind(&events, AgentDecisionKind::RequestReconciliation);
    assert_eq!(opens.len(), 1, "one reconciliation open: {events:?}");
    let open = opens[0];
    assert_eq!(open.reason_code.as_deref(), Some("mcp-outcome-unknown"));
    let checkpoint = open
        .checkpoint
        .as_ref()
        .expect("the open names its checkpoint");
    assert_eq!(
        checkpoint.kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(
        checkpoint.expires_at, None,
        "a reconciliation never expires"
    );
    assert!(checkpoint.due_at.is_some());
    assert!(open.effect.is_some());
    assert_decision_stream_whole(&sink, &run_scope());

    // Under Version1 the flow records no such event and stays contiguous.
    let (fx1, sink1) = fixture(
        AgentEffectSpec::non_idempotent(),
        AgentDecisionEventVocabulary::Version1,
    );
    report_unknown(&fx1).await;
    let events = sink1.events(&run_scope());
    assert!(!events.is_empty());
    assert!(of_kind(&events, AgentDecisionKind::RequestReconciliation).is_empty());
    assert_bc0c224_decodes(&events);
    assert_decision_stream_whole(&sink1, &run_scope());
}
