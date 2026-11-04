//! Checkpoint transitions on the decision stream (issue #119, asks 1 and 2).
//!
//! Specification: sections 12 and 17.7. A run that parks on an approval or
//! authorization checkpoint, or on the reconciliation of an effect whose
//! outcome is unknown, records the park as a decision with the checkpoint's
//! own references, so an operator inbox can be fed from the run's decision
//! stream alone. The same stream says when the checkpoint closes — granted,
//! denied, expired or cancelled — when it escalates, and when a
//! reconciliation decision is applied.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    agent_operational_snapshot, AgentApprovalDecision, AgentCheckpointDecision,
    AgentCheckpointKind, AgentCheckpointSla, AgentCompensationRef, AgentDecisionEvent,
    AgentDecisionEventVocabulary, AgentDecisionKind, AgentDecisionOutcome, AgentDecisionSource,
    AgentEffectGeneration, AgentEffectPolicies, AgentEffectResolution, AgentEffectSpec,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentReconciliationDecision,
    AgentRunEffectOutcome, AgentRunEntityCommand, AgentRunEntityReply, AgentRunError,
    AgentRunStatus, AgentSchemaPolicy, AgentTaskContent, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEffectId, AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "charge-card";
const SHIP: &str = "ship-order";
const COMPENSATION: &str = "refund-charge";

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
        .with_compensation_result(
            COMPENSATION,
            AgentTaskContent::inline(serde_json::json!({ "refunded": true }))
                .expect("the compensation result is inline-bounded"),
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
    assert_bc0c224_round_trips(&events);
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
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink1, &run_scope());
}

// ---------------------------------------------------------------------------
// Closes, escalations and reconciliations (ask 1).
// ---------------------------------------------------------------------------

fn resolver() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "operator".to_string(),
        display_name: None,
    }
}

fn decision_key(discriminator: &str) -> AgentOperationId {
    AgentOperationId::for_agent(
        AgentOperationKind::CheckpointResolution,
        &agent_scope(),
        discriminator,
    )
    .expect("the decision key derives")
}

fn resolve(
    checkpoint_id: &HumanCheckpointId,
    key: &str,
    decision: AgentCheckpointDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: decision_key(key),
        checkpoint_id: checkpoint_id.clone(),
        resolver: resolver(),
        decision: Box::new(decision),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

fn approve() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
        credential_binding: None,
        expires_at: AgentTimestampMillis::new(10_000_000),
        allowed_use_count: 1,
    })
}

fn deny() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Deny {
        reason: "not this card".to_string(),
    })
}

fn reconcile(decision: AgentReconciliationDecision) -> AgentCheckpointDecision {
    AgentCheckpointDecision::Reconciliation(decision)
}

fn confirmed_executed() -> AgentEffectResolution {
    AgentEffectResolution::ConfirmedExecuted {
        outcome: Box::new(AgentRunEffectOutcome::Tool {
            call_id: AgentToolCallId::new("call-1").expect("call id is valid"),
            content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-9" }))
                .expect("the content is inline-bounded"),
        }),
    }
}

fn resolve_effect(
    key: &str,
    effect_id: &AgentEffectId,
    generation: AgentEffectGeneration,
    resolution: AgentEffectResolution,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveIndeterminateEffect {
        operation_id: decision_key(key),
        effect_id: effect_id.clone(),
        generation,
        resolution: Box::new(resolution),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

fn fire_timers(key: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: decision_key(key),
    }
}

fn cancel_run() -> AgentRunEntityCommand {
    AgentRunEntityCommand::Cancel {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Cancellation,
            [TENANT, AGENT, "cancel-1"],
        )
        .expect("the operation id derives"),
        reason: "operator stopped the work".to_string(),
    }
}

/// Applies one command through a freshly recovered entity, as a caller on any
/// node would.
async fn apply(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(command, &fx.router, fx.now()).await
}

/// The checkpoint the run holds of `kind`, read from durable state.
async fn held_checkpoint(fx: &Fixture, kind: AgentCheckpointKind) -> rakka_agent::AgentCheckpoint {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let state = run.state().expect("state reads");
    state
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .iter()
        .find(|checkpoint| checkpoint.kind == kind)
        .expect("the run holds the checkpoint")
        .clone()
}

/// Every event naming `checkpoint_id`, in sequence order.
fn naming<'a>(
    events: &'a [AgentDecisionEvent],
    checkpoint_id: &HumanCheckpointId,
) -> Vec<&'a AgentDecisionEvent> {
    events
        .iter()
        .filter(|event| {
            event
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| &checkpoint.checkpoint_id == checkpoint_id)
        })
        .collect()
}

/// How every test here ends: the command replayed under its own operation id,
/// two more settle passes and a re-pump record nothing more, and the run's
/// stream has no hole and no duplicate. Returns the replay's answer.
async fn replay_and_assert_whole(
    fx: &Fixture,
    sink: &CountingDecisionSink,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let before = sink.events(&run_scope());
    assert!(!before.is_empty(), "the flow recorded its decisions");
    let replay = apply(fx, command).await;
    for _ in 0..2 {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(&fx.router, fx.now())
            .await
            .expect("the run settles");
    }
    fx.pump().await.expect("the re-pump rests");
    assert_eq!(
        sink.events(&run_scope()),
        before,
        "a replay, two settle passes and a re-pump record nothing more"
    );
    assert_decision_stream_whole(sink, &run_scope());
    replay
}

/// Parks a gated call, then applies `decision` to its approval checkpoint and
/// pumps the run to rest.
async fn decide_approval(
    vocabulary: AgentDecisionEventVocabulary,
    decision: AgentCheckpointDecision,
) -> (
    Fixture,
    Arc<CountingDecisionSink>,
    HumanCheckpointId,
    AgentRunEntityCommand,
) {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        vocabulary,
    );
    park(&fx, AgentRunStatus::WaitingForApproval).await;
    let checkpoint_id = held_checkpoint(&fx, AgentCheckpointKind::Approval)
        .await
        .checkpoint_id;
    let command = resolve(&checkpoint_id, "d1", decision);
    let reply = apply(&fx, command.clone())
        .await
        .expect("the decision applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    fx.pump().await.expect("the decided run rests");
    (fx, sink, checkpoint_id, command)
}

#[tokio::test]
async fn a_granted_and_a_denied_checkpoint_each_record_resolve_checkpoint() {
    for (decision, outcome, status) in [
        (
            approve(),
            AgentDecisionOutcome::Granted,
            AgentRunStatus::Completed,
        ),
        (deny(), AgentDecisionOutcome::Denied, AgentRunStatus::Failed),
    ] {
        let (fx, sink, checkpoint_id, command) =
            decide_approval(AgentDecisionEventVocabulary::Version2, decision).await;
        assert_eq!(
            fx.run_snapshot().await.expect("the run exists").status,
            status
        );

        let replay = replay_and_assert_whole(&fx, &sink, command).await;
        assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));

        let events = sink.events(&run_scope());
        let closes = of_kind(&events, AgentDecisionKind::ResolveCheckpoint);
        assert_eq!(closes.len(), 1, "one close for {outcome}: {events:?}");
        let close = closes[0];
        assert_eq!(close.outcome, Some(outcome));
        assert_eq!(close.reason_code.as_deref(), Some(outcome.as_label()));
        assert_eq!(close.source, AgentDecisionSource::Human);
        let named = naming(&events, &checkpoint_id);
        assert_eq!(
            named.iter().map(|event| event.kind).collect::<Vec<_>>(),
            [
                AgentDecisionKind::RequestApproval,
                AgentDecisionKind::ResolveCheckpoint
            ],
            "the close names the parked checkpoint, after its open"
        );
        assert_eq!(close.effect, named[0].effect, "the same gated effect");
        assert_eq!(close.safety_class, named[0].safety_class);
    }

    // Under the default vocabulary the close is not written, and the stream
    // stays readable by bc0c224 and contiguous.
    let (_fx, sink, _checkpoint_id, _command) =
        decide_approval(AgentDecisionEventVocabulary::Version1, approve()).await;
    let events = sink.events(&run_scope());
    assert!(of_kind(&events, AgentDecisionKind::ResolveCheckpoint).is_empty());
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &run_scope());
}

/// The open event's deadlines, which the timer tests fire at.
fn deadlines(
    sink: &CountingDecisionSink,
    checkpoint_id: &HumanCheckpointId,
) -> (AgentTimestampMillis, AgentTimestampMillis) {
    let events = sink.events(&run_scope());
    let open = naming(&events, checkpoint_id)[0]
        .checkpoint
        .clone()
        .expect("the open names its checkpoint");
    (
        open.due_at.expect("the SLA sets a due date"),
        open.expires_at.expect("the SLA sets an expiry"),
    )
}

#[tokio::test]
async fn an_expired_checkpoint_records_resolve_checkpoint_expired() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        AgentDecisionEventVocabulary::Version2,
    );
    park(&fx, AgentRunStatus::WaitingForApproval).await;
    let checkpoint_id = held_checkpoint(&fx, AgentCheckpointKind::Approval)
        .await
        .checkpoint_id;
    let (_due_at, expires_at) = deadlines(&sink, &checkpoint_id);

    fx.clock.store(expires_at.as_millis(), Ordering::SeqCst);
    let command = fire_timers("t1");
    apply(&fx, command.clone()).await.expect("the expiry fires");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Failed
    );
    let replay = replay_and_assert_whole(&fx, &sink, command).await;
    assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));

    let events = sink.events(&run_scope());
    let named = naming(&events, &checkpoint_id);
    assert_eq!(
        named.iter().map(|event| event.kind).collect::<Vec<_>>(),
        [
            AgentDecisionKind::RequestApproval,
            AgentDecisionKind::ResolveCheckpoint
        ],
        "{events:?}"
    );
    let close = named[1];
    assert_eq!(close.outcome, Some(AgentDecisionOutcome::Expired));
    assert_eq!(close.reason_code.as_deref(), Some("expired"));
    assert_eq!(close.source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(close.effect, named[0].effect);
}

#[tokio::test]
async fn an_sla_escalation_then_expiry_records_one_escalation_then_one_expiry() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        AgentDecisionEventVocabulary::Version2,
    );
    park(&fx, AgentRunStatus::WaitingForApproval).await;
    let checkpoint_id = held_checkpoint(&fx, AgentCheckpointKind::Approval)
        .await
        .checkpoint_id;
    let (due_at, expires_at) = deadlines(&sink, &checkpoint_id);

    // At the due date the checkpoint escalates and keeps waiting.
    fx.clock.store(due_at.as_millis(), Ordering::SeqCst);
    apply(&fx, fire_timers("t1"))
        .await
        .expect("the escalation fires");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval
    );
    // A pass between the due date and the expiry finds nothing new.
    fx.clock.store(
        (due_at.as_millis() + expires_at.as_millis()) / 2,
        Ordering::SeqCst,
    );
    apply(&fx, fire_timers("t2"))
        .await
        .expect("the idle pass answers");
    // At the expiry the escalated checkpoint expires.
    fx.clock.store(expires_at.as_millis(), Ordering::SeqCst);
    let expiry = fire_timers("t3");
    apply(&fx, expiry.clone()).await.expect("the expiry fires");
    let replay = replay_and_assert_whole(&fx, &sink, expiry).await;
    assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));

    let events = sink.events(&run_scope());
    let named = naming(&events, &checkpoint_id);
    assert_eq!(
        named.iter().map(|event| event.kind).collect::<Vec<_>>(),
        [
            AgentDecisionKind::RequestApproval,
            AgentDecisionKind::EscalateCheckpoint,
            AgentDecisionKind::ResolveCheckpoint
        ],
        "{events:?}"
    );
    let escalation = named[1];
    assert_eq!(escalation.reason_code.as_deref(), Some("sla-due"));
    assert_eq!(escalation.source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(escalation.outcome, None, "an escalation settles nothing");
    let expiry = named[2];
    assert_eq!(expiry.outcome, Some(AgentDecisionOutcome::Expired));
    assert_eq!(expiry.reason_code.as_deref(), Some("expired"));
}

#[tokio::test]
async fn a_persons_escalate_records_once() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent(),
        AgentDecisionEventVocabulary::Version2,
    );
    report_unknown(&fx).await;
    let checkpoint_id =
        held_checkpoint(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation)
            .await
            .checkpoint_id;

    let first = resolve(
        &checkpoint_id,
        "e1",
        reconcile(AgentReconciliationDecision::Escalate),
    );
    apply(&fx, first.clone())
        .await
        .expect("the first escalation applies");
    // A second Escalate under its own key finds the checkpoint escalated
    // already: nothing visible changes, so nothing is recorded.
    let second = resolve(
        &checkpoint_id,
        "e2",
        reconcile(AgentReconciliationDecision::Escalate),
    );
    apply(&fx, second.clone())
        .await
        .expect("the second escalation applies");
    let replay = replay_and_assert_whole(&fx, &sink, first).await;
    assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));

    let events = sink.events(&run_scope());
    let escalations = of_kind(&events, AgentDecisionKind::EscalateCheckpoint);
    assert_eq!(escalations.len(), 1, "{events:?}");
    let escalation = escalations[0];
    assert_eq!(escalation.reason_code.as_deref(), Some("escalated"));
    assert_eq!(escalation.source, AgentDecisionSource::Human);
    assert_eq!(
        escalation
            .checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.checkpoint_id),
        Some(&checkpoint_id)
    );
    assert!(
        of_kind(&events, AgentDecisionKind::Reconcile).is_empty(),
        "an escalation is not a reconciliation decision"
    );
}

fn charge_and_ship_dispatcher() -> ScriptedDispatcher {
    let call = |id: &str, tool: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("call id is valid"),
            AgentToolId::new(tool).expect("the tool id is valid"),
            serde_json::json!({ "order": 7 }),
        )
        .expect("the tool call is bounded")
    };
    ScriptedDispatcher::new()
        .with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Charge it and ship it.")
                .with_tool_call(call("call-1", TOOL))
                .with_tool_call(call("call-2", SHIP)),
        )
        .with_tool_result(
            TOOL,
            AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                .expect("the tool result is inline-bounded"),
        )
        .with_tool_result(
            SHIP,
            AgentTaskContent::inline(serde_json::json!({ "shipped": true }))
                .expect("the tool result is inline-bounded"),
        )
}

#[tokio::test]
async fn a_run_cancel_closes_approval_checkpoints_and_keeps_reconciliation_ones() {
    let sink = CountingDecisionSink::new();
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            tool_id(),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the gated tool spec is valid")
        .with_tool_spec(
            AgentToolId::new(SHIP).expect("the tool id is valid"),
            AgentEffectSpec::non_idempotent(),
        )
        .expect("the ungated tool spec is valid")
        .with_checkpoint_sla(sla());
    let fx = Fixture::with_sink(
        charge_and_ship_dispatcher(),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);

    // The model call is answered: the gated charge parks behind an approval
    // and the ungated shipment dispatches, then is reported ambiguous.
    fx.instantiate_agent().await;
    fx.create_task().await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run settles");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");
    let shipment = {
        let state = run.state().expect("state reads");
        state
            .loop_state()
            .expect("the loop exists")
            .effects()
            .iter()
            .find(|effect| {
                effect
                    .request
                    .tool_call()
                    .is_some_and(|call| call.tool.as_str() == SHIP)
            })
            .expect("the shipment effect exists")
            .clone()
    };
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: shipment
                .result_operation_id(&run_scope())
                .expect("the operation id derives"),
            effect_id: shipment.effect_id.clone(),
            generation: shipment.generation,
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
    let approval = held_checkpoint(&fx, AgentCheckpointKind::Approval)
        .await
        .checkpoint_id;
    let reconciliation =
        held_checkpoint(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation)
            .await
            .checkpoint_id;

    let command = cancel_run();
    apply(&fx, command.clone())
        .await
        .expect("the cancellation applies");
    // A second cancellation under its own operation id finds the approval
    // already closed, and closes nothing again.
    apply(
        &fx,
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "cancel-2"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work again".to_string(),
        },
    )
    .await
    .expect("the repeated cancellation applies");
    let replay = replay_and_assert_whole(&fx, &sink, command).await;
    assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));

    let events = sink.events(&run_scope());
    let closes = of_kind(&events, AgentDecisionKind::ResolveCheckpoint);
    assert_eq!(closes.len(), 1, "{events:?}");
    let close = closes[0];
    assert_eq!(
        close
            .checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.checkpoint_id),
        Some(&approval)
    );
    assert_eq!(close.outcome, Some(AgentDecisionOutcome::Cancelled));
    assert_eq!(close.reason_code.as_deref(), Some("cancelled"));
    assert_eq!(close.source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(
        naming(&events, &reconciliation)
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        [AgentDecisionKind::RequestReconciliation],
        "the reconciliation checkpoint survives the cancellation unclosed"
    );
}

/// Parks an ambiguous tool effect, applies the reconciliation `command_for`
/// builds, pumps to rest, and asserts the one `reconcile` it recorded.
async fn reconcile_once(
    vocabulary: AgentDecisionEventVocabulary,
    command_for: impl FnOnce(
        &HumanCheckpointId,
        &AgentEffectId,
        AgentEffectGeneration,
    ) -> AgentRunEntityCommand,
    outcome: AgentDecisionOutcome,
) -> Arc<CountingDecisionSink> {
    let (fx, sink) = fixture(AgentEffectSpec::non_idempotent(), vocabulary);
    report_unknown(&fx).await;
    let bound = held_checkpoint(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    let command = command_for(
        &bound.checkpoint_id,
        &bound.bound_effect.effect_id,
        bound.bound_effect.generation,
    );
    let reply = apply(&fx, command.clone())
        .await
        .expect("the reconciliation applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    fx.pump().await.expect("the reconciled run rests");
    let replay = replay_and_assert_whole(&fx, &sink, command).await;
    assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));

    let events = sink.events(&run_scope());
    let reconciles = of_kind(&events, AgentDecisionKind::Reconcile);
    assert_eq!(
        reconciles.len(),
        1,
        "one reconcile for {outcome}: {events:?}"
    );
    let reconciled = reconciles[0];
    assert_eq!(reconciled.reason_code.as_deref(), Some(outcome.as_label()));
    assert_eq!(reconciled.source, AgentDecisionSource::Human);
    assert_eq!(
        reconciled.safety_class,
        Some(bound.bound_effect.safety_class)
    );
    if vocabulary == AgentDecisionEventVocabulary::Version1 {
        // The default vocabulary leaves the version-2 fields off: the caller's
        // round trip checks all four.
        return sink;
    }
    assert_eq!(reconciled.outcome, Some(outcome));
    assert_eq!(
        reconciled
            .checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.checkpoint_id),
        Some(&bound.checkpoint_id),
        "the reconcile names the checkpoint bound to the generation"
    );
    let effect = reconciled
        .effect
        .as_ref()
        .expect("the reconcile names its effect");
    assert_eq!(effect.effect_id, bound.bound_effect.effect_id);
    assert_eq!(effect.generation, bound.bound_effect.generation);
    assert_eq!(effect.safety_class, bound.bound_effect.safety_class);
    sink
}

#[tokio::test]
async fn each_reconciliation_outcome_records_one_reconcile() {
    let version2 = AgentDecisionEventVocabulary::Version2;
    reconcile_once(
        version2,
        |checkpoint, _, _| {
            resolve(
                checkpoint,
                "d1",
                reconcile(AgentReconciliationDecision::ConfirmedCompleted {
                    resolution: Box::new(confirmed_executed()),
                }),
            )
        },
        AgentDecisionOutcome::ConfirmedExecuted,
    )
    .await;
    reconcile_once(
        version2,
        |checkpoint, _, _| {
            resolve(
                checkpoint,
                "d1",
                reconcile(AgentReconciliationDecision::ConfirmedNotExecuted),
            )
        },
        AgentDecisionOutcome::ConfirmedNotExecuted,
    )
    .await;
    reconcile_once(
        version2,
        |checkpoint, _, _| {
            resolve(
                checkpoint,
                "d1",
                reconcile(AgentReconciliationDecision::Compensate {
                    compensation: AgentCompensationRef::new(COMPENSATION)
                        .expect("the compensation ref is valid"),
                }),
            )
        },
        AgentDecisionOutcome::Compensate,
    )
    .await;
    reconcile_once(
        version2,
        |checkpoint, _, _| {
            resolve(
                checkpoint,
                "d1",
                reconcile(AgentReconciliationDecision::AbandonAndFail),
            )
        },
        AgentDecisionOutcome::Abandon,
    )
    .await;
    reconcile_once(
        version2,
        |_, effect_id, generation| {
            resolve_effect("r1", effect_id, generation, confirmed_executed())
        },
        AgentDecisionOutcome::ConfirmedExecuted,
    )
    .await;

    // `reconcile` is a kind bc0c224 already decodes: under the default
    // vocabulary it is written, without the fields it gained, so a bc0c224
    // binary writes it back unchanged.
    let sink = reconcile_once(
        AgentDecisionEventVocabulary::Version1,
        |_, effect_id, generation| {
            resolve_effect("r1", effect_id, generation, confirmed_executed())
        },
        AgentDecisionOutcome::ConfirmedExecuted,
    )
    .await;
    assert_bc0c224_round_trips(&sink.events(&run_scope()));
}

#[tokio::test]
async fn a_deduplicated_checkpoint_decision_records_nothing() {
    // A grant replayed under its operation id is answered from the decision
    // ledger and records nothing.
    let (fx, sink, _checkpoint_id, command) =
        decide_approval(AgentDecisionEventVocabulary::Version2, approve()).await;
    let replay = replay_and_assert_whole(&fx, &sink, command).await;
    assert!(matches!(replay, Ok(AgentRunEntityReply::Duplicate { .. })));
    assert_eq!(
        of_kind(
            &sink.events(&run_scope()),
            AgentDecisionKind::ResolveCheckpoint
        )
        .len(),
        1
    );

    // A record without its ledger (one written before the ledger existed)
    // reaches the checkpoint's own deduplication: the escalation replayed
    // there is applied as a no-op and records nothing.
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent(),
        AgentDecisionEventVocabulary::Version2,
    );
    report_unknown(&fx).await;
    let checkpoint_id =
        held_checkpoint(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation)
            .await
            .checkpoint_id;
    let escalate = resolve(
        &checkpoint_id,
        "e1",
        reconcile(AgentReconciliationDecision::Escalate),
    );
    apply(&fx, escalate.clone())
        .await
        .expect("the escalation applies");
    forget_operations(&fx.runs, &run_scope().persistence_id(), true).await;
    let replay = replay_and_assert_whole(&fx, &sink, escalate).await;
    assert!(
        matches!(replay, Ok(AgentRunEntityReply::Applied { .. })),
        "the checkpoint, not the ledger, deduplicated it: {replay:?}"
    );
    assert_eq!(
        of_kind(
            &sink.events(&run_scope()),
            AgentDecisionKind::EscalateCheckpoint
        )
        .len(),
        1
    );
}

/// The run's durable decision sequence.
async fn decision_sequence(fx: &Fixture) -> u64 {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.state()
        .expect("state reads")
        .loop_state()
        .expect("the loop exists")
        .decision_sequence()
}

#[tokio::test]
async fn a_refused_resolution_records_nothing_and_consumes_no_sequence() {
    // The gated charge is parked `Pending` behind its approval, so its
    // generation is not `Indeterminate`: the reconciliation draft is queued
    // and the core then refuses, rejecting the transition with it.
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        AgentDecisionEventVocabulary::Version2,
    );
    park(&fx, AgentRunStatus::WaitingForApproval).await;
    let gated = held_checkpoint(&fx, AgentCheckpointKind::Approval).await;
    let events = sink.events(&run_scope());
    let sequence = decision_sequence(&fx).await;
    assert_eq!(sequence, events.len() as u64);

    let command = resolve_effect(
        "r1",
        &gated.bound_effect.effect_id,
        gated.bound_effect.generation,
        AgentEffectResolution::ConfirmedNotExecuted,
    );
    let refusal = apply(&fx, command.clone())
        .await
        .expect_err("a generation that is not indeterminate is refused");
    assert_eq!(refusal.code(), "run-stale-effect-result");
    assert_eq!(decision_sequence(&fx).await, sequence);
    assert_eq!(sink.events(&run_scope()), events);

    let replay = replay_and_assert_whole(&fx, &sink, command).await;
    assert!(replay.is_err(), "the replay meets the same refusal");
    assert_eq!(decision_sequence(&fx).await, sequence);
    assert!(of_kind(&sink.events(&run_scope()), AgentDecisionKind::Reconcile).is_empty());
}

#[tokio::test]
async fn both_reconciliation_paths_on_one_generation_record_one_reconcile() {
    let (fx, sink) = fixture(
        AgentEffectSpec::non_idempotent(),
        AgentDecisionEventVocabulary::Version2,
    );
    report_unknown(&fx).await;
    let bound = held_checkpoint(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    apply(
        &fx,
        resolve(
            &bound.checkpoint_id,
            "d1",
            reconcile(AgentReconciliationDecision::ConfirmedCompleted {
                resolution: Box::new(confirmed_executed()),
            }),
        ),
    )
    .await
    .expect("the checkpoint decision applies");

    // The effect-layer command aimed at the generation the checkpoint
    // already resolved is refused, and records nothing.
    let command = resolve_effect(
        "r1",
        &bound.bound_effect.effect_id,
        bound.bound_effect.generation,
        confirmed_executed(),
    );
    apply(&fx, command.clone())
        .await
        .expect_err("the resolved generation refuses a second decision");
    fx.pump().await.expect("the reconciled run rests");
    let replay = replay_and_assert_whole(&fx, &sink, command).await;
    assert!(replay.is_err());

    let events = sink.events(&run_scope());
    assert_eq!(
        of_kind(&events, AgentDecisionKind::Reconcile).len(),
        1,
        "{events:?}"
    );
}
