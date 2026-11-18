//! What a run does with a call it committed and never sent.
//!
//! Specification: sections 8.7, 9.7 and 12. A call that is still `Pending`
//! has provably not reached the outbox, so a run that stops dispatching
//! cancels it in place: the wind-down fence. Issues #133 and #132 are what
//! the fence left behind. It cancelled a gated call and left the call's
//! approval or authorization waiting, on a run that then ended and could
//! answer nothing; and it kept the attempts the call had reserved. A run that
//! ended without winding down ran no fence at all.
//!
//! Each test here ends by checking that the attempts the run holds reserved
//! are those of the calls it still waits on, and no others.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    agent_operational_snapshot, evaluation_operation_id, AgentApprovalDecision,
    AgentBudgetCeilings, AgentCheckpoint, AgentCheckpointDecision, AgentCheckpointKind,
    AgentCheckpointStatus, AgentCompensationRef, AgentDecisionEvent, AgentDecisionEventVocabulary,
    AgentDecisionKind, AgentDecisionOutcome, AgentDecisionSource, AgentEffectPolicies,
    AgentEffectSpec, AgentGoalEvaluationMethod, AgentLoopState, AgentModelTurn, AgentModelUsage,
    AgentOperationId, AgentOperationKind, AgentReconciliationDecision, AgentRevisionNumber,
    AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunError, AgentRunStatus, AgentSchemaPolicy,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

/// The call a test puts behind a gate.
const CHARGE: &str = "charge-card";
/// A second call a test can gate, beside an ungated [`CHARGE`].
const WIRE: &str = "wire-funds";
/// A call that fails definitively.
const FLAKY: &str = "flaky-tool";
/// A call that succeeds.
const LOOKUP: &str = "lookup";
/// The compensation an operator schedules for an ambiguous [`CHARGE`].
const COMPENSATION: &str = "refund-charge";

/// Which gate a test puts on a call.
#[derive(Debug, Clone, Copy)]
enum Gate {
    Approval,
    Authorization,
}

impl Gate {
    fn spec(self) -> AgentEffectSpec {
        match self {
            Self::Approval => AgentEffectSpec::non_idempotent().with_checkpoint_required(),
            Self::Authorization => AgentEffectSpec::non_idempotent().with_authorization_required(),
        }
    }

    fn kind(self) -> AgentCheckpointKind {
        match self {
            Self::Approval => AgentCheckpointKind::Approval,
            Self::Authorization => AgentCheckpointKind::SecurityAuthorization,
        }
    }
}

fn tool(name: &str) -> AgentToolId {
    AgentToolId::new(name).expect("the tool id is valid")
}

fn usage() -> AgentModelUsage {
    AgentModelUsage {
        input_tokens: 8,
        output_tokens: 4,
        cost_micros: 2,
        ..Default::default()
    }
}

/// A model turn that calls each of `calls` once, in order.
fn calling_turn(calls: &[&str]) -> AgentModelTurn {
    let mut turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_usage(usage());
    for (index, name) in calls.iter().enumerate() {
        turn = turn.with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new(format!("call-{}", index + 1)).expect("the call id is valid"),
                tool(name),
                serde_json::json!({ "order": 7 }),
            )
            .expect("the tool call is bounded"),
        );
    }
    turn
}

/// Effect policies under which every one of `calls` takes one attempt, the
/// `gated` call waits behind its gate, and a compensation takes one attempt.
fn policies(calls: &[&str], gated: Option<(&str, Gate)>) -> AgentEffectPolicies {
    let mut policies = AgentEffectPolicies::new()
        .with_compensation_spec(AgentEffectSpec::non_idempotent())
        .expect("the compensation spec is valid");
    for name in calls {
        let spec = match gated {
            Some((gated, gate)) if gated == *name => gate.spec(),
            _ => AgentEffectSpec::non_idempotent(),
        };
        policies = policies
            .with_tool_spec(tool(name), spec)
            .expect("the tool spec is valid");
    }
    policies
}

/// A world whose turn one calls `calls` under `policies`: [`FLAKY`] fails
/// definitively and every other call succeeds. Its decisions reach the
/// returned sink.
fn world(calls: &[&str], policies: AgentEffectPolicies) -> (Fixture, Arc<CountingDecisionSink>) {
    let content = || {
        AgentTaskContent::inline(serde_json::json!({ "done": true }))
            .expect("the result is inline-bounded")
    };
    let mut dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, calling_turn(calls)),
    )
    .with_tool_failure(FLAKY, "tool-unavailable", "the tool is down")
    .with_compensation_result(COMPENSATION, content());
    for name in calls.iter().filter(|name| **name != FLAKY) {
        dispatcher = dispatcher.with_tool_result(name, content());
    }
    let sink = CountingDecisionSink::new();
    let fx = Fixture::with_sink(
        dispatcher,
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);
    (fx, sink)
}

/// Creates the task, under a ceiling on effect attempts when one is given.
async fn start(fx: &Fixture, attempt_ceiling: Option<u32>) {
    fx.instantiate_agent().await;
    match attempt_ceiling {
        Some(ceiling) => {
            fx.create_task_with(task_definition().with_budgets(AgentBudgetCeilings {
                max_effect_attempts: Some(ceiling),
                ..AgentBudgetCeilings::unbounded()
            }))
            .await;
        }
        None => fx.create_task().await,
    }
}

/// The run's durable loop state, read exactly as recovery reads it.
async fn loop_state(fx: &Fixture) -> AgentLoopState {
    rakka_agent::load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run state exists")
        .loop_state()
        .expect("the loop state exists")
        .clone()
}

async fn status(fx: &Fixture) -> AgentRunStatus {
    fx.run_snapshot().await.expect("the run exists").status
}

/// The run's stored revision.
async fn revision(fx: &Fixture) -> rakka_persistence::Revision {
    fx.runs
        .load(&run_scope().persistence_id())
        .await
        .expect("the run record loads")
        .expect("the run record exists")
        .revision
}

async fn apply(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let mut run = fx.run();
    run.recover(fx.now()).await?;
    run.apply(command, &fx.router, fx.now()).await
}

/// Runs the run's settle pass from durable state.
async fn settle(fx: &Fixture) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("the run settles");
}

/// Answers turn one's model call and flushes the turn it commits: an ungated
/// call is out, and a gated one waits `Pending` behind its gate.
async fn commit_the_turn(fx: &Fixture) {
    settle(fx).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let answered = fx
        .dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");
    assert_eq!(answered, 1, "only the model call was answered");
    settle(fx).await;
}

/// The effect of the turn's call to `name`.
async fn call_to(fx: &Fixture, name: &str) -> AgentRunEffect {
    loop_state(fx)
        .await
        .effects()
        .iter()
        .find(|effect| {
            effect
                .request
                .tool_call()
                .is_some_and(|call| call.tool.as_str() == name)
        })
        .cloned()
        .unwrap_or_else(|| panic!("the run holds a call to {name}"))
}

/// Lands the scripted result of `effect`, and nothing else.
async fn land(fx: &Fixture, effect: &AgentRunEffect) {
    let outcome = fx.dispatcher.answer(effect).await;
    record(fx, effect, outcome).await;
}

/// Reports `effect`'s one attempt as having an outcome nobody can establish.
async fn report_unknown(fx: &Fixture, effect: &AgentRunEffect) {
    record(
        fx,
        effect,
        AgentRunEffectOutcome::indeterminate("connection-lost", "the worker died mid-invocation"),
    )
    .await;
}

async fn record(fx: &Fixture, effect: &AgentRunEffect, outcome: AgentRunEffectOutcome) {
    apply(
        fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(outcome),
        },
    )
    .await
    .expect("the result applies");
}

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

async fn decide(
    fx: &Fixture,
    checkpoint_id: &HumanCheckpointId,
    key: &str,
    decision: AgentCheckpointDecision,
) -> Result<AgentRunEntityReply, AgentRunError> {
    apply(
        fx,
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: decision_key(key),
            checkpoint_id: checkpoint_id.clone(),
            resolver: resolver(),
            decision: Box::new(decision),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await
}

fn approve() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
        credential_binding: None,
        expires_at: AgentTimestampMillis::new(1_000_000),
        allowed_use_count: 1,
    })
}

fn deny() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Deny {
        reason: "not this time".to_string(),
    })
}

fn compensate() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::Compensate {
        compensation: AgentCompensationRef::new(COMPENSATION)
            .expect("the compensation ref is valid"),
    })
}

/// The checkpoints the run holds that still wait for a decision.
async fn waiting(fx: &Fixture) -> Vec<AgentCheckpoint> {
    loop_state(fx)
        .await
        .open_checkpoints()
        .iter()
        .filter(|checkpoint| checkpoint.status.is_waiting())
        .cloned()
        .collect()
}

/// The one checkpoint of `kind` the run holds, whatever its status.
async fn held(fx: &Fixture, kind: AgentCheckpointKind) -> AgentCheckpoint {
    let held: Vec<AgentCheckpoint> = loop_state(fx)
        .await
        .open_checkpoints()
        .iter()
        .filter(|checkpoint| checkpoint.kind == kind)
        .cloned()
        .collect();
    assert_eq!(held.len(), 1, "one {kind} checkpoint is held: {held:?}");
    held.into_iter().next().expect("one checkpoint")
}

/// The attempts the run holds reserved are those of the calls it still waits
/// on: a call that has an outcome, or was cancelled before it was sent, holds
/// none.
async fn assert_only_what_is_out_is_reserved(fx: &Fixture, at: &str) {
    let state = loop_state(fx).await;
    let out: u64 = state
        .outstanding_effects()
        .map(|effect| u64::from(effect.max_attempts))
        .sum();
    assert_eq!(
        state.budget().reserved_attempts(),
        out,
        "{at}: the attempts reserved are those of the calls still out: {:?}",
        state
            .effects()
            .iter()
            .map(|effect| (effect.kind(), effect.status, effect.max_attempts))
            .collect::<Vec<_>>()
    );
}

/// Every `resolve-checkpoint` the stream holds for `checkpoint_id`.
fn closes_of<'a>(
    events: &'a [AgentDecisionEvent],
    checkpoint_id: &HumanCheckpointId,
) -> Vec<&'a AgentDecisionEvent> {
    events
        .iter()
        .filter(|event| {
            event.kind == AgentDecisionKind::ResolveCheckpoint
                && event
                    .checkpoint
                    .as_ref()
                    .is_some_and(|checkpoint| &checkpoint.checkpoint_id == checkpoint_id)
        })
        .collect()
}

/// Asserts the stream holds exactly one close of `checkpoint_id`, recorded as
/// a cancel's is: `cancelled`, by the run's own policy.
fn assert_closed_as_a_cancel_closes(
    events: &[AgentDecisionEvent],
    checkpoint_id: &HumanCheckpointId,
) -> u64 {
    let closes = closes_of(events, checkpoint_id);
    assert_eq!(closes.len(), 1, "one close is recorded: {events:?}");
    let close = closes[0];
    assert_eq!(close.outcome, Some(AgentDecisionOutcome::Cancelled));
    assert_eq!(close.reason_code.as_deref(), Some("cancelled"));
    assert_eq!(close.source, AgentDecisionSource::DeterministicPolicy);
    close.sequence
}

/// The sequence of the one event of `kind` the stream holds.
fn the_one(events: &[AgentDecisionEvent], kind: AgentDecisionKind) -> u64 {
    let found: Vec<u64> = events
        .iter()
        .filter(|event| event.kind == kind)
        .map(|event| event.sequence)
        .collect();
    assert_eq!(found.len(), 1, "one {kind:?} is recorded: {events:?}");
    found[0]
}

/// The sequence of the run's one `winding-down` wait: the event the
/// transition that begins a wind-down records after its own decisions.
fn the_wind_down(events: &[AgentDecisionEvent]) -> u64 {
    let found: Vec<u64> = events
        .iter()
        .filter(|event| {
            event.kind == AgentDecisionKind::Wait
                && event.reason_code.as_deref() == Some("winding-down")
        })
        .map(|event| event.sequence)
        .collect();
    assert_eq!(found.len(), 1, "one wind-down is recorded: {events:?}");
    found[0]
}

/// The checkpoints the run's operational snapshot lists as open.
async fn listed_open(fx: &Fixture) -> usize {
    agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists")
    .open_checkpoints
    .len()
}

/// Two more settle passes write nothing and record nothing.
async fn assert_at_rest(fx: &Fixture, sink: &CountingDecisionSink) {
    settle(fx).await;
    let rested = (revision(fx).await, sink.events(&run_scope()));
    settle(fx).await;
    assert_eq!(
        (revision(fx).await, sink.events(&run_scope())),
        rested,
        "a further settle pass writes nothing"
    );
    assert_decision_stream_whole(sink, &run_scope());
}

// ---------------------------------------------------------------------------
// Issue #133: a gate left waiting on a call the fence cancelled.
// ---------------------------------------------------------------------------

/// Issue #133, as filed. One turn calls a gated tool and a tool that fails.
/// The failure cancels the gated call, which was never sent, and the run ends
/// `Failed`. The call's gate closes with it, recorded as a cancel records it,
/// before the run's end. It used to stay open on the ended run, where a
/// decision is refused, the timer skips it and a cancel is refused.
#[tokio::test]
async fn a_failed_call_closes_the_gate_of_the_call_it_cancels() {
    for gate in [Gate::Approval, Gate::Authorization] {
        let calls = [CHARGE, FLAKY];
        let (fx, sink) = world(&calls, policies(&calls, Some((CHARGE, gate))));
        start(&fx, None).await;
        fx.pump().await.expect("the loop runs to its end");

        assert_eq!(status(&fx).await, AgentRunStatus::Failed, "{gate:?}");
        assert_eq!(
            call_to(&fx, CHARGE).await.status,
            AgentRunEffectStatus::Cancelled,
            "{gate:?}: the gated call was cancelled before it was sent"
        );
        assert_eq!(
            waiting(&fx).await,
            Vec::new(),
            "{gate:?}: no gate waits on the ended run"
        );
        let closed = held(&fx, gate.kind()).await;
        assert_eq!(closed.status, AgentCheckpointStatus::Cancelled, "{gate:?}");
        assert_eq!(
            listed_open(&fx).await,
            0,
            "{gate:?}: the snapshot lists none"
        );

        let events = sink.events(&run_scope());
        let close = assert_closed_as_a_cancel_closes(&events, &closed.checkpoint_id);
        let end = the_one(&events, AgentDecisionKind::Fail);
        assert!(close < end, "{gate:?}: the close precedes the end");
        assert_eq!(
            events.last().map(|event| event.kind),
            Some(AgentDecisionKind::Fail),
            "{gate:?}: the end is the last event"
        );
        assert_only_what_is_out_is_reserved(&fx, "after the end").await;
        assert_at_rest(&fx, &sink).await;
    }
}

/// The gate closes in the transition that cancels its call, not when the run
/// ends. Here the failure lands with another call of the turn still out, so
/// the run winds down and has not ended. A decision that arrives then is
/// refused as one for a closed checkpoint and changes nothing. It used to be
/// applied: a grant was stored for a cancelled call, and a denial rewrote the
/// cancelled call as failed.
#[tokio::test]
async fn a_gate_closes_with_its_call_while_the_run_is_still_winding_down() {
    let calls = [CHARGE, FLAKY, LOOKUP];
    let (fx, sink) = world(&calls, policies(&calls, Some((CHARGE, Gate::Approval))));
    start(&fx, None).await;
    commit_the_turn(&fx).await;
    let gate = held(&fx, AgentCheckpointKind::Approval).await;
    assert_eq!(gate.status, AgentCheckpointStatus::Open);
    assert_eq!(status(&fx).await, AgentRunStatus::WaitingForApproval);

    land(&fx, &call_to(&fx, FLAKY).await).await;

    assert_eq!(
        status(&fx).await,
        AgentRunStatus::Cancelling,
        "the lookup is still out, so the run winds down"
    );
    assert_eq!(
        call_to(&fx, CHARGE).await.status,
        AgentRunEffectStatus::Cancelled
    );
    assert_eq!(
        waiting(&fx).await,
        Vec::new(),
        "the gate closed with its call"
    );
    let events = sink.events(&run_scope());
    let close = assert_closed_as_a_cancel_closes(&events, &gate.checkpoint_id);
    assert!(
        close < the_wind_down(&events),
        "the failure's own transition closed it, and not a later pass: {events:?}"
    );
    assert_only_what_is_out_is_reserved(&fx, "winding down").await;

    let before = (revision(&fx).await, call_to(&fx, CHARGE).await);
    for (key, decision) in [("grant", approve()), ("deny", deny())] {
        let refused = decide(&fx, &gate.checkpoint_id, key, decision)
            .await
            .expect_err("a decision for a closed checkpoint is refused");
        assert_eq!(refused.code(), "checkpoint-already-resolved", "{key}");
        assert_eq!(
            (revision(&fx).await, call_to(&fx, CHARGE).await),
            before,
            "{key}: the refusal changes nothing"
        );
    }

    land(&fx, &call_to(&fx, LOOKUP).await).await;
    assert_eq!(status(&fx).await, AgentRunStatus::Failed);
    let events = sink.events(&run_scope());
    assert_closed_as_a_cancel_closes(&events, &gate.checkpoint_id);
    assert_eq!(
        events.last().map(|event| event.kind),
        Some(AgentDecisionKind::Fail)
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A compensation's wind-down runs the same fence. An ungated call's outcome
/// is unknown and an operator schedules its compensation, which cancels the
/// gated call beside it. That call's gate closes in the decision's
/// transition; it used to stay open through the wind-down and past the end.
#[tokio::test]
async fn a_compensation_closes_the_gate_of_the_call_it_cancels() {
    let calls = [CHARGE, WIRE];
    let (fx, sink) = world(&calls, policies(&calls, Some((WIRE, Gate::Approval))));
    start(&fx, None).await;
    commit_the_turn(&fx).await;
    report_unknown(&fx, &call_to(&fx, CHARGE).await).await;
    assert_eq!(status(&fx).await, AgentRunStatus::WaitingForReconciliation);
    let gate = held(&fx, AgentCheckpointKind::Approval).await;
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;

    decide(&fx, &park.checkpoint_id, "compensate", compensate())
        .await
        .expect("the compensation is scheduled");

    assert_eq!(
        call_to(&fx, WIRE).await.status,
        AgentRunEffectStatus::Cancelled,
        "the decision's wind-down cancelled the unsent call"
    );
    assert_eq!(waiting(&fx).await, Vec::new(), "its gate closed with it");
    assert!(
        !status(&fx).await.is_terminal(),
        "the run ends after its compensation"
    );
    let events = sink.events(&run_scope());
    let close = assert_closed_as_a_cancel_closes(&events, &gate.checkpoint_id);
    assert!(
        close < the_wind_down(&events),
        "the decision's own transition closed it, and not a later pass: {events:?}"
    );
    assert_only_what_is_out_is_reserved(&fx, "behind the compensation").await;

    fx.pump().await.expect("the compensation lands");
    assert!(status(&fx).await.is_terminal());
    assert_eq!(waiting(&fx).await, Vec::new());
    assert_eq!(listed_open(&fx).await, 0);
    assert_closed_as_a_cancel_closes(&sink.events(&run_scope()), &gate.checkpoint_id);
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A goal-bearing world whose turn one proposes a result and calls nothing,
/// returned with the model call out. Its decisions reach the returned sink.
async fn proposing_goal_world() -> (Fixture, Arc<CountingDecisionSink>) {
    let proposing = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(usage());
    let sink = CountingDecisionSink::new();
    let fx = Fixture::with_sink(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new().with_turn_for(1, proposing),
        ),
        InMemoryAgentRunEffectSink::new(),
        AgentEffectPolicies::default(),
        Arc::new(AtomicU64::new(1)),
    )
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec_with_evaluator(), true),
    ))
    .await
    .expect("the goal-bearing creation applies");
    settle(&fx).await;
    (fx, sink)
}

/// Commands a human review of the goal and returns its evaluation effect and
/// the approval the command opened on it.
async fn command_a_review(fx: &Fixture) -> (AgentRunEffect, AgentCheckpoint) {
    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.method = AgentGoalEvaluationMethod::HumanReview;
    apply(
        fx,
        AgentRunEntityCommand::EvaluateGoal {
            operation_id: evaluation_operation_id(&run_scope(), "1")
                .expect("the operation id derives"),
            evaluation: Box::new(request),
        },
    )
    .await
    .expect("the review commits");
    let evaluation = evaluation_of(fx).await;
    assert_eq!(evaluation.status, AgentRunEffectStatus::Pending);
    let gate = held(fx, AgentCheckpointKind::Approval).await;
    assert_eq!(gate.bound_effect.effect_id, evaluation.effect_id);
    (evaluation, gate)
}

/// The goal evaluation the run holds.
async fn evaluation_of(fx: &Fixture) -> AgentRunEffect {
    loop_state(fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::GoalEvaluationCall)
        .cloned()
        .expect("the run holds its goal evaluation")
}

/// A run that ends without winding down ran no fence at all. A review of the
/// goal is open when the run proposes its result and the task accepts it:
/// a turn that makes no call is not held for an evaluation. The run completed
/// holding the evaluation `Pending`, which an ended run never sends, behind a
/// review nothing could answer. The end now cancels the evaluation and closes
/// its review, as any wind-down does.
#[tokio::test]
async fn a_run_that_completes_closes_a_review_it_can_no_longer_answer() {
    let (fx, sink) = proposing_goal_world().await;
    let (_evaluation, review) = command_a_review(&fx).await;

    fx.pump().await.expect("the task accepts the result");

    assert_eq!(status(&fx).await, AgentRunStatus::Completed);
    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Cancelled,
        "the evaluation the ended run would never send is cancelled"
    );
    assert_eq!(waiting(&fx).await, Vec::new(), "the review closed with it");
    assert_eq!(listed_open(&fx).await, 0);
    let events = sink.events(&run_scope());
    let close = assert_closed_as_a_cancel_closes(&events, &review.checkpoint_id);
    assert!(close < the_one(&events, AgentDecisionKind::Complete));
    assert_eq!(
        events.last().map(|event| event.kind),
        Some(AgentDecisionKind::Complete),
        "the end is the last event"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;

    let refused = decide(&fx, &review.checkpoint_id, "late", approve())
        .await
        .expect_err("the ended run answers nothing for the review");
    assert_eq!(refused.code(), "run-terminal");
}

// ---------------------------------------------------------------------------
// Issue #132: the attempts a cancelled call had reserved.
// ---------------------------------------------------------------------------

/// Issue #132. The fence releases the attempts each call it cancels had
/// reserved, so a reservation the wind-down then makes is checked against
/// what the run really holds.
///
/// Three calls under a ceiling of four attempts: one is gated, one fails and
/// one's outcome is unknown. The model call and the two calls that ran each
/// consumed one. The failure cancels the gated call, which made none. The
/// operator then schedules a compensation for the unknown call, which needs
/// one: three consumed and one reserved is the ceiling. It used to be refused
/// `run-redispatch-unaffordable`, because the cancelled call still held its
/// one.
#[tokio::test]
async fn a_cancelled_calls_attempts_are_free_for_the_compensation_that_follows() {
    let calls = [CHARGE, WIRE, FLAKY];
    let (fx, sink) = world(&calls, policies(&calls, Some((WIRE, Gate::Approval))));
    start(&fx, Some(4)).await;
    commit_the_turn(&fx).await;
    report_unknown(&fx, &call_to(&fx, CHARGE).await).await;
    land(&fx, &call_to(&fx, FLAKY).await).await;
    assert_eq!(
        status(&fx).await,
        AgentRunStatus::WaitingForReconciliation,
        "the failure winds the run down behind its park"
    );
    assert_eq!(
        call_to(&fx, WIRE).await.status,
        AgentRunEffectStatus::Cancelled
    );
    let after_the_failure = loop_state(&fx).await;

    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    decide(&fx, &park.checkpoint_id, "compensate", compensate())
        .await
        .expect("the run can afford its compensation");
    assert_eq!(
        after_the_failure.budget().reserved_attempts(),
        0,
        "the failure left nothing reserved: the cancelled call's one was released"
    );

    let compensation = loop_state(&fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::CompensationCall)
        .cloned()
        .expect("the compensation is scheduled");
    assert_eq!(compensation.max_attempts, 1);
    assert_only_what_is_out_is_reserved(&fx, "behind the compensation").await;
    fx.pump().await.expect("the compensation lands");
    assert!(status(&fx).await.is_terminal());
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// The decision that schedules a compensation cancels the unsent calls
/// first, so the compensation can spend what they had reserved. Under a
/// ceiling of three attempts, the model call and the unknown call consumed
/// two and the gated call holds one. Scheduling the compensation cancels the
/// gated call and reserves its one. It used to reserve before it cancelled,
/// and was refused.
#[tokio::test]
async fn a_compensation_can_spend_what_its_own_wind_down_releases() {
    let calls = [CHARGE, WIRE];
    let (fx, sink) = world(&calls, policies(&calls, Some((WIRE, Gate::Approval))));
    start(&fx, Some(3)).await;
    commit_the_turn(&fx).await;
    report_unknown(&fx, &call_to(&fx, CHARGE).await).await;
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;

    decide(&fx, &park.checkpoint_id, "compensate", compensate())
        .await
        .expect("the run can afford its compensation");

    assert_eq!(
        call_to(&fx, WIRE).await.status,
        AgentRunEffectStatus::Cancelled
    );
    assert_only_what_is_out_is_reserved(&fx, "behind the compensation").await;
    fx.pump().await.expect("the compensation lands");
    assert!(status(&fx).await.is_terminal());
    assert_at_rest(&fx, &sink).await;
}

/// A compensation the run cannot afford even then is refused, and the
/// refusal changes nothing: the unsent call is still `Pending` behind its
/// gate, and the unknown call still waits for a decision.
#[tokio::test]
async fn a_compensation_the_run_cannot_afford_changes_nothing() {
    let calls = [CHARGE, WIRE];
    let two_attempts = policies(&calls, Some((WIRE, Gate::Approval)))
        .with_compensation_spec(
            AgentEffectSpec::idempotent(2).expect("the compensation spec is valid"),
        )
        .expect("the compensation spec is valid");
    let (fx, _sink) = world(&calls, two_attempts);
    start(&fx, Some(3)).await;
    commit_the_turn(&fx).await;
    report_unknown(&fx, &call_to(&fx, CHARGE).await).await;
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    let before = (revision(&fx).await, loop_state(&fx).await);

    let refused = decide(&fx, &park.checkpoint_id, "compensate", compensate())
        .await
        .expect_err("two consumed and two more is past a ceiling of three");

    assert_eq!(refused.code(), "run-redispatch-unaffordable");
    assert_eq!((revision(&fx).await, loop_state(&fx).await), before);
    assert_eq!(
        call_to(&fx, WIRE).await.status,
        AgentRunEffectStatus::Pending
    );
    assert_eq!(waiting(&fx).await.len(), 2, "both checkpoints still wait");
}

/// A cancel runs the fence too. It already closed the gate; it now releases
/// what the cancelled call had reserved.
#[tokio::test]
async fn a_cancel_releases_the_attempts_of_the_call_it_cancels() {
    let calls = [CHARGE];
    let (fx, sink) = world(&calls, policies(&calls, Some((CHARGE, Gate::Approval))));
    start(&fx, None).await;
    commit_the_turn(&fx).await;
    let gate = held(&fx, AgentCheckpointKind::Approval).await;
    assert_only_what_is_out_is_reserved(&fx, "parked on the approval").await;

    apply(
        &fx,
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "cancel-1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
    )
    .await
    .expect("the cancellation applies");

    assert_eq!(status(&fx).await, AgentRunStatus::Cancelled);
    assert_eq!(
        call_to(&fx, CHARGE).await.status,
        AgentRunEffectStatus::Cancelled
    );
    assert_closed_as_a_cancel_closes(&sink.events(&run_scope()), &gate.checkpoint_id);
    assert_only_what_is_out_is_reserved(&fx, "after the cancel").await;
    assert_at_rest(&fx, &sink).await;
}

// ---------------------------------------------------------------------------
// Records an older binary wrote.
// ---------------------------------------------------------------------------

/// Rewrites the stored run record's loop state through `edit`, as JSON.
async fn rewrite_loop_state(
    fx: &Fixture,
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) {
    let id = run_scope().persistence_id();
    let record = fx
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    edit(
        value["run"]["loop_state"]
            .as_object_mut()
            .expect("the loop state is an object"),
    );
    let rewritten = serde_json::from_value(value).expect("the rewritten record deserializes");
    fx.runs
        .compare_and_set(&id, record.revision, rewritten)
        .await
        .expect("the rewritten record persists");
}

/// Puts the record's one approval back to waiting and its call's one attempt
/// back in the reservation: what a binary before issues #133 and #132 left
/// when its fence cancelled the call.
async fn reopen_the_approval_as_an_older_binary_left_it(fx: &Fixture) {
    rewrite_loop_state(fx, |loop_state| {
        let mut reopened = Vec::new();
        for checkpoint in loop_state["open_checkpoints"]
            .as_array_mut()
            .expect("the checkpoints are an array")
        {
            if checkpoint["kind"].as_str() == Some("approval") {
                checkpoint["status"] = serde_json::json!("open");
                reopened.push(checkpoint["checkpoint_id"].clone());
            }
        }
        assert_eq!(reopened.len(), 1, "the record holds one approval");
        loop_state.insert("pending_checkpoint".to_string(), reopened.remove(0));
        loop_state["budget"]["reserved_attempts"] = serde_json::json!(1);
    })
    .await;
}

/// The terminal facts of the run that must never move once it has ended.
async fn terminal_record(
    fx: &Fixture,
) -> (
    AgentRunStatus,
    Option<rakka_agent::AgentRunTerminalReason>,
    Option<AgentTimestampMillis>,
) {
    let run = fx.run_snapshot().await.expect("the run exists");
    (run.status, run.terminal_reason, run.terminal_at)
}

/// A run that ended under an older binary still holding a cancelled call's
/// approval has it closed by the first settle pass that touches the record,
/// once: the terminal status, reason and stamp do not move, and a further
/// pass writes nothing.
#[tokio::test]
async fn an_ended_record_holding_a_cancelled_calls_approval_is_closed_once() {
    let calls = [CHARGE, FLAKY];
    let (fx, _sink) = world(&calls, policies(&calls, Some((CHARGE, Gate::Approval))));
    start(&fx, None).await;
    fx.pump().await.expect("the loop runs to its end");
    reopen_the_approval_as_an_older_binary_left_it(&fx).await;
    let ended = terminal_record(&fx).await;
    assert_eq!(ended.0, AgentRunStatus::Failed);
    assert_eq!(waiting(&fx).await.len(), 1, "the record as it was left");
    assert_eq!(listed_open(&fx).await, 1);
    let sequence = loop_state(&fx).await.decision_sequence();

    settle(&fx).await;

    assert_eq!(waiting(&fx).await, Vec::new(), "the pass closed it");
    assert_eq!(
        held(&fx, AgentCheckpointKind::Approval).await.status,
        AgentCheckpointStatus::Cancelled
    );
    assert_eq!(listed_open(&fx).await, 0);
    assert_eq!(
        loop_state(&fx).await.decision_sequence(),
        sequence + 1,
        "the close is recorded once"
    );
    assert_eq!(terminal_record(&fx).await, ended, "the end does not move");
    let repaired = revision(&fx).await;
    settle(&fx).await;
    assert_eq!(
        revision(&fx).await,
        repaired,
        "a further pass writes nothing"
    );
}

/// The same record met while it is still winding down: the pass closes the
/// approval then, and does not wait for the run's end.
#[tokio::test]
async fn a_winding_down_record_holding_a_cancelled_calls_approval_is_closed_once() {
    let calls = [CHARGE, FLAKY, LOOKUP];
    let (fx, _sink) = world(&calls, policies(&calls, Some((CHARGE, Gate::Approval))));
    start(&fx, None).await;
    commit_the_turn(&fx).await;
    land(&fx, &call_to(&fx, FLAKY).await).await;
    reopen_the_approval_as_an_older_binary_left_it(&fx).await;
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);
    assert_eq!(waiting(&fx).await.len(), 1, "the record as it was left");
    let sequence = loop_state(&fx).await.decision_sequence();

    settle(&fx).await;

    assert_eq!(waiting(&fx).await, Vec::new(), "the pass closed it");
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);
    assert_eq!(loop_state(&fx).await.decision_sequence(), sequence + 1);
    let repaired = revision(&fx).await;
    settle(&fx).await;
    assert_eq!(
        revision(&fx).await,
        repaired,
        "a further pass writes nothing"
    );

    land(&fx, &call_to(&fx, LOOKUP).await).await;
    assert_eq!(status(&fx).await, AgentRunStatus::Failed);
    assert_eq!(waiting(&fx).await, Vec::new());
}

/// A run that completed under an older binary still holding its goal
/// evaluation `Pending` behind an open review: the first settle pass cancels
/// the evaluation the ended run will never send, releases what it reserved
/// and closes the review.
#[tokio::test]
async fn a_completed_record_holding_an_unsent_review_is_cancelled_and_closed_once() {
    let (fx, _sink) = proposing_goal_world().await;
    let (evaluation, _review) = command_a_review(&fx).await;
    fx.pump().await.expect("the task accepts the result");
    reopen_the_approval_as_an_older_binary_left_it(&fx).await;
    rewrite_loop_state(&fx, |loop_state| {
        let mut pending = 0;
        for effect in loop_state["effects"]
            .as_array_mut()
            .expect("the effects are an array")
        {
            // The model call succeeded; the other effect is the evaluation,
            // which this binary's end cancelled.
            if effect["status"].as_str() != Some("succeeded") {
                effect["status"] = serde_json::json!("pending");
                pending += 1;
            }
        }
        assert_eq!(
            pending, 1,
            "the one effect with no result is the evaluation"
        );
        loop_state["budget"]["reserved_attempts"] = serde_json::json!(evaluation.max_attempts);
    })
    .await;
    let ended = terminal_record(&fx).await;
    assert_eq!(ended.0, AgentRunStatus::Completed);
    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Pending
    );
    assert_eq!(waiting(&fx).await.len(), 1, "the record as it was left");
    let sequence = loop_state(&fx).await.decision_sequence();

    settle(&fx).await;

    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Cancelled
    );
    assert_eq!(waiting(&fx).await, Vec::new(), "the pass closed the review");
    assert_eq!(loop_state(&fx).await.decision_sequence(), sequence + 1);
    assert_only_what_is_out_is_reserved(&fx, "after the repair").await;
    assert_eq!(terminal_record(&fx).await, ended, "the end does not move");
    let repaired = revision(&fx).await;
    settle(&fx).await;
    assert_eq!(
        revision(&fx).await,
        repaired,
        "a further pass writes nothing"
    );
}

/// Where a forged record's run stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// Live, parked on the gate of the one call its turn made.
    Live,
    /// Winding down after a failed call, with another call still out.
    WindingDown,
    /// Ended `Failed`.
    Ended,
}

impl Standing {
    /// A world brought to this standing through commands, whose turn's first
    /// call is [`CHARGE`] behind `gate`.
    async fn world(self, gate: Gate) -> Fixture {
        let calls: &[&str] = match self {
            Self::Live => &[CHARGE],
            Self::WindingDown => &[CHARGE, FLAKY, LOOKUP],
            Self::Ended => &[CHARGE, FLAKY],
        };
        let (fx, _sink) = world(calls, policies(calls, Some((CHARGE, gate))));
        start(&fx, None).await;
        match self {
            Self::Live => commit_the_turn(&fx).await,
            Self::WindingDown => {
                commit_the_turn(&fx).await;
                land(&fx, &call_to(&fx, FLAKY).await).await;
            }
            Self::Ended => fx.pump().await.expect("the loop runs to its end"),
        }
        let expected = match (self, gate) {
            (Self::Live, Gate::Approval) => AgentRunStatus::WaitingForApproval,
            (Self::Live, Gate::Authorization) => AgentRunStatus::WaitingForAuthorization,
            (Self::WindingDown, _) => AgentRunStatus::Cancelling,
            (Self::Ended, _) => AgentRunStatus::Failed,
        };
        assert_eq!(status(&fx).await, expected, "{self:?} {gate:?}");
        fx
    }
}

/// What a forged record makes of the gated call: each status it can hold,
/// and `missing`, a record that no longer holds the call at all.
/// `indeterminate` is left out: a call whose outcome is unknown is the
/// reconciliation park's, which has its own checkpoint and its own repair.
const FORGED_CALLS: [&str; 8] = [
    "pending",
    "ready",
    "succeeded",
    "failed",
    "exhausted",
    "compensated",
    "cancelled",
    "missing",
];

/// Every record a settle pass could meet holding a gate, whatever wrote it:
/// each standing of the run, each kind of gate, each status of the call the
/// gate is bound to or no call at all, and the gate open or escalated.
/// Ninety-six records, three passes each.
///
/// - Every record is at rest after two passes: a third writes nothing. A
///   repair that did not clear what it asks would write on every pass.
/// - A live run that is not winding down never has its gate closed for it,
///   whatever the call's status: the gate is its wait, and the repair does
///   not ask such a run.
/// - A run that has ended is left with no waiting gate, and a call it never
///   sent is cancelled.
/// - A run that is winding down has the gate closed unless its call is still
///   `Pending` or is not held. No wind-down leaves a call `Pending` and no
///   record drops a call and keeps its gate, so neither is a record a binary
///   writes, and the pass leaves both as they are.
#[tokio::test]
async fn no_forged_gate_is_closed_on_a_live_run_or_left_waiting_on_an_ended_one() {
    let mut forged = 0;
    for standing in [Standing::Live, Standing::WindingDown, Standing::Ended] {
        for gate in [Gate::Approval, Gate::Authorization] {
            for call_status in FORGED_CALLS {
                for gate_status in ["open", "escalated"] {
                    let at = format!("{standing:?} {gate:?} call {call_status} gate {gate_status}");
                    let fx = standing.world(gate).await;
                    rewrite_loop_state(&fx, |loop_state| {
                        let effects = loop_state["effects"]
                            .as_array_mut()
                            .expect("the effects are an array");
                        let call = effects
                            .iter()
                            .position(|effect| effect["slot"].as_u64() == Some(1))
                            .unwrap_or_else(|| panic!("{at}: the gated call is the turn's first"));
                        if call_status == "missing" {
                            effects.remove(call);
                        } else {
                            effects[call]["status"] = serde_json::json!(call_status);
                        }
                        let held = loop_state["open_checkpoints"]
                            .as_array_mut()
                            .expect("the checkpoints are an array");
                        assert_eq!(held.len(), 1, "{at}: the record holds the one gate");
                        held[0]["status"] = serde_json::json!(gate_status);
                        let waiting = held[0]["checkpoint_id"].clone();
                        loop_state.insert("pending_checkpoint".to_string(), waiting);
                    })
                    .await;
                    let ended = terminal_record(&fx).await;

                    settle(&fx).await;
                    settle(&fx).await;
                    let rested = revision(&fx).await;
                    settle(&fx).await;
                    assert_eq!(revision(&fx).await, rested, "{at}: at rest in two passes");

                    let state = loop_state(&fx).await;
                    let gate_now = state
                        .open_checkpoints()
                        .iter()
                        .find(|checkpoint| checkpoint.kind == gate.kind())
                        .map(|checkpoint| checkpoint.status);
                    let call_now = state
                        .effects()
                        .iter()
                        .find(|effect| effect.slot == 1 && effect.turn == 1)
                        .map(|effect| effect.status);
                    match standing {
                        Standing::Live => assert_ne!(
                            gate_now,
                            Some(AgentCheckpointStatus::Cancelled),
                            "{at}: a live run's gate is not closed for it"
                        ),
                        Standing::WindingDown if matches!(call_status, "pending" | "missing") => {
                            assert!(
                                gate_now.is_some_and(AgentCheckpointStatus::is_waiting),
                                "{at}: left as it is, got {gate_now:?}"
                            );
                        }
                        Standing::WindingDown => assert_eq!(
                            gate_now,
                            Some(AgentCheckpointStatus::Cancelled),
                            "{at}: the gate gates nothing, and is closed"
                        ),
                        Standing::Ended => {
                            assert_eq!(
                                gate_now,
                                Some(AgentCheckpointStatus::Cancelled),
                                "{at}: an ended run holds no waiting gate"
                            );
                            assert_ne!(
                                call_now,
                                Some(AgentRunEffectStatus::Pending),
                                "{at}: an ended run holds no unsent call"
                            );
                            assert_eq!(
                                terminal_record(&fx).await,
                                ended,
                                "{at}: the end does not move"
                            );
                        }
                    }
                    forged += 1;
                }
            }
        }
    }
    assert_eq!(forged, 96);
}

/// The step that closes such a gate is not a turn. The loop measures a turn
/// at each step it commits in `RecordingTurn`, and a run can be winding down
/// there: the repair's step folds no turn, so it must record no duration and
/// no tokens.
#[tokio::test]
async fn the_step_that_closes_a_gate_measures_no_turn() {
    use rakka_core::InMemoryMetricsRecorder;

    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let calls = [CHARGE, FLAKY, LOOKUP];
    let (fx, _sink) = world(&calls, policies(&calls, Some((CHARGE, Gate::Approval))));
    let fx = fx.with_metrics(metrics.clone());
    start(&fx, None).await;
    commit_the_turn(&fx).await;
    land(&fx, &call_to(&fx, FLAKY).await).await;
    reopen_the_approval_as_an_older_binary_left_it(&fx).await;
    rewrite_loop_state(&fx, |loop_state| {
        loop_state["phase"] = serde_json::json!("recording-turn");
    })
    .await;
    assert_eq!(
        loop_state(&fx).await.phase(),
        rakka_agent::AgentLoopPhase::RecordingTurn
    );
    assert!(
        loop_state(&fx).await.pending_turn().is_some(),
        "the turn is not folded"
    );
    assert_eq!(waiting(&fx).await.len(), 1, "the record as it was left");
    let count = |name: &str| metrics.snapshot().observations_named(name).len();
    let before = (
        count(rakka_agent::METRIC_AGENT_TURN_DURATION),
        count(rakka_agent::METRIC_AGENT_MODEL_TOKENS),
    );

    settle(&fx).await;

    assert_eq!(waiting(&fx).await, Vec::new(), "the pass closed the gate");
    assert_eq!(
        (
            count(rakka_agent::METRIC_AGENT_TURN_DURATION),
            count(rakka_agent::METRIC_AGENT_MODEL_TOKENS),
        ),
        before,
        "no turn was measured"
    );
}
