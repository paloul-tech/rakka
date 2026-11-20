//! What a run does when its end is decided while it still holds a call it
//! sent.
//!
//! Specification: sections 8.7 and 11.5. A run must not become terminal over
//! work whose outcome it does not know. A run that winds down waited for
//! that already. A run whose end was decided for it did not: its result
//! accepted, its proposal refused, its budget spent. It ended at once, and
//! an ended run takes no result and answers no checkpoint for most kinds of
//! call. Issues #172, #173 and #123 are what that left: a goal evaluation
//! `Ready` for good, an evaluation's reconciliation checkpoint nothing could
//! answer, and a compensation or workflow cancel that parked with no
//! checkpoint at all.
//!
//! The end now waits. The run records the reason it was given, takes the
//! results of the calls it sent and answers their parks as any live run
//! does, and ends under that reason when the last of them is known.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    agent_operational_snapshot, evaluation_operation_id, AgentCheckpoint, AgentCheckpointDecision,
    AgentCheckpointKind, AgentCompensationRef, AgentDecisionEventVocabulary, AgentEffectPolicies,
    AgentLoopState, AgentModelTurn, AgentModelUsage, AgentOperationId, AgentOperationKind,
    AgentReconciliationDecision, AgentRevisionNumber, AgentRunEffect, AgentRunEffectKind,
    AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply,
    AgentRunError, AgentRunStatus, AgentSchemaPolicy, AgentTaskContent, InMemoryAgentRunEffectSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{HumanCheckpointId, PrincipalRef};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

/// The compensation an operator schedules for a call whose outcome is
/// unknown.
const COMPENSATION: &str = "refund-charge";

fn usage() -> AgentModelUsage {
    AgentModelUsage {
        input_tokens: 8,
        output_tokens: 4,
        cost_micros: 2,
        ..Default::default()
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

/// A goal-bearing world whose turn one proposes a result and calls nothing,
/// returned with the model call out. Its decisions reach the returned sink.
async fn proposing_goal_world() -> (Fixture, Arc<CountingDecisionSink>) {
    goal_world_proposing("resolved", task_definition()).await
}

/// [`proposing_goal_world`], with the answer every turn proposes and the
/// task's definition chosen, under the goal spec every test but two uses.
/// The task's rule wants a non-empty answer, so an empty one is rejected.
async fn goal_world_proposing(
    answer: &str,
    definition: rakka_agent::AgentTaskDefinition,
) -> (Fixture, Arc<CountingDecisionSink>) {
    goal_world(answer, definition, goal_spec_with_evaluator()).await
}

/// A goal-bearing world under `spec`, whose every turn proposes `answer` and
/// calls nothing, returned with the model call out.
async fn goal_world(
    answer: &str,
    definition: rakka_agent::AgentTaskDefinition,
    spec: rakka_agent::AgentGoalSpec,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let proposing = || {
        AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
            .with_text("Done.")
            .with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                    .expect("the proposal is inline-bounded"),
            )
            .with_usage(usage())
    };
    let sink = CountingDecisionSink::new();
    let fx = Fixture::with_sink(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn_for(1, proposing())
                .with_turn_for(2, proposing()),
        ),
        InMemoryAgentRunEffectSink::new(),
        AgentEffectPolicies::default(),
        Arc::new(AtomicU64::new(1)),
    )
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        definition,
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal-bearing creation applies");
    settle(&fx).await;
    (fx, sink)
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

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// The run's status, the code of the reason it will end or has ended under,
/// and whether its result was accepted.
async fn standing(fx: &Fixture) -> (AgentRunStatus, Option<String>, bool) {
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    (
        snapshot.status,
        snapshot
            .terminal_reason
            .as_ref()
            .map(|reason| reason.code().to_string()),
        loop_state(fx).await.accepted_result().is_some(),
    )
}

/// Commands an evaluation the spec does not gate, which is sent at once.
async fn send_an_evaluation(fx: &Fixture) -> AgentRunEffect {
    apply(
        fx,
        AgentRunEntityCommand::EvaluateGoal {
            operation_id: evaluation_operation_id(&run_scope(), "1")
                .expect("the operation id derives"),
            evaluation: Box::new(goal_evaluation_request(AgentRevisionNumber::INITIAL)),
        },
    )
    .await
    .expect("the evaluation commits");
    let evaluation = evaluation_of(fx).await;
    assert_eq!(
        evaluation.status,
        AgentRunEffectStatus::Ready,
        "the ungated evaluation was sent"
    );
    evaluation
}

/// The model call the run holds.
async fn model_call_of(fx: &Fixture) -> AgentRunEffect {
    loop_state(fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::ModelCall)
        .cloned()
        .expect("the run holds its model call")
}

/// Issue #172. A run awaits its model call with a goal evaluation out. The
/// model proposes a result and calls nothing, and the task accepts. The run
/// used to end `Completed` there, holding the evaluation `Ready` for good:
/// the dispatcher cancels an ended run's ticket and the ended run refused to
/// hear so. It now waits, accepted, takes the cancellation, and ends.
#[tokio::test]
async fn an_accepted_run_waits_for_the_evaluation_it_sent() {
    let (fx, sink) = proposing_goal_world().await;
    let evaluation = send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;

    record(&fx, &model, turn).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelling,
            Some("result-accepted".to_string()),
            true
        ),
        "the result is accepted, and the run has not ended"
    );
    assert_eq!(evaluation_of(&fx).await.status, AgentRunEffectStatus::Ready);
    assert!(
        loop_state(&fx).await.proposal().is_none(),
        "the answered proposal is no longer pending"
    );
    assert_only_what_is_out_is_reserved(&fx, "while the evaluation is out").await;

    // What the dispatcher reports for a ticket of a run that is winding down.
    record(
        &fx,
        &evaluation,
        AgentRunEffectOutcome::Cancelled {
            reason: "run-cancelled".to_string(),
        },
    )
    .await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        ),
        "the run ends as its accepted result says"
    );
    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Cancelled
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// Issue #173. The evaluation's result lands unknown while the run's
/// proposal is with the task, and the task then accepts. The run used to end
/// `Completed` holding the reconciliation checkpoint, which an ended run
/// answers for no goal evaluation. It now stays parked, accepted, until a
/// person answers, and then ends.
#[tokio::test]
async fn an_accepted_run_parked_on_an_unknown_outcome_is_answered_before_it_ends() {
    let (fx, sink) = proposing_goal_world().await;
    let evaluation = send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    // The task cannot be reached, so the proposal stays with the courier.
    let nowhere = rakka_agent::AgentExchangeRouter::new();
    for (effect, outcome) in [
        (&model, turn),
        (
            &evaluation,
            AgentRunEffectOutcome::indeterminate("connection-lost", "the evaluator died"),
        ),
    ] {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.apply(
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
            &nowhere,
            fx.now(),
        )
        .await
        .expect("the result applies");
    }
    assert_eq!(status(&fx).await, AgentRunStatus::WaitingForReconciliation);
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;

    fx.pump().await.expect("the task accepts the result");

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::WaitingForReconciliation,
            Some("result-accepted".to_string()),
            true
        ),
        "the result is accepted, and the run is still parked"
    );
    assert_eq!(
        waiting(&fx)
            .await
            .iter()
            .map(|checkpoint| checkpoint.checkpoint_id.clone())
            .collect::<Vec<_>>(),
        vec![park.checkpoint_id.clone()]
    );

    decide(
        &fx,
        &park.checkpoint_id,
        "answer",
        AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::AbandonAndFail),
    )
    .await
    .expect("the parked run answers its checkpoint");

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        ),
        "the answer ends the run as its accepted result says"
    );
    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Failed
    );
    assert_eq!(waiting(&fx).await, Vec::new());
    assert_eq!(listed_open(&fx).await, 0);
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// The evaluation's own result lands after the acceptance. The waiting run
/// takes it, as a run that is winding down takes the result of any call it
/// sent, and ends. This world wires no evaluator, so the result is a
/// failure, which a run survives for a goal evaluation.
#[tokio::test]
async fn an_accepted_run_takes_the_result_of_the_evaluation_it_sent() {
    let (fx, sink) = proposing_goal_world().await;
    let evaluation = send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    record(&fx, &model, turn).await;
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);

    let result = fx.dispatcher.answer(&evaluation).await;
    let settles_as = result.resolved_status();
    assert_eq!(settles_as, AgentRunEffectStatus::Failed);
    record(&fx, &evaluation, result).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        ),
        "a failed evaluation does not take the accepted result away"
    );
    assert_eq!(evaluation_of(&fx).await.status, settles_as);
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A run that holds nothing it sent ends at once, as it always did.
#[tokio::test]
async fn an_accepted_run_that_holds_nothing_ends_at_once() {
    let (fx, sink) = proposing_goal_world().await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;

    record(&fx, &model, turn).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        )
    );
    assert!(
        loop_state(&fx).await.proposal().is_some(),
        "the ended record keeps its proposal, as it always did"
    );
    assert_at_rest(&fx, &sink).await;
}

/// A call the run committed and never sent does not hold its end. A review
/// of the goal waits behind its approval when the task accepts. The end
/// cancels the evaluation and closes the review, in the transition that
/// ends the run, and the record is the one an end at once always left.
#[tokio::test]
async fn an_accepted_run_that_holds_only_a_call_it_never_sent_ends_at_once() {
    let (fx, sink) = proposing_goal_world().await;
    let mut review = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    review.method = rakka_agent::AgentGoalEvaluationMethod::HumanReview;
    apply(
        &fx,
        AgentRunEntityCommand::EvaluateGoal {
            operation_id: evaluation_operation_id(&run_scope(), "1")
                .expect("the operation id derives"),
            evaluation: Box::new(review),
        },
    )
    .await
    .expect("the review commits");
    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Pending
    );
    held(&fx, AgentCheckpointKind::Approval).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;

    record(&fx, &model, turn).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        )
    );
    assert_eq!(
        evaluation_of(&fx).await.status,
        AgentRunEffectStatus::Cancelled
    );
    assert_eq!(waiting(&fx).await, Vec::new());
    assert!(
        loop_state(&fx).await.proposal().is_some(),
        "the ended record keeps its proposal, as it always did"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A claim append does not hold a run's end either. It is owed after the
/// end: the run ends at once with the append out, and the ended run takes
/// its result (issue #120).
#[tokio::test]
async fn an_accepted_run_does_not_wait_for_a_claim_append() {
    let space = rakka_agent::KnowledgeSpaceId::new("space-alpha").expect("the space id is valid");
    let mut spec = goal_spec_with_evaluator();
    spec.knowledge_spaces.insert(space.clone());
    let (fx, _sink) = goal_world("resolved", task_definition(), spec).await;
    apply(
        &fx,
        AgentRunEntityCommand::AppendClaim {
            operation_id: rakka_agent::claim_append_operation_id(&run_scope(), "beside-the-turn")
                .expect("the operation id derives"),
            append: Box::new(rakka_agent::AgentClaimAppendRequest {
                space,
                subject: "finding".to_string(),
                predicate: "links".to_string(),
                object: rakka_agent::AgentClaimObjectRequest::Value(
                    AgentTaskContent::inline(serde_json::json!({ "note": "observed" }))
                        .expect("the object is inline-bounded"),
                ),
                confidence_bps: 5_000,
                classification: rakka_agent::MemoryClassification::Unclassified,
                evidence: Vec::new(),
                requested_by: resolver(),
            }),
        },
    )
    .await
    .expect("the append commits");
    let append = loop_state(&fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::ClaimAppendCall)
        .cloned()
        .expect("the run holds its claim append");
    assert_eq!(append.status, AgentRunEffectStatus::Ready);
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;

    record(&fx, &model, turn).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        ),
        "the run ends with the append out"
    );
    let result = fx.dispatcher.answer(&append).await;
    record(&fx, &append, result).await;
    let landed = loop_state(&fx).await;
    assert!(
        landed.outstanding_effects().next().is_none(),
        "the ended run took the append's result"
    );
    assert_eq!(status(&fx).await, AgentRunStatus::Completed);
}

/// Answers the model call while the task cannot be reached, so the run's
/// proposal stays with the courier until a settle pass delivers it.
async fn propose_out_of_reach(fx: &Fixture) {
    let model = model_call_of(fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    let nowhere = rakka_agent::AgentExchangeRouter::new();
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: model
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: model.effect_id.clone(),
            generation: model.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(turn),
        },
        &nowhere,
        fx.now(),
    )
    .await
    .expect("the model's answer applies");
    assert!(
        loop_state(fx).await.proposal().is_some(),
        "the proposal is with the courier"
    );
}

/// Applies `command` to the run while the task cannot be reached, so a
/// proposal the run holds stays with the courier.
async fn apply_out_of_reach(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let nowhere = rakka_agent::AgentExchangeRouter::new();
    let mut run = fx.run();
    run.recover(fx.now()).await?;
    run.apply(command, &nowhere, fx.now()).await
}

/// The result of `effect` with an unknown outcome.
fn unknown(effect: &AgentRunEffect) -> AgentRunEntityCommand {
    AgentRunEntityCommand::RecordEffectResult {
        operation_id: effect
            .result_operation_id(&run_scope())
            .expect("the result operation id derives"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        attempt: 1,
        fence: 1,
        outcome: Box::new(AgentRunEffectOutcome::indeterminate(
            "connection-lost",
            "the worker died mid-invocation",
        )),
    }
}

/// What the dispatcher reports for the ticket of a run that is winding
/// down: the call was never invoked.
fn never_invoked() -> AgentRunEffectOutcome {
    AgentRunEffectOutcome::Cancelled {
        reason: "run-cancelled".to_string(),
    }
}

/// Cancels the run, as an operator does.
async fn cancel_the_run(fx: &Fixture) {
    apply(
        fx,
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
}

/// Issue #123, as filed. A run is winding down for a compensation it has
/// sent when the task accepts its result. Acceptance wins the race: the
/// accepted result replaces the wind-down's reason. The run used to end
/// there, with the compensation in flight. It now waits for it, and ends as
/// accepted when the compensation is known.
#[tokio::test]
async fn an_accepted_run_waits_for_the_compensation_it_sent() {
    let (fx, sink) = proposing_goal_world().await;
    let evaluation = send_an_evaluation(&fx).await;
    propose_out_of_reach(&fx).await;
    // The evaluation ends unknown and a person answers with a compensation,
    // both while the task cannot be reached.
    apply_out_of_reach(&fx, unknown(&evaluation))
        .await
        .expect("the unknown outcome applies");
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    apply_out_of_reach(
        &fx,
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: decision_key("compensate"),
            checkpoint_id: park.checkpoint_id.clone(),
            resolver: resolver(),
            decision: Box::new(compensate()),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await
    .expect("the compensation is scheduled");
    let compensation = loop_state(&fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::CompensationCall)
        .cloned()
        .expect("the run holds its compensation");
    assert_eq!(compensation.status, AgentRunEffectStatus::Ready);
    assert_eq!(
        (
            standing(&fx).await,
            loop_state(&fx).await.proposal().is_some()
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("effect-compensated".to_string()),
                false
            ),
            true
        ),
        "winding down for the compensation, its proposal unanswered"
    );

    settle(&fx).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelling,
            Some("result-accepted".to_string()),
            true
        ),
        "the accepted result replaces the wind-down's reason, and the run waits"
    );

    let result = fx.dispatcher.answer(&compensation).await;
    record(&fx, &compensation, result).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        )
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A run that is cancelling holds a proposal and a call it sent. The task
/// rejects the proposal. A cancelling run takes no further turn, and it used
/// to end there, with the call still out. It now goes on winding down,
/// without the proposal the task has answered, and ends as cancelled when
/// the call is known.
#[tokio::test]
async fn a_cancelling_run_whose_proposal_is_rejected_goes_on_winding_down() {
    let (fx, sink) = goal_world_proposing("", task_definition()).await;
    let evaluation = send_an_evaluation(&fx).await;
    propose_out_of_reach(&fx).await;
    cancel_the_run(&fx).await;

    settle(&fx).await;

    let state = loop_state(&fx).await;
    assert_eq!(
        (
            standing(&fx).await,
            state.proposal().is_some(),
            state.turn()
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("cancellation-requested".to_string()),
                false
            ),
            false,
            1
        ),
        "the rejection is taken, no turn begins, and the run still waits"
    );
    assert_eq!(evaluation_of(&fx).await.status, AgentRunEffectStatus::Ready);

    record(&fx, &evaluation, never_invoked()).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelled,
            Some("cancellation-requested".to_string()),
            false
        )
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// The same, with the run parked. A run whose evaluation ended unknown is
/// cancelled: it is `WaitingForReconciliation`, and it is winding down. The
/// rejection was read against the status alone, found no `Cancelling`, and
/// began a new turn on the cancelled run, whose model call was then never
/// sent. It is taken as on any run that is winding down: no turn begins,
/// and the run ends as cancelled once the park is answered.
#[tokio::test]
async fn a_parked_cancelled_run_whose_proposal_is_rejected_begins_no_turn() {
    let (fx, sink) = goal_world_proposing("", task_definition()).await;
    let evaluation = send_an_evaluation(&fx).await;
    propose_out_of_reach(&fx).await;
    // The evaluation ends unknown and the run is cancelled, both while the
    // task cannot be reached, so the proposal is unanswered through both.
    let nowhere = rakka_agent::AgentExchangeRouter::new();
    for command in [
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: evaluation
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: evaluation.effect_id.clone(),
            generation: evaluation.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(AgentRunEffectOutcome::indeterminate(
                "connection-lost",
                "the evaluator died",
            )),
        },
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "cancel-1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
    ] {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.apply(command, &nowhere, fx.now())
            .await
            .expect("the command applies");
    }
    let parked = loop_state(&fx).await;
    assert_eq!(
        (standing(&fx).await, parked.proposal().is_some()),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                Some("cancellation-requested".to_string()),
                false
            ),
            true
        ),
        "cancelled, parked, and its proposal unanswered"
    );
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;

    settle(&fx).await;

    let state = loop_state(&fx).await;
    assert_eq!(
        (
            standing(&fx).await,
            state.proposal().is_some(),
            state.turn(),
            state.outstanding_effects().count()
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                Some("cancellation-requested".to_string()),
                false
            ),
            false,
            1,
            0
        ),
        "the rejection is taken, and no turn begins on the cancelled run"
    );

    decide(
        &fx,
        &park.checkpoint_id,
        "answer",
        AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::AbandonAndFail),
    )
    .await
    .expect("the parked run answers its checkpoint");

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelled,
            Some("cancellation-requested".to_string()),
            false
        )
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// The task's last rejection ends a run. One that still holds a call it sent
/// waits for it, and then ends as the rejection says.
#[tokio::test]
async fn a_run_out_of_rejections_waits_for_the_evaluation_it_sent() {
    let definition = task_definition()
        .with_limits(rakka_agent::AgentTaskLimits::new().with_max_result_rejections(1));
    let (fx, sink) = goal_world_proposing("", definition).await;
    let evaluation = send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;

    record(&fx, &model, turn).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelling,
            Some("result-rejections-exhausted".to_string()),
            false
        )
    );

    record(&fx, &evaluation, never_invoked()).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Failed,
            Some("result-rejections-exhausted".to_string()),
            false
        )
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A budget that cannot be topped up ends a run. Here the next turn would
/// put a second call out beside the evaluation, past the ceiling. The run
/// waits for the evaluation it sent, and then ends as the budget says.
#[tokio::test]
async fn a_run_out_of_budget_waits_for_the_evaluation_it_sent() {
    let definition = task_definition().with_budgets(rakka_agent::AgentBudgetCeilings {
        max_concurrent_effects: Some(1),
        ..rakka_agent::AgentBudgetCeilings::unbounded()
    });
    let (fx, sink) = goal_world_proposing("", definition).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    let nowhere = rakka_agent::AgentExchangeRouter::new();
    {
        // The model answers while the task cannot be reached, so the
        // evaluation below is the only call out when the rejection lands.
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.apply(
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: model
                    .result_operation_id(&run_scope())
                    .expect("the result operation id derives"),
                effect_id: model.effect_id.clone(),
                generation: model.generation,
                attempt: 1,
                fence: 1,
                outcome: Box::new(turn),
            },
            &nowhere,
            fx.now(),
        )
        .await
        .expect("the model's answer applies");
    }
    let evaluation = send_an_evaluation(&fx).await;

    settle(&fx).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelling,
            Some("budget-exhausted".to_string()),
            false
        ),
        "the rejection began a turn the budget refused, and the run waits"
    );
    assert_eq!(evaluation_of(&fx).await.status, AgentRunEffectStatus::Ready);

    record(&fx, &evaluation, never_invoked()).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Failed,
            Some("budget-exhausted".to_string()),
            false
        )
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// Nothing is fed to the run by hand here. The dispatcher finds the ticket of
/// a run that is winding down, settles it, and tells the run, which ends.
#[tokio::test]
async fn the_dispatcher_settles_what_an_accepted_run_waits_for() {
    let (fx, sink) = proposing_goal_world().await;
    send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    record(&fx, &model, turn).await;
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);

    fx.pump().await.expect("the pass runs");

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        )
    );
    assert!(
        !evaluation_of(&fx).await.is_outstanding(),
        "the evaluation is settled"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

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

/// Mixed builds. An older build can load a run this build left waiting. It
/// takes the call's result, and then ends the run only when the run's
/// children have reported too: its wind-down waits for them, and a decided
/// end does not. Where a child is still out it leaves the record
/// `Cancelling` with no call out, and this build's next settle pass ends
/// it. This test builds that record without the child, which the repair
/// does not read.
#[tokio::test]
async fn a_waiting_run_an_older_build_settled_is_ended_by_the_next_settle_pass() {
    let (fx, sink) = proposing_goal_world().await;
    send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    record(&fx, &model, turn).await;
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);
    // What the older build's result transition leaves: the call settled and
    // its attempts released, the run still `Cancelling`.
    let released = evaluation_of(&fx).await.max_attempts;
    rewrite_loop_state(&fx, |loop_state| {
        let mut settled = 0;
        for effect in loop_state["effects"]
            .as_array_mut()
            .expect("the effects are an array")
        {
            if effect["status"].as_str() == Some("ready") {
                effect["status"] = serde_json::json!("cancelled");
                settled += 1;
            }
        }
        assert_eq!(settled, 1, "the record holds one call that is out");
        let reserved = &mut loop_state["budget"]["reserved_attempts"];
        let held = reserved.as_u64().expect("the reservation is a number");
        *reserved = serde_json::json!(held - u64::from(released));
    })
    .await;
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);
    assert_only_what_is_out_is_reserved(&fx, "as the older build left it").await;

    settle(&fx).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        )
    );
    assert_at_rest(&fx, &sink).await;
}

/// The task's cancellation is requested while the run's proposal is on its
/// way. The task refuses the proposal for that reason, and the run takes it
/// as its own cancellation. The refused proposal stayed on the record, and
/// a wind-down waits for a proposal's answer: the run stayed `Cancelling`
/// for good, and the task with it. The answered proposal no longer holds
/// the wind-down.
#[tokio::test]
async fn a_proposal_refused_for_the_tasks_cancellation_does_not_hold_the_wind_down() {
    let (fx, _sink) = proposing_goal_world().await;
    propose_out_of_reach(&fx).await;
    cancel_the_task(&fx).await;

    settle(&fx).await;
    fx.pump().await.expect("the pass runs");

    let state = loop_state(&fx).await;
    assert_eq!(
        (status(&fx).await, state.proposal().is_some()),
        (AgentRunStatus::Cancelled, false),
        "the run ends cancelled, with no proposal pending"
    );
    assert!(
        fx.task_snapshot().await.status.is_terminal(),
        "and the task's cancellation completes"
    );
}

/// A parked claim append does not hold a decided end, and does not decide
/// what the waiting run reports. The run waits for the evaluation it sent,
/// `Cancelling`, and ends when that is known. The claim append's park stays
/// on the ended run with its checkpoint, which the ended run answers
/// (issue #120).
#[tokio::test]
async fn a_parked_claim_append_neither_holds_the_end_nor_parks_the_wait() {
    let space = rakka_agent::KnowledgeSpaceId::new("space-alpha").expect("the space id is valid");
    let mut spec = goal_spec_with_evaluator();
    spec.knowledge_spaces.insert(space.clone());
    let (fx, _sink) = goal_world("resolved", task_definition(), spec).await;
    apply(
        &fx,
        AgentRunEntityCommand::AppendClaim {
            operation_id: rakka_agent::claim_append_operation_id(&run_scope(), "beside-the-turn")
                .expect("the operation id derives"),
            append: Box::new(rakka_agent::AgentClaimAppendRequest {
                space,
                subject: "finding".to_string(),
                predicate: "links".to_string(),
                object: rakka_agent::AgentClaimObjectRequest::Value(
                    AgentTaskContent::inline(serde_json::json!({ "note": "observed" }))
                        .expect("the object is inline-bounded"),
                ),
                confidence_bps: 5_000,
                classification: rakka_agent::MemoryClassification::Unclassified,
                evidence: Vec::new(),
                requested_by: resolver(),
            }),
        },
    )
    .await
    .expect("the append commits");
    let append = loop_state(&fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::ClaimAppendCall)
        .cloned()
        .expect("the run holds its claim append");
    let evaluation = send_an_evaluation(&fx).await;
    propose_out_of_reach(&fx).await;
    apply_out_of_reach(&fx, unknown(&append))
        .await
        .expect("the append's unknown outcome applies");
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    assert_eq!(park.bound_effect.effect_id, append.effect_id);

    settle(&fx).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelling,
            Some("result-accepted".to_string()),
            true
        ),
        "the wait is for the evaluation, which is not parked"
    );

    record(&fx, &evaluation, never_invoked()).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        ),
        "the run ends over the parked append"
    );
    assert_eq!(
        waiting(&fx)
            .await
            .iter()
            .map(|checkpoint| checkpoint.checkpoint_id.clone())
            .collect::<Vec<_>>(),
        vec![park.checkpoint_id],
        "whose checkpoint the ended run still holds, to answer"
    );
}

/// Requests the task's cancellation, as an operator does.
async fn cancel_the_task(fx: &Fixture) {
    fx.apply_task_command(rakka_agent::AgentTaskEntityCommand::Cancel {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Cancellation,
            [TENANT, task_scope().task().as_str(), "root-cancel"],
        )
        .expect("the operation id derives"),
        reason: "operator".to_string(),
    })
    .await
    .expect("the task's cancellation is requested");
}

/// A record an earlier build left stuck is repaired. That build took the
/// task's refusal of a proposal as the run's cancellation and left the
/// refused proposal on the record, so its wind-down waited for an answer
/// that had already come, for good, and the task's cancellation with it.
/// The run's own journal says the proposal's exchange is no longer owed.
/// The settle pass reads that, drops the proposal, and the wind-down ends.
///
/// The record is built with this build, which drops the proposal as it
/// takes the refusal, and the proposal is then put back as the earlier
/// build left it. An evaluation is out through the refusal, so the run is
/// still winding down when the proposal is put back.
#[tokio::test]
async fn a_wind_down_an_earlier_build_left_waiting_for_a_refused_proposal_ends() {
    let (fx, sink) = proposing_goal_world().await;
    let evaluation = send_an_evaluation(&fx).await;
    propose_out_of_reach(&fx).await;
    let refused = serde_json::to_value(
        loop_state(&fx)
            .await
            .proposal()
            .expect("the proposal is on the record"),
    )
    .expect("the proposal serializes");
    cancel_the_task(&fx).await;
    settle(&fx).await;
    assert_eq!(
        (
            standing(&fx).await,
            loop_state(&fx).await.proposal().is_some()
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("cancellation-requested".to_string()),
                false
            ),
            false
        ),
        "this build took the refusal, dropped the proposal, and waits for its evaluation"
    );
    rewrite_loop_state(&fx, |loop_state| {
        loop_state.insert("proposal".to_string(), refused);
    })
    .await;
    assert!(
        loop_state(&fx).await.proposal().is_some(),
        "the record is as the earlier build left it"
    );

    record(&fx, &evaluation, never_invoked()).await;
    settle(&fx).await;

    assert_eq!(
        (
            standing(&fx).await,
            loop_state(&fx).await.proposal().is_some()
        ),
        (
            (
                AgentRunStatus::Cancelled,
                Some("cancellation-requested".to_string()),
                false
            ),
            false
        ),
        "the answered proposal is dropped, and the wind-down ends"
    );
    fx.pump().await.expect("the pass runs");
    assert!(
        fx.task_snapshot().await.status.is_terminal(),
        "and the task's cancellation completes"
    );
    assert_at_rest(&fx, &sink).await;
}

/// The repair reads the journal, and not the wind-down alone. A cancelled
/// run whose proposal the task has not answered goes on waiting for the
/// answer, through settle passes that cannot reach the task. When the task
/// does answer, an acceptance still wins the race with the cancel.
#[tokio::test]
async fn a_wind_down_still_waits_for_a_proposal_the_task_has_not_answered() {
    let (fx, sink) = proposing_goal_world().await;
    propose_out_of_reach(&fx).await;
    apply_out_of_reach(
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
    let nowhere = rakka_agent::AgentExchangeRouter::new();
    for _ in 0..2 {
        let mut run = fx.run();
        let now = fx.now();
        run.recover(now).await.expect("the run recovers");
        run.settle_side_effects(&nowhere, now)
            .await
            .expect("the run settles");
    }
    assert_eq!(
        (
            standing(&fx).await,
            loop_state(&fx).await.proposal().is_some()
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("cancellation-requested".to_string()),
                false
            ),
            true
        ),
        "cancelled, and still waiting for the proposal's answer"
    );

    settle(&fx).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string()),
            true
        ),
        "the task accepts, and acceptance wins the race with the cancel"
    );
    assert_at_rest(&fx, &sink).await;
}

/// Rewrites the stored run record's run through `edit`, as JSON.
async fn rewrite_run(
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
    edit(value["run"].as_object_mut().expect("the run is an object"));
    let rewritten = serde_json::from_value(value).expect("the rewritten record deserializes");
    fx.runs
        .compare_and_set(&id, record.revision, rewritten)
        .await
        .expect("the rewritten record persists");
}

/// The model call of `turn`, if the run recorded one.
async fn model_call_of_turn(fx: &Fixture, turn: u64) -> Option<AgentRunEffect> {
    loop_state(fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::ModelCall && effect.turn == turn)
        .cloned()
}

/// A cancelled run parked on an unknown outcome, on which an earlier build
/// began a new turn, returned with its park. That build read the task's
/// rejection of the run's proposal against the status alone, and a parked
/// run is `WaitingForReconciliation`, not `Cancelling`.
///
/// The record is built with this build, which takes the rejection and begins
/// no turn. The two writes of the earlier build's arm are then made in its
/// place: a turn begun with the rejection's feedback, and `Running`. The
/// next settle pass commits the new turn's model call, as the earlier
/// build's did, by code the two share. Run under that build's sources, the
/// same world leaves the same record but for two clock values.
async fn a_turn_begun_on_a_cancelled_run() -> (Fixture, Arc<CountingDecisionSink>, AgentCheckpoint)
{
    let (fx, sink) = goal_world_proposing("", task_definition()).await;
    let evaluation = send_an_evaluation(&fx).await;
    propose_out_of_reach(&fx).await;
    apply_out_of_reach(&fx, unknown(&evaluation))
        .await
        .expect("the unknown outcome applies");
    apply_out_of_reach(
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
    let park = held(&fx, AgentCheckpointKind::IndeterminateEffectReconciliation).await;
    settle(&fx).await;
    assert_eq!(
        (
            standing(&fx).await,
            loop_state(&fx).await.turn(),
            loop_state(&fx).await.proposal().is_some()
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                Some("cancellation-requested".to_string()),
                false
            ),
            1,
            false
        ),
        "this build took the rejection and began no turn"
    );

    rewrite_run(&fx, |run| {
        run.insert("status".to_string(), serde_json::json!("running"));
        let loop_state = run["loop_state"]
            .as_object_mut()
            .expect("the loop state is an object");
        loop_state.insert("turn".to_string(), serde_json::json!(2));
        loop_state.insert("next_slot".to_string(), serde_json::json!(0));
        loop_state.insert("phase".to_string(), serde_json::json!("preparing-context"));
        loop_state.insert("context_snapshot".to_string(), serde_json::Value::Null);
        loop_state.insert("proposal".to_string(), serde_json::Value::Null);
        loop_state.insert(
            "feedback".to_string(),
            serde_json::json!("empty-string-field: /answer must be a non-empty string"),
        );
    })
    .await;
    (fx, sink, park)
}

/// Takes the record of [`a_turn_begun_on_a_cancelled_run`] on to where the
/// earlier build's next step left it, and answers the model call of the new
/// turn: committed, and never sent.
///
/// This build takes no step on a run that is winding down, so the run's
/// reason is set aside for the one step that commits the call, and put
/// back. A run with no reason also hands the call to the sink, which the
/// earlier build's wind-down never did, so the call is put back as unsent
/// too. The sink of this world then holds a ticket the record does not
/// know of; nothing here claims it. Run under the earlier build's sources,
/// the same world leaves the same record but for two clock values.
async fn the_model_call_that_turn_never_sent(fx: &Fixture) -> AgentRunEffect {
    let mut reason = None;
    rewrite_run(fx, |run| {
        reason = run.insert("terminal_reason".to_string(), serde_json::Value::Null);
    })
    .await;
    settle(fx).await;
    let call = model_call_of_turn(fx, 2)
        .await
        .expect("the step committed the new turn's model call");
    rewrite_run(fx, |run| {
        run.insert(
            "terminal_reason".to_string(),
            reason.expect("the run recorded its reason"),
        );
        let unsent = run["loop_state"]["effects"]
            .as_array_mut()
            .expect("the effects are an array")
            .iter_mut()
            .find(|effect| effect["effect_id"].as_str() == Some(call.effect_id.as_str()))
            .expect("the record holds the call");
        unsent["status"] = serde_json::json!("pending");
        unsent["dispatched_at"] = serde_json::Value::Null;
    })
    .await;
    let call = model_call_of_turn(fx, 2)
        .await
        .expect("the record holds the call");
    assert_eq!(
        (
            standing(fx).await,
            call.status,
            call.dispatched_at,
            loop_state(fx).await.budget().reserved_attempts()
        ),
        (
            (
                AgentRunStatus::WaitingForEffect,
                Some("cancellation-requested".to_string()),
                false
            ),
            AgentRunEffectStatus::Pending,
            None,
            u64::from(call.max_attempts)
        ),
        "the record as the earlier build left it: cancelled, and waiting for a call it never sent"
    );
    call
}

/// A turn an earlier build began on a cancelled run goes no further. The
/// record is caught between that build's two writes: the turn is begun and
/// the run is `Running`, and the step that would commit the turn's model
/// call has not run. A run that is winding down takes no step of the loop.
/// The settle pass puts it back on its wind-down's rest: no call is
/// committed, nothing is asked of its budget, and it ends as cancelled once
/// its park is answered.
#[tokio::test]
async fn a_turn_an_earlier_build_began_on_a_cancelled_run_goes_no_further() {
    let (fx, sink, park) = a_turn_begun_on_a_cancelled_run().await;
    let budget = serde_json::to_value(loop_state(&fx).await.budget()).expect("the budget encodes");

    settle(&fx).await;

    let state = loop_state(&fx).await;
    assert_eq!(
        (
            standing(&fx).await,
            state.turn(),
            model_call_of_turn(&fx, 2).await.map(|call| call.status),
            state.pending_top_up().is_some()
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                Some("cancellation-requested".to_string()),
                false
            ),
            2,
            None,
            false
        ),
        "the run rests on its park, and the turn that was begun commits nothing"
    );
    assert_eq!(
        serde_json::to_value(state.budget()).expect("the budget encodes"),
        budget,
        "nothing is charged to a run that takes no step"
    );
    let rested = revision(&fx).await;
    settle(&fx).await;
    assert_eq!(revision(&fx).await, rested, "a further pass writes nothing");

    decide(
        &fx,
        &park.checkpoint_id,
        "answer",
        AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::AbandonAndFail),
    )
    .await
    .expect("the parked run answers its checkpoint");

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelled,
            Some("cancellation-requested".to_string()),
            false
        ),
        "and it ends as cancelled once the park is answered"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// A second record an earlier build left stuck is repaired. The build began
/// a new turn on a cancelled run
/// ([`a_turn_begun_on_a_cancelled_run`]), and its next step committed that
/// turn's model call ([`the_model_call_that_turn_never_sent`]). A run that
/// is winding down sends no such call, and no fence ran after it, so the
/// call stayed unsent for good and the run could not end.
///
/// The settle pass runs the fence the wind-down owed. The call is cancelled
/// and gives back the attempts it reserved, the run rests on its park, and
/// it ends as cancelled once the park is answered.
#[tokio::test]
async fn a_cancelled_run_an_earlier_build_began_a_turn_on_is_fenced_and_ends() {
    let (fx, sink, park) = a_turn_begun_on_a_cancelled_run().await;
    the_model_call_that_turn_never_sent(&fx).await;

    settle(&fx).await;

    let call = model_call_of_turn(&fx, 2)
        .await
        .expect("the record holds the call");
    assert_eq!(
        (
            standing(&fx).await,
            call.status,
            call.dispatched_at,
            loop_state(&fx).await.outstanding_effects().count()
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                Some("cancellation-requested".to_string()),
                false
            ),
            AgentRunEffectStatus::Cancelled,
            None,
            0
        ),
        "the call the wind-down never sends is cancelled, and the run rests on its park"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the fence").await;
    let fenced = revision(&fx).await;
    settle(&fx).await;
    assert_eq!(
        revision(&fx).await,
        fenced,
        "the fence runs once: a further pass writes nothing"
    );

    decide(
        &fx,
        &park.checkpoint_id,
        "answer",
        AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::AbandonAndFail),
    )
    .await
    .expect("the parked run answers its checkpoint");

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelled,
            Some("cancellation-requested".to_string()),
            false
        ),
        "and it ends as cancelled once the park is answered"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// The same record with its park answered while it was stuck. Under the
/// earlier build that left the run `Cancelling` for good, waiting for the
/// call it would never send. The park's answer takes the record there in
/// the command's own transition, and the pass that follows the command
/// fences the call. That step ends the run, and it owes what any end owes:
/// the run settles its budget with its task.
#[tokio::test]
async fn a_stuck_cancelled_run_whose_park_was_answered_is_ended_by_the_fence() {
    let (fx, sink, park) = a_turn_begun_on_a_cancelled_run().await;
    the_model_call_that_turn_never_sent(&fx).await;

    decide(
        &fx,
        &park.checkpoint_id,
        "answer",
        AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::AbandonAndFail),
    )
    .await
    .expect("the stuck run answers its checkpoint");

    assert_eq!(
        (
            standing(&fx).await,
            model_call_of_turn(&fx, 2).await.map(|call| call.status)
        ),
        (
            (
                AgentRunStatus::Cancelled,
                Some("cancellation-requested".to_string()),
                false
            ),
            Some(AgentRunEffectStatus::Cancelled)
        ),
        "the call is cancelled and the run ends, in the pass that follows the answer"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    let ended =
        rakka_agent::load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .expect("the run state exists");
    assert_eq!(
        ended.run().expect("the run exists").settlement,
        rakka_agent::AgentRunSettlementStatus::Returned,
        "the run that ended in the repair's step settled its budget with its task"
    );
    assert_at_rest(&fx, &sink).await;
}

/// A transport that will not carry a top-up request, so a run stays parked
/// on one until a pass uses the fixture's own router.
struct HoldTopUps(Arc<dyn rakka_agent::AgentExchangeTransport>);

impl rakka_agent::AgentExchangeTransport for HoldTopUps {
    fn deliver<'a>(
        &'a self,
        envelope: &'a rakka_agent::AgentExchangeEnvelope,
    ) -> rakka_agent::AgentExchangeDeliveryFuture<'a> {
        if envelope.kind() == rakka_agent::AgentExchangeKind::BudgetAllocation {
            return Box::pin(async {
                Err(rakka_agent::AgentExchangeDeliveryError::new(
                    "held",
                    "the top-up request is held back",
                ))
            });
        }
        self.0.deliver(envelope)
    }
}

/// A top-up granted to a cancelled run does not resume it. The run is
/// parked on a top-up request when it is cancelled, and holds an evaluation
/// it sent, so it has not ended when the task's grant arrives.
///
/// The grant used to set the run `Running`, cancelled or not. The loop then
/// took another step on the cancelled run and committed its next turn's
/// model call, which a run that is winding down never sends: the record of
/// [`the_model_call_that_turn_never_sent`], written through commands alone.
/// The grant now clears the park and moves nothing else. No turn is taken,
/// nothing more is charged, and the run ends as cancelled when the
/// evaluation is settled. The review of the repair found this.
#[tokio::test]
async fn a_top_up_granted_to_a_cancelled_run_does_not_resume_it() {
    let definition = task_definition()
        .with_budgets(rakka_agent::AgentBudgetCeilings {
            max_loop_iterations: Some(4),
            ..rakka_agent::AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(rakka_agent::AgentBudgetAllocation {
            loop_iterations: Some(1),
            ..rakka_agent::AgentBudgetAllocation::unbounded()
        });
    let (fx, sink) = goal_world("", definition, goal_spec_with_evaluator()).await;
    let holding = rakka_agent::AgentExchangeRouter::new()
        .with_route(
            rakka_agent::AgentEntityClass::Task,
            Arc::new(HoldTopUps(Arc::new(fx.task_transport.clone()))),
        )
        .with_route(
            rakka_agent::AgentEntityClass::Run,
            Arc::new(fx.run_transport.clone()),
        );
    let evaluation = send_an_evaluation(&fx).await;

    // The task rejects the empty answer, the run begins its second turn,
    // and preparing it finds the one iteration it was given spent. The run
    // is then cancelled with its request for more still on its way.
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    for command in [
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: model
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: model.effect_id.clone(),
            generation: model.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(turn),
        },
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "cancel-1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
    ] {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.apply(command, &holding, fx.now())
            .await
            .expect("the command applies");
    }
    let parked = loop_state(&fx).await;
    assert_eq!(
        (
            standing(&fx).await,
            parked.turn(),
            parked.pending_top_up().is_some()
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("cancellation-requested".to_string()),
                false
            ),
            2,
            true
        ),
        "cancelled, waiting for its evaluation, and still parked on its top-up"
    );
    let spent = fx.run_snapshot().await.expect("the run exists").budget;
    let spent = (spent.loop_iterations(), spent.model_calls());

    settle(&fx).await;

    let state = loop_state(&fx).await;
    let after = fx.run_snapshot().await.expect("the run exists").budget;
    assert_eq!(
        (
            standing(&fx).await,
            state.pending_top_up().is_some(),
            model_call_of_turn(&fx, 2).await.map(|call| call.status),
            (after.loop_iterations(), after.model_calls())
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("cancellation-requested".to_string()),
                false
            ),
            false,
            None,
            spent
        ),
        "the grant clears the park, and the cancelled run takes no turn"
    );

    record(&fx, &evaluation, never_invoked()).await;

    assert_eq!(
        standing(&fx).await,
        (
            AgentRunStatus::Cancelled,
            Some("cancellation-requested".to_string()),
            false
        ),
        "and it ends as cancelled when the evaluation is settled"
    );
    assert_only_what_is_out_is_reserved(&fx, "after the end").await;
    assert_at_rest(&fx, &sink).await;
}

/// The repair asks only about a call the fence cancels. A run that is
/// winding down can hold a call that was never sent and that the fence
/// exempts: here a claim append that waits behind its approval, on a run
/// that waits for the evaluation it sent. That record is at rest. The
/// append is left alone, and outlives the run's end with its gate still
/// waiting, for the ended run to answer (issue #120).
#[tokio::test]
async fn a_waiting_run_that_holds_a_gated_claim_append_is_at_rest() {
    async fn append_status(fx: &Fixture) -> AgentRunEffectStatus {
        loop_state(fx)
            .await
            .effects()
            .iter()
            .find(|effect| effect.kind() == AgentRunEffectKind::ClaimAppendCall)
            .map(|effect| effect.status)
            .expect("the run holds its claim append")
    }

    let space = rakka_agent::KnowledgeSpaceId::new("space-alpha").expect("the space id is valid");
    let mut spec = goal_spec_with_evaluator();
    spec.knowledge_spaces.insert(space.clone());
    let (mut fx, sink) = goal_world("resolved", task_definition(), spec).await;
    fx.policies = AgentEffectPolicies::default()
        .with_claim_append_spec(
            rakka_agent::AgentEffectSpec::idempotent(3)
                .expect("the spec is valid")
                .with_checkpoint_required(),
        )
        .expect("the policies are valid");
    apply(
        &fx,
        AgentRunEntityCommand::AppendClaim {
            operation_id: rakka_agent::claim_append_operation_id(&run_scope(), "beside-the-turn")
                .expect("the operation id derives"),
            append: Box::new(rakka_agent::AgentClaimAppendRequest {
                space,
                subject: "finding".to_string(),
                predicate: "links".to_string(),
                object: rakka_agent::AgentClaimObjectRequest::Value(
                    AgentTaskContent::inline(serde_json::json!({ "note": "observed" }))
                        .expect("the object is inline-bounded"),
                ),
                confidence_bps: 5_000,
                classification: rakka_agent::MemoryClassification::Unclassified,
                evidence: Vec::new(),
                requested_by: resolver(),
            }),
        },
    )
    .await
    .expect("the append commits");
    let evaluation = send_an_evaluation(&fx).await;
    let model = model_call_of(&fx).await;
    let turn = fx.dispatcher.answer(&model).await;
    record(&fx, &model, turn).await;
    assert_eq!(
        (
            standing(&fx).await,
            append_status(&fx).await,
            waiting(&fx).await.len()
        ),
        (
            (
                AgentRunStatus::Cancelling,
                Some("result-accepted".to_string()),
                true
            ),
            AgentRunEffectStatus::Pending,
            1
        ),
        "accepted, waiting for its evaluation, and holding the append behind its gate"
    );

    let rested = revision(&fx).await;
    settle(&fx).await;
    settle(&fx).await;
    assert_eq!(
        revision(&fx).await,
        rested,
        "the waiting record is at rest: no pass writes it"
    );

    record(&fx, &evaluation, never_invoked()).await;

    assert_eq!(
        (
            standing(&fx).await,
            append_status(&fx).await,
            waiting(&fx).await.len(),
            listed_open(&fx).await
        ),
        (
            (
                AgentRunStatus::Completed,
                Some("result-accepted".to_string()),
                true
            ),
            AgentRunEffectStatus::Pending,
            1,
            1
        ),
        "the run ends, and the append waits on behind its gate"
    );
    assert_at_rest(&fx, &sink).await;
}
