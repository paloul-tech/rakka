//! A turn waits for every call of its own, one whose outcome is unknown
//! included (issue #189).
//!
//! A turn makes two tool calls. The first ends with an unknown outcome and
//! the run parks for a person to say what the call did. The second call's
//! result then arrives. It is the last call of the turn still out, and it
//! used to rest the turn, with the first call unknown. Answered "never
//! executed", the first call was sent again, and the run did not wait for
//! it: it recorded the turn without that call's result and sent the next
//! turn's model call.
//!
//! The turn now stays open until the unknown call is answered and, where the
//! answer sends it again, until that call returns.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    AgentCheckpointDecision, AgentCheckpointKind, AgentEffectPolicies, AgentEffectResolution,
    AgentEffectSpec, AgentLoopPhase, AgentLoopState, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentReconciliationDecision, AgentRunEffect, AgentRunEffectOutcome,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply, AgentRunMemory,
    AgentRunStatus, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    MemoryEntryRole, SessionMemoryCursor, SessionMemoryStore, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{HumanCheckpointId, PrincipalRef};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

/// The call that ends unknown: a charge, which is not safe to repeat blind.
const CHARGE: &str = "charge-card";
/// The call beside it, whose result arrives while the charge is unknown.
const LOOKUP: &str = "look-up";

fn tool(name: &str) -> AgentToolId {
    AgentToolId::new(name).expect("the tool id is valid")
}

fn call(call_id: &str, name: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call_id).expect("the call id is valid"),
        tool(name),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the tool call is bounded")
}

/// A world whose first turn makes the two calls and whose second proposes a
/// result, with its session memory kept so the recorded turns can be read.
fn world() -> (Fixture, Arc<InMemorySessionMemoryStore>) {
    let (fixture, session, _) = world_measured();
    (fixture, session)
}

/// [`world`], with the recorder its run's metrics go to.
fn world_measured() -> (
    Fixture,
    Arc<InMemorySessionMemoryStore>,
    Arc<rakka_core::InMemoryMetricsRecorder>,
) {
    let content = |value: serde_json::Value| {
        AgentTaskContent::inline(value).expect("the content is inline-bounded")
    };
    let dispatcher = ScriptedDispatcher::new()
        .with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Charging, and looking the order up.")
                .with_tool_call(call("call-charge", CHARGE))
                .with_tool_call(call("call-lookup", LOOKUP)),
        )
        .with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Done.")
                .with_proposal(content(serde_json::json!({ "answer": "charged" }))),
        )
        .with_tool_result(CHARGE, content(serde_json::json!({ "charged": true })))
        .with_tool_result(LOOKUP, content(serde_json::json!({ "order": "o-7" })));
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(tool(CHARGE), AgentEffectSpec::non_idempotent())
        .expect("the tool spec is valid");
    let session = Arc::new(InMemorySessionMemoryStore::new());
    let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());
    let fixture = Fixture::with_sink(
        dispatcher,
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
    .with_memory(AgentRunMemory::new(
        session.clone(),
        Arc::new(InMemoryContextSnapshotStore::new()),
    ))
    .with_metrics(metrics.clone());
    (fixture, session, metrics)
}

async fn apply(fx: &Fixture, command: AgentRunEntityCommand) -> AgentRunEntityReply {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(command, &fx.router, fx.now())
        .await
        .expect("the command applies")
}

async fn loop_state(fx: &Fixture) -> AgentLoopState {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.state()
        .expect("the state reads")
        .loop_state()
        .expect("the loop exists")
        .clone()
}

/// The effect of the turn-one call to `name`, as the run now holds it.
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

/// Where the run stands: its status, its phase and its turn.
async fn standing(fx: &Fixture) -> (AgentRunStatus, AgentLoopPhase, u64) {
    let state = loop_state(fx).await;
    (
        fx.run_snapshot().await.expect("the run exists").status,
        state.phase(),
        state.turn(),
    )
}

/// The status of turn two's model call, when the run has committed one.
async fn next_model_call(fx: &Fixture) -> Option<AgentRunEffectStatus> {
    loop_state(fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.turn == 2 && effect.request.tool_call().is_none())
        .map(|effect| effect.status)
}

/// The tool results the run has recorded to its session memory, as the turn
/// each was recorded with and the tool that returned it, in order of tool
/// name. A turn's results are recorded with the turn.
async fn recorded_results(session: &InMemorySessionMemoryStore) -> Vec<(u64, String)> {
    let mut results: Vec<(u64, String)> = session
        .read(&run_scope(), SessionMemoryCursor::start())
        .await
        .expect("the session reads")
        .entries
        .into_iter()
        .filter(|entry| entry.role == MemoryEntryRole::ToolResult)
        .map(|entry| {
            let tool = entry.tool.expect("a tool result names its tool");
            (entry.turn, tool.as_str().to_string())
        })
        .collect();
    results.sort();
    results
}

/// What a turn recorded with the results of both its calls leaves in the
/// session memory.
fn both_results() -> Vec<(u64, String)> {
    vec![(1, CHARGE.to_string()), (1, LOOKUP.to_string())]
}

async fn revision(fx: &Fixture) -> rakka_persistence::Revision {
    fx.runs
        .load(&run_scope().persistence_id())
        .await
        .expect("the run record loads")
        .expect("the run record exists")
        .revision
}

async fn settle(fx: &Fixture) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run settles");
}

/// When the run's record was last written, by its own clock.
async fn stamped_at(fx: &Fixture) -> u64 {
    let record = fx
        .runs
        .load(&run_scope().persistence_id())
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    serde_json::to_value(&record.state).expect("the run state serializes")["updated_at"]
        .as_u64()
        .expect("the record carries its time")
}

/// Asserts a further settle pass writes and sends nothing.
async fn assert_at_rest(fx: &Fixture) {
    let rested = (revision(fx).await, fx.dispatched_effects());
    settle(fx).await;
    assert_eq!(
        (revision(fx).await, fx.dispatched_effects()),
        rested,
        "a further settle pass writes and sends nothing"
    );
}

/// Lands the result of one outstanding effect, as its executor answers it.
async fn land(fx: &Fixture, effect: &AgentRunEffect) -> AgentRunEntityReply {
    let outcome = fx.dispatcher.answer(effect).await;
    apply(
        fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 1,
            outcome: Box::new(outcome),
        },
    )
    .await
}

/// Drives the run to its first turn's two calls, both out.
async fn start_the_turn(fx: &Fixture) {
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
}

/// Drives the run to its first turn's two calls, both out, and reports the
/// charge's outcome as unknown. Returns the checkpoint that parks the run.
async fn park_on_the_charge(fx: &Fixture) -> HumanCheckpointId {
    start_the_turn(fx).await;
    lose(fx, &call_to(fx, CHARGE).await).await;
    assert_eq!(
        (
            standing(fx).await,
            call_to(fx, CHARGE).await.status,
            call_to(fx, LOOKUP).await.status
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                AgentLoopPhase::AwaitingTools,
                1
            ),
            AgentRunEffectStatus::Indeterminate,
            AgentRunEffectStatus::Ready
        ),
        "parked on the charge, with the look-up still out"
    );
    let open = loop_state(fx).await.open_checkpoints().to_vec();
    assert_eq!(open.len(), 1);
    assert_eq!(
        open[0].kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    open[0].checkpoint_id.clone()
}

/// The open reconciliation checkpoint parked on `effect`.
async fn park_of(fx: &Fixture, effect: &AgentRunEffect) -> HumanCheckpointId {
    loop_state(fx)
        .await
        .open_checkpoints()
        .iter()
        .find(|checkpoint| {
            checkpoint.kind == AgentCheckpointKind::IndeterminateEffectReconciliation
                && checkpoint.bound_effect.effect_id == effect.effect_id
                && checkpoint.status.is_waiting()
        })
        .map(|checkpoint| checkpoint.checkpoint_id.clone())
        .expect("the effect's park is open")
}

fn answer(
    checkpoint_id: &HumanCheckpointId,
    key: &str,
    decision: AgentReconciliationDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: checkpoint_id.clone(),
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator".to_string(),
            display_name: None,
        },
        decision: Box::new(AgentCheckpointDecision::Reconciliation(decision)),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

/// The answer that the call ran and returned `receipt`.
fn it_returned(call_id: &str, receipt: &str) -> AgentReconciliationDecision {
    AgentReconciliationDecision::ConfirmedCompleted {
        resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(AgentRunEffectOutcome::Tool {
                call_id: AgentToolCallId::new(call_id).expect("the call id is valid"),
                content: AgentTaskContent::inline(serde_json::json!({ "receipt": receipt }))
                    .expect("the content is inline-bounded"),
            }),
        }),
    }
}

/// Reports the outcome of an outstanding call as unknown.
async fn lose(fx: &Fixture, effect: &AgentRunEffect) {
    apply(
        fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 1,
            outcome: Box::new(AgentRunEffectOutcome::indeterminate(
                "connection-lost",
                "the worker died mid-invocation",
            )),
        },
    )
    .await;
}

/// Rewrites the run entity's durable record in place.
async fn rewrite_run(fx: &Fixture, edit: impl FnOnce(&mut serde_json::Value)) {
    let id = run_scope().persistence_id();
    let record = fx
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    edit(&mut value);
    fx.runs
        .compare_and_set(
            &id,
            record.revision,
            serde_json::from_value(value).expect("the rewritten record deserializes"),
        )
        .await
        .expect("the rewritten record persists");
}

/// The result of the last call still out does not rest a turn whose other
/// call has an unknown outcome. The command's own reply says so, which holds
/// the transition and not a later pass. The turn used to rest there, at
/// `RecordingTurn`, parked.
#[tokio::test]
async fn the_last_result_does_not_rest_a_turn_whose_other_call_is_unknown() {
    let (fx, session) = world();
    park_on_the_charge(&fx).await;

    let reply = land(&fx, &call_to(&fx, LOOKUP).await).await;

    let AgentRunEntityReply::Applied { outcome } = reply else {
        panic!("the result applies, got {reply:?}");
    };
    assert_eq!(
        (outcome.status, outcome.phase),
        (
            AgentRunStatus::WaitingForReconciliation,
            AgentLoopPhase::AwaitingTools
        ),
        "the look-up's result is taken, and the turn stays open on the charge"
    );
    assert_eq!(
        (
            standing(&fx).await,
            loop_state(&fx).await.tool_results().len()
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                AgentLoopPhase::AwaitingTools,
                1
            ),
            1
        )
    );
    assert_eq!(recorded_results(&session).await, Vec::new());
    assert_at_rest(&fx).await;
}

/// Where the turn stands does not depend on which of the two outcomes
/// arrived first. The look-up's result and then the charge unknown is the
/// order that was already right: nothing rested the turn, because the charge
/// was still out when the look-up returned. The charge unknown and then the
/// look-up's result is the order that rested it. Both now leave the same
/// run.
#[tokio::test]
async fn the_order_of_the_two_outcomes_does_not_decide_where_the_turn_stands() {
    let (result_first, _) = world();
    start_the_turn(&result_first).await;
    land(&result_first, &call_to(&result_first, LOOKUP).await).await;
    lose(&result_first, &call_to(&result_first, CHARGE).await).await;

    let (unknown_first, _) = world();
    park_on_the_charge(&unknown_first).await;
    land(&unknown_first, &call_to(&unknown_first, LOOKUP).await).await;

    let picture = |fx: Fixture| async move {
        (
            standing(&fx).await,
            call_to(&fx, CHARGE).await.status,
            call_to(&fx, LOOKUP).await.status,
            loop_state(&fx).await.tool_results().len(),
        )
    };
    let open_on_the_charge = (
        (
            AgentRunStatus::WaitingForReconciliation,
            AgentLoopPhase::AwaitingTools,
            1,
        ),
        AgentRunEffectStatus::Indeterminate,
        AgentRunEffectStatus::Succeeded,
        1,
    );
    assert_eq!(
        picture(result_first).await,
        open_on_the_charge,
        "the look-up's result, then the charge unknown"
    );
    assert_eq!(
        picture(unknown_first).await,
        open_on_the_charge,
        "the charge unknown, then the look-up's result"
    );
}

/// Answered "never executed", the call is sent again, and the run waits for
/// it. It used to record the turn with one result and send turn two's model
/// call at once. The turn is recorded when the re-sent call returns, with
/// both results.
#[tokio::test]
async fn a_call_answered_never_executed_is_waited_for_before_the_turn_is_recorded() {
    let (fx, session) = world();
    let park = park_on_the_charge(&fx).await;
    land(&fx, &call_to(&fx, LOOKUP).await).await;
    let sent = fx.dispatched_effects();

    apply(
        &fx,
        answer(
            &park,
            "answer",
            AgentReconciliationDecision::ConfirmedNotExecuted,
        ),
    )
    .await;

    assert_waiting_for_the_resend(&fx, &session, sent).await;

    land(&fx, &call_to(&fx, CHARGE).await).await;

    assert_recorded_with_both_results(&fx, &session).await;
}

/// The charge is out again and the run waits for it on the turn that made
/// it: one more call is sent, and it is not turn two's model call. Nothing
/// is recorded, and a further settle pass changes nothing.
async fn assert_waiting_for_the_resend(
    fx: &Fixture,
    session: &InMemorySessionMemoryStore,
    sent_before_the_answer: usize,
) {
    assert_eq!(
        (
            standing(fx).await,
            call_to(fx, CHARGE).await.status,
            next_model_call(fx).await,
            fx.dispatched_effects(),
            recorded_results(session).await
        ),
        (
            (
                AgentRunStatus::WaitingForEffect,
                AgentLoopPhase::AwaitingTools,
                1
            ),
            AgentRunEffectStatus::Ready,
            None,
            sent_before_the_answer + 1,
            Vec::new()
        ),
        "the charge is out again, and the run waits for it with the turn open"
    );
    assert_at_rest(fx).await;
}

/// The turn is recorded with the results of both its calls, and turn two's
/// model call is out.
async fn assert_recorded_with_both_results(fx: &Fixture, session: &InMemorySessionMemoryStore) {
    assert_eq!(
        (
            standing(fx).await,
            next_model_call(fx).await,
            recorded_results(session).await
        ),
        (
            (
                AgentRunStatus::WaitingForEffect,
                AgentLoopPhase::AwaitingModel,
                2
            ),
            Some(AgentRunEffectStatus::Ready),
            both_results()
        ),
        "the turn is recorded with both results, and turn two begins"
    );
}

/// Answered with what the call returned, the turn rests on that answer and
/// is recorded with both results. This held before the change and holds
/// after it: the answer is the last of the turn's effects to come back.
#[tokio::test]
async fn a_call_answered_as_executed_rests_the_turn_with_its_result() {
    let (fx, session) = world();
    let park = park_on_the_charge(&fx).await;
    land(&fx, &call_to(&fx, LOOKUP).await).await;

    apply(
        &fx,
        answer(&park, "answer", it_returned("call-charge", "r-9")),
    )
    .await;

    assert_recorded_with_both_results(&fx, &session).await;
}

/// Both calls of the turn end unknown. The answer to the first does not rest
/// the turn, whatever it says, because the second is still unknown. It used
/// to. The turn rests when the second, sent again, returns.
#[tokio::test]
async fn a_turn_with_two_unknown_calls_rests_when_the_second_comes_back() {
    let (fx, session) = world();
    let charge_park = park_on_the_charge(&fx).await;
    let lookup = call_to(&fx, LOOKUP).await;
    lose(&fx, &lookup).await;
    let lookup_park = park_of(&fx, &lookup).await;

    apply(
        &fx,
        answer(
            &charge_park,
            "answer-charge",
            it_returned("call-charge", "r-9"),
        ),
    )
    .await;

    assert_eq!(
        (
            standing(&fx).await,
            call_to(&fx, LOOKUP).await.status,
            recorded_results(&session).await
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                AgentLoopPhase::AwaitingTools,
                1
            ),
            AgentRunEffectStatus::Indeterminate,
            Vec::new()
        ),
        "the charge is answered, and the turn stays open on the look-up"
    );
    assert_at_rest(&fx).await;

    apply(
        &fx,
        answer(
            &lookup_park,
            "answer-lookup",
            AgentReconciliationDecision::ConfirmedNotExecuted,
        ),
    )
    .await;
    assert_eq!(
        (standing(&fx).await, recorded_results(&session).await),
        (
            (
                AgentRunStatus::WaitingForEffect,
                AgentLoopPhase::AwaitingTools,
                1
            ),
            Vec::new()
        ),
        "the look-up is out again, and the turn waits for it"
    );

    land(&fx, &call_to(&fx, LOOKUP).await).await;

    assert_recorded_with_both_results(&fx, &session).await;
}

/// Leaves the record as a build before this change left it: the look-up's
/// result rested the turn over the unknown charge, so the run is parked at
/// `RecordingTurn`. That state lasts until a person answers, so such records
/// exist.
///
/// The record is this build's own with the two values the earlier build
/// wrote differently: the phase, and the phase in the stored reply to the
/// look-up's result. Compared field by field with the record the earlier
/// build writes at this point, nothing else differs.
async fn rested_by_an_earlier_build(fx: &Fixture) -> HumanCheckpointId {
    let park = park_on_the_charge(fx).await;
    let lookup = call_to(fx, LOOKUP).await;
    land(fx, &lookup).await;
    let reply_to = serde_json::json!(lookup
        .result_operation_id(&run_scope())
        .expect("the operation id derives"));
    rewrite_run(fx, |state| {
        state["run"]["loop_state"]["phase"] = serde_json::json!("recording-turn");
        let replies = state["applied_operations"]
            .as_array_mut()
            .expect("the stored replies are a list");
        let last = replies.last_mut().expect("the look-up's result is stored");
        assert_eq!(
            last["operation_id"], reply_to,
            "the last stored reply is the one to the look-up's result"
        );
        last["outcome"]["phase"] = serde_json::json!("recording-turn");
    })
    .await;
    assert_eq!(
        standing(fx).await,
        (
            AgentRunStatus::WaitingForReconciliation,
            AgentLoopPhase::RecordingTurn,
            1
        ),
        "the record as the earlier build left it: rested, with the charge unknown"
    );
    park
}

/// The settle pass opens such a turn again: the phase goes back to the
/// turn's wait, the park stands, and nothing is sent. One write does it, and
/// the next pass writes nothing. The pass is no turn, and is not measured as
/// one. From there the answer is taken as this build takes it.
#[tokio::test]
async fn a_turn_an_earlier_build_rested_over_an_unknown_call_is_opened_again() {
    let (fx, session, metrics) = world_measured();
    let park = rested_by_an_earlier_build(&fx).await;
    let sent = fx.dispatched_effects();
    let turns_measured = || {
        metrics
            .snapshot()
            .observations_named(rakka_agent::METRIC_AGENT_TURN_DURATION)
            .len()
    };
    let measured = turns_measured();
    let rested = (revision(&fx).await, stamped_at(&fx).await);

    settle(&fx).await;

    assert_eq!(
        (
            standing(&fx).await,
            call_to(&fx, CHARGE).await.status,
            fx.dispatched_effects()
        ),
        (
            (
                AgentRunStatus::WaitingForReconciliation,
                AgentLoopPhase::AwaitingTools,
                1
            ),
            AgentRunEffectStatus::Indeterminate,
            sent
        ),
        "the turn is open again, on the park it was under"
    );
    assert_eq!(
        revision(&fx).await.get(),
        rested.0.get() + 1,
        "in one write: the run was not taken off its park and put back"
    );
    assert!(
        stamped_at(&fx).await > rested.1,
        "the write carries the time of its pass"
    );
    assert_at_rest(&fx).await;
    assert_eq!(
        turns_measured(),
        measured,
        "the pass that opens the turn is no turn, and measures none"
    );

    apply(
        &fx,
        answer(
            &park,
            "answer",
            AgentReconciliationDecision::ConfirmedNotExecuted,
        ),
    )
    .await;
    assert_waiting_for_the_resend(&fx, &session, sent).await;
    land(&fx, &call_to(&fx, CHARGE).await).await;
    assert_recorded_with_both_results(&fx, &session).await;
    assert_eq!(
        turns_measured(),
        measured + 1,
        "the turn is measured once, when it is recorded"
    );
}

/// The answer may be the first thing this build does to such a record. The
/// call it sends again is waited for all the same: the pass that follows the
/// answer opens the turn before the loop can record it.
#[tokio::test]
async fn a_turn_an_earlier_build_rested_waits_for_a_call_its_first_command_sends_again() {
    let (fx, session) = world();
    let park = rested_by_an_earlier_build(&fx).await;
    let sent = fx.dispatched_effects();

    apply(
        &fx,
        answer(
            &park,
            "answer",
            AgentReconciliationDecision::ConfirmedNotExecuted,
        ),
    )
    .await;

    assert_waiting_for_the_resend(&fx, &session, sent).await;
    land(&fx, &call_to(&fx, CHARGE).await).await;
    assert_recorded_with_both_results(&fx, &session).await;
}

/// Leaves the record of a run whose turn an earlier build had already
/// recorded without the charge's result: answered "never executed", that
/// build sent the charge again, recorded turn one with the look-up alone and
/// sent turn two's model call. The run is on turn two with a call of turn
/// one out.
///
/// It is built from this build's own record after the answer, where the turn
/// is open on the charge. The charge is taken out of the record for the one
/// pass in which the loop records the turn and begins the next, as the
/// earlier build did with the charge in it, and is then put back. Compared
/// field by field with the record the earlier build writes at this point,
/// it differs in three clock values.
async fn recorded_without_a_result_by_an_earlier_build(fx: &Fixture) {
    let park = park_on_the_charge(fx).await;
    land(fx, &call_to(fx, LOOKUP).await).await;
    apply(
        fx,
        answer(
            &park,
            "answer",
            AgentReconciliationDecision::ConfirmedNotExecuted,
        ),
    )
    .await;
    let mut charge = serde_json::Value::Null;
    rewrite_run(fx, |state| {
        let effects = state["run"]["loop_state"]["effects"]
            .as_array_mut()
            .expect("the effects are a list");
        let at = effects
            .iter()
            .position(|effect| effect.to_string().contains(CHARGE))
            .expect("the charge is held");
        charge = effects.remove(at);
        state["run"]["loop_state"]["phase"] = serde_json::json!("recording-turn");
        state["run"]["status"] = serde_json::json!("running");
        // The earlier build's replies to the look-up's result and to the
        // answer, and its record of the decision, say the turn rested.
        let replies = state["applied_operations"]
            .as_array_mut()
            .expect("the stored replies are a list");
        let last_two = replies.len() - 2;
        for reply in &mut replies[last_two..] {
            reply["outcome"]["phase"] = serde_json::json!("recording-turn");
        }
        for decision in state["decision_ledger"]
            .as_array_mut()
            .expect("the decisions are a list")
        {
            decision["outcome"]["phase"] = serde_json::json!("recording-turn");
        }
    })
    .await;
    settle(fx).await;
    rewrite_run(fx, |state| {
        state["run"]["loop_state"]["effects"]
            .as_array_mut()
            .expect("the effects are a list")
            .insert(0, charge);
    })
    .await;
    assert_eq!(
        (
            standing(fx).await,
            call_to(fx, CHARGE).await.status,
            next_model_call(fx).await
        ),
        (
            (
                AgentRunStatus::WaitingForEffect,
                AgentLoopPhase::AwaitingModel,
                2
            ),
            AgentRunEffectStatus::Ready,
            Some(AgentRunEffectStatus::Ready)
        ),
        "the record as the earlier build left it: on turn two, with turn one's charge out"
    );
}

/// What is not repaired: a turn an earlier build had already recorded
/// without a result. The run is on a later turn, and that turn is not held
/// for the earlier turn's call. Turn two here makes no call, so it is
/// recorded as soon as its model output lands, and the run proposes its
/// result, as it did under the build that left the record. The earlier
/// turn's record is not rewritten: the session memory holds turn one with
/// the look-up alone, before and after the charge returns.
#[tokio::test]
async fn a_turn_an_earlier_build_recorded_without_a_result_is_left_as_it_was() {
    let (fx, session) = world();
    recorded_without_a_result_by_an_earlier_build(&fx).await;
    assert_eq!(
        recorded_results(&session).await,
        vec![(1, LOOKUP.to_string())],
        "turn one was recorded with the look-up alone"
    );

    let model_call = loop_state(&fx)
        .await
        .effects()
        .iter()
        .find(|effect| effect.turn == 2 && effect.request.tool_call().is_none())
        .cloned()
        .expect("turn two's model call is out");
    land(&fx, &model_call).await;

    assert_eq!(
        (
            standing(&fx).await,
            call_to(&fx, CHARGE).await.status,
            recorded_results(&session).await
        ),
        (
            (
                AgentRunStatus::Cancelling,
                AgentLoopPhase::DecidingContinuation,
                2
            ),
            AgentRunEffectStatus::Ready,
            vec![(1, LOOKUP.to_string())]
        ),
        "turn two is recorded and its result proposed and accepted, with the charge still out: \
         the run waits for that call to end, and not to record it"
    );

    land(&fx, &call_to(&fx, CHARGE).await).await;

    assert_eq!(
        (
            fx.run_snapshot().await.expect("the run exists").status,
            recorded_results(&session).await
        ),
        (AgentRunStatus::Completed, vec![(1, LOOKUP.to_string())]),
        "the charge returned and the run ended: no turn's record carries its result"
    );
}
