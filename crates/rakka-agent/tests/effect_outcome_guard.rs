//! The outcome guard, driven through the run entity (issue #125).
//!
//! An outcome settles only the effect it belongs to: the success variant the
//! effect's request produces, naming the effect's own call, cell or
//! generation. The run refuses any other outcome with
//! `effect-outcome-mismatch` before anything changes, on both of its entry
//! points: a dispatcher's `RecordEffectResult` and a person's
//! `ConfirmedExecuted`, whether that arrives through
//! `ResolveIndeterminateEffect` or the reconciliation checkpoint. A stale or
//! duplicate delivery still answers its stale error first, and a confirmation
//! that establishes nothing keeps `effect-policy-invalid`.
//!
//! These are live runs. The ended-run tests, which cover confirmations, live
//! in `private_memory_promotion.rs`. Each other effect kind but one is tested
//! once on a live run's result path, in its own file:
//! `communal_claim_append.rs`, for one, offers a claim append a promotion's
//! receipt. The exception is a workflow cancel: no test drives one through a
//! run, and the guard's unit tests in `src/effect.rs` cover it.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    child_workflow_run_id, delegation_id_for, goal_evaluation_record_id, handoff_id_for,
    promotion_operation_id, workflow_invocation_id_for, AgentA2aHandoffReceipt,
    AgentA2aSendReceipt, AgentCheckpoint, AgentCheckpointDecision, AgentCheckpointKind,
    AgentCommunalClaimId, AgentEffectGeneration, AgentEffectResolution,
    AgentGoalEvaluationMethodKind, AgentGoalEvaluationOutcome, AgentGoalEvaluationRecord,
    AgentGoalId, AgentMemoryPromotionRequest, AgentModelTurn, AgentModelUsage, AgentOperationId,
    AgentOperationKind, AgentPolicyRef, AgentPrivateMemoryKind, AgentReconciliationDecision,
    AgentRevisionNumber, AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply,
    AgentRunError, AgentRunMemory, AgentTaskContent, AgentTaskId, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, AgentWorkflowStartReceipt, InMemoryAgentPrivateMemoryStore,
    InMemoryContextSnapshotStore, InMemorySessionMemoryStore, MemorySequence,
    SessionMemoryPromotionExecutor, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

mod common;

use common::*;

// ---------------------------------------------------------------------------
// The world, copied from `private_memory_promotion.rs`: a run whose first turn
// calls a tool, with the promotion executor wired over its stores.
// ---------------------------------------------------------------------------

fn usage() -> AgentModelUsage {
    AgentModelUsage {
        input_tokens: 8,
        output_tokens: 4,
        cost_micros: 2,
        ..Default::default()
    }
}

fn tool_call(call_id: &str, tool: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call_id).expect("call id"),
        AgentToolId::new(tool).expect("tool id"),
        serde_json::json!({ "query": "ticket" }),
    )
    .expect("the tool call is bounded")
}

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(tool_call("call-1", tool))
        .with_usage(usage())
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(usage())
}

fn found() -> AgentTaskContent {
    AgentTaskContent::inline(serde_json::json!({ "found": true }))
        .expect("the tool result is inline-bounded")
}

struct Stores {
    session: Arc<InMemorySessionMemoryStore>,
    snapshots: Arc<InMemoryContextSnapshotStore>,
    private: Arc<InMemoryAgentPrivateMemoryStore>,
}

fn stores() -> Stores {
    Stores {
        session: Arc::new(InMemorySessionMemoryStore::new()),
        snapshots: Arc::new(InMemoryContextSnapshotStore::new()),
        private: Arc::new(InMemoryAgentPrivateMemoryStore::new()),
    }
}

fn requested_by() -> PrincipalRef {
    PrincipalRef {
        principal_type: "service".to_string(),
        principal_id: "memory-curator".to_string(),
        display_name: None,
    }
}

fn promotion(from: u64, to: u64) -> AgentMemoryPromotionRequest {
    AgentMemoryPromotionRequest {
        from_sequence: MemorySequence::new(from),
        to_sequence: MemorySequence::new(to),
        kind: AgentPrivateMemoryKind::Semantic,
        target: None,
        confidence_bps: 9_000,
        requested_by: requested_by(),
        roles: None,
    }
}

fn promote_command(request: AgentMemoryPromotionRequest, disc: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::PromoteMemory {
        operation_id: promotion_operation_id(&run_scope(), disc).expect("operation id"),
        promotion: Box::new(request),
    }
}

/// A world whose first turn calls one tool and whose second proposes, with
/// the promotion executor wired over the fixture's session and private
/// stores.
fn tool_promoting_world() -> (Fixture, Stores) {
    let stores = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result("lookup", found())
    .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
        stores.session.clone(),
        stores.private.clone(),
    )));
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    (fx, stores)
}

/// Cranks the run to its current durable wait: recovery, then the settle
/// pass that flushes whatever is dispatchable.
async fn crank(fx: &Fixture) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("settle");
}

/// Applies one command to the run entity; a refused transition surfaces as the
/// entity error.
async fn apply(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.apply(command, &fx.router, now).await
}

/// Applies one command that must succeed.
async fn apply_ok(fx: &Fixture, command: AgentRunEntityCommand) -> AgentRunEntityReply {
    apply(fx, command).await.expect("the command applies")
}

/// Answers every dispatched effect once, leaving whatever that commits for
/// the next crank.
async fn drive_turn(fx: &Fixture) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("answer the dispatched effects");
}

/// The run's whole durable state, recovered and encoded: the refusals below
/// compare it before and after.
async fn run_state_json(fx: &Fixture) -> serde_json::Value {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    serde_json::to_value(run.state().expect("state")).expect("the run state serializes")
}

/// Every dispatched, still-outstanding effect of `kind` the run holds, in
/// slot order.
async fn dispatched_effects_of(fx: &Fixture, kind: AgentRunEffectKind) -> Vec<AgentRunEffect> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .filter(|effect| effect.kind() == kind && effect.status == AgentRunEffectStatus::Ready)
        .cloned()
        .collect()
}

/// The one dispatched, still-outstanding effect of `kind` the run holds.
async fn dispatched_effect(fx: &Fixture, kind: AgentRunEffectKind) -> AgentRunEffect {
    let effects = dispatched_effects_of(fx, kind).await;
    assert_eq!(effects.len(), 1, "exactly one dispatched {kind} effect");
    effects[0].clone()
}

/// The effect the run holds under `effect`'s id, re-read from durable state.
async fn effect_record(fx: &Fixture, effect: &AgentRunEffect) -> AgentRunEffect {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .find(|held| held.effect_id == effect.effect_id)
        .cloned()
        .expect("the effect record survives")
}

/// A `RecordEffectResult` for `effect` at `generation`, under `operation_id`.
fn result_command(
    effect: &AgentRunEffect,
    generation: AgentEffectGeneration,
    operation_id: AgentOperationId,
    outcome: AgentRunEffectOutcome,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::RecordEffectResult {
        operation_id,
        effect_id: effect.effect_id.clone(),
        generation,
        attempt: effect.attempts.saturating_add(1),
        fence: 0,
        outcome: Box::new(outcome),
    }
}

/// Delivers `outcome` as a dispatcher's result for `effect`'s current
/// generation, under that generation's own result operation id.
async fn deliver(
    fx: &Fixture,
    effect: &AgentRunEffect,
    outcome: AgentRunEffectOutcome,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let operation_id = effect
        .result_operation_id(&run_scope())
        .expect("result operation id");
    apply(
        fx,
        result_command(effect, effect.generation, operation_id, outcome),
    )
    .await
}

/// A result operation id no delivery has used, so the replay window cannot
/// answer before the run's own fences do.
fn fresh_result_operation_id(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::Command,
        [
            TENANT,
            AGENT,
            run_scope().run().as_str(),
            "late-foreign-result",
            discriminator,
        ],
    )
    .expect("the operation id derives")
}

/// One of each success variant, labelled as the effect layer labels it. Every
/// identity it names is derived for `effect`'s own coordinate, and the
/// evaluation record is stamped with `effect`'s own id and generation, so the
/// only thing wrong with each is its kind.
fn success_outcomes(effect: &AgentRunEffect) -> Vec<(&'static str, AgentRunEffectOutcome)> {
    let scope = run_scope();
    let delegation =
        delegation_id_for(&scope, effect.turn, effect.slot).expect("the delegation id derives");
    let handoff = handoff_id_for(&scope, effect.turn, effect.slot).expect("the handoff id derives");
    let invocation = workflow_invocation_id_for(&scope, effect.turn, effect.slot)
        .expect("the invocation id derives");
    let record = AgentGoalEvaluationRecord::new(
        goal_evaluation_record_id(&scope, effect.turn, effect.slot, effect.generation)
            .expect("the evaluation id derives"),
        AgentGoalId::new("ship-the-fix").expect("the goal id is valid"),
        AgentPolicyRef::new("evaluator-v1").expect("the evaluator ref is valid"),
        AgentGoalEvaluationMethodKind::DeterministicAssertion,
        AgentRevisionNumber::INITIAL,
        AgentGoalEvaluationOutcome::Satisfied,
        "criteria-met",
        Vec::new(),
        None,
        effect.effect_id.clone(),
        effect.generation,
        AgentTimestampMillis::new(1),
    )
    .expect("the evaluation record is valid");
    vec![
        (
            "model",
            AgentRunEffectOutcome::Model {
                turn: Box::new(tool_calling_turn("lookup")),
            },
        ),
        (
            "tool",
            AgentRunEffectOutcome::Tool {
                call_id: AgentToolCallId::new("call-1").expect("call id"),
                content: found(),
            },
        ),
        (
            "memory-promotion",
            AgentRunEffectOutcome::MemoryPromotion {
                promoted: Vec::new(),
            },
        ),
        (
            "evaluation",
            AgentRunEffectOutcome::Evaluation {
                record: Box::new(record),
            },
        ),
        (
            "a2a-send",
            AgentRunEffectOutcome::A2aSend {
                receipt: AgentA2aSendReceipt {
                    delegation,
                    child_task: AgentTaskId::new("child-elsewhere").expect("task id"),
                    child_run: None,
                    peer_status: "submitted".to_string(),
                },
            },
        ),
        (
            "a2a-handoff",
            AgentRunEffectOutcome::A2aHandoff {
                receipt: AgentA2aHandoffReceipt {
                    handoff,
                    target_generation: None,
                    peer_status: "working".to_string(),
                },
            },
        ),
        (
            "workflow-start",
            AgentRunEffectOutcome::WorkflowStart {
                receipt: AgentWorkflowStartReceipt {
                    child_run: child_workflow_run_id(&invocation),
                    invocation,
                    adopted: false,
                },
            },
        ),
        (
            "workflow-cancel",
            AgentRunEffectOutcome::WorkflowCancel {
                already_finished: false,
            },
        ),
        (
            "claim-append",
            AgentRunEffectOutcome::ClaimAppend {
                claim: AgentCommunalClaimId::new("claim-elsewhere").expect("the claim id is valid"),
            },
        ),
    ]
}

/// Delivers every success variant but `own` to `effect` as a dispatcher's
/// result, and checks each is refused `effect-outcome-mismatch` with the
/// run's state exactly as it was.
async fn every_foreign_result_is_refused(fx: &Fixture, effect: &AgentRunEffect, own: &str) {
    let foreign: Vec<_> = success_outcomes(effect)
        .into_iter()
        .filter(|(label, _)| *label != own)
        .collect();
    assert_eq!(foreign.len(), 8, "every other success variant, for {own}");
    for (label, outcome) in foreign {
        let at = format!("a {label} outcome delivered to the {own} effect");
        let before = run_state_json(fx).await;
        let refusal = deliver(fx, effect, outcome)
            .await
            .expect_err("a foreign result settles nothing");
        assert_eq!(refusal.code(), "effect-outcome-mismatch", "{at}: {refusal}");
        assert_eq!(run_state_json(fx).await, before, "{at}: the state moved");
    }
}

/// For a live run's outstanding model effect, tool effect and promotion
/// effect in turn, a dispatcher result carrying any other kind's success
/// variant is refused, and the run's state does not move. Applied, a model
/// turn would install a pending turn, a tool result would rest the turn, and
/// a promotion receipt would land on an effect that never promoted.
#[tokio::test]
async fn a_live_run_refuses_a_foreign_result() {
    let (fx, _stores) = tool_promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;

    // Turn one's model call is in flight.
    let model = dispatched_effect(&fx, AgentRunEffectKind::ModelCall).await;
    every_foreign_result_is_refused(&fx, &model, "model").await;
    assert_eq!(
        effect_record(&fx, &model).await.status,
        AgentRunEffectStatus::Ready
    );

    // The model's own turn lands and commits the tool call; a promotion is
    // committed beside it, and both are dispatched.
    drive_turn(&fx).await;
    crank(&fx).await;
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "mid-turn")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    crank(&fx).await;
    let tool = dispatched_effect(&fx, AgentRunEffectKind::ToolCall).await;
    let promotion = dispatched_effect(&fx, AgentRunEffectKind::MemoryPromotionCall).await;

    every_foreign_result_is_refused(&fx, &tool, "tool").await;
    every_foreign_result_is_refused(&fx, &promotion, "memory-promotion").await;
    assert_eq!(
        effect_record(&fx, &tool).await.status,
        AgentRunEffectStatus::Ready
    );
    assert_eq!(
        effect_record(&fx, &promotion).await.status,
        AgentRunEffectStatus::Ready
    );

    // The tool still takes its own result, which rests the turn: its effect
    // leaves the loop with the turn, and the promotion outside the turn stays.
    let outcome = fx.dispatcher.answer(&tool).await;
    let reply = deliver(&fx, &tool, outcome)
        .await
        .expect("the tool's own result applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let loop_state = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .clone();
    assert!(
        !loop_state
            .effects()
            .iter()
            .any(|held| held.effect_id == tool.effect_id && held.is_outstanding()),
        "the tool effect settled"
    );
    assert_eq!(
        effect_record(&fx, &promotion).await.status,
        AgentRunEffectStatus::Ready
    );
}

/// A turn with two tool calls: the result for call A, delivered to call B's
/// effect id and generation, is refused, and neither call's result is
/// recorded. Before the guard, the tool arm recorded it under call A's id and
/// settled call B's effect.
#[tokio::test]
async fn a_live_run_refuses_a_tool_result_under_another_calls_id() {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(
                1,
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_text("Let me look up both.")
                    .with_tool_call(tool_call("call-a", "lookup"))
                    .with_tool_call(tool_call("call-b", "search"))
                    .with_usage(usage()),
            )
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result("lookup", found())
    .with_tool_result("search", found());
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;

    let tools = dispatched_effects_of(&fx, AgentRunEffectKind::ToolCall).await;
    assert_eq!(tools.len(), 2, "both tool calls are dispatched");
    let call_of = |effect: &AgentRunEffect| match &effect.request {
        AgentRunEffectRequest::Tool { call } => call.call_id.as_str().to_string(),
        other => panic!("not a tool effect: {other:?}"),
    };
    let a = tools
        .iter()
        .find(|effect| call_of(effect) == "call-a")
        .expect("call A's effect")
        .clone();
    let b = tools
        .iter()
        .find(|effect| call_of(effect) == "call-b")
        .expect("call B's effect")
        .clone();

    let a_result = fx.dispatcher.answer(&a).await;
    assert!(
        matches!(&a_result, AgentRunEffectOutcome::Tool { call_id, .. } if call_id.as_str() == "call-a"),
        "call A's own result: {a_result:?}"
    );
    let before = run_state_json(&fx).await;
    let refusal = deliver(&fx, &b, a_result)
        .await
        .expect_err("a result under another call's id settles nothing");
    assert_eq!(refusal.code(), "effect-outcome-mismatch", "{refusal}");
    assert_eq!(run_state_json(&fx).await, before, "the state moved");

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let loop_state = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .clone();
    assert!(
        loop_state.tool_results().is_empty(),
        "neither call's result is recorded: {:?}",
        loop_state.tool_results()
    );
    assert_eq!(
        effect_record(&fx, &a).await.status,
        AgentRunEffectStatus::Ready
    );
    assert_eq!(
        effect_record(&fx, &b).await.status,
        AgentRunEffectStatus::Ready
    );
}

// ---------------------------------------------------------------------------
// A live run's parked promotion, and the person's confirmation of it.
// ---------------------------------------------------------------------------

/// Commits a promotion on the live run, dispatches it, and parks it
/// `Indeterminate`, which opens its reconciliation checkpoint.
async fn park_promotion(fx: &Fixture, discriminator: &str) -> AgentRunEffect {
    let reply = apply_ok(fx, promote_command(promotion(1, 1), discriminator)).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    crank(fx).await;
    let effect = dispatched_effect(fx, AgentRunEffectKind::MemoryPromotionCall).await;
    deliver(
        fx,
        &effect,
        AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        ),
    )
    .await
    .expect("the ambiguity records");
    let parked = effect_record(fx, &effect).await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    parked
}

/// The checkpoints the run holds open.
async fn open_checkpoints(fx: &Fixture) -> Vec<AgentCheckpoint> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .open_checkpoints()
        .to_vec()
}

/// The one checkpoint the run holds open.
async fn the_open_checkpoint(fx: &Fixture) -> AgentCheckpoint {
    let open = open_checkpoints(fx).await;
    assert_eq!(open.len(), 1, "one checkpoint is open");
    open.into_iter().next().expect("one checkpoint")
}

/// A live run resting on turn one's model call, its promotion parked behind
/// the reconciliation checkpoint the park opened.
async fn live_parked_promotion() -> (Fixture, Stores, AgentRunEffect, AgentCheckpoint) {
    let (fx, stores) = tool_promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    let parked = park_promotion(&fx, "live-park").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    assert_eq!(
        checkpoint.kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(checkpoint.bound_effect.effect_id, parked.effect_id);
    let run = fx.run_snapshot().await.expect("the run exists");
    assert!(
        !run.status.is_terminal(),
        "the run is live: {:?}",
        run.status
    );
    (fx, stores, parked, checkpoint)
}

/// A `ResolveIndeterminateEffect` command under a checkpoint-resolution key.
fn resolve_indeterminate(
    key: &str,
    effect: &AgentRunEffect,
    generation: AgentEffectGeneration,
    resolution: AgentEffectResolution,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveIndeterminateEffect {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        effect_id: effect.effect_id.clone(),
        generation,
        resolution: Box::new(resolution),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

/// A `ResolveCheckpoint` command under a checkpoint-resolution key.
fn resolve_checkpoint_command(
    checkpoint_id: &HumanCheckpointId,
    key: &str,
    decision: AgentCheckpointDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: checkpoint_id.clone(),
        resolver: requested_by(),
        decision: Box::new(decision),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

/// `ConfirmedExecuted { outcome }` for the parked effect, through the
/// reconciliation checkpoint when `via_checkpoint` holds, else through
/// `ResolveIndeterminateEffect`.
fn confirm(
    via_checkpoint: bool,
    key: &str,
    parked: &AgentRunEffect,
    checkpoint: &AgentCheckpoint,
    outcome: AgentRunEffectOutcome,
) -> AgentRunEntityCommand {
    let resolution = AgentEffectResolution::ConfirmedExecuted {
        outcome: Box::new(outcome),
    };
    if via_checkpoint {
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            key,
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedCompleted {
                    resolution: Box::new(resolution),
                },
            ),
        )
    } else {
        resolve_indeterminate(key, parked, parked.generation, resolution)
    }
}

/// A live run's parked promotion: `ConfirmedExecuted` carrying a model turn,
/// a goal evaluation or a claim append's receipt is refused on both paths,
/// the run's state does not move, and the reconciliation checkpoint stays
/// open. Before the guard a live run applied each: the model turn installed a
/// pending turn, the evaluation filled the goal-evaluation cell.
#[tokio::test]
async fn a_live_run_refuses_a_foreign_confirmation() {
    let (fx, _stores, parked, checkpoint) = live_parked_promotion().await;
    let foreign: Vec<_> = success_outcomes(&parked)
        .into_iter()
        .filter(|(label, _)| ["model", "evaluation", "claim-append"].contains(label))
        .collect();
    assert_eq!(foreign.len(), 3);

    for via_checkpoint in [false, true] {
        for (label, outcome) in foreign.clone() {
            let at = format!("a {label} outcome, via the checkpoint: {via_checkpoint}");
            let before = run_state_json(&fx).await;
            let key = format!("{label}-{via_checkpoint}");
            let refusal = apply(
                &fx,
                confirm(via_checkpoint, &key, &parked, &checkpoint, outcome),
            )
            .await
            .expect_err("a foreign confirmation settles nothing");
            assert_eq!(refusal.code(), "effect-outcome-mismatch", "{at}: {refusal}");
            assert_eq!(run_state_json(&fx).await, before, "{at}: the state moved");
            assert_eq!(the_open_checkpoint(&fx).await, checkpoint, "{at}");
            let held = effect_record(&fx, &parked).await;
            assert_eq!(held.status, AgentRunEffectStatus::Indeterminate, "{at}");
            assert_eq!(held.generation, parked.generation, "{at}");
        }
    }

    // The promotion's own receipt still settles it.
    apply_ok(
        &fx,
        confirm(
            true,
            "own-receipt",
            &parked,
            &checkpoint,
            AgentRunEffectOutcome::MemoryPromotion {
                promoted: Vec::new(),
            },
        ),
    )
    .await;
    assert_eq!(
        effect_record(&fx, &parked).await.status,
        AgentRunEffectStatus::Succeeded
    );
    assert!(open_checkpoints(&fx).await.is_empty());
}

/// A stale delivery answers its stale error whatever it carries: a foreign
/// outcome for a superseded generation answers `StaleEffectGeneration`, and
/// for a settled effect `StaleEffectResult`, exactly as the effect's own
/// outcome would. Each is delivered under a fresh operation id, so the replay
/// window cannot answer first.
#[tokio::test]
async fn a_stale_mismatched_result_still_answers_stale() {
    let (fx, _stores, parked, checkpoint) = live_parked_promotion().await;

    // A person says the first generation never ran: the second is minted.
    apply_ok(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "not-executed",
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedNotExecuted,
            ),
        ),
    )
    .await;
    crank(&fx).await;
    let current = dispatched_effect(&fx, AgentRunEffectKind::MemoryPromotionCall).await;
    assert_eq!(current.generation, parked.generation.next());

    let model_turn = || AgentRunEffectOutcome::Model {
        turn: Box::new(tool_calling_turn("lookup")),
    };
    let before = run_state_json(&fx).await;
    let refusal = apply(
        &fx,
        result_command(
            &current,
            parked.generation,
            fresh_result_operation_id("superseded"),
            model_turn(),
        ),
    )
    .await
    .expect_err("a superseded generation settles nothing");
    assert_eq!(refusal.code(), "run-stale-effect-generation", "{refusal}");
    assert!(
        matches!(refusal, AgentRunError::StaleEffectGeneration { .. }),
        "{refusal:?}"
    );
    assert_eq!(run_state_json(&fx).await, before, "the state moved");

    // The current generation settles on its own receipt; a foreign outcome
    // for it afterwards finds it settled.
    deliver(
        &fx,
        &current,
        AgentRunEffectOutcome::MemoryPromotion {
            promoted: Vec::new(),
        },
    )
    .await
    .expect("the promotion's own receipt applies");
    let settled = effect_record(&fx, &current).await;
    assert_eq!(settled.status, AgentRunEffectStatus::Succeeded);

    let before = run_state_json(&fx).await;
    let refusal = apply(
        &fx,
        result_command(
            &settled,
            settled.generation,
            fresh_result_operation_id("settled"),
            model_turn(),
        ),
    )
    .await
    .expect_err("a settled effect settles nothing");
    assert_eq!(refusal.code(), "run-stale-effect-result", "{refusal}");
    assert!(
        matches!(refusal, AgentRunError::StaleEffectResult { .. }),
        "{refusal:?}"
    );
    assert_eq!(run_state_json(&fx).await, before, "the state moved");
}

/// The confirmation path holds the same order (design 5.1.3): a foreign
/// `ConfirmedExecuted` sent through `ResolveIndeterminateEffect` answers its
/// stale error before the outcome guard. Sent to a superseded generation it
/// answers `StaleEffectGeneration`, and sent to a settled effect
/// `StaleEffectResult`, as the effect's own receipt would. The run's state
/// does not move. `ResolveCheckpoint` cannot reach either case: the
/// reconciliation checkpoint goes with the generation it parked.
#[tokio::test]
async fn a_stale_mismatched_confirmation_still_answers_stale() {
    let (fx, _stores, parked, checkpoint) = live_parked_promotion().await;

    // A person says the first generation never ran: the second is minted.
    apply_ok(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "not-executed",
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedNotExecuted,
            ),
        ),
    )
    .await;
    crank(&fx).await;
    let current = dispatched_effect(&fx, AgentRunEffectKind::MemoryPromotionCall).await;
    assert_eq!(current.generation, parked.generation.next());

    let confirm_model_turn = |key: &str, generation: AgentEffectGeneration| {
        resolve_indeterminate(
            key,
            &current,
            generation,
            AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(AgentRunEffectOutcome::Model {
                    turn: Box::new(tool_calling_turn("lookup")),
                }),
            },
        )
    };
    let before = run_state_json(&fx).await;
    let refusal = apply(&fx, confirm_model_turn("superseded", parked.generation))
        .await
        .expect_err("a superseded generation settles nothing");
    assert_eq!(refusal.code(), "run-stale-effect-generation", "{refusal}");
    assert!(
        matches!(refusal, AgentRunError::StaleEffectGeneration { .. }),
        "{refusal:?}"
    );
    assert_eq!(run_state_json(&fx).await, before, "the state moved");

    // The current generation settles on its own receipt; a foreign
    // confirmation for it afterwards finds it settled.
    deliver(
        &fx,
        &current,
        AgentRunEffectOutcome::MemoryPromotion {
            promoted: Vec::new(),
        },
    )
    .await
    .expect("the promotion's own receipt applies");
    let settled = effect_record(&fx, &current).await;
    assert_eq!(settled.status, AgentRunEffectStatus::Succeeded);

    let before = run_state_json(&fx).await;
    let refusal = apply(&fx, confirm_model_turn("settled", settled.generation))
        .await
        .expect_err("a settled effect settles nothing");
    assert_eq!(refusal.code(), "run-stale-effect-result", "{refusal}");
    assert!(
        matches!(refusal, AgentRunError::StaleEffectResult { .. }),
        "{refusal:?}"
    );
    assert_eq!(run_state_json(&fx).await, before, "the state moved");
}

/// A confirmation that establishes nothing keeps its code: `ConfirmedExecuted`
/// carrying an `Indeterminate` (or a `Cancelled`) outcome is refused
/// `effect-policy-invalid` on both paths, because the resolution's own
/// validation runs before the outcome guard. Nothing moves.
#[tokio::test]
async fn a_confirmation_of_an_indeterminate_outcome_keeps_its_code() {
    let (fx, _stores, parked, checkpoint) = live_parked_promotion().await;
    let unestablished = [
        (
            "indeterminate",
            AgentRunEffectOutcome::indeterminate("still-unknown", "nobody saw what happened"),
        ),
        (
            "cancelled",
            AgentRunEffectOutcome::Cancelled {
                reason: "fenced".to_string(),
            },
        ),
    ];
    for via_checkpoint in [false, true] {
        for (label, outcome) in unestablished.clone() {
            let at = format!("a {label} outcome, via the checkpoint: {via_checkpoint}");
            let before = run_state_json(&fx).await;
            let key = format!("{label}-{via_checkpoint}");
            let refusal = apply(
                &fx,
                confirm(via_checkpoint, &key, &parked, &checkpoint, outcome),
            )
            .await
            .expect_err("a confirmation that establishes nothing is refused");
            assert_eq!(refusal.code(), "effect-policy-invalid", "{at}: {refusal}");
            assert_eq!(run_state_json(&fx).await, before, "{at}: the state moved");
            assert_eq!(the_open_checkpoint(&fx).await, checkpoint, "{at}");
        }
    }
}
