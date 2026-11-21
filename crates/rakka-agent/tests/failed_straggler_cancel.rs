//! A failed straggler cancel leaves the run going (issue #197).
//!
//! An `Any` fan-in over two workflow invocations resolves on its first
//! child, and the run chases the other, the straggler, with a
//! `WorkflowCancelCall`. The run is live, and the fan-in has already decided
//! what it waited for. A cancel that fails (no cancel executor, an executor
//! that answers `Refused`, retries spent, or a reconciliation answer that
//! says it ran and failed) used to wind the run down as `EffectFailed`. In
//! two orders the run's result then never reached its task: the cancel
//! failed while the model call was out, or the task rejected the proposal
//! that was out.
//!
//! A failed cancel now does what a successful one does: it settles its own
//! effect and moves nothing else. The effect keeps the failure's code, and
//! the straggler's cell keeps `Committed`. A run that was cancelled still
//! waits for the straggler's own result, as the `Refused` finding says.

mod common;

use std::sync::Arc;

use common::{
    delegation_config_with_fan_in, fan_in_tool_id, goal_spec_draft, goal_spec_with_workflow,
    goal_task_creation_command, run_scope, task_definition, Fixture, TENANT, WORKFLOW_TOOL,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    child_workflow_run_id, AgentDispatchFuture, AgentEffectResolution, AgentExchangeRouter,
    AgentLoopPhase, AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunEffect,
    AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand,
    AgentRunEntityReply, AgentRunError, AgentRunScope, AgentRunStatus, AgentTaskContent,
    AgentTaskStatus, AgentToolCallId, AgentToolCallRequest, AgentWorkflowCancelDisposition,
    AgentWorkflowCancelExecutor, AgentWorkflowCancelFinding, AgentWorkflowInvocationId,
    AgentWorkflowInvocationRecord, AgentWorkflowStartExecutor, AgentWorkflowStartFinding,
    AgentWorkflowTerminalStatus, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::json;

/// The code a cancel executor that refuses answers with.
const REFUSED_CODE: &str = "workflow-cancel-refused-by-registry";

/// A start executor that always starts.
struct StartingExecutor;

impl AgentWorkflowStartExecutor for StartingExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _invocation: &'a AgentWorkflowInvocationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentWorkflowStartFinding> {
        Box::pin(async move { Ok(AgentWorkflowStartFinding::Started) })
    }
}

/// A cancel executor that answers every request with one finding.
struct ScriptedCancel {
    finding: AgentWorkflowCancelFinding,
}

impl AgentWorkflowCancelExecutor for ScriptedCancel {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _invocation: &'a AgentWorkflowInvocationRecord,
        _reason: &'a str,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentWorkflowCancelFinding> {
        let finding = self.finding.clone();
        Box::pin(async move { Ok(finding) })
    }
}

/// How the straggler's cancel fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Door {
    /// No cancel executor is wired.
    NoExecutor,
    /// The executor answers `Refused`.
    Refused,
    /// The cancel's retries are spent: the outcome is `Exhausted`.
    Exhausted,
}

/// What is out when the cancel fails, and how the task answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    /// The cancel fails while turn two's model call is still out.
    ModelCallOut,
    /// The proposal is made and held back when the cancel fails, and the
    /// task accepts it.
    ProposalOutThenAccepted,
    /// The proposal is made and held back when the cancel fails, and the
    /// task rejects it; the model's next turn proposes a good one.
    ProposalOutThenRejected,
}

/// The code each door leaves on the cancel's effect record.
fn door_code(door: Door) -> &'static str {
    match door {
        Door::NoExecutor => "workflow-cancel-executor-missing",
        Door::Refused => REFUSED_CODE,
        Door::Exhausted => "workflow-cancel-attempt-failed",
    }
}

/// The outcome a door's cancel is answered with, where the dispatcher does
/// not answer it.
fn door_outcome(door: Door) -> Option<AgentRunEffectOutcome> {
    match door {
        Door::NoExecutor | Door::Refused => None,
        Door::Exhausted => Some(AgentRunEffectOutcome::exhausted(
            door_code(door),
            "every attempt of the cancel failed",
        )),
    }
}

/// The status a door's failed cancel settles at.
fn door_status(door: Door) -> AgentRunEffectStatus {
    match door {
        Door::NoExecutor | Door::Refused => AgentRunEffectStatus::Failed,
        Door::Exhausted => AgentRunEffectStatus::Exhausted,
    }
}

fn workflow_call(call_id: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call_id).expect("call id should be valid"),
        rakka_agent::AgentToolId::new(WORKFLOW_TOOL).expect("tool id should be valid"),
        json!({ "order": "o-1" }),
    )
    .expect("the tool call is bounded")
}

/// Turn one: two workflow invocations, awaited under `Any`.
fn first_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking two workflows and awaiting any.")
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(workflow_call("invoke-2"))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({}),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Synthesizing the child workflow's evidence.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "refunded" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn rejected_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("An empty answer the task's rules reject.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// The straggler world: the goal task is created, both children are
/// started, and the first child's `Completed` result is taken over a router
/// that reaches nothing. The `Any` fan-in resolves, and turn two's model call
/// and the straggler's cancel are both out. Returns the fixture and the
/// straggler's invocation.
async fn world(door: Door, turns: Vec<AgentModelTurn>) -> (Fixture, AgentWorkflowInvocationId) {
    let mut adapter = DeterministicModelAdapter::new();
    for turn in turns {
        adapter = adapter.with_turn(turn);
    }
    let mut dispatcher = ScriptedDispatcher::with_adapter(adapter)
        .with_workflow_start_executor(Arc::new(StartingExecutor));
    if door == Door::Refused {
        dispatcher = dispatcher.with_workflow_cancel_executor(Arc::new(ScriptedCancel {
            finding: AgentWorkflowCancelFinding::Refused {
                code: REFUSED_CODE.to_string(),
                message: "the registry refused the cancel".to_string(),
            },
        }));
    }
    let fixture = Fixture::new(dispatcher)
        .with_delegation(delegation_config_with_fan_in())
        .with_workflow_tools(
            rakka_agent::AgentRunWorkflowConfig::new()
                .with_descriptor(common::workflow_tool_descriptor().with_cancellation_support())
                .expect("the workflow configuration should accept the descriptor"),
        );
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(
                goal_spec_with_workflow(Some(rakka_agent::AgentFanInPolicy::Any)),
                true,
            ),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");

    let invocations: Vec<AgentWorkflowInvocationId> = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .workflow_invocations()
            .keys()
            .cloned()
            .collect()
    };
    assert_eq!(invocations.len(), 2, "both children are started");
    let (first, straggler) = (invocations[0].clone(), invocations[1].clone());
    deliver_result(
        &fixture,
        &AgentExchangeRouter::new(),
        &first,
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the first child's result is taken");
    assert_eq!(
        cancel_effect(&fixture).await.status,
        AgentRunEffectStatus::Ready,
        "the fan-in resolved and the straggler is chased"
    );
    (fixture, straggler)
}

/// The child's terminal result, through the application-owed command, over
/// `router`.
async fn deliver_result(
    fixture: &Fixture,
    router: &AgentExchangeRouter,
    invocation: &AgentWorkflowInvocationId,
    status: AgentWorkflowTerminalStatus,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let command = AgentRunEntityCommand::record_workflow_result(
        &TenantId::new(TENANT),
        invocation.clone(),
        child_workflow_run_id(invocation),
        status,
        None,
        None,
        Some(rakka_agent::AgentContentDigest::sha256_of_bytes(b"result")),
    )
    .expect("the command derives");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(command, router, fixture.now()).await
}

/// Applies `command` to the run over `router`.
async fn apply(
    fixture: &Fixture,
    router: &AgentExchangeRouter,
    command: AgentRunEntityCommand,
) -> AgentRunEntityReply {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(command, router, fixture.now())
        .await
        .expect("the command applies")
}

/// The run's effects, read from durable state.
async fn effects(fixture: &Fixture) -> Vec<AgentRunEffect> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .map(|loop_state| loop_state.effects().to_vec())
        .unwrap_or_default()
}

/// The straggler's cancel effect.
async fn cancel_effect(fixture: &Fixture) -> AgentRunEffect {
    effects(fixture)
        .await
        .into_iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::WorkflowCancelCall)
        .expect("the straggler's cancel is held")
}

/// The cancel disposition the straggler's cell records.
async fn straggler_cancel(
    fixture: &Fixture,
    straggler: &AgentWorkflowInvocationId,
) -> Option<AgentWorkflowCancelDisposition> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .workflow_invocation(straggler)
        .expect("the straggler's cell is held")
        .cancel
        .clone()
}

/// Records the outcome of the one ready effect of `kind` over `router`: the
/// fixture's dispatcher answers it, or, where `outcome` is given, that.
async fn answer_one(
    fixture: &Fixture,
    router: &AgentExchangeRouter,
    kind: AgentRunEffectKind,
    outcome: Option<AgentRunEffectOutcome>,
) -> AgentRunEntityReply {
    let effect = effects(fixture)
        .await
        .into_iter()
        .find(|effect| effect.kind() == kind && effect.status == AgentRunEffectStatus::Ready)
        .unwrap_or_else(|| panic!("a ready {kind} is held"));
    let outcome = match outcome {
        Some(outcome) => outcome,
        None => fixture.dispatcher.answer(&effect).await,
    };
    apply(
        fixture,
        router,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect.result_operation_id(&run_scope()).expect("derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        },
    )
    .await
}

/// The run's status, the code of its terminal reason, and its turn.
async fn standing(fixture: &Fixture) -> (AgentRunStatus, Option<&'static str>, u64) {
    let snapshot = fixture.run_snapshot().await.expect("the run exists");
    (
        snapshot.status,
        snapshot
            .terminal_reason
            .as_ref()
            .map(|reason| reason.code()),
        snapshot.turn,
    )
}

/// Asserts the run is live and was not wound down by its failed cancel.
async fn assert_live(fixture: &Fixture, when: &str) {
    let (status, reason, _) = standing(fixture).await;
    assert_eq!(reason, None, "{when}: the run records no terminal reason");
    assert_ne!(
        status,
        AgentRunStatus::Cancelling,
        "{when}: the run is not winding down"
    );
}

/// Asserts the run ended with its result accepted at `turn`, and the task
/// completed with that result after `rejections` rejections.
async fn assert_completed(fixture: &Fixture, turn: u64, rejections: u32) {
    assert_eq!(
        standing(fixture).await,
        (AgentRunStatus::Completed, Some("result-accepted"), turn),
        "the run ends with its result accepted"
    );
    let task = fixture.task_snapshot().await;
    assert_eq!(
        task.status,
        AgentTaskStatus::Completed,
        "the task completes"
    );
    assert!(task.accepted_result.is_some(), "the task holds the result");
    assert_eq!(task.rejection_count, rejections);
}

/// One door in one order, to the run's end and past the straggler's report.
async fn walk(door: Door, order: Order) {
    let turns = match order {
        Order::ProposalOutThenRejected => vec![first_turn(), rejected_turn(), proposing_turn()],
        Order::ModelCallOut | Order::ProposalOutThenAccepted => {
            vec![first_turn(), proposing_turn()]
        }
    };
    let (fixture, straggler) = world(door, turns).await;
    let unreachable = AgentExchangeRouter::new();

    if order != Order::ModelCallOut {
        answer_one(&fixture, &unreachable, AgentRunEffectKind::ModelCall, None).await;
        assert!(
            fixture
                .run_snapshot()
                .await
                .expect("the run exists")
                .proposal
                .is_some(),
            "the proposal is made and held back"
        );
    }

    answer_one(
        &fixture,
        &unreachable,
        AgentRunEffectKind::WorkflowCancelCall,
        door_outcome(door),
    )
    .await;
    assert_live(&fixture, "after the cancel fails").await;
    let cancel = cancel_effect(&fixture).await;
    assert_eq!(cancel.status, door_status(door));
    assert_eq!(cancel.last_error_code.as_deref(), Some(door_code(door)));
    assert!(
        matches!(
            straggler_cancel(&fixture, &straggler).await,
            Some(AgentWorkflowCancelDisposition::Committed { .. })
        ),
        "the straggler's cell still reads committed"
    );

    if order == Order::ModelCallOut {
        answer_one(&fixture, &unreachable, AgentRunEffectKind::ModelCall, None).await;
    }

    fixture.pump().await.expect("the loop should converge");
    let (turn, rejections) = match order {
        Order::ProposalOutThenRejected => (3, 1),
        Order::ModelCallOut | Order::ProposalOutThenAccepted => (2, 0),
    };
    assert_completed(&fixture, turn, rejections).await;

    deliver_result(
        &fixture,
        &fixture.router,
        &straggler,
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the straggler's own result is taken");
    assert_eq!(
        standing(&fixture).await.0,
        AgentRunStatus::Completed,
        "the straggler's report changes nothing"
    );
}

/// No cancel executor, with the model call out: the turn's proposal is still
/// evaluated, and the run completes.
#[tokio::test]
async fn no_executor_while_the_model_call_is_out() {
    walk(Door::NoExecutor, Order::ModelCallOut).await;
}

/// No cancel executor, with the proposal out and then accepted.
#[tokio::test]
async fn no_executor_while_the_proposal_is_out_then_accepted() {
    walk(Door::NoExecutor, Order::ProposalOutThenAccepted).await;
}

/// No cancel executor, with the proposal out and then rejected: the run
/// begins its next turn and completes.
#[tokio::test]
async fn no_executor_while_the_proposal_is_out_then_rejected() {
    walk(Door::NoExecutor, Order::ProposalOutThenRejected).await;
}

/// A refused cancel, with the model call out.
#[tokio::test]
async fn refused_while_the_model_call_is_out() {
    walk(Door::Refused, Order::ModelCallOut).await;
}

/// A refused cancel, with the proposal out and then accepted.
#[tokio::test]
async fn refused_while_the_proposal_is_out_then_accepted() {
    walk(Door::Refused, Order::ProposalOutThenAccepted).await;
}

/// A refused cancel, with the proposal out and then rejected.
#[tokio::test]
async fn refused_while_the_proposal_is_out_then_rejected() {
    walk(Door::Refused, Order::ProposalOutThenRejected).await;
}

/// A cancel whose retries are spent, with the model call out.
#[tokio::test]
async fn exhausted_while_the_model_call_is_out() {
    walk(Door::Exhausted, Order::ModelCallOut).await;
}

/// A cancel whose retries are spent, with the proposal out and then
/// accepted.
#[tokio::test]
async fn exhausted_while_the_proposal_is_out_then_accepted() {
    walk(Door::Exhausted, Order::ProposalOutThenAccepted).await;
}

/// A cancel whose retries are spent, with the proposal out and then
/// rejected.
#[tokio::test]
async fn exhausted_while_the_proposal_is_out_then_rejected() {
    walk(Door::Exhausted, Order::ProposalOutThenRejected).await;
}

/// A turn whose last tool result came back while the cancel was out stays
/// open for the cancel. When the cancel fails, the turn rests and the run
/// goes on to its next turn and completes.
#[tokio::test]
async fn a_turn_held_for_a_failed_cancel_rests_and_the_run_completes() {
    let lookup_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking it up.")
        .with_tool_call(common::tool_call("lookup"));
    let (fixture, _) = world(
        Door::NoExecutor,
        vec![first_turn(), lookup_turn, proposing_turn()],
    )
    .await;
    let unreachable = AgentExchangeRouter::new();

    answer_one(&fixture, &unreachable, AgentRunEffectKind::ModelCall, None).await;
    answer_one(&fixture, &unreachable, AgentRunEffectKind::ToolCall, None).await;
    let snapshot = fixture.run_snapshot().await.expect("the run exists");
    assert_eq!(
        (snapshot.phase, snapshot.turn),
        (AgentLoopPhase::AwaitingTools, 2),
        "the look-up is back, and the cancel holds the turn"
    );

    answer_one(
        &fixture,
        &unreachable,
        AgentRunEffectKind::WorkflowCancelCall,
        None,
    )
    .await;
    assert_live(&fixture, "after the cancel fails").await;

    fixture.pump().await.expect("the loop should converge");
    assert_completed(&fixture, 3, 0).await;
}

/// A cancel whose outcome is unknown parks the live run. A person answers
/// that it ran and was refused: the run is not wound down, takes its model
/// call's proposal, and completes.
#[tokio::test]
async fn a_park_answered_with_a_failed_cancel_leaves_the_run_going() {
    let (fixture, _) = world(Door::NoExecutor, vec![first_turn(), proposing_turn()]).await;
    let unreachable = AgentExchangeRouter::new();

    let cancel = cancel_effect(&fixture).await;
    answer_one(
        &fixture,
        &unreachable,
        AgentRunEffectKind::WorkflowCancelCall,
        Some(AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        )),
    )
    .await;
    assert_eq!(
        standing(&fixture).await.0,
        AgentRunStatus::WaitingForReconciliation,
        "the unknown cancel parks the run"
    );

    apply(
        &fixture,
        &unreachable,
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "park-answer",
            )
            .expect("the decision key derives"),
            effect_id: cancel.effect_id.clone(),
            generation: cancel.generation,
            resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(AgentRunEffectOutcome::failed(
                    REFUSED_CODE,
                    "the registry refused the cancel",
                )),
            }),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await;
    assert_live(&fixture, "after the park is answered").await;
    assert_eq!(
        cancel_effect(&fixture).await.status,
        AgentRunEffectStatus::Failed
    );

    answer_one(&fixture, &unreachable, AgentRunEffectKind::ModelCall, None).await;
    fixture.pump().await.expect("the loop should converge");
    assert_completed(&fixture, 2, 0).await;
}

/// Unchanged: a run whose result was accepted while the straggler's cancel
/// was out waits for the cancel (issue #123), and when the cancel fails it
/// ends with its result accepted.
#[tokio::test]
async fn an_accepted_run_waiting_for_its_cancel_ends_accepted_when_the_cancel_fails() {
    let (fixture, _) = world(Door::NoExecutor, vec![first_turn(), proposing_turn()]).await;

    answer_one(
        &fixture,
        &fixture.router,
        AgentRunEffectKind::ModelCall,
        None,
    )
    .await;
    let (status, reason, _) = standing(&fixture).await;
    assert_eq!(
        (status, reason),
        (AgentRunStatus::Cancelling, Some("result-accepted")),
        "the result is accepted, and the run waits for the cancel it sent"
    );

    answer_one(
        &fixture,
        &fixture.router,
        AgentRunEffectKind::WorkflowCancelCall,
        None,
    )
    .await;
    assert_eq!(
        standing(&fixture).await,
        (AgentRunStatus::Completed, Some("result-accepted"), 2)
    );
    assert_eq!(
        cancel_effect(&fixture).await.status,
        AgentRunEffectStatus::Failed
    );
}

/// Unchanged: a run that was cancelled, whose straggler cancel then fails,
/// waits for the straggler's own result and ends cancelled.
#[tokio::test]
async fn a_cancelled_run_whose_cancel_fails_still_waits_for_its_straggler() {
    let (fixture, straggler) = world(Door::NoExecutor, vec![first_turn(), proposing_turn()]).await;

    apply(
        &fixture,
        &fixture.router,
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [common::TENANT, common::AGENT, "1"],
            )
            .expect("derivable"),
            reason: "operator stopped the work".to_string(),
        },
    )
    .await;
    fixture.pump().await.expect("the loop should converge");
    let (status, reason, _) = standing(&fixture).await;
    assert_eq!(
        (status, reason),
        (AgentRunStatus::Cancelling, Some("cancellation-requested")),
        "the cancelled run waits for its straggler"
    );
    assert_eq!(
        cancel_effect(&fixture).await.status,
        AgentRunEffectStatus::Failed
    );

    deliver_result(
        &fixture,
        &fixture.router,
        &straggler,
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the straggler's own result is taken");
    fixture.pump().await.expect("the loop should converge");
    assert_eq!(standing(&fixture).await.0, AgentRunStatus::Cancelled);
}
