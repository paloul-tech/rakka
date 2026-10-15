//! Issue #106: an exhausted handoff send parks behind a reconciliation
//! checkpoint, and the task's handoff result releases it.
//!
//! A handoff send whose retry budget spends out without an answer parks its
//! run `Indeterminate`: the executor's probe could not establish whether the
//! task recorded the transfer
//! ([specification 8.9](../../docs/plans/rakka-agent/spec.md)). These worlds
//! lose the send's answer after the task recorded the transfer, or before it
//! reached the task, and drive the real task, the scripted dispatcher, and
//! the run entity to quiescence.

mod common;

use std::sync::Arc;

use common::{
    agent_scope, goal_spec_draft, goal_spec_with_handoff, goal_task_creation_command,
    handoff_config, handoff_target_scope, handoff_tool_id, task_definition, task_scope,
    ApplyingHandoffExecutor, Fixture, HANDOFF_SKILL,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor, AgentBudgetAllocation,
    AgentBudgetCeilings, AgentBudgetDimension, AgentCheckpoint, AgentCheckpointDecision,
    AgentCheckpointKind, AgentDispatchError, AgentDispatchFuture, AgentHandoffRecord,
    AgentHandoffStatus, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentReconciliationDecision, AgentRun, AgentRunEffect, AgentRunEffectKind,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunScope, AgentRunStatus,
    AgentRunTerminalReason, AgentSegmentOperation, AgentTaskContent, AgentTaskDefinition,
    AgentTaskHandoffStatus, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    InMemoryAgentSegmentSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEphemeralCredential, PrincipalRef};
use serde_json::json;

fn handoff_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                handoff_tool_id(),
                json!({ "skill": HANDOFF_SKILL, "reason": "needs billing authority" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A bounded per-run allocation, so the source's open escrow leaves headroom
/// for the target's generation; `handoff_record.rs` documents why an
/// exact-fit budget makes every transfer unaffordable.
fn handoff_task_definition() -> AgentTaskDefinition {
    let mut per_run = AgentBudgetAllocation::unbounded();
    per_run.set(AgentBudgetDimension::LoopIterations, Some(3));
    task_definition()
        .with_budgets(AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(per_run)
}

/// A handoff executor whose answer is always lost.
///
/// With `applying` set, the transfer reaches the task first: the write
/// landed, and only the reply was lost. Without it, the send never reached
/// the task. Either way the attempt fails retryably. The scripted
/// dispatcher records that as a spent budget (`Exhausted`), which is what
/// the real dispatcher records once the executor's probe has failed on
/// every attempt.
struct LostAnswerExecutor {
    applying: Option<Arc<ApplyingHandoffExecutor>>,
}

impl AgentA2aHandoffSendExecutor for LostAnswerExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        handoff: &'a AgentHandoffRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
        Box::pin(async move {
            if let Some(applying) = &self.applying {
                let _ = applying.apply(handoff).await;
            }
            Err(AgentDispatchError::Invocation {
                code: "handoff-unrecorded",
                message: "the answer was lost and the probe could not read the task".to_string(),
            })
        })
    }
}

/// A world one turn from committing the transfer. `record_transfer` says
/// whether the lost send reached the task, and `target_ready` whether the
/// target agent exists to accept it (a missing target refuses).
async fn parking_world(
    record_transfer: bool,
    target_ready: bool,
) -> (Fixture, Arc<InMemoryAgentSegmentSink>) {
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn())
            .with_turn(proposing_turn()),
    ))
    .with_delegation(handoff_config())
    .with_segments(segments.clone());
    let executor = Arc::new(LostAnswerExecutor {
        applying: record_transfer.then(|| ApplyingHandoffExecutor::over(&fixture)),
    });
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor);
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            handoff_task_definition(),
            goal_spec_draft(goal_spec_with_handoff(), true),
        ))
        .await
        .expect("the goal task creates");
    if target_ready {
        fixture.instantiate_agent_at(handoff_target_scope()).await;
    }
    (fixture, segments)
}

/// Drives the world to quiescence. First the run's loop and the dispatcher;
/// then the task's remaining legs (the offer, the target's answer, and the
/// handoff result back to the source); then the run again, so a resumed
/// source finishes its task.
async fn settle(fixture: &Fixture) {
    fixture.pump().await.expect("the loop quiesces");
    for _ in 0..4 {
        fixture
            .settle_task_at(&task_scope())
            .await
            .expect("the task settles");
    }
    fixture.pump().await.expect("the loop quiesces again");
}

/// The run's durable record, re-materialized: recovery is a read path.
async fn run_record(fixture: &Fixture) -> AgentRun {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("the run recovers");
    run.state()
        .expect("the state reads")
        .run()
        .expect("the run exists")
        .clone()
}

fn send_effect(run: &AgentRun) -> &AgentRunEffect {
    run.loop_state
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::A2aSendCall)
        .expect("the handoff send is held")
}

/// How many `checkpoint-open` segments the run emitted: one per park.
fn checkpoint_opens(segments: &InMemoryAgentSegmentSink) -> usize {
    segments
        .segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointOpen))
        .count()
}

/// Issue #106, Ask 1: a send that never reached the task parks behind its
/// reconciliation checkpoint and waits there. No notice is owed, so only a
/// decision can end the wait, and the checkpoint is what offers one.
#[tokio::test]
async fn an_exhausted_send_that_never_reached_the_task_waits_behind_its_checkpoint() {
    let (fixture, segments) = parking_world(false, true).await;
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(run.status, AgentRunStatus::WaitingForReconciliation);
    let send = send_effect(&run);
    assert_eq!(send.status, AgentRunEffectStatus::Indeterminate);
    let open = run.loop_state.open_checkpoints();
    assert_eq!(open.len(), 1, "the park carries its checkpoint: {open:?}");
    assert_eq!(
        open[0].kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(
        open[0].checkpoint_id,
        AgentCheckpoint::id_for_effect(
            &send.effect_id,
            send.generation,
            AgentCheckpointKind::IndeterminateEffectReconciliation,
        )
    );
    assert_eq!(open[0].bound_effect.effect_id, send.effect_id);
    assert_eq!(
        checkpoint_opens(&segments),
        1,
        "the park emitted its checkpoint-open segment"
    );
    let task = fixture.task_snapshot().await;
    assert!(
        task.handoff.is_none(),
        "the transfer never reached the task"
    );
}

/// Issue #106, Ask 2: the transfer landed and only the answer was lost. The
/// run parks behind its checkpoint, and the task's acceptance (its durable
/// record of the transfer) releases it into `HandedOff` with no decision.
#[tokio::test]
async fn a_lost_answer_whose_transfer_landed_hands_off_once_the_task_answers() {
    let (fixture, segments) = parking_world(true, true).await;
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(
        run.status,
        AgentRunStatus::HandedOff,
        "the task's acceptance released the park"
    );
    assert!(matches!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::HandedOff { .. })
    ));
    assert_eq!(
        send_effect(&run).status,
        AgentRunEffectStatus::Succeeded,
        "the task's record proved the send executed"
    );
    assert!(
        run.loop_state.open_checkpoints().is_empty(),
        "the release dropped the checkpoint"
    );
    assert_eq!(
        checkpoint_opens(&segments),
        1,
        "the send parked behind its checkpoint first"
    );
    let task = fixture.task_snapshot().await;
    let provenance = task
        .handoff
        .as_deref()
        .expect("the task recorded the transfer");
    assert!(matches!(
        provenance.status,
        AgentTaskHandoffStatus::Accepted
    ));
}

/// Ruling 1, end to end: the target refused, so the task restored the source,
/// and its refusal releases the park. The source resumes and finishes the
/// task itself.
#[tokio::test]
async fn a_lost_answer_the_target_refused_resumes_the_source() {
    let (fixture, segments) = parking_world(true, false).await;
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(
        run.status,
        AgentRunStatus::Completed,
        "the source resumed past the refusal and finished the task"
    );
    let cell = run.loop_state.handoff().expect("the cell survives");
    assert!(
        matches!(&cell.status, AgentHandoffStatus::Refused { code } if code == "agent-not-instantiated"),
        "the cell settles under the target's refusal, got {:?}",
        cell.status
    );
    assert!(run.loop_state.open_checkpoints().is_empty());
    assert_eq!(checkpoint_opens(&segments), 1, "the send parked first");
    let task = fixture.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);
}

/// Ruling 2, end to end: a send that never reached the task parks, and the
/// operator's `AbandonAndFail` through the checkpoint fails the run instead of
/// resuming it.
#[tokio::test]
async fn abandoning_a_send_that_never_reached_the_task_fails_the_run() {
    let (fixture, _segments) = parking_world(false, true).await;
    settle(&fixture).await;
    let checkpoint_id = run_record(&fixture)
        .await
        .loop_state
        .open_checkpoints()
        .first()
        .expect("the park carries its checkpoint")
        .checkpoint_id
        .clone();

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "abandon",
            )
            .expect("the decision key derives"),
            checkpoint_id,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::AbandonAndFail,
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the abandonment applies");
    settle(&fixture).await;

    let run = run_record(&fixture).await;
    assert_eq!(run.status, AgentRunStatus::Failed);
    match &run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, .. }) => {
            assert_eq!(code, "reconciliation-abandoned");
        }
        other => panic!("expected the abandoned send to fail the run, found {other:?}"),
    }
    let cell = run.loop_state.handoff().expect("the cell survives");
    assert!(
        matches!(&cell.status, AgentHandoffStatus::Failed { code, .. } if code == "reconciliation-abandoned"),
        "got {:?}",
        cell.status
    );
}
