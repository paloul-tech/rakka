//! Durable handoff: the record persisted before the send, and the same-task
//! transfer it commits.
//!
//! Slice 5.1's half of scenario 38
//! ([specification 8.9](../../docs/plans/rakka-agent/spec.md),
//! [14.2](../../docs/plans/rakka-agent/spec.md)): a model call to the
//! declared handoff tool commits the handoff record and its outbound send
//! effect in one compare-and-set — strictly before anything reaches the sink
//! — every identity is a pure derivation of the run's `(turn, slot)`
//! coordinate, the source run is fenced until the transfer resolves, and
//! `HandedOff` is recorded only after the target's assignment is durably
//! accepted. The `AgentTaskId` is preserved verbatim: the transfer drives a
//! new assignment generation on the *same* task, never a new task.

mod common;

use std::sync::{Arc, Mutex};

use common::{
    goal_spec_draft, goal_spec_with_handoff, goal_task_creation_command, handoff_config,
    handoff_target_run_scope, handoff_target_scope, handoff_tool_id, run_scope, task_definition,
    ApplyingHandoffExecutor, Fixture, HANDOFF_SKILL, HANDOFF_TARGET,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::SessionMemoryStore;
use rakka_agent::{
    handoff_id_for, AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor, AgentAssignmentGeneration,
    AgentAssignmentStatus, AgentDispatchFuture, AgentHandoffRecord, AgentHandoffStatus,
    AgentLoopPhase, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunCollaborationView, AgentRunEffect, AgentRunEffectKind, AgentRunScope, AgentRunStatus,
    AgentTaskContent, AgentTaskEntityCommand, AgentTaskEntityReply, AgentTaskEntityStore,
    AgentTaskHandoffStatus, AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::json;

fn handoff_turn(arguments: serde_json::Value) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                handoff_tool_id(),
                arguments,
            )
            .expect("the tool call is bounded"),
        )
}

fn handoff_arguments() -> serde_json::Value {
    json!({
        "skill": HANDOFF_SKILL,
        "reason": "needs billing authority",
        "context": ["artifact://ticket-1/notes"],
    })
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A scripted handoff-send executor: answers every send with the finding it
/// was built with and records the records it saw. It never touches the task,
/// so the fence and persisted-before-send windows stay observable.
struct StubHandoffExecutor {
    finding: AgentA2aHandoffFinding,
    seen: Mutex<Vec<AgentHandoffRecord>>,
}

impl StubHandoffExecutor {
    fn recorded() -> Arc<Self> {
        Arc::new(Self {
            finding: AgentA2aHandoffFinding::Recorded {
                target_generation: Some(AgentAssignmentGeneration::new(2)),
                peer_status: "working".to_string(),
            },
            seen: Mutex::new(Vec::new()),
        })
    }
}

impl AgentA2aHandoffSendExecutor for StubHandoffExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        handoff: &'a AgentHandoffRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
        self.seen
            .lock()
            .expect("the record log should not be poisoned")
            .push(handoff.clone());
        let finding = self.finding.clone();
        Box::pin(async move { Ok(finding) })
    }
}

fn handoff_fixture(
    executor: Arc<dyn AgentA2aHandoffSendExecutor>,
    turns: Vec<AgentModelTurn>,
) -> Fixture {
    let mut adapter = DeterministicModelAdapter::new();
    for turn in turns {
        adapter = adapter.with_turn(turn);
    }
    Fixture::new(ScriptedDispatcher::with_adapter(adapter).with_a2a_handoff_executor(executor))
        .with_delegation(handoff_config())
}

/// The fixture definition with a bounded *per-run* allocation: the source's
/// escrow child stays open across the transfer — settlement travels only
/// post-terminal — so the task must still afford the target's generation
/// beside it. A definition that escrows everything to one run makes a
/// handoff deterministically unaffordable, which is its own test below.
fn handoff_task_definition() -> rakka_agent::AgentTaskDefinition {
    let mut per_run = rakka_agent::AgentBudgetAllocation::unbounded();
    per_run.set(rakka_agent::AgentBudgetDimension::LoopIterations, Some(3));
    task_definition()
        .with_budgets(rakka_agent::AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..rakka_agent::AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(per_run)
}

async fn create_goal_task(fixture: &Fixture) {
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            handoff_task_definition(),
            goal_spec_draft(goal_spec_with_handoff(), true),
        ))
        .await
        .expect("the goal task should create");
}

/// The record and its send effect commit together, strictly before the sink
/// sees anything; the receipt marks the cell `Sent`, the turn stays open, and
/// the fenced source parks non-terminal awaiting the resolution.
#[tokio::test]
async fn a_handoff_persists_its_record_before_the_send_and_holds_the_fence() {
    let executor = StubHandoffExecutor::recorded();
    let fixture = handoff_fixture(executor.clone(), vec![handoff_turn(handoff_arguments())]);
    create_goal_task(&fixture).await;

    // Drive the entities without the dispatcher: the run commits the model
    // effect, the scripted turn, and then — evaluating it — the handoff
    // record plus its send effect. Nothing has answered the send yet.
    for _ in 0..8 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
        let now = fixture.now();
        let mut run = fixture.run();
        run.recover(now).await.expect("the run should recover");
        run.settle_side_effects(&fixture.router, now)
            .await
            .expect("the run should settle");
        let outstanding: Vec<_> = run
            .state()
            .expect("the run state should read")
            .loop_state()
            .map(|loop_state| {
                loop_state
                    .effects()
                    .iter()
                    .filter(|effect| effect.is_outstanding())
                    .map(rakka_agent::AgentRunEffect::clone)
                    .collect()
            })
            .unwrap_or_default();
        if outstanding
            .iter()
            .any(|effect| effect.kind() == AgentRunEffectKind::A2aSendCall)
        {
            break;
        }
        let answered = fixture
            .dispatcher
            .drive(&mut run, &fixture.router, fixture.now())
            .await
            .expect("the dispatcher should drive");
        if answered == 0 {
            continue;
        }
    }

    // The cell exists — pending — in the same durable state as the committed
    // effect, and the executor has seen nothing: persisted before send.
    let cell = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("the loop is running");
        let cell = loop_state.handoff().expect("the cell exists").clone();
        assert_eq!(cell.status, AgentHandoffStatus::Pending);
        assert!(loop_state.handoff_fenced());
        cell
    };
    assert!(executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .is_empty());

    // Every identity is the pure derivation of the committing coordinate,
    // and it doubles as the message id and deduplication key.
    let expected =
        handoff_id_for(&run_scope(), cell.record.turn, cell.record.slot).expect("derives");
    assert_eq!(cell.record.handoff, expected);
    assert_eq!(cell.record.a2a_message_id, expected.as_str());
    assert_eq!(cell.record.deduplication_key, expected.as_str());
    assert_eq!(cell.record.requested_skill.as_str(), HANDOFF_SKILL);
    assert_eq!(cell.record.resolved.agent.as_str(), HANDOFF_TARGET);
    assert_eq!(cell.record.task, common::task_scope().task().clone());
    assert_eq!(cell.record.source_generation.get(), 1);

    // Answer the send: the receipt marks the cell `Sent`, the run rests
    // awaiting the resolution — fenced, non-terminal, no tool result yet.
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let cell = loop_state.handoff().expect("the cell survives");
    assert_eq!(
        cell.status,
        AgentHandoffStatus::Sent {
            target_generation: Some(AgentAssignmentGeneration::new(2)),
        }
    );
    assert!(loop_state.handoff_fenced());
    assert_eq!(loop_state.phase(), AgentLoopPhase::AwaitingChildren);
    assert_eq!(state.status(), Some(AgentRunStatus::Running));
    assert!(state.run().is_some_and(|run| run.terminal_reason.is_none()));

    // The executor saw the persisted record verbatim, exactly once.
    let seen = executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].handoff, expected);
}

/// Reads the refusal code the run's recorded session shows the model — the
/// durable form of the failed tool result.
async fn session_refusal_code(
    session: &Arc<rakka_agent::InMemorySessionMemoryStore>,
) -> Option<String> {
    let page = session
        .read(&run_scope(), rakka_agent::SessionMemoryCursor::start())
        .await
        .expect("the session should read");
    page.entries
        .iter()
        .filter(|entry| entry.role == rakka_agent::MemoryEntryRole::ToolResult)
        .find_map(|entry| {
            entry
                .content
                .inline_value()
                .and_then(|value| value.get("error"))
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
}

/// Every interception refusal is a failed tool result under its stable code;
/// the run survives, corrects course on the next turn, and holds no cell.
#[tokio::test]
async fn interception_refusals_fail_the_call_and_the_run_survives() {
    for (turn, code) in [
        // The goal allows only the fixture skills.
        (
            handoff_turn(json!({ "skill": "surgery", "reason": "escalate" })),
            "handoff-skill-not-allowed",
        ),
        // Model output cannot name a target: unknown fields fail the parse.
        (
            handoff_turn(json!({
                "skill": HANDOFF_SKILL,
                "reason": "escalate",
                "target_agent": "attacker",
            })),
            "handoff-invalid-arguments",
        ),
        // The transfer must be the turn's only work.
        (
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Searching, then transferring.")
                .with_tool_call(
                    AgentToolCallRequest::new(
                        AgentToolCallId::new("call-0").expect("call id"),
                        rakka_agent::AgentToolId::new("search").expect("tool id"),
                        json!({}),
                    )
                    .expect("the tool call is bounded"),
                )
                .with_tool_call(
                    AgentToolCallRequest::new(
                        AgentToolCallId::new("call-1").expect("call id"),
                        handoff_tool_id(),
                        handoff_arguments(),
                    )
                    .expect("the tool call is bounded"),
                ),
            "handoff-with-planned-calls",
        ),
    ] {
        let executor = StubHandoffExecutor::recorded();
        let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
        let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
        let fixture = Fixture::new(
            ScriptedDispatcher::with_adapter(
                DeterministicModelAdapter::new()
                    .with_turn(turn)
                    .with_turn(proposing_turn()),
            )
            .with_a2a_handoff_executor(executor)
            .with_tool_result(
                "search",
                AgentTaskContent::inline(json!({ "hits": [] })).expect("bounded"),
            ),
        )
        .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
        .with_delegation(handoff_config());
        create_goal_task(&fixture).await;
        fixture.pump().await.expect("the loop should converge");

        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("loop state");
        assert!(loop_state.handoff().is_none(), "refusal {code} left a cell");
        assert_eq!(
            state.status(),
            Some(AgentRunStatus::Completed),
            "the run should survive the {code} refusal"
        );
        assert_eq!(session_refusal_code(&session).await.as_deref(), Some(code));
    }
}

/// The transfer end to end, scenario 38's clauses together: the same
/// `AgentTaskId` gains a new accepted generation under the target agent; the
/// fenced source terminalizes `HandedOff` strictly after that acceptance;
/// exactly one target run exists; and the target completes the task while
/// the source's session memory stays its own.
#[tokio::test]
async fn a_handoff_transfers_the_task_and_the_source_terminates_handed_off() {
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn(handoff_arguments()))
            .with_turn(proposing_turn()),
    ))
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());
    create_goal_task(&fixture).await;
    fixture.instantiate_agent_at(handoff_target_scope()).await;

    fixture.pump().await.expect("the loop should converge");
    // The courier's remaining legs: the acceptance settled on the task, and
    // the settle pass re-derives and delivers the owed handoff result to the
    // source — one pass to initiate, one to deliver, exactly as a recovery
    // sweep would.
    for _ in 0..4 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
    }

    // The source run terminalized `HandedOff` — only after the target's
    // durable acceptance settled the cell.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::HandedOff));
    let source = state.run().expect("the source record survives");
    assert_eq!(
        source.terminal_reason.as_ref().map(|reason| reason.code()),
        Some("handed-off")
    );
    let cell = source.loop_state.handoff().expect("the cell survives");
    let expected = handoff_id_for(&run_scope(), cell.record.turn, cell.record.slot).expect("id");
    assert_eq!(
        cell.status,
        AgentHandoffStatus::Accepted {
            target_run: handoff_target_run_scope().run().clone(),
            generation: AgentAssignmentGeneration::new(2),
        }
    );
    // The collaboration view carries the same cell — the lockstep surface.
    let view = AgentRunCollaborationView::derive(&source.loop_state);
    assert_eq!(
        view.handoff.as_ref().map(|handoff| handoff.status.as_str()),
        Some("accepted")
    );
    drop(run);

    // The task is the same identity, now owned by the target at generation
    // two — accepted, with the transfer's provenance settled and the result
    // exchange marked delivered.
    let task = fixture.task_snapshot().await;
    assert_eq!(task.scope.task().as_str(), common::TASK);
    let assignment = task.assignment.as_ref().expect("the target owns the task");
    assert_eq!(assignment.agent.as_str(), HANDOFF_TARGET);
    assert_eq!(assignment.generation, AgentAssignmentGeneration::new(2));
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    let provenance = task.handoff.as_deref().expect("the provenance survives");
    assert_eq!(provenance.handoff, expected);
    assert_eq!(provenance.status, AgentTaskHandoffStatus::Accepted);
    assert!(provenance.result_settled);
    assert_eq!(provenance.source_assignment.agent.as_str(), common::AGENT);
    assert_eq!(
        provenance.source_assignment.generation,
        AgentAssignmentGeneration::new(1)
    );

    // The executor saw the record exactly once: the send converged on one
    // transfer.
    assert_eq!(
        executor
            .seen
            .lock()
            .expect("the record log should not be poisoned")
            .len(),
        1
    );

    // Exactly one target run exists, serving the same task; drive it to its
    // own completion — the target, not the source, completes the task.
    for _ in 0..16 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
        let now = fixture.now();
        let mut target = fixture.run_at(&handoff_target_run_scope());
        target.recover(now).await.expect("the target run recovers");
        target
            .settle_side_effects(&fixture.router, fixture.now())
            .await
            .expect("the target run settles");
        let answered = fixture
            .dispatcher
            .drive(&mut target, &fixture.router, fixture.now())
            .await
            .expect("the dispatcher drives the target");
        let terminal = target
            .state()
            .ok()
            .and_then(|state| state.status())
            .is_some_and(AgentRunStatus::is_terminal);
        if terminal && answered == 0 {
            break;
        }
    }
    fixture
        .settle_task_at(&common::task_scope())
        .await
        .expect("the task should settle");
    let mut target = fixture.run_at(&handoff_target_run_scope());
    target.recover(fixture.now()).await.expect("recover");
    let target_state = target.state().expect("state");
    assert_eq!(target_state.status(), Some(AgentRunStatus::Completed));
    assert_eq!(
        target_state.run().map(|run| run.task().clone()),
        Some(common::task_scope().task().clone()),
        "the target run serves the same AgentTaskId"
    );
    drop(target);
    let task = fixture.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);

    // No source session memory crossed: the target's session namespace is
    // its own, and empty of the source's entries.
    let target_session = session
        .read(
            &handoff_target_run_scope(),
            rakka_agent::SessionMemoryCursor::start(),
        )
        .await
        .expect("the target session reads");
    assert!(
        target_session
            .entries
            .iter()
            .all(|entry| !format!("{:?}", entry.content).contains("Transferring the ticket")),
        "the source's turns must not appear in the target's session"
    );
}

/// A replayed handoff command converges on the recorded transfer: the
/// materialized provenance is the deduplication echo past the journal's
/// bounded window, and no second generation is minted.
#[tokio::test]
async fn a_replayed_handoff_send_converges_on_one_transfer() {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(handoff_turn(handoff_arguments())),
    ))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());
    create_goal_task(&fixture).await;
    fixture.instantiate_agent_at(handoff_target_scope()).await;
    fixture.pump().await.expect("the loop should converge");

    let record = executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .first()
        .cloned()
        .expect("the executor saw the record");
    // Replay the send twice more, as a retried attempt would.
    for _ in 0..2 {
        let finding = executor.apply(&record).await;
        assert!(
            matches!(
                finding,
                AgentA2aHandoffFinding::Recorded {
                    target_generation: Some(generation),
                    ..
                } if generation == AgentAssignmentGeneration::new(2)
            ),
            "a replay converges on the recorded transfer, got {finding:?}"
        );
    }
    let task = fixture.task_snapshot().await;
    assert_eq!(task.handoffs, 1, "one transfer, however often replayed");
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(2)
    );
}

/// A re-dispatched handoff send replaying past the journal's bounded
/// deduplication window — modeled by a fresh operation id — converges on the
/// recorded transfer even after the target completed the task: the
/// materialized provenance is the deduplication echo, checked before the
/// terminal guard. A `task-terminal` refusal here would tell the source no
/// transfer was ever recorded and resume it beside the target's completed
/// work.
#[tokio::test]
async fn a_past_window_replay_on_a_terminal_task_echoes_the_recorded_transfer() {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn(handoff_arguments()))
            .with_turn(proposing_turn()),
    ))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());
    create_goal_task(&fixture).await;
    fixture.instantiate_agent_at(handoff_target_scope()).await;
    fixture.pump().await.expect("the loop should converge");
    for _ in 0..4 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
    }

    // Drive the target run to its own completion, terminalizing the task.
    for _ in 0..16 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
        let now = fixture.now();
        let mut target = fixture.run_at(&handoff_target_run_scope());
        target.recover(now).await.expect("the target run recovers");
        target
            .settle_side_effects(&fixture.router, fixture.now())
            .await
            .expect("the target run settles");
        let answered = fixture
            .dispatcher
            .drive(&mut target, &fixture.router, fixture.now())
            .await
            .expect("the dispatcher drives the target");
        let terminal = target
            .state()
            .ok()
            .and_then(|state| state.status())
            .is_some_and(AgentRunStatus::is_terminal);
        if terminal && answered == 0 {
            break;
        }
    }
    fixture
        .settle_task_at(&common::task_scope())
        .await
        .expect("the task should settle");
    let task = fixture.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);

    // The re-dispatched send: same request, a fresh operation id — exactly
    // what an aged-out journal window leaves the entity to answer from its
    // materialized provenance.
    let record = executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .first()
        .cloned()
        .expect("the executor saw the record");
    let operation_id = AgentOperationId::new(
        AgentOperationKind::Handoff,
        [
            record.source_run.tenant().as_str(),
            record.task.as_str(),
            "past-window-redispatch",
        ],
    )
    .expect("the operation id derives");
    let mut store = AgentTaskEntityStore::new(
        AgentTaskScope::new(record.source_run.tenant().clone(), record.task.clone())
            .expect("the task scope is valid"),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    )
    .with_wake_timers(fixture.rewake_parker.clone());
    store
        .recover(fixture.now())
        .await
        .expect("the task recovers");
    let reply = store
        .apply(
            AgentTaskEntityCommand::RecordHandoff {
                operation_id,
                request: Box::new(ApplyingHandoffExecutor::request_for(&record)),
            },
            &fixture.router,
            fixture.now(),
        )
        .await
        .expect("the echo accepts instead of refusing task-terminal");
    assert!(
        matches!(reply, AgentTaskEntityReply::Applied { .. }),
        "the recorded transfer echoes, got {reply:?}"
    );
    let task = fixture.task_snapshot().await;
    assert_eq!(task.handoffs, 1, "one transfer, however late the replay");
    assert_eq!(task.status, AgentTaskStatus::Completed);
}

/// A target that cannot accept restores the source: the handoff offer gets
/// exactly one generation attempt, its refusal reverts the assignment to the
/// stashed source, and the source resumes with the failed tool result — the
/// run survives and completes the task itself.
#[tokio::test]
async fn a_refused_target_restores_the_source_and_the_run_resumes() {
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn(handoff_arguments()))
            .with_turn(proposing_turn()),
    ))
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());
    create_goal_task(&fixture).await;
    // The target agent is deliberately never instantiated: its readiness
    // refuses, and the single-attempt rule resolves the transfer refused.

    fixture.pump().await.expect("the loop should converge");

    // The source resumed past the refusal and completed the task itself.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let source = state.run().expect("the record survives");
    let cell = source.loop_state.handoff().expect("the cell survives");
    assert!(
        matches!(&cell.status, AgentHandoffStatus::Refused { code } if code == "agent-not-instantiated"),
        "the cell settles under the target's refusal, got {:?}",
        cell.status
    );
    assert!(!source.loop_state.handoff_fenced(), "the fence released");
    drop(run);
    assert_eq!(
        session_refusal_code(&session).await.as_deref(),
        Some("agent-not-instantiated")
    );

    // The task: the source assignment was restored exactly — same agent,
    // same generation — the transfer settled refused, and the source's own
    // proposal completed the task.
    let task = fixture.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);
    let provenance = task.handoff.as_deref().expect("the provenance survives");
    assert!(matches!(
        provenance.status,
        AgentTaskHandoffStatus::Refused { .. }
    ));
    assert!(provenance.result_settled);
    assert!(task.accepted_result.is_some());
}

/// Records persisted before this slice decode without the new fields, and a
/// run holding no handoff serializes byte-identically to one persisted
/// before the field existed.
#[tokio::test]
async fn pre_slice_records_decode_without_the_handoff_fields() {
    use rakka_persistence::DurableStateStore;
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(proposing_turn()),
    ));
    fixture.instantiate_agent().await;
    fixture.create_task().await;
    fixture.pump().await.expect("the loop should converge");

    // The task record: stripping the handoff fields decodes to the defaults.
    let task_entry = fixture
        .tasks
        .load(&common::task_scope().persistence_id())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let mut encoded = serde_json::to_value(&task_entry.state).expect("encodes");
    let task_value = encoded
        .as_object_mut()
        .expect("an object")
        .get_mut("task")
        .and_then(serde_json::Value::as_object_mut)
        .expect("the task object");
    assert!(
        !task_value.contains_key("handoff"),
        "a task that never handed off serializes without the field"
    );
    assert!(
        !task_value.contains_key("handoffs"),
        "a zero handoff count serializes invisibly"
    );
    task_value.remove("handoff");
    task_value.remove("handoffs");
    let decoded: rakka_agent::AgentTaskState =
        serde_json::from_value(encoded).expect("a pre-slice task decodes");
    assert!(decoded.task().expect("the task").handoff.is_none());
    assert_eq!(decoded.task().expect("the task").handoffs, 0);

    // The run's loop state: no handoff cell means no field on the wire, and
    // a stripped record decodes without one.
    let run_entry = fixture
        .runs
        .load(&run_scope().persistence_id())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let encoded = serde_json::to_value(&run_entry.state).expect("encodes");
    let loop_value = encoded
        .pointer("/run/loop_state")
        .and_then(serde_json::Value::as_object)
        .expect("the loop state object");
    assert!(
        !loop_value.contains_key("handoff"),
        "a run that never handed off serializes without the field"
    );

    // The collaboration view: empty without a handoff, and its snapshot
    // serialization is unchanged for pre-handoff runs.
    let loop_state = run_entry
        .state
        .run()
        .expect("the run record")
        .loop_state
        .clone();
    let view = AgentRunCollaborationView::derive(&loop_state);
    assert!(view.is_empty());
    let encoded = serde_json::to_value(&view).expect("encodes");
    assert!(
        !encoded
            .as_object()
            .expect("an object")
            .contains_key("handoff"),
        "an absent handoff never reaches the serialized view"
    );
}
