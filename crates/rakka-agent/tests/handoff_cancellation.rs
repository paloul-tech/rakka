//! Handoff under cancellation: the fence, the chase, and the one-owner rule.
//!
//! Slice 5.1's cancellation half of scenario 38
//! ([specification 8.7 and 8.9](../../docs/plans/rakka-agent/spec.md)): a
//! pending unsent handoff send is fenced by the wind-down and settles its
//! cell failed — the task never saw the transfer — while a transfer whose
//! target generation is already offered routes the cancellation to exactly
//! one owner: the target once it durably accepts, with the source
//! terminalizing `HandedOff` through the result exchange; and an unresolved
//! transfer holds the source's disposition open rather than letting a
//! wind-down terminalize over a responsibility that may have durably moved.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::{
    goal_spec_draft, goal_spec_with_handoff, goal_task_creation_command, handoff_config,
    handoff_target_run_scope, handoff_target_scope, handoff_tool_id, task_definition, Fixture,
    HANDOFF_SKILL,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor, AgentDispatchFuture, AgentExchangeRouter,
    AgentHandoffRecord, AgentHandoffStatus, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunEffect, AgentRunEffectKind, AgentRunScope, AgentRunStatus, AgentTaskEntityCommand,
    AgentTaskEntityStore, AgentTaskHandoffRequest, AgentTaskHandoffStatus, AgentTaskScope,
    AgentTaskStatus, AgentToolCallId, AgentToolCallRequest, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use serde_json::json;

fn handoff_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                handoff_tool_id(),
                json!({ "skill": HANDOFF_SKILL, "reason": "needs billing authority" }),
            )
            .expect("the tool call is bounded"),
        )
}

/// The applying executor of `handoff_record.rs`, reduced to what these tests
/// need: the deduplicated `RecordHandoff` command over the fixture's stores,
/// with the owed exchanges left to the courier.
struct ApplyingHandoffExecutor {
    tasks: common::TaskStore,
    agents: common::AgentStore,
    history: rakka_agent::InMemoryAgentTaskHistoryStore,
    rewake: Arc<dyn rakka_agent::AgentWakeRewakeParker>,
    clock: Arc<AtomicU64>,
}

impl ApplyingHandoffExecutor {
    fn over(fixture: &Fixture) -> Arc<Self> {
        Arc::new(Self {
            tasks: fixture.tasks.clone(),
            agents: fixture.agents.clone(),
            history: fixture.history.clone(),
            rewake: fixture.rewake_parker.clone(),
            clock: fixture.clock.clone(),
        })
    }
}

impl AgentA2aHandoffSendExecutor for ApplyingHandoffExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        handoff: &'a AgentHandoffRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
        Box::pin(async move {
            let scope =
                AgentTaskScope::new(handoff.source_run.tenant().clone(), handoff.task.clone())
                    .expect("the task scope is valid");
            let operation_id = AgentOperationId::new(
                AgentOperationKind::Handoff,
                [
                    handoff.source_run.tenant().as_str(),
                    handoff.task.as_str(),
                    handoff.deduplication_key.as_str(),
                ],
            )
            .expect("the operation id derives");
            let request = AgentTaskHandoffRequest {
                handoff: handoff.handoff.clone(),
                source_agent: handoff.source_run.agent().clone(),
                source_run: handoff.source_run.run().clone(),
                source_generation: handoff.source_generation,
                target: handoff.resolved.agent.clone(),
                target_task_definition: handoff.resolved.task_definition.clone(),
                result_schema: handoff.resolved.result_schema.clone(),
                reason: handoff.reason.clone(),
                policy_revision: handoff.policy_revision,
                context: handoff.context.clone(),
                knowledge_spaces: handoff.resolved.knowledge_spaces.clone(),
            };
            let mut store = AgentTaskEntityStore::new(
                scope,
                self.tasks.clone(),
                self.agents.clone(),
                self.history.clone(),
            )
            .with_wake_timers(self.rewake.clone());
            let now = AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst));
            if let Err(error) = store.recover(now).await {
                return Ok(AgentA2aHandoffFinding::Refused {
                    code: error.code().to_string(),
                    message: error.to_string(),
                });
            }
            let router = AgentExchangeRouter::new();
            let reply = store
                .apply(
                    AgentTaskEntityCommand::RecordHandoff {
                        operation_id,
                        request: Box::new(request),
                    },
                    &router,
                    AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst)),
                )
                .await;
            let echo = store.snapshot().ok().flatten().and_then(|snapshot| {
                snapshot
                    .handoff
                    .filter(|recorded| recorded.handoff == handoff.handoff)
                    .map(|recorded| recorded.target_generation)
            });
            match (reply, echo) {
                (Ok(_), echo) | (Err(_), echo @ Some(_)) => Ok(AgentA2aHandoffFinding::Recorded {
                    target_generation: echo.flatten(),
                    peer_status: "working".to_string(),
                }),
                (Err(error), None) => Ok(AgentA2aHandoffFinding::Refused {
                    code: error.code().to_string(),
                    message: error.to_string(),
                }),
            }
        })
    }
}

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

fn cancel_command(reason: &str) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Cancel {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Cancellation,
            [common::TENANT, common::TASK, "cancel-1"],
        )
        .expect("the operation id derives"),
        reason: reason.to_string(),
    }
}

/// A cancellation that lands between the compare-and-set committing a
/// handoff's send effect and the flush that would hand it to the sink fences
/// the effect in place — and the fence settles the cell
/// `Failed { run-winding-down }` in the same transition, exactly the fenced
/// delegation send's posture. The task never saw the transfer, so a
/// winding-down source never leaves a `Pending` cell under a cancelled
/// effect.
///
/// The sweep is self-checking: it requires at least one crash point to land
/// in the committed-but-unsent window, so flow growth that pushes the window
/// past the sweep fails loudly instead of eroding coverage.
#[tokio::test]
async fn a_cancellation_fence_settles_the_unsent_handoff_cell() {
    use rakka_agent::testkit::CrashPoint;
    let mut fence_observed = false;
    for point in 1..24 {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
                DeterministicModelAdapter::new().with_turn(handoff_turn()),
            ))
            .with_delegation(handoff_config());
            // Deliberately no handoff executor: the send is never answered,
            // so the committed-but-unsent window is what the sweep catches.
            create_goal_task(&fixture).await;

            fixture.runs.crash_at(point, window);
            let _ = fixture.pump().await;
            fixture.runs.survive();

            // The operator cancels the crashed source before any recovery
            // sweep re-drives the flush.
            let now = fixture.now();
            let mut run = fixture.run();
            if run.recover(now).await.is_err() {
                continue;
            }
            let caught_window = {
                let Ok(state) = run.state() else { continue };
                state.loop_state().is_some_and(|loop_state| {
                    loop_state.handoff().is_some()
                        && loop_state.effects().iter().any(|effect| {
                            effect.kind() == AgentRunEffectKind::A2aSendCall && effect.is_pending()
                        })
                })
            };
            if run
                .apply(
                    rakka_agent::AgentRunEntityCommand::Cancel {
                        operation_id: AgentOperationId::new(
                            AgentOperationKind::Cancellation,
                            [common::TENANT, common::AGENT, "1"],
                        )
                        .expect("the operation id derives"),
                        reason: "operator stopped the source".to_string(),
                    },
                    &fixture.router,
                    fixture.now(),
                )
                .await
                .is_err()
            {
                // The crash point landed before acceptance or after the
                // terminal settle; there is nothing to fence.
                continue;
            }
            if caught_window {
                fence_observed = true;
                let state = run.state().expect("state");
                let loop_state = state.loop_state().expect("loop state");
                let cell = loop_state.handoff().expect("the cell exists");
                assert!(
                    matches!(
                        &cell.status,
                        AgentHandoffStatus::Failed { code } if code == "run-winding-down"
                    ),
                    "the fence settles the unsent send's cell in the same transition, got {:?}",
                    cell.status
                );
                assert!(
                    !loop_state.handoff_fenced(),
                    "a settled cell releases the fence into the wind-down"
                );
            }
        }
    }
    assert!(
        fence_observed,
        "no crash point landed in the committed-but-unsent window; the sweep lost its coverage"
    );
}

/// A cancellation landing after the transfer was recorded reaches exactly
/// one owner: the target's accepted generation takes the run-cancel, while
/// the source — whose responsibility durably moved first — terminalizes
/// `HandedOff` through the result exchange, never `Cancelled`.
#[tokio::test]
async fn a_cancellation_mid_transfer_reaches_exactly_one_owner() {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(handoff_turn()),
    ))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor);
    create_goal_task(&fixture).await;
    fixture.instantiate_agent_at(handoff_target_scope()).await;

    // Drive until the transfer is recorded at the task — the send answered,
    // the source's cell `Sent` — but before the courier delivers anything
    // further.
    fixture.pump().await.expect("the loop should converge");
    let task = fixture.task_snapshot().await;
    assert!(task.handoff.is_some(), "the transfer is recorded");

    // The cancellation lands over the offered-or-accepted target generation.
    fixture
        .apply_task_command(cancel_command("operator-cancel"))
        .await
        .expect("the cancellation applies");
    for _ in 0..8 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
        // The target's own settle-and-dispatch pass terminalizes its
        // wind-down: the model effect its acceptance flushed resolves under
        // the fence, and the disposition settles `Cancelled` once nothing is
        // outstanding — the courier half a recovery sweep runs.
        let now = fixture.now();
        let mut target = fixture.run_at(&handoff_target_run_scope());
        if target.recover(now).await.is_ok() {
            let _ = target
                .settle_side_effects(&fixture.router, fixture.now())
                .await;
            let _ = fixture
                .dispatcher
                .drive(&mut target, &fixture.router, fixture.now())
                .await;
        }
        // Both terminal runs owe their ledger settlements — the finalization
        // gate closes only when every escrow child is settled and returned.
        let now = fixture.now();
        let mut source = fixture.run();
        if source.recover(now).await.is_ok() {
            let _ = source
                .settle_side_effects(&fixture.router, fixture.now())
                .await;
        }
    }

    // The source terminalized `HandedOff` — responsibility moved before the
    // cancellation, and the task's durable record says so.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::HandedOff));
    drop(run);

    // The target took the cancellation: its run wound down, and the task
    // finalized `Cancelled` with the ledger closed.
    let mut target = fixture.run_at(&handoff_target_run_scope());
    target.recover(fixture.now()).await.expect("recover");
    let target_state = target.state().expect("state");
    assert_eq!(target_state.status(), Some(AgentRunStatus::Cancelled));
    drop(target);
    let task = fixture.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Cancelled);
    assert_eq!(task.outstanding_escrow, 0, "the ledger closed");
    let provenance = task.handoff.as_deref().expect("the provenance survives");
    assert_eq!(provenance.status, AgentTaskHandoffStatus::Accepted);
}

/// An unresolved transfer holds the source's disposition open: a wind-down
/// racing a sent-but-unresolved handoff stays `Cancelling` rather than
/// terminalizing over a responsibility that may have durably moved.
#[tokio::test]
async fn an_unresolved_transfer_holds_the_source_disposition_open() {
    /// Answers `Recorded` without ever touching the task: the executor's
    /// claim and the task's durable state deliberately disagree, so no
    /// handoff result can ever arrive and the wait is observable.
    struct ClaimingExecutor;
    impl AgentA2aHandoffSendExecutor for ClaimingExecutor {
        fn execute<'a>(
            &'a self,
            _scope: &'a AgentRunScope,
            _intent: &'a AgentRunEffect,
            _handoff: &'a AgentHandoffRecord,
            _credential: Option<&'a AgentEphemeralCredential>,
        ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
            Box::pin(async move {
                Ok(AgentA2aHandoffFinding::Recorded {
                    target_generation: None,
                    peer_status: "working".to_string(),
                })
            })
        }
    }

    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(handoff_turn()),
    ))
    .with_delegation(handoff_config());
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(Arc::new(ClaimingExecutor));
    create_goal_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    // The cell is `Sent` and no resolution can ever arrive. A cancellation
    // now finds a run whose transfer is unresolved.
    fixture
        .apply_task_command(cancel_command("operator-cancel"))
        .await
        .expect("the cancellation applies");
    for _ in 0..8 {
        fixture
            .settle_task_at(&common::task_scope())
            .await
            .expect("the task should settle");
        let now = fixture.now();
        let mut run = fixture.run();
        run.recover(now).await.expect("recover");
        run.settle_side_effects(&fixture.router, now)
            .await
            .expect("the run should settle");
    }

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(
        state.status(),
        Some(AgentRunStatus::Cancelling),
        "an unresolved transfer holds the wind-down open"
    );
    let source = state.run().expect("the record survives");
    let cell = source.loop_state.handoff().expect("the cell survives");
    assert!(
        matches!(cell.status, AgentHandoffStatus::Sent { .. }),
        "the cell stays sent, got {:?}",
        cell.status
    );
    assert!(source.loop_state.awaits_children());
}
