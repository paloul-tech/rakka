//! The outbound A2A send at the dispatch boundary.
//!
//! Slice 4.3's executor-side proofs
//! ([specification 14.4](../../docs/plans/rakka-agent/spec.md)): an absent
//! executor fails the send closed under a stable code, an explicit conflict
//! settles the cell as `Conflicted` — the "same child or an explicit
//! conflict" of specification 6.6 — and either way the parent winds down
//! rather than proceeding as if the child existed. The in-process driver
//! reports through the same finding vocabulary the real pipeline's executor
//! arm maps.

mod common;

use std::sync::Arc;

use common::{
    delegation_config, delegation_tool_id, goal_spec_draft, goal_spec_with_delegation,
    goal_task_creation_command, task_definition, Fixture, AGENT, SKILL, TENANT,
};
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentA2aSendExecutor, AgentA2aSendFinding, AgentDelegationRecord, AgentDelegationStatus,
    AgentDispatchFuture, AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunEffect,
    AgentRunEffectKind, AgentRunEntityCommand, AgentRunScope, AgentRunStatus, AgentToolCallId,
    AgentToolCallRequest, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::json;

fn delegating_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating the translation.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
}

struct ConflictExecutor;

impl AgentA2aSendExecutor for ConflictExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        Box::pin(async move {
            Ok(AgentA2aSendFinding::Conflict {
                code: "delegation-child-conflict".to_string(),
                message: "the peer holds a child this delegation does not own".to_string(),
            })
        })
    }
}

async fn drive(fixture: &Fixture) -> (AgentDelegationStatus, Option<AgentRunStatus>) {
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_delegation(), true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let status = state.status();
    let loop_state = state.loop_state().expect("loop state");
    assert_eq!(loop_state.delegation_count(), 1);
    let cell = loop_state
        .delegations()
        .values()
        .next()
        .expect("the cell exists");
    (cell.status.clone(), status)
}

/// An unwired dispatcher fails the send closed: the cell settles `Failed`
/// under the stable code and the parent winds down — a delegation whose send
/// cannot execute is a failed effect, never a silently pending child.
#[tokio::test]
async fn an_absent_executor_fails_the_send_closed() {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(delegating_turn()),
    ))
    .with_delegation(delegation_config());

    let (status, run_status) = drive(&fixture).await;
    assert_eq!(
        status,
        AgentDelegationStatus::Failed {
            code: "a2a-send-executor-missing".to_string()
        }
    );
    assert_eq!(run_status, Some(AgentRunStatus::Failed));
}

/// A cancellation that lands between the compare-and-set committing a
/// delegation's send effect and the flush that would hand it to the sink
/// fences the effect in place — and the fence settles the cell
/// `Failed { run-winding-down }` in the same transition. A winding-down
/// parent never leaves a `Pending` cell under a cancelled effect for the
/// fan-in slice to misread as an in-flight child.
///
/// The sweep is self-checking: it requires at least one crash point to land
/// in the committed-but-unsent window, so flow growth that pushes the window
/// past the sweep fails loudly instead of eroding coverage.
#[tokio::test]
async fn a_cancellation_fence_settles_the_unsent_delegation_cell() {
    let mut fence_observed = false;
    for point in 1..24 {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
                DeterministicModelAdapter::new().with_turn(delegating_turn()),
            ))
            .with_delegation(delegation_config());
            fixture.instantiate_agent().await;
            fixture
                .apply_task_command(goal_task_creation_command(
                    task_definition(),
                    goal_spec_draft(goal_spec_with_delegation(), true),
                ))
                .await
                .expect("the goal task should create");

            fixture.runs.crash_at(point, window);
            let _ = fixture.pump().await;
            fixture.runs.survive();

            // The operator cancels the crashed coordinator before any
            // recovery sweep re-drives the flush.
            let now = fixture.now();
            let mut run = fixture.run();
            if run.recover(now).await.is_err() {
                continue;
            }
            let caught_window = {
                let Ok(state) = run.state() else { continue };
                state.loop_state().is_some_and(|loop_state| {
                    loop_state.delegation_count() == 1
                        && loop_state.effects().iter().any(|effect| {
                            effect.kind() == AgentRunEffectKind::A2aSendCall && effect.is_pending()
                        })
                })
            };
            if run
                .apply(
                    AgentRunEntityCommand::Cancel {
                        operation_id: AgentOperationId::new(
                            AgentOperationKind::Cancellation,
                            [TENANT, AGENT, "1"],
                        )
                        .expect("the operation id derives"),
                        reason: "operator stopped the coordinator".to_string(),
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
                let cell = loop_state
                    .delegations()
                    .values()
                    .next()
                    .expect("the cell exists");
                assert_eq!(
                    cell.status,
                    AgentDelegationStatus::Failed {
                        code: "run-winding-down".to_string()
                    },
                    "the fence settles the unsent send's cell in the same transition"
                );
            }
            drop(run);

            // Whatever the window, a parent that reaches a terminal status
            // leaves no delegation cell pending.
            let _ = fixture.pump().await;
            let mut run = fixture.run();
            run.recover(fixture.now()).await.expect("recover");
            let state = run.state().expect("state");
            if state.status().is_some_and(AgentRunStatus::is_terminal) {
                if let Some(loop_state) = state.loop_state() {
                    assert!(
                        loop_state
                            .delegations()
                            .values()
                            .all(|cell| cell.status.is_settled()),
                        "a terminal parent left a pending delegation cell (crash point {point})"
                    );
                }
            }
        }
    }
    assert!(
        fence_observed,
        "no crash point produced the committed-but-unsent send window; widen the sweep"
    );
}

/// The peer's explicit conflict settles the cell `Conflicted` — never a
/// silent second child, and never an adoption — and the parent winds down.
#[tokio::test]
async fn a_peer_conflict_settles_the_cell_as_conflicted() {
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new().with_turn(delegating_turn()),
        )
        .with_a2a_send_executor(Arc::new(ConflictExecutor)),
    )
    .with_delegation(delegation_config());

    let (status, run_status) = drive(&fixture).await;
    assert_eq!(
        status,
        AgentDelegationStatus::Conflicted {
            code: "delegation-child-conflict".to_string()
        }
    );
    assert_eq!(run_status, Some(AgentRunStatus::Failed));
}
