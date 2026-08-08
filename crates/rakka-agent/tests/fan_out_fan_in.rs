//! Durable fan-out groups and deterministic fan-in
//! ([specification 8.4 and 8.7](../../docs/plans/rakka-agent/spec.md),
//! scenario 27's loop half).
//!
//! A fan-out turn commits its delegations, joins them to the one durable
//! group — opened with a policy taken from trusted state, never model output
//! — and the declared await verb closes membership. The run then rests
//! `AwaitingChildren` with no outstanding effect and no resident claim; each
//! child's terminal result is a durable, deduplicated exchange that
//! re-activates the owner, and the policy resolves as a pure function of the
//! durable cells, so arrival order can never change the outcome. The
//! resolution is the awaiting call's bounded tool result: evidence the model
//! consumes and proposes from — never a completed parent task.

mod common;

use std::sync::{Arc, Mutex};

use common::{
    delegation_config_with_fan_in, delegation_tool_id, fan_in_tool_id, goal_spec_draft,
    goal_spec_with_fan_out, goal_task_creation_command, run_scope, task_definition, task_scope,
    Fixture, SKILL, SKILL_2, TENANT,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    delegation_result_operation_id, AgentA2aSendExecutor, AgentA2aSendFinding,
    AgentDelegationRecord, AgentDelegationReport, AgentDelegationStatus, AgentDispatchFuture,
    AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentFanInPolicy, AgentLoopPhase, AgentModelTurn, AgentRunEffect, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunEntityCommand, AgentRunScope, AgentRunStatus, AgentTaskContent,
    AgentTaskId, AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest, TenantId,
    AGENT_DELEGATION_RESULT_PAYLOAD_TYPE, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentEphemeralCredential};
use rakka_core::InMemoryMetricsRecorder;
use serde_json::json;

/// A send executor that names each child after the skill it serves, so a
/// two-skill fan-out creates two distinct children.
struct SkillNamedExecutor {
    seen: Mutex<Vec<AgentDelegationRecord>>,
    fail_skill: Option<&'static str>,
}

impl SkillNamedExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            fail_skill: None,
        })
    }

    fn failing(skill: &'static str) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            fail_skill: Some(skill),
        })
    }
}

fn child_task_for(skill: &str) -> AgentTaskId {
    AgentTaskId::new(format!("child-{skill}")).expect("task id should be valid")
}

impl AgentA2aSendExecutor for SkillNamedExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        self.seen
            .lock()
            .expect("the record log should not be poisoned")
            .push(delegation.clone());
        let skill = delegation.requested_skill.as_str().to_string();
        let refused = self.fail_skill == Some(skill.as_str());
        Box::pin(async move {
            if refused {
                return Ok(AgentA2aSendFinding::Refused {
                    code: "peer-unavailable".to_string(),
                    message: "the specialist surface refused the send".to_string(),
                });
            }
            Ok(AgentA2aSendFinding::Sent {
                child_task: child_task_for(&skill),
                child_run: None,
                peer_status: "submitted".to_string(),
            })
        })
    }
}

fn fan_out_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Fanning out to both specialists and awaiting them.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-2").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL_2, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
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
        .with_text("Synthesizing the children's evidence.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "synthesized" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn fan_out_fixture(executor: Arc<SkillNamedExecutor>) -> Fixture {
    Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(fan_out_turn())
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor),
    )
    .with_delegation(delegation_config_with_fan_in())
}

async fn create_fan_out_task(fixture: &Fixture, policy: Option<AgentFanInPolicy>) {
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_fan_out(policy, None), true),
        ))
        .await
        .expect("the goal task should create");
}

/// The committed members, read from the durable cells: `(delegation id,
/// created child task)` per settled cell, in deterministic map order.
async fn committed_children(
    fixture: &Fixture,
) -> Vec<(rakka_agent::AgentDelegationId, AgentTaskId)> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    loop_state
        .delegations()
        .iter()
        .filter_map(|(id, cell)| match &cell.status {
            AgentDelegationStatus::ChildCreated { child_task, .. } => {
                Some((id.clone(), child_task.clone()))
            }
            _ => None,
        })
        .collect()
}

async fn parked_phase(fixture: &Fixture) -> (AgentLoopPhase, Option<AgentRunStatus>, usize) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    let outstanding = loop_state
        .effects()
        .iter()
        .filter(|effect| effect.is_outstanding())
        .count();
    (loop_state.phase(), state.status(), outstanding)
}

/// One child's terminal report, exactly as the child task's owed exchange
/// carries it.
fn child_result_envelope(
    fixture: &Fixture,
    delegation: &rakka_agent::AgentDelegationId,
    child_task: &AgentTaskId,
    status: AgentTaskStatus,
) -> AgentExchangeEnvelope {
    let tenant = TenantId::new(TENANT);
    let operation_id =
        delegation_result_operation_id(&tenant, delegation).expect("the operation id derives");
    let report = AgentDelegationReport {
        delegation: delegation.clone(),
        child_task: child_task.clone(),
        child_run: None,
        status,
        terminal_reason: (status != AgentTaskStatus::Completed)
            .then(|| "cancellation-requested".to_string()),
        result_digest: (status == AgentTaskStatus::Completed).then(|| {
            AgentTaskContent::inline(json!({ "answer": "done" }))
                .expect("the content is inline-bounded")
                .digest()
        }),
        descendants_created: 0,
    };
    let payload = AgentExchangePayload::encode(AGENT_DELEGATION_RESULT_PAYLOAD_TYPE, &report)
        .expect("the report encodes");
    let child_scope =
        AgentTaskScope::new(tenant, child_task.clone()).expect("the child scope is valid");
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::DelegationResult,
        AgentEntityAddress::Task(child_scope),
        AgentEntityAddress::Run(run_scope()),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid")
}

async fn deliver(fixture: &Fixture, envelope: &AgentExchangeEnvelope) -> bool {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    reply.result().is_accepted()
}

/// Delivers one exchange and returns the refusal code it was answered with,
/// when it was refused.
async fn deliver_code(fixture: &Fixture, envelope: &AgentExchangeEnvelope) -> Option<String> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    reply
        .result()
        .status()
        .rejection_code()
        .map(ToString::to_string)
}

/// The fan-out turn opens the group with the trusted policy, the await verb
/// closes it, and the run rests `AwaitingChildren` — status `Running`, no
/// outstanding effect, nothing resident. The first child result records
/// without resolving an `All` group; the second resolves it, the awaiting
/// call receives the bounded table, and the resumed model proposes the
/// parent's own result — child completion never completes the parent task by
/// itself.
#[tokio::test]
async fn a_fan_out_parks_awaiting_children_and_results_resume_it_once() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    let (phase, status, outstanding) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(status, Some(AgentRunStatus::Running));
    assert_eq!(
        outstanding, 0,
        "a parked fan-in holds no outstanding effect"
    );

    let group = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let cell = state
            .loop_state()
            .expect("the loop is running")
            .fan_in()
            .expect("the group exists")
            .clone();
        cell
    };
    assert!(group.closed, "the await verb closed the membership");
    assert_eq!(group.members.len(), 2);
    assert_eq!(group.policy, AgentFanInPolicy::All);
    assert!(group.resolution.is_none());

    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);

    // The first result records the child outcome without resolving `All`.
    let first = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &first).await);
    let (phase, _, _) = parked_phase(&fixture).await;
    assert_eq!(
        phase,
        AgentLoopPhase::AwaitingChildren,
        "one of two results does not resolve an all-members policy"
    );

    // The second resolves, resumes, and the scripted model proposes.
    let second = child_result_envelope(
        &fixture,
        &children[1].0,
        &children[1].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &second).await);
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("loop state");
    let group = loop_state.fan_in().expect("the group is retained");
    let resolution = group.resolution.as_ref().expect("the group resolved");
    assert!(resolution.satisfied);
    assert_eq!(resolution.code, "all-settled");
    for (delegation, _) in &children {
        let cell = loop_state.delegation(delegation).expect("the cell exists");
        let result = cell.result.as_ref().expect("the child result recorded");
        assert_eq!(result.status, AgentTaskStatus::Completed);
    }

    // The parent task completed through its own proposal — the evidence fed
    // the model, not the task's decision door.
    let task = fixture.task_snapshot().await;
    assert!(task.accepted_result.is_some());
}

/// Under `Any`, the first success resolves the group and resumes the run
/// exactly once; the straggler's later result is still accepted and recorded
/// as evidence, with no second resumption — the resolution is absorbing.
#[tokio::test]
async fn any_resolves_on_the_first_success_and_the_straggler_records_quietly() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, Some(AgentFanInPolicy::Any)).await;
    fixture.pump().await.expect("the loop should converge");

    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);

    let first = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &first).await);
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed)
    );

    // The straggler reports late: accepted, recorded, and nothing resumes —
    // the run is already terminal and the resolution absorbing.
    let straggler = child_result_envelope(
        &fixture,
        &children[1].0,
        &children[1].1,
        AgentTaskStatus::Cancelled,
    );
    assert!(deliver(&fixture, &straggler).await);
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("loop state");
    let cell = loop_state
        .delegation(&children[1].0)
        .expect("the cell exists");
    assert_eq!(
        cell.result.as_ref().expect("the evidence recorded").status,
        AgentTaskStatus::Cancelled
    );
    let resolution = loop_state
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the group resolved");
    assert_eq!(resolution.code, "any-satisfied");
}

/// A duplicate delivery of one logical result converges on the recorded
/// result without a second transition — and a conflicting later delivery
/// under the same derived operation id cannot overwrite the first writer:
/// the journal answers in-window replays, and the cell's recorded result is
/// the durable fence behind it.
#[tokio::test]
async fn duplicate_results_accept_idempotently() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;

    let envelope = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &envelope).await);
    // In-window duplicate: the journal answers the replay.
    assert!(deliver(&fixture, &envelope).await);
    assert!(deliver(&fixture, &envelope).await);
    // A conflicting duplicate — the same derived operation id claiming a
    // different outcome — cannot become the recorded result: first writer
    // wins, whichever layer answers the replay.
    let conflicting = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Cancelled,
    );
    let _ = deliver(&fixture, &conflicting).await;

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let cell = loop_state
        .delegation(&children[0].0)
        .expect("the cell exists");
    assert_eq!(
        cell.result.as_ref().expect("one result").status,
        AgentTaskStatus::Completed
    );
}

/// Forged and misaddressed reports refuse on the parent's own durable state:
/// a sender that is not the created child, an unknown delegation, and a
/// non-terminal claimed status are each refused under their stable code.
#[tokio::test]
async fn forged_reports_refuse_on_the_parents_durable_state() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;

    // A sender that is not the child the delegation created.
    let mut forged = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    let impostor = AgentTaskScope::new(
        TenantId::new(TENANT),
        AgentTaskId::new("impostor").expect("task id should be valid"),
    )
    .expect("the scope is valid");
    forged = AgentExchangeEnvelope::new(
        forged.operation_id().clone(),
        AgentExchangeKind::DelegationResult,
        AgentEntityAddress::Task(impostor),
        AgentEntityAddress::Run(run_scope()),
        forged.payload().clone(),
        AgentCorrelationId::new(forged.operation_id().as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid");
    // The exact code matters: it is one of the four definitive answers the
    // child-side settle rule advances on, so a misrouted report can never
    // leave a real child retrying forever.
    assert_eq!(
        deliver_code(&fixture, &forged).await.as_deref(),
        Some("delegation-result-forged")
    );

    // A delegation this run never committed.
    let foreign = rakka_agent::delegation_id_for(&run_scope(), 99, 0).expect("derives");
    let unknown = child_result_envelope(
        &fixture,
        &foreign,
        &AgentTaskId::new("child-somewhere").expect("task id should be valid"),
        AgentTaskStatus::Completed,
    );
    assert_eq!(
        deliver_code(&fixture, &unknown).await.as_deref(),
        Some("delegation-result-unknown-delegation")
    );

    // The cell records nothing from either refusal.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let cell = state
        .loop_state()
        .expect("loop state")
        .delegation(&children[0].0)
        .expect("the cell exists")
        .clone();
    assert!(cell.result.is_none());
}

/// A cancelled parent records a late child result as evidence and resumes
/// nothing: the wait belongs to a run that is still going somewhere.
#[tokio::test]
async fn a_wound_down_parent_records_evidence_without_resuming() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, "parent-cancel", "1"],
            )
            .expect("the operation id derives"),
            reason: "operator-cancelled".to_string(),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the cancellation applies");

    let (phase_before, _, outstanding_before) = parked_phase(&fixture).await;
    let envelope = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &envelope).await);

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let status = state.status().expect("the run exists");
    assert!(
        status.is_terminal() || status == AgentRunStatus::Cancelling,
        "the late evidence never resumes a wound-down run, but it is recorded"
    );
    let cell = state
        .loop_state()
        .expect("loop state")
        .delegation(&children[0].0)
        .expect("the cell exists")
        .clone();
    assert!(cell.result.is_some());
    // "Resumes nothing" pinned by state, not status alone: no new effect was
    // committed and no new turn began. The phase may advance to `Complete` —
    // these children are fictional, so their delegation-cancels refuse
    // definitively and release the subtree quiesce condition, letting the
    // wind-down finish on this evidence instead of parking on children that
    // can never report ([specification 8.7](../../docs/plans/rakka-agent/spec.md)).
    let (phase_after, _, outstanding_after) = parked_phase(&fixture).await;
    assert!(
        phase_after == phase_before || phase_after == AgentLoopPhase::Complete,
        "the late evidence parked or completed the wind-down, got {phase_after:?}"
    );
    assert_eq!(outstanding_after, outstanding_before);
}

/// The parked deadline fires through the durable command: before it is due
/// nothing moves; past due the stragglers are marked timed out — a
/// parent-side disposition — the policy resolves, and the model consumes the
/// table. Firing twice converges.
#[tokio::test]
async fn the_deadline_marks_stragglers_and_resolves_timed_out() {
    let executor = SkillNamedExecutor::new();
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Fanning out with a wait deadline.")
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("delegate-1").expect("call id"),
                                delegation_tool_id(),
                                json!({ "skill": SKILL, "input": { "text": "hello" } }),
                            )
                            .expect("the tool call is bounded"),
                        )
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("await-1").expect("call id"),
                                fan_in_tool_id(),
                                json!({ "deadline": 5_000 }),
                            )
                            .expect("the tool call is bounded"),
                        ),
                )
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor),
    )
    .with_delegation(delegation_config_with_fan_in());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    let (phase, _, _) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren);

    let fire = |suffix: &'static str| AgentRunEntityCommand::FireFanInDeadline {
        operation_id: rakka_agent::AgentOperationId::new(
            rakka_agent::AgentOperationKind::Command,
            [TENANT, "fan-in-deadline", suffix],
        )
        .expect("the operation id derives"),
    };

    // Not due yet: the fixture clock is far below the deadline.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(fire("early"), &fixture.router, fixture.now())
        .await
        .expect("the early fire applies");
    let (phase, _, _) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren, "not due, no move");

    // Past due: the straggler times out, the policy resolves unsatisfied,
    // and the resumed model proposes from the evidence.
    fixture
        .clock
        .store(10_000, std::sync::atomic::Ordering::SeqCst);
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(fire("due"), &fixture.router, fixture.now())
        .await
        .expect("the due fire applies");
    run.apply(fire("again"), &fixture.router, fixture.now())
        .await
        .expect("a duplicate fire converges");
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let resolution = state
        .loop_state()
        .expect("loop state")
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the deadline resolved the group");
    assert!(!resolution.satisfied);
    assert_eq!(resolution.code, "timed-out");

    // A late result racing the fired deadline is still accepted as
    // evidence: the cell records it, and the absorbing resolution does not
    // change — timed out is what the parent decided from, however the
    // straggler answers afterwards.
    let children = committed_children(&fixture).await;
    let late = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &late).await);
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("loop state");
    assert_eq!(
        loop_state
            .fan_in()
            .expect("the group is retained")
            .resolution
            .as_ref()
            .expect("still resolved")
            .code,
        "timed-out",
        "the absorbing resolution ignores the late result"
    );
    assert_eq!(
        loop_state
            .delegation(&children[0].0)
            .expect("the cell exists")
            .result
            .as_ref()
            .expect("the evidence recorded")
            .status,
        AgentTaskStatus::Completed
    );
}

/// A definitively failed send is a fan-in disposition, not a coordinator
/// failure: the cell settles, the failure reaches the model as the call's
/// failed tool result, the policy branches on it, and the run survives to
/// synthesize from the children it has.
#[tokio::test]
async fn a_failed_send_is_a_fan_in_disposition_not_a_coordinator_failure() {
    let executor = SkillNamedExecutor::failing(SKILL_2);
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    // One child exists; the other send failed definitively and settled its
    // cell — the group closed over both.
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 1, "one send failed to create a child");
    let (phase, status, _) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(
        status,
        Some(AgentRunStatus::Running),
        "the failed send did not wind the coordinator down"
    );

    // The surviving child completes; `All` resolves — unsatisfied, with the
    // failed member an explicit disposition — and the run proposes.
    let envelope = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &envelope).await);
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let resolution = state
        .loop_state()
        .expect("loop state")
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the group resolved");
    assert!(!resolution.satisfied);
}

/// The run's dispatched-but-unanswered effects, each paired with the skill it
/// delegates when it is an A2A send.
async fn dispatched_effects(fixture: &Fixture) -> Vec<(AgentRunEffect, Option<String>)> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let Some(loop_state) = state.loop_state() else {
        return Vec::new();
    };
    loop_state
        .effects()
        .iter()
        .filter(|effect| effect.is_outstanding())
        .map(|effect| {
            let skill = match &effect.request {
                AgentRunEffectRequest::A2aSend { delegation } => {
                    Some(delegation.requested_skill.as_str().to_string())
                }
                _ => None,
            };
            (effect.clone(), skill)
        })
        .collect()
}

/// Records one effect's outcome exactly as the dispatcher's driver would,
/// without touching its siblings — the lever the interleaving below needs.
async fn record_outcome(
    fixture: &Fixture,
    effect: &AgentRunEffect,
    outcome: AgentRunEffectOutcome,
) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the effect result applies");
}

/// Drives the task and the run the way [`Fixture::pump`] does, except that a
/// send serving one of `held` skills stays with its peer: dispatched, and
/// unanswered. That is the interleaving a real peer surface produces when one
/// specialist replies while another is still working.
async fn pump_holding(fixture: &Fixture, held: &[&str]) {
    for _round in 0..32 {
        fixture
            .settle_task_at(&task_scope())
            .await
            .expect("the task settles");
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let progress = run
            .settle_side_effects(&fixture.router, fixture.now())
            .await
            .expect("the run settles");
        let mut answered = 0;
        for (effect, skill) in dispatched_effects(fixture).await {
            if skill.is_some_and(|skill| held.contains(&skill.as_str())) {
                continue;
            }
            let outcome = fixture.dispatcher.answer(&effect).await;
            record_outcome(fixture, &effect, outcome).await;
            answered += 1;
        }
        if answered == 0
            && progress.transitions == 0
            && progress.effects_dispatched == 0
            && progress.settled == 0
            && progress.failed == 0
        {
            return;
        }
    }
    panic!("the held pump did not quiesce");
}

/// A straggler whose send fails *after* the group already resolved is still a
/// fan-in disposition, not a coordinator failure.
///
/// `Any` resolves on the first child's result while its sibling's send is
/// still in flight — the run is `AwaitingTools`, not `AwaitingChildren`, so
/// the resolution does not resume it. When that sibling's send then fails
/// definitively, the coordinator must survive: it already has what it waited
/// for, and its own straggler must not wind it down. Membership in the group,
/// not a still-unresolved group, is what makes the failure a disposition.
#[tokio::test]
async fn a_straggler_send_failing_after_the_resolution_does_not_wind_the_run_down() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, Some(AgentFanInPolicy::Any)).await;

    // The first specialist answers and creates its child; the second's send
    // is still with its peer.
    pump_holding(&fixture, &[SKILL_2]).await;
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 1, "only the answered send created a child");
    let result = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &result).await);
    let (phase, _, outstanding) = parked_phase(&fixture).await;
    assert_eq!(
        phase,
        AgentLoopPhase::AwaitingTools,
        "the straggler's send keeps the turn on its effect wait"
    );
    assert_eq!(outstanding, 1);
    assert_eq!(
        resolution_code(&fixture).await.as_deref(),
        Some("any-satisfied"),
        "the first child's result resolved the group already"
    );

    // Now the straggler's send fails definitively. The coordinator survives.
    let straggler = dispatched_effects(&fixture)
        .await
        .into_iter()
        .find(|(_, skill)| skill.as_deref() == Some(SKILL_2))
        .map(|(effect, _)| effect)
        .expect("the straggler's send is still outstanding");
    record_outcome(
        &fixture,
        &straggler,
        AgentRunEffectOutcome::Failed {
            code: "peer-unavailable".to_string(),
            message: "the specialist surface refused the send".to_string(),
        },
    )
    .await;
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let status = run
        .state()
        .expect("state")
        .status()
        .expect("the run exists");
    assert!(
        !status.is_terminal(),
        "a resolved group's own straggler is a disposition, not a wind-down; the run \
         is {status}"
    );

    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed),
        "the coordinator finishes with the child it has"
    );
}

/// The fan-in group's persisted resolution code, when it has one.
async fn resolution_code(fixture: &Fixture) -> Option<String> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    loop_state
        .fan_in()
        .and_then(|group| group.resolution.as_ref())
        .map(|resolution| resolution.code.clone())
}

/// Every path that resolves a group counts one bounded
/// `rakka.agent.fan_in.resolutions` observation, not just the one an arriving
/// child result takes: the deadline timer's `timed-out` resolution is
/// reachable only from `FireFanInDeadline`, so a counter emitted at the
/// result door alone could never observe it.
#[tokio::test]
async fn a_deadline_resolution_is_counted_like_a_result_resolution() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let executor = SkillNamedExecutor::new();
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(fan_out_turn_with_deadline())
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_metrics(metrics.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    assert_eq!(
        parked_phase(&fixture).await.0,
        AgentLoopPhase::AwaitingChildren
    );

    fixture
        .clock
        .store(10_000, std::sync::atomic::Ordering::SeqCst);
    let fire = |suffix: &'static str| AgentRunEntityCommand::FireFanInDeadline {
        operation_id: rakka_agent::AgentOperationId::new(
            rakka_agent::AgentOperationKind::Command,
            [TENANT, "fan-in-deadline", suffix],
        )
        .expect("the operation id derives"),
    };
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(fire("counted"), &fixture.router, fixture.now())
        .await
        .expect("the deadline fires");

    assert_eq!(
        resolution_code(&fixture).await.as_deref(),
        Some("timed-out")
    );
    let resolutions = metrics
        .snapshot()
        .observations_named(rakka_agent::METRIC_AGENT_FAN_IN_RESOLUTIONS)
        .len();
    assert_eq!(
        resolutions, 1,
        "the deadline's resolution is counted once, at the command that made it"
    );

    // A duplicate fire converges on the absorbing resolution and counts
    // nothing: an unchanged identity is a replay.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(fire("counted-again"), &fixture.router, fixture.now())
        .await
        .expect("a duplicate fire converges");
    let resolutions = metrics
        .snapshot()
        .observations_named(rakka_agent::METRIC_AGENT_FAN_IN_RESOLUTIONS)
        .len();
    assert_eq!(
        resolutions, 1,
        "the duplicate fire counted no second resolution"
    );
}

/// The fan-out turn with a wait deadline the parked group can time out on.
fn fan_out_turn_with_deadline() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Fanning out to both specialists and awaiting them briefly.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-2").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL_2, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({ "deadline": 5_000 }),
            )
            .expect("the tool call is bounded"),
        )
}

/// Scenario 27 under `Quorum`, through the run entity: the goal spec's
/// declared policy reaches the group via the assignment envelope, one
/// success does not resolve a two-of-two quorum, the second does, and the
/// resumed model proposes — the integration twin of `fan_in.rs`'s policy
/// units.
#[tokio::test]
async fn a_goal_declared_quorum_resolves_at_n_through_the_run_entity() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, Some(AgentFanInPolicy::Quorum { n: 2 })).await;
    fixture.pump().await.expect("the loop should converge");

    let group = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is running")
            .fan_in()
            .expect("the group exists")
            .clone()
    };
    assert_eq!(group.policy, AgentFanInPolicy::Quorum { n: 2 });

    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);
    let first = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &first).await);
    assert_eq!(
        parked_phase(&fixture).await.0,
        AgentLoopPhase::AwaitingChildren,
        "one success is not a two-of-two quorum"
    );

    let second = child_result_envelope(
        &fixture,
        &children[1].0,
        &children[1].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &second).await);
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed)
    );
    assert_eq!(
        resolution_code(&fixture).await.as_deref(),
        Some("quorum-satisfied")
    );
}

/// A model-supplied await deadline that does not lie in the future refuses
/// (`fan-in-invalid-arguments`) before it can reach durable timer state —
/// otherwise the first `FireFanInDeadline` would mark every child timed out
/// before any could report. The group stays open, the model corrects course
/// with a plain await, and the run completes.
#[tokio::test]
async fn a_past_await_deadline_is_refused_before_it_reaches_the_timer() {
    let executor = SkillNamedExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let stale = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating and awaiting under a deadline already past.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({ "deadline": 1 }),
            )
            .expect("the tool call is bounded"),
        );
    let corrected = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Awaiting without the stale deadline.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-2").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({}),
            )
            .expect("the tool call is bounded"),
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(stale)
                .with_turn(corrected)
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(delegation_config_with_fan_in());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    // The corrected await parked the group — with no deadline, because the
    // stale one never reached the timer.
    let group = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is running")
            .fan_in()
            .expect("the group exists")
            .clone()
    };
    assert!(group.closed);
    assert!(group.deadline.is_none(), "the stale deadline was refused");
    assert_eq!(
        parked_phase(&fixture).await.0,
        AgentLoopPhase::AwaitingChildren
    );

    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 1);
    let result = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &result).await);
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed)
    );

    // The refusal reached the model under the await verb's stable code.
    let codes: Vec<String> = {
        use rakka_agent::SessionMemoryStore;
        let page = session
            .read(&run_scope(), rakka_agent::SessionMemoryCursor::start())
            .await
            .expect("the session should read");
        page.entries
            .iter()
            .filter(|entry| entry.role == rakka_agent::MemoryEntryRole::ToolResult)
            .filter_map(|entry| {
                entry
                    .content
                    .inline_value()
                    .and_then(|value| value.get("error"))
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string)
            })
            .collect()
    };
    assert_eq!(codes, vec!["fan-in-invalid-arguments".to_string()]);
}

/// Removes every occurrence of the named keys from a JSON tree — the shape
/// of a record persisted before the field existed.
fn strip_keys(value: &mut serde_json::Value, keys: &[&str]) {
    match value {
        serde_json::Value::Object(map) => {
            for key in keys {
                map.remove(*key);
            }
            for entry in map.values_mut() {
                strip_keys(entry, keys);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_keys(item, keys);
            }
        }
        _ => {}
    }
}

/// A pre-4.4 run state — no `fan_in` cell, no `delegation_envelope` on the
/// loop — still decodes, with both fields defaulting to absent: exactly the
/// pre-slice semantics.
#[tokio::test]
async fn a_pre_slice_run_state_decodes_without_the_fan_out_fields() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let mut encoded = serde_json::to_value(run.state().expect("state")).expect("encodes");
    strip_keys(&mut encoded, &["fan_in", "delegation_envelope"]);
    let decoded: rakka_agent::AgentRunState =
        serde_json::from_value(encoded).expect("a pre-slice run state decodes");
    let loop_state = decoded.loop_state().expect("loop state");
    assert!(loop_state.fan_in().is_none());
    assert!(loop_state.delegation_envelope().is_none());
}

/// An await with nothing to wait for refuses as a failed tool result the run
/// survives — the model corrects course and proposes.
#[tokio::test]
async fn an_await_with_no_children_is_refused_and_the_run_survives() {
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Awaiting children that were never delegated.")
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("await-1").expect("call id"),
                                fan_in_tool_id(),
                                json!({}),
                            )
                            .expect("the tool call is bounded"),
                        ),
                )
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(SkillNamedExecutor::new()),
    )
    .with_delegation(delegation_config_with_fan_in());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    assert!(
        state.loop_state().expect("loop state").fan_in().is_none(),
        "no group ever opened"
    );
}

/// Scenario 27's exchange-fabric half with real child task entities: the
/// parked parent's children are durable tasks that, on reaching terminal
/// with their ledgers closed, owe the delegation-result exchange from their
/// own transition — the courier delivers it through the router, the parent
/// resolves deterministically, and a re-driven child settle converges
/// without a second transition.
#[tokio::test]
async fn real_child_tasks_return_results_through_the_exchange_fabric() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);

    let tenant = TenantId::new(TENANT);
    for (index, (delegation, child_task)) in children.iter().enumerate() {
        let scope =
            AgentTaskScope::new(tenant.clone(), child_task.clone()).expect("the scope is valid");
        // The child exists with the provenance its creation validated: the
        // delegation that created it, under this parent run.
        let provenance = rakka_agent::AgentTaskDelegationProvenance {
            environments: Default::default(),
            knowledge_spaces: Default::default(),
            delegation: delegation.clone(),
            parent_task: task_scope().task().clone(),
            parent_run: run_scope(),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 1,
            requested_skill: rakka_agent::AgentCapabilityId::new(if index == 0 {
                SKILL
            } else {
                SKILL_2
            })
            .expect("capability id should be valid"),
            capability_scopes: Default::default(),
            credential_bindings: Vec::new(),
            result_schema: None,
            budget: None,
            deadline: None,
        };
        fixture
            .apply_task_command_at(
                &scope,
                rakka_agent::AgentTaskEntityCommand::Create {
                    operation_id: rakka_agent::AgentOperationId::new(
                        rakka_agent::AgentOperationKind::TaskCreation,
                        [TENANT, child_task.as_str(), "1"],
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(rakka_agent::AgentTaskCreation {
                        // Human-owned, so the child reaches terminal without
                        // entering the assignment machinery: the test's
                        // subject is the exchange fabric, not the child's own
                        // loop.
                        definition: task_definition()
                            .with_ownership(rakka_agent::AgentTaskOwnership::Human),
                        input: AgentTaskContent::inline(json!({ "text": "hello" }))
                            .expect("the input is inline-bounded"),
                        assignee: None,
                        team: None,
                        goal: None,
                        goal_mode: Default::default(),
                        goal_spec: None,
                        parent: Some(task_scope().task().clone()),
                        dependencies: Vec::new(),
                        escrow: None,
                        wake: None,
                        delegation: Some(Box::new(provenance)),
                        telemetry: Default::default(),
                    }),
                },
            )
            .await
            .expect("the child task creates");

        // The child terminates before any assignment settles: terminal with
        // a closed ledger, so its own transition owes the report.
        fixture
            .apply_task_command_at(
                &scope,
                rakka_agent::AgentTaskEntityCommand::Cancel {
                    operation_id: rakka_agent::AgentOperationId::new(
                        rakka_agent::AgentOperationKind::Cancellation,
                        [TENANT, child_task.as_str(), "1"],
                    )
                    .expect("the operation id derives"),
                    reason: "specialist-declined".to_string(),
                },
            )
            .await
            .expect("the child cancels");

        // The courier drains the owed exchange — and a re-driven settle is
        // the same operation, converging without a second transition.
        fixture
            .settle_task_at(&scope)
            .await
            .expect("the child settles");
        fixture
            .settle_task_at(&scope)
            .await
            .expect("the re-driven settle converges");
    }

    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("loop state");
    let resolution = loop_state
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the group resolved");
    assert!(!resolution.satisfied, "both children cancelled");
    for (delegation, _) in &children {
        let result = loop_state
            .delegation(delegation)
            .expect("the cell exists")
            .result
            .clone()
            .expect("the child's report recorded");
        assert_eq!(result.status, AgentTaskStatus::Cancelled);
        assert_eq!(
            result.terminal_reason.as_deref(),
            Some("cancellation-requested")
        );
    }
}

/// A delegation the model plans after the same turn's await is refused
/// (`delegation-after-await`): the await closes the run's one fan-out group,
/// so a later call could join nothing — a member no await covers, whose
/// definitive send failure would wind the coordinator down over a child the
/// group never held. The await still parks over the membership committed
/// before it, the refusal reaches the model as the call's failed tool
/// result, and the run survives to resolution.
#[tokio::test]
async fn a_delegation_planned_after_the_await_is_refused_and_the_run_survives() {
    let executor = SkillNamedExecutor::new();
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let sandwich = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating, awaiting, then delegating out of order.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({}),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-2").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL_2, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(sandwich)
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(delegation_config_with_fan_in());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");

    // Only the delegation planned before the await committed and crossed.
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 1);
    assert_eq!(
        executor
            .seen
            .lock()
            .expect("the record log should not be poisoned")
            .len(),
        1,
        "the refused delegation never reached the executor"
    );

    // The run parked over that single member.
    let (phase, status, outstanding) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(status, Some(AgentRunStatus::Running));
    assert_eq!(outstanding, 0);

    // The one member's result resolves the group and the coordinator
    // finishes with the children it has.
    let result = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    assert!(deliver(&fixture, &result).await);
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));

    // The refusal reached the resumed model under its stable code — the
    // session records what the model actually saw.
    let codes: Vec<String> = {
        use rakka_agent::SessionMemoryStore;
        let page = session
            .read(&run_scope(), rakka_agent::SessionMemoryCursor::start())
            .await
            .expect("the session should read");
        page.entries
            .iter()
            .filter(|entry| entry.role == rakka_agent::MemoryEntryRole::ToolResult)
            .filter_map(|entry| {
                entry
                    .content
                    .inline_value()
                    .and_then(|value| value.get("error"))
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string)
            })
            .collect()
    };
    assert_eq!(codes, vec!["delegation-after-await".to_string()]);
    let resolution = state
        .loop_state()
        .expect("loop state")
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the group resolved");
    assert!(resolution.satisfied);
    assert_eq!(resolution.code, "all-settled");
}

/// The child-side settle rule: a refused report settles only under the
/// parent's definitive answers — unknown, forged, not owned, or a run that
/// never existed. A receipt race, an undecodable payload, and an owner that
/// predates the kind are the receiver's inability, and the exchange stays
/// outstanding for re-drive.
#[test]
fn the_delegation_result_settle_rule_is_definitive_answers_only() {
    use rakka_agent::{AgentExchangeParticipant, AgentExchangeResult, AgentTaskParticipant};

    let tenant = TenantId::new(TENANT);
    let delegation = rakka_agent::delegation_id_for(&run_scope(), 1, 0).expect("derives");
    let operation_id =
        delegation_result_operation_id(&tenant, &delegation).expect("the operation id derives");
    let child_scope = AgentTaskScope::new(
        tenant,
        AgentTaskId::new("child-1").expect("task id should be valid"),
    )
    .expect("the scope is valid");
    let envelope = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::DelegationResult,
        AgentEntityAddress::Task(child_scope),
        AgentEntityAddress::Run(run_scope()),
        AgentExchangePayload::empty(AGENT_DELEGATION_RESULT_PAYLOAD_TYPE),
        AgentCorrelationId::new(operation_id.as_str()),
        rakka_agent_workflow::AgentTimestampMillis::new(1),
    )
    .expect("the envelope is valid");

    let participant = AgentTaskParticipant;
    let refusal = |code: &str| {
        AgentExchangeResult::rejected(
            code,
            "refused",
            AgentExchangePayload::empty(AGENT_DELEGATION_RESULT_PAYLOAD_TYPE),
        )
    };
    for code in [
        "delegation-result-unknown-run",
        "delegation-result-unknown-delegation",
        "delegation-result-forged",
        "delegation-result-not-owned",
    ] {
        assert!(
            participant.check_settle(&envelope, &refusal(code)).is_ok(),
            "{code} is the parent answering definitively; the child advances"
        );
    }
    for code in [
        "delegation-result-early",
        "delegation-result-undecodable",
        "unsupported-exchange",
    ] {
        assert!(
            participant.check_settle(&envelope, &refusal(code)).is_err(),
            "{code} is the receiver's inability; the exchange stays outstanding"
        );
    }
    let accepted = AgentExchangeResult::accepted(AgentExchangePayload::empty(
        AGENT_DELEGATION_RESULT_PAYLOAD_TYPE,
    ));
    assert!(participant.check_settle(&envelope, &accepted).is_ok());
}
