//! Workflows as tools
//! ([specification 8.6 and 11.7](../../docs/plans/rakka-agent/spec.md),
//! scenario 32).
//!
//! A workflow-tool call commits — in one compare-and-set — the durable
//! invocation record, its cell, its fan-in membership, and the start effect.
//! Create-or-adopt is an identity property: the derived invocation id is the
//! child workflow run id and the generation-free `StartRun` deduplication
//! key, so a replayed invocation addresses the one child run's own durable
//! inbox and adopts it rather than starting a second — and the child's
//! internal work executes once, whatever the parent replays. The parent
//! waits as fan-in membership, and the child's terminal outcome returns as
//! the deduplicated `RecordWorkflowResult` command the hosting application
//! relays.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use common::{
    delegation_config_with_fan_in, delegation_tool_id, fan_in_tool_id, goal_spec_draft,
    goal_spec_with_workflow, goal_task_creation_command, run_scope, task_definition,
    workflow_config, Fixture, SKILL, TENANT, WORKFLOW_TOOL,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    child_workflow_run_id, workflow_invocation_id_for, workflow_start_command,
    workflow_start_command_id, AgentDispatchFuture, AgentFanInMemberId, AgentLoopPhase,
    AgentModelTurn, AgentRunEffect, AgentRunEntityCommand, AgentRunScope, AgentRunStatus,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentWorkflowInvocationRecord,
    AgentWorkflowInvocationStatus, AgentWorkflowStartExecutor, AgentWorkflowStartFinding,
    AgentWorkflowTerminalStatus, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};
use rakka_agent_workflow::{
    AgentEphemeralCredential, AgentRunInbox, AgentTimestampMillis, AgentWorkflowId,
};
use rakka_persistence::InMemoryDurableStateStore;
use serde_json::json;

type ChildStore = InMemoryDurableStateStore<WorkflowState>;
type ChildInbox = AgentRunInbox<ChildStore, ManualWorkflowClock>;

/// One executor sighting: the identities every replay must share.
#[derive(Debug, Clone)]
struct Sighting {
    invocation: String,
    child_run: String,
    command_id: String,
}

fn sighting_of(record: &AgentWorkflowInvocationRecord) -> Sighting {
    Sighting {
        invocation: record.invocation.as_str().to_string(),
        child_run: record.child_run.as_str().to_string(),
        command_id: workflow_start_command_id(&record.invocation)
            .as_str()
            .to_string(),
    }
}

/// What the scripted executor answers.
#[derive(Debug, Clone, Copy)]
enum StartMode {
    Started,
    Adopted,
    Refused,
    Conflict,
}

/// A recording executor: every sighting's derived identities, and a scripted
/// finding.
struct RecordingWorkflowExecutor {
    seen: Mutex<Vec<Sighting>>,
    mode: StartMode,
}

impl RecordingWorkflowExecutor {
    fn new(mode: StartMode) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            mode,
        })
    }

    fn seen(&self) -> Vec<Sighting> {
        self.seen
            .lock()
            .expect("the sighting log should not be poisoned")
            .clone()
    }
}

impl AgentWorkflowStartExecutor for RecordingWorkflowExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        invocation: &'a AgentWorkflowInvocationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentWorkflowStartFinding> {
        self.seen
            .lock()
            .expect("the sighting log should not be poisoned")
            .push(sighting_of(invocation));
        let mode = self.mode;
        Box::pin(async move {
            Ok(match mode {
                StartMode::Started => AgentWorkflowStartFinding::Started,
                StartMode::Adopted => AgentWorkflowStartFinding::Adopted,
                StartMode::Refused => AgentWorkflowStartFinding::Refused {
                    code: "workflow-registry-unknown".to_string(),
                    message: "the registry serves no such workflow".to_string(),
                },
                StartMode::Conflict => AgentWorkflowStartFinding::Conflict {
                    // Deliberately not the canonical code: the dispatch layer
                    // must normalize any conflict finding onto
                    // `workflow-invocation-conflict`, whatever the executor
                    // reports.
                    code: "child-run-pins-mismatch".to_string(),
                    message: "a child run exists that this invocation does not own".to_string(),
                },
            })
        })
    }
}

/// The application-owed bridge, real: every start builds the derived
/// `StartRun` command and accepts it into the child run's own durable inbox
/// over one shared in-memory store. Acceptance is a started finding;
/// duplicate acceptance is adoption — exactly the contract the trait states.
struct RealInboxExecutor {
    store: ChildStore,
    clock: AtomicU64,
    seen: Mutex<Vec<Sighting>>,
}

impl RealInboxExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            store: ChildStore::new(),
            clock: AtomicU64::new(1),
            seen: Mutex::new(Vec::new()),
        })
    }

    fn child_inbox(&self, record: &AgentWorkflowInvocationRecord) -> ChildInbox {
        AgentRunInbox::with_clock(
            record.child_run.clone(),
            self.store.clone(),
            ManualWorkflowClock::new(WorkflowTimestamp::from_millis(
                self.clock.fetch_add(1, Ordering::SeqCst),
            )),
        )
    }
}

impl AgentWorkflowStartExecutor for RealInboxExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        invocation: &'a AgentWorkflowInvocationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentWorkflowStartFinding> {
        self.seen
            .lock()
            .expect("the sighting log should not be poisoned")
            .push(sighting_of(invocation));
        Box::pin(async move {
            let command = workflow_start_command(
                invocation,
                AgentWorkflowId::new("wf-refund"),
                None,
                AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst)),
            )
            .map_err(|error| rakka_agent::AgentDispatchError::Invocation {
                code: "workflow-start-command-invalid",
                message: error.to_string(),
            })?;
            let mut inbox = self.child_inbox(invocation);
            inbox
                .recover()
                .await
                .map_err(|error| rakka_agent::AgentDispatchError::Invocation {
                    code: "workflow-start-inbox-unavailable",
                    message: error.to_string(),
                })?;
            let acceptance = inbox.accept_command(command).await.map_err(|error| {
                rakka_agent::AgentDispatchError::Invocation {
                    code: "workflow-start-inbox-refused",
                    message: error.to_string(),
                }
            })?;
            Ok(if acceptance.is_accepted() {
                AgentWorkflowStartFinding::Started
            } else {
                AgentWorkflowStartFinding::Adopted
            })
        })
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

/// One workflow invocation plus the declared await: the parent parks
/// `AwaitingChildren` on the child workflow run.
fn workflow_await_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking the refund workflow and awaiting it.")
        .with_tool_call(workflow_call("invoke-1"))
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

fn workflow_fixture(
    executor: Arc<dyn AgentWorkflowStartExecutor + Send + Sync>,
    first_turn: AgentModelTurn,
) -> Fixture {
    Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(executor),
    )
    // The await verb is declared on the delegation wiring, and mixed
    // delegation-and-workflow turns need both configurations anyway.
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config())
}

async fn create_workflow_task(fixture: &Fixture) {
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_workflow(None), true),
        ))
        .await
        .expect("the goal task should create");
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

/// The one committed invocation, read from the durable cell.
async fn committed_invocation(
    fixture: &Fixture,
) -> (
    rakka_agent::AgentWorkflowInvocationId,
    AgentWorkflowInvocationStatus,
) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    let (id, cell) = loop_state
        .workflow_invocations()
        .iter()
        .next()
        .expect("the invocation cell exists");
    (id.clone(), cell.status.clone())
}

/// Applies the child's terminal result through the application-owed command.
async fn deliver_result(
    fixture: &Fixture,
    invocation: &rakka_agent::AgentWorkflowInvocationId,
    child_run: rakka_agent_workflow::AgentRunId,
    status: AgentWorkflowTerminalStatus,
) -> Result<rakka_agent::AgentRunEntityReply, rakka_agent::AgentRunError> {
    let command = AgentRunEntityCommand::record_workflow_result(
        &TenantId::new(TENANT),
        invocation.clone(),
        child_run,
        status,
        None,
        None,
        Some(rakka_agent::AgentContentDigest::sha256_of_bytes(b"result")),
    )
    .expect("the command derives");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(command, &fixture.router, fixture.now()).await
}

/// The invocation, its cell, its membership, and its start effect commit in
/// one compare-and-set; the run parks `AwaitingChildren` with no outstanding
/// effect once the start receipt lands, and every derived identity is a pure
/// function of the parent's `(turn, slot)` coordinate.
#[tokio::test]
async fn a_workflow_call_commits_one_invocation_with_its_derived_identity() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor.clone(), workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (phase, status, outstanding) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(status, Some(AgentRunStatus::Running));
    assert_eq!(
        outstanding, 0,
        "a parked fan-in holds no outstanding effect"
    );

    let (invocation, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Started { adopted: false }
    );

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let cell = loop_state
        .workflow_invocation(&invocation)
        .expect("the cell exists");
    let expected = workflow_invocation_id_for(&run_scope(), cell.record.turn, cell.record.slot)
        .expect("derives");
    assert_eq!(cell.record.invocation, expected);
    assert_eq!(
        cell.record.child_run,
        child_workflow_run_id(&expected),
        "the child run id is the invocation id verbatim"
    );
    assert_eq!(cell.record.deduplication_key, expected.as_str());
    assert_eq!(
        cell.record.required_capabilities,
        common::workflow_tool_descriptor().required_capabilities,
        "the descriptor's capability surface is copied onto the record at commit"
    );
    let group = loop_state.fan_in().expect("the group exists");
    assert!(group
        .members
        .contains(&AgentFanInMemberId::from(expected.clone())));

    let seen = executor.seen();
    assert!(!seen.is_empty());
    assert!(seen
        .iter()
        .all(|sighting| sighting.invocation == expected.as_str()
            && sighting.child_run == expected.as_str()
            && sighting.command_id == format!("{}#start-run", expected.as_str())));
}

/// A start the executor reports as adopted settles the cell `Started` with
/// the adoption flag — the same shape as a fresh start, because the receipt
/// derives from the record, never from the acceptance.
#[tokio::test]
async fn an_adopted_start_settles_the_cell_with_the_adoption_flag() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Adopted);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (_, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Started { adopted: true }
    );
    let (phase, _, _) = parked_phase(&fixture).await;
    assert_eq!(
        phase,
        AgentLoopPhase::AwaitingChildren,
        "adoption parks exactly as a fresh start does"
    );
}

/// The scenario-32 sweep: a crash at every durable write between the
/// invoking turn and the parked wait converges on one cell, one derived
/// identity, and one `StartRun` identity across every executor sighting — a
/// re-driven attempt is a replay, never a second child run.
#[tokio::test]
async fn the_invocation_survives_any_owner_loss_with_one_identity() {
    for point in 1..24 {
        for window in [
            rakka_agent::testkit::CrashPoint::BeforeWrite,
            rakka_agent::testkit::CrashPoint::AfterWrite,
        ] {
            let executor = RecordingWorkflowExecutor::new(StartMode::Started);
            let fixture = workflow_fixture(executor.clone(), workflow_await_turn());
            create_workflow_task(&fixture).await;

            fixture.runs.crash_at(point, window);
            let _ = fixture.pump().await;
            fixture.runs.survive();
            fixture.pump().await.unwrap_or_else(|error| {
                panic!("recovery after crash point {point} failed: {error}")
            });

            let mut run = fixture.run();
            run.recover(fixture.now()).await.expect("recover");
            let state = run.state().expect("state");
            let loop_state = state.loop_state().expect("loop state");
            assert_eq!(loop_state.workflow_invocation_count(), 1);
            let cell = loop_state
                .workflow_invocations()
                .values()
                .next()
                .expect("the cell exists");
            let expected =
                workflow_invocation_id_for(&run_scope(), cell.record.turn, cell.record.slot)
                    .expect("derives");
            assert_eq!(cell.record.invocation, expected);
            assert!(cell.status.is_settled());
            let seen = executor.seen();
            assert!(!seen.is_empty());
            let command_id = format!("{}#start-run", expected.as_str());
            assert!(
                seen.iter().all(|sighting| {
                    sighting.invocation == expected.as_str()
                        && sighting.child_run == expected.as_str()
                        && sighting.command_id == command_id
                }),
                "every sighting at crash point {point} carried the derived identity"
            );
        }
    }
}

/// The end-to-end half of scenario 32, over a real child inbox: a replayed
/// invocation deduplicates in the child run's own durable inbox and adopts
/// the one child, the child's internal work executes once, and its terminal
/// outcome resumes the parent through the deduplicated result command.
#[tokio::test]
async fn a_replayed_invocation_adopts_one_child_run_and_its_internal_effects_are_stable() {
    let executor = RealInboxExecutor::new();
    let fixture = workflow_fixture(executor.clone(), workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (invocation, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Started { adopted: false },
        "the first start durably accepted"
    );

    // The record, exactly as persisted — what any replay re-sends.
    let record = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        (*state
            .loop_state()
            .expect("loop state")
            .workflow_invocation(&invocation)
            .expect("the cell exists")
            .record)
            .clone()
    };

    // A replayed invocation — an operator-driven new effect generation, a
    // redispatched attempt, any re-drive — sends the identical derived
    // `StartRun` and the child inbox answers it as a duplicate: adoption.
    let replayed = executor
        .execute(&run_scope(), &fake_intent(&record), &record, None)
        .await
        .expect("the replay reaches the inbox");
    assert!(matches!(replayed, AgentWorkflowStartFinding::Adopted));

    // The child holds exactly one accepted StartRun: its internal work has
    // one cause, whatever the parent replayed.
    let mut inbox = executor.child_inbox(&record);
    inbox.recover().await.expect("the child inbox recovers");
    let pending = inbox
        .inner()
        .recoverable_inbox()
        .expect("the child inbox loads");
    assert_eq!(pending.len(), 1, "one durable StartRun, ever");

    // The child executes its one step — the internal effect — exactly once,
    // then completes; its inbox entry settles with it.
    inbox
        .inner_mut()
        .transition_inbox(
            pending[0].message_id(),
            rakka_agent_workflow::substrate::InboxStatus::Completed,
        )
        .await
        .expect("the child completes its one entry");

    // A start replayed even after the child completed still adopts: the
    // inbox's durable entry is the fence, not its processing status.
    let late = executor
        .execute(&run_scope(), &fake_intent(&record), &record, None)
        .await
        .expect("the late replay reaches the inbox");
    assert!(matches!(late, AgentWorkflowStartFinding::Adopted));

    // The application-owed relay returns the child's terminal outcome; the
    // fan-in resolves and the parent proposes its own result.
    let reply = deliver_result(
        &fixture,
        &invocation,
        record.child_run.clone(),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the result applies");
    assert!(matches!(
        reply,
        rakka_agent::AgentRunEntityReply::Applied { .. }
    ));
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let task = fixture.task_snapshot().await;
    assert!(task.accepted_result.is_some());
}

/// Builds a throwaway intent for direct executor re-invocation: the executor
/// reads only the record, exactly as the contract states.
fn fake_intent(record: &AgentWorkflowInvocationRecord) -> AgentRunEffect {
    let request = build_start_request(record.clone());
    let spec = rakka_agent::AgentEffectPolicies::default()
        .spec_for(&request)
        .clone();
    AgentRunEffect::new(
        &run_scope(),
        record.turn,
        record.slot,
        request,
        &spec,
        rakka_agent::AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the intent builds")
}

fn build_start_request(
    record: AgentWorkflowInvocationRecord,
) -> rakka_agent::AgentRunEffectRequest {
    // The request variant is loop-constructible only in production; tests
    // re-encode the persisted record through serde, the same path a durable
    // replay takes.
    let encoded = json!({ "workflow-start": { "invocation": record } });
    serde_json::from_value(encoded).expect("the request round-trips")
}

/// A duplicate result answers from the operation log with no second
/// transition, and a conflicting duplicate under the same derived operation
/// id cannot rewrite the first writer.
#[tokio::test]
async fn a_re_driven_result_settles_without_a_second_transition() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");
    let (invocation, _) = committed_invocation(&fixture).await;
    let child_run = child_workflow_run_id(&invocation);

    let first = deliver_result(
        &fixture,
        &invocation,
        child_run.clone(),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the result applies");
    assert!(matches!(
        first,
        rakka_agent::AgentRunEntityReply::Applied { .. }
    ));

    // The duplicate — and even a conflicting duplicate — carries the same
    // derived operation id, so the journal answers it without a transition.
    let duplicate = deliver_result(
        &fixture,
        &invocation,
        child_run,
        AgentWorkflowTerminalStatus::Failed,
    )
    .await
    .expect("the duplicate is answered");
    assert!(matches!(
        duplicate,
        rakka_agent::AgentRunEntityReply::Duplicate { .. }
    ));

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let cell = state
        .loop_state()
        .expect("loop state")
        .workflow_invocation(&invocation)
        .expect("the cell exists")
        .clone();
    assert_eq!(
        cell.result.expect("the first result stands").status,
        AgentWorkflowTerminalStatus::Completed
    );
}

/// The exact refusal codes, each a non-committing error: unknown invocation,
/// a child run the invocation does not own, and a cell that settled without
/// reaching a child.
#[tokio::test]
async fn a_forged_result_is_refused_with_the_exact_codes() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");
    let (invocation, _) = committed_invocation(&fixture).await;

    // An invocation the run never committed.
    let foreign = workflow_invocation_id_for(&run_scope(), 99, 0).expect("derives");
    let error = deliver_result(
        &fixture,
        &foreign,
        child_workflow_run_id(&foreign),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect_err("an unknown invocation refuses");
    assert_eq!(error.code(), "workflow-result-unknown-invocation");

    // A child run the invocation does not own.
    let error = deliver_result(
        &fixture,
        &invocation,
        rakka_agent_workflow::AgentRunId::new("foreign-run"),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect_err("a foreign child run refuses");
    assert_eq!(error.code(), "workflow-result-forged");

    // Nothing refused committed: the true result still applies.
    let applied = deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the corrected re-send applies");
    assert!(matches!(
        applied,
        rakka_agent::AgentRunEntityReply::Applied { .. }
    ));
}

/// A definitively refused start settles the cell and becomes a fan-in
/// disposition — the coordinator survives to work with what it has — and a
/// result later claiming that child refuses `workflow-result-not-owned`.
#[tokio::test]
async fn a_failed_start_is_a_fan_in_disposition_not_a_coordinator_failure() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Refused);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    // The group's one member failed to exist, `All` is unsatisfiable, the
    // model consumed the table and proposed: the coordinator survived.
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("loop state");
    let (invocation, cell) = loop_state
        .workflow_invocations()
        .iter()
        .next()
        .expect("the cell exists");
    assert_eq!(
        cell.status,
        AgentWorkflowInvocationStatus::Failed {
            code: "workflow-registry-unknown".to_string()
        }
    );
    let resolution = loop_state
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the group resolved");
    assert!(!resolution.satisfied);

    let invocation = invocation.clone();
    drop(run);
    let error = deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect_err("a settled non-started cell owns no child");
    assert_eq!(error.code(), "workflow-result-not-owned");
}

/// An executor conflict settles the cell `Conflicted`: a child exists that
/// this invocation's identity does not own, and recovery uses a new
/// invocation, never this one. The executor reported its own detail code —
/// the dispatch layer normalizes every conflict finding onto the canonical
/// code, so the `Conflicted` settlement is structural, never a string
/// convention.
#[tokio::test]
async fn a_conflicting_child_settles_the_cell_conflicted() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Conflict);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (_, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Conflicted {
            code: rakka_agent::AGENT_WORKFLOW_INVOCATION_CONFLICT_CODE.to_string()
        },
        "the executor's own conflict code normalized onto the canonical code"
    );
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed),
        "the conflict is a fan-in disposition, not a coordinator failure"
    );
}

/// An unwired start fails closed with the stable code — and the failure is a
/// fan-in disposition like any other definitive start failure.
#[tokio::test]
async fn an_unwired_start_fails_closed() {
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(workflow_await_turn())
                .with_turn(proposing_turn()),
        ),
        // No workflow start executor.
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (_, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Failed {
            code: "workflow-start-executor-missing".to_string()
        }
    );
}

/// Advances by hand, answering only model effects, so the start effect is
/// committed — the cell is durable and `Pending` — but unanswered.
async fn drive_model_turn_only(fixture: &Fixture) {
    let scope = run_scope();
    let mut answered = 0;
    for _round in 0..8 {
        let effects: Vec<AgentRunEffect> = {
            let mut run = fixture.run();
            run.recover(fixture.now()).await.expect("recover");
            let state = run.state().expect("state");
            match state.loop_state() {
                Some(loop_state) => loop_state
                    .effects()
                    .iter()
                    .filter(|effect| effect.status == rakka_agent::AgentRunEffectStatus::Ready)
                    .cloned()
                    .collect(),
                None => Vec::new(),
            }
        };
        let model_effect = effects.iter().find(|effect| {
            matches!(
                effect.request,
                rakka_agent::AgentRunEffectRequest::Model { .. }
            )
        });
        let Some(effect) = model_effect else { break };
        let outcome = fixture.dispatcher.answer(effect).await;
        let command = AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect.result_operation_id(&scope).expect("derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        };
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.apply(command, &fixture.router, fixture.now())
            .await
            .expect("the model result applies");
        answered += 1;
    }
    assert!(answered >= 1, "the model turn was answered");
}

/// A result arriving before the start receipt records first-writer-wins:
/// there is deliberately no early window, because the child's identity is
/// derived on the record at commit — status and result are separate cell
/// fields, and the receipt later settles the effect independently.
#[tokio::test]
async fn a_result_arriving_before_the_receipt_records_first_writer_wins() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;

    let (invocation, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Pending,
        "the start effect is committed but unanswered"
    );

    // The result arrives before the receipt: recorded, no refusal.
    let reply = deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("an early result records first-writer-wins");
    assert!(matches!(
        reply,
        rakka_agent::AgentRunEntityReply::Applied { .. }
    ));

    // The receipt lands afterwards and the run completes normally.
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let cell = state
        .loop_state()
        .expect("loop state")
        .workflow_invocation(&invocation)
        .expect("the cell exists")
        .clone();
    assert_eq!(
        cell.status,
        AgentWorkflowInvocationStatus::Started { adopted: false }
    );
    assert_eq!(
        cell.result.expect("the early result stands").status,
        AgentWorkflowTerminalStatus::Completed
    );
}

/// A start receipt naming a child run the invocation does not own — a broken
/// executor — settles the cell `Failed { workflow-start-run-mismatch }`
/// instead of grafting the foreign child onto the invocation, and the
/// coordinator survives it as a fan-in disposition.
#[tokio::test]
async fn a_mismatched_start_receipt_settles_the_cell_failed_and_the_run_survives() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;

    let (invocation, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Pending,
        "the start effect is committed but unanswered"
    );

    // The forged receipt, applied exactly as a dispatch worker reports its
    // outcome — but naming a child run the record never derived.
    let scope = run_scope();
    let effect = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        state
            .loop_state()
            .expect("loop state")
            .effects()
            .iter()
            .find(|effect| {
                matches!(
                    effect.request,
                    rakka_agent::AgentRunEffectRequest::WorkflowStart { .. }
                )
            })
            .expect("the start effect is committed")
            .clone()
    };
    let command = AgentRunEntityCommand::RecordEffectResult {
        operation_id: effect.result_operation_id(&scope).expect("derives"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        attempt: effect.attempts.saturating_add(1),
        fence: 0,
        outcome: Box::new(rakka_agent::AgentRunEffectOutcome::WorkflowStart {
            receipt: rakka_agent::AgentWorkflowStartReceipt {
                invocation: invocation.clone(),
                child_run: rakka_agent_workflow::AgentRunId::new("foreign-run"),
                adopted: false,
            },
        }),
    };
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(command, &fixture.router, fixture.now())
        .await
        .expect("the forged receipt applies as a definitive disposition");
    drop(run);

    let (_, cell_status) = committed_invocation(&fixture).await;
    assert_eq!(
        cell_status,
        AgentWorkflowInvocationStatus::Failed {
            code: "workflow-start-run-mismatch".to_string()
        },
        "the foreign child is never grafted onto the invocation"
    );

    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    assert_eq!(
        run.state().expect("state").status(),
        Some(AgentRunStatus::Completed),
        "the mismatch is a fan-in disposition the coordinator survives"
    );
}

/// A cancelled parent records a late workflow result as evidence and resumes
/// nothing.
#[tokio::test]
async fn a_result_for_a_wound_down_parent_records_evidence_and_resumes_nothing() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");
    let (invocation, _) = committed_invocation(&fixture).await;

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
    let reply = deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the late evidence records");
    assert!(matches!(
        reply,
        rakka_agent::AgentRunEntityReply::Applied { .. }
    ));

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
        .workflow_invocation(&invocation)
        .expect("the cell exists")
        .clone();
    assert!(cell.result.is_some());
    let (phase_after, status_after, outstanding_after) = parked_phase(&fixture).await;
    // Recording the last child's evidence completes the wind-down's
    // quiescence ([specification 8.7](../../docs/plans/rakka-agent/spec.md)):
    // the run terminalizes under the reason its cancellation recorded. It
    // still resumes nothing — no new turn, no new effect — which is what
    // "records evidence and resumes nothing" always meant.
    assert!(
        phase_after == phase_before || phase_after == AgentLoopPhase::Complete,
        "the run rests or completes its wind-down, got {phase_after:?}"
    );
    assert_eq!(status_after, Some(AgentRunStatus::Cancelled));
    assert_eq!(outstanding_after, outstanding_before);
}

/// A workflow invocation planned after the same turn's await is refused and
/// the run survives: the member would revive a superseded wind-down.
#[tokio::test]
async fn a_workflow_invocation_planned_after_the_await_is_refused_and_the_run_survives() {
    let turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating, awaiting, then invoking too late.")
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
        .with_tool_call(workflow_call("invoke-late"));
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(executor.clone())
        .with_a2a_send_executor(failing_send_executor()),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    assert_eq!(
        loop_state.workflow_invocation_count(),
        0,
        "the late invocation committed nothing"
    );
    assert!(executor.seen().is_empty(), "no start ever dispatched");
    assert!(
        state.status().is_some_and(|status| !status.is_terminal())
            || state.status() == Some(AgentRunStatus::Completed),
        "the refusal is a failed tool result, never a wind-down"
    );
}

/// A send executor whose delegation send fails definitively — enough for a
/// test whose delegation member only needs to exist.
fn failing_send_executor() -> Arc<dyn rakka_agent::AgentA2aSendExecutor> {
    struct FailingSend;
    impl rakka_agent::AgentA2aSendExecutor for FailingSend {
        fn execute<'a>(
            &'a self,
            _scope: &'a AgentRunScope,
            _intent: &'a AgentRunEffect,
            _delegation: &'a rakka_agent::AgentDelegationRecord,
            _credential: Option<&'a AgentEphemeralCredential>,
        ) -> AgentDispatchFuture<'a, rakka_agent::AgentA2aSendFinding> {
            Box::pin(async move {
                Ok(rakka_agent::AgentA2aSendFinding::Refused {
                    code: "peer-unavailable".to_string(),
                    message: "the specialist surface refused the send".to_string(),
                })
            })
        }
    }
    Arc::new(FailingSend)
}

/// A goal whose non-empty workflow set does not name the called tool refuses
/// at the door with `goal-workflow-not-allowed`, and the run survives.
#[tokio::test]
async fn a_goal_scoped_workflow_set_narrows_invocation() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(workflow_await_turn())
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(executor.clone()),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config());
    fixture.instantiate_agent().await;
    let mut spec = goal_spec_with_workflow(None);
    spec.allowed_workflows = std::collections::BTreeSet::from([
        rakka_agent::AgentWorkflowToolId::new("other-flow").expect("tool id should be valid"),
    ]);
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(spec, true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    assert_eq!(loop_state.workflow_invocation_count(), 0);
    assert!(
        executor.seen().is_empty(),
        "the door refused before dispatch"
    );
    assert_eq!(
        state.status(),
        Some(AgentRunStatus::Completed),
        "the refusal is a failed tool result the model corrects course from"
    );
}

/// A mixed fan-out — one delegated specialist and one workflow member —
/// resolves over both member kinds under one policy.
#[tokio::test]
async fn a_mixed_fan_out_resolves_over_delegations_and_workflow_members() {
    let turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating and invoking, then awaiting both.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({}),
            )
            .expect("the tool call is bounded"),
        );
    let workflow_executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(workflow_executor.clone())
        .with_a2a_send_executor(sending_executor()),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (phase, _, _) = parked_phase(&fixture).await;
    assert_eq!(phase, AgentLoopPhase::AwaitingChildren);
    let (delegation_id, child_task) = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("loop state");
        assert_eq!(
            loop_state.fan_in().expect("the group exists").members.len(),
            2,
            "both member kinds joined the one group"
        );
        let (id, cell) = loop_state
            .delegations()
            .iter()
            .next()
            .expect("the delegation cell exists");
        let rakka_agent::AgentDelegationStatus::ChildCreated { child_task, .. } = &cell.status
        else {
            panic!("the delegated child exists");
        };
        (id.clone(), child_task.clone())
    };
    let (invocation, _) = committed_invocation(&fixture).await;

    // The delegated child's result crosses the exchange fabric; the workflow
    // child's result crosses the application-owed command. One policy
    // resolves over both.
    let envelope = child_result_envelope(&fixture, &delegation_id, &child_task);
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(&envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert!(reply.result().is_accepted());
    drop(run);

    deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the workflow result applies");
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
    assert!(resolution.satisfied);
    assert_eq!(resolution.satisfied_by.len(), 2);
}

/// A send executor that reports one named child, for the mixed test.
fn sending_executor() -> Arc<dyn rakka_agent::AgentA2aSendExecutor> {
    struct Sending;
    impl rakka_agent::AgentA2aSendExecutor for Sending {
        fn execute<'a>(
            &'a self,
            _scope: &'a AgentRunScope,
            _intent: &'a AgentRunEffect,
            delegation: &'a rakka_agent::AgentDelegationRecord,
            _credential: Option<&'a AgentEphemeralCredential>,
        ) -> AgentDispatchFuture<'a, rakka_agent::AgentA2aSendFinding> {
            let skill = delegation.requested_skill.as_str().to_string();
            Box::pin(async move {
                Ok(rakka_agent::AgentA2aSendFinding::Sent {
                    child_task: rakka_agent::AgentTaskId::new(format!("child-{skill}"))
                        .expect("task id should be valid"),
                    child_run: None,
                    peer_status: "submitted".to_string(),
                })
            })
        }
    }
    Arc::new(Sending)
}

/// One delegated child's terminal report, exactly as the child task's owed
/// exchange carries it.
fn child_result_envelope(
    fixture: &Fixture,
    delegation: &rakka_agent::AgentDelegationId,
    child_task: &rakka_agent::AgentTaskId,
) -> rakka_agent::AgentExchangeEnvelope {
    let tenant = TenantId::new(TENANT);
    let operation_id = rakka_agent::delegation_result_operation_id(&tenant, delegation)
        .expect("the operation id derives");
    let report = rakka_agent::AgentDelegationReport {
        delegation: delegation.clone(),
        child_task: child_task.clone(),
        child_run: None,
        status: rakka_agent::AgentTaskStatus::Completed,
        terminal_reason: None,
        result_digest: Some(
            AgentTaskContent::inline(json!({ "answer": "done" }))
                .expect("the content is inline-bounded")
                .digest(),
        ),
        descendants_created: 0,
    };
    let payload = rakka_agent::AgentExchangePayload::encode(
        rakka_agent::AGENT_DELEGATION_RESULT_PAYLOAD_TYPE,
        &report,
    )
    .expect("the report encodes");
    let child_scope = rakka_agent::AgentTaskScope::new(tenant, child_task.clone())
        .expect("the child scope is valid");
    rakka_agent::AgentExchangeEnvelope::new(
        operation_id.clone(),
        rakka_agent::AgentExchangeKind::DelegationResult,
        rakka_agent::AgentEntityAddress::Task(child_scope),
        rakka_agent::AgentEntityAddress::Run(run_scope()),
        payload,
        rakka_agent_workflow::AgentCorrelationId::new(operation_id.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid")
}

/// Run state persisted before this slice decodes without the workflow
/// fields, and the new request variant's external tag is pinned: a pre-4.5
/// binary refuses a state holding one loudly, as unknown-variant, rather
/// than misreading it.
#[tokio::test]
async fn a_pre_slice_run_state_decodes_without_the_workflow_fields() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");
    let (invocation, _) = committed_invocation(&fixture).await;

    // The request tag pin, taken while the parked state still retains its
    // turn's effects: the payload is the kebab-case `workflow-start`
    // variant, which a pre-4.5 binary fails closed on instead of
    // misreading. Once the turn records, the effects clear — from then on
    // the widened member set's deny-when-unknown park is the cross-version
    // fence.
    {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let effects = serde_json::to_value(
            run.state()
                .expect("state")
                .loop_state()
                .expect("loop state")
                .effects(),
        )
        .expect("encodes");
        assert!(
            effects.to_string().contains("\"workflow-start\""),
            "the external tag is the wire contract: {effects}"
        );
    }

    deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the result applies");
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state").clone();
    let mut encoded = serde_json::to_value(&state).expect("encodes");
    strip_keys(&mut encoded, &["workflow_invocations"]);
    let decoded: rakka_agent::AgentRunState =
        serde_json::from_value(encoded).expect("a pre-slice state decodes");
    assert!(decoded
        .loop_state()
        .expect("loop state")
        .workflow_invocations()
        .is_empty());
}

fn strip_keys(value: &mut serde_json::Value, keys: &[&str]) {
    match value {
        serde_json::Value::Object(map) => {
            for key in keys {
                map.remove(*key);
            }
            for value in map.values_mut() {
                strip_keys(value, keys);
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
