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
    /// Refused under a code longer than the run keeps.
    RefusedOversized,
    Conflict,
}

/// A code longer than the run keeps: the detail bound and then some.
fn oversized_start_code() -> String {
    format!(
        "workflow-registry-refused-{}",
        "x".repeat(rakka_agent::AGENT_RUN_DETAIL_MAX_LENGTH)
    )
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
                StartMode::RefusedOversized => AgentWorkflowStartFinding::Refused {
                    code: oversized_start_code(),
                    message: "the registry refused the start".to_string(),
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

/// The code a failed start's cell keeps is bounded as the effect record's
/// is: an executor's oversized code is cut at `AGENT_RUN_DETAIL_MAX_LENGTH`
/// bytes on a character boundary, so the cell a run keeps past its turn
/// cannot grow the record by whatever a deployment's executor answers.
#[tokio::test]
async fn a_failed_starts_cell_keeps_its_code_bounded() {
    let executor = RecordingWorkflowExecutor::new(StartMode::RefusedOversized);
    let fixture = workflow_fixture(executor, workflow_await_turn());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let (_, cell_status) = committed_invocation(&fixture).await;
    let AgentWorkflowInvocationStatus::Failed { code } = cell_status else {
        panic!("the refused start settles its cell failed: {cell_status:?}");
    };
    let delivered = oversized_start_code();
    assert!(delivered.len() > rakka_agent::AGENT_RUN_DETAIL_MAX_LENGTH);
    assert!(
        code.len() <= rakka_agent::AGENT_RUN_DETAIL_MAX_LENGTH,
        "the cell keeps {} bytes",
        code.len()
    );
    assert!(
        !code.is_empty() && delivered.starts_with(&code),
        "the cell keeps a prefix of the delivered code"
    );
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

/// The held start effects whose cells the run holds, and those cells, read
/// from durable state.
async fn held_starts(
    fixture: &Fixture,
) -> (
    Vec<AgentRunEffect>,
    std::collections::BTreeMap<
        rakka_agent::AgentWorkflowInvocationId,
        Box<rakka_agent::AgentWorkflowInvocationCell>,
    >,
) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let starts = loop_state
        .effects()
        .iter()
        .filter(|effect| {
            matches!(
                effect.request,
                rakka_agent::AgentRunEffectRequest::WorkflowStart { .. }
            )
        })
        .cloned()
        .collect();
    (starts, loop_state.workflow_invocations().clone())
}

/// Delivers `receipt` as the dispatcher's result for `effect`.
async fn deliver_start_receipt(
    fixture: &Fixture,
    effect: &AgentRunEffect,
    receipt: rakka_agent::AgentWorkflowStartReceipt,
) -> Result<rakka_agent::AgentRunEntityReply, rakka_agent::AgentRunError> {
    let command = AgentRunEntityCommand::RecordEffectResult {
        operation_id: effect.result_operation_id(&run_scope()).expect("derives"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        attempt: effect.attempts.saturating_add(1),
        fence: 0,
        outcome: Box::new(rakka_agent::AgentRunEffectOutcome::WorkflowStart { receipt }),
    };
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(command, &fixture.router, fixture.now()).await
}

/// Issue #125: a start receipt naming another held invocation settles
/// neither start. Two invocations are in flight; invocation A's receipt,
/// delivered to invocation B's start, is refused `effect-outcome-mismatch`
/// before anything changes, and neither cell moves. Before the guard, B's
/// start settled and cell A took the child. A receipt naming the right
/// invocation and the wrong child run is not the guard's business: it still
/// settles that cell `Failed { workflow-start-run-mismatch }`.
#[tokio::test]
async fn a_foreign_outcome_does_not_settle_a_workflow_start() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking the refund workflow twice and awaiting both.")
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(workflow_call("invoke-2"))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({}),
            )
            .expect("the tool call is bounded"),
        );
    let fixture = workflow_fixture(executor, turn);
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;

    let (starts, cells_before) = held_starts(&fixture).await;
    assert_eq!(starts.len(), 2, "both starts are committed");
    assert_eq!(cells_before.len(), 2);
    assert!(cells_before
        .values()
        .all(|cell| cell.status == AgentWorkflowInvocationStatus::Pending));
    let invocation_of = |effect: &AgentRunEffect| match &effect.request {
        rakka_agent::AgentRunEffectRequest::WorkflowStart { invocation } => {
            invocation.invocation.clone()
        }
        other => panic!("not a start: {other:?}"),
    };
    let (start_a, start_b) = (&starts[0], &starts[1]);
    let (invocation_a, invocation_b) = (invocation_of(start_a), invocation_of(start_b));
    assert_ne!(invocation_a, invocation_b);

    let refusal = deliver_start_receipt(
        &fixture,
        start_b,
        rakka_agent::AgentWorkflowStartReceipt {
            child_run: child_workflow_run_id(&invocation_a),
            invocation: invocation_a.clone(),
            adopted: false,
        },
    )
    .await
    .expect_err("another invocation's receipt settles nothing");
    assert_eq!(refusal.code(), "effect-outcome-mismatch", "{refusal}");
    let (starts_after, cells_after) = held_starts(&fixture).await;
    assert_eq!(cells_after, cells_before, "neither cell moved");
    assert_eq!(starts_after, starts, "neither start moved");

    // The right invocation, the wrong child run: the cell settles failed.
    deliver_start_receipt(
        &fixture,
        start_b,
        rakka_agent::AgentWorkflowStartReceipt {
            invocation: invocation_b.clone(),
            child_run: rakka_agent_workflow::AgentRunId::new("foreign-run"),
            adopted: false,
        },
    )
    .await
    .expect("the mismatched receipt applies as a definitive disposition");
    let (_, cells) = held_starts(&fixture).await;
    assert_eq!(
        cells.get(&invocation_b).expect("cell B is held").status,
        AgentWorkflowInvocationStatus::Failed {
            code: "workflow-start-run-mismatch".to_string()
        }
    );
    assert_eq!(
        cells.get(&invocation_a),
        cells_before.get(&invocation_a),
        "cell A is unmoved"
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
                    reason: None,
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

// ---------------------------------------------------------------------------
// A live run's reconciliation park, answered (issues #131 and #130).
// ---------------------------------------------------------------------------

/// The run's phase, status and turn, its outstanding effects by kind and
/// status, and whether its turn counts one of them.
async fn run_picture(
    fixture: &Fixture,
) -> (
    AgentLoopPhase,
    Option<AgentRunStatus>,
    u64,
    Vec<(String, String)>,
    bool,
) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    (
        loop_state.phase(),
        state.status(),
        loop_state.turn(),
        loop_state
            .outstanding_effects()
            .map(|effect| {
                (
                    effect.kind().as_label().to_string(),
                    effect.status.as_label().to_string(),
                )
            })
            .collect(),
        loop_state.awaits_turn_effect(),
    )
}

/// Applies `command` to the run.
async fn apply_to_run(
    fixture: &Fixture,
    command: AgentRunEntityCommand,
) -> rakka_agent::AgentRunEntityReply {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(command, &fixture.router, fixture.now())
        .await
        .expect("the command applies")
}

/// A run resting on its children whose claim append parks and is answered
/// goes back to resting on them, `Running`, whichever way it is answered.
#[tokio::test]
async fn an_answered_park_puts_a_run_resting_on_its_children_back_there() {
    for answer in ["confirmed", "failed", "not-executed"] {
        let executor = RecordingWorkflowExecutor::new(StartMode::Started);
        let fixture = workflow_fixture(executor, workflow_await_turn());
        fixture.instantiate_agent().await;
        let space =
            rakka_agent::KnowledgeSpaceId::new("space-alpha").expect("the space id is valid");
        let mut spec = goal_spec_with_workflow(None);
        spec.knowledge_spaces.insert(space.clone());
        fixture
            .apply_task_command(goal_task_creation_command(
                task_definition(),
                goal_spec_draft(spec, true),
            ))
            .await
            .expect("the goal task should create");
        fixture.pump().await.expect("the loop should converge");
        let resting = run_picture(&fixture).await;
        assert_eq!(
            (resting.0, resting.1),
            (
                AgentLoopPhase::AwaitingChildren,
                Some(AgentRunStatus::Running)
            )
        );
        apply_to_run(
            &fixture,
            AgentRunEntityCommand::AppendClaim {
                operation_id: rakka_agent::claim_append_operation_id(
                    &run_scope(),
                    "beside-the-park",
                )
                .expect("the operation id derives"),
                append: Box::new(rakka_agent::AgentClaimAppendRequest {
                    space,
                    subject: "finding".to_string(),
                    predicate: "links".to_string(),
                    object: rakka_agent::AgentClaimObjectRequest::Value(
                        AgentTaskContent::inline(json!({ "note": "observed" }))
                            .expect("the object is inline-bounded"),
                    ),
                    confidence_bps: 5_000,
                    classification: rakka_agent::MemoryClassification::Unclassified,
                    evidence: Vec::new(),
                    requested_by: rakka_agent_workflow::PrincipalRef {
                        principal_type: "service".to_string(),
                        principal_id: "researcher".to_string(),
                        display_name: None,
                    },
                }),
            },
        )
        .await;
        let append = {
            let mut run = fixture.run();
            run.recover(fixture.now()).await.expect("recover");
            let state = run.state().expect("state");
            state
                .loop_state()
                .expect("the loop is running")
                .effects()
                .iter()
                .find(|effect| effect.kind() == rakka_agent::AgentRunEffectKind::ClaimAppendCall)
                .cloned()
                .expect("the append is held")
        };
        apply_to_run(
            &fixture,
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: append.result_operation_id(&run_scope()).expect("derives"),
                effect_id: append.effect_id.clone(),
                generation: append.generation,
                attempt: append.attempts.saturating_add(1),
                fence: 0,
                outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                    "dispatcher-lost-after-started",
                    "the recovery retry was refused",
                )),
            },
        )
        .await;
        let parked = run_picture(&fixture).await;
        assert_eq!(parked.1, Some(AgentRunStatus::WaitingForReconciliation));
        let resolution = match answer {
            "confirmed" => rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(rakka_agent::AgentRunEffectOutcome::ClaimAppend {
                    claim: rakka_agent::AgentCommunalClaimId::new("claim-established")
                        .expect("the claim id is valid"),
                }),
            },
            "failed" => rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(rakka_agent::AgentRunEffectOutcome::failed(
                    "claim-refused".to_string(),
                    "a person found the attempt had failed".to_string(),
                )),
            },
            _ => rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        };
        apply_to_run(
            &fixture,
            AgentRunEntityCommand::ResolveIndeterminateEffect {
                operation_id: rakka_agent::AgentOperationId::for_agent(
                    rakka_agent::AgentOperationKind::CheckpointResolution,
                    &common::agent_scope(),
                    "park-answer",
                )
                .expect("the decision key derives"),
                effect_id: append.effect_id.clone(),
                generation: append.generation,
                resolution: Box::new(resolution),
                telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
            },
        )
        .await;
        let answered = run_picture(&fixture).await;
        assert_eq!(
            (answered.0, answered.1),
            (resting.0, resting.1),
            "{answer}: back on its children"
        );
    }
}

/// [`workflow_fixture`], for a workflow whose descriptor says a started run
/// can be cancelled: the run chases a child its fan-in wait timed out.
fn cancellable_workflow_fixture(
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
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(
        rakka_agent::AgentRunWorkflowConfig::new()
            .with_descriptor(common::workflow_tool_descriptor().with_cancellation_support())
            .expect("the workflow configuration should accept the descriptor"),
    )
}

/// A run resting on a child workflow under a deadline, parked on a claim
/// append whose outcome is unknown: the fixture, and the append. Its metrics
/// go to `metrics` where one is given. The child can be cancelled where
/// `cancellable` says so; where it cannot, a deadline that times it out
/// sends nothing after it.
async fn parked_beside_a_fan_in_wait(
    metrics: Option<Arc<rakka_core::InMemoryMetricsRecorder>>,
    cancellable: bool,
) -> (Fixture, AgentRunEffect) {
    parked_beside_a_fan_in_wait_observed(metrics, None, cancellable).await
}

/// [`parked_beside_a_fan_in_wait`], with the run's segments going to
/// `segments` where one is given.
async fn parked_beside_a_fan_in_wait_observed(
    metrics: Option<Arc<rakka_core::InMemoryMetricsRecorder>>,
    segments: Option<Arc<rakka_agent::InMemoryAgentSegmentSink>>,
    cancellable: bool,
) -> (Fixture, AgentRunEffect) {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking the workflow and awaiting it, with a deadline.")
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({ "deadline": 5_000 }),
            )
            .expect("the tool call is bounded"),
        );
    let mut fixture = if cancellable {
        cancellable_workflow_fixture(executor, first_turn)
    } else {
        workflow_fixture(executor, first_turn)
    };
    if let Some(metrics) = metrics {
        fixture = fixture.with_metrics(metrics);
    }
    if let Some(segments) = segments {
        fixture = fixture.with_segments(segments);
    }
    fixture.instantiate_agent().await;
    let space = rakka_agent::KnowledgeSpaceId::new("space-alpha").expect("the space id is valid");
    let mut spec = goal_spec_with_workflow(None);
    spec.knowledge_spaces.insert(space.clone());
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(spec, true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");
    let resting = run_picture(&fixture).await;
    assert_eq!(
        (resting.0, resting.1, resting.2),
        (
            AgentLoopPhase::AwaitingChildren,
            Some(AgentRunStatus::Running),
            1
        )
    );
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::AppendClaim {
            operation_id: rakka_agent::claim_append_operation_id(&run_scope(), "beside-the-wait")
                .expect("the operation id derives"),
            append: Box::new(rakka_agent::AgentClaimAppendRequest {
                space,
                subject: "finding".to_string(),
                predicate: "links".to_string(),
                object: rakka_agent::AgentClaimObjectRequest::Value(
                    AgentTaskContent::inline(json!({ "note": "observed" }))
                        .expect("the object is inline-bounded"),
                ),
                confidence_bps: 5_000,
                classification: rakka_agent::MemoryClassification::Unclassified,
                evidence: Vec::new(),
                requested_by: rakka_agent_workflow::PrincipalRef {
                    principal_type: "service".to_string(),
                    principal_id: "researcher".to_string(),
                    display_name: None,
                },
            }),
        },
    )
    .await;
    let append = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        state
            .loop_state()
            .expect("the loop is running")
            .effects()
            .iter()
            .find(|effect| effect.kind() == rakka_agent::AgentRunEffectKind::ClaimAppendCall)
            .cloned()
            .expect("the append is held")
    };
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: append.result_operation_id(&run_scope()).expect("derives"),
            effect_id: append.effect_id.clone(),
            generation: append.generation,
            attempt: append.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            )),
        },
    )
    .await;
    let parked = run_picture(&fixture).await;
    assert_eq!(
        (parked.0, parked.1, parked.2),
        (
            AgentLoopPhase::AwaitingChildren,
            Some(AgentRunStatus::WaitingForReconciliation),
            1
        ),
        "parked on the append's unknown outcome, still resting on its child"
    );
    (fixture, append)
}

/// Fires the fan-in wait's deadline, which is past.
async fn fire_the_deadline(fixture: &Fixture) -> rakka_agent::AgentRunEntityReply {
    fixture
        .clock
        .store(10_000, std::sync::atomic::Ordering::SeqCst);
    apply_to_run(
        fixture,
        AgentRunEntityCommand::FireFanInDeadline {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, "fan-in-deadline", "1"],
            )
            .expect("the operation id derives"),
        },
    )
    .await
}

/// Answers the append's park: a person found it was never executed.
async fn answer_the_append(fixture: &Fixture, append: &AgentRunEffect) {
    apply_to_run(
        fixture,
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "park-answer",
            )
            .expect("the decision key derives"),
            effect_id: append.effect_id.clone(),
            generation: append.generation,
            resolution: Box::new(rakka_agent::AgentEffectResolution::ConfirmedNotExecuted),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await;
}

/// A fan-in wait's deadline does not resume a run parked on an unknown
/// outcome. The run rests on its child under a deadline, and a claim append
/// it holds beside the wait ends unknown: the run parks. The deadline then
/// fires. The wait is resolved, with the child timed out, and the run stays
/// parked: it records no turn and sends no model call until the
/// reconciliation is answered. The deadline used to write `Running` over the
/// park, and the run went on to its next model call with the outcome still
/// unknown. A child's result that resolves the wait already left the park
/// standing.
///
/// This child cannot be cancelled, so the deadline sends nothing at all. One
/// that can is chased, parked or not: the next test.
#[tokio::test]
async fn a_fan_in_deadline_does_not_resume_a_run_parked_on_an_unknown_outcome() {
    let (fixture, append) = parked_beside_a_fan_in_wait(None, false).await;
    let sent = fixture.dispatched_effects();

    let reply = fire_the_deadline(&fixture).await;

    let rakka_agent::AgentRunEntityReply::Applied { outcome } = reply else {
        panic!("the deadline resolves the wait, got {reply:?}");
    };
    assert_eq!(
        (outcome.status, outcome.phase),
        (
            AgentRunStatus::WaitingForReconciliation,
            AgentLoopPhase::RecordingTurn
        ),
        "the transition that fires the deadline leaves the run parked, and no later pass has \
         to put it back"
    );
    let fired = run_picture(&fixture).await;
    assert_eq!(
        (fired.0, fired.1, fired.2, fixture.dispatched_effects()),
        (
            AgentLoopPhase::RecordingTurn,
            Some(AgentRunStatus::WaitingForReconciliation),
            1,
            sent
        ),
        "the wait is resolved, and the run stays parked: no turn is recorded, and with a child \
         that cannot be cancelled nothing is sent"
    );

    answer_the_append(&fixture, &append).await;

    let answered = run_picture(&fixture).await;
    assert_eq!(
        (answered.0, answered.1, answered.2),
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        ),
        "the answer sets the run going: it records the turn and sends the next model call"
    );
}

/// The park holds the loop. It does not hold the chase a deadline sets off:
/// a child the deadline timed out, and that can be cancelled, is sent its
/// cancellation while the run is parked, as it is on a run that is not. That
/// cancellation is no step of the loop, and a child left running until a
/// person answers an unrelated outcome would run on for as long as that
/// takes. The run stays parked, records no turn, and sends no model call.
#[tokio::test]
async fn a_fan_in_deadline_on_a_parked_run_still_cancels_the_child_it_timed_out() {
    let (fixture, append) = parked_beside_a_fan_in_wait(None, true).await;
    let sent = fixture.dispatched_effects();

    fire_the_deadline(&fixture).await;

    let fired = run_picture(&fixture).await;
    assert_eq!(
        (fired.0, fired.1, fired.2, fixture.dispatched_effects()),
        (
            AgentLoopPhase::RecordingTurn,
            Some(AgentRunStatus::WaitingForReconciliation),
            1,
            sent + 1
        ),
        "the run stays parked on its turn, and one call is sent"
    );
    let out: Vec<(&str, &str)> = fired
        .3
        .iter()
        .map(|(kind, status)| (kind.as_str(), status.as_str()))
        .filter(|(_, status)| *status == "ready")
        .collect();
    assert_eq!(
        out,
        [("workflow-cancel-call", "ready")],
        "the one call out is the timed-out child's cancellation: {:?}",
        fired.3
    );
    let rested = run_revision(&fixture).await;
    settle(&fixture).await;
    assert_eq!(
        (run_revision(&fixture).await, fixture.dispatched_effects()),
        (rested, sent + 1),
        "and the run is at rest there: a further pass writes and sends nothing"
    );

    answer_the_append(&fixture, &append).await;

    let answered = run_picture(&fixture).await;
    assert_eq!(
        (answered.0, answered.1, answered.2),
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        ),
        "the answer sets the run going: it records the turn and sends the next model call"
    );
}

/// On a parked run that is winding down, the deadline resumes nothing: the
/// wait is resolved as evidence, and the run stays where its wind-down rests,
/// on the phase it had. The settle at the deadline's end changes nothing
/// there.
#[tokio::test]
async fn a_fan_in_deadline_resumes_nothing_on_a_parked_run_that_is_winding_down() {
    let (fixture, _append) = parked_beside_a_fan_in_wait(None, false).await;
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, "cancel", "beside-the-wait"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
    )
    .await;
    let cancelled = run_picture(&fixture).await;
    assert_eq!(
        (cancelled.0, cancelled.1),
        (
            AgentLoopPhase::AwaitingChildren,
            Some(AgentRunStatus::WaitingForReconciliation)
        ),
        "cancelled where it stands: parked, on its wait"
    );
    let sent = fixture.dispatched_effects();

    let reply = fire_the_deadline(&fixture).await;

    let rakka_agent::AgentRunEntityReply::Applied { outcome } = reply else {
        panic!("the deadline resolves the wait, got {reply:?}");
    };
    assert_eq!(
        (outcome.status, outcome.phase, fixture.dispatched_effects()),
        (
            AgentRunStatus::WaitingForReconciliation,
            AgentLoopPhase::AwaitingChildren,
            sent
        ),
        "the wait is resolved and nothing resumes: the status and the phase are as they were"
    );
}

/// A record an earlier build left between a deadline and its next step is
/// parked by the settle pass, whatever kind of call the unknown outcome
/// belongs to. Here it is a claim append, which is no call of the turn. The
/// record is this build's own after the deadline, with its status written
/// `Running` as the earlier transition left it. The settle pass parks it
/// before the loop records the turn, and sends nothing: this child cannot be
/// cancelled, so nothing is owed after its deadline. The pass that parks
/// it is no turn: the turn is measured once, when it is recorded.
#[tokio::test]
async fn a_run_an_earlier_build_left_going_past_a_parked_append_is_parked_again() {
    let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());
    let (fixture, append) = parked_beside_a_fan_in_wait(Some(metrics.clone()), false).await;
    fire_the_deadline(&fixture).await;
    let sent = fixture.dispatched_effects();
    let turns_measured = || {
        metrics
            .snapshot()
            .observations_named(rakka_agent::METRIC_AGENT_TURN_DURATION)
            .len()
    };
    let measured = turns_measured();
    let id = run_scope().persistence_id();
    let record = rakka_persistence::DurableStateStore::load(&fixture.runs, &id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    value["run"]["status"] = json!("running");
    rakka_persistence::DurableStateStore::compare_and_set(
        &fixture.runs,
        &id,
        record.revision,
        serde_json::from_value(value).expect("the rewritten record deserializes"),
    )
    .await
    .expect("the rewritten record persists");
    let forged = run_picture(&fixture).await;
    assert_eq!(
        (forged.0, forged.1),
        (AgentLoopPhase::RecordingTurn, Some(AgentRunStatus::Running)),
        "the record as the earlier build left it, before its next step"
    );

    settle(&fixture).await;

    let parked = run_picture(&fixture).await;
    assert_eq!(
        (parked.0, parked.1, parked.2, fixture.dispatched_effects()),
        (
            AgentLoopPhase::RecordingTurn,
            Some(AgentRunStatus::WaitingForReconciliation),
            1,
            sent
        ),
        "the settle pass parks the run before the loop records the turn"
    );
    assert_eq!(
        turns_measured(),
        measured,
        "the pass that parks the run measures no turn"
    );

    answer_the_append(&fixture, &append).await;

    let answered = run_picture(&fixture).await;
    assert_eq!(
        (answered.0, answered.1, answered.2),
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        )
    );
    assert_eq!(
        turns_measured(),
        measured + 1,
        "the turn is measured once, when it is recorded"
    );
}

/// A turn that looks something up, starts a workflow and awaits it under a
/// deadline, at the point issue #189 is about: the look-up's outcome is
/// unknown, so the run is parked, and the workflow's start receipt has just
/// landed. Returns the fixture, the look-up, and the reply to the receipt.
async fn a_tool_call_unknown_beside_a_fan_in_wait(
) -> (Fixture, AgentRunEffect, rakka_agent::AgentRunEntityReply) {
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking it up, invoking the workflow and awaiting it, with a deadline.")
        .with_tool_call(common::tool_call("lookup"))
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({ "deadline": 5_000 }),
            )
            .expect("the tool call is bounded"),
        );
    let fixture = workflow_fixture(
        RecordingWorkflowExecutor::new(StartMode::Started),
        first_turn,
    );
    create_workflow_task(&fixture).await;
    settle(&fixture).await;
    drive_model_turn_only(&fixture).await;
    let lookup = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall).await;
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: lookup.result_operation_id(&run_scope()).expect("derives"),
            effect_id: lookup.effect_id.clone(),
            generation: lookup.generation,
            attempt: lookup.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            )),
        },
    )
    .await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForReconciliation),
            1
        ),
        "parked on the look-up, with the workflow start still out"
    );
    let start = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::WorkflowStartCall).await;
    let receipt = land_result_of(&fixture, &start).await;
    (fixture, lookup, receipt)
}

/// The run's effect of `kind`, as the run now holds it.
async fn turn_call_of(fixture: &Fixture, kind: rakka_agent::AgentRunEffectKind) -> AgentRunEffect {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is running")
        .effects()
        .iter()
        .find(|effect| effect.kind() == kind)
        .cloned()
        .unwrap_or_else(|| panic!("the run holds a {kind}"))
}

/// Lands the result of one outstanding effect, as its executor answers it.
async fn land_result_of(
    fixture: &Fixture,
    effect: &AgentRunEffect,
) -> rakka_agent::AgentRunEntityReply {
    let outcome = fixture.dispatcher.answer(effect).await;
    apply_to_run(
        fixture,
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

/// Answers the park of `effect`: a person found it was never executed, or,
/// where `returned` is given, that it ran and returned that.
async fn answer_the_park_of(
    fixture: &Fixture,
    effect: &AgentRunEffect,
    returned: Option<rakka_agent::AgentRunEffectOutcome>,
) {
    apply_to_run(
        fixture,
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "park-answer",
            )
            .expect("the decision key derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            resolution: Box::new(match returned {
                Some(outcome) => rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                    outcome: Box::new(outcome),
                },
                None => rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
            }),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await;
}

/// Delivers the child workflow's completed result to the run.
async fn the_child_reports(fixture: &Fixture) {
    let (id, _) = committed_invocation(fixture).await;
    deliver_result(
        fixture,
        &id,
        child_workflow_run_id(&id),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the child's result is taken");
}

/// A workflow's start receipt does not rest a turn that holds a tool call
/// with an unknown outcome (issue #189). The receipt is the last of the
/// turn's calls that was out, and it used to rest the turn on its children,
/// parked. The turn now stays open. Its wait is resolved under it, here by
/// the deadline, and that moves nothing. Answered "never executed", the
/// look-up is sent again and the run waits for it, where it used to record
/// the turn without it and send the next model call. The turn is recorded
/// when the look-up returns.
#[tokio::test]
async fn a_start_receipt_does_not_rest_a_turn_whose_tool_call_is_unknown() {
    let (fixture, lookup, receipt) = a_tool_call_unknown_beside_a_fan_in_wait().await;

    let rakka_agent::AgentRunEntityReply::Applied { outcome } = receipt else {
        panic!("the receipt applies, got {receipt:?}");
    };
    assert_eq!(
        (outcome.status, outcome.phase),
        (
            AgentRunStatus::WaitingForReconciliation,
            AgentLoopPhase::AwaitingTools
        ),
        "the receipt is taken, and the turn stays open on the look-up"
    );
    let sent = fixture.dispatched_effects();

    fire_the_deadline(&fixture).await;

    assert_eq!(
        (rest_of(&fixture).await, fixture.dispatched_effects()),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForReconciliation),
                1
            ),
            sent
        ),
        "the wait is resolved under the open turn, and nothing moves"
    );

    answer_the_park_of(&fixture, &lookup, None).await;

    let resent = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall).await;
    assert_eq!(
        (
            rest_of(&fixture).await,
            resent.status,
            fixture.dispatched_effects()
        ),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                1
            ),
            rakka_agent::AgentRunEffectStatus::Ready,
            sent + 1
        ),
        "the look-up is out again, and the run waits for it: one call is sent, and the turn is \
         not recorded"
    );

    land_result_of(&fixture, &resent).await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        ),
        "the look-up returned: the turn is recorded and the next model call is out"
    );
}

/// The look-up is answered "never executed" and returns while the child is
/// still running. The turn then rests where a turn with a child out rests,
/// and goes on when the child reports.
#[tokio::test]
async fn a_tool_call_sent_again_rests_its_turn_on_the_child_it_still_awaits() {
    let (fixture, lookup, _) = a_tool_call_unknown_beside_a_fan_in_wait().await;

    answer_the_park_of(&fixture, &lookup, None).await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForEffect),
            1
        ),
        "the look-up is out again, and the turn is open on it while the child runs"
    );
    let resent = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall).await;
    land_result_of(&fixture, &resent).await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingChildren,
            Some(AgentRunStatus::Running),
            1
        ),
        "the last call of the turn is back, and the turn rests on its child"
    );

    the_child_reports(&fixture).await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        )
    );
}

/// A turn that looks something up, delegates, starts a workflow and awaits
/// both under a deadline, with every one of its calls out.
async fn a_mixed_turn_with_every_call_out(
    mode: StartMode,
    send: Arc<dyn rakka_agent::AgentA2aSendExecutor>,
) -> Fixture {
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking up, delegating, invoking, awaiting.")
        .with_tool_call(common::tool_call("lookup"))
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
                json!({ "deadline": 5_000 }),
            )
            .expect("the tool call is bounded"),
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(RecordingWorkflowExecutor::new(mode))
        .with_a2a_send_executor(send),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config());
    create_workflow_task(&fixture).await;
    settle(&fixture).await;
    drive_model_turn_only(&fixture).await;
    fixture
}

/// Reports the outcome of the run's call of `kind` as unknown.
async fn lose_call_of(fixture: &Fixture, kind: rakka_agent::AgentRunEffectKind) -> AgentRunEffect {
    let effect = turn_call_of(fixture, kind).await;
    apply_to_run(
        fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect.result_operation_id(&run_scope()).expect("derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            )),
        },
    )
    .await;
    effect
}

/// Lands what the executor of the run's call of `kind` answers, and returns
/// where the reply to that command says the run stands. The reply is the
/// transition's own account, written before any later pass.
async fn the_reply_to_the_outcome_of(
    fixture: &Fixture,
    kind: rakka_agent::AgentRunEffectKind,
) -> (AgentRunStatus, AgentLoopPhase) {
    let effect = turn_call_of(fixture, kind).await;
    let reply = land_result_of(fixture, &effect).await;
    let rakka_agent::AgentRunEntityReply::Applied { outcome } = reply else {
        panic!("the outcome of the {kind} applies, got {reply:?}");
    };
    (outcome.status, outcome.phase)
}

/// Where a run stands whose turn is open on a call with an unknown outcome.
const OPEN_AND_PARKED: (AgentRunStatus, AgentLoopPhase) = (
    AgentRunStatus::WaitingForReconciliation,
    AgentLoopPhase::AwaitingTools,
);

/// The receipt of a delegation's send does not rest a turn whose tool call
/// is unknown. It is the last of the turn's calls that was out, and the
/// reply to it used to say the turn rested on its children.
#[tokio::test]
async fn a_send_receipt_does_not_rest_a_turn_whose_tool_call_is_unknown() {
    use rakka_agent::AgentRunEffectKind as Kind;

    let fixture = a_mixed_turn_with_every_call_out(StartMode::Started, sending_executor()).await;
    lose_call_of(&fixture, Kind::ToolCall).await;
    the_reply_to_the_outcome_of(&fixture, Kind::WorkflowStartCall).await;

    assert_eq!(
        the_reply_to_the_outcome_of(&fixture, Kind::A2aSendCall).await,
        OPEN_AND_PARKED,
        "the send's receipt is taken, and the turn stays open on the look-up"
    );
}

/// Nor does the send of a fan-out member that fails. The failure is a
/// disposition of the wait and not of the run, so the run goes on, and its
/// turn stays open on the look-up.
#[tokio::test]
async fn a_members_failed_send_does_not_rest_a_turn_whose_tool_call_is_unknown() {
    use rakka_agent::AgentRunEffectKind as Kind;

    let fixture =
        a_mixed_turn_with_every_call_out(StartMode::Started, failing_send_executor()).await;
    lose_call_of(&fixture, Kind::ToolCall).await;
    the_reply_to_the_outcome_of(&fixture, Kind::WorkflowStartCall).await;

    assert_eq!(
        the_reply_to_the_outcome_of(&fixture, Kind::A2aSendCall).await,
        OPEN_AND_PARKED,
        "the member's failed send is taken, and the turn stays open on the look-up"
    );
}

/// Nor does the start of a fan-out member that is refused.
#[tokio::test]
async fn a_members_failed_start_does_not_rest_a_turn_whose_tool_call_is_unknown() {
    use rakka_agent::AgentRunEffectKind as Kind;

    let fixture = a_mixed_turn_with_every_call_out(StartMode::Refused, sending_executor()).await;
    lose_call_of(&fixture, Kind::ToolCall).await;
    the_reply_to_the_outcome_of(&fixture, Kind::A2aSendCall).await;

    assert_eq!(
        the_reply_to_the_outcome_of(&fixture, Kind::WorkflowStartCall).await,
        OPEN_AND_PARKED,
        "the member's refused start is taken, and the turn stays open on the look-up"
    );
}

/// A turn is held for a delegation's send whose outcome is unknown, and for
/// a workflow start whose outcome is unknown, as it is for a tool call: the
/// look-up's result, the last call that was out, does not rest it.
#[tokio::test]
async fn a_tool_result_does_not_rest_a_turn_whose_send_or_start_is_unknown() {
    use rakka_agent::AgentRunEffectKind as Kind;

    for (unknown, beside) in [
        (Kind::A2aSendCall, Kind::WorkflowStartCall),
        (Kind::WorkflowStartCall, Kind::A2aSendCall),
    ] {
        let fixture =
            a_mixed_turn_with_every_call_out(StartMode::Started, sending_executor()).await;
        lose_call_of(&fixture, unknown).await;
        the_reply_to_the_outcome_of(&fixture, beside).await;

        assert_eq!(
            the_reply_to_the_outcome_of(&fixture, Kind::ToolCall).await,
            OPEN_AND_PARKED,
            "the look-up's result is taken, and the turn stays open on the unknown {unknown}"
        );
    }
}

/// A run on its second turn, whose tool call is out, with a workflow cancel
/// out beside it. Turn one started a workflow that can be cancelled and
/// awaited it under a deadline; the deadline timed the child out, and the
/// run chases it with a cancel.
async fn a_workflow_cancel_out_beside_a_tool_call() -> Fixture {
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking the workflow and awaiting it, with a deadline.")
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("call id should be valid"),
                fan_in_tool_id(),
                json!({ "deadline": 5_000 }),
            )
            .expect("the tool call is bounded"),
        );
    let second_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking it up.")
        .with_tool_call(common::tool_call("lookup"));
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(second_turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(RecordingWorkflowExecutor::new(StartMode::Started)),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(
        rakka_agent::AgentRunWorkflowConfig::new()
            .with_descriptor(common::workflow_tool_descriptor().with_cancellation_support())
            .expect("the workflow configuration should accept the descriptor"),
    );
    create_workflow_task(&fixture).await;
    settle(&fixture).await;
    drive_model_turn_only(&fixture).await;
    the_reply_to_the_outcome_of(&fixture, rakka_agent::AgentRunEffectKind::WorkflowStartCall).await;
    fire_the_deadline(&fixture).await;
    drive_model_turn_only(&fixture).await;
    assert_eq!(
        (
            rest_of(&fixture).await,
            turn_call_of(
                &fixture,
                rakka_agent::AgentRunEffectKind::WorkflowCancelCall
            )
            .await
            .status,
            turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall)
                .await
                .status
        ),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                2
            ),
            rakka_agent::AgentRunEffectStatus::Ready,
            rakka_agent::AgentRunEffectStatus::Ready
        ),
        "turn two's look-up is out, and so is the cancel that chases the timed-out child"
    );
    fixture
}

/// A turn is held for a workflow cancel whose outcome is unknown, as it is
/// for one that is out. The look-up's result does not rest the turn. It used
/// to, parked. Answered "never executed", the cancel is sent again and the
/// turn is held for it, and the pass that follows its return rests the turn.
/// The run used to record the turn and send its next model call with the
/// cancel out.
#[tokio::test]
async fn a_turn_is_held_for_a_workflow_cancel_whose_outcome_is_unknown() {
    use rakka_agent::AgentRunEffectKind as Kind;

    let fixture = a_workflow_cancel_out_beside_a_tool_call().await;
    let cancel = lose_call_of(&fixture, Kind::WorkflowCancelCall).await;

    assert_eq!(
        the_reply_to_the_outcome_of(&fixture, Kind::ToolCall).await,
        OPEN_AND_PARKED,
        "the look-up's result is taken, and the turn is held for the cancel"
    );
    let sent = fixture.dispatched_effects();

    answer_the_park_of(&fixture, &cancel, None).await;

    assert_eq!(
        (rest_of(&fixture).await, fixture.dispatched_effects()),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::Running),
                2
            ),
            sent + 1
        ),
        "the cancel is out again, and the turn is held for it: one call is sent, and it is not \
         the next model call"
    );

    let resent = turn_call_of(&fixture, Kind::WorkflowCancelCall).await;
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: resent.result_operation_id(&run_scope()).expect("derives"),
            effect_id: resent.effect_id.clone(),
            generation: resent.generation,
            attempt: resent.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::WorkflowCancel {
                already_finished: false,
            }),
        },
    )
    .await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            3
        ),
        "the cancel returned: the turn is recorded and the next model call is out"
    );
}

/// Leaves the record of [`a_tool_call_unknown_beside_a_fan_in_wait`] as a
/// build before issue #189 left it: the start receipt rested the turn on its
/// children over the unknown look-up. The record is this build's own with
/// the two values the earlier build wrote differently: the phase, and the
/// phase in the stored reply to the receipt.
async fn rested_on_its_children_by_an_earlier_build() -> (Fixture, AgentRunEffect) {
    let (fixture, lookup, _) = a_tool_call_unknown_beside_a_fan_in_wait().await;
    let id = run_scope().persistence_id();
    let record = rakka_persistence::DurableStateStore::load(&fixture.runs, &id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    value["run"]["loop_state"]["phase"] = json!("awaiting-children");
    let replies = value["applied_operations"]
        .as_array_mut()
        .expect("the stored replies are a list");
    let last = replies.last_mut().expect("the receipt's reply is stored");
    let start = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::WorkflowStartCall).await;
    assert_eq!(
        last["operation_id"],
        json!(start.result_operation_id(&run_scope()).expect("derives")),
        "the last stored reply is the one to the start receipt"
    );
    last["outcome"]["phase"] = json!("awaiting-children");
    rakka_persistence::DurableStateStore::compare_and_set(
        &fixture.runs,
        &id,
        record.revision,
        serde_json::from_value(value).expect("the rewritten record deserializes"),
    )
    .await
    .expect("the rewritten record persists");
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingChildren,
            Some(AgentRunStatus::WaitingForReconciliation),
            1
        ),
        "the record as the earlier build left it: resting on its child, the look-up unknown"
    );
    (fixture, lookup)
}

/// The settle pass opens again a turn an earlier build rested on its
/// children over an unknown call. One pass does it and the next writes
/// nothing. The child's report then resolves the wait under the open turn,
/// and the answer, here what the look-up returned, rests the turn.
#[tokio::test]
async fn a_turn_an_earlier_build_rested_on_its_children_over_an_unknown_call_is_opened_again() {
    let (fixture, lookup) = rested_on_its_children_by_an_earlier_build().await;
    let sent = fixture.dispatched_effects();

    settle(&fixture).await;

    assert_eq!(
        (rest_of(&fixture).await, fixture.dispatched_effects()),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForReconciliation),
                1
            ),
            sent
        ),
        "the turn is open again, on the park it was under"
    );
    let opened = run_revision(&fixture).await;
    settle(&fixture).await;
    assert_eq!(
        run_revision(&fixture).await,
        opened,
        "a further settle pass writes nothing"
    );

    the_child_reports(&fixture).await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForReconciliation),
            1
        ),
        "the child's report resolves the wait under the open turn"
    );

    let returned = fixture.dispatcher.answer(&lookup).await;
    answer_the_park_of(&fixture, &lookup, Some(returned)).await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        ),
        "the answer is the look-up's result: the turn rests, is recorded, and the next model \
         call is out"
    );
}

/// The child's report may be the first thing this build does to such a
/// record. The report moves the resting turn on, as it moves any turn that
/// rests on its children, and the pass that follows opens it before the loop
/// can record it. The run stays parked throughout.
#[tokio::test]
async fn a_turn_an_earlier_build_rested_on_its_children_is_opened_when_its_child_reports_first() {
    let (fixture, lookup) = rested_on_its_children_by_an_earlier_build().await;
    let sent = fixture.dispatched_effects();

    the_child_reports(&fixture).await;

    assert_eq!(
        (rest_of(&fixture).await, fixture.dispatched_effects()),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForReconciliation),
                1
            ),
            sent
        ),
        "the wait is resolved, and the turn is open on the look-up, parked"
    );

    answer_the_park_of(&fixture, &lookup, None).await;
    let resent = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall).await;
    assert_eq!(
        (
            rest_of(&fixture).await,
            resent.status,
            fixture.dispatched_effects()
        ),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                1
            ),
            rakka_agent::AgentRunEffectStatus::Ready,
            sent + 1
        ),
        "the look-up is out again, and the run waits for it"
    );
    land_result_of(&fixture, &resent).await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        )
    );
}

/// A record an earlier build left between two steps: the look-up was sent
/// again while the turn rested on its child, the child then reported, and
/// that report moved the turn on to be recorded, `Running`, with the look-up
/// still out. The earlier build recorded the turn in its next step. A
/// process that stopped between the two left this record.
///
/// The settle pass opens the turn before the loop records it, and puts the
/// run on the wait of a turn with a call out. The record is this build's
/// own at that point, with what the earlier report wrote: the phase and the
/// status, on the run and in the stored reply to the report.
#[tokio::test]
async fn a_turn_an_earlier_build_moved_on_with_a_call_out_is_put_back_on_its_wait() {
    let (fixture, lookup) = rested_on_its_children_by_an_earlier_build().await;
    answer_the_park_of(&fixture, &lookup, None).await;
    the_child_reports(&fixture).await;
    let id = run_scope().persistence_id();
    let record = rakka_persistence::DurableStateStore::load(&fixture.runs, &id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    value["run"]["loop_state"]["phase"] = json!("recording-turn");
    value["run"]["status"] = json!("running");
    let replies = value["applied_operations"]
        .as_array_mut()
        .expect("the stored replies are a list");
    let last = replies.last_mut().expect("the report's reply is stored");
    last["outcome"]["phase"] = json!("recording-turn");
    last["outcome"]["status"] = json!("running");
    rakka_persistence::DurableStateStore::compare_and_set(
        &fixture.runs,
        &id,
        record.revision,
        serde_json::from_value(value).expect("the rewritten record deserializes"),
    )
    .await
    .expect("the rewritten record persists");
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::RecordingTurn,
            Some(AgentRunStatus::Running),
            1
        ),
        "the record as the earlier build left it, before its next step"
    );
    let sent = fixture.dispatched_effects();

    settle(&fixture).await;

    assert_eq!(
        (rest_of(&fixture).await, fixture.dispatched_effects()),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                1
            ),
            sent
        ),
        "the turn is open again and the run waits on the look-up: no turn is recorded and no \
         model call is sent"
    );

    let resent = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall).await;
    land_result_of(&fixture, &resent).await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        )
    );
}

/// The answer may be the first thing this build does to such a record, and
/// the child may report while the call it sent again is out. The pass that
/// follows the answer opens the turn, so the child's report finds a turn
/// that waits on a call and leaves it there. Left resting on its children,
/// the report would have moved the turn on to be recorded with the look-up
/// still out.
#[tokio::test]
async fn a_turn_an_earlier_build_rested_on_its_children_waits_for_a_call_sent_again() {
    let (fixture, lookup) = rested_on_its_children_by_an_earlier_build().await;
    let sent = fixture.dispatched_effects();

    answer_the_park_of(&fixture, &lookup, None).await;

    let resent = turn_call_of(&fixture, rakka_agent::AgentRunEffectKind::ToolCall).await;
    assert_eq!(
        (
            rest_of(&fixture).await,
            resent.status,
            fixture.dispatched_effects()
        ),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                1
            ),
            rakka_agent::AgentRunEffectStatus::Ready,
            sent + 1
        ),
        "the look-up is out again, and the turn is open on it"
    );

    the_child_reports(&fixture).await;

    assert_eq!(
        (rest_of(&fixture).await, fixture.dispatched_effects()),
        (
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                1
            ),
            sent + 1
        ),
        "the child's report leaves the turn waiting on the look-up"
    );

    land_result_of(&fixture, &resent).await;

    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        )
    );
}

/// Issue #123. An `Any` fan-in that resolves on its first child chases the
/// other with a workflow cancel, and the run goes on to propose its result.
/// The task accepts while the cancel is still out. The run used to end
/// there; a cancel that then ended unknown parked on the ended run with no
/// checkpoint, and nothing could answer it. The run now waits, accepted. The
/// park opens its checkpoint as on any live run, a person answers it, and
/// the run ends.
#[tokio::test]
async fn an_accepted_run_waits_for_the_stragglers_cancel_it_sent() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
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
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(executor),
    )
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
    let (_, cells) = held_starts(&fixture).await;
    let first = cells.keys().next().expect("one").clone();
    deliver_result(
        &fixture,
        &first,
        child_workflow_run_id(&first),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the first child's result records");

    /// The run's status, the code of its terminal reason, and its waiting
    /// checkpoints.
    async fn standing(
        fixture: &Fixture,
    ) -> (
        AgentRunStatus,
        Option<String>,
        Vec<rakka_agent::AgentCheckpoint>,
    ) {
        let snapshot = fixture.run_snapshot().await.expect("the run exists");
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let waiting = run
            .state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .open_checkpoints()
            .iter()
            .filter(|checkpoint| checkpoint.status.is_waiting())
            .cloned()
            .collect();
        (
            snapshot.status,
            snapshot
                .terminal_reason
                .as_ref()
                .map(|reason| reason.code().to_string()),
            waiting,
        )
    }

    // A review of the goal is commanded too. It waits behind its approval,
    // so it is committed and not sent.
    apply_to_run(&fixture, evaluate_goal(true)).await;

    // The model proposes, and the task accepts, with the cancel out.
    drive_model_turn_only(&fixture).await;
    let (status, reason, waiting) = standing(&fixture).await;
    assert_eq!(
        (status, reason.as_deref(), waiting.len()),
        (AgentRunStatus::Cancelling, Some("result-accepted"), 0),
        "the result is accepted, the run has not ended, and the review's \
         approval is closed: what was never sent is cancelled when the end \
         is decided, not when the run ends"
    );
    {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let review = run
            .state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .find(|effect| effect.kind() == rakka_agent::AgentRunEffectKind::GoalEvaluationCall)
            .cloned()
            .expect("the review is held");
        assert_eq!(review.status, rakka_agent::AgentRunEffectStatus::Cancelled);
    }
    let cancel = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .find(|effect| effect.kind() == rakka_agent::AgentRunEffectKind::WorkflowCancelCall)
            .cloned()
            .expect("the straggler's cancel is held")
    };
    assert_eq!(cancel.status, rakka_agent::AgentRunEffectStatus::Ready);

    // The cancel's attempt ends unknown.
    let reply = apply_to_run(
        &fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: cancel.result_operation_id(&run_scope()).expect("derives"),
            effect_id: cancel.effect_id.clone(),
            generation: cancel.generation,
            attempt: cancel.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            )),
        },
    )
    .await;
    assert!(
        matches!(reply, rakka_agent::AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    let (status, reason, waiting) = standing(&fixture).await;
    assert_eq!(
        (status, reason.as_deref()),
        (
            AgentRunStatus::WaitingForReconciliation,
            Some("result-accepted")
        )
    );
    assert_eq!(waiting.len(), 1, "the park opened its checkpoint");
    assert_eq!(
        waiting[0].kind,
        rakka_agent::AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(waiting[0].bound_effect.effect_id, cancel.effect_id);

    // A person answers it, and the run ends as its accepted result says.
    let reply = apply_to_run(
        &fixture,
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "the-stragglers-cancel",
            )
            .expect("the decision key derives"),
            checkpoint_id: waiting[0].checkpoint_id.clone(),
            resolver: rakka_agent_workflow::PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(rakka_agent::AgentCheckpointDecision::Reconciliation(
                rakka_agent::AgentReconciliationDecision::AbandonAndFail,
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await;
    assert!(
        matches!(reply, rakka_agent::AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    let (status, reason, waiting) = standing(&fixture).await;
    assert_eq!(
        (status, reason.as_deref(), waiting.len()),
        (AgentRunStatus::Completed, Some("result-accepted"), 0)
    );
}

/// A run that waits for a call it sent does not cancel its children. The
/// wait shows as `Cancelling`, which the chase read as a cancelled scope: an
/// accepted run that happened to hold a goal evaluation sent a workflow
/// cancel to every child it had started and awaited none of, where an
/// accepted run that held nothing left them running. Whether a completed
/// task's children were stopped turned on an unrelated call.
#[tokio::test]
async fn an_accepted_run_that_waits_leaves_its_children_as_one_that_does_not() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking two workflows and awaiting neither.")
        .with_tool_call(workflow_call("invoke-1"))
        .with_tool_call(workflow_call("invoke-2"));
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(executor),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(
        rakka_agent::AgentRunWorkflowConfig::new()
            .with_descriptor(common::workflow_tool_descriptor().with_cancellation_support())
            .expect("the workflow configuration should accept the descriptor"),
    );
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;
    let (starts, _) = held_starts(&fixture).await;
    assert_eq!(starts.len(), 2);
    for start in &starts {
        let invocation = match &start.request {
            rakka_agent::AgentRunEffectRequest::WorkflowStart { invocation } => {
                invocation.invocation.clone()
            }
            other => panic!("not a start: {other:?}"),
        };
        deliver_start_receipt(
            &fixture,
            start,
            rakka_agent::AgentWorkflowStartReceipt {
                child_run: child_workflow_run_id(&invocation),
                invocation,
                adopted: false,
            },
        )
        .await
        .expect("the receipt applies");
    }
    // An evaluation is committed beside turn two's model call, and sent.
    apply_to_run(&fixture, evaluate_goal(false)).await;

    // Turn two proposes, and the task accepts with the evaluation out.
    drive_model_turn_only(&fixture).await;
    settle(&fixture).await;

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let record = state.run().expect("the run exists");
    assert_eq!(
        (
            record.status,
            record
                .terminal_reason
                .as_ref()
                .map(|reason| reason.code().to_string())
        ),
        (
            AgentRunStatus::Cancelling,
            Some("result-accepted".to_string())
        ),
        "the run waits for its evaluation"
    );
    let loop_state = state.loop_state().expect("the loop is started");
    assert_eq!(
        loop_state
            .effects()
            .iter()
            .filter(|effect| effect.is_outstanding())
            .map(|effect| effect.kind())
            .collect::<Vec<_>>(),
        vec![rakka_agent::AgentRunEffectKind::GoalEvaluationCall],
        "and for nothing else: no cancel is sent to a child"
    );
    for (invocation, cell) in loop_state.workflow_invocations() {
        assert!(
            cell.cancel.is_none(),
            "{invocation}: the child is not chased"
        );
    }
}

/// The same for a child the run delegated to. A waiting run owes it no
/// delegation cancel: not while it waits, not after an operator's `Cancel`,
/// which changes neither the run's reason nor what it does to its children,
/// and not at its `Completed` end. From the second review's probe.
#[tokio::test]
async fn an_accepted_run_that_waits_leaves_a_delegated_child_alone() {
    use rakka_persistence::DurableStateStore;

    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating and awaiting nothing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("delegate-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(sending_executor()),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config());
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;

    // Answer the delegation send, so the child exists.
    let send = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .find(|effect| {
                effect.kind() == rakka_agent::AgentRunEffectKind::A2aSendCall
                    && effect.is_outstanding()
            })
            .cloned()
            .expect("the send is out")
    };
    let outcome = fixture.dispatcher.answer(&send).await;
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: send.result_operation_id(&run_scope()).expect("derives"),
            effect_id: send.effect_id.clone(),
            generation: send.generation,
            attempt: send.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        },
    )
    .await;

    // An evaluation is committed beside turn two's model call, and sent.
    apply_to_run(&fixture, evaluate_goal(false)).await;
    // Turn two proposes, and the task accepts with the evaluation out.
    drive_model_turn_only(&fixture).await;
    settle(&fixture).await;
    settle(&fixture).await;

    /// Status, reason code, the delegation cell's status and cancel outcome,
    /// and whether the stored record names the delegation-cancel operation.
    async fn picture(fixture: &Fixture) -> (AgentRunStatus, Option<String>, String, String, bool) {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let record = state.run().expect("the run exists");
        let loop_state = state.loop_state().expect("the loop is started");
        let (delegation, cell) = loop_state
            .delegations()
            .iter()
            .next()
            .expect("the delegation cell exists");
        let operation =
            rakka_agent::delegation_cancel_operation_id(&TenantId::new(TENANT), delegation)
                .expect("derives");
        let stored = fixture
            .runs
            .load(&run_scope().persistence_id())
            .await
            .expect("loads")
            .expect("exists");
        let json = serde_json::to_string(&stored.state).expect("serializes");
        (
            record.status,
            record
                .terminal_reason
                .as_ref()
                .map(|reason| reason.code().to_string()),
            format!("{:?}", cell.status).chars().take(40).collect(),
            format!("{:?}", cell.cancel),
            json.contains(operation.as_str()),
        )
    }

    let waiting = picture(&fixture).await;

    // An operator cancels the waiting run.
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::Cancellation,
                &common::agent_scope(),
                "operator-cancel",
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
    )
    .await;
    settle(&fixture).await;
    let cancelled = picture(&fixture).await;

    land_the_evaluation(&fixture).await;
    settle(&fixture).await;
    let ended = picture(&fixture).await;

    assert_eq!(
        (waiting.0, waiting.1.as_deref(), waiting.4),
        (AgentRunStatus::Cancelling, Some("result-accepted"), false),
        "waiting, and no delegation cancel is owed: {waiting:?}"
    );
    assert!(waiting.2.starts_with("ChildCreated"), "{waiting:?}");
    assert_eq!(waiting.3, "None");
    assert_eq!(
        (cancelled.0, cancelled.1.as_deref(), cancelled.4),
        (AgentRunStatus::Cancelling, Some("result-accepted"), false)
    );
    assert_eq!(
        (ended.0, ended.1.as_deref(), ended.4),
        (AgentRunStatus::Completed, Some("result-accepted"), false)
    );
}

/// A cancelling run waits for its children as well as its calls. Its
/// proposal is rejected while a child it started has not reported. It used
/// to end there, `Cancelled`, over a child whose outcome it did not know.
/// It goes on winding down until the child reports.
#[tokio::test]
async fn a_cancelling_run_whose_proposal_is_rejected_waits_for_its_child() {
    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking one workflow and awaiting nothing.")
        .with_tool_call(workflow_call("invoke-1"));
    // The task's rule wants a non-empty answer, so this proposal is rejected.
    let unacceptable = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "" }))
                .expect("the proposal is inline-bounded"),
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(unacceptable),
        )
        .with_workflow_start_executor(executor),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(
        rakka_agent::AgentRunWorkflowConfig::new()
            .with_descriptor(common::workflow_tool_descriptor().with_cancellation_support())
            .expect("the workflow configuration should accept the descriptor"),
    );
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;
    let (starts, _) = held_starts(&fixture).await;
    assert_eq!(starts.len(), 1);
    let invocation = match &starts[0].request {
        rakka_agent::AgentRunEffectRequest::WorkflowStart { invocation } => {
            invocation.invocation.clone()
        }
        other => panic!("not a start: {other:?}"),
    };
    deliver_start_receipt(
        &fixture,
        &starts[0],
        rakka_agent::AgentWorkflowStartReceipt {
            child_run: child_workflow_run_id(&invocation),
            invocation: invocation.clone(),
            adopted: false,
        },
    )
    .await
    .expect("the receipt applies");

    /// Applies `command` while the task cannot be reached.
    async fn apply_out_of_reach(fixture: &Fixture, command: AgentRunEntityCommand) {
        let nowhere = rakka_agent::AgentExchangeRouter::new();
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.apply(command, &nowhere, fixture.now())
            .await
            .expect("the command applies");
    }
    /// The outstanding effect of `kind`, with its result as a command.
    async fn result_of(
        fixture: &Fixture,
        kind: rakka_agent::AgentRunEffectKind,
    ) -> AgentRunEntityCommand {
        let effect = {
            let mut run = fixture.run();
            run.recover(fixture.now()).await.expect("recover");
            run.state()
                .expect("state")
                .loop_state()
                .expect("the loop is started")
                .effects()
                .iter()
                .find(|effect| effect.kind() == kind && effect.is_outstanding())
                .cloned()
                .expect("the effect is out")
        };
        let outcome = fixture.dispatcher.answer(&effect).await;
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect.result_operation_id(&run_scope()).expect("derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        }
    }
    /// The run's status, whether its proposal is pending, and how many
    /// calls it has out.
    async fn standing(fixture: &Fixture) -> (AgentRunStatus, bool, usize) {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("the loop is started");
        (
            state.status().expect("the run exists"),
            loop_state.proposal().is_some(),
            loop_state.outstanding_effects().count(),
        )
    }

    // Turn two proposes while the task cannot be reached, the run is
    // cancelled, and the cancel it sends its child is settled. All it still
    // waits for is its proposal's answer and its child.
    let proposal = result_of(&fixture, rakka_agent::AgentRunEffectKind::ModelCall).await;
    apply_out_of_reach(&fixture, proposal).await;
    apply_out_of_reach(
        &fixture,
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::Cancellation,
                &common::agent_scope(),
                "operator-cancel",
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
    )
    .await;
    let chase = result_of(
        &fixture,
        rakka_agent::AgentRunEffectKind::WorkflowCancelCall,
    )
    .await;
    apply_out_of_reach(&fixture, chase).await;
    assert_eq!(
        standing(&fixture).await,
        (AgentRunStatus::Cancelling, true, 0),
        "cancelling, its proposal unanswered, no call out"
    );

    // The rejection lands.
    settle(&fixture).await;

    assert_eq!(
        standing(&fixture).await,
        (AgentRunStatus::Cancelling, false, 0),
        "the rejection is taken, and the run still waits for its child"
    );

    deliver_result(
        &fixture,
        &invocation,
        child_workflow_run_id(&invocation),
        AgentWorkflowTerminalStatus::Cancelled,
    )
    .await
    .expect("the child's result records");

    assert_eq!(
        standing(&fixture).await,
        (AgentRunStatus::Cancelled, false, 0),
        "the child reported, and the run ends as cancelled"
    );
}

/// A run with a straggler's workflow cancel out goes back to `Running` when
/// a claim append's park is answered.
///
/// An `Any` fan-in that resolves on its first child chases the other with a
/// workflow cancel. The run is not winding down, and the cancel is counted
/// among its turn's effects, but it was committed without a status and its
/// success writes none. A status read from it when the park ended put the
/// run on `WaitingForEffect`, with nothing to take it off again when the
/// cancel settled.
#[tokio::test]
async fn an_answered_park_with_a_stragglers_cancel_out_leaves_the_run_running() {
    use rakka_agent::testkit::ExchangeFault;

    let executor = RecordingWorkflowExecutor::new(StartMode::Started);
    let first_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
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
        );
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first_turn)
                .with_turn(proposing_turn()),
        )
        .with_workflow_start_executor(executor),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(
        rakka_agent::AgentRunWorkflowConfig::new()
            .with_descriptor(common::workflow_tool_descriptor().with_cancellation_support())
            .expect("the workflow configuration should accept the descriptor"),
    );
    fixture.instantiate_agent().await;
    let space = rakka_agent::KnowledgeSpaceId::new("space-alpha").expect("the space id is valid");
    let mut spec = goal_spec_with_workflow(Some(rakka_agent::AgentFanInPolicy::Any));
    spec.knowledge_spaces.insert(space.clone());
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(spec, true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");

    let (_, cells) = held_starts(&fixture).await;
    assert_eq!(cells.len(), 2, "two invocations");
    let first = cells.keys().next().expect("one").clone();
    deliver_result(
        &fixture,
        &first,
        child_workflow_run_id(&first),
        AgentWorkflowTerminalStatus::Completed,
    )
    .await
    .expect("the first child's result records");

    // The proposal's deliveries are lost from here on, so the run rests on it.
    for _ in 0..64 {
        fixture.task_transport.inject(ExchangeFault::LoseEnvelope);
    }
    drive_model_turn_only(&fixture).await;
    let resting = run_picture(&fixture).await;
    let snapshot = fixture.run_snapshot().await.expect("the run exists");
    assert!(snapshot.proposal.is_some(), "the run rests on its proposal");
    assert!(
        snapshot.terminal_reason.is_none(),
        "and is not winding down"
    );
    assert_eq!(
        resting.1,
        Some(AgentRunStatus::Running),
        "it rests `Running`"
    );
    assert!(
        resting
            .3
            .iter()
            .any(|(kind, _)| kind == "workflow-cancel-call"),
        "with the straggler's cancel out: {:?}",
        resting.3
    );
    assert!(resting.4, "which its turn counts among its effects");

    // A claim append parks and is confirmed.
    let reply = apply_to_run(
        &fixture,
        AgentRunEntityCommand::AppendClaim {
            operation_id: rakka_agent::claim_append_operation_id(&run_scope(), "beside-the-park")
                .expect("the operation id derives"),
            append: Box::new(rakka_agent::AgentClaimAppendRequest {
                space,
                subject: "finding".to_string(),
                predicate: "links".to_string(),
                object: rakka_agent::AgentClaimObjectRequest::Value(
                    AgentTaskContent::inline(json!({ "note": "observed" }))
                        .expect("the object is inline-bounded"),
                ),
                confidence_bps: 5_000,
                classification: rakka_agent::MemoryClassification::Unclassified,
                evidence: Vec::new(),
                requested_by: rakka_agent_workflow::PrincipalRef {
                    principal_type: "service".to_string(),
                    principal_id: "researcher".to_string(),
                    display_name: None,
                },
            }),
        },
    )
    .await;
    assert!(
        matches!(reply, rakka_agent::AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    let append = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        state
            .loop_state()
            .expect("the loop is running")
            .effects()
            .iter()
            .find(|effect| effect.kind() == rakka_agent::AgentRunEffectKind::ClaimAppendCall)
            .cloned()
            .expect("the append is held")
    };
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: append.result_operation_id(&run_scope()).expect("derives"),
            effect_id: append.effect_id.clone(),
            generation: append.generation,
            attempt: append.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            )),
        },
    )
    .await;
    apply_to_run(
        &fixture,
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "park-confirmed",
            )
            .expect("the decision key derives"),
            effect_id: append.effect_id.clone(),
            generation: append.generation,
            resolution: Box::new(rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(rakka_agent::AgentRunEffectOutcome::ClaimAppend {
                    claim: rakka_agent::AgentCommunalClaimId::new("claim-established")
                        .expect("the claim id is valid"),
                }),
            }),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await;
    let answered = run_picture(&fixture).await;
    assert_eq!(
        (answered.0, answered.1),
        (resting.0, resting.1),
        "the answered park puts the run back where it rested"
    );
}

// ---------------------------------------------------------------------------
// An approval wait whose gate closed without a grant (issue #166).
// ---------------------------------------------------------------------------

/// Where the run rests: its phase, status and turn.
async fn rest_of(fixture: &Fixture) -> (AgentLoopPhase, Option<AgentRunStatus>, u64) {
    let picture = run_picture(fixture).await;
    (picture.0, picture.1, picture.2)
}

/// The run's stored revision.
async fn run_revision(fixture: &Fixture) -> rakka_persistence::Revision {
    use rakka_persistence::DurableStateStore;

    fixture
        .runs
        .load(&run_scope().persistence_id())
        .await
        .expect("the run record loads")
        .expect("the run record exists")
        .revision
}

/// Runs the run's settle pass from durable state.
async fn settle(fixture: &Fixture) {
    let mut run = fixture.run();
    let now = fixture.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fixture.router, now)
        .await
        .expect("the run settles");
}

/// The run's waiting gates, each with the kind of effect it is bound to.
async fn waiting_gates(fixture: &Fixture) -> Vec<(rakka_agent::AgentCheckpoint, String)> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    loop_state
        .open_checkpoints()
        .iter()
        .filter(|checkpoint| checkpoint.status.is_waiting())
        .map(|checkpoint| {
            let bound = loop_state
                .effects()
                .iter()
                .find(|effect| effect.effect_id == checkpoint.bound_effect.effect_id)
                .map_or("gone", |effect| effect.kind().as_label());
            (checkpoint.clone(), bound.to_string())
        })
        .collect()
}

/// The one waiting gate bound to an effect of `kind`.
async fn the_gate_on(fixture: &Fixture, kind: &str) -> rakka_agent::AgentCheckpoint {
    let gates = waiting_gates(fixture).await;
    let mut bound: Vec<_> = gates
        .iter()
        .filter(|(_, bound)| bound == kind)
        .map(|(gate, _)| gate.clone())
        .collect();
    assert_eq!(bound.len(), 1, "one gate on a {kind}: {gates:?}");
    bound.remove(0)
}

/// A grant or a denial of `gate`, under a decision key of its own.
fn gate_decision(
    gate: &rakka_agent::AgentCheckpoint,
    key: &str,
    grant: bool,
) -> AgentRunEntityCommand {
    let decision = if grant {
        rakka_agent::AgentApprovalDecision::Approve {
            credential_binding: None,
            expires_at: AgentTimestampMillis::new(10_000_000),
            allowed_use_count: 1,
        }
    } else {
        rakka_agent::AgentApprovalDecision::Deny {
            reason: "not this one".to_string(),
        }
    };
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: rakka_agent::AgentOperationId::for_agent(
            rakka_agent::AgentOperationKind::CheckpointResolution,
            &common::agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: gate.checkpoint_id.clone(),
        resolver: rakka_agent_workflow::PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "approver".to_string(),
            display_name: None,
        },
        decision: Box::new(rakka_agent::AgentCheckpointDecision::Approval(decision)),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

/// An evaluation of the goal: a human review, which is always gated, or an
/// assertion, which is gated only where the deployment's spec gates it.
fn evaluate_goal(human_review: bool) -> AgentRunEntityCommand {
    let mut request = common::goal_evaluation_request(rakka_agent::AgentRevisionNumber::INITIAL);
    if human_review {
        request.method = rakka_agent::AgentGoalEvaluationMethod::HumanReview;
    }
    AgentRunEntityCommand::EvaluateGoal {
        operation_id: rakka_agent::evaluation_operation_id(&run_scope(), "beside-the-fan-out")
            .expect("the operation id derives"),
        evaluation: Box::new(request),
    }
}

/// Lands the dispatcher's answer to the outstanding goal evaluation, and
/// nothing else.
async fn land_the_evaluation(fixture: &Fixture) {
    let evaluation = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let found = state
            .loop_state()
            .expect("the loop is running")
            .outstanding_effects()
            .find(|effect| effect.kind() == rakka_agent::AgentRunEffectKind::GoalEvaluationCall)
            .cloned();
        found.expect("the evaluation is out")
    };
    let outcome = fixture.dispatcher.answer(&evaluation).await;
    apply_to_run(
        fixture,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: evaluation
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: evaluation.effect_id.clone(),
            generation: evaluation.generation,
            attempt: evaluation.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        },
    )
    .await;
}

/// One delegation plus the declared await.
fn delegate_await_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating, then awaiting.")
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
}

/// Gates that expire a second after they open.
fn a_second() -> rakka_agent::AgentCheckpointSla {
    rakka_agent::AgentCheckpointSla {
        due_after_ms: None,
        expire_after_ms: Some(1_000),
        escalation_target: None,
    }
}

/// An idempotent spec of two attempts that needs an approval.
fn approval_gated() -> rakka_agent::AgentEffectSpec {
    rakka_agent::AgentEffectSpec::idempotent(2)
        .expect("the spec is valid")
        .with_checkpoint_required()
}

/// Issue #166: the gate on a fan-out's one member closes without a grant
/// while a goal evaluation is out. The member's failure is a fan-in
/// disposition, which the run survives, and it does not rest the turn, which
/// is held for the evaluation. The run is taken off the gate's approval wait
/// in that transition, onto the wait a held turn has, and goes on once the
/// evaluation settles. It used to stay `AwaitingTools` and
/// `WaitingForApproval` with no gate and nothing in flight, and no review of
/// the goal is involved.
///
/// The member is a workflow start whose gate is denied, one whose gate
/// expires, and an A2A send whose gate is denied.
#[tokio::test]
async fn a_fan_out_members_gate_that_closes_with_an_evaluation_out_does_not_strand_the_run() {
    for (member, closing) in [
        ("workflow-start-call", "denied"),
        ("workflow-start-call", "expired"),
        ("a2a-send-call", "denied"),
    ] {
        let case = format!("{member} {closing}");
        let mut fixture = if member == "a2a-send-call" {
            Fixture::new(
                ScriptedDispatcher::with_adapter(
                    DeterministicModelAdapter::new()
                        .with_turn(delegate_await_turn())
                        .with_turn(proposing_turn()),
                )
                .with_a2a_send_executor(sending_executor()),
            )
            .with_delegation(delegation_config_with_fan_in())
            .with_workflow_tools(workflow_config())
        } else {
            workflow_fixture(
                RecordingWorkflowExecutor::new(StartMode::Started),
                workflow_await_turn(),
            )
        };
        fixture.policies = rakka_agent::AgentEffectPolicies::new()
            .with_workflow_start_spec(approval_gated())
            .expect("the gated start spec is valid")
            .with_a2a_send_spec(approval_gated())
            .expect("the gated send spec is valid")
            .with_checkpoint_sla(a_second());
        create_workflow_task(&fixture).await;

        // An evaluation of the goal, ungated, committed beside the model
        // call.
        apply_to_run(&fixture, evaluate_goal(false)).await;
        drive_model_turn_only(&fixture).await;
        assert_eq!(
            rest_of(&fixture).await,
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForApproval),
                1
            ),
            "{case}: the turn waits on the member's gate"
        );
        let gate = the_gate_on(&fixture, member).await;

        if closing == "denied" {
            apply_to_run(&fixture, gate_decision(&gate, "member-denied", false)).await;
        } else {
            let expires = gate.expires_at.expect("the SLA stamped an expiry");
            fixture
                .clock
                .store(expires.as_millis() + 1, Ordering::SeqCst);
            apply_to_run(
                &fixture,
                AgentRunEntityCommand::FireCheckpointTimers {
                    operation_id: rakka_agent::AgentOperationId::for_agent(
                        rakka_agent::AgentOperationKind::CheckpointResolution,
                        &common::agent_scope(),
                        "timer",
                    )
                    .expect("the timer key derives"),
                },
            )
            .await;
        }
        let held = run_picture(&fixture).await;
        assert_eq!(
            (held.0, held.1, held.2, held.4),
            (
                AgentLoopPhase::AwaitingTools,
                Some(AgentRunStatus::WaitingForEffect),
                1,
                true
            ),
            "{case}: the turn is held for the evaluation, on the effect wait: {held:?}"
        );
        assert!(
            waiting_gates(&fixture).await.is_empty(),
            "{case}: no gate is left"
        );

        land_the_evaluation(&fixture).await;
        assert_eq!(
            rest_of(&fixture).await,
            (
                AgentLoopPhase::AwaitingModel,
                Some(AgentRunStatus::WaitingForEffect),
                2
            ),
            "{case}: the run goes on to its next turn"
        );
    }
}

/// A released run waits on a gate that still stands. The member's approval
/// is denied while the evaluation the turn is held for waits on a security
/// authorization of its own: the run reports that authorization, a settle
/// pass writes nothing, and its grant puts the run on the effect wait.
#[tokio::test]
async fn a_denied_start_gate_leaves_the_run_on_the_evaluations_authorization() {
    let mut fixture = workflow_fixture(
        RecordingWorkflowExecutor::new(StartMode::Started),
        workflow_await_turn(),
    );
    let policies = rakka_agent::AgentEffectPolicies::new();
    let evaluation_spec = policies
        .spec_for(&rakka_agent::AgentRunEffectRequest::Evaluation {
            evaluation: Box::new(common::goal_evaluation_request(
                rakka_agent::AgentRevisionNumber::INITIAL,
            )),
        })
        .clone()
        .with_authorization_required();
    fixture.policies = policies
        .with_workflow_start_spec(approval_gated())
        .expect("the gated start spec is valid")
        .with_goal_evaluation_spec(evaluation_spec)
        .expect("the gated evaluation spec is valid");
    create_workflow_task(&fixture).await;
    apply_to_run(&fixture, evaluate_goal(false)).await;
    drive_model_turn_only(&fixture).await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForApproval),
            1
        ),
        "an approval is reported ahead of an authorization"
    );
    let start = the_gate_on(&fixture, "workflow-start-call").await;
    let authorization = the_gate_on(&fixture, "goal-evaluation-call").await;

    apply_to_run(&fixture, gate_decision(&start, "start-denied", false)).await;
    let standing = (rest_of(&fixture).await, run_revision(&fixture).await);
    assert_eq!(
        standing.0,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForAuthorization),
            1
        ),
        "the run waits on the gate that still stands"
    );
    settle(&fixture).await;
    assert_eq!(
        (rest_of(&fixture).await, run_revision(&fixture).await),
        standing,
        "a settle pass writes nothing"
    );

    apply_to_run(
        &fixture,
        gate_decision(&authorization, "evaluation-authorized", true),
    )
    .await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForEffect),
            1
        ),
        "the grant puts the held turn on the effect wait"
    );
    land_the_evaluation(&fixture).await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingModel,
            Some(AgentRunStatus::WaitingForEffect),
            2
        ),
        "the run goes on to its next turn"
    );
}

/// A review of the goal granted while the run rests on its children puts the
/// run on no dispatch wait: it rests there `Running`, as it did before the
/// review was commanded.
#[tokio::test]
async fn a_review_granted_while_the_run_rests_on_its_children_leaves_its_status() {
    let fixture = workflow_fixture(
        RecordingWorkflowExecutor::new(StartMode::Started),
        workflow_await_turn(),
    );
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");
    let resting = (
        AgentLoopPhase::AwaitingChildren,
        Some(AgentRunStatus::Running),
        1,
    );
    assert_eq!(rest_of(&fixture).await, resting);

    apply_to_run(&fixture, evaluate_goal(true)).await;
    assert_eq!(
        rest_of(&fixture).await,
        resting,
        "an open review moves no status"
    );
    let review = the_gate_on(&fixture, "goal-evaluation-call").await;
    apply_to_run(&fixture, gate_decision(&review, "review-granted", true)).await;
    assert_eq!(
        rest_of(&fixture).await,
        resting,
        "the grant puts the run on no dispatch wait"
    );
    settle(&fixture).await;
    assert_eq!(rest_of(&fixture).await, resting);
}

/// A turn that fans out to two workflows and awaits them.
fn two_starts_await_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Invoking two workflows and awaiting them.")
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

/// Two fan-out members, each behind a security authorization, and a review
/// of the goal commanded after the turn committed: the run is on the
/// authorizations' wait, with the review's approval open beside them.
async fn two_authorizations_and_a_late_review() -> Fixture {
    let mut fixture = workflow_fixture(
        RecordingWorkflowExecutor::new(StartMode::Started),
        two_starts_await_turn(),
    );
    fixture.policies = rakka_agent::AgentEffectPolicies::new()
        .with_workflow_start_spec(
            rakka_agent::AgentEffectSpec::idempotent(2)
                .expect("the spec is valid")
                .with_authorization_required(),
        )
        .expect("the gated start spec is valid");
    create_workflow_task(&fixture).await;
    drive_model_turn_only(&fixture).await;
    apply_to_run(&fixture, evaluate_goal(true)).await;
    let gates: Vec<String> = waiting_gates(&fixture)
        .await
        .iter()
        .map(|(gate, bound)| format!("{} on {bound}", gate.kind.as_label()))
        .collect();
    assert_eq!(
        gates,
        [
            "security-authorization on workflow-start-call",
            "security-authorization on workflow-start-call",
            "approval on goal-evaluation-call"
        ]
    );
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForAuthorization),
            1
        ),
        "the review commanded after the commit moved no status"
    );
    fixture
}

/// A grant recomputes the dispatch wait whatever else stands; a gate that
/// closes without one releases a wait only when it was the last of its kind.
/// So with two members behind authorizations and a review commanded after
/// the turn committed, granting one member's gate reports the review, and
/// denying that gate leaves the run on the other member's authorization.
/// Both are true waits. The grant's half is what issue #166 left as it was;
/// the denial's half writes nothing, because the wait still stands.
#[tokio::test]
async fn a_members_grant_recomputes_the_wait_and_its_denial_keeps_a_wait_that_stands() {
    let fixture = two_authorizations_and_a_late_review().await;
    let gate = waiting_gates(&fixture).await.remove(0).0;
    apply_to_run(&fixture, gate_decision(&gate, "first-start", true)).await;
    assert_eq!(
        rest_of(&fixture).await,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForApproval),
            1
        ),
        "the grant recomputes the dispatch wait, and an approval is reported first"
    );

    let fixture = two_authorizations_and_a_late_review().await;
    let gate = waiting_gates(&fixture).await.remove(0).0;
    apply_to_run(&fixture, gate_decision(&gate, "first-start", false)).await;
    let standing = (rest_of(&fixture).await, run_revision(&fixture).await);
    assert_eq!(
        standing.0,
        (
            AgentLoopPhase::AwaitingTools,
            Some(AgentRunStatus::WaitingForAuthorization),
            1
        ),
        "the other member's authorization still holds the wait"
    );
    settle(&fixture).await;
    assert_eq!(
        (rest_of(&fixture).await, run_revision(&fixture).await),
        standing,
        "a settle pass writes nothing"
    );
}

/// The labels of the segments `sink` has closed since it held `since`.
fn closed_since(sink: &rakka_agent::InMemoryAgentSegmentSink, since: usize) -> Vec<&'static str> {
    sink.operations().split_off(since)
}

/// A command that resolves the fan-in wait of a parked run has not resumed
/// it (issue #191). The run rests on its child, parked on a claim append
/// whose outcome is unknown. The deadline, or the child's report, resolves
/// the wait: the phase moves off the wait and the run stays parked, so it
/// records no turn until a person answers. That command used to close a
/// `run-resume` segment, because only the phase was asked. The answer then
/// takes the run on to its next model call, which is its next wait, and
/// closes none either, as no command that leaves a run on a wait does.
#[tokio::test]
async fn a_wait_resolved_on_a_parked_run_closes_no_resume_segment() {
    for resolver in ["the deadline", "the child's report"] {
        let sink = Arc::new(rakka_agent::InMemoryAgentSegmentSink::new());
        let (fixture, append) =
            parked_beside_a_fan_in_wait_observed(None, Some(sink.clone()), false).await;
        let before = sink.operations().len();

        if resolver == "the deadline" {
            fire_the_deadline(&fixture).await;
        } else {
            the_child_reports(&fixture).await;
        }

        let closed = closed_since(&sink, before);
        assert_eq!(
            (rest_of(&fixture).await, closed.contains(&"run-resume")),
            (
                (
                    AgentLoopPhase::RecordingTurn,
                    Some(AgentRunStatus::WaitingForReconciliation),
                    1
                ),
                false
            ),
            "{resolver} resolves the wait and the run stays parked: nothing resumed: {closed:?}"
        );

        let before = sink.operations().len();
        answer_the_append(&fixture, &append).await;

        let closed = closed_since(&sink, before);
        assert_eq!(
            (rest_of(&fixture).await, closed.contains(&"run-resume")),
            (
                (
                    AgentLoopPhase::AwaitingModel,
                    Some(AgentRunStatus::WaitingForEffect),
                    2
                ),
                false
            ),
            "the answer takes the run on to its next wait: {closed:?}"
        );
    }
}

/// What the change leaves as it was: a run resting on its children is on a
/// wait by its phase, under `Running`. The start receipt that rests the turn
/// there closes no `run-resume` segment, and neither does the child's
/// report, which takes the run on to its next model call.
#[tokio::test]
async fn a_command_that_rests_the_run_on_its_children_closes_no_resume_segment() {
    let sink = Arc::new(rakka_agent::InMemoryAgentSegmentSink::new());
    let fixture = workflow_fixture(
        RecordingWorkflowExecutor::new(StartMode::Started),
        workflow_await_turn(),
    )
    .with_segments(sink.clone());
    create_workflow_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    assert_eq!(
        (
            rest_of(&fixture).await,
            sink.operations().contains(&"run-resume")
        ),
        (
            (
                AgentLoopPhase::AwaitingChildren,
                Some(AgentRunStatus::Running),
                1
            ),
            false
        ),
        "the run rests on its child, and nothing has resumed: {:?}",
        sink.operations()
    );

    the_child_reports(&fixture).await;

    assert_eq!(
        (
            rest_of(&fixture).await,
            sink.operations().contains(&"run-resume")
        ),
        (
            (
                AgentLoopPhase::AwaitingModel,
                Some(AgentRunStatus::WaitingForEffect),
                2
            ),
            false
        ),
        "the child's report takes the run on to its next wait: {:?}",
        sink.operations()
    );
}
