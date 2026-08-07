//! Durable delegation: the record persisted before the send.
//!
//! Slice 4.3's parent half of scenarios 28 and 39
//! ([specification 8.4](../../docs/plans/rakka-agent/spec.md),
//! [6.6](../../docs/plans/rakka-agent/spec.md)): a model call to the declared
//! coordination tool commits the delegation record and its outbound send
//! effect in one compare-and-set — strictly before anything reaches the sink
//! — every identity is a pure derivation of the run's `(turn, slot)`
//! coordinate, and a refusal is a failed tool result the run survives, never
//! a dead coordinator. The parent *task* record is never written by a
//! delegation, which is what keeps scenario 39's "parent task identity and
//! ownership unchanged" a construction property.

mod common;

use std::sync::Arc;
use std::sync::Mutex;

use common::{
    delegation_config, delegation_tool_id, goal_spec_draft, goal_spec_with_delegation,
    goal_task_creation_command, run_scope, task_definition, Fixture, SKILL,
};
use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::SessionMemoryStore;
use rakka_agent::{
    delegation_id_for, AgentA2aSendExecutor, AgentA2aSendFinding, AgentDelegationRecord,
    AgentDelegationStatus, AgentDispatchFuture, AgentModelTurn, AgentRunEffect, AgentRunEffectKind,
    AgentRunScope, AgentRunStatus, AgentTaskContent, AgentTaskId, AgentToolCallId,
    AgentToolCallRequest, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::json;

fn delegating_turn(arguments: serde_json::Value) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating the translation.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                delegation_tool_id(),
                arguments,
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

/// A scripted send executor: answers every send with the finding it was
/// built with and records the records it saw.
struct StubSendExecutor {
    finding: AgentA2aSendFinding,
    seen: Mutex<Vec<AgentDelegationRecord>>,
}

impl StubSendExecutor {
    fn sent(child_task: &str) -> Arc<Self> {
        Arc::new(Self {
            finding: AgentA2aSendFinding::Sent {
                child_task: AgentTaskId::new(child_task).expect("task id should be valid"),
                child_run: None,
                peer_status: "submitted".to_string(),
            },
            seen: Mutex::new(Vec::new()),
        })
    }
}

impl AgentA2aSendExecutor for StubSendExecutor {
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
        let finding = self.finding.clone();
        Box::pin(async move { Ok(finding) })
    }
}

fn delegating_fixture(executor: Arc<StubSendExecutor>) -> Fixture {
    Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(delegating_turn(json!({
                    "skill": SKILL,
                    "input": { "text": "hello" },
                })))
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor),
    )
    .with_delegation(delegation_config())
}

async fn create_goal_task(fixture: &Fixture) {
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_delegation(), true),
        ))
        .await
        .expect("the goal task should create");
}

/// The record and its send effect commit together, strictly before the sink
/// sees anything; the receipt settles the cell in the same compare-and-set
/// that succeeds the effect, and the turn completes through the synthesized
/// tool result.
#[tokio::test]
async fn a_delegation_persists_its_record_before_the_send_and_settles_on_the_receipt() {
    let executor = StubSendExecutor::sent("child-task-1");
    let fixture = delegating_fixture(executor.clone());
    create_goal_task(&fixture).await;

    // Drive the entities without the dispatcher: the run commits the model
    // effect, the scripted turn, and then — evaluating it — the delegation
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
        // Answer only the model effect, so the send stays outstanding.
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
    let state = fixture.run_snapshot().await.expect("the run should exist");
    let cell = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("the loop is running");
        assert_eq!(loop_state.delegation_count(), 1);
        let (id, cell) = loop_state
            .delegations()
            .iter()
            .next()
            .map(|(id, cell)| (id.clone(), (**cell).clone()))
            .expect("the cell exists");
        assert_eq!(cell.status, AgentDelegationStatus::Pending);
        assert_eq!(id, cell.record.delegation);
        cell
    };
    assert!(executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .is_empty());
    drop(state);

    // Every identity is the pure derivation of the committing coordinate.
    let expected =
        delegation_id_for(&run_scope(), cell.record.turn, cell.record.slot).expect("derives");
    assert_eq!(cell.record.delegation, expected);
    assert_eq!(cell.record.a2a_message_id, expected.as_str());
    assert_eq!(cell.record.deduplication_key, expected.as_str());
    assert_eq!(cell.record.requested_skill.as_str(), SKILL);
    assert_eq!(cell.record.depth, 1);
    assert!(cell.record.lineage.is_empty());

    // Now let the dispatcher answer the send: the cell settles on the
    // receipt, the synthesized tool result completes the turn, and the next
    // scripted turn proposes the parent's own result.
    fixture.pump().await.expect("the loop should converge");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let cell = loop_state
        .delegation(&expected)
        .expect("the cell survives settlement");
    assert_eq!(
        cell.status,
        AgentDelegationStatus::ChildCreated {
            child_task: AgentTaskId::new("child-task-1").expect("task id"),
            child_run: None,
        }
    );
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));

    // The executor saw the persisted record verbatim, exactly once.
    let seen = executor
        .seen
        .lock()
        .expect("the record log should not be poisoned")
        .clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].delegation, expected);

    // Scenario 39's parent half: the parent task's identity and ownership
    // are untouched by the delegation — same assignment generation, same
    // run, and its own accepted result.
    let task = fixture.task_snapshot().await;
    assert_eq!(task.assignment_generation.get(), 1);
    assert!(task.accepted_result.is_some());
    assert!(task.delegation.is_none());
}

/// Reads the refusal code the run's recorded session shows the model — the
/// durable form of the failed tool result, which the loop clears from its
/// own state when the turn records.
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
    for (arguments, code) in [
        // The goal allows only the fixture skill.
        (
            json!({ "skill": "surgery", "input": {} }),
            "delegation-skill-not-allowed",
        ),
        // Model output cannot name an agent: unknown fields fail the parse.
        (
            json!({ "skill": SKILL, "input": {}, "agent": "attacker" }),
            "delegation-invalid-arguments",
        ),
    ] {
        let adapter = DeterministicModelAdapter::new()
            .with_turn(delegating_turn(arguments))
            .with_turn(proposing_turn());
        let executor = StubSendExecutor::sent("child-task-1");
        let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
        let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
        let fixture = Fixture::new(
            ScriptedDispatcher::with_adapter(adapter).with_a2a_send_executor(executor),
        )
        .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
        .with_delegation(delegation_config());
        create_goal_task(&fixture).await;
        fixture.pump().await.expect("the loop should converge");

        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("loop state");
        assert_eq!(
            loop_state.delegation_count(),
            0,
            "refusal {code} left a cell"
        );
        assert_eq!(
            state.status(),
            Some(AgentRunStatus::Completed),
            "the run should survive the {code} refusal"
        );
        assert_eq!(session_refusal_code(&session).await.as_deref(), Some(code));
    }
}

/// A catalog refusal of unchecked length is bounded at the refusal door: the
/// run records a truncated failed tool result under the catalog's own stable
/// code and survives. Unbounded, the oversized text would fail the inline
/// content bound and turn a refusal the run corrects course from into a
/// transition failure every re-drive repeats.
#[tokio::test]
async fn an_unbounded_catalog_refusal_is_truncated_and_survived() {
    struct UnavailableCatalog;
    impl rakka_agent::AgentDelegationCatalog for UnavailableCatalog {
        fn resolve(
            &self,
            _tenant: &rakka_agent::TenantId,
            _skill: &rakka_agent::AgentCapabilityId,
        ) -> Result<rakka_agent::AgentDelegationTarget, rakka_agent::AgentDelegationResolutionError>
        {
            Err(rakka_agent::AgentDelegationResolutionError::Unavailable {
                code: "catalog-backend-down".to_string(),
                message: "x".repeat(64 * 1024),
            })
        }
    }
    let config = rakka_agent::AgentRunDelegationConfig::new(
        delegation_tool_id(),
        Arc::new(UnavailableCatalog),
        std::collections::BTreeSet::from([
            rakka_agent::AgentCoordinationCapabilityKind::Delegation,
        ]),
    )
    .expect("the delegation configuration declares the capability");

    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(delegating_turn(json!({
                "skill": SKILL,
                "input": { "text": "hello" },
            })))
            .with_turn(proposing_turn()),
    ))
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots))
    .with_delegation(config);
    create_goal_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("loop state");
    assert_eq!(loop_state.delegation_count(), 0);
    assert_eq!(
        session_refusal_code(&session).await.as_deref(),
        Some("catalog-backend-down")
    );
}

/// Goal-scope tool narrowing: a generic tool outside the goal's non-empty
/// `allowed_tools` set is refused with a stable code and the run survives.
#[tokio::test]
async fn a_generic_tool_outside_the_goal_set_is_refused() {
    let mut spec = goal_spec_with_delegation();
    spec.allowed_tools =
        std::collections::BTreeSet::from([
            rakka_agent::AgentToolId::new("search").expect("tool id")
        ]);

    let untrusted = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Trying a tool the goal does not allow.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                rakka_agent::AgentToolId::new("exfiltrate").expect("tool id"),
                json!({}),
            )
            .expect("the tool call is bounded"),
        );
    let executor = StubSendExecutor::sent("child-task-1");
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(untrusted)
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor),
    )
    .with_delegation(delegation_config())
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots));
    fixture.instantiate_agent().await;
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
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    drop(run);
    assert_eq!(
        session_refusal_code(&session).await.as_deref(),
        Some("goal-tool-not-allowed")
    );
}

/// A crash at every durable write between the delegating turn and the run's
/// completion converges on one cell, one derived identity, and one executor
/// send — the parent half of scenario 28.
#[tokio::test]
async fn the_delegation_survives_any_owner_loss_with_one_identity() {
    // Sweep run-store crash points: arm point N, drive to the failure,
    // disarm, re-drive from durable state alone, and require convergence.
    for point in 1..24 {
        for window in [
            rakka_agent::testkit::CrashPoint::BeforeWrite,
            rakka_agent::testkit::CrashPoint::AfterWrite,
        ] {
            let executor = StubSendExecutor::sent("child-task-1");
            let fixture = delegating_fixture(executor.clone());
            create_goal_task(&fixture).await;

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
            assert_eq!(loop_state.delegation_count(), 1);
            let cell = loop_state
                .delegations()
                .values()
                .next()
                .expect("the cell exists");
            let expected = delegation_id_for(&run_scope(), cell.record.turn, cell.record.slot)
                .expect("derives");
            assert_eq!(cell.record.delegation, expected);
            assert!(cell.status.is_settled());
            // Every send the executor saw carried the same identity: a re-driven
            // attempt is a replay, never a second delegation.
            let seen = executor
                .seen
                .lock()
                .expect("the record log should not be poisoned")
                .clone();
            assert!(!seen.is_empty());
            assert!(seen.iter().all(|record| record.delegation == expected));
        }
    }
}

/// Records persisted before this slice decode without the new fields: the
/// creation's provenance, the assignment's envelope, and the loop state's
/// cells are all additive.
#[test]
fn pre_slice_records_decode_without_the_delegation_fields() {
    let creation = rakka_agent::AgentTaskCreation {
        definition: task_definition(),
        input: AgentTaskContent::inline(json!({ "ticket": 1 })).expect("bounded"),
        assignee: None,
        goal: None,
        goal_mode: Default::default(),
        goal_spec: None,
        parent: None,
        dependencies: Vec::new(),
        escrow: None,
        wake: None,
        delegation: None,
        telemetry: Default::default(),
    };
    let mut encoded = serde_json::to_value(&creation).expect("encodes");
    encoded
        .as_object_mut()
        .expect("an object")
        .remove("delegation");
    let decoded: rakka_agent::AgentTaskCreation =
        serde_json::from_value(encoded).expect("a pre-slice creation decodes");
    assert!(decoded.delegation.is_none());
}

/// Records persisted before slice 4.4 decode without its additive fields:
/// the ledger's descendants dimension, the record's ancestry and sub-quota,
/// the provenance's ancestry, the envelope's ancestry and fan-in policy, and
/// the goal spec's fan-in declaration all default — and default to the
/// pre-slice semantics.
#[test]
fn pre_slice_records_decode_without_the_fan_out_fields() {
    // The conserved allocation and consumption: a pre-4.4 ledger record has
    // no descendants field, and decodes as unbounded / zero.
    let mut allocation =
        serde_json::to_value(rakka_agent::AgentBudgetAllocation::nothing()).expect("encodes");
    allocation
        .as_object_mut()
        .expect("an object")
        .remove("descendants");
    let allocation: rakka_agent::AgentBudgetAllocation =
        serde_json::from_value(allocation).expect("a pre-slice allocation decodes");
    assert!(allocation.descendants.is_none(), "unbounded, as before");
    let mut consumption =
        serde_json::to_value(rakka_agent::AgentBudgetConsumption::zero()).expect("encodes");
    consumption
        .as_object_mut()
        .expect("an object")
        .remove("descendants");
    let consumption: rakka_agent::AgentBudgetConsumption =
        serde_json::from_value(consumption).expect("a pre-slice consumption decodes");
    assert_eq!(consumption.descendants, 0);

    // The delegation record: ancestry and the descendant sub-quota default,
    // and the untagged grant is what the bounded door refuses over.
    let parent_run = run_scope();
    let delegation = delegation_id_for(&parent_run, 1, 0).expect("the delegation id derives");
    let record = AgentDelegationRecord {
        environments: Default::default(),
        knowledge_spaces: Default::default(),
        delegation: delegation.clone(),
        goal: None,
        parent_task: AgentTaskId::new("goal-root").expect("task id should be valid"),
        parent_run: parent_run.clone(),
        lineage: Vec::new(),
        ancestors: Vec::new(),
        depth: 1,
        requested_skill: rakka_agent::AgentCapabilityId::new(SKILL).expect("capability id"),
        resolved: common::delegation_target(),
        a2a_message_id: delegation.as_str().to_string(),
        deduplication_key: delegation.as_str().to_string(),
        turn: 1,
        slot: 0,
        effect: rakka_agent_workflow::AgentEffectId::new("effect-1"),
        call_id: AgentToolCallId::new("call-1").expect("call id"),
        input: AgentTaskContent::inline(json!({ "text": "hello" })).expect("bounded"),
        result_schema: None,
        budget: None,
        granted_descendants: Some(3),
        deadline: None,
        definition_revision: rakka_agent::AgentRevisionNumber::INITIAL,
        settings_revision: rakka_agent::AgentRevisionNumber::INITIAL,
        telemetry: Default::default(),
        created_at: rakka_agent_workflow::AgentTimestampMillis::new(1),
    };
    let mut encoded = serde_json::to_value(&record).expect("encodes");
    let object = encoded.as_object_mut().expect("an object");
    object.remove("ancestors");
    object.remove("granted_descendants");
    let decoded: AgentDelegationRecord =
        serde_json::from_value(encoded).expect("a pre-slice record decodes");
    assert!(decoded.ancestors.is_empty());
    assert!(decoded.granted_descendants.is_none());
    decoded.validate().expect("the decoded record is coherent");

    // The cell: a pre-slice cell has no child result.
    let mut cell = serde_json::to_value(rakka_agent::AgentDelegationCell::pending(Box::new(
        record.clone(),
    )))
    .expect("encodes");
    cell.as_object_mut().expect("an object").remove("result");
    let cell: rakka_agent::AgentDelegationCell =
        serde_json::from_value(cell).expect("a pre-slice cell decodes");
    assert!(cell.result.is_none());
    assert!(!cell.child_settled());

    // The envelope: ancestry and the fan-in declaration default.
    let mut envelope =
        serde_json::to_value(rakka_agent::AgentRunDelegationEnvelope::default()).expect("encodes");
    let object = envelope.as_object_mut().expect("an object");
    object.remove("ancestors");
    object.remove("fan_in");
    let envelope: rakka_agent::AgentRunDelegationEnvelope =
        serde_json::from_value(envelope).expect("a pre-slice envelope decodes");
    assert!(envelope.ancestors.is_empty());
    assert!(envelope.fan_in.is_none());

    // The goal spec: a pre-slice spec declares no fan-in policy.
    let mut spec = serde_json::to_value(goal_spec_with_delegation()).expect("encodes");
    spec.as_object_mut().expect("an object").remove("fan_in");
    let spec: rakka_agent::AgentGoalSpec =
        serde_json::from_value(spec).expect("a pre-slice goal spec decodes");
    assert!(spec.fan_in.is_none());
}
