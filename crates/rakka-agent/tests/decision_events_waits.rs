//! The waits a run enters, on the decision stream (issue #119, ask 4).
//!
//! Specification: sections 8.4, 8.7, 8.9, 9.7 and 17.7. A run's own decision
//! stream says when the run enters a wait that something outside it must
//! end: a budget top-up its parent owes, the children of a fan-in, a handoff
//! whose target has not resolved, or its own wind-down. Each wait is recorded
//! once, by the transition that leaves the run in it having started outside
//! it — never by a settle pass that re-rests the same wait, and never by a
//! transition that ends the run. The waits are derived when the write is
//! sealed, from the record before and after the transition, so no site that
//! rests a turn needs to know about them.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor,
    AgentAssignmentGeneration, AgentBudgetAllocation, AgentBudgetCeilings, AgentDecisionEvent,
    AgentDecisionEventVocabulary, AgentDecisionKind, AgentDecisionSource, AgentDispatchFuture,
    AgentEffectPolicies, AgentEffectResolution, AgentEffectSpec, AgentEntityClass,
    AgentExchangeDeliveryError, AgentExchangeDeliveryFuture, AgentExchangeEnvelope,
    AgentExchangeKind, AgentExchangeRouter, AgentExchangeTransport, AgentHandoffRecord,
    AgentLoopPhase, AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunEffect,
    AgentRunEffectOutcome, AgentRunEntityCommand, AgentRunEntityReply, AgentRunError,
    AgentRunScope, AgentRunStatus, AgentSchemaPolicy, AgentTaskContent, AgentTaskDefinition,
    AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskStatus,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, InMemoryAgentRunEffectSink, TenantId,
    VersionedAgentRecord, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentEphemeralCredential;

mod common;

use common::*;

const BUDGET_TOP_UP: &str = "budget-top-up";
const AWAITING_CHILDREN: &str = "awaiting-children";
const AWAITING_HANDOFF: &str = "awaiting-handoff";
const WINDING_DOWN: &str = "winding-down";

// ---------------------------------------------------------------------------
// Reading the stream.
// ---------------------------------------------------------------------------

fn kinds(events: &[AgentDecisionEvent]) -> Vec<AgentDecisionKind> {
    events.iter().map(|event| event.kind).collect()
}

fn waits(events: &[AgentDecisionEvent]) -> Vec<&AgentDecisionEvent> {
    events
        .iter()
        .filter(|event| event.kind == AgentDecisionKind::Wait)
        .collect()
}

fn waits_for<'a>(events: &'a [AgentDecisionEvent], reason: &str) -> Vec<&'a AgentDecisionEvent> {
    waits(events)
        .into_iter()
        .filter(|event| event.reason_code.as_deref() == Some(reason))
        .collect()
}

/// The operation id a decision with `slot` is recorded under in `turn`.
fn decision_operation_id(turn: u64, slot: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::Command,
        [
            TENANT,
            AGENT,
            run_scope().run().as_str(),
            "decision",
            &turn.to_string(),
            slot,
        ],
    )
    .expect("the decision's operation id derives")
}

/// Asserts `events` hold exactly one wait for `reason`, recorded by the
/// run's deterministic policy with no checkpoint, effect or outcome, and
/// returns it. A version-2 wait carries `status`, the status the run entered
/// it in; a version-1 wait carries none of the version-2 fields.
fn one_wait<'a>(
    events: &'a [AgentDecisionEvent],
    reason: &str,
    status: AgentRunStatus,
) -> &'a AgentDecisionEvent {
    let found = waits_for(events, reason);
    assert_eq!(found.len(), 1, "one {reason} wait: {:?}", kinds(events));
    let wait = found[0];
    assert_eq!(wait.source, AgentDecisionSource::DeterministicPolicy);
    let written = (VersionedAgentRecord::schema_version(wait).get() == 2).then_some(status);
    assert_eq!(wait.run_status, written, "{reason} wait's status");
    assert_eq!(wait.checkpoint, None);
    assert_eq!(wait.effect, None);
    assert_eq!(wait.outcome, None);
    wait
}

/// The entry ordinal a fan-in or handoff wait's slot carries,
/// `{prefix}-q{n}`: the run's decision sequence as it stood before the write
/// that entered the wait committed, so it is below the wait's own sequence.
fn entry_ordinal(wait: &AgentDecisionEvent, prefix: &str) -> u64 {
    (0..wait.sequence)
        .find(|n| wait.operation_id == decision_operation_id(wait.turn, &format!("{prefix}-q{n}")))
        .unwrap_or_else(|| {
            panic!(
                "the wait's slot is {prefix}-q{{n}} with n below its sequence: {}",
                wait.operation_id.as_str()
            )
        })
}

/// Runs three more settle passes over `router`, as a recovery sweep would.
async fn settle_three_times(fx: &Fixture, router: &AgentExchangeRouter) {
    for _ in 0..3 {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(router, fx.now())
            .await
            .expect("the run settles");
    }
}

/// The run's durable decision cursor: how many decisions it has given a
/// sequence, including any the sink refused as duplicates.
async fn decision_sequence(fx: &Fixture) -> u64 {
    load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
        .loop_state()
        .expect("the loop exists")
        .decision_sequence()
}

/// Asserts that three more settle passes over `router` record nothing: the
/// sink holds the same events, and the run gave no sequence to a decision
/// the sink then refused as a duplicate.
async fn assert_re_rest_records_nothing(
    fx: &Fixture,
    sink: &CountingDecisionSink,
    router: &AgentExchangeRouter,
) {
    let before = sink.events(&run_scope());
    assert!(!waits(&before).is_empty(), "the run rests in a wait");
    let cursor = decision_sequence(fx).await;
    assert_eq!(cursor, before.len() as u64, "every decision was flushed");
    settle_three_times(fx, router).await;
    assert_eq!(
        sink.events(&run_scope()),
        before,
        "settle passes that re-rest the same wait record nothing"
    );
    assert_eq!(
        decision_sequence(fx).await,
        cursor,
        "no sequence is consumed"
    );
    assert_decision_stream_whole(sink, &run_scope());
}

/// Applies one command through a freshly recovered entity, as a caller on any
/// node would.
async fn apply(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, AgentRunError> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(command, &fx.router, fx.now()).await
}

/// Replays `command` under its own operation id: it is answered from the
/// run's ledger and records nothing more.
async fn assert_replay_records_nothing(
    fx: &Fixture,
    sink: &CountingDecisionSink,
    command: AgentRunEntityCommand,
) {
    let before = sink.events(&run_scope());
    let reply = apply(fx, command).await.expect("the replay is answered");
    assert!(
        matches!(reply, AgentRunEntityReply::Duplicate { .. }),
        "{reply:?}"
    );
    assert_eq!(
        sink.events(&run_scope()),
        before,
        "a replayed command records nothing more"
    );
    assert_decision_stream_whole(sink, &run_scope());
}

fn cancel_run(key: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::Cancel {
        operation_id: AgentOperationId::new(AgentOperationKind::Cancellation, [TENANT, AGENT, key])
            .expect("the operation id derives"),
        reason: "operator stopped the work".to_string(),
    }
}

fn sink_wired(
    fx: Fixture,
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let sink = CountingDecisionSink::new();
    let fx = fx
        .with_decision_events(sink.clone())
        .with_decision_event_vocabulary(vocabulary);
    (fx, sink)
}

// ---------------------------------------------------------------------------
// The budget top-up: tests/escrow_ledger.rs's flow.
// ---------------------------------------------------------------------------

fn empty_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("Still working.")
}

fn answering_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A task that escrows its run one loop iteration but holds four, so the
/// run's first top-up can be funded.
fn toppable_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve a ticket whose run may need a second iteration.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(4),
        ..AgentBudgetCeilings::unbounded()
    })
    .with_run_allocation(AgentBudgetAllocation {
        loop_iterations: Some(1),
        ..AgentBudgetAllocation::unbounded()
    })
}

/// A task route whose parent has not yet answered the run's top-up request:
/// the request stays outstanding, and every other exchange reaches the
/// fixture's own task.
struct WithheldTopUp {
    task: Arc<dyn AgentExchangeTransport>,
}

impl AgentExchangeTransport for WithheldTopUp {
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        if envelope.kind() == AgentExchangeKind::BudgetAllocation {
            return Box::pin(async {
                Err(AgentExchangeDeliveryError::new(
                    "test-withheld",
                    "the parent has not answered the top-up yet",
                ))
            });
        }
        self.task.deliver(envelope)
    }
}

fn withholding_router(fx: &Fixture) -> AgentExchangeRouter {
    AgentExchangeRouter::new()
        .with_route(
            AgentEntityClass::Task,
            Arc::new(WithheldTopUp {
                task: Arc::new(fx.task_transport.clone()),
            }),
        )
        .with_route(AgentEntityClass::Run, Arc::new(fx.run_transport.clone()))
}

/// Drives the run over `router` until a pass changes nothing.
async fn drive_with(fx: &Fixture, router: &AgentExchangeRouter) {
    for _ in 0..16 {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        let progress = run
            .settle_side_effects(router, fx.now())
            .await
            .expect("the run settles");
        let answered = fx
            .dispatcher
            .drive(&mut run, router, fx.now())
            .await
            .expect("the dispatcher drives");
        if progress.transitions == 0 && progress.effects_dispatched == 0 && answered == 0 {
            return;
        }
    }
    panic!("the run did not rest");
}

/// Drives a run that exhausts its one escrowed iteration to its top-up
/// park, with the parent's answer withheld.
async fn parked_on_top_up(
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>, AgentExchangeRouter) {
    let (fx, sink) = sink_wired(
        Fixture::new(
            ScriptedDispatcher::new()
                .with_turn(empty_turn())
                .with_turn(answering_turn()),
        ),
        vocabulary,
    );
    fx.instantiate_agent().await;
    fx.create_task_with(toppable_definition()).await;
    let withheld = withholding_router(&fx);
    drive_with(&fx, &withheld).await;
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, AgentRunStatus::Running);
    assert_eq!(
        snapshot.pending_top_up.map(|pending| pending.sequence),
        Some(1),
        "the run parked on its first top-up"
    );
    (fx, sink, withheld)
}

#[tokio::test]
async fn a_top_up_park_records_one_budget_wait() {
    let (fx, sink, _withheld) = parked_on_top_up(AgentDecisionEventVocabulary::Version2).await;
    let events = sink.events(&run_scope());
    let wait = one_wait(&events, BUDGET_TOP_UP, AgentRunStatus::Running);
    assert_eq!(
        wait.operation_id,
        decision_operation_id(wait.turn, "wait-top-up-1"),
        "the slot is the top-up's own sequence"
    );
    // The transition that iterates into the exhausted turn records its own
    // decision first, then the wait it left the run in.
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::Continue,
            AgentDecisionKind::Wait,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // The parent answers: the grant resumes the run, which completes with
    // still the one wait on its stream.
    fx.pump().await.expect("the run tops up and completes");
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, AgentRunStatus::Completed);
    assert_eq!(snapshot.budget.top_ups(), 1);
    let events = sink.events(&run_scope());
    let resumed = one_wait(&events, BUDGET_TOP_UP, AgentRunStatus::Running);
    assert_eq!(resumed, wait, "the wait is not recorded again");
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::Continue,
            AgentDecisionKind::Wait,
            AgentDecisionKind::SubmitResult,
            AgentDecisionKind::Complete,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // A parent with nothing to grant: the run asks once and fails, its one
    // wait before its end.
    let (fx, sink) = sink_wired(
        Fixture::new(ScriptedDispatcher::new()),
        AgentDecisionEventVocabulary::Version2,
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump()
        .await
        .expect("the run asks, is refused, and stops");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Failed
    );
    let events = sink.events(&run_scope());
    one_wait(&events, BUDGET_TOP_UP, AgentRunStatus::Running);
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::Continue,
            AgentDecisionKind::Continue,
            AgentDecisionKind::Continue,
            AgentDecisionKind::Wait,
            AgentDecisionKind::Fail,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // `wait` is a version-1 kind: the default vocabulary records it, and a
    // bc0c224 reader decodes the whole flow.
    let (fx, sink, _withheld) = parked_on_top_up(AgentDecisionEventVocabulary::Version1).await;
    fx.pump().await.expect("the run tops up and completes");
    let events = sink.events(&run_scope());
    one_wait(&events, BUDGET_TOP_UP, AgentRunStatus::Running);
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &run_scope());
}

// ---------------------------------------------------------------------------
// The fan-in: tests/fan_out_fan_in.rs's flow.
// ---------------------------------------------------------------------------

/// Drives the fan-out turn until the run rests awaiting both children.
async fn fan_out_parked(
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let (fx, sink) = sink_wired(fan_out_fixture(SkillNamedExecutor::new()), vocabulary);
    create_fan_out_task(&fx, None).await;
    fx.pump().await.expect("the fan-out parks");
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(snapshot.status, AgentRunStatus::Running);
    (fx, sink)
}

async fn deliver(fx: &Fixture, envelope: &AgentExchangeEnvelope) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .accept(envelope, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(reply.result().is_accepted(), "{reply:?}");
}

#[tokio::test]
async fn a_fan_in_records_one_children_wait() {
    let (fx, sink) = fan_out_parked(AgentDecisionEventVocabulary::Version2).await;
    let events = sink.events(&run_scope());
    let wait = one_wait(&events, AWAITING_CHILDREN, AgentRunStatus::Running);
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Wait,
        ]
    );
    let call_tools = &events[1];
    assert_eq!(
        wait.turn, call_tools.turn,
        "the wait keeps the fan-out's turn"
    );
    assert_eq!(
        entry_ordinal(wait, "wait-children"),
        call_tools.sequence,
        "the ordinal is the sequence as it stood before the entering write"
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // Both children report: the first records without resolving `All`, the
    // second resolves the group and the run proposes and completes — still
    // with the one wait.
    let children = committed_children(&fx).await;
    assert_eq!(children.len(), 2);
    for (delegation, child_task) in &children {
        deliver(
            &fx,
            &child_result_envelope(&fx, delegation, child_task, AgentTaskStatus::Completed),
        )
        .await;
    }
    fx.pump().await.expect("the resumed run completes");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
    let events = sink.events(&run_scope());
    let resumed = one_wait(&events, AWAITING_CHILDREN, AgentRunStatus::Running);
    assert_eq!(resumed, wait, "the wait is not recorded again");
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Wait,
            AgentDecisionKind::Continue,
            AgentDecisionKind::SubmitResult,
            AgentDecisionKind::Complete,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // The default vocabulary records it too, readable by bc0c224.
    let (_fx, sink) = fan_out_parked(AgentDecisionEventVocabulary::Version1).await;
    let events = sink.events(&run_scope());
    one_wait(&events, AWAITING_CHILDREN, AgentRunStatus::Running);
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &run_scope());
}

// ---------------------------------------------------------------------------
// The handoff awaiting its target: tests/handoff_record.rs's flow.
// ---------------------------------------------------------------------------

fn handoff_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                handoff_tool_id(),
                serde_json::json!({
                    "skill": HANDOFF_SKILL,
                    "reason": "needs billing authority",
                    "context": ["artifact://ticket-1/notes"],
                }),
            )
            .expect("the tool call is bounded"),
        )
}

/// A handoff send whose peer recorded the transfer without the task ever
/// learning of it, so the target never resolves and the source run rests
/// awaiting it.
struct UnresolvedHandoff;

impl AgentA2aHandoffSendExecutor for UnresolvedHandoff {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _handoff: &'a AgentHandoffRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
        Box::pin(async {
            Ok(AgentA2aHandoffFinding::Recorded {
                target_generation: Some(AgentAssignmentGeneration::new(2)),
                peer_status: "working".to_string(),
            })
        })
    }
}

/// The handoff fixture of tests/handoff_record.rs: a goal task whose run
/// hands off on its first turn. With `resolving`, the send records the
/// transfer on the task, which resolves it; otherwise nothing ever does.
async fn handoff_flow(
    vocabulary: AgentDecisionEventVocabulary,
    resolving: bool,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let (fx, sink) = sink_wired(
        Fixture::new(ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(handoff_turn())
                .with_turn(answering_turn()),
        ))
        .with_delegation(handoff_config()),
        vocabulary,
    );
    let executor: Arc<dyn AgentA2aHandoffSendExecutor> = if resolving {
        ApplyingHandoffExecutor::over(&fx)
    } else {
        Arc::new(UnresolvedHandoff)
    };
    let _ = fx.dispatcher.clone().with_a2a_handoff_executor(executor);

    let mut per_run = AgentBudgetAllocation::unbounded();
    per_run.set(rakka_agent::AgentBudgetDimension::LoopIterations, Some(3));
    let definition = task_definition()
        .with_budgets(AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(per_run);
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        definition,
        goal_spec_draft(goal_spec_with_handoff(), true),
    ))
    .await
    .expect("the goal task creates");
    fx.instantiate_agent_at(handoff_target_scope()).await;
    fx.pump().await.expect("the loop converges");
    if resolving {
        for _ in 0..4 {
            fx.settle_task_at(&task_scope())
                .await
                .expect("the task settles");
        }
    }
    (fx, sink)
}

#[tokio::test]
async fn a_handoff_awaiting_its_target_records_one_handoff_wait() {
    // The target never resolves: the run rests awaiting it.
    let (fx, sink) = handoff_flow(AgentDecisionEventVocabulary::Version2, false).await;
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(snapshot.status, AgentRunStatus::Running);
    let events = sink.events(&run_scope());
    let wait = one_wait(&events, AWAITING_HANDOFF, AgentRunStatus::Running);
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Wait,
        ],
        "a handoff awaiting its target is not also a fan-in"
    );
    assert_eq!(entry_ordinal(wait, "wait-handoff"), events[1].sequence);
    assert_decision_stream_whole(&sink, &run_scope());

    // The target resolves: the run ends handed off, its one wait before the
    // end.
    let (fx, sink) = handoff_flow(AgentDecisionEventVocabulary::Version2, true).await;
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::HandedOff
    );
    let events = sink.events(&run_scope());
    let wait = one_wait(&events, AWAITING_HANDOFF, AgentRunStatus::Running);
    entry_ordinal(wait, "wait-handoff");
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Wait,
            AgentDecisionKind::Handoff,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());

    // The default vocabulary records the wait, though not the handoff end.
    let (_fx, sink) = handoff_flow(AgentDecisionEventVocabulary::Version1, true).await;
    let events = sink.events(&run_scope());
    one_wait(&events, AWAITING_HANDOFF, AgentRunStatus::Running);
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &run_scope());
}

// ---------------------------------------------------------------------------
// The wind-down: a cancel with children outstanding.
// ---------------------------------------------------------------------------

/// Parks the fan-out with two real children, then cancels the run: the
/// children are still outstanding, so the run rests `Cancelling`.
async fn winding_down_with_children(
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>, Vec<AgentTaskScope>) {
    let (fx, sink) = fan_out_parked(vocabulary).await;
    let children = committed_children(&fx).await;
    assert_eq!(children.len(), 2);
    for (index, (delegation, child_task)) in children.iter().enumerate() {
        create_real_child(
            &fx,
            delegation,
            child_task,
            if index == 0 { SKILL } else { SKILL_2 },
        )
        .await;
    }
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelling
    );
    let child_scopes = children
        .iter()
        .map(|(_, child)| {
            AgentTaskScope::new(TenantId::new(TENANT), child.clone()).expect("the scope is valid")
        })
        .collect();
    (fx, sink, child_scopes)
}

/// The children end; their reports reach the run, and the last one ends it.
async fn end_children(fx: &Fixture, child_scopes: &[AgentTaskScope]) {
    for _round in 0..8 {
        for scope in child_scopes {
            fx.settle_task_at(scope).await.expect("the child settles");
        }
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(&fx.router, fx.now())
            .await
            .expect("the run settles");
    }
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelled
    );
}

#[tokio::test]
async fn a_wind_down_records_one_winding_down_wait() {
    let (fx, sink, child_scopes) =
        winding_down_with_children(AgentDecisionEventVocabulary::Version2).await;
    let events = sink.events(&run_scope());
    let wait = one_wait(&events, WINDING_DOWN, AgentRunStatus::Cancelling);
    assert_eq!(
        wait.operation_id,
        decision_operation_id(wait.turn, "wait-winding-down")
    );
    let children = one_wait(&events, AWAITING_CHILDREN, AgentRunStatus::Running);
    assert_eq!(
        (children.sequence, wait.sequence),
        (3, 4),
        "the fan-in's wait, then the wind-down's: {:?}",
        kinds(&events)
    );
    assert_eq!(events.len(), 4, "{:?}", kinds(&events));
    assert_decision_stream_whole(&sink, &run_scope());

    // The children end, and the run with them: still one wind-down, before
    // the end.
    end_children(&fx, &child_scopes).await;
    let events = sink.events(&run_scope());
    let ended = one_wait(&events, WINDING_DOWN, AgentRunStatus::Cancelling);
    assert_eq!(ended, wait, "the wait is not recorded again");
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Wait,
            AgentDecisionKind::Wait,
            AgentDecisionKind::Cancel,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());
    assert_replay_records_nothing(&fx, &sink, cancel_run("cancel-1")).await;

    // The default vocabulary records the wait, though not the cancel end.
    let (fx, sink, child_scopes) =
        winding_down_with_children(AgentDecisionEventVocabulary::Version1).await;
    end_children(&fx, &child_scopes).await;
    let events = sink.events(&run_scope());
    one_wait(&events, WINDING_DOWN, AgentRunStatus::Cancelling);
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &run_scope());
}

// ---------------------------------------------------------------------------
// Re-resting a wait.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn settle_passes_that_re_rest_the_same_wait_record_nothing() {
    let version2 = AgentDecisionEventVocabulary::Version2;

    // The top-up park, its request re-driven and still unanswered.
    let (fx, sink, withheld) = parked_on_top_up(version2).await;
    one_wait(
        &sink.events(&run_scope()),
        BUDGET_TOP_UP,
        AgentRunStatus::Running,
    );
    assert_re_rest_records_nothing(&fx, &sink, &withheld).await;

    // The fan-in, its children still out.
    let (fx, sink) = fan_out_parked(version2).await;
    one_wait(
        &sink.events(&run_scope()),
        AWAITING_CHILDREN,
        AgentRunStatus::Running,
    );
    assert_re_rest_records_nothing(&fx, &sink, &fx.router).await;

    // The handoff, its target never resolving.
    let (fx, sink) = handoff_flow(version2, false).await;
    one_wait(
        &sink.events(&run_scope()),
        AWAITING_HANDOFF,
        AgentRunStatus::Running,
    );
    assert_re_rest_records_nothing(&fx, &sink, &fx.router).await;

    // The wind-down, its children still out: the run still rests
    // `Cancelling` after the passes.
    let (fx, sink, _children) = winding_down_with_children(version2).await;
    one_wait(
        &sink.events(&run_scope()),
        WINDING_DOWN,
        AgentRunStatus::Cancelling,
    );
    assert_re_rest_records_nothing(&fx, &sink, &fx.router).await;
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelling
    );
    assert_decision_stream_whole(&sink, &run_scope());
}

// ---------------------------------------------------------------------------
// The wind-down through reconciliation: scenario 57's shape.
// ---------------------------------------------------------------------------

const CHARGE: &str = "charge-card";
const SHIP: &str = "ship-order";

/// A model that charges the card and ships the order in one turn, both
/// ungated and non-idempotent.
fn charge_and_ship_fixture(
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let call = |id: &str, tool: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("call id is valid"),
            AgentToolId::new(tool).expect("the tool id is valid"),
            serde_json::json!({ "order": 7 }),
        )
        .expect("the tool call is bounded")
    };
    let dispatcher = ScriptedDispatcher::new()
        .with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Charge it and ship it.")
                .with_tool_call(call("call-1", CHARGE))
                .with_tool_call(call("call-2", SHIP)),
        )
        .with_tool_result(
            CHARGE,
            AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                .expect("the tool result is inline-bounded"),
        )
        .with_tool_result(
            SHIP,
            AgentTaskContent::inline(serde_json::json!({ "shipped": true }))
                .expect("the tool result is inline-bounded"),
        );
    let mut policies = AgentEffectPolicies::new();
    for tool in [CHARGE, SHIP] {
        policies = policies
            .with_tool_spec(
                AgentToolId::new(tool).expect("the tool id is valid"),
                AgentEffectSpec::non_idempotent(),
            )
            .expect("the tool spec is valid");
    }
    sink_wired(
        Fixture::with_sink(
            dispatcher,
            InMemoryAgentRunEffectSink::new(),
            policies,
            Arc::new(AtomicU64::new(1)),
        ),
        vocabulary,
    )
}

/// Answers the model call, so both tool effects reach the dispatch layer,
/// and returns the charge's effect.
async fn dispatch_charge_and_ship(fx: &Fixture) -> AgentRunEffect {
    fx.instantiate_agent().await;
    fx.create_task().await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run settles");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");
    let state = run.state().expect("state reads");
    state
        .loop_state()
        .expect("the loop exists")
        .effects()
        .iter()
        .find(|effect| {
            effect
                .request
                .tool_call()
                .is_some_and(|call| call.tool.as_str() == CHARGE)
        })
        .expect("the charge effect exists")
        .clone()
}

/// Reports the charge's outcome unknown: the worker died mid-invocation.
fn charge_unknown(charge: &AgentRunEffect) -> AgentRunEntityCommand {
    AgentRunEntityCommand::RecordEffectResult {
        operation_id: charge
            .result_operation_id(&run_scope())
            .expect("the operation id derives"),
        effect_id: charge.effect_id.clone(),
        generation: charge.generation,
        attempt: 1,
        fence: 1,
        outcome: Box::new(AgentRunEffectOutcome::indeterminate(
            "mcp-outcome-unknown",
            "the worker died mid-invocation",
        )),
    }
}

/// An operator confirms the charge executed.
fn charge_confirmed(charge: &AgentRunEffect) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveIndeterminateEffect {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            "resolve-57",
        )
        .expect("the decision key derives"),
        effect_id: charge.effect_id.clone(),
        generation: charge.generation,
        resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(AgentRunEffectOutcome::Tool {
                call_id: AgentToolCallId::new("call-1").expect("call id is valid"),
                content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-9" }))
                    .expect("the content is inline-bounded"),
            }),
        }),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

async fn status(fx: &Fixture) -> AgentRunStatus {
    fx.run_snapshot().await.expect("the run exists").status
}

#[tokio::test]
async fn a_wind_down_through_reconciliation_records_one_winding_down() {
    // Scenario 57: the cancel arrives while both effects are in flight, so
    // the run winds down `Cancelling`. The charge's outcome then turns out
    // unknown, and the wind-down rests `WaitingForReconciliation`; the
    // operator confirms it, and the run returns to `Cancelling` — the
    // shipment is still out — until the shipment lands and the run ends.
    let (fx, sink) = charge_and_ship_fixture(AgentDecisionEventVocabulary::Version2);
    let charge = dispatch_charge_and_ship(&fx).await;
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);
    let entered = one_wait(
        &sink.events(&run_scope()),
        WINDING_DOWN,
        AgentRunStatus::Cancelling,
    )
    .clone();

    apply(&fx, charge_unknown(&charge))
        .await
        .expect("the ambiguity records");
    assert_eq!(status(&fx).await, AgentRunStatus::WaitingForReconciliation);
    settle_three_times(&fx, &fx.router).await;
    apply(&fx, charge_confirmed(&charge))
        .await
        .expect("the reconciliation applies");
    assert_eq!(
        status(&fx).await,
        AgentRunStatus::Cancelling,
        "the shipment still holds the wind-down open"
    );
    settle_three_times(&fx, &fx.router).await;
    fx.pump()
        .await
        .expect("the shipment lands and the run ends");
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelled);

    let events = sink.events(&run_scope());
    let wait = one_wait(&events, WINDING_DOWN, AgentRunStatus::Cancelling);
    assert_eq!(wait, &entered, "the wind-down is recorded once, as entered");
    assert_eq!(waits(&events).len(), 1, "{:?}", kinds(&events));
    let tail: Vec<AgentDecisionKind> = events
        .iter()
        .filter(|event| event.sequence > wait.sequence)
        .map(|event| event.kind)
        .collect();
    assert_eq!(
        tail,
        [
            AgentDecisionKind::RequestReconciliation,
            AgentDecisionKind::Reconcile,
            AgentDecisionKind::Cancel,
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());
    assert_replay_records_nothing(&fx, &sink, cancel_run("cancel-1")).await;

    // The wind-down begins while the charge's outcome is already unknown:
    // it is entered `WaitingForReconciliation`, and recorded so.
    let (fx, sink) = charge_and_ship_fixture(AgentDecisionEventVocabulary::Version2);
    let charge = dispatch_charge_and_ship(&fx).await;
    apply(&fx, charge_unknown(&charge))
        .await
        .expect("the ambiguity records");
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");
    assert_eq!(status(&fx).await, AgentRunStatus::WaitingForReconciliation);
    apply(&fx, charge_confirmed(&charge))
        .await
        .expect("the reconciliation applies");
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelling);
    fx.pump()
        .await
        .expect("the shipment lands and the run ends");
    assert_eq!(status(&fx).await, AgentRunStatus::Cancelled);
    let events = sink.events(&run_scope());
    one_wait(
        &events,
        WINDING_DOWN,
        AgentRunStatus::WaitingForReconciliation,
    );
    assert_eq!(waits(&events).len(), 1, "{:?}", kinds(&events));
    assert_decision_stream_whole(&sink, &run_scope());

    // Under the default vocabulary the same wind-down reads as bc0c224.
    let (fx, sink) = charge_and_ship_fixture(AgentDecisionEventVocabulary::Version1);
    let charge = dispatch_charge_and_ship(&fx).await;
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");
    apply(&fx, charge_unknown(&charge))
        .await
        .expect("the ambiguity records");
    apply(&fx, charge_confirmed(&charge))
        .await
        .expect("the reconciliation applies");
    fx.pump()
        .await
        .expect("the shipment lands and the run ends");
    let events = sink.events(&run_scope());
    one_wait(&events, WINDING_DOWN, AgentRunStatus::Cancelling);
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &run_scope());
}

// ---------------------------------------------------------------------------
// A cancel that ends the run in its own transition.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cancel_that_ends_in_its_own_transition_records_the_end_and_no_wait() {
    // The gated call parks behind its approval, so nothing reached the
    // dispatch layer: the cancel fences it and ends the run in the same
    // compare-and-set, which records the closed approval and the end, and
    // no wind-down.
    for vocabulary in [
        AgentDecisionEventVocabulary::Version2,
        AgentDecisionEventVocabulary::Version1,
    ] {
        let policies = AgentEffectPolicies::new()
            .with_tool_spec(
                AgentToolId::new(CHARGE).expect("the tool id is valid"),
                AgentEffectSpec::non_idempotent().with_checkpoint_required(),
            )
            .expect("the gated tool spec is valid");
        let (fx, sink) = sink_wired(
            Fixture::with_sink(
                ScriptedDispatcher::new().with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Let me charge that.")
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("call-1").expect("call id is valid"),
                                AgentToolId::new(CHARGE).expect("the tool id is valid"),
                                serde_json::json!({ "amount": 42 }),
                            )
                            .expect("the tool call is bounded"),
                        ),
                ),
                InMemoryAgentRunEffectSink::new(),
                policies,
                Arc::new(AtomicU64::new(1)),
            ),
            vocabulary,
        );
        fx.instantiate_agent().await;
        fx.create_task().await;
        fx.pump().await.expect("the loop parks on the checkpoint");
        assert_eq!(status(&fx).await, AgentRunStatus::WaitingForApproval);
        let parked = sink.events(&run_scope()).len();
        assert!(parked > 0, "the park recorded its decisions");

        apply(&fx, cancel_run("cancel-1"))
            .await
            .expect("the cancellation applies");
        assert_eq!(status(&fx).await, AgentRunStatus::Cancelled);
        fx.pump().await.expect("the re-pump rests");
        let events = sink.events(&run_scope());
        assert!(waits(&events).is_empty(), "{:?}", kinds(&events));
        let expected: &[AgentDecisionKind] = match vocabulary {
            AgentDecisionEventVocabulary::Version2 => &[
                AgentDecisionKind::ResolveCheckpoint,
                AgentDecisionKind::Cancel,
            ],
            _ => &[],
        };
        assert_eq!(kinds(&events[parked..]), expected);
        if vocabulary == AgentDecisionEventVocabulary::Version1 {
            assert_bc0c224_round_trips(&events);
        }
        assert_decision_stream_whole(&sink, &run_scope());
    }
}
