//! The run's end on the decision stream (issue #119, ask 3).
//!
//! Specification: sections 8.7, 8.9 and 17.7. A run's own decision stream
//! says when the run ends, and how: `complete`, `fail`, `cancel`, `handoff`
//! or `supersede`, carrying the terminal status and the terminal reason's
//! code. The end is recorded once per run by the terminal transition itself,
//! however the wind-down is re-driven, and on whichever write path that
//! transition commits: a command, a delivered exchange, or the settle of an
//! exchange the run initiated.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ExchangeFault, ScriptedDispatcher};
use rakka_agent::{
    AgentApprovalDecision, AgentCheckpointDecision, AgentCheckpointKind, AgentDecisionEvent,
    AgentDecisionEventVocabulary, AgentDecisionKind, AgentDecisionOutcome, AgentDecisionSource,
    AgentEffectPolicies, AgentEffectSpec, AgentEntityClass, AgentExchangeDeliveryError,
    AgentExchangeDeliveryFuture, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter, AgentExchangeTransport,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunEntityCommand,
    AgentRunEntityReply, AgentRunError, AgentRunStatus, AgentTaskContent, AgentTaskDecision,
    AgentTaskEntityCommand, AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, InMemoryAgentRunEffectSink, TenantId, VersionedAgentRecord,
    AGENT_TASK_DECISION_PAYLOAD_TYPE, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

fn tool_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me charge that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id is valid"),
                tool_id(),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn answer_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// Calls the tool on turn one and proposes on turn two.
fn two_turn_dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_turn())
            .with_turn_for(2, answer_turn()),
    )
    .with_tool_result(
        TOOL,
        AgentTaskContent::inline(serde_json::json!({ "charged": true }))
            .expect("the tool result is inline-bounded"),
    )
}

/// A fixture over `policies`, wired with a counting sink under `vocabulary`.
fn fixture_with(
    dispatcher: ScriptedDispatcher,
    policies: AgentEffectPolicies,
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let sink = CountingDecisionSink::new();
    let fx = Fixture::with_sink(
        dispatcher,
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(vocabulary);
    (fx, sink)
}

/// A fixture whose tool call parks behind an approval checkpoint.
fn gated_fixture(vocabulary: AgentDecisionEventVocabulary) -> (Fixture, Arc<CountingDecisionSink>) {
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            tool_id(),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the gated tool spec is valid");
    fixture_with(two_turn_dispatcher(), policies, vocabulary)
}

/// Drives the gated run to its approval wait.
async fn park_on_approval(fx: &Fixture) {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval
    );
}

fn cancel_run(key: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::Cancel {
        operation_id: AgentOperationId::new(AgentOperationKind::Cancellation, [TENANT, AGENT, key])
            .expect("the operation id derives"),
        reason: "operator stopped the work".to_string(),
    }
}

fn deny_checkpoint(
    checkpoint_id: rakka_agent_workflow::HumanCheckpointId,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            "d1",
        )
        .expect("the decision key derives"),
        checkpoint_id,
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator".to_string(),
            display_name: None,
        },
        decision: Box::new(AgentCheckpointDecision::Approval(
            AgentApprovalDecision::Deny {
                reason: "not this card".to_string(),
            },
        )),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
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

/// The approval checkpoint the parked run holds.
async fn approval_checkpoint(fx: &Fixture) -> rakka_agent_workflow::HumanCheckpointId {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let state = run.state().expect("state reads");
    state
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .iter()
        .find(|checkpoint| checkpoint.kind == AgentCheckpointKind::Approval)
        .expect("the run holds the approval")
        .checkpoint_id
        .clone()
}

fn kinds(events: &[AgentDecisionEvent]) -> Vec<AgentDecisionKind> {
    events.iter().map(|event| event.kind).collect()
}

fn of_kind(events: &[AgentDecisionEvent], kind: AgentDecisionKind) -> Vec<&AgentDecisionEvent> {
    events.iter().filter(|event| event.kind == kind).collect()
}

/// The operation id a run's end is recorded under in `turn`: its slot is
/// `end`, which the terminal guard makes once per run.
fn end_operation_id(turn: u64) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::Command,
        [
            TENANT,
            AGENT,
            run_scope().run().as_str(),
            "decision",
            &turn.to_string(),
            "end",
        ],
    )
    .expect("the end's operation id derives")
}

/// Asserts the run ended `status`, that its stream's last event is the one
/// end that status records — with the terminal reason's own code — and that
/// two more settle passes and a re-pump record nothing more.
async fn assert_one_end(
    fx: &Fixture,
    sink: &CountingDecisionSink,
    status: AgentRunStatus,
    kind: AgentDecisionKind,
) -> AgentDecisionEvent {
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, status);
    let reason = snapshot
        .terminal_reason
        .as_ref()
        .expect("a terminal run records why")
        .code();

    let events = sink.events(&run_scope());
    let ends = of_kind(&events, kind);
    assert_eq!(ends.len(), 1, "one {kind} end: {events:?}");
    let end = events
        .last()
        .expect("the run recorded its decisions")
        .clone();
    assert_eq!(end.kind, kind, "the end is the last decision: {events:?}");
    assert_eq!(end.run_status, Some(status));
    assert_eq!(end.reason_code.as_deref(), Some(reason));
    assert_eq!(end.source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(end.operation_id, end_operation_id(end.turn));
    assert!(end.operation_id.as_str().ends_with("end"));
    assert_eq!(end.checkpoint, None);
    assert_eq!(end.effect, None);
    assert_eq!(end.outcome, None);

    for _ in 0..2 {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(&fx.router, fx.now())
            .await
            .expect("the run settles");
    }
    fx.pump().await.expect("the re-pump rests");
    assert_eq!(
        sink.events(&run_scope()),
        events,
        "two settle passes and a re-pump record nothing more"
    );
    assert_decision_stream_whole(sink, &run_scope());
    end
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

/// A task route that answers the run's result proposal the way a task that
/// another run already completed does, and leaves every other exchange
/// outstanding.
struct CompletedTask {
    at: AgentTimestampMillis,
}

impl AgentExchangeTransport for CompletedTask {
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        Box::pin(async move {
            if envelope.kind() != AgentExchangeKind::ResultProposal {
                return Err(AgentExchangeDeliveryError::new(
                    "test-unrouted",
                    "this route answers only the result proposal",
                ));
            }
            let refusal = AgentTaskDecision::Refused {
                code: "task-terminal".to_string(),
                status: AgentTaskStatus::Completed,
            };
            let payload = AgentExchangePayload::encode(AGENT_TASK_DECISION_PAYLOAD_TYPE, &refusal)
                .expect("the refusal encodes");
            Ok(AgentExchangeReply::applied(
                envelope,
                AgentExchangeResult::rejected(
                    "task-terminal",
                    "the task is already completed",
                    payload,
                ),
                self.at,
            ))
        })
    }
}

/// Drives a run that proposes on its first turn to a task another run
/// already completed: the proposal's refusal settles on the run, which ends
/// `Superseded` in that settle.
async fn supersede(fx: &Fixture) -> AgentExchangeRouter {
    let refusing = AgentExchangeRouter::new().with_route(
        AgentEntityClass::Task,
        Arc::new(CompletedTask {
            at: AgentTimestampMillis::new(1),
        }),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&refusing, fx.now())
        .await
        .expect("the run starts its model call");
    fx.dispatcher
        .drive(&mut run, &refusing, fx.now())
        .await
        .expect("the model call is answered, and the proposal refused");
    refusing
}

fn proposing_dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::new().with_turn(answer_turn())
}

#[tokio::test]
async fn each_terminal_status_records_its_end() {
    let version2 = AgentDecisionEventVocabulary::Version2;

    // Completed: the two-turn flow ends on the task's acceptance.
    let (fx, sink) = fixture_with(two_turn_dispatcher(), AgentEffectPolicies::new(), version2);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");
    let end = assert_one_end(
        &fx,
        &sink,
        AgentRunStatus::Completed,
        AgentDecisionKind::Complete,
    )
    .await;
    assert_eq!(end.turn, 2, "the end keeps the turn it ended in");
    assert_eq!(
        kinds(&sink.events(&run_scope())),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Continue,
            AgentDecisionKind::SubmitResult,
            AgentDecisionKind::Complete,
        ]
    );

    // Failed: a person's denial fails the gated effect, and the run with it.
    // The end is still the run's deterministic policy, not the person.
    let (fx, sink) = gated_fixture(version2);
    park_on_approval(&fx).await;
    let checkpoint_id = approval_checkpoint(&fx).await;
    let denial = deny_checkpoint(checkpoint_id);
    apply(&fx, denial.clone())
        .await
        .expect("the denial applies");
    fx.pump().await.expect("the denied run winds down");
    assert_one_end(&fx, &sink, AgentRunStatus::Failed, AgentDecisionKind::Fail).await;
    assert_replay_records_nothing(&fx, &sink, denial).await;

    // Cancelled: a cancel command on the parked run.
    let (fx, sink) = gated_fixture(version2);
    park_on_approval(&fx).await;
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");
    assert_one_end(
        &fx,
        &sink,
        AgentRunStatus::Cancelled,
        AgentDecisionKind::Cancel,
    )
    .await;
    assert_replay_records_nothing(&fx, &sink, cancel_run("cancel-1")).await;

    // HandedOff: the transfer of tests/handoff_record.rs, ended on the source.
    let (fx, sink) = handed_off(version2).await;
    assert_one_end(
        &fx,
        &sink,
        AgentRunStatus::HandedOff,
        AgentDecisionKind::Handoff,
    )
    .await;

    // Superseded: the task another run already completed refuses the
    // proposal. Only the refusing route answers it; the fixture's own task
    // would accept it.
    let (fx, sink) = fixture_with(proposing_dispatcher(), AgentEffectPolicies::new(), version2);
    let refusing = supersede(&fx).await;
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, AgentRunStatus::Superseded);
    let events = sink.events(&run_scope());
    let end = events.last().expect("the run recorded its decisions");
    assert_eq!(end.kind, AgentDecisionKind::Supersede, "{events:?}");
    assert_eq!(end.run_status, Some(AgentRunStatus::Superseded));
    assert_eq!(end.reason_code.as_deref(), Some("task-refused-proposal"));
    assert_eq!(end.source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(end.operation_id, end_operation_id(end.turn));
    for _ in 0..2 {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(&refusing, fx.now())
            .await
            .expect("the run settles");
    }
    assert_eq!(
        sink.events(&run_scope()),
        events,
        "settle passes record nothing more"
    );
    assert_eq!(of_kind(&events, AgentDecisionKind::Supersede).len(), 1);
    assert_decision_stream_whole(&sink, &run_scope());
}

/// The handoff transfer of `tests/handoff_record.rs`, with the source run
/// wired to a counting sink.
async fn handed_off(
    vocabulary: AgentDecisionEventVocabulary,
) -> (Fixture, Arc<CountingDecisionSink>) {
    let sink = CountingDecisionSink::new();
    let handoff_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
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
        );
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn)
            .with_turn(answer_turn()),
    ))
    .with_delegation(handoff_config())
    .with_decision_events(sink.clone())
    .with_decision_event_vocabulary(vocabulary);
    let executor = ApplyingHandoffExecutor::over(&fx);
    let _ = fx
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());

    let mut per_run = rakka_agent::AgentBudgetAllocation::unbounded();
    per_run.set(rakka_agent::AgentBudgetDimension::LoopIterations, Some(3));
    let definition = task_definition()
        .with_budgets(rakka_agent::AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..rakka_agent::AgentBudgetCeilings::unbounded()
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
    for _ in 0..4 {
        fx.settle_task_at(&task_scope())
            .await
            .expect("the task settles");
    }
    (fx, sink)
}

/// A wind-down that waits on its children is re-driven by every settle pass,
/// and records its end once, in the transition that ends it.
#[tokio::test]
async fn a_wind_down_re_driven_over_several_passes_records_one_end() {
    let sink = CountingDecisionSink::new();
    let executor = SkillNamedExecutor::new();
    let fx = fan_out_fixture(executor)
        .with_decision_events(sink.clone())
        .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);
    create_fan_out_task(&fx, None).await;
    fx.pump().await.expect("the fan-out parks");
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
    let child_scopes: Vec<AgentTaskScope> = children
        .iter()
        .map(|(_, child)| {
            AgentTaskScope::new(TenantId::new(TENANT), child.clone()).expect("the scope is valid")
        })
        .collect();

    // The cancel begins the wind-down; the children are still outstanding,
    // so the run rests `Cancelling` across three settle passes.
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");
    for _ in 0..3 {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(&fx.router, fx.now())
            .await
            .expect("the run settles");
    }
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelling
    );
    assert!(
        of_kind(&sink.events(&run_scope()), AgentDecisionKind::Cancel).is_empty(),
        "no end while the wind-down waits"
    );

    // The children end; their reports reach the run, and the last one ends it.
    for _round in 0..8 {
        for scope in &child_scopes {
            fx.settle_task_at(scope).await.expect("the child settles");
        }
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        run.settle_side_effects(&fx.router, fx.now())
            .await
            .expect("the run settles");
    }
    assert_one_end(
        &fx,
        &sink,
        AgentRunStatus::Cancelled,
        AgentDecisionKind::Cancel,
    )
    .await;
    assert_replay_records_nothing(&fx, &sink, cancel_run("cancel-1")).await;
}

/// The end is recorded wherever the terminal transition commits: on the
/// `accept` path, when a delivered run-cancel ends the run, and on the settle
/// path, when the refusal of the run's own proposal settles.
#[tokio::test]
async fn an_end_reached_through_the_accept_and_settle_paths_reaches_the_sink() {
    // Accept: the task's cancellation reaches the parked run as a run-cancel
    // exchange. Nothing drives the run but that delivery. The first reply is
    // lost after the run accepted, so the task re-delivers the same exchange,
    // and that re-delivery arrives twice.
    let (fx, sink) = gated_fixture(AgentDecisionEventVocabulary::Version2);
    park_on_approval(&fx).await;
    let before = sink.events(&run_scope()).len();
    let accepted_before = fx.run_transport.acceptances();
    fx.run_transport.inject(ExchangeFault::LoseReply);
    fx.run_transport.inject(ExchangeFault::DeliverTwice);
    fx.apply_task_command(AgentTaskEntityCommand::Cancel {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Cancellation,
            [TENANT, task_scope().task().as_str(), "root-cancel"],
        )
        .expect("the operation id derives"),
        reason: "operator".to_string(),
    })
    .await
    .expect("the task cancel applies");
    for _ in 0..3 {
        fx.settle_task_at(&task_scope())
            .await
            .expect("the task settles");
    }
    assert_eq!(
        fx.run_transport.acceptances() - accepted_before,
        3,
        "the run-cancel was accepted, then re-delivered twice"
    );
    let events = sink.events(&run_scope());
    assert_eq!(
        kinds(&events[before..]),
        [
            AgentDecisionKind::ResolveCheckpoint,
            AgentDecisionKind::Cancel
        ],
        "the delivery itself recorded and flushed the end, once"
    );
    assert_decision_stream_whole(&sink, &run_scope());
    assert_one_end(
        &fx,
        &sink,
        AgentRunStatus::Cancelled,
        AgentDecisionKind::Cancel,
    )
    .await;

    // Settle: the refusal of the run's own proposal ends it.
    let (fx, sink) = fixture_with(
        proposing_dispatcher(),
        AgentEffectPolicies::new(),
        AgentDecisionEventVocabulary::Version2,
    );
    supersede(&fx).await;
    let events = sink.events(&run_scope());
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::SubmitResult,
            AgentDecisionKind::Supersede
        ]
    );
    assert_decision_stream_whole(&sink, &run_scope());
}

/// A cancel that closes an open approval records the close first, then the
/// end, in that order and in one transition.
#[tokio::test]
async fn a_cancel_with_an_open_approval_records_its_closure_before_the_end() {
    let (fx, sink) = gated_fixture(AgentDecisionEventVocabulary::Version2);
    park_on_approval(&fx).await;
    let checkpoint_id = approval_checkpoint(&fx).await;
    apply(&fx, cancel_run("cancel-1"))
        .await
        .expect("the cancellation applies");

    let events = sink.events(&run_scope());
    let open = events
        .iter()
        .position(|event| event.kind == AgentDecisionKind::RequestApproval)
        .expect("the approval opened");
    let after = &events[open + 1..];
    assert_eq!(
        kinds(after),
        [
            AgentDecisionKind::ResolveCheckpoint,
            AgentDecisionKind::Cancel
        ]
    );
    assert_eq!(after[0].outcome, Some(AgentDecisionOutcome::Cancelled));
    assert_eq!(
        after[0]
            .checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.checkpoint_id),
        Some(&checkpoint_id)
    );
    assert_eq!(after[1].sequence, after[0].sequence + 1);
    assert_eq!(after[1].occurred_at, after[0].occurred_at, "one transition");
    assert_one_end(
        &fx,
        &sink,
        AgentRunStatus::Cancelled,
        AgentDecisionKind::Cancel,
    )
    .await;
    assert_replay_records_nothing(&fx, &sink, cancel_run("cancel-1")).await;
}

/// Review Focus 3: a run driven by entity instances with different
/// vocabularies, as during the roll that flips the switch. Each instance
/// writes only the kinds its vocabulary writes, stamps its own schema
/// version, and the run's sequence stays contiguous either way.
#[tokio::test]
async fn a_vocabulary_switch_mid_run_keeps_one_contiguous_sequence() {
    // Version1 parks; a Version2 instance applies the cancel.
    let (fx, sink) = gated_fixture(AgentDecisionEventVocabulary::Version1);
    park_on_approval(&fx).await;
    let mut run = fx
        .run()
        .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(cancel_run("cancel-1"), &fx.router, fx.now())
        .await
        .expect("the cancellation applies");
    fx.pump().await.expect("the re-pump rests");

    let events = sink.events(&run_scope());
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::RequestApproval,
            AgentDecisionKind::ResolveCheckpoint,
            AgentDecisionKind::Cancel,
        ]
    );
    let versions: Vec<u32> = events
        .iter()
        .map(|event| VersionedAgentRecord::schema_version(event).get())
        .collect();
    assert_eq!(versions, [1, 1, 1, 2, 2], "each instance stamps its own");
    assert_bc0c224_decodes(&events[..3]);
    assert_decision_stream_whole(&sink, &run_scope());

    // Version2 parks; a Version1 instance applies the cancel and writes
    // neither the close nor the end, and leaves no hole for them.
    let (fx, sink) = gated_fixture(AgentDecisionEventVocabulary::Version2);
    park_on_approval(&fx).await;
    let mut run = fx
        .run()
        .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version1);
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(cancel_run("cancel-1"), &fx.router, fx.now())
        .await
        .expect("the cancellation applies");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelled
    );
    fx.pump().await.expect("the re-pump rests");
    let events = sink.events(&run_scope());
    assert_eq!(
        kinds(&events),
        [
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::RequestApproval,
        ]
    );
    let state = rakka_agent::load_agent_run_state(
        &fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    let loop_state = state.loop_state().expect("the loop exists");
    assert_eq!(
        loop_state.decision_sequence(),
        3,
        "no sequence was consumed"
    );
    assert_eq!(loop_state.decision_drops(), 0);
    assert_decision_stream_whole(&sink, &run_scope());
}
