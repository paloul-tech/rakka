//! The authoritative operational snapshot and the session view.
//!
//! Specification: section 17.18; scenarios 21 and 56 of section 18. The
//! snapshot is derived from the durable run record alone and returns the
//! durable state revision it read, so it stays correct — lifecycle, waits,
//! budget, effects, cancellation — when telemetry is sampled, delayed,
//! dropped, or entirely unavailable, and when the entity is passivated
//! (scenario 56). The session view is a projection over it: it joins the
//! decision events a sink retained and the trace segments the durable records
//! carry, and it is explicit about its own freshness rather than degrading
//! the authoritative half (scenario 21).

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    agent_operational_snapshot, assemble_agent_session_view, AgentCancellationProgress,
    AgentCheckpointKind, AgentDecisionEventSink, AgentDecisionKind, AgentEffectPolicies,
    AgentEffectSpec, AgentModelTurn, AgentModelUsage, AgentObservabilityError,
    AgentObservabilityFuture, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunStatus,
    AgentSchemaPolicy, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentDecisionEventSink, InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::*;

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 3,
        })
}

/// A fixture whose tool is checkpoint-required, so the run parks on an
/// approval before the tool may dispatch.
fn checkpointed_fixture() -> Fixture {
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new("charge-card").expect("tool id"),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("charge-card"))
            .with_turn(proposing_turn("charged")),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

/// A sink that always refuses: what an unavailable telemetry backend looks
/// like to the session view.
#[derive(Debug)]
struct UnavailableSink;

impl AgentDecisionEventSink for UnavailableSink {
    fn backend_name(&self) -> &'static str {
        "unavailable"
    }

    fn append<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _event: &'a rakka_agent::AgentDecisionEvent,
    ) -> AgentObservabilityFuture<'a, rakka_agent::AgentDecisionWriteStatus> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }

    fn read<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _after: u64,
        _limit: usize,
    ) -> AgentObservabilityFuture<'a, rakka_agent::AgentDecisionEventPage> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }
}

/// With no telemetry wired at all, the snapshot answers everything scenario 56
/// asks — lifecycle, waits, budget, effects, cancellation — from the durable
/// record, with the revision it read.
#[tokio::test]
async fn the_snapshot_answers_from_durable_state_with_telemetry_unavailable() {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");

    assert!(
        snapshot.revision.get() > 0,
        "an authoritative point read returns its durable revision"
    );
    assert_eq!(snapshot.observed_at, AgentTimestampMillis::new(9_999));
    let run = snapshot.run.as_ref().expect("the run accepted");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2);
    assert!(
        run.budget.tokens() > 0,
        "the budget consumption is the durable ledger's"
    );
    assert_eq!(snapshot.wait_reason, None);
    assert_eq!(snapshot.next_wake, None);
    assert!(
        snapshot.pending_effects.is_empty(),
        "a completed run holds no unresolved effect"
    );
    assert!(snapshot.open_checkpoints.is_empty());
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::NotRequested
    );
    assert_eq!(
        snapshot.decision_cursor, 0,
        "an unwired run sequenced no decision events, and the snapshot says so"
    );

    // Deriving again from the same revision yields the same answer: the
    // snapshot is pure over the durable record.
    let again = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(again, snapshot);

    // The additive-field compatibility posture the struct's own docs
    // promise: a snapshot serialized before `has_accepted_result` existed
    // still deserializes, loading the field unset — the same rule every
    // other additive field in the family carries.
    let mut before_field = serde_json::to_value(&snapshot).expect("the snapshot serializes");
    before_field
        .as_object_mut()
        .expect("the snapshot serializes as an object")
        .remove("has_accepted_result")
        .expect("the field is present on a current snapshot");
    let decoded: rakka_agent::AgentOperationalSnapshot =
        serde_json::from_value(before_field).expect("a pre-field snapshot still deserializes");
    assert!(
        !decoded.has_accepted_result,
        "a pre-field snapshot loads with the fact unset"
    );

    // The rule reaches the *nested* run projection too: `AgentRunSnapshot` is
    // embedded here, so an operational answer serialized by an older peer is
    // what a reader deserializes. `terminal_at` gets there by being an
    // `Option`, which serde already loads as `None` when the field is absent
    // — this pins that, so narrowing the field later fails here rather than
    // in a rolling update.
    let mut before_terminal_at = serde_json::to_value(&snapshot).expect("the snapshot serializes");
    before_terminal_at
        .get_mut("run")
        .expect("the operational answer carries a run projection")
        .as_object_mut()
        .expect("the run projection serializes as an object")
        .remove("terminal_at")
        .expect("the field is present on a current run projection");
    let decoded: rakka_agent::AgentOperationalSnapshot = serde_json::from_value(before_terminal_at)
        .expect("a pre-field operational snapshot still deserializes");
    assert!(
        decoded
            .run
            .expect("the run projection loads")
            .terminal_at
            .is_none(),
        "a pre-field run projection loads with the stamp unset"
    );
}

/// A run parked behind an approval checkpoint reports the wait, the gated
/// effect with its safety class and grant state, and the checkpoint's bounded
/// resolver requirements.
#[tokio::test]
async fn a_parked_run_reports_its_wait_effects_and_checkpoint() {
    let fx = checkpointed_fixture();
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the run parks on its checkpoint");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");

    let run = snapshot.run.as_ref().expect("the run accepted");
    assert_eq!(run.status, AgentRunStatus::WaitingForApproval);
    assert_eq!(
        snapshot.wait_reason.as_deref(),
        Some("waiting-for-approval"),
        "the wait reason is the bounded status label"
    );

    let gated = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.checkpoint_required)
        .expect("the gated tool effect is pending");
    assert_eq!(gated.status, AgentRunEffectStatus::Pending);
    assert_eq!(
        gated.attempts, 0,
        "a gated effect has never been dispatched"
    );
    assert!(!gated.granted, "no grant exists before the decision");

    assert_eq!(snapshot.open_checkpoints.len(), 1);
    assert_eq!(
        snapshot.open_checkpoints[0].kind,
        AgentCheckpointKind::Approval
    );
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::NotRequested
    );
}

/// Cancellation progress follows section 8.7: an ambiguous consequential
/// effect holds `WaitingForReconciliation` — terminal cancellation is never
/// inferred from acceptance of the request — and only the durably recorded
/// terminal state reports `Completed`.
#[tokio::test]
async fn cancellation_progress_follows_the_durable_record() {
    // Stage scenario 57's shape: a non-idempotent tool attempt reported
    // ambiguous, then a cancellation request.
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, tool_calling_turn("charge-card")),
    );
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;

    let now = fx.now();
    let mut run = fx.run();
    run.recover(now).await.expect("the run recovers");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("the run settles");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");

    let (effect_id, generation, operation_id) = {
        let state = run.state().expect("state reads");
        let loop_state = state.loop_state().expect("the loop exists");
        let effect = loop_state
            .effects()
            .iter()
            .find(|effect| effect.request.tool_call().is_some())
            .expect("the tool effect exists")
            .clone();
        let operation_id = effect
            .result_operation_id(&run_scope())
            .expect("the operation id derives");
        (effect.effect_id, effect.generation, operation_id)
    };
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id,
            effect_id,
            generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(rakka_agent::AgentRunEffectOutcome::Indeterminate {
                code: "connection-lost".to_string(),
                message: "the worker died mid-invocation".to_string(),
            }),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: cancel_operation_id("cancel-1"),
            reason: "operator-requested".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the cancellation request is accepted");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::WaitingForReconciliation,
        "acceptance of the request is not terminal cancellation"
    );
    let ambiguous = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.status == AgentRunEffectStatus::Indeterminate)
        .expect("the ambiguous effect is reported as indeterminate work");
    assert_eq!(ambiguous.attempts, 1);
}

/// A cancelling run that has proposed a result but not yet had it accepted is
/// still `Requested`, not `Quiesced`: the proposal exchange it owes its task is
/// work the run started that is still resolving (the task may yet accept or
/// reject it), so cancellation progress must read the run's own settlement gate
/// — a pending proposal, not just its effect set (section 8.7).
#[tokio::test]
async fn a_cancelling_run_awaiting_its_proposal_is_still_requested() {
    // The model proposes on its first turn, so the run reaches a pending
    // proposal with no effect left in flight.
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Drive the run to the point where it owes its proposal. The proposal is
    // built through the loop and then delivered to the task as an exchange, and
    // in this harness that round-trip is synchronous — so the run is driven
    // under a router with no task route, modelling the real distributed window
    // between the run proposing and the task accepting: the assignment already
    // reached the run when the task was created, delivery of the proposal is
    // left owed, and the proposal stands (`exchange-no-route` is a delivery
    // failure, not a transition failure, so the run does not complete).
    let undelivered = rakka_agent::AgentExchangeRouter::new();
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.settle_side_effects(&undelivered, fx.now())
        .await
        .expect("the model effect dispatches");
    fx.dispatcher
        .drive(&mut run, &undelivered, fx.now())
        .await
        .expect("the model call is answered and the loop settles to its proposal");

    // Precondition: a pending, undelivered proposal on a still-active run, no
    // blocking effect, and no cancellation yet.
    let before = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert!(
        before.has_pending_proposal,
        "the run must owe its task a result proposal before it is cancelled"
    );
    assert_eq!(
        before.run.as_ref().map(|run| run.status),
        Some(AgentRunStatus::Running),
        "the run is still active — the proposal was never accepted"
    );
    assert!(before.pending_effects.is_empty(), "no effect is in flight");
    assert_eq!(before.cancellation, AgentCancellationProgress::NotRequested);

    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: cancel_operation_id("cancel-proposal"),
            reason: "operator-requested".to_string(),
        },
        &undelivered,
        fx.now(),
    )
    .await
    .expect("the cancellation request is accepted");

    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert!(
        snapshot.has_pending_proposal,
        "the proposal still stands after the cancellation request"
    );
    assert_eq!(
        snapshot.cancellation,
        AgentCancellationProgress::Requested,
        "an outstanding proposal is work still resolving, never a quiesced run"
    );
}

/// The session view joins the snapshot with the sink's decision events and is
/// explicit about freshness; a failing sink degrades only the projection half,
/// never the authoritative one.
#[tokio::test]
async fn the_session_view_joins_decisions_and_reports_its_own_lag() {
    let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher).with_decision_events(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let view = assemble_agent_session_view(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        Some(sink.as_ref()),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");

    assert!(view.decisions_available);
    assert_eq!(
        view.decision_lag, 0,
        "every sequenced decision is projected"
    );
    let kinds: Vec<AgentDecisionKind> = view.decisions.iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        vec![
            AgentDecisionKind::Continue,
            AgentDecisionKind::CallTools,
            AgentDecisionKind::Continue,
            AgentDecisionKind::SubmitResult,
        ],
        "the session's decisions are reconstructable in order (scenario 21)"
    );
    assert_eq!(
        view.snapshot.decision_cursor, 4,
        "the durable cursor backs the lag computation"
    );

    // The same view over an unavailable sink: the authoritative half is
    // untouched, and the projection half says exactly what it is missing.
    let degraded = assemble_agent_session_view(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        Some(&UnavailableSink),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");
    assert!(!degraded.decisions_available);
    assert!(degraded.decisions.is_empty());
    assert_eq!(
        degraded.decision_lag, 4,
        "all four decisions are unprojected"
    );
    assert_eq!(degraded.snapshot, view.snapshot);

    // A hole in the retained stream — the ring dropped an unflushed event —
    // is not an outage: the view shows every decision the sink still retains,
    // resuming past the declared gap, and only the missing one is absent.
    // Blanking the whole view would turn one dropped record into "the sink is
    // down" on every read for the rest of the run's life.
    let holed = Arc::new(InMemoryAgentDecisionEventSink::new());
    for event in sink
        .events(&run_scope())
        .iter()
        .filter(|event| event.sequence != 3)
    {
        holed
            .append(&run_scope(), event)
            .await
            .expect("the holed sink accepts the append");
    }
    let gapped = assemble_agent_session_view(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        Some(holed.as_ref()),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");
    assert!(
        gapped.decisions_available,
        "a retention hole is a declared loss, not a sink outage"
    );
    assert_eq!(
        gapped
            .decisions
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 4],
        "every retained decision is shown; only the hole is absent"
    );
    assert_eq!(gapped.snapshot, view.snapshot);
}

fn cancel_operation_id(label: &str) -> rakka_agent::AgentOperationId {
    rakka_agent::AgentOperationId::new(rakka_agent::AgentOperationKind::Command, ["acme", label])
        .expect("the operation id derives")
}

/// Scenarios 21 and 56 under the owner-kill sweep: kill the run's owner at
/// every durable write of the two-turn flow, on both sides of the
/// compare-and-set. However the owner died, the converged authoritative
/// snapshot answers from the durable record alone — nothing wired — and
/// reports exactly the facts the crash-free reference reported: same status,
/// same turn, same settled budget, no residual waits, effects, or
/// checkpoints. The revision is the one durable fact allowed to differ, since
/// recovery legitimately re-drives writes.
#[tokio::test]
async fn the_snapshot_reports_the_reference_facts_after_any_owner_loss() {
    let build = || {
        let dispatcher = ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn_for(1, tool_calling_turn("lookup"))
                .with_turn_for(2, proposing_turn("resolved")),
        )
        .with_tool_result(
            "lookup",
            AgentTaskContent::inline(serde_json::json!({ "found": true }))
                .expect("the tool result is inline-bounded"),
        );
        Fixture::new(dispatcher)
    };

    let reference = build();
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(
        writes >= 6,
        "the two-turn flow should make several durable writes, saw {writes}"
    );
    let expected = agent_operational_snapshot(
        &reference.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    let expected_run = expected.run.as_ref().expect("the run accepted");
    let expected_facts = (
        expected_run.status,
        expected_run.turn,
        expected_run.budget.model_calls(),
        expected_run.budget.tokens(),
        expected_run.budget.loop_iterations(),
        expected.decision_cursor,
    );

    rakka_agent::testkit::sweep_crash_points(writes, |nth, point| async move {
        let fx = build();
        fx.instantiate_agent().await;

        fx.runs.crash_at(nth, point);
        fx.create_task().await;
        let _crashed = fx.pump().await;

        // A new owner activates and finds only what was durably committed.
        fx.runs.assert_crash_fired(nth, point);
        fx.runs.survive();
        fx.pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let snapshot = agent_operational_snapshot(
            &fx.runs,
            &run_scope(),
            &AgentSchemaPolicy::default(),
            AgentTimestampMillis::new(9_999),
        )
        .await
        .expect("the point query answers")
        .expect("the run exists");
        let run = snapshot.run.as_ref().expect("the run accepted");
        assert_eq!(
            (
                run.status,
                run.turn,
                run.budget.model_calls(),
                run.budget.tokens(),
                run.budget.loop_iterations(),
                snapshot.decision_cursor,
            ),
            expected_facts,
            "crash {point:?} at write {nth} changed an authoritative fact"
        );
        assert_eq!(
            snapshot.wait_reason, None,
            "crash {point:?} at write {nth} left a stale wait reason"
        );
        assert_eq!(
            snapshot.next_wake, None,
            "crash {point:?} at write {nth} left a stale wake"
        );
        assert!(
            snapshot.pending_effects.is_empty(),
            "crash {point:?} at write {nth} left an unresolved effect behind"
        );
        assert!(
            snapshot.open_checkpoints.is_empty(),
            "crash {point:?} at write {nth} left a checkpoint open"
        );
        assert_eq!(
            snapshot.cancellation,
            AgentCancellationProgress::NotRequested,
            "crash {point:?} at write {nth} surfaced a cancellation"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// Task-scoped operational query (M3)

/// A legitimate epoch-result envelope for one admitted wake.
fn epoch_result_envelope(
    binding: &rakka_agent::AgentWakeBinding,
    status: rakka_agent::AgentTaskStatus,
) -> rakka_agent::AgentExchangeEnvelope {
    let epoch_task =
        rakka_agent::epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        rakka_agent::AgentTaskScope::new(tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        rakka_agent::epoch_result_operation_id(&tenant(), &goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let result = rakka_agent::AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status,
        consumed: rakka_agent::AgentBudgetConsumption::zero(),
        result_digest: None,
    };
    rakka_agent::AgentExchangeEnvelope::new(
        operation_id.clone(),
        rakka_agent::AgentExchangeKind::EpochResult,
        rakka_agent::AgentEntityAddress::Task(epoch_scope),
        rakka_agent::AgentEntityAddress::Task(task_scope()),
        rakka_agent::AgentExchangePayload::encode(
            rakka_agent::AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
            &result,
        )
        .expect("the payload encodes"),
        rakka_agent_workflow::AgentCorrelationId::new(operation_id.as_str()),
        AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds")
}

/// The M3 operational facts, answered from the durable task record alone —
/// no entity is resident when the query runs, which is exactly the
/// passivated case — and the "next wake" joined purely from the wake-timer
/// store's state.
#[tokio::test]
async fn the_task_snapshot_answers_the_continuous_checklist_while_passivated() {
    use rakka_agent::ScheduleRevision;

    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    // One occurrence admits and attaches its epoch; a second coalesces
    // behind it.
    let first = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(
        rakka_agent::wake_admission_command(first.clone()).expect("the command derives"),
    )
    .await
    .expect("the first admission applies");
    let second = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(
        rakka_agent::wake_admission_command(second.clone()).expect("the command derives"),
    )
    .await
    .expect("the second delivery coalesces");

    // Every entity store built by the fixture was dropped after its call:
    // the answer below is derived from the durable record alone.
    let snapshot = rakka_agent::agent_task_operational_snapshot(
        &fx.tasks,
        &task_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the root exists");
    assert_eq!(snapshot.observed_at, AgentTimestampMillis::new(9_999));
    assert!(!snapshot.has_accepted_result);
    let task = snapshot.task.as_ref().expect("the root is created");
    let wake = task.wake.as_ref().expect("the goal has a wake view");
    assert_eq!(wake.schedule_revision, ScheduleRevision::INITIAL);
    assert_eq!(wake.active, vec![first.wake_id().clone()]);
    assert_eq!(wake.pending, vec![second.wake_id().clone()]);
    assert_eq!(wake.counters.admitted, 1);
    assert_eq!(wake.counters.coalesced, 1);
    let (epoch_task, epoch_run) = epoch_scopes_for(first.wake_id());
    assert_eq!(
        wake.epochs,
        vec![rakka_agent::AgentEpochRef {
            task: epoch_task.task().clone(),
            run: epoch_run.run().clone(),
        }],
        "the active occurrence's epoch is the view's epoch"
    );
    let lifecycle = wake.lifecycle.as_ref().expect("the lifecycle view rides");
    assert_eq!(
        lifecycle.status(),
        rakka_agent::AgentGoalLifecycleStatus::Active
    );
    assert_eq!(lifecycle.consecutive_failures(), 0);

    // The epoch fails: the streak, the backoff, and the parked backoff
    // re-wake all surface in the same one-read answer.
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    let reply = root
        .accept(
            &epoch_result_envelope(&first, rakka_agent::AgentTaskStatus::Failed),
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the result is answered");
    assert!(reply.result().is_accepted());
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    drop(root);

    let snapshot = rakka_agent::agent_task_operational_snapshot(
        &fx.tasks,
        &task_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(10_000),
    )
    .await
    .expect("the point query answers")
    .expect("the root exists");
    assert_eq!(snapshot.owed_history, 0, "the settle pass flushed history");
    let task = snapshot.task.as_ref().expect("the root is created");
    let wake = task.wake.as_ref().expect("the goal has a wake view");
    assert!(wake.active.is_empty(), "the failed wake released");
    assert_eq!(wake.pending, vec![second.wake_id().clone()]);
    assert!(wake.epochs.is_empty());
    let lifecycle = wake.lifecycle.as_ref().expect("the lifecycle view rides");
    assert_eq!(lifecycle.consecutive_failures(), 1);
    let until = lifecycle.backoff_until().expect("the backoff is in force");
    let slot = lifecycle
        .rewakes()
        .backoff
        .expect("the backoff re-wake is owed");
    assert!(slot.parked, "the settle pass parked it durably");
    assert_eq!(slot.due_at, until);

    // "Next wake" joins from the wake-timer store: the parked backoff
    // re-wake is the earliest pending entry for this task.
    let mut scanner = fx.wake_scanner();
    let timer_state = scanner
        .timers_mut()
        .recover(AgentTimestampMillis::new(0))
        .await
        .expect("the timer store recovers")
        .clone();
    let (next_wake, next_due) =
        rakka_agent::next_pending_wake_for_task(&timer_state, &tenant(), task_scope().task())
            .expect("the parked re-wake is pending");
    assert_eq!(next_due, until);
    assert!(
        next_wake.as_str().starts_with("wake-"),
        "the joined id is a derived wake identity"
    );

    // A scope that was never created answers `None`, not an error.
    let absent = rakka_agent::AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new("task-operational-query-absent").expect("the id is valid"),
    )
    .expect("the scope is valid");
    assert!(rakka_agent::agent_task_operational_snapshot(
        &fx.tasks,
        &absent,
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(10_001),
    )
    .await
    .expect("the point query answers")
    .is_none());
}

/// A blocked task whose dependency registration never settled is the documented
/// stuck-dependency struggle signal
/// ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)) — but only
/// once it has stayed that way. A registration is normally outstanding for the
/// length of one settle pass, so a threshold-free derivation would report every
/// freshly blocked task as stuck the instant it committed.
#[tokio::test]
async fn a_stuck_dependency_reports_only_after_the_edge_has_actually_stalled() {
    use rakka_agent::{
        agent_task_operational_snapshot, agent_task_struggle_signals, AgentStrugglePolicy,
        AgentStruggleSignalKind, AgentTaskDependencyDeclaration, AgentTaskEntityCommand,
        AgentTaskStatus,
    };

    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_agent().await;

    // Created *with* an upstream that does not exist, so the task is born
    // blocked and its registration exchange stays outstanding forever — which
    // is exactly the shape a never-created upstream leaves behind.
    fx.apply_task_command(AgentTaskEntityCommand::Create {
        operation_id: rakka_agent::AgentOperationId::new(
            rakka_agent::AgentOperationKind::TaskCreation,
            [tenant().as_str(), "ticket-1", "1"],
        )
        .expect("the operation id derives"),
        creation: Box::new(rakka_agent::AgentTaskCreation {
            definition: task_definition(),
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
            team: None,
            goal: None,
            goal_mode: Default::default(),
            goal_spec: None,
            parent: None,
            dependencies: vec![AgentTaskDependencyDeclaration::new(
                rakka_agent::AgentTaskId::new("never-created").expect("the task id is valid"),
            )],
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        }),
    })
    .await
    .expect("the dependent task creates");
    let _ = fx.settle_task_at(&task_scope()).await;

    let snapshot = agent_task_operational_snapshot(
        &fx.tasks,
        &task_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the task snapshot reads")
    .expect("the task exists");
    assert_eq!(
        snapshot.task.as_ref().expect("the task").status,
        AgentTaskStatus::Blocked,
        "the late edge demoted the task"
    );

    // Under the default threshold the edge is young, so nothing is reported —
    // the signal is not just "an unsettled registration exists".
    let quiet = agent_task_struggle_signals(&snapshot, &AgentStrugglePolicy::new());
    assert!(
        quiet.is_empty(),
        "a registration younger than the stall threshold is not a struggle: {quiet:?}"
    );

    // With the threshold at zero — an operator who wants every outstanding edge
    // — the same snapshot reports it, so the derivation reads the edge and not
    // merely the clock.
    let mut eager = AgentStrugglePolicy::new();
    eager.dependency_stall_millis = 0;
    let reported = agent_task_struggle_signals(&snapshot, &eager);
    assert_eq!(
        reported
            .iter()
            .map(|signal| signal.kind)
            .collect::<Vec<_>>(),
        vec![AgentStruggleSignalKind::StuckDependency],
        "the stalled edge is the signal: {reported:?}"
    );

    // Deriving twice from the same snapshot gives the same answer: these are
    // projections, and they observe nothing they could change.
    assert_eq!(reported, agent_task_struggle_signals(&snapshot, &eager));
}

/// A moderated conversation parked at its round ceiling names no next speaker,
/// so nothing can advance it but the moderator's early end — the moderation
/// exhaustion signal. Like every struggle signal it is a read-time projection:
/// the conversation stays `Active` and nothing about it changes.
#[tokio::test]
async fn moderation_exhaustion_reports_a_conversation_nothing_can_advance() {
    use rakka_agent::{
        agent_conversation_struggle_signals, AgentConversationCompletionRule,
        AgentConversationCreation, AgentConversationEntityCommand, AgentConversationId,
        AgentConversationMode, AgentConversationScope, AgentConversationStatus, AgentId,
        AgentModerationPolicy, AgentRevisionNumber, AgentStrugglePolicy, AgentStruggleSignalKind,
        AgentTaskId,
    };

    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    let conversation = AgentConversationId::new("panel").expect("the conversation id is valid");
    let scope = AgentConversationScope::new(tenant(), conversation.clone()).expect("the scope");
    let agent = |name: &str| AgentId::new(name).expect("the agent id is valid");
    // The turn door reads the speaker's definition, so the roster's members
    // are instantiated with the `Moderation` capability their turns spend.
    fx.instantiate_conversation_participants(&["moderator", "p1"])
        .await;

    // One round, one turn: the ceiling is reached the moment that turn lands,
    // and `ModeratorDecides` parks rather than completing.
    let policy = AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
        .with_max_rounds(1)
        .with_max_turns_per_round(1);
    fx.apply_conversation_command_at(
        &scope,
        AgentConversationEntityCommand::Create {
            operation_id: rakka_agent::conversation_create_operation_id(&tenant(), &conversation)
                .expect("the operation id derives"),
            creation: Box::new(AgentConversationCreation {
                moderator: agent("moderator"),
                participants: vec![agent("p1")],
                mode: AgentConversationMode::RoundRobin,
                completion: AgentConversationCompletionRule::ModeratorDecides,
                policy,
                task: AgentTaskId::new("debate-task").expect("the task id is valid"),
                tokens: None,
                max_wall_clock_millis: None,
                transcript_ref: None,
            }),
        },
    )
    .await
    .expect("the conversation creates");

    let opening = fx
        .conversation_snapshot_at(&scope)
        .await
        .expect("the conversation snapshots");
    assert!(
        agent_conversation_struggle_signals(&opening, &AgentStrugglePolicy::new(), fx.now())
            .is_empty(),
        "a conversation with a live speaker is not exhausted"
    );

    fx.apply_conversation_command_at(
        &scope,
        AgentConversationEntityCommand::SubmitTurn {
            operation_id: rakka_agent::conversation_turn_operation_id(
                &tenant(),
                &conversation,
                0,
                0,
                &agent("p1"),
                &rakka_agent::conversation_turn_content_digest("a position", None),
            )
            .expect("the operation id derives"),
            submit: Box::new(rakka_agent::AgentConversationTurnSubmit {
                round: 0,
                turn: 0,
                participant: agent("p1"),
                body: "a position".to_string(),
                direction: None,
                usage: rakka_agent::AgentBudgetConsumption::zero(),
            }),
        },
    )
    .await
    .expect("the only admissible turn records");

    let parked = fx
        .conversation_snapshot_at(&scope)
        .await
        .expect("the conversation snapshots");
    assert_eq!(
        parked.status,
        AgentConversationStatus::Active,
        "the ceiling parks the cursor; it does not terminalize"
    );
    assert!(
        parked.current_speaker.is_none(),
        "a parked cursor names no next speaker"
    );

    let signals =
        agent_conversation_struggle_signals(&parked, &AgentStrugglePolicy::new(), fx.now());
    assert_eq!(
        signals.iter().map(|signal| signal.kind).collect::<Vec<_>>(),
        vec![AgentStruggleSignalKind::ModerationExhaustion],
        "a conversation nothing can advance is reported: {signals:?}"
    );

    // The projection changed nothing it observed.
    let after = fx
        .conversation_snapshot_at(&scope)
        .await
        .expect("the conversation snapshots");
    assert_eq!(parked, after, "a struggle signal mutates nothing");
}
