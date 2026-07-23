//! The run entity and its durable loop.
//!
//! Specification: sections 6.5, 9.3, 9.4, 9.5, and 15; scenario 2 of section 18.
//! A run must survive an actor restart after *every* loop transition and resume
//! from what it durably persisted — never by replaying a model call it already
//! made, and never by waiting forever on an effect it forgot to dispatch.
//!
//! The dispatcher here is the `ScriptedDispatcher` of `rakka_agent::testkit`. It
//! plays exactly the role the real dispatcher of slice 1.7 will play: it reads
//! the effects the run committed, does the bounded work, and returns each outcome
//! as a durable result command through the entity's command surface. The loop
//! never calls a model; it persists an effect and waits, which is the whole point
//! of specification 9.5.

use rakka_agent::testkit::{run_entity, sweep_crash_points, CrashPoint, ScriptedDispatcher};
use rakka_agent::{load_agent_run_state, AgentSchemaPolicy};
use rakka_agent::{
    AgentLoopPhase, AgentModelTurn, AgentModelUsage, AgentOperationId, AgentOperationKind,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunId, AgentRunScope, AgentRunState,
    AgentRunStatus, AgentRunTerminalReason, AgentTaskContent, AgentTaskStatus, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
    CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION,
};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

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

fn tool_call(call: &str, tool: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call).expect("call id should be valid"),
        AgentToolId::new(tool).expect("tool id should be valid"),
        serde_json::json!({ "query": "ticket" }),
    )
    .expect("the tool call is bounded")
}

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(tool_call("call-1", tool))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_run_serves_its_task_through_the_durable_loop_and_never_completes_it_alone() {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // The task assigned the run and the run durably accepted, so the task is in
    // progress and the run exists — all before a single model call.
    let accepted = fx.run_snapshot().await.expect("the run accepted");
    assert_eq!(accepted.generation.get(), 1);
    assert_eq!(accepted.task, *task_scope().task());

    fx.pump().await.expect("the loop should run to completion");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    assert_eq!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::ResultAccepted)
    );

    // The run's own state records the consequence; the *task* is what made the
    // public task terminal ([specification 9.5]).
    let accepted_result = run.accepted_result.expect("the task accepted a result");
    assert_eq!(
        accepted_result.content.inline_value(),
        Some(&serde_json::json!({ "answer": "resolved" }))
    );

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);
    assert_eq!(
        task.accepted_result.expect("the task holds the result").run,
        *run_scope().run()
    );

    // One turn, one model call, one durable effect.
    assert_eq!(fx.dispatcher.model_calls(), 1);
    assert_eq!(fx.dispatched_effects(), 1);
    assert_eq!(run.turn, 1);

    // The run charged what it spent, in its own ledger and nobody else's.
    assert_eq!(run.budget.model_calls(), 1);
    assert_eq!(run.budget.loop_iterations(), 1);
    assert_eq!(run.budget.tokens(), 15);
}

#[tokio::test]
async fn restart_after_every_loop_transition_resumes_correctly() {
    // Scenario 2.
    //
    // First, the crash-free flow, to learn how many durable writes a run makes.
    let reference = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(writes >= 5, "the loop should make several durable writes");
    assert_eq!(reference.dispatched_effects(), 1);

    // Then kill the run's owner at each of those writes, on both sides of the
    // compare-and-set, and re-drive from durable state alone.
    sweep_crash_points(writes, |nth, point| async move {
        let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
        fx.instantiate_agent().await;

        fx.runs.crash_at(nth, point);
        fx.create_task().await;
        // The crash may surface here or on a later pass; either way the run's
        // owner is gone and nothing in memory survives it.
        let _crashed = fx.pump().await;

        // A new owner activates and finds only what was durably committed.
        fx.runs.assert_crash_fired(nth, point);
        fx.runs.survive();
        fx.pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let run = fx
            .run_snapshot()
            .await
            .unwrap_or_else(|| panic!("crash {point:?} at write {nth}: the run was lost"));
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} should still complete"
        );
        assert_eq!(
            run.accepted_result
                .as_ref()
                .and_then(|result| result.content.inline_value()),
            Some(&serde_json::json!({ "answer": "resolved" })),
            "crash {point:?} at write {nth} should accept the same result"
        );

        let task = fx.task_snapshot().await;
        assert_eq!(
            task.status,
            AgentTaskStatus::Completed,
            "crash {point:?} at write {nth} should complete the task exactly once"
        );
        assert_eq!(
            task.rejection_count, 0,
            "crash {point:?} at write {nth} must not re-validate the proposal"
        );

        // The effect id is derived from the run and the turn, so a re-driven
        // dispatch names the same effect. A restart therefore cannot make the
        // model run twice ([specification 15]).
        assert_eq!(
            fx.dispatched_effects(),
            1,
            "crash {point:?} at write {nth} dispatched a duplicate effect"
        );
        assert_eq!(
            run.turn, 1,
            "crash {point:?} at write {nth} replayed a turn it had already taken"
        );
    })
    .await;
}

#[tokio::test]
async fn a_duplicate_effect_result_does_not_advance_the_loop_twice() {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Advance to the model wait.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("the run should recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("the run should crank its loop");

    let waiting = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(waiting.status, AgentRunStatus::WaitingForEffect);
    assert_eq!(waiting.phase, AgentLoopPhase::AwaitingModel);
    assert_eq!(waiting.outstanding_effects, 1);

    let effect = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .first()
        .expect("the model effect is persisted")
        .clone();

    // The dispatcher returns the same result twice, under the same derived
    // operation id — a redelivered inbox command.
    let outcome = fx.dispatcher.answer(&effect).await;
    let command = || AgentRunEntityCommand::RecordEffectResult {
        operation_id: effect
            .result_operation_id(&run_scope())
            .expect("the result operation id is derivable"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        attempt: 1,
        fence: 0,
        outcome: Box::new(outcome.clone()),
    };

    let first = run
        .apply(command(), &fx.router, fx.now())
        .await
        .expect("the first result applies");
    assert!(matches!(first, AgentRunEntityReply::Applied { .. }));

    let second = run
        .apply(command(), &fx.router, fx.now())
        .await
        .expect("the duplicate is deduplicated, not rejected");
    let AgentRunEntityReply::Duplicate { outcome } = second else {
        panic!("a redelivered effect result must be deduplicated, got {second:?}");
    };

    // One model call was consumed, one turn was taken, and the loop moved once.
    assert_eq!(outcome.turn, 1);
    fx.pump().await.expect("the loop completes");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.budget.model_calls(), 1);
    assert_eq!(fx.task_snapshot().await.rejection_count, 0);
}

#[tokio::test]
async fn a_stale_effect_result_is_refused_by_the_runs_own_fence() {
    // A turn that asks for two tools, so the run is still waiting on the second
    // when the first is answered a second time. The deduplication log is the fast
    // path; this proves the run's *own* fence refuses a stale completion even when
    // the log cannot ([specification 18] scenario 10).
    let two_tools = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_tool_call(tool_call("call-1", "knowledge-base"))
        .with_tool_call(tool_call("call-2", "ticket-history"));

    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(two_tools));
    fx.instantiate_agent().await;
    fx.create_task().await;

    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("the run should recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("the run should crank its loop");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model turn comes back");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run persists its tool effects");

    let tool_effects: Vec<_> = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .iter()
        .filter(|effect| effect.is_outstanding())
        .cloned()
        .collect();
    assert_eq!(tool_effects.len(), 2, "both tool effects are outstanding");

    let first = &tool_effects[0];
    let outcome = fx.dispatcher.answer(first).await;
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: first.result_operation_id(&run_scope()).expect("derivable"),
            effect_id: first.effect_id.clone(),
            generation: first.generation,
            attempt: 1,
            fence: 0,
            outcome: Box::new(outcome.clone()),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the first tool result applies");

    // The same result arrives again under a *different* operation id, so the
    // deduplication log cannot absorb it. The run's own fence must: the effect is
    // resolved, and a resolved effect does not accept a second result.
    let stale = run
        .apply(
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::Command,
                    [TENANT, AGENT, "stale"],
                )
                .expect("derivable"),
                effect_id: first.effect_id.clone(),
                generation: first.generation,
                attempt: 1,
                fence: 0,
                outcome: Box::new(outcome),
            },
            &fx.router,
            fx.now(),
        )
        .await;

    let error = stale.expect_err("a stale completion must be refused");
    assert_eq!(error.code(), "run-stale-effect-result");

    // The run is exactly where it was: still waiting on the second tool.
    let waiting = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(waiting.phase, AgentLoopPhase::AwaitingTools);
    assert_eq!(waiting.outstanding_effects, 1);
    assert_eq!(waiting.turn, 1);
}

#[tokio::test]
async fn a_redelivered_completion_after_any_owner_loss_never_advances_twice() {
    // Scenario 10 under the owner-kill sweep: whatever write the owner died
    // at, the converged run has taken its turn exactly once — and a dispatcher
    // redelivering the model completion afterward never applies again. The
    // effect identity is deterministic, so the reference flow captures it
    // mid-wait; the converged run has pruned the resolved record (bounded
    // state), which is exactly the late-duplicate shape scenario 10 is about.
    // The run store is the only store this flow's crash windows live in; the
    // driver is the in-process dispatcher, so owner kill at every write is the
    // complete boundary set here.
    let reference = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;

    // Pause the reference at the model wait to capture the effect identity and
    // the outcome the dispatcher will deliver.
    let mut run = reference.run();
    let now = reference.now();
    run.recover(now).await.expect("the run recovers");
    run.settle_side_effects(&reference.router, now)
        .await
        .expect("the run cranks to its model wait");
    let effect = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .first()
        .expect("the model effect is persisted")
        .clone();
    let outcome = reference.dispatcher.answer(&effect).await;
    drop(run);
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(writes >= 5);

    sweep_crash_points(writes, |nth, point| {
        let effect = effect.clone();
        let outcome = outcome.clone();
        async move {
            let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
            fx.instantiate_agent().await;

            fx.runs.crash_at(nth, point);
            fx.create_task().await;
            let _crashed = fx.pump().await;

            fx.runs.assert_crash_fired(nth, point);
            fx.runs.survive();
            fx.pump().await.unwrap_or_else(|error| {
                panic!("crash {point:?} at write {nth} did not converge: {error}")
            });

            // Redeliver the turn's model completion under the operation id the
            // dispatcher originally minted — a late duplicate inbox command.
            let mut run = fx.run();
            run.recover(fx.now()).await.expect("the run recovers");
            let redelivery = run
                .apply(
                    AgentRunEntityCommand::RecordEffectResult {
                        operation_id: effect
                            .result_operation_id(&run_scope())
                            .expect("the result operation id is derivable"),
                        effect_id: effect.effect_id.clone(),
                        generation: effect.generation,
                        attempt: 1,
                        fence: 0,
                        outcome: Box::new(outcome),
                    },
                    &fx.router,
                    fx.now(),
                )
                .await;
            match redelivery {
                Ok(AgentRunEntityReply::Duplicate { .. }) | Err(_) => {}
                Ok(other) => panic!(
                    "crash {point:?} at write {nth}: a redelivered completion \
                     must not apply, got {other:?}"
                ),
            }

            let run = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(run.status, AgentRunStatus::Completed);
            assert_eq!(
                run.turn, 1,
                "crash {point:?} at write {nth} advanced the loop twice"
            );
            assert_eq!(
                run.budget.model_calls(),
                1,
                "crash {point:?} at write {nth} consumed a second model call"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn a_tool_calling_turn_waits_on_its_tools_before_it_records_the_turn() {
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("knowledge-base"))
            .with_turn(proposing_turn("resolved"))
            .with_tool_result(
                "knowledge-base",
                AgentTaskContent::inline(serde_json::json!({ "article": 7 }))
                    .expect("the tool result is inline-bounded"),
            ),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Turn one: the model asks for a tool. The run persists the tool effect and
    // waits — it does not call the tool inside its handler.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model turn comes back");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("crank");

    let awaiting = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(awaiting.phase, AgentLoopPhase::AwaitingTools);
    assert_eq!(awaiting.status, AgentRunStatus::WaitingForEffect);
    assert_eq!(awaiting.outstanding_effects, 1);

    fx.pump().await.expect("the loop completes");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2, "the tool turn and the proposing turn");
    assert_eq!(fx.dispatcher.model_calls(), 2);
    assert_eq!(fx.dispatcher.tool_calls(), 1);
    // Two model effects and one tool effect, each dispatched exactly once.
    assert_eq!(fx.dispatched_effects(), 3);
    assert_eq!(run.budget.tool_calls(), 1);
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::Completed);
}

#[tokio::test]
async fn an_exhausted_iteration_budget_stops_the_run_with_a_structured_reason() {
    // The model never proposes anything, so the loop iterates until its ceiling.
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should stop on its own");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);

    let Some(AgentRunTerminalReason::BudgetExhausted { exhaustion }) = run.terminal_reason else {
        panic!(
            "expected a structured budget exhaustion, got {:?}",
            run.terminal_reason
        );
    };
    assert_eq!(
        exhaustion.dimension,
        rakka_agent::AgentBudgetDimension::LoopIterations
    );
    assert_eq!(exhaustion.limit, 3);
    assert_eq!(exhaustion.consumed, 3);

    // The ceiling is the ceiling: three iterations, three model calls, and no
    // fourth of either.
    assert_eq!(fx.dispatcher.model_calls(), 3);
    assert_eq!(run.budget.loop_iterations(), 3);

    // The run failed; the *task* did not, because the run never proposed a result
    // and a run is not the task.
    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn tool_budget_exhaustion_mid_turn_commits_no_partial_fan_out() {
    // A turn asks for two tools under a ceiling of one. The refusal must not
    // commit the first call's effect alongside the terminal status: a run that
    // has already failed performs no further external work, and the result of
    // that work could only be refused as `run-terminal` anyway. The whole
    // fan-out is charged before any effect is recorded, so the turn either fits
    // its budget or terminates having committed nothing.
    let turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look those up.")
        .with_tool_call(tool_call("call-1", "search"))
        .with_tool_call(tool_call("call-2", "fetch"));
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(turn));
    fx.instantiate_agent().await;
    fx.create_task_with(
        task_definition().with_budgets(rakka_agent::AgentBudgetCeilings {
            max_loop_iterations: Some(3),
            max_tool_calls: Some(1),
            ..rakka_agent::AgentBudgetCeilings::unbounded()
        }),
    )
    .await;
    fx.pump().await.expect("the loop should stop on its own");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    let Some(AgentRunTerminalReason::BudgetExhausted { exhaustion }) = run.terminal_reason else {
        panic!(
            "expected a structured budget exhaustion, got {:?}",
            run.terminal_reason
        );
    };
    assert_eq!(
        exhaustion.dimension,
        rakka_agent::AgentBudgetDimension::ToolCalls
    );
    assert_eq!(exhaustion.limit, 1);

    // No partial fan-out survives into the terminal record, and only the model
    // effect ever reached the sink: the refused turn dispatched no tool.
    assert_eq!(
        run.outstanding_effects, 0,
        "a terminal run must hold no pending effect"
    );
    assert_eq!(
        fx.dispatched_effects(),
        1,
        "only the model effect dispatched"
    );
    assert_eq!(fx.dispatcher.tool_calls(), 0);

    // The run failed; the task did not, because the run never proposed a result.
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn a_failed_effect_stops_the_run_with_the_dispatchers_reason() {
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("flaky-tool"))
            .with_tool_failure("flaky-tool", "tool-unavailable", "the tool is down"),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should stop");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    let Some(AgentRunTerminalReason::EffectFailed { code, .. }) = run.terminal_reason else {
        panic!("expected a failed effect, got {:?}", run.terminal_reason);
    };
    assert_eq!(code, "tool-unavailable");
}

#[tokio::test]
async fn a_loop_state_from_a_newer_binary_fails_closed() {
    // Specification 20. The loop state versions separately from the run state that
    // carries it, so a rolling update meets a record whose *nested* version it does
    // not understand — and must refuse it rather than interpret a phase, an effect,
    // or a turn with guessed semantics.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("the run should recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("the run should crank its loop");

    // Rewrite the durable record as a newer binary would have written it.
    let record = DurableStateStore::load(&fx.runs, &run_scope().persistence_id())
        .await
        .expect("the record loads")
        .expect("the run was written");
    let mut raw = serde_json::to_value(&record.state).expect("the state serializes");
    let ahead = u64::from(CURRENT_AGENT_LOOP_STATE_SCHEMA_VERSION.get()) + 1;
    raw["run"]["loop_state"]["schema_version"] = serde_json::json!(ahead);
    let from_the_future: AgentRunState =
        serde_json::from_value(raw).expect("the newer record still parses structurally");
    DurableStateStore::compare_and_set(
        &fx.runs,
        &run_scope().persistence_id(),
        record.revision,
        from_the_future,
    )
    .await
    .expect("the newer record is written");

    // Both the point read and the entity's own recovery must refuse it.
    let read = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default()).await;
    assert_eq!(
        read.expect_err("a newer loop state must fail closed")
            .code(),
        "schema-version-ahead"
    );

    let mut recovered = fx.run();
    let error = recovered
        .recover(fx.now())
        .await
        .expect_err("recovery must fail closed too");
    assert_eq!(error.code(), "schema-version-ahead");
}

#[tokio::test]
async fn cancelling_a_run_with_nothing_outstanding_is_terminal_at_once() {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Advance to the model wait, then let the result command commit and kill
    // the owner before its settle pass advances the loop. The recovered state —
    // an executable phase, the effect resolved, no proposal — is a durable
    // state in which the run is not terminal yet has nothing in flight.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the model wait");
    fx.runs.crash_at(1, CrashPoint::AfterWrite);
    let _crashed = fx.dispatcher.drive(&mut run, &fx.router, fx.now()).await;
    fx.runs.survive();

    let idle = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(idle.phase, AgentLoopPhase::EvaluatingModelOutput);
    assert_eq!(idle.outstanding_effects, 0);
    assert!(idle.proposal.is_none());

    // Nothing is in flight, so cancellation is terminal immediately. The
    // reason ends exactly at a multi-byte character straddling the detail
    // bound, which must be truncated at a char boundary rather than panic the
    // entity.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("derivable"),
            reason: format!("{}é", "a".repeat(511)),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("cancellation applies");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Cancelled);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
}

#[tokio::test]
async fn cancelling_a_run_with_a_proposal_in_flight_waits_for_the_decision() {
    use rakka_agent::testkit::ExchangeFault;

    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Drive the run until it has proposed its result and is waiting for the task's
    // decision. Every delivery of the proposal is lost, so the decision never comes
    // back and the run sits at `Running` with a pending proposal and no effect
    // outstanding. One settle pass re-drives an owed exchange more than once, so
    // several faults are queued to outlast the passes up to and including the
    // cancellation's own.
    for _ in 0..6 {
        fx.task_transport.inject(ExchangeFault::LoseEnvelope);
    }
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the model wait");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model turn comes back");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the run proposes its result");

    let proposing = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(proposing.status, AgentRunStatus::Running);
    assert_eq!(proposing.phase, AgentLoopPhase::DecidingContinuation);
    assert_eq!(proposing.outstanding_effects, 0);
    assert!(proposing.proposal.is_some());

    // The proposal is outstanding work: the task may already have decided, so
    // the run quiesces instead of becoming terminal — a late decision must
    // never find a terminal run to resurrect or contradict.
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("derivable"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("cancellation applies");

    let quiescing = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        quiescing.status,
        AgentRunStatus::Cancelling,
        "a run with a proposal in flight quiesces before it is terminal"
    );
    assert_eq!(quiescing.phase, AgentLoopPhase::DecidingContinuation);
    assert!(quiescing.proposal.is_some());

    // The faults are exhausted, so the next sweep delivers the proposal and the
    // decision settles it. Acceptance wins the race with the wind-down: the
    // task durably completed on this result, and the run records that
    // truthfully rather than holding an accepted result under a cancelled
    // status.
    fx.pump().await.expect("the decision settles");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::Completed);
}

#[tokio::test]
async fn a_failed_effect_while_cancelling_preserves_the_cancellation() {
    // Cancellation is durably recorded before the in-flight effect resolves; a
    // failure coming back afterwards must not rewrite the run's outcome from
    // "the operator stopped it" to "it failed".
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("search"))
            .with_tool_failure("search", "tool-unavailable", "the tool backend is down"),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Advance to the model wait, resolve the model turn, and stop at the tool
    // wait.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the model wait");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model turn comes back");
    let waiting = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(waiting.phase, AgentLoopPhase::AwaitingTools);

    // Cancel while the tool effect is in flight.
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("derivable"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("cancellation applies");

    // The tool fails. The run records the failure and finishes the wind-down
    // with the reason the operator recorded, not the failure's.
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the failed result applies");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Cancelled);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    assert_eq!(
        run.terminal_reason.expect("the reason is recorded").code(),
        "cancellation-requested",
        "the failure must not rewrite the recorded cancellation"
    );
}

#[tokio::test]
async fn a_failed_tool_waits_for_its_siblings_before_the_run_is_terminal() {
    // A turn fans out two tools. The first fails, which decides the run's
    // outcome — but the second is dispatched work whose result must still be
    // recordable, not refused by an already-terminal run ([specification 8.7]).
    let turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look those up.")
        .with_tool_call(tool_call("call-1", "search"))
        .with_tool_call(tool_call("call-2", "fetch"));
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(turn)
            .with_tool_failure("search", "tool-unavailable", "the tool backend is down")
            .with_tool_result(
                "fetch",
                AgentTaskContent::inline(serde_json::json!({ "page": "..." }))
                    .expect("the result is inline-bounded"),
            ),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;

    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the model wait");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model turn comes back");
    let waiting = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(waiting.phase, AgentLoopPhase::AwaitingTools);
    assert_eq!(waiting.outstanding_effects, 2);

    // Both results apply: the failure first, then the sibling's outcome — which
    // the old fail-fast contract would have refused as `run-terminal`.
    let delivered = fx
        .dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("both results apply");
    assert_eq!(delivered, 2);

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    assert_eq!(
        run.outstanding_effects, 0,
        "nothing was abandoned in flight"
    );
    assert_eq!(
        run.terminal_reason.expect("the reason is recorded").code(),
        "effect-failed"
    );
}

#[tokio::test]
async fn cancelling_a_run_with_an_effect_in_flight_waits_for_it_and_does_not_resume() {
    // Cancellation fences further dispatch, but a run with an effect in flight does
    // not become terminal until it resolves ([specification 8.7]) — and recording
    // that outcome must not resume the run. This is the interaction that a naive
    // "set status to Running on every result" would silently break.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Advance to the model wait.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank");
    let waiting = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(waiting.status, AgentRunStatus::WaitingForEffect);
    assert_eq!(waiting.outstanding_effects, 1);

    // Cancel while the effect is outstanding.
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("derivable"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("cancellation applies");

    let cancelling = run.snapshot().expect("snapshot").expect("the run exists");
    assert_eq!(
        cancelling.status,
        AgentRunStatus::Cancelling,
        "a run with an effect in flight quiesces before it is terminal"
    );

    // The effect result comes back. The run records it — the outcome of work
    // already in flight is not lost — and then becomes terminal, rather than
    // resuming as though it were never cancelled.
    let effect = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .first()
        .expect("the model effect is persisted")
        .clone();
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect.result_operation_id(&run_scope()).expect("derivable"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: 1,
            fence: 0,
            outcome: Box::new(fx.dispatcher.answer(&effect).await),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the result applies");

    // Draining any further work must not revive it.
    fx.pump().await.expect("the loop is already terminal");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Cancelled);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    // It never proposed anything, so the task is still in progress: cancelling a
    // run is not completing its task.
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn cancellation_fences_a_provably_unsent_effect_in_place() {
    // The owner dies after committing the model effect but before the flush
    // marked it dispatchable, and cancellation arrives before recovery
    // re-drives anything. The effect is durably `Pending` — and `Pending`
    // *proves* it never reached the sink, because the flush hands an effect
    // over only after the transition that marked it `Ready` committed. The
    // acceptance of the cancellation may therefore fence it in the same
    // compare-and-set ([specification 8.7]): nothing was invoked, nothing is
    // abandoned, and nothing may be dispatched after the fence.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;

    // Write 1 accepts the assignment; write 2 commits the model effect and the
    // wait. Dying after write 2 leaves the effect pending and the sink empty.
    fx.runs.crash_at(2, CrashPoint::AfterWrite);
    fx.create_task().await;
    fx.runs.survive();

    let stranded = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(stranded.status, AgentRunStatus::WaitingForEffect);
    assert_eq!(stranded.outstanding_effects, 1);
    assert_eq!(
        fx.dispatched_effects(),
        0,
        "the committed effect never reached the sink"
    );

    // Cancellation arrives before any settle pass. The unsent effect is fenced
    // in the same transition, so the run has nothing left to wait for.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("derivable"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("cancellation applies");

    let run_snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run_snapshot.status, AgentRunStatus::Cancelled);
    assert_eq!(run_snapshot.phase, AgentLoopPhase::Complete);
    assert_eq!(
        run_snapshot
            .terminal_reason
            .expect("the reason is recorded")
            .code(),
        "cancellation-requested"
    );

    // No settle pass — before or after the fence — may hand the fenced effect
    // to the sink: dispatching it now would be exactly the new external work
    // the fence forbids.
    fx.pump().await.expect("nothing further moves");
    assert_eq!(
        fx.dispatched_effects(),
        0,
        "a fenced effect is never dispatched after the cancellation"
    );
    assert_eq!(fx.dispatcher.model_calls(), 0, "the model was never called");

    // The crash lost the acceptance reply, so the task re-drives its
    // assignment exchange during the pump and the run answers with the
    // acceptance it already recorded. The run being cancelled neither
    // completes nor fails the task: that consequence belongs to a later
    // decision, not to the cancellation.
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn a_run_that_was_never_assigned_is_inert_and_addressable() {
    let fx = Fixture::new(ScriptedDispatcher::new());

    let unassigned = AgentRunScope::new(
        tenant(),
        agent_id(),
        AgentRunId::new("never-assigned").expect("run id should be valid"),
    )
    .expect("run scope should be valid");

    let mut run = run_entity(&unassigned, &fx.runs, &fx.effects);
    let now = fx.now();
    let state = run.recover(now).await.expect("an unwritten run recovers");
    assert!(!state.is_accepted());
    assert!(run.snapshot().expect("snapshot").is_none());

    // Cranking it does nothing at all, and writes nothing.
    let progress = run
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("a run with no assignment settles trivially");
    assert_eq!(progress.transitions, 0);
    assert_eq!(progress.effects_dispatched, 0);
    assert_eq!(progress.outstanding, 0);
}
