//! The escrow budget ledger, end to end across the task and run entities.
//!
//! Specification: section 9.7, and scenario 61 of section 18. A task debits a
//! run's allocation from its own ledger inside the assignment transition; the
//! run charges only its own ledger; and when the run reaches a terminal outcome
//! it settles what it consumed and returns what it did not, both as deduplicated
//! exchanges back to the task. Replaying any of it credits the parent exactly
//! once.
//!
//! These tests drive the exchanges through the same in-process router the rest
//! of the suite uses, and read the durable escrow off the task's own state —
//! never a telemetry projection — so they are correct while every entity is
//! passivated.

use rakka_agent::testkit::{CrashPoint, ExchangeFault, ScriptedDispatcher};
use rakka_agent::{
    load_agent_task_state, AgentBudgetAllocation, AgentBudgetCeilings, AgentBudgetDimension,
    AgentEscrowChildId, AgentModelTurn, AgentModelUsage, AgentRunSettlementStatus, AgentRunStatus,
    AgentSchemaPolicy, AgentTaskContent, AgentTaskDefinition, AgentTaskResultCheck,
    AgentTaskResultRule, AgentTaskRuleId, AgentTaskStatus, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

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

/// A turn that proposes nothing and asks for nothing, so the run takes another
/// iteration.
fn empty_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("Still working.")
}

/// A task with headroom to grant a top-up: it escrows each run one loop
/// iteration but holds four, so an exhausted run's first ask can be funded.
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

/// A task whose runs are escrowed a bounded, partial slice of the task's own
/// budget, so both the settlement (what a run spent) and the return (what it did
/// not) carry something to assert on.
fn escrowed_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket under a bounded budget.",
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
        max_loop_iterations: Some(10),
        max_model_calls: Some(10),
        max_tokens: Some(1_000),
        ..AgentBudgetCeilings::unbounded()
    })
}

fn run_child() -> AgentEscrowChildId {
    AgentEscrowChildId::for_run(run_scope().run()).expect("the run child id is derivable")
}

#[tokio::test]
async fn a_completed_run_settles_what_it_spent_and_returns_what_it_did_not() {
    // The full round trip: the task debits the run's allocation at assignment,
    // the run consumes part of it, and on completion the run settles its
    // consumption and returns the remainder — all upward through the
    // deduplicated ledger exchanges of specification 9.7.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task_with(escrowed_definition()).await;
    fx.pump().await.expect("the run completes and settles");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        run.settlement,
        AgentRunSettlementStatus::Returned,
        "a completed run must settle and then return its escrow"
    );

    let task = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let escrow = &task.task().expect("the task is created").escrow;

    // The run took exactly one model turn, which the ledger counts as one
    // iteration, one model call, one durable effect, and one dispatch attempt,
    // and the turn billed 15 tokens. The effect and attempt dimensions are the
    // proof that a real dispatched effect debits the run's ledger — not only the
    // per-turn counters.
    let consumed = escrow.consumed();
    assert_eq!(consumed.loop_iterations, 1);
    assert_eq!(consumed.model_calls, 1);
    assert_eq!(consumed.effects, 1);
    assert_eq!(consumed.effect_attempts, 1);
    assert_eq!(consumed.tokens, 15);

    // The escrow is closed: the return removed the child, so the task's headroom
    // is its whole allocation less only what the run actually spent.
    assert!(
        escrow.child(&run_child()).is_none(),
        "a returned child leaves no outstanding escrow"
    );
    assert_eq!(
        escrow.available(AgentBudgetDimension::LoopIterations),
        Some(9)
    );
    assert_eq!(escrow.available(AgentBudgetDimension::ModelCalls), Some(9));
    assert_eq!(escrow.available(AgentBudgetDimension::Tokens), Some(985));
}

#[tokio::test]
async fn a_run_that_exhausts_its_budget_still_settles_what_it_spent() {
    // The other terminal door: a run that never proposes a result iterates until
    // its loop budget is spent, goes terminal through the loop transition itself,
    // and settles from there. An exhausted run has still consumed everything it
    // was charged, and its escrow must reflect that — ambiguity about the outcome
    // does not make the work free ([specification 9.7]).
    let tight = AgentTaskDefinition::new(
        task_definition_id(),
        "A ticket whose run runs out of iterations before it answers.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(2),
        ..AgentBudgetCeilings::unbounded()
    });

    // No scripted proposal: the deterministic adapter returns empty turns, so the
    // run iterates to its ceiling.
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_task_with(tight).await;
    fx.pump().await.expect("the run exhausts and settles");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert!(run.status.is_terminal(), "an exhausted run is terminal");
    assert_eq!(
        run.settlement,
        AgentRunSettlementStatus::Returned,
        "an exhausted run settles and returns like any other terminal run"
    );

    let task = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let escrow = &task.task().expect("the task is created").escrow;

    assert_eq!(
        escrow.consumed().loop_iterations,
        2,
        "the run consumed every iteration it was charged"
    );
    assert!(
        escrow.child(&run_child()).is_none(),
        "the escrow is closed even when the run failed"
    );
    assert_eq!(
        escrow.available(AgentBudgetDimension::LoopIterations),
        Some(0)
    );
}

#[tokio::test]
async fn replaying_the_settlement_and_return_never_credits_the_parent_twice() {
    // Scenario 61, the replay half: the ledger exchanges are deduplicated on the
    // task's own escrow record, so re-driving them after the run has already
    // settled and returned credits nothing further.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task_with(escrowed_definition()).await;
    fx.pump().await.expect("the run completes and settles");

    let after_first =
        load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the task state loads")
            .expect("the task exists");
    let consumed_once = *after_first
        .task()
        .expect("the task is created")
        .escrow
        .consumed();

    // A second full sweep re-drives every exchange either side still believes it
    // owes. Nothing is owed — the run is Returned — but even if the run re-drove
    // its settlement, the escrow would answer from what it already recorded.
    fx.pump().await.expect("a second sweep converges");

    let after_second =
        load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the task state loads")
            .expect("the task exists");
    let escrow = &after_second.task().expect("the task is created").escrow;

    assert_eq!(
        *escrow.consumed(),
        consumed_once,
        "replaying the settlement must not double-count consumption"
    );
    assert_eq!(
        escrow.available(AgentBudgetDimension::LoopIterations),
        Some(9),
        "replaying the return must not credit the parent twice"
    );
    assert_eq!(
        fx.task_snapshot().await.status,
        AgentTaskStatus::Completed,
        "the ledger round trip does not disturb the task's own lifecycle"
    );
}

#[tokio::test]
async fn an_exhausted_run_asks_its_parent_for_more_and_resumes_on_the_grant() {
    // Specification 9.7: a run that exhausts its escrowed allocation parks with a
    // structured reason and asks its parent for more, rather than failing at
    // once. The parent's grant is an ordinary parent-local allocation decision
    // under its own ceilings, and it resumes the run exactly where its
    // exhaustion parked it.
    //
    // The run is escrowed one iteration but its first turn proposes nothing, so
    // it exhausts on the second. Its task holds four iterations, so the first ask
    // is funded and the run completes.
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(empty_turn())
            .with_turn(proposing_turn("resolved")),
    );
    fx.instantiate_agent().await;
    fx.create_task_with(toppable_definition()).await;
    fx.pump().await.expect("the run tops up and completes");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert!(
        run.pending_top_up.is_none(),
        "a resumed run holds no pending top-up"
    );
    assert_eq!(
        run.budget.top_ups(),
        1,
        "the run asked for and received exactly one top-up"
    );
    // Nothing already consumed is forgotten: the run took a second iteration on
    // the topped-up budget.
    assert_eq!(run.budget.loop_iterations(), 2);
    assert_eq!(run.settlement, AgentRunSettlementStatus::Returned);

    // The task funded the top-up from its own ledger: it granted one more
    // iteration, and the run consumed two of the task's four.
    let task = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let escrow = &task.task().expect("the task is created").escrow;
    assert_eq!(escrow.consumed().loop_iterations, 2);
    assert!(escrow.child(&run_child()).is_none());
    assert_eq!(
        escrow.available(AgentBudgetDimension::LoopIterations),
        Some(2)
    );
}

#[tokio::test]
async fn a_run_stops_with_its_original_reason_when_its_parent_has_nothing_to_grant() {
    // A grant of nothing is the parent's honest answer when it has nothing left,
    // and the run must not park on it forever: it stops with the *same*
    // structured exhaustion it first hit. Here the task escrows the run its whole
    // budget, so the ask cannot be funded.
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_task().await; // default: three iterations, all escrowed to the run
    fx.pump()
        .await
        .expect("the run asks, is refused, and stops");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert!(run.pending_top_up.is_none());
    assert_eq!(
        run.budget.top_ups(),
        1,
        "the run asked once and was answered with nothing"
    );
    let Some(rakka_agent::AgentRunTerminalReason::BudgetExhausted { exhaustion }) =
        run.terminal_reason
    else {
        panic!(
            "expected the original budget exhaustion, got {:?}",
            run.terminal_reason
        );
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::LoopIterations);
    assert_eq!(exhaustion.limit, 3);

    // The refused run still hands its escrow back, so the task is whole again.
    assert_eq!(run.settlement, AgentRunSettlementStatus::Returned);
}

#[tokio::test]
async fn a_duplicated_top_up_delivery_credits_the_run_once() {
    // The top-up is a deduplicated exchange (scenario 61's shape): a re-delivered
    // request returns the parent's original grant from its escrow record, and the
    // run's journal settles it once, so the run is credited exactly once.
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(empty_turn())
            .with_turn(proposing_turn("resolved")),
    );
    fx.instantiate_agent().await;
    fx.create_task_with(toppable_definition()).await;

    // The first run→task exchange is the top-up request; deliver it twice.
    fx.task_transport.inject(ExchangeFault::DeliverTwice);
    fx.pump().await.expect("the duplicated top-up converges");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        run.budget.top_ups(),
        1,
        "a duplicated grant must not be credited twice"
    );
    assert_eq!(run.budget.loop_iterations(), 2);

    let task = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let escrow = &task.task().expect("the task is created").escrow;
    assert_eq!(
        escrow.consumed().loop_iterations,
        2,
        "the parent debited the top-up once despite the duplicate delivery"
    );
    assert_eq!(
        escrow.available(AgentBudgetDimension::LoopIterations),
        Some(2)
    );
}

#[tokio::test]
async fn an_effect_attempt_budget_is_reserved_up_front_and_denies_an_unaffordable_turn() {
    // Scenario 52's reservation clause: the effect and its attempts are reserved
    // before dispatch from the run's own ledger. A run escrowed a single effect
    // attempt takes exactly one model turn — the second turn's model effect
    // cannot be reserved — so it stops with a structured `effect-attempts`
    // exhaustion. The parent has nothing more to give (it escrowed its whole
    // attempt budget to the run), so the run fails rather than looping.
    let tight = AgentTaskDefinition::new(
        task_definition_id(),
        "A ticket whose run may make exactly one dispatch attempt.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_budgets(AgentBudgetCeilings {
        max_effect_attempts: Some(1),
        ..AgentBudgetCeilings::unbounded()
    });

    // The model never proposes, so the run would take a second turn — but the
    // second turn's model effect cannot reserve an attempt.
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_task_with(tight).await;
    fx.pump()
        .await
        .expect("the run stops when it cannot reserve a second attempt");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    let Some(rakka_agent::AgentRunTerminalReason::BudgetExhausted { exhaustion }) =
        run.terminal_reason
    else {
        panic!(
            "expected an effect-attempts exhaustion, got {:?}",
            run.terminal_reason
        );
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::EffectAttempts);
    assert_eq!(exhaustion.limit, 1);

    // Exactly one attempt reached the dispatcher, and the ledger consumed it.
    assert_eq!(run.budget.consumption().effect_attempts, 1);
    assert_eq!(run.budget.consumption().effects, 1);

    let task = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let escrow = &task.task().expect("the task is created").escrow;
    assert_eq!(escrow.consumed().effect_attempts, 1);
    assert_eq!(
        escrow.available(AgentBudgetDimension::EffectAttempts),
        Some(0)
    );
}

#[tokio::test]
async fn a_run_past_its_deadline_stops_without_asking_for_a_top_up() {
    // A wall-clock deadline is elapsed time, not a quantity a parent can grant.
    // A run that crosses it must stop with the structured reason — never park to
    // ask for more and re-park on a ceiling that would never move.
    let d1 = AgentTaskDefinition::new(
        task_definition_id(),
        "A ticket whose run is already past its deadline.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_budgets(AgentBudgetCeilings {
        // The deadline is the acceptance instant itself, so the run's first turn
        // is already past it.
        max_wall_clock_millis: Some(0),
        ..AgentBudgetCeilings::unbounded()
    });

    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task_with(d1).await;
    fx.pump()
        .await
        .expect("the run stops at its deadline without looping");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert!(
        run.pending_top_up.is_none(),
        "a deadline is not a quantity to ask a parent for"
    );
    assert_eq!(
        run.budget.top_ups(),
        0,
        "the run never asked for a top-up on a wall-clock ceiling"
    );
    let Some(rakka_agent::AgentRunTerminalReason::BudgetExhausted { exhaustion }) =
        run.terminal_reason
    else {
        panic!(
            "expected a wall-clock exhaustion, got {:?}",
            run.terminal_reason
        );
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::WallClock);
}

#[tokio::test]
async fn concurrent_run_allocations_cannot_oversubscribe_their_parent() {
    // The escrow ledger is a single writer, so concurrent allocation requests
    // serialize through it and none can grant budget the parent does not hold.
    // Ten runs race to claim four tokens each from a parent holding only ten;
    // whatever interleaving wins, the granted total never exceeds ten and the
    // parent's headroom never goes negative ([specification 9.7]; scenario 52's
    // no-oversubscription clause).
    use std::sync::{Arc, Mutex};

    let ledger = Arc::new(Mutex::new(rakka_agent::AgentEscrowLedger::new(
        rakka_agent::AgentBudgetGrant::new(
            AgentBudgetAllocation {
                tokens: Some(10),
                ..AgentBudgetAllocation::unbounded()
            },
            rakka_agent::AgentBudgetLimits::unbounded(),
        ),
    )));

    let mut handles = Vec::new();
    for index in 0..10u32 {
        let ledger = ledger.clone();
        handles.push(tokio::spawn(async move {
            let child = AgentEscrowChildId::new(format!("run-{index}")).expect("a valid child id");
            let request = AgentBudgetAllocation {
                tokens: Some(4),
                ..AgentBudgetAllocation::unbounded()
            };
            let mut guard = ledger.lock().expect("the ledger mutex is not poisoned");
            guard
                .open_child(child, &request)
                .expect("opening a child never errors")
                .tokens
                .unwrap_or(0)
        }));
    }

    let mut granted_total = 0u64;
    for handle in handles {
        granted_total += handle.await.expect("the task joins");
    }

    let guard = ledger.lock().expect("the ledger mutex is not poisoned");
    assert!(
        granted_total <= 10,
        "granted {granted_total} tokens from a parent holding only 10"
    );
    assert_eq!(
        guard.available(AgentBudgetDimension::Tokens),
        Some(0),
        "the parent is fully allocated and never oversubscribed"
    );
    // The outstanding children hold exactly what was granted — no more than the
    // parent ever had.
    let outstanding: u64 = guard
        .outstanding()
        .map(|(_, escrow)| escrow.allocated().tokens.unwrap_or(0))
        .sum();
    assert_eq!(outstanding, granted_total);
}

#[tokio::test]
async fn the_escrow_round_trip_survives_a_restart_at_every_durable_boundary() {
    // Scenario 52's restart clause, joined to scenario 61's replay clause: kill
    // the run's owner at each durable write of the whole escrow round trip —
    // allocation, reservation, settlement, and return — on both sides of the
    // compare-and-set, and re-drive from durable state alone. Every crash must
    // converge on the same escrow: the parent debited once, the run's consumption
    // settled once, and the remainder returned once. A re-driven settlement or
    // return is answered from the task's escrow record, so a restart never
    // double-debits or double-credits.
    let reference = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task_with(escrowed_definition()).await;
    reference
        .pump()
        .await
        .expect("the reference round trip completes");
    let writes = reference.runs.writes();
    assert!(
        writes >= 7,
        "the escrow round trip should make several durable writes, saw {writes}"
    );

    for point in [CrashPoint::AfterWrite, CrashPoint::BeforeWrite] {
        for nth in 1..=writes {
            let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
            fx.instantiate_agent().await;

            fx.runs.crash_at(nth, point);
            fx.create_task_with(escrowed_definition()).await;
            let _crashed = fx.pump().await;

            // A new owner activates and finds only what was durably committed.
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
                run.settlement,
                AgentRunSettlementStatus::Returned,
                "crash {point:?} at write {nth} should still hand its escrow back"
            );

            let task =
                load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
                    .await
                    .expect("the task state loads")
                    .expect("the task exists");
            let escrow = &task.task().expect("the task is created").escrow;

            // Exactly what the crash-free run consumed — never twice.
            let consumed = escrow.consumed();
            assert_eq!(
                (
                    consumed.loop_iterations,
                    consumed.model_calls,
                    consumed.effects,
                    consumed.effect_attempts,
                    consumed.tokens
                ),
                (1, 1, 1, 1, 15),
                "crash {point:?} at write {nth} settled the wrong consumption"
            );
            // The child returned exactly once, so the headroom is the full
            // allocation less only what the run spent — no double-credit.
            assert!(
                escrow.child(&run_child()).is_none(),
                "crash {point:?} at write {nth} left the escrow open"
            );
            assert_eq!(
                escrow.available(AgentBudgetDimension::LoopIterations),
                Some(9),
                "crash {point:?} at write {nth} double-credited the parent"
            );
            assert_eq!(escrow.available(AgentBudgetDimension::Tokens), Some(985));
        }
    }
}
