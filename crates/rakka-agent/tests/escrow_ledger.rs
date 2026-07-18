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

use rakka_agent::testkit::{ExchangeFault, ScriptedDispatcher};
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
    // iteration and one model call, and the turn billed 15 tokens.
    let consumed = escrow.consumed();
    assert_eq!(consumed.loop_iterations, 1);
    assert_eq!(consumed.model_calls, 1);
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
