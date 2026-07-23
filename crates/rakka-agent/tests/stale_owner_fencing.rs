//! Stale owner writes are rejected by the revision fence.
//!
//! Specification: section 15 ("Recovery MUST reacquire the latest revision
//! and reject stale owner writes"); scenario 4 of section 18. Shard movement
//! is proven at the store level, because the fence *is* the compare-and-set
//! revision: after a movement, the old owner's cached record is behind the
//! new owner's committed one, and its next persisting write must be rejected
//! — never clobber the revision it never saw — after which the rejection
//! drops the stale cache and the reloaded owner reconciles with the
//! authoritative record. The transport half of movement — an exchange
//! converging across a real 2-node ownership move — is scenario 60's proof
//! in `choreography_cluster.rs`; together they cover the spec 15 clause. The
//! agent entity's own variant of this fence is proven in `agent_entity.rs`.

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    AgentModelTurn, AgentModelUsage, AgentOperationId, AgentOperationKind, AgentRunEntityCommand,
    AgentRunEntityReply, AgentRunStatus, AgentTaskContent, AgentTaskCreation,
    AgentTaskEntityCommand, AgentTaskEntityReply, AgentTaskEntityStore, AgentTaskHistoryCursor,
    AgentTaskHistoryKind, AgentTaskHistoryStore, AgentTaskStatus,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
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

async fn history_count(fx: &Fixture, kind: AgentTaskHistoryKind) -> usize {
    let mut count = 0;
    let mut cursor = Some(AgentTaskHistoryCursor::start());
    while let Some(position) = cursor {
        let page = AgentTaskHistoryStore::read(&fx.history, &task_scope(), position)
            .await
            .expect("the history reads");
        count += page
            .entries
            .iter()
            .filter(|entry| entry.kind == kind)
            .count();
        cursor = page.next;
    }
    count
}

#[tokio::test]
async fn a_stale_run_owner_write_is_rejected_and_the_new_owners_progress_survives() {
    // Scenario 4, the run's side. Owner A materializes the cranked run —
    // model effect outstanding — and caches that revision. The shard moves:
    // owner B activates and records the model completion, advancing the
    // revision. A, fenced out but unaware, then applies the same completion
    // from its stale cache — the window where an old owner would take the
    // turn a second time if the fence did not hold.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Crank to the model wait, then capture the outstanding effect and the
    // completion the dispatcher will deliver.
    let mut owner_a = fx.run();
    owner_a.recover(fx.now()).await.expect("owner A recovers");
    owner_a
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the loop cranks to its model wait");
    assert_eq!(fx.dispatched_effects(), 1, "the model effect dispatched");
    let effect = owner_a
        .state()
        .expect("state reads")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .first()
        .expect("the model effect is outstanding")
        .clone();
    let outcome = fx.dispatcher.answer(&effect).await;
    let completion = || AgentRunEntityCommand::RecordEffectResult {
        operation_id: effect
            .result_operation_id(&run_scope())
            .expect("the result operation id derives"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        attempt: 1,
        fence: 0,
        outcome: Box::new(outcome.clone()),
    };

    // Owner A re-recovers here — this is its cached revision from before the
    // movement. Owner B then activates and records the completion.
    owner_a
        .recover(fx.now())
        .await
        .expect("owner A caches the pre-movement revision");
    let mut owner_b = fx.run();
    owner_b.recover(fx.now()).await.expect("owner B recovers");
    let applied = owner_b
        .apply(completion(), &fx.router, fx.now())
        .await
        .expect("owner B records the completion");
    let AgentRunEntityReply::Applied {
        outcome: authoritative,
    } = applied
    else {
        panic!("owner B's completion applies, got {applied:?}");
    };

    // A's write lands behind B's committed revision: rejected, not applied —
    // the turn is not taken twice.
    let error = owner_a
        .apply(completion(), &fx.router, fx.now())
        .await
        .expect_err("a stale owner's write must be rejected");
    assert_eq!(error.code(), "revision-conflict");

    // The rejection dropped A's cache. Reloaded, A is answered from the
    // record: the same completion is a duplicate, never a second transition.
    owner_a
        .recover(fx.now())
        .await
        .expect("owner A reacquires the latest revision");
    let reply = owner_a
        .apply(completion(), &fx.router, fx.now())
        .await
        .expect("the redelivered completion is absorbed");
    let AgentRunEntityReply::Duplicate { outcome } = reply else {
        panic!("the reloaded owner answers from the record, got {reply:?}");
    };
    assert_eq!(
        outcome, authoritative,
        "the duplicate must carry the outcome the authoritative owner produced"
    );

    // The flow converges normally: one turn, one model call, one effect.
    fx.pump().await.expect("the run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 1, "the turn was taken exactly once");
    assert_eq!(run.budget.model_calls(), 1);
    assert_eq!(fx.dispatched_effects(), 1);
}

#[tokio::test]
async fn a_stale_task_owner_write_is_rejected_and_answered_from_the_authoritative_record() {
    // Scenario 4, the task's side. Owner A materializes the not-yet-created
    // task. The shard moves: owner B activates and commits the creation. The
    // ingress then redelivers the same creation to A, whose cache still says
    // the task does not exist — its write must be fenced, and its reloaded
    // retry must be answered from the record as a duplicate, never a second
    // task.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent().await;

    let mut owner_a = AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    owner_a
        .recover(fx.now())
        .await
        .expect("owner A recovers the empty task");

    let creation = || AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(AgentOperationKind::TaskCreation, [TENANT, TASK, "1"])
            .expect("operation id should be derivable"),
        creation: Box::new(AgentTaskCreation {
            definition: task_definition(),
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
            goal: None,
            parent: None,
            dependencies: Vec::new(),
            telemetry: Default::default(),
        }),
    };

    // Owner B: a fresh entity over the same store — exactly a new owner
    // after a movement — commits the creation.
    let mut owner_b = AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    owner_b.recover(fx.now()).await.expect("owner B recovers");
    let applied = owner_b
        .apply(creation(), &fx.router, fx.now())
        .await
        .expect("owner B commits the creation");
    let AgentTaskEntityReply::Applied {
        outcome: authoritative,
    } = applied
    else {
        panic!("owner B's creation applies, got {applied:?}");
    };

    let error = owner_a
        .apply(creation(), &fx.router, fx.now())
        .await
        .expect_err("a stale owner's creation write must be rejected");
    assert_eq!(error.code(), "revision-conflict");

    // Reloaded, the same redelivery is deduplicated against B's record.
    owner_a
        .recover(fx.now())
        .await
        .expect("owner A reacquires the latest revision");
    let reply = owner_a
        .apply(creation(), &fx.router, fx.now())
        .await
        .expect("the redelivered creation is absorbed");
    let AgentTaskEntityReply::Duplicate { outcome } = reply else {
        panic!("the reloaded owner answers from the record, got {reply:?}");
    };
    assert_eq!(
        outcome, authoritative,
        "the duplicate must carry the outcome the authoritative owner produced"
    );

    // One task, one creation row, one assignment — and the flow converges.
    assert_eq!(history_count(&fx, AgentTaskHistoryKind::Created).await, 1);
    fx.pump().await.expect("the run completes");
    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);
    assert_eq!(task.assignment_generation.get(), 1);
    assert_eq!(
        history_count(&fx, AgentTaskHistoryKind::AssignmentDecided).await,
        1
    );
}
