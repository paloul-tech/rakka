//! The terminal transition resolves a still-unresolved handoff.
//!
//! The wedge this pins ([specification 8.9](../../docs/plans/rakka-agent/spec.md)'s
//! single-attempt rule, terminal arm): a transfer mints its offer, the target
//! run durably accepts run-side, but the task-side acceptance settle is still
//! in flight when the target's result proposal lands. The result-acceptance
//! fence checks generation and run — an `Offered` assignment passes — so the
//! task terminalizes `Completed` with the handoff still `Initiated`. Every
//! handoff-result derivation gates on a settled provenance, so without the
//! terminal-side resolution the owed `HandoffResult` never derives and the
//! fenced source run is stranded forever, with no converging re-drive. The
//! terminalization now settles the provenance from durable facts — a result
//! accepted from the transfer's own minted generation is the target durably
//! holding responsibility, so the transfer settles `Accepted` — and the
//! ordinary courier machinery carries the source to `HandedOff`.

mod common;

use common::{
    goal_spec_draft, goal_spec_with_handoff, goal_task_creation_command, handoff_config,
    handoff_target_run_scope, handoff_target_scope, handoff_tool_id, schema, task_definition,
    task_scope, ApplyingHandoffExecutor, Fixture, HANDOFF_SKILL,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    proposal_operation_id, AgentAssignmentGeneration, AgentAssignmentStatus, AgentEntityAddress,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentExchangeRouter,
    AgentModelTurn, AgentRunEffectKind, AgentRunStatus, AgentTaskContent, AgentTaskEntityStore,
    AgentTaskHandoffStatus, AgentTaskResultProposal, AgentTaskStatus, AgentToolCallId,
    AgentToolCallRequest, AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentCausationId, AgentCorrelationId};
use serde_json::json;

fn handoff_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                handoff_tool_id(),
                json!({ "skill": HANDOFF_SKILL, "reason": "needs billing authority" }),
            )
            .expect("the tool call is bounded"),
        )
}

/// The transfer's task definition, `handoff_record.rs`'s shape: a bounded
/// per-run allocation so the target's generation is affordable beside the
/// source's still-open escrow child.
fn handoff_task_definition() -> rakka_agent::AgentTaskDefinition {
    let mut per_run = rakka_agent::AgentBudgetAllocation::unbounded();
    per_run.set(rakka_agent::AgentBudgetDimension::LoopIterations, Some(3));
    task_definition()
        .with_budgets(rakka_agent::AgentBudgetCeilings {
            max_loop_iterations: Some(12),
            ..rakka_agent::AgentBudgetCeilings::unbounded()
        })
        .with_run_allocation(per_run)
}

/// A result accepted from the still-`Offered` handoff generation settles the
/// transfer at the terminal boundary, and the source run converges to
/// `HandedOff` through the ordinary courier — instead of staying fenced
/// forever behind an `Initiated` provenance no derivation can act on.
#[tokio::test]
async fn a_result_from_the_offered_handoff_generation_still_resolves_the_source() {
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(handoff_turn()),
    ))
    .with_delegation(handoff_config());
    let executor = ApplyingHandoffExecutor::over(&fixture);
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(executor.clone());
    fixture.instantiate_agent().await;
    fixture.instantiate_agent_at(handoff_target_scope()).await;
    fixture
        .apply_task_command(goal_task_creation_command(
            handoff_task_definition(),
            goal_spec_draft(goal_spec_with_handoff(), true),
        ))
        .await
        .expect("the goal task creates");

    // Drive the source run until its handoff record and send effect commit,
    // without answering the send — the `handoff_record.rs` loop.
    for _ in 0..8 {
        fixture
            .settle_task_at(&task_scope())
            .await
            .expect("the task settles");
        let now = fixture.now();
        let mut run = fixture.run();
        run.recover(now).await.expect("the run recovers");
        run.settle_side_effects(&fixture.router, now)
            .await
            .expect("the run settles");
        let outstanding = run
            .state()
            .expect("the run state reads")
            .loop_state()
            .map(|loop_state| {
                loop_state.effects().iter().any(|effect| {
                    effect.is_outstanding() && effect.kind() == AgentRunEffectKind::A2aSendCall
                })
            })
            .unwrap_or(false);
        if outstanding {
            break;
        }
        let _ = fixture
            .dispatcher
            .drive(&mut run, &fixture.router, fixture.now())
            .await
            .expect("the dispatcher drives");
    }

    // Answer the send once: the executor applies `RecordHandoff` to the task
    // with an empty router, so the transfer commits but nothing delivers.
    {
        let now = fixture.now();
        let mut run = fixture.run();
        run.recover(now).await.expect("the run recovers");
        let answered = fixture
            .dispatcher
            .drive(&mut run, &fixture.router, fixture.now())
            .await
            .expect("the send answers");
        assert!(answered >= 1, "the handoff send was answered");
    }

    // Mint the target's offer without ever delivering it: settle the task
    // over a routeless courier, so the assignment stays durably `Offered` —
    // the acceptance-reply-in-flight window, held open.
    let unrouted = AgentExchangeRouter::new();
    let mut task = AgentTaskEntityStore::new(
        task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    for _ in 0..4 {
        let now = fixture.now();
        task.recover(now).await.expect("the task recovers");
        let _ = task.settle_side_effects(&unrouted, now).await;
    }
    let snapshot = fixture.task_snapshot().await;
    let assignment = snapshot.assignment.as_ref().expect("the offer minted");
    assert_eq!(assignment.generation, AgentAssignmentGeneration::new(2));
    assert_eq!(
        assignment.status,
        AgentAssignmentStatus::Offered,
        "the acceptance reply is still in flight"
    );
    let handoff = snapshot.handoff.as_deref().expect("the transfer recorded");
    assert_eq!(handoff.status, AgentTaskHandoffStatus::Initiated);
    assert_eq!(
        handoff.target_generation,
        Some(AgentAssignmentGeneration::new(2))
    );
    let handoff_id = handoff.handoff.clone();

    // The target run — durably accepted run-side in the raced world —
    // proposes its result while the task still shows `Offered`. The fence
    // checks generation and run, so the proposal is validated and the task
    // terminalizes `Completed`.
    let target_run = handoff_target_run_scope();
    let proposal_id = proposal_operation_id(&target_run, 1).expect("the proposal id derives");
    let proposal = AgentTaskResultProposal {
        proposal_id: proposal_id.clone(),
        agent: target_run.agent().clone(),
        run: target_run.run().clone(),
        generation: AgentAssignmentGeneration::new(2),
        definition_id: snapshot.definition_id.clone(),
        definition_version: snapshot.definition_version,
        result_schema: schema("ticket-result"),
        content: AgentTaskContent::inline(json!({ "answer": "resolved" }))
            .expect("the result is inline-bounded"),
        evidence: Vec::new(),
        causation_id: AgentCausationId::new(proposal_id.as_str()),
        proposed_at: fixture.now(),
    };
    let envelope = AgentExchangeEnvelope::new(
        proposal_id.clone(),
        AgentExchangeKind::ResultProposal,
        AgentEntityAddress::Run(target_run.clone()),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE, &proposal)
            .expect("the proposal encodes"),
        AgentCorrelationId::new(proposal_id.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid");
    let now = fixture.now();
    task.recover(now).await.expect("the task recovers");
    let outcome = task
        .accept(&envelope, &unrouted, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        outcome.result().is_accepted(),
        "the offered-generation proposal is validated"
    );
    drop(task);

    let snapshot = fixture.task_snapshot().await;
    assert_eq!(snapshot.status, AgentTaskStatus::Completed);
    let handoff = snapshot.handoff.as_deref().expect("the provenance rides");
    assert_eq!(
        handoff.status,
        AgentTaskHandoffStatus::Accepted,
        "a result from the transfer's own minted generation is the target \
         durably holding responsibility, settled at the terminal boundary"
    );
    assert_eq!(handoff.handoff, handoff_id);

    // The ordinary courier now converges the source: the settled provenance
    // derives the owed `HandoffResult`, the source's cell resolves, and the
    // run terminalizes `HandedOff` — the wedge this test exists to refute.
    for _ in 0..8 {
        let _ = fixture.settle_task_at(&task_scope()).await;
    }
    let mut run = fixture.run();
    run.recover(fixture.now())
        .await
        .expect("the source recovers");
    let source_status = run.state().expect("the source state reads").status();
    assert_eq!(
        source_status,
        Some(AgentRunStatus::HandedOff),
        "the fenced source un-fences through the terminal-settled provenance"
    );
    drop(run);

    let snapshot = fixture.task_snapshot().await;
    let handoff = snapshot.handoff.as_deref().expect("the provenance rides");
    assert!(
        handoff.result_settled,
        "the handoff-result exchange settled, quiescing the derivation"
    );
}
