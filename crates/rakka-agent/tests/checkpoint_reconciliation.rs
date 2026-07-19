//! Run-side reconciliation checkpoints: the durable wait on an ambiguous
//! effect, and the full specification 12.5 decision set applied through it.
//!
//! Specification: sections 12.1, 12.2, and 12.5; scenarios 3 (the
//! reconciliation wait passivates and resumes), 11 (duplicate decisions do not
//! resume twice), and 57 (terminal cancellation only after the outcome is
//! resolved) of section 18. An effect whose outcome cannot be established
//! parks the run behind an `IndeterminateEffectReconciliation` checkpoint in
//! the same transition that recorded the ambiguity; every decision of the
//! specification 12.5 set — and only that set — resolves it.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    AgentApprovalDecision, AgentCheckpointDecision, AgentCheckpointKind, AgentCheckpointStatus,
    AgentCompensationRef, AgentEffectGeneration, AgentEffectPolicies, AgentEffectResolution,
    AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentReconciliationDecision, AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunStatus, AgentRunTerminalReason,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEffectId, AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "charge-card";
const COMPENSATION: &str = "refund-charge";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

fn tool_calling_turn() -> AgentModelTurn {
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

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn fixture() -> Fixture {
    let dispatcher = ScriptedDispatcher::new()
        .with_turn(tool_calling_turn())
        .with_turn(proposing_turn())
        .with_tool_result(
            TOOL,
            AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                .expect("the tool result is inline-bounded"),
        )
        .with_compensation_result(
            COMPENSATION,
            AgentTaskContent::inline(serde_json::json!({ "refunded": true }))
                .expect("the compensation result is inline-bounded"),
        );
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(tool_id(), AgentEffectSpec::non_idempotent())
        .expect("the tool spec is valid");
    Fixture::with_sink(
        dispatcher,
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

fn resolver() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "operator".to_string(),
        display_name: None,
    }
}

fn decision_key(discriminator: &str) -> AgentOperationId {
    AgentOperationId::for_agent(
        AgentOperationKind::CheckpointResolution,
        &agent_scope(),
        discriminator,
    )
    .expect("the decision key derives")
}

fn reconcile(decision: AgentReconciliationDecision) -> Box<AgentCheckpointDecision> {
    Box::new(AgentCheckpointDecision::Reconciliation(decision))
}

fn confirmed_completed() -> Box<AgentCheckpointDecision> {
    reconcile(AgentReconciliationDecision::ConfirmedCompleted {
        resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(AgentRunEffectOutcome::Tool {
                call_id: AgentToolCallId::new("call-1").expect("call id is valid"),
                content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-9" }))
                    .expect("the content is inline-bounded"),
            }),
        }),
    })
}

/// Drives the run until its non-idempotent tool attempt is reported ambiguous,
/// and returns the parked effect and the reconciliation checkpoint gating it.
async fn park_indeterminate(
    fx: &Fixture,
) -> (AgentEffectId, AgentEffectGeneration, HumanCheckpointId) {
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Answer only the model call, so the tool effect is dispatched and still
    // outstanding when the ambiguous loss is reported.
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
            effect_id: effect_id.clone(),
            generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(AgentRunEffectOutcome::Indeterminate {
                code: "connection-lost".to_string(),
                message: "the worker died mid-invocation".to_string(),
            }),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");

    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        snapshot.status,
        AgentRunStatus::WaitingForReconciliation,
        "the ambiguous effect parks the run in reconciliation"
    );
    assert!(!snapshot.status.is_terminal());

    // A fresh store recovery is a passivation/reactivation cycle (scenario 3):
    // the checkpoint is durable state, not a live task.
    let mut store = fx.run();
    store.recover(fx.now()).await.expect("the run recovers");
    let open = store
        .state()
        .expect("state reads")
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .to_vec();
    assert_eq!(open.len(), 1, "exactly one checkpoint gates the ambiguity");
    assert_eq!(
        open[0].kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        "the wait carries the full reconciliation checkpoint record"
    );
    assert_eq!(open[0].bound_effect.effect_id, effect_id);
    assert_eq!(open[0].bound_effect.generation, generation);
    (effect_id, generation, open[0].checkpoint_id.clone())
}

fn resolve_command(
    checkpoint_id: HumanCheckpointId,
    key: &str,
    decision: Box<AgentCheckpointDecision>,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: decision_key(key),
        checkpoint_id,
        resolver: resolver(),
        decision,
    }
}

#[tokio::test]
async fn a_confirmed_completed_decision_through_the_checkpoint_resumes_the_run() {
    // Scenario 3, reconciliation wait: the parked run resumes on the decision
    // command, records the established outcome without re-invoking, and
    // completes.
    let fx = fixture();
    let (_effect_id, _generation, checkpoint_id) = park_indeterminate(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .apply(
            resolve_command(checkpoint_id, "d1", confirmed_completed()),
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the reconciliation applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));

    fx.pump().await.expect("the resumed run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        fx.dispatcher.tool_calls(),
        0,
        "the established outcome was recorded without re-invocation"
    );
}

#[tokio::test]
async fn a_confirmed_not_executed_decision_mints_a_new_generation_once() {
    // Specification 12.5: `ConfirmedNotExecuted` is the one decision that
    // authorizes a new effect generation. Scenario 11: its replay is
    // deduplicated and does not mint a second one.
    let fx = fixture();
    let (effect_id, generation, checkpoint_id) = park_indeterminate(&fx).await;
    assert_eq!(generation, AgentEffectGeneration::FIRST);

    let command = |cp| {
        resolve_command(
            cp,
            "d1",
            reconcile(AgentReconciliationDecision::ConfirmedNotExecuted),
        )
    };
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .apply(command(checkpoint_id.clone()), &fx.router, fx.now())
        .await
        .expect("the reconciliation applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));

    let replay = run
        .apply(command(checkpoint_id), &fx.router, fx.now())
        .await
        .expect("the replay is accepted");
    assert!(matches!(replay, AgentRunEntityReply::Duplicate { .. }));

    let held = {
        let state = run.state().expect("state reads");
        let loop_state = state.loop_state().expect("the loop exists");
        loop_state
            .effects()
            .iter()
            .find(|effect| effect.effect_id == effect_id)
            .expect("the effect is held")
            .generation
    };
    assert_eq!(
        held,
        generation.next(),
        "exactly one new generation was authorized"
    );

    // The new generation is a fresh dispatchable intent; the scripted tool
    // result answers it and the run completes.
    fx.pump().await.expect("the re-invoked run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn a_compensate_decision_schedules_the_compensation_and_settles_behind_it() {
    // Specification 12.5: `Compensate` schedules an explicitly defined
    // compensation effect. The ambiguous generation settles as compensated,
    // the compensation dispatches even though the run is winding down, and the
    // run becomes terminal only after the compensation's own outcome arrives.
    let fx = fixture();
    let (effect_id, _generation, checkpoint_id) = park_indeterminate(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(
            checkpoint_id,
            "d1",
            reconcile(AgentReconciliationDecision::Compensate {
                compensation: AgentCompensationRef::new(COMPENSATION)
                    .expect("the compensation ref is valid"),
            }),
        ),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the compensation decision applies");

    // Nonterminal while the compensation is outstanding; the ambiguous
    // generation is settled as compensated; the compensation reached the sink
    // despite the wind-down fence.
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert!(
        !snapshot.status.is_terminal(),
        "the run settles only after the compensation's outcome"
    );
    {
        let mut store = fx.run();
        store.recover(fx.now()).await.expect("the run recovers");
        let state = store.state().expect("state reads");
        let loop_state = state.loop_state().expect("the loop exists");
        let original = loop_state
            .effects()
            .iter()
            .find(|effect| effect.effect_id == effect_id)
            .expect("the compensated effect is held");
        assert_eq!(original.status, AgentRunEffectStatus::Compensated);
        let compensation = loop_state
            .effects()
            .iter()
            .find(|effect| effect.kind() == AgentRunEffectKind::CompensationCall)
            .expect("the compensation effect is committed");
        assert_eq!(compensation.status, AgentRunEffectStatus::Ready);
    }
    assert_eq!(
        fx.dispatched_effects(),
        3,
        "model, tool, and compensation all reached the sink"
    );

    // The scripted compensation result settles the run terminally, under the
    // compensation reason.
    fx.pump().await.expect("the compensation settles the run");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert!(run.status.is_terminal());
    assert_eq!(
        run.terminal_reason
            .as_ref()
            .expect("the terminal reason is recorded")
            .code(),
        "effect-compensated"
    );
}

#[tokio::test]
async fn an_abandon_and_fail_decision_fails_the_run_terminally() {
    let fx = fixture();
    let (_effect_id, _generation, checkpoint_id) = park_indeterminate(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(
            checkpoint_id,
            "d1",
            reconcile(AgentReconciliationDecision::AbandonAndFail),
        ),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the abandonment applies");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, .. }) => {
            assert_eq!(code, "reconciliation-abandoned");
        }
        other => panic!("expected an abandoned-effect failure, found {other:?}"),
    }
}

#[tokio::test]
async fn an_escalate_decision_keeps_the_wait_nonterminal_until_a_resolving_decision() {
    // Specification 12.5: `Escalate` is not a resolution — the checkpoint
    // stays open, the run stays parked, and a later decision with a new key
    // still resolves it.
    let fx = fixture();
    let (_effect_id, _generation, checkpoint_id) = park_indeterminate(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(
            checkpoint_id.clone(),
            "d1",
            reconcile(AgentReconciliationDecision::Escalate),
        ),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the escalation applies");

    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, AgentRunStatus::WaitingForReconciliation);
    {
        let mut store = fx.run();
        store.recover(fx.now()).await.expect("the run recovers");
        let state = store.state().expect("state reads");
        let open = state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        assert_eq!(open.len(), 1, "the escalated checkpoint is still open");
        assert_eq!(open[0].status, AgentCheckpointStatus::Escalated);
    }

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(checkpoint_id, "d2", confirmed_completed()),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the later resolving decision applies");
    fx.pump().await.expect("the resumed run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn cancellation_leaves_the_reconciliation_checkpoint_resolvable() {
    // Scenario 57, checkpoint half: cancellation fences new work but does not
    // make an unknown outcome known — the reconciliation checkpoint survives
    // the cancellation, and terminal cancellation projects only after its
    // decision resolves the ambiguity.
    let fx = fixture();
    let (_effect_id, _generation, checkpoint_id) = park_indeterminate(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "cancel-1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("cancellation applies");

    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        snapshot.status,
        AgentRunStatus::WaitingForReconciliation,
        "the cancelled run stays nonterminal in reconciliation"
    );
    {
        let mut store = fx.run();
        store.recover(fx.now()).await.expect("the run recovers");
        let state = store.state().expect("state reads");
        let open = state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        assert_eq!(open.len(), 1);
        assert!(
            open[0].status.is_waiting(),
            "cancellation must not cancel the reconciliation checkpoint"
        );
    }

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(checkpoint_id, "d1", confirmed_completed()),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the resolution applies");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::Cancelled,
        "terminal cancellation projects only after the outcome is resolved"
    );
}

#[tokio::test]
async fn a_confirmed_not_executed_gated_effect_reparks_behind_a_fresh_approval() {
    // The grant that authorized the ambiguous generation binds that generation
    // exactly (specification 12.3), so `ConfirmedNotExecuted` on a
    // checkpoint-required effect must park the new generation behind a fresh
    // approval checkpoint — not leave it undispatchable in `WaitingForEffect`
    // with no wait left to resolve.
    let dispatcher = ScriptedDispatcher::new()
        .with_turn(tool_calling_turn())
        .with_turn(proposing_turn())
        .with_tool_result(
            TOOL,
            AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                .expect("the tool result is inline-bounded"),
        );
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            tool_id(),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    let fx = Fixture::with_sink(
        dispatcher,
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    );

    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the approval");

    let first_approval = {
        let mut store = fx.run();
        store.recover(fx.now()).await.expect("the run recovers");
        let state = store.state().expect("state reads");
        let open = state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        assert_eq!(open.len(), 1, "the gated tool parks behind one approval");
        assert_eq!(open[0].kind, AgentCheckpointKind::Approval);
        open[0].checkpoint_id.clone()
    };

    let approve = || {
        Box::new(AgentCheckpointDecision::Approval(
            AgentApprovalDecision::Approve {
                credential_binding: None,
                expires_at: AgentTimestampMillis::new(1_000_000),
                allowed_use_count: 1,
            },
        ))
    };
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(first_approval.clone(), "a1", approve()),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the first approval applies");

    // The approved dispatch is reported ambiguous.
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
            effect_id: effect_id.clone(),
            generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(AgentRunEffectOutcome::Indeterminate {
                code: "connection-lost".to_string(),
                message: "the worker died mid-invocation".to_string(),
            }),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");

    let reconciliation = {
        let state = run.state().expect("state reads");
        let open = state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        open.iter()
            .find(|checkpoint| {
                checkpoint.kind == AgentCheckpointKind::IndeterminateEffectReconciliation
            })
            .expect("the reconciliation checkpoint gates the ambiguity")
            .checkpoint_id
            .clone()
    };
    run.apply(
        resolve_command(
            reconciliation,
            "d1",
            reconcile(AgentReconciliationDecision::ConfirmedNotExecuted),
        ),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the reconciliation applies");

    // The new generation re-parks behind a fresh approval checkpoint bound to
    // it, rather than stranding undispatchable.
    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        snapshot.status,
        AgentRunStatus::WaitingForApproval,
        "the redispatched gated generation parks for a fresh approval"
    );
    let second_approval = {
        let mut store = fx.run();
        store.recover(fx.now()).await.expect("the run recovers");
        let state = store.state().expect("state reads");
        let open = state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        assert_eq!(open.len(), 1, "exactly one fresh checkpoint gates it");
        assert_eq!(open[0].kind, AgentCheckpointKind::Approval);
        assert_eq!(open[0].bound_effect.effect_id, effect_id);
        assert_eq!(
            open[0].bound_effect.generation,
            generation.next(),
            "the fresh checkpoint binds the new generation"
        );
        assert_ne!(
            open[0].checkpoint_id, first_approval,
            "the fresh checkpoint is a new record, not the resolved one"
        );
        open[0].checkpoint_id.clone()
    };

    // The fresh approval resumes the run and the re-invocation completes.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(second_approval, "a2", approve()),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the second approval applies");
    fx.pump().await.expect("the re-invoked run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}
