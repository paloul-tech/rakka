//! A run's decisions answer a replay `Duplicate` after the operation window
//! has moved past them (issue #96, ask 2).
//!
//! The run remembers its last 64 operation ids, and ordinary traffic — every
//! effect result — fills that window. A checkpoint resolution or an
//! indeterminate effect's resolution is also recorded in the run's decision
//! ledger, which only decisions write, so a client that lost the answer to a
//! committed decision and re-posts it later is answered with the original
//! outcome rather than the refusal a first attempt would meet.
//!
//! Each test commits a decision, then rewrites the stored record as if 64
//! later operations had evicted its id ([`forget_operations`]).

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentApprovalDecision, AgentCheckpointDecision, AgentDispatchWindow, AgentEffectPolicies,
    AgentEffectResolution, AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply, AgentRunOutcome,
    AgentRunStatus, AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_call() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me charge that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id is valid"),
                AgentToolId::new(TOOL).expect("the tool id is valid"),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposal() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A run whose one tool needs an approval before it dispatches.
fn gated() -> Fixture {
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new(TOOL).expect("the tool id is valid"),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(tool_call())
            .with_turn(proposal())
            .with_tool_result(
                TOOL,
                AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                    .expect("the tool result is inline-bounded"),
            ),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

fn decision_key(discriminator: &str) -> AgentOperationId {
    AgentOperationId::for_agent(
        AgentOperationKind::CheckpointResolution,
        &agent_scope(),
        discriminator,
    )
    .expect("the decision key derives")
}

fn resolve(
    checkpoint_id: &HumanCheckpointId,
    key: &str,
    decision: AgentCheckpointDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: decision_key(key),
        checkpoint_id: checkpoint_id.clone(),
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "approver".to_string(),
            display_name: None,
        },
        decision: Box::new(decision),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

fn approve() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
        credential_binding: None,
        expires_at: AgentTimestampMillis::new(1_000_000),
        allowed_use_count: 1,
    })
}

fn deny() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Deny {
        reason: "not this time".to_string(),
    })
}

/// The id of the one checkpoint the run is parked on, read from durable state.
async fn open_checkpoint(fx: &Fixture) -> HumanCheckpointId {
    let mut store = fx.run();
    store.recover(fx.now()).await.expect("the run recovers");
    let open = store
        .state()
        .expect("state reads")
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .to_vec();
    assert_eq!(open.len(), 1, "exactly one checkpoint gates the tool");
    open[0].checkpoint_id.clone()
}

async fn park(fx: &Fixture) -> HumanCheckpointId {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");
    open_checkpoint(fx).await
}

/// Applies `command` through a fresh facade and returns the outcome it
/// committed.
async fn apply_fresh(fx: &Fixture, command: AgentRunEntityCommand) -> AgentRunOutcome {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    match run
        .apply(command, &fx.router, fx.now())
        .await
        .expect("the command applies")
    {
        AgentRunEntityReply::Applied { outcome } => outcome,
        other => panic!("expected the command to apply, got {other:?}"),
    }
}

async fn replay_fresh(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> rakka_agent::AgentRunResult<AgentRunEntityReply> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(command, &fx.router, fx.now()).await
}

#[tokio::test]
async fn a_replayed_approval_answers_duplicate_after_the_window_has_moved_past_it() {
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let undecided = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        serde_json::to_value(run.state().expect("state reads")).expect("the state encodes")
    };
    assert!(
        undecided.get("decision_ledger").is_none(),
        "a run that has decided nothing encodes exactly as before the ledger"
    );

    let approval = resolve(&checkpoint_id, "approve", approve());
    assert!(approval.records_decision());
    let outcome = apply_fresh(&fx, approval.clone()).await;
    fx.pump().await.expect("the approved run completes");
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = replay_fresh(&fx, approval)
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let state = run.state().expect("state reads");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    assert_eq!(
        state.decision_ledger().len(),
        1,
        "the effect results the run went on to record are not decisions"
    );
}

#[tokio::test]
async fn a_replayed_denial_that_ended_the_run_answers_duplicate_rather_than_run_terminal() {
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let denial = resolve(&checkpoint_id, "deny", deny());
    let outcome = apply_fresh(&fx, denial.clone()).await;
    fx.pump().await.expect("the denied run winds down");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Failed
    );
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = replay_fresh(&fx, denial).await.expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[tokio::test]
async fn a_different_body_under_a_decided_id_answers_the_original_outcome() {
    // The window keys on the id alone, and so does the ledger: a Deny
    // re-posted under the approval's id is that approval's replay.
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let outcome = apply_fresh(&fx, resolve(&checkpoint_id, "approve", approve())).await;
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = replay_fresh(&fx, resolve(&checkpoint_id, "approve", deny()))
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[tokio::test]
async fn a_decision_the_ledger_no_longer_holds_meets_the_refusal_a_first_attempt_would() {
    // A record written before the ledger existed, or one whose decision 32
    // later decisions evicted: the replay is refused exactly as before.
    let fx = gated();
    let checkpoint_id = park(&fx).await;
    let approval = resolve(&checkpoint_id, "approve", approve());
    apply_fresh(&fx, approval.clone()).await;
    forget_operations(&fx.runs, &run_scope().persistence_id(), true).await;

    let refusal = replay_fresh(&fx, approval)
        .await
        .expect_err("the replay is refused");
    assert_eq!(refusal.code(), "run-unknown-checkpoint");
}

#[tokio::test]
async fn a_resident_facade_answers_a_replay_from_the_ledger_after_its_one_re_read() {
    // The resident facade cached the run before it parked, so its cache has
    // never seen the checkpoint: it refuses the replay, re-reads once, and the
    // durable record's ledger answers.
    let fx = gated();
    fx.instantiate_agent().await;
    fx.create_task().await;
    let mut resident = fx.run();
    resident.recover(fx.now()).await.expect("the run recovers");

    fx.pump().await.expect("the loop parks on the checkpoint");
    let checkpoint_id = open_checkpoint(&fx).await;
    let approval = resolve(&checkpoint_id, "approve", approve());
    let outcome = apply_fresh(&fx, approval.clone()).await;
    forget_operations(&fx.runs, &run_scope().persistence_id(), false).await;

    let replay = resident
        .apply(approval, &fx.router, fx.now())
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[tokio::test]
async fn a_replayed_indeterminate_resolution_answers_duplicate_after_the_window_has_moved_past_it()
{
    let spec = AgentEffectSpec::non_idempotent();
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_call())
            .with_turn_for(2, proposal()),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    // The worker dies after invoking, so the attempt is ambiguous and the
    // non-idempotent generation parks for reconciliation.
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _died = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    fx.expire_lease();
    fx.pump().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    assert_eq!(effect.status, AgentRunEffectStatus::Indeterminate);

    let resolution = AgentRunEntityCommand::ResolveIndeterminateEffect {
        operation_id: decision_key("reconcile"),
        effect_id: effect.effect_id.clone(),
        generation: effect.generation,
        resolution: Box::new(AgentEffectResolution::ConfirmedNotExecuted),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    };
    assert!(resolution.records_decision());
    let outcome = {
        let mut run = fx.fx.run();
        run.recover(fx.fx.now()).await.expect("the run recovers");
        match run
            .apply(resolution.clone(), &fx.fx.router, fx.fx.now())
            .await
            .expect("the resolution applies")
        {
            AgentRunEntityReply::Applied { outcome } => outcome,
            other => panic!("expected the resolution to apply, got {other:?}"),
        }
    };
    fx.pump().await;
    forget_operations(&fx.fx.runs, &run_scope().persistence_id(), false).await;

    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    let replay = run
        .apply(resolution, &fx.fx.router, fx.fx.now())
        .await
        .expect("the replay answers");
    assert_eq!(replay, AgentRunEntityReply::Duplicate { outcome });
}

#[test]
fn only_resolutions_are_run_decisions() {
    assert!(!AgentRunEntityCommand::Describe.records_decision());
    assert!(!AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: decision_key("timers"),
    }
    .records_decision());
    assert!(!AgentRunEntityCommand::FireFanInDeadline {
        operation_id: decision_key("deadline"),
    }
    .records_decision());
}
