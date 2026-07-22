//! Run-side checkpoint park/resume and durable SLA timers.
//!
//! Specification: section 12; scenarios 3 and 11 of section 18. A run that wants
//! to dispatch a checkpoint-required tool opens an approval checkpoint and parks
//! — passivated, no live task — rather than dispatching. A decision resumes it:
//! an approval stores the digest-bound grant and the effect dispatches under it;
//! a denial or an expired SLA timer fails the gated effect. A duplicate decision
//! never resumes the run twice, and a timer never auto-approves.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{sweep_crash_points, ScriptedDispatcher};
use rakka_agent::{
    AgentApprovalDecision, AgentCheckpointDecision, AgentCheckpointKind, AgentCheckpointSla,
    AgentEffectPolicies, AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunStatus, AgentRunTerminalReason,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

mod common;

use common::*;

const TOOL: &str = "charge-card";

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

fn gated_dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::new()
        .with_turn(tool_calling_turn())
        .with_turn(proposing_turn())
        .with_tool_result(
            TOOL,
            AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                .expect("the tool result is inline-bounded"),
        )
}

fn gated_policies() -> AgentEffectPolicies {
    AgentEffectPolicies::new()
        .with_tool_spec(
            tool_id(),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid")
}

fn fixture_with(policies: AgentEffectPolicies) -> Fixture {
    Fixture::with_sink(
        gated_dispatcher(),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

fn resolver() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "approver".to_string(),
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

fn approve() -> Box<AgentCheckpointDecision> {
    Box::new(AgentCheckpointDecision::Approval(
        AgentApprovalDecision::Approve {
            credential_binding: None,
            expires_at: AgentTimestampMillis::new(1_000_000),
            allowed_use_count: 1,
        },
    ))
}

/// Drives the run to its checkpoint wait and returns the id of the checkpoint it
/// is parked on.
async fn park(fx: &Fixture) -> HumanCheckpointId {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::WaitingForApproval,
        "a checkpoint-required tool parks the run for approval"
    );
    assert!(!run.status.is_terminal(), "an approval wait is nonterminal");

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

#[tokio::test]
async fn a_checkpoint_required_tool_parks_then_dispatches_under_an_approval() {
    // Scenario 3: the run passivates behind the checkpoint and resumes on the
    // decision command, dispatching the gated tool under the issued grant.
    let fx = fixture_with(gated_policies());
    let checkpoint_id = park(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .apply(
            AgentRunEntityCommand::ResolveCheckpoint {
                operation_id: decision_key("d1"),
                checkpoint_id,
                resolver: resolver(),
                decision: approve(),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the approval applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));

    fx.pump().await.expect("the resumed run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn a_duplicate_checkpoint_decision_does_not_resume_twice() {
    // Scenario 11: the same decision, replayed, returns the original outcome and
    // makes no second transition.
    let fx = fixture_with(gated_policies());
    let checkpoint_id = park(&fx).await;

    let command = |cp: HumanCheckpointId| AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: decision_key("d1"),
        checkpoint_id: cp,
        resolver: resolver(),
        decision: approve(),
    };

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let first = run
        .apply(command(checkpoint_id.clone()), &fx.router, fx.now())
        .await
        .expect("the approval applies");
    assert!(matches!(first, AgentRunEntityReply::Applied { .. }));

    // The same operation id again: deduplicated, no second transition.
    let replay = run
        .apply(command(checkpoint_id), &fx.router, fx.now())
        .await
        .expect("the replay is accepted");
    assert!(matches!(replay, AgentRunEntityReply::Duplicate { .. }));

    fx.pump().await.expect("the run completes exactly once");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn a_denied_checkpoint_fails_the_gated_effect() {
    let fx = fixture_with(gated_policies());
    let checkpoint_id = park(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: decision_key("d1"),
            checkpoint_id,
            resolver: resolver(),
            decision: Box::new(AgentCheckpointDecision::Approval(
                AgentApprovalDecision::Deny {
                    reason: "not this time".to_string(),
                },
            )),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the denial applies");

    fx.pump().await.expect("the denied run winds down");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, .. }) => {
            assert_eq!(code, "checkpoint-denied");
        }
        other => panic!("expected a denied-effect failure, found {other:?}"),
    }
}

#[tokio::test]
async fn an_authorization_required_tool_parks_then_dispatches_under_the_authorization() {
    // Scenario 3, authorization wait: a tool the deployment marked
    // authorization-required parks the run behind a security-authorization
    // checkpoint — passivated, no live task — and the resolving grant resumes
    // it. A duplicate decision is deduplicated (scenario 11, authorization
    // flavor).
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            tool_id(),
            AgentEffectSpec::non_idempotent().with_authorization_required(),
        )
        .expect("the authorization-required tool spec is valid");
    let fx = fixture_with(policies);

    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop parks on the checkpoint");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::WaitingForAuthorization,
        "an authorization-required tool parks the run for authorization"
    );
    assert!(
        !run.status.is_terminal(),
        "an authorization wait is nonterminal"
    );

    // A fresh store recovery is a passivation/reactivation cycle: the wait is
    // durable state, not a live task.
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
    assert_eq!(
        open[0].kind,
        AgentCheckpointKind::SecurityAuthorization,
        "the gate is a security-authorization checkpoint, not an approval"
    );
    let checkpoint_id = open[0].checkpoint_id.clone();

    let command = |cp| AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: decision_key("authz-1"),
        checkpoint_id: cp,
        resolver: resolver(),
        decision: approve(),
    };
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .apply(command(checkpoint_id.clone()), &fx.router, fx.now())
        .await
        .expect("the authorization applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));

    let replay = run
        .apply(command(checkpoint_id), &fx.router, fx.now())
        .await
        .expect("the replay is accepted");
    assert!(matches!(replay, AgentRunEntityReply::Duplicate { .. }));

    fx.pump().await.expect("the resumed run completes");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn an_expired_checkpoint_denies_without_auto_approving() {
    // Spec 12.6: a durable timer can only escalate or expire a waiting
    // checkpoint. A timeout on non-idempotent work fails closed.
    let sla = AgentCheckpointSla {
        due_after_ms: None,
        expire_after_ms: Some(100),
        escalation_target: None,
    };
    let fx = fixture_with(gated_policies().with_checkpoint_sla(sla));
    let _checkpoint_id = park(&fx).await;

    // Fire the timer before the deadline: nothing is due, the run stays parked.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::FireCheckpointTimers {
            operation_id: decision_key("t1"),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the early timer fires");
    let waiting = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(waiting.status, AgentRunStatus::WaitingForApproval);

    // Advance past the expiration and fire again: the checkpoint expires, and the
    // gated effect fails — it never auto-approved.
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::FireCheckpointTimers {
            operation_id: decision_key("t2"),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the expiry timer fires");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, .. }) => {
            assert_eq!(code, "checkpoint-expired");
        }
        other => panic!("expected an expired-effect failure, found {other:?}"),
    }
}

/// One full drive of the gated flow from durable state alone: park if not yet
/// parked, decide if the checkpoint is still open, and pump to convergence.
/// Every step surfaces an injected crash instead of panicking, so calling it
/// after a loss is the same operation as calling it after a success. The
/// checkpoint id is deterministic (derived from the gated effect), so the
/// caller passes the one the reference flow observed.
async fn drive_gated_flow(fx: &Fixture, checkpoint_id: &HumanCheckpointId) -> Result<(), String> {
    fx.pump().await?;
    let mut run = fx.run();
    run.recover(fx.now())
        .await
        .map_err(|error| error.code().to_string())?;
    let open = run
        .state()
        .map_err(|error| error.code().to_string())?
        .loop_state()
        .map(|state| !state.open_checkpoints().is_empty())
        .unwrap_or(false);
    if open {
        // The approver retries the same decision after a loss, under the same
        // operation id — Applied the first time, Duplicate on a re-drive.
        run.apply(
            AgentRunEntityCommand::ResolveCheckpoint {
                operation_id: decision_key("d1"),
                checkpoint_id: checkpoint_id.clone(),
                resolver: resolver(),
                decision: approve(),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .map_err(|error| error.code().to_string())?;
    }
    fx.pump().await
}

#[tokio::test]
async fn the_gated_park_decide_resume_flow_survives_any_owner_loss() {
    // Scenarios 3, 11, and 12 under the owner-kill sweep: kill the run's owner
    // at every durable write of park -> decide -> resume, on both sides of the
    // compare-and-set. Every crash converges on one completion, the gated tool
    // dispatches exactly once under the digest-bound grant, and a decision
    // replayed after convergence resumes nothing. The run store is the only
    // store this flow's crash windows live in; the driver is the in-process
    // dispatcher, so owner kill at every write is the complete boundary set.
    let reference = fixture_with(gated_policies());
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow parks on the checkpoint");
    let checkpoint_id = {
        let mut store = reference.run();
        store
            .recover(reference.now())
            .await
            .expect("the run recovers");
        let open = store
            .state()
            .expect("state reads")
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .to_vec();
        assert_eq!(open.len(), 1, "exactly one checkpoint gates the tool");
        open[0].checkpoint_id.clone()
    };
    drive_gated_flow(&reference, &checkpoint_id)
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(
        writes >= 6,
        "the gated flow should make several durable writes, saw {writes}"
    );
    let reference_effects = reference.dispatched_effects();

    sweep_crash_points(writes, |nth, point| {
        let checkpoint_id = checkpoint_id.clone();
        async move {
            let fx = fixture_with(gated_policies());
            fx.instantiate_agent().await;

            fx.runs.crash_at(nth, point);
            fx.create_task().await;
            let _crashed = drive_gated_flow(&fx, &checkpoint_id).await;

            // A new owner activates and finds only what was durably committed.
            fx.runs.survive();
            drive_gated_flow(&fx, &checkpoint_id)
                .await
                .unwrap_or_else(|error| {
                    panic!("crash {point:?} at write {nth} did not converge: {error}")
                });

            let run = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(
                run.status,
                AgentRunStatus::Completed,
                "crash {point:?} at write {nth} should still complete"
            );
            assert_eq!(
                run.turn, 2,
                "crash {point:?} at write {nth} replayed a turn"
            );
            assert_eq!(
                fx.dispatched_effects(),
                reference_effects,
                "crash {point:?} at write {nth} dispatched the gated tool twice"
            );

            // Scenario 11 under the sweep: the approver replays its decision
            // after everything settled. It must not resume anything.
            let mut run = fx.run();
            run.recover(fx.now()).await.expect("the run recovers");
            let replay = run
                .apply(
                    AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: decision_key("d1"),
                        checkpoint_id,
                        resolver: resolver(),
                        decision: approve(),
                    },
                    &fx.router,
                    fx.now(),
                )
                .await;
            match replay {
                Ok(AgentRunEntityReply::Duplicate { .. }) | Err(_) => {}
                Ok(other) => panic!(
                    "crash {point:?} at write {nth}: a replayed decision must \
                     not resume, got {other:?}"
                ),
            }
            let after = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(
                after.turn, 2,
                "crash {point:?} at write {nth}: the replayed decision advanced the run"
            );
        }
    })
    .await;
}
