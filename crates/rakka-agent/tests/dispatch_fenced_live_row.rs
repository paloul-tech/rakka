//! A fleet entry fenced by a cancellation request while its outbox row is
//! live (issues #211 and #209).
//!
//! The workflow tier's cancellation (`AgentDispatcherWorker`'s
//! `cancel_run_dispatches` and `refresh_run`) settles such an entry, once no
//! worker holds it, by cancelling its row and finishing the entry.
//!
//! - **#209.** A `Dispatching` row is an attempt whose worker was lost after
//!   `Started`. It used to be cancelled as a ticket that never started, and
//!   the run was told `Cancelled`, charged nothing, for every safety class.
//!   It is now cancelled with that attempt counted under a witness line, and
//!   the agent dispatcher parks the call for a decision, unless it is
//!   read-only or produces only its result. A reconcileable call whose failed
//!   attempt was counted parks as well: the claim path would have asked the
//!   reconciler about it.
//! - **#211.** An agent-only deployment never settled the entry, so its run
//!   stalled unless the workflow tier's `refresh_run` was called. The agent
//!   dispatcher's per-run sweep now settles it as `refresh_run` would.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentDispatchWindow, AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentReconciliationProtocolRef, AgentRunEffect, AgentRunEffectKind, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunStatus, AgentTaskContent, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{OutboxMessageId, OutboxStatus};
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherWorker, AgentDispatcherWorkerId,
    DISPATCH_CANCELLED_IN_FLIGHT_LINE,
};
use serde_json::json;

mod common;

use common::*;

const TOOL: &str = "charge-card";

/// The code a call that may have executed is parked under on a cancelled run.
const PARK_CODE: &str = "run-cancelled-after-attempt";

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Let me do that.")
                .with_tool_call(
                    AgentToolCallRequest::new(
                        AgentToolCallId::new("call-1").expect("call id should be valid"),
                        AgentToolId::new(TOOL).expect("tool id should be valid"),
                        json!({ "amount": 42 }),
                    )
                    .expect("the call is bounded"),
                ),
        )
        .with_turn_for(2, proposing_turn("done"))
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": answer })).expect("the answer is inline"),
        )
}

fn idempotent() -> AgentEffectSpec {
    AgentEffectSpec::idempotent(3).expect("the spec is valid")
}

fn read_only() -> AgentEffectSpec {
    AgentEffectSpec::read_only()
        .with_max_attempts(3)
        .expect("the spec is valid")
}

fn reconcileable() -> AgentEffectSpec {
    AgentEffectSpec::reconcileable(
        AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid"),
        3,
    )
    .expect("the spec is valid")
}

/// The workflow tier's cancellation of the run's dispatches:
/// `AgentDispatcherWorker::cancel_run_dispatches`.
async fn cancel_with_the_workflow_tier(fx: &AuthorityFixture) {
    let mut worker = AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new("workflow-tier"),
        fx.fleet_store.clone(),
        fx.workflow_store.clone(),
        AgentDispatcherFleetSettings::new(16, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    );
    worker
        .cancel_run_dispatches(&rakka_agent::workflow_run_id(&run_scope()))
        .await
        .expect("the workflow tier cancels the run's dispatches");
}

/// Cancels the run.
async fn cancel_the_run(fx: &AuthorityFixture) {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the cancellation applies");
}

/// A run whose tool call, under `spec`, has its ticket out.
async fn a_tool_call_out(spec: &AgentEffectSpec) -> AuthorityFixture {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, spec),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx
}

/// A run whose tool call's first attempt was lost after `Started`: its row
/// is `Dispatching`, counting nothing.
async fn a_lost_attempt(spec: &AgentEffectSpec) -> AuthorityFixture {
    let fx = a_tool_call_out(spec).await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    fx.expire_lease();
    assert!(fx.one_pass().await.died, "the worker is lost after Started");
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Dispatching, 0, None));
    fx
}

/// A run whose tool call's first attempt failed with a failure a retry may
/// clear: its row is `Failed`, counting one.
async fn a_failed_attempt(spec: &AgentEffectSpec) -> AuthorityFixture {
    let mut fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, spec),
        None,
    );
    fx.tools = RecordingToolExecutor::new().with_failure(TOOL, "upstream-unavailable", "later");
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.expire_lease();
    let first = fx.one_pass().await;
    assert_eq!((first.invoked, first.failed_attempts), (1, 1));
    assert_eq!(tool_row(&fx).await.0, OutboxStatus::Failed);
    fx
}

/// The tool call's outbox row: its status, its attempts and its last line.
async fn tool_row(fx: &AuthorityFixture) -> (OutboxStatus, u32, Option<String>) {
    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    row_of(fx, &effect).await
}

async fn row_of(
    fx: &AuthorityFixture,
    effect: &AgentRunEffect,
) -> (OutboxStatus, u32, Option<String>) {
    let row = fx.outbox_row(effect).await.expect("the ticket has a row");
    (
        row.status(),
        row.attempts().attempts(),
        row.attempts().last_error().map(str::to_string),
    )
}

/// The run's record of its tool call: status, attempts and code.
async fn tool_effect(fx: &AuthorityFixture) -> (AgentRunEffectStatus, u32, Option<String>) {
    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    (effect.status, effect.attempts, effect.last_error_code)
}

async fn run_status(fx: &AuthorityFixture) -> Option<AgentRunStatus> {
    fx.fx.run_snapshot().await.map(|run| run.status)
}

/// The witness line, as a row carries it.
fn witness() -> Option<String> {
    Some(DISPATCH_CANCELLED_IN_FLIGHT_LINE.to_string())
}

// ---------------------------------------------------------------------------
// The workflow tier's cancellation, read by the agent dispatcher (issue #209).
// ---------------------------------------------------------------------------

/// A lost attempt the workflow tier cancels parks. Its worker died after
/// `Started`, the run is cancelled, and the workflow tier cancels the run's
/// dispatches. The row keeps the witness and counts the attempt, and the
/// next pass parks the call for a decision, charged that attempt, for a
/// non-idempotent call and an idempotent one alike. It used to be told
/// `Cancelled`, charged nothing.
#[tokio::test]
async fn a_lost_attempt_the_workflow_tier_cancels_parks() {
    for spec in [AgentEffectSpec::non_idempotent(), idempotent()] {
        let fx = a_lost_attempt(&spec).await;
        cancel_the_run(&fx).await;
        fx.expire_lease();
        cancel_with_the_workflow_tier(&fx).await;
        assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 1, witness()));

        fx.one_pass().await;

        assert_eq!(
            tool_effect(&fx).await,
            (
                AgentRunEffectStatus::Indeterminate,
                1,
                Some(PARK_CODE.to_string())
            ),
            "{:?}",
            spec.safety_class
        );
        assert_eq!(
            run_status(&fx).await,
            Some(AgentRunStatus::WaitingForReconciliation),
            "{:?}",
            spec.safety_class
        );
    }
}

/// A read-only call's lost attempt, cancelled the same way, is cancelled,
/// charged the attempt it made, and the cancel ends the run.
#[tokio::test]
async fn a_lost_read_only_attempt_the_workflow_tier_cancels_is_cancelled_charged_one() {
    let fx = a_lost_attempt(&read_only()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();
    cancel_with_the_workflow_tier(&fx).await;

    fx.one_pass().await;

    let (status, attempts, _) = tool_effect(&fx).await;
    assert_eq!((status, attempts), (AgentRunEffectStatus::Cancelled, 1));
    assert_eq!(run_status(&fx).await, Some(AgentRunStatus::Cancelled));
}

/// A reconcileable call whose failed attempt was counted, cancelled by the
/// workflow tier on a cancelled run, parks (the owner's ruling). The claim
/// path would have asked the reconciler about that attempt; nothing asks it
/// here, so a person decides. It used to be told `Cancelled`.
#[tokio::test]
async fn a_failed_reconcileable_call_the_workflow_tier_cancels_parks() {
    let fx = a_failed_attempt(&reconcileable()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();
    cancel_with_the_workflow_tier(&fx).await;
    assert_eq!(tool_row(&fx).await.0, OutboxStatus::Cancelled);

    fx.one_pass().await;

    assert_eq!(
        tool_effect(&fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            1,
            Some(PARK_CODE.to_string())
        )
    );
}

/// The run's model call effect.
async fn model_effect(fx: &AuthorityFixture) -> Option<AgentRunEffect> {
    let state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")?;
    state
        .loop_state()?
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::ModelCall)
        .cloned()
}

/// A model call's lost attempt, cancelled by the workflow tier, is
/// cancelled, charged the attempt: a completion acts on nothing outside the
/// run, whatever its class (the owner's #206 ruling).
#[tokio::test]
async fn a_lost_model_call_the_workflow_tier_cancels_is_cancelled() {
    let fx = AuthorityFixture::over(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("done")),
        rakka_agent::AgentToolRegistry::new(),
        None,
    );
    fx.start().await;
    for _ in 0..4 {
        fx.settle().await;
        if model_effect(&fx).await.map(|effect| effect.status) == Some(AgentRunEffectStatus::Ready)
        {
            break;
        }
    }
    let model = model_effect(&fx)
        .await
        .expect("the run holds its model call");
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    fx.expire_lease();
    assert!(fx.one_pass().await.died, "the worker is lost after Started");
    assert_eq!(
        row_of(&fx, &model).await,
        (OutboxStatus::Dispatching, 0, None)
    );
    cancel_the_run(&fx).await;
    fx.expire_lease();
    cancel_with_the_workflow_tier(&fx).await;
    assert_eq!(
        row_of(&fx, &model).await,
        (OutboxStatus::Cancelled, 1, witness())
    );

    let pass = fx.one_pass().await;

    let effect = model_effect(&fx)
        .await
        .expect("the run holds its model call");
    assert_eq!(
        (effect.status, effect.attempts, pass.parked_indeterminate),
        (AgentRunEffectStatus::Cancelled, 1, 0)
    );
    assert_eq!(run_status(&fx).await, Some(AgentRunStatus::Cancelled));
}

/// A reconcileable row cancelled before its run was told parks (the design
/// record's call 8). On a cancelled run, a claim told "not executed" cancels
/// the row and then tells the run; a worker lost between the two leaves a
/// cancelled row that counts the failed attempt, with the run untold. The
/// claim that follows parks the call: nothing durable tells this row from
/// one the workflow tier cancelled, and parking is the conservative
/// reading.
#[tokio::test]
async fn a_reconcileable_row_cancelled_before_its_run_was_told_parks() {
    let fx = a_failed_attempt(&reconcileable()).await;
    cancel_the_run(&fx).await;
    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    let mut inbox = rakka_agent_workflow::AgentRunInbox::with_clock(
        rakka_agent::workflow_run_id(&run_scope()),
        fx.workflow_store.clone(),
        fx.wf_clock.clone(),
    );
    inbox.recover().await.expect("the inbox recovers");
    inbox
        .inner_mut()
        .record_outbox_cancelled(
            &OutboxMessageId::new(effect.dispatch_ticket_id().as_str()),
            "run-cancelled",
        )
        .await
        .expect("the row is cancelled");
    assert_eq!(tool_row(&fx).await.0, OutboxStatus::Cancelled);
    fx.expire_lease();

    fx.one_pass().await;

    assert_eq!(
        tool_effect(&fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            1,
            Some(PARK_CODE.to_string())
        )
    );
}
