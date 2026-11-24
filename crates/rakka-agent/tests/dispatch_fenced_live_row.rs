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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, RecordingToolExecutor, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor, AgentDispatchWindow,
    AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentReconciliationProtocolRef, AgentRunEffect, AgentRunEffectKind, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunScope, AgentRunStatus, AgentTaskContent, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{OutboxMessageId, OutboxStatus};
use rakka_agent_workflow::{
    AgentDispatchStatus, AgentDispatcherFleet, AgentDispatcherFleetSettings, AgentDispatcherWorker,
    AgentDispatcherWorkerId, AgentEphemeralCredential, DISPATCH_CANCELLED_IN_FLIGHT_LINE,
};
use rakka_persistence::DurableStateStore;
use serde_json::json;
use tokio::sync::oneshot;

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

// ---------------------------------------------------------------------------
// The agent dispatcher settles a fenced live row on its own (issue #211).
// ---------------------------------------------------------------------------

type Fleet = AgentDispatcherFleet<FleetStore, SharedAtomicWorkflowClock>;

/// A fleet handle of the deployment's own, over the fixture's fleet store.
fn fleet_handle(fx: &AuthorityFixture) -> Fleet {
    AgentDispatcherFleet::with_clock_and_metrics(
        fx.fleet_store.clone(),
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
        AgentDispatcherFleetSettings::new(16, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    )
}

/// The deployment fences the run's dispatches at the fleet, and answers how
/// many of its entries were in flight.
async fn fence_at_the_fleet(fx: &AuthorityFixture) -> usize {
    fleet_handle(fx)
        .request_run_cancellation(&rakka_agent::workflow_run_id(&run_scope()))
        .await
        .expect("the fleet fences the run")
        .in_flight_entries
}

/// The tool call's fleet entry: its status, and whether a cancellation
/// request fenced it.
async fn tool_entry(fx: &AuthorityFixture) -> (AgentDispatchStatus, bool) {
    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    let mut fleet = fleet_handle(fx);
    fleet.recover().await.expect("the fleet recovers");
    let state = fleet.state().expect("the fleet state reads");
    let entry = state
        .entries()
        .values()
        .find(|entry| entry.effect_id.as_str() == effect.dispatch_ticket_id().as_str())
        .expect("the ticket is registered");
    (entry.status, entry.cancellation_requested)
}

/// A tool whose call, while it is in flight, has the run's dispatches fenced
/// at the fleet, and then fails with a failure a retry may clear.
#[derive(Default)]
struct FencedWhileInFlight {
    fleet: tokio::sync::Mutex<Option<Fleet>>,
}

impl AgentDispatchToolExecutor for FencedWhileInFlight {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            if let Some(fleet) = self.fleet.lock().await.as_mut() {
                fleet
                    .request_run_cancellation(&rakka_agent::workflow_run_id(&run_scope()))
                    .await
                    .expect("the fleet fences the run");
            }
            Err(AgentDispatchError::collaborator(
                "upstream-unavailable",
                "try again later",
            ))
        })
    }
}

/// Shape A: the run's dispatches are fenced at the fleet while the call's
/// first attempt is in flight, and the attempt then fails. The row is
/// `Failed`, counting one; the entry is fenced, and no worker holds it.
async fn a_failed_attempt_fenced_in_flight(spec: &AgentEffectSpec) -> AuthorityFixture {
    let tool = Arc::new(FencedWhileInFlight::default());
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, spec),
        None,
    )
    .with_tool_executor(tool.clone());
    *tool.fleet.lock().await = Some(fleet_handle(&fx));
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.expire_lease();
    let first = fx.one_pass().await;
    assert_eq!((first.invoked, first.failed_attempts), (1, 1));
    assert_eq!(tool_row(&fx).await.0, OutboxStatus::Failed);
    assert!(tool_entry(&fx).await.1, "the entry is fenced");
    fx
}

/// Shape B: the call's first attempt is lost after `Started`, and the run's
/// dispatches are fenced at the fleet while the lost worker's lease stands.
async fn a_lost_attempt_fenced_under_its_lease(spec: &AgentEffectSpec) -> AuthorityFixture {
    let fx = a_lost_attempt(spec).await;
    assert_eq!(fence_at_the_fleet(&fx).await, 1, "the entry is in flight");
    fx
}

/// The call is parked for a decision under the fence's code, charged one.
async fn assert_parked(fx: &AuthorityFixture, label: &str) {
    assert_eq!(
        tool_effect(fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            1,
            Some(PARK_CODE.to_string())
        ),
        "{label}"
    );
    assert_eq!(
        run_status(fx).await,
        Some(AgentRunStatus::WaitingForReconciliation),
        "{label}"
    );
    assert_eq!(
        tool_entry(fx).await.0,
        AgentDispatchStatus::Cancelled,
        "{label}: the entry is finished"
    );
}

/// The call is cancelled, charged one, and the cancel ends the run.
async fn assert_cancelled_charged_one(fx: &AuthorityFixture) {
    let (status, attempts, _) = tool_effect(fx).await;
    assert_eq!((status, attempts), (AgentRunEffectStatus::Cancelled, 1));
    assert_eq!(run_status(fx).await, Some(AgentRunStatus::Cancelled));
    assert_eq!(tool_entry(fx).await.0, AgentDispatchStatus::Cancelled);
}

/// A failed idempotent call fenced at the fleet parks on the agent tier
/// alone. The run is cancelled and pumped only by the agent dispatcher; one
/// pass settles the fenced entry as the workflow tier's `refresh_run` would
/// and parks the call. The run used to stay `Cancelling`, its call `Ready`.
#[tokio::test]
async fn a_failed_idempotent_call_fenced_at_the_fleet_parks_on_the_agent_tier_alone() {
    let fx = a_failed_attempt_fenced_in_flight(&idempotent()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();

    fx.one_pass().await;

    assert_parked(&fx, "idempotent").await;
}

/// The same for a read-only call: cancelled, charged one, and the run ends.
#[tokio::test]
async fn a_failed_read_only_call_fenced_at_the_fleet_is_cancelled_on_the_agent_tier_alone() {
    let fx = a_failed_attempt_fenced_in_flight(&read_only()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();

    fx.one_pass().await;

    assert_cancelled_charged_one(&fx).await;
}

/// The same for a reconcileable call: parked (the owner's ruling).
#[tokio::test]
async fn a_failed_reconcileable_call_fenced_at_the_fleet_parks_on_the_agent_tier_alone() {
    let fx = a_failed_attempt_fenced_in_flight(&reconcileable()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();

    fx.one_pass().await;

    assert_parked(&fx, "reconcileable").await;
}

/// A lost attempt fenced at the fleet parks on the agent tier alone. Once
/// the lost worker's lease lapses, one pass cancels the row with the
/// witness, finishes the entry and parks the call, for a non-idempotent and
/// an idempotent call alike.
#[tokio::test]
async fn a_lost_attempt_fenced_at_the_fleet_parks_on_the_agent_tier_alone() {
    for spec in [AgentEffectSpec::non_idempotent(), idempotent()] {
        let fx = a_lost_attempt_fenced_under_its_lease(&spec).await;
        cancel_the_run(&fx).await;
        fx.expire_lease();

        fx.one_pass().await;

        let label = format!("{:?}", spec.safety_class);
        assert_eq!(
            tool_row(&fx).await,
            (OutboxStatus::Cancelled, 1, witness()),
            "{label}"
        );
        assert_parked(&fx, &label).await;
    }
}

/// The same for a read-only call: cancelled, charged the attempt.
#[tokio::test]
async fn a_lost_read_only_attempt_fenced_at_the_fleet_is_cancelled_on_the_agent_tier_alone() {
    let fx = a_lost_attempt_fenced_under_its_lease(&read_only()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();

    fx.one_pass().await;

    assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 1, witness()));
    assert_cancelled_charged_one(&fx).await;
}

/// An entry under an active lease is left to its worker, as the workflow
/// tier leaves it: the worker may still finish and record what happened.
#[tokio::test]
async fn an_entry_under_an_active_lease_is_left_to_its_worker() {
    let fx = a_lost_attempt_fenced_under_its_lease(&AgentEffectSpec::non_idempotent()).await;
    cancel_the_run(&fx).await;

    fx.one_pass().await;

    assert_eq!(tool_row(&fx).await, (OutboxStatus::Dispatching, 0, None));
    assert_eq!(tool_entry(&fx).await, (AgentDispatchStatus::Claimed, true));
    assert_eq!(run_status(&fx).await, Some(AgentRunStatus::Cancelling));
}

/// A row the run put back under a fenced entry is parked
/// `ticket-row-missing` (issue #153's rule). The ticket's row is lost after
/// its entry was registered, and the run puts a new row back, which the
/// entry was not registered from. The run's dispatches are then fenced and
/// the run cancelled. The sweep cancels the row put back with no witness,
/// finishes the entry, and parks the call as the claim path parks such a
/// row: its count of nothing says nothing about the row that was lost.
#[tokio::test]
async fn a_row_the_run_put_back_under_a_fenced_entry_parks_ticket_row_missing() {
    let fx = a_tool_call_out(&AgentEffectSpec::non_idempotent()).await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let id = workflow_persistence_id(&run_scope());
    let record = fx
        .workflow_store
        .load(&id)
        .await
        .expect("the workflow store loads")
        .expect("the run has a workflow record");
    fx.workflow_store
        .delete(&id, record.revision)
        .await
        .expect("the workflow record is deleted");
    fx.wf_clock.advance(1_000);
    fx.settle().await;
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Pending, 0, None));
    assert_eq!(fence_at_the_fleet(&fx).await, 0);
    cancel_the_run(&fx).await;
    fx.expire_lease();

    fx.one_pass().await;

    let (status, _, code) = tool_effect(&fx).await;
    assert_eq!(
        (status, code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("ticket-row-missing")
        )
    );
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 0, None));
    assert_eq!(tool_entry(&fx).await.0, AgentDispatchStatus::Cancelled);
}

/// A live run whose dispatches are fenced at the fleet, without the run
/// being cancelled, parks after a lease. The first pass settles the fenced
/// entry and tells the run nothing yet; once a lease has passed since, the
/// cancelled row is read as any is on a live run, and the call is parked.
/// The run used to wait on the call for ever.
#[tokio::test]
async fn a_live_run_fenced_at_the_fleet_parks_after_a_lease() {
    let fx = a_failed_attempt_fenced_in_flight(&idempotent()).await;
    fx.expire_lease();
    fx.one_pass().await;
    assert_eq!(tool_entry(&fx).await.0, AgentDispatchStatus::Cancelled);
    assert_eq!(tool_row(&fx).await.0, OutboxStatus::Cancelled);
    assert_eq!(tool_effect(&fx).await.0, AgentRunEffectStatus::Ready);

    fx.wf_clock.advance(LEASE_MS);
    fx.one_pass().await;

    let (status, _, code) = tool_effect(&fx).await;
    assert_eq!(
        (status, code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("ticket-row-settled")
        )
    );
}

/// A tool whose first call signals `entered`, waits for `release`, and then
/// succeeds.
struct HeldSuccess {
    calls: AtomicUsize,
    gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl AgentDispatchToolExecutor for HeldSuccess {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate.lock().expect("the lock holds").take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
            Ok(AgentTaskContent::inline(json!({ "charged": true })).expect("the result is inline"))
        })
    }
}

/// A worker that finishes after the sweep settled its row changes nothing.
/// Worker one's call is out when the run's dispatches are fenced and the run
/// cancelled, and outlives its lease. Worker two's sweep cancels the row
/// with the witness and parks the call. Worker one's call then succeeds: its
/// result is delivered and the run refuses it, the call being parked; its
/// settle finds the row finished. The call stays parked, and the row keeps
/// the witness, written once.
#[tokio::test]
async fn a_worker_that_finishes_after_the_sweep_settled_its_row_changes_nothing() {
    let (entered, out) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let tool = Arc::new(HeldSuccess {
        calls: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered, released))),
    });
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &idempotent()),
        None,
    )
    .with_tool_executor(tool.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.expire_lease();
    let mut one = fx.pipeline();
    let mut two = fx.worker("worker-two", fx.tools.clone(), None);
    let scope = run_scope();

    let (one_pass, two_pass) = tokio::join!(one.pump_run(&scope), async {
        let _ = out.await;
        assert_eq!(fence_at_the_fleet(&fx).await, 1, "worker one's call is out");
        cancel_the_run(&fx).await;
        fx.expire_lease();
        fx.settle().await;
        let pass = two.pump_run(&run_scope()).await;
        let _ = release.send(());
        pass
    });

    let two_pass = two_pass.expect("worker two's pass runs");
    assert_eq!(two_pass.parked_indeterminate, 1);
    let one_pass = one_pass.expect("worker one's pass runs");
    assert_eq!(one_pass.invoked, 1, "worker one made the one call");
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 1, witness()));
    assert_eq!(
        tool_effect(&fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            1,
            Some(PARK_CODE.to_string())
        ),
        "the call stays parked"
    );
}

/// A cancel whose finish was lost is finished by the next pass. The sweep
/// cancels the row with the witness, and its write finishing the fleet entry
/// is lost: the pass fails, and the entry is left fenced and unfinished
/// under a cancelled row. The next pass finishes the entry and parks the
/// call, and does not write the row again.
#[tokio::test]
async fn a_cancel_whose_finish_was_lost_is_finished_by_the_next_pass() {
    let fx = a_lost_attempt_fenced_under_its_lease(&AgentEffectSpec::non_idempotent()).await;
    cancel_the_run(&fx).await;
    fx.expire_lease();
    fx.settle().await;
    fx.fleet_store
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);

    fx.pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("the fleet write is lost");

    fx.fleet_store
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    fx.fleet_store.survive();
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 1, witness()));
    assert_eq!(tool_entry(&fx).await, (AgentDispatchStatus::Claimed, true));

    fx.one_pass().await;

    assert_eq!(
        tool_row(&fx).await,
        (OutboxStatus::Cancelled, 1, witness()),
        "the row is written once"
    );
    assert_parked(&fx, "after the lost finish").await;
}
