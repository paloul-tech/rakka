//! The wind-down fence and a call that may have executed (issue #206).
//!
//! The fence cancels a ticket whose next attempt has not started, and tells
//! the run `Cancelled`. An idempotent call with an attempt already counted is
//! not such a ticket: the dispatcher's own rule for an idempotent call is that
//! every attempt after the first follows a possibly delivered one, which is
//! why a refused retry of one parks. So a winding-down run's idempotent call
//! with a counted attempt is parked `Indeterminate` under
//! `run-cancelled-after-attempt`, charged the attempts that started, and the
//! cancelled run waits for a reconciliation decision.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    DeterministicModelAdapter, InProcessRunResultDelivery, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentA2aSendExecutor, AgentA2aSendFinding, AgentA2aSendReceipt, AgentAuthorityRefusal,
    AgentCancellationProgress, AgentDelegationRecord, AgentDelegationStatus,
    AgentDispatchAuthority, AgentDispatchDecision, AgentDispatchError, AgentDispatchFuture,
    AgentDispatchWindow, AgentEffectResolution, AgentEffectSpec, AgentEntityAuthority,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunEffect, AgentRunEffectDispatcher,
    AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand,
    AgentRunScope, AgentRunState, AgentRunStatus, AgentTaskContent, AgentTaskId, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherWorkerId, AgentEphemeralCredential,
    AgentTelemetryContext, AgentTimestampMillis,
};
use serde_json::json;

mod common;

use common::*;

const TOOL: &str = "charge-card";

/// The code a call parked by the wind-down fence carries.
const PARK_CODE: &str = "run-cancelled-after-attempt";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("tool id should be valid")
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                tool_id(),
                json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn("charged"))
}

/// A run whose first turn calls an idempotent tool with a budget of three,
/// with the call's ticket out.
async fn an_idempotent_tool_ticket_out() -> AuthorityFixture {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::idempotent(3).expect("the spec is valid"),
        ),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx
}

/// The tool call's outbox row: its status, and the attempts it counts.
async fn tool_row(fx: &AuthorityFixture) -> (OutboxStatus, u32) {
    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    let row = fx.outbox_row(&effect).await.expect("the ticket has a row");
    (row.status(), row.attempts().attempts())
}

/// The first attempt's worker dies after its durable `Started`: the row is
/// left `Dispatching`, counting nothing, and the attempt may have executed.
async fn lose_the_first_attempt(fx: &AuthorityFixture) {
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    fx.expire_lease();
    assert!(fx.one_pass().await.died, "the worker dies after Started");
    assert_eq!(tool_row(fx).await, (OutboxStatus::Dispatching, 0));
}

/// An operator cancels the run.
async fn cancel_the_run(fx: &AuthorityFixture) {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "cancel-1"],
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

/// A person answers the park on `effect`.
async fn resolve(
    fx: &AuthorityFixture,
    effect: &AgentRunEffect,
    resolution: AgentEffectResolution,
) {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "park-answer",
            )
            .expect("the decision key derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            resolution: Box::new(resolution),
            telemetry: AgentTelemetryContext::default(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the decision applies");
}

/// The run's status.
async fn run_status(fx: &AuthorityFixture) -> Option<AgentRunStatus> {
    fx.fx.run_snapshot().await.map(|run| run.status)
}

/// What the run records of the tool call: its status, its code, and the
/// attempts it was charged.
async fn tool_call(fx: &AuthorityFixture) -> (AgentRunEffectStatus, Option<String>, u32) {
    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    (effect.status, effect.last_error_code, effect.attempts)
}

/// After the run is cancelled, the call is parked and the run waits.
fn assert_parked(pass: &rakka_agent::AgentDispatchPass) {
    assert_eq!(
        (pass.parked_indeterminate, pass.cancelled),
        (1, 1),
        "the call is parked, and its ticket settled"
    );
}

// ---------------------------------------------------------------------------
// A lost attempt the next claim counted, then a cancel.
// ---------------------------------------------------------------------------

fn the_fixtures_authority(fx: &AuthorityFixture) -> Arc<dyn AgentDispatchAuthority> {
    Arc::new(AgentEntityAuthority::new(
        fx.fx.agents.clone(),
        fx.authority.clone(),
    ))
}

/// A dispatch worker over the fixture's stores, asking `authority`.
fn worker(fx: &AuthorityFixture, authority: Arc<dyn AgentDispatchAuthority>) -> Pipeline {
    let delivery = InProcessRunResultDelivery::new(
        fx.fx.runs.clone(),
        fx.fx.effects.clone(),
        fx.fx.router.clone(),
        fx.fx.clock.clone(),
    )
    .with_effect_policies(fx.fx.policies.clone());
    AgentRunEffectDispatcher::new(
        AgentDispatcherWorkerId::new("worker-late"),
        fx.workflow_store.clone(),
        fx.fleet_store.clone(),
        fx.fx.runs.clone(),
        fx.wf_clock.clone(),
        Arc::new(fx.adapter.clone()),
        Arc::new(fx.tools.clone()),
        authority,
        Arc::new(delivery),
    )
    .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
}

/// An authority whose first answer comes after the claim's lease: it moves
/// the clock past the lease and then refuses. Later questions are answered
/// by the fixture's own authority.
struct LateRefusal {
    inner: Arc<dyn AgentDispatchAuthority>,
    clock: SharedAtomicWorkflowClock,
    answered: AtomicBool,
}

impl AgentDispatchAuthority for LateRefusal {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            if self.answered.swap(true, Ordering::SeqCst) {
                return self.inner.authorize(scope, run, intent, attempt, now).await;
            }
            self.clock.advance(LEASE_MS + 1);
            Ok(AgentDispatchDecision::Refused(AgentAuthorityRefusal::of(
                "tool-revoked",
                "the tool was revoked",
            )))
        })
    }

    fn review_tool_response<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        tool: Option<&'a AgentToolId>,
        content: AgentTaskContent,
    ) -> AgentDispatchFuture<'a, rakka_agent::AgentToolResponseDecision> {
        self.inner
            .review_tool_response(scope, intent, tool, content)
    }

    fn review_model_response<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        turn: AgentModelTurn,
    ) -> AgentDispatchFuture<'a, rakka_agent::AgentModelResponseDecision> {
        self.inner.review_model_response(scope, intent, turn)
    }
}

/// The route the issue reproduces. The first attempt is lost after its
/// `Started`. The next claim counts it and asks the authority about the
/// retry, and the authority's refusal comes after the claim's lease: the
/// claim ends out of lease with the lost attempt counted (issue #196). The
/// run is then cancelled. The row reads as an ordinary failed attempt, and
/// the fence cancelled the call as never started, though attempt one may
/// have executed.
#[tokio::test]
async fn a_counted_lost_attempt_whose_refusal_came_after_the_lease_is_parked_when_the_run_is_cancelled(
) {
    let fx = an_idempotent_tool_ticket_out().await;
    lose_the_first_attempt(&fx).await;

    let late = Arc::new(LateRefusal {
        inner: the_fixtures_authority(&fx),
        clock: fx.wf_clock.clone(),
        answered: AtomicBool::new(false),
    });
    fx.expire_lease();
    fx.settle().await;
    let pass = worker(&fx, late)
        .pump_run(&run_scope())
        .await
        .expect("the late worker's pass runs");
    assert_eq!(
        (pass.failed_attempts, pass.lapsed),
        (1, 1),
        "the lost attempt is counted, and the claim ends out of lease"
    );
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Failed, 1));
    cancel_the_run(&fx).await;

    fx.expire_lease();
    let pass = fx.one_pass().await;

    assert_parked(&pass);
    assert_eq!(
        tool_call(&fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            Some(PARK_CODE.to_string()),
            1
        )
    );
    assert_eq!(
        run_status(&fx).await,
        Some(AgentRunStatus::WaitingForReconciliation)
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        0,
        "the probe killed the lost attempt before the tool ran; nothing ran after the cancel"
    );
}

/// The same row on `main` before issue #196: the claim that counts the lost
/// attempt dies before its retry starts.
async fn a_counted_lost_attempt_then_a_cancel(fx: &AuthorityFixture) {
    lose_the_first_attempt(fx).await;
    fx.probe.arm(AgentDispatchWindow::BeforeStarted);
    fx.expire_lease();
    assert!(
        fx.one_pass().await.died,
        "the worker dies before the retry's Started"
    );
    assert_eq!(
        tool_row(fx).await,
        (OutboxStatus::Failed, 1),
        "the lost attempt is counted, and the retry never started"
    );
    cancel_the_run(fx).await;
}

/// A claim counts the lost attempt and its worker dies before the retry. The
/// run is then cancelled: the call is parked, not cancelled. Pumped again,
/// the parked call is neither dispatched nor parked a second time.
#[tokio::test]
async fn a_counted_lost_attempt_whose_worker_died_before_the_retry_is_parked_when_the_run_is_cancelled(
) {
    let fx = an_idempotent_tool_ticket_out().await;
    a_counted_lost_attempt_then_a_cancel(&fx).await;

    fx.expire_lease();
    let pass = fx.one_pass().await;

    assert_parked(&pass);
    assert_eq!(
        tool_call(&fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            Some(PARK_CODE.to_string()),
            1
        )
    );
    assert_eq!(
        run_status(&fx).await,
        Some(AgentRunStatus::WaitingForReconciliation)
    );

    fx.expire_lease();
    let again = fx.one_pass().await;
    assert_eq!(
        (
            again.claimed,
            again.invoked,
            again.parked_indeterminate,
            again.cancelled
        ),
        (0, 0, 0, 0),
        "a parked call is not dispatched or parked again"
    );
    assert_eq!(
        run_status(&fx).await,
        Some(AgentRunStatus::WaitingForReconciliation)
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// The park asks a person whether the lost attempt executed. Answered "not
/// executed", the cancel goes on and the run ends `Cancelled`, and the call
/// is never dispatched again.
#[tokio::test]
async fn the_park_answered_not_executed_lets_the_cancel_finish() {
    let fx = an_idempotent_tool_ticket_out().await;
    a_counted_lost_attempt_then_a_cancel(&fx).await;
    fx.expire_lease();
    assert_parked(&fx.one_pass().await);

    let effect = fx.effect_at(1).await.expect("the run holds its tool call");
    resolve(&fx, &effect, AgentEffectResolution::ConfirmedNotExecuted).await;
    fx.settle().await;

    assert_eq!(run_status(&fx).await, Some(AgentRunStatus::Cancelled));
    fx.expire_lease();
    let after = fx.one_pass().await;
    assert_eq!((after.claimed, after.invoked), (0, 0));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// A delegation send: the cell waits for the decision, not "never spawned".
// ---------------------------------------------------------------------------

fn delegating_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating the translation.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                delegation_tool_id(),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
}

/// A peer that is unavailable: every send fails, retryably.
#[derive(Default)]
struct UnavailablePeer {
    sends: AtomicUsize,
}

impl AgentA2aSendExecutor for UnavailablePeer {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        Box::pin(async move {
            self.sends.fetch_add(1, Ordering::SeqCst);
            Err(AgentDispatchError::collaborator(
                "peer-unavailable",
                "the peer did not answer",
            ))
        })
    }
}

/// The run's delegation send effect.
async fn send_effect(fx: &AuthorityFixture) -> Option<AgentRunEffect> {
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
        .find(|effect| effect.kind() == AgentRunEffectKind::A2aSendCall)
        .cloned()
}

/// The delegation cell's status, whether the run has sent its child a
/// cancel, and how far the run's cancellation has got.
async fn delegation_and_progress(
    fx: &AuthorityFixture,
) -> (AgentDelegationStatus, bool, AgentCancellationProgress) {
    let state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    let cell = state
        .loop_state()
        .expect("loop state")
        .delegations()
        .values()
        .next()
        .expect("the cell exists")
        .clone();
    let progress = state.run().map_or(
        AgentCancellationProgress::NotRequested,
        AgentCancellationProgress::derive,
    );
    (cell.status, cell.cancel.is_some(), progress)
}

/// A delegation send whose first attempt failed may have created the child.
/// Cancelled as never started, its cell was settled `Failed
/// { run-winding-down }`, "the parent never spawned the child", and the
/// wind-down sent no cancel to a child the attempt did create. Parked, the
/// cell waits for the decision, and an answer of "executed" records the
/// child, and the run sends it the cancel the wind-down owes it.
///
/// The child named in the answer is not a task this fixture holds, so it
/// refuses the cancel definitively, and the run, which then has nothing to
/// wait for, ends: that the cancel was sent is what is asserted.
#[tokio::test]
async fn a_delegation_send_with_a_failed_attempt_waits_for_its_decision_and_then_cancels_its_child()
{
    let peer = Arc::new(UnavailablePeer::default());
    let fx = AuthorityFixture::over(
        DeterministicModelAdapter::new().with_turn(delegating_turn()),
        rakka_agent::AgentToolRegistry::new(),
        None,
    )
    .with_delegation(delegation_config())
    .with_a2a_send_executor(peer.clone());
    fx.fx
        .instantiate_agent_with_envelope(fx.envelope.clone())
        .await;
    fx.fx
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec_with_delegation(), true),
        ))
        .await
        .expect("the goal task should create");
    for _ in 0..8 {
        if peer.sends.load(Ordering::SeqCst) > 0 {
            break;
        }
        fx.one_pass().await;
    }
    let send = send_effect(&fx).await.expect("the run holds its send");
    let row = fx.outbox_row(&send).await.expect("the send has a row");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Failed, 1),
        "the send's first attempt failed, and the row counts it"
    );
    cancel_the_run(&fx).await;

    fx.expire_lease();
    let pass = fx.one_pass().await;

    assert_parked(&pass);
    let send = send_effect(&fx).await.expect("the run holds its send");
    assert_eq!(
        (send.status, send.last_error_code.as_deref(), send.attempts),
        (AgentRunEffectStatus::Indeterminate, Some(PARK_CODE), 1)
    );
    assert_eq!(
        delegation_and_progress(&fx).await,
        (
            AgentDelegationStatus::Pending,
            false,
            AgentCancellationProgress::WaitingForReconciliation
        ),
        "the cell waits for the decision"
    );
    assert_eq!(peer.sends.load(Ordering::SeqCst), 1);

    let AgentRunEffect { request, .. } = &send;
    let rakka_agent::AgentRunEffectRequest::A2aSend { delegation } = request else {
        panic!("the send carries its delegation: {request:?}");
    };
    resolve(
        &fx,
        &send,
        AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(AgentRunEffectOutcome::A2aSend {
                receipt: AgentA2aSendReceipt {
                    delegation: delegation.delegation.clone(),
                    child_task: AgentTaskId::new("child-task-1").expect("the task id is valid"),
                    child_run: None,
                    peer_status: "submitted".to_string(),
                },
            }),
        },
    )
    .await;

    let (cell, cancel_sent, _) = delegation_and_progress(&fx).await;
    assert!(
        matches!(cell, AgentDelegationStatus::ChildCreated { .. }),
        "the decision records the child: {cell:?}"
    );
    assert!(cancel_sent, "the run sends the child its cancel");
}

// ---------------------------------------------------------------------------
// A cancelled row the claim's fence did not write, read by a claim or by the
// per-run sweep.
// ---------------------------------------------------------------------------

/// A tool that always fails with a failure a retry may clear, and counts its
/// calls.
#[derive(Default)]
struct CountingRetryable {
    calls: AtomicUsize,
}

impl rakka_agent::AgentDispatchToolExecutor for CountingRetryable {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Err(AgentDispatchError::collaborator(
                "upstream-unavailable",
                "try again later",
            ))
        })
    }
}

/// An idempotent three-attempt call whose first attempt ran and failed: the
/// row is `Failed` and counts one attempt, and the run was told of none. The
/// run is then cancelled, before anything claims the retry.
async fn a_failed_attempt_then_a_cancel() -> (AuthorityFixture, Arc<CountingRetryable>) {
    let tool = Arc::new(CountingRetryable::default());
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::idempotent(3).expect("the spec is valid"),
        ),
        None,
    )
    .with_tool_executor(tool.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let pass = fx.one_pass().await;
    assert_eq!((pass.invoked, pass.failed_attempts), (1, 1));
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Failed, 1));
    cancel_the_run(&fx).await;
    (fx, tool)
}

/// Cancels the ticket's row alone, as a fence before issue #206 did first,
/// and leaves its fleet entry as it is: what that fence left when its worker
/// was lost before its fleet write and its delivery.
async fn cancel_the_row_as_an_earlier_fence_did(fx: &AuthorityFixture) {
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
            &rakka_agent_workflow::substrate::OutboxMessageId::new(
                effect.dispatch_ticket_id().as_str(),
            ),
            "run-cancelled",
        )
        .await
        .expect("the row is cancelled");
    assert_eq!(tool_row(fx).await, (OutboxStatus::Cancelled, 1));
}

/// The run's record of the call once it is parked, and the run waiting.
async fn assert_the_call_is_parked(fx: &AuthorityFixture, tool: &CountingRetryable) {
    assert_eq!(
        tool_call(fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            Some(PARK_CODE.to_string()),
            1
        ),
        "the call is parked, charged the attempt the row counts"
    );
    assert_eq!(
        run_status(fx).await,
        Some(AgentRunStatus::WaitingForReconciliation)
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1, "the retry never ran");
}

/// A deployment cancels the run's dispatches through the workflow tier
/// (`AgentDispatcherWorker::cancel_run_dispatches`), which cancels the rows
/// of the entries no worker holds and finishes those entries. The per-run
/// sweep then tells the winding-down run about the cancelled row. It used to
/// tell it `Cancelled`, as though the call had never started.
#[tokio::test]
async fn the_fleets_own_cancellation_of_a_failed_call_parks_it() {
    let (fx, tool) = a_failed_attempt_then_a_cancel().await;
    let mut canceller = rakka_agent_workflow::AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new("fleet-canceller"),
        fx.fleet_store.clone(),
        fx.workflow_store.clone(),
        AgentDispatcherFleetSettings::new(16, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    );
    let cancellation = canceller
        .cancel_run_dispatches(&rakka_agent::workflow_run_id(&run_scope()))
        .await
        .expect("the fleet cancels the run's dispatches");
    assert_eq!(cancellation.cancelled_entries, 1);
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 1));

    let pass = fx.one_pass().await;

    assert_eq!(
        (pass.parked_indeterminate, pass.cancelled),
        (1, 0),
        "the sweep parks the call, and has no ticket of its own to settle"
    );
    assert_the_call_is_parked(&fx, &tool).await;
}

/// A fence before issue #206 cancelled the row and was lost before it wrote
/// the fleet entry or told the run. The claim that next takes the ticket
/// finds the cancelled row, and parks the call from it.
#[tokio::test]
async fn an_earlier_builds_cancelled_row_is_parked_by_the_claim() {
    let (fx, tool) = a_failed_attempt_then_a_cancel().await;
    cancel_the_row_as_an_earlier_fence_did(&fx).await;
    fx.expire_lease();

    // The claim alone: the per-run half would answer first.
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(
        (pass.claimed, pass.parked_indeterminate, pass.cancelled),
        (1, 1, 1),
        "the claim settles the ticket to follow the row, and parks the call"
    );
    assert_the_call_is_parked(&fx, &tool).await;
}

/// The same row, reached by the per-run half of a pass: the sweep reads the
/// cancelled row first, and parks the call from it.
#[tokio::test]
async fn an_earlier_builds_cancelled_row_is_parked_by_the_sweep() {
    let (fx, tool) = a_failed_attempt_then_a_cancel().await;
    cancel_the_row_as_an_earlier_fence_did(&fx).await;
    fx.expire_lease();

    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1);
    assert_the_call_is_parked(&fx, &tool).await;
}

// ---------------------------------------------------------------------------
// A model call a deployment raised to `Idempotent`: cancelled with its run.
// ---------------------------------------------------------------------------

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

/// The model call's outbox row: its status, and the attempts it counts.
async fn model_row(fx: &AuthorityFixture) -> (OutboxStatus, u32) {
    let effect = model_effect(fx)
        .await
        .expect("the run holds its model call");
    let row = fx.outbox_row(&effect).await.expect("the ticket has a row");
    (row.status(), row.attempts().attempts())
}

/// A run whose model call a deployment raised to `Idempotent` with three
/// attempts, as `AgentModelRetryPolicy` allows for a provider with an
/// idempotency key. Its first attempt is lost after `Started`, a claim counts
/// it and dies before the retry, and the run is then cancelled.
async fn an_idempotent_model_call_with_a_counted_attempt_then_a_cancel() -> AuthorityFixture {
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, proposing_turn("done"))
        .with_retry_policy(rakka_agent::AgentModelRetryPolicy {
            safety_class: rakka_agent::AgentEffectSafetyClass::Idempotent,
            max_attempts: 3,
        })
        .expect("the policy is valid");
    let fx = AuthorityFixture::over(
        adapter,
        rakka_agent::AgentToolRegistry::new(),
        Some(AgentEffectSpec::idempotent(3).expect("the spec is valid")),
    );
    fx.start().await;
    for _ in 0..4 {
        fx.settle().await;
        if model_effect(&fx).await.map(|effect| effect.status) == Some(AgentRunEffectStatus::Ready)
        {
            break;
        }
    }
    assert_eq!(
        model_effect(&fx).await.map(|effect| effect.status),
        Some(AgentRunEffectStatus::Ready),
        "the model call's ticket is out"
    );
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    fx.expire_lease();
    assert!(fx.one_pass().await.died, "the worker dies after Started");
    fx.probe.arm(AgentDispatchWindow::BeforeStarted);
    fx.expire_lease();
    assert!(
        fx.one_pass().await.died,
        "the counting claim dies before its retry"
    );
    assert_eq!(model_row(&fx).await, (OutboxStatus::Failed, 1));
    cancel_the_run(&fx).await;
    fx.expire_lease();
    fx
}

/// The model call is cancelled with its run, charged the attempt it made,
/// and the cancel ends the run.
async fn assert_the_model_call_is_cancelled(
    fx: &AuthorityFixture,
    pass: &rakka_agent::AgentDispatchPass,
) {
    let effect = model_effect(fx)
        .await
        .expect("the run holds its model call");
    assert_eq!(
        (effect.status, effect.attempts, pass.parked_indeterminate),
        (AgentRunEffectStatus::Cancelled, 1, 0),
        "the model call is cancelled, charged its attempt, and not parked"
    );
    assert_eq!(run_status(fx).await, Some(AgentRunStatus::Cancelled));
}

/// A completion changes nothing outside the run, and the cancelled run would
/// throw it away, so the fence cancels a model call whatever its safety
/// class: an operator is not asked whether a completion "executed". Parked,
/// the cancel waited on that answer.
#[tokio::test]
async fn an_idempotent_model_call_with_a_counted_attempt_is_cancelled_with_its_run() {
    let fx = an_idempotent_model_call_with_a_counted_attempt_then_a_cancel().await;

    let pass = fx.one_pass().await;

    assert_the_model_call_is_cancelled(&fx, &pass).await;
}

/// The same model call, its row cancelled by the workflow tier's own
/// cancellation and read by the per-run sweep: cancelled too.
#[tokio::test]
async fn the_fleets_own_cancellation_of_an_idempotent_model_call_cancels_it() {
    let fx = an_idempotent_model_call_with_a_counted_attempt_then_a_cancel().await;
    let mut canceller = rakka_agent_workflow::AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new("fleet-canceller"),
        fx.fleet_store.clone(),
        fx.workflow_store.clone(),
        AgentDispatcherFleetSettings::new(16, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    );
    canceller
        .cancel_run_dispatches(&rakka_agent::workflow_run_id(&run_scope()))
        .await
        .expect("the fleet cancels the run's dispatches");
    assert_eq!(model_row(&fx).await, (OutboxStatus::Cancelled, 1));

    let pass = fx.one_pass().await;

    assert_the_model_call_is_cancelled(&fx, &pass).await;
}

/// One crash window parks under another code. A claim that finds the row
/// cancelled finishes the fleet entry from the row before it tells the run;
/// lost between the two, it leaves an entry that no longer reads as one that
/// may never have started, so the sweep parks the call from its general arm,
/// under `ticket-row-settled`. The call is parked either way; only the code
/// differs from the fence's. (Found by the review of issue #206; left as it
/// is, and documented.)
#[tokio::test]
async fn a_claim_lost_between_its_fleet_write_and_its_tell_leaves_the_park_to_the_sweep() {
    let (fx, tool) = a_failed_attempt_then_a_cancel().await;
    cancel_the_row_as_an_earlier_fence_did(&fx).await;
    fx.expire_lease();

    fx.fx
        .runs
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect_err("the claim's tell is refused");
    fx.fx
        .runs
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    fx.fx.runs.survive();
    assert_eq!(
        tool_call(&fx).await.0,
        AgentRunEffectStatus::Ready,
        "the run was not told"
    );

    fx.expire_lease();
    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1);
    assert_eq!(
        tool_call(&fx).await,
        (
            AgentRunEffectStatus::Indeterminate,
            Some("ticket-row-settled".to_string()),
            1
        )
    );
    assert_eq!(
        run_status(&fx).await,
        Some(AgentRunStatus::WaitingForReconciliation)
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1, "the retry never ran");
}
