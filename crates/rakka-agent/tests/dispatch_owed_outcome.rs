//! A ticket that is finished while its run was never told (issue #134).
//!
//! A dispatch worker spends a ticket's budget by writing the outbox row
//! `Exhausted`, then the fleet entry, then telling the run, and it can be
//! lost between any two. The row is then finished, the run still waits, and
//! what the worker knew is on the row: its attempt count and the last
//! failure line.
//!
//! A claim that finds a finished row under an effect the run still waits on
//! starts no attempt. It settles the fleet entry to follow the row, and then
//! tells the run what the row says: for an `Exhausted` row, the outcome the
//! lost worker was about to deliver, less the collaborator's reason, which
//! no durable record keeps. Each crashed case below is held against a twin
//! run where nothing crashed.
//!
//! The two tests named `pinned_` are reproductions of the other side of the
//! window, where no claim will come. They state the behaviour as it stands.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    CrashPoint, DeterministicModelAdapter, ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor, AgentDispatchWindow,
    AgentEffectSpec, AgentModelTurn, AgentReconciliationFinding, AgentReconciliationProtocolRef,
    AgentRunEffect, AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand,
    AgentRunScope, AgentRunStatus, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{OutboxMessageId, OutboxStatus, WorkflowTelemetryEvent};
use rakka_agent_workflow::{
    AgentDispatchEntry, AgentDispatchStatus, AgentDispatcherFleet, AgentDispatcherFleetSettings,
    AgentDispatcherWorkerId, AgentEphemeralCredential,
};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_tool_call(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("call-1").expect("the call id is valid"),
                    AgentToolId::new(TOOL).expect("the tool id is valid"),
                    serde_json::json!({ "amount": 42 }),
                )
                .expect("the tool call is bounded"),
            ),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                    .expect("the proposal is inline-bounded"),
            ),
        )
}

/// A run whose tool ticket is in the outbox, under `spec`, with `tools`, and
/// with the reconciler a reconcileable effect's recovery asks.
async fn ticket(
    spec: &AgentEffectSpec,
    tools: Option<Arc<dyn AgentDispatchToolExecutor>>,
    reconciler: Option<&ScriptedReconciler>,
) -> (AuthorityFixture, AgentRunEffect) {
    let mut fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    );
    if let Some(tools) = tools {
        fx = fx.with_tool_executor(tools);
    }
    if let Some(reconciler) = reconciler {
        fx = fx.with_reconciler(Arc::new(reconciler.clone()));
    }
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    (fx, effect)
}

fn reconcileable(max_attempts: u32) -> AgentEffectSpec {
    AgentEffectSpec::reconcileable(
        AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid"),
        max_attempts,
    )
    .expect("the spec is valid")
}

fn idempotent(max_attempts: u32) -> AgentEffectSpec {
    AgentEffectSpec::idempotent(max_attempts).expect("the spec is valid")
}

/// What the run records for the effect once it is resolved: the part a
/// rebuilt outcome must share with the one an uncrashed worker delivers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    status: AgentRunEffectStatus,
    code: Option<String>,
    attempts: u32,
}

/// The effect's record, and whether it carries a failure reason.
async fn recorded(fx: &AuthorityFixture) -> (Recorded, bool) {
    let effect = fx.effect_at(1).await.expect("the effect is held");
    (
        Recorded {
            status: effect.status,
            code: effect.last_error_code.clone(),
            attempts: effect.attempts,
        },
        effect.last_error_reason.is_some(),
    )
}

async fn row_status(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<OutboxStatus> {
    fx.outbox_row(effect).await.map(|row| row.status())
}

/// Finishes the effect's outbox row through the substrate, as a worker that
/// was then lost would have: `Exhausted` with `line`, `Dispatched`, or
/// `Cancelled`.
async fn finish_row(
    fx: &AuthorityFixture,
    effect: &AgentRunEffect,
    status: OutboxStatus,
    line: &str,
) {
    finish_row_in(&fx.workflow_store, &fx.wf_clock, effect, status, line).await;
    assert_eq!(row_status(fx, effect).await, Some(status));
}

/// [`finish_row`] over the stores alone, for a store hook to call.
async fn finish_row_in(
    store: &WorkflowStore,
    clock: &SharedAtomicWorkflowClock,
    effect: &AgentRunEffect,
    status: OutboxStatus,
    line: &str,
) {
    let mut inbox = rakka_agent_workflow::AgentRunInbox::with_clock(
        rakka_agent::workflow_run_id(&run_scope()),
        store.clone(),
        clock.clone(),
    );
    inbox.recover().await.expect("the inbox recovers");
    let id = OutboxMessageId::new(effect.dispatch_ticket_id().as_str());
    let substrate = inbox.inner_mut();
    match status {
        OutboxStatus::Exhausted => {
            substrate
                .record_outbox_exhausted(&id, line)
                .await
                .expect("the row is exhausted");
        }
        OutboxStatus::Dispatched => {
            substrate
                .record_outbox_success(&id)
                .await
                .expect("the row records its success");
        }
        OutboxStatus::Cancelled => {
            substrate
                .record_outbox_cancelled(&id, line)
                .await
                .expect("the row is cancelled");
        }
        other => panic!("{other:?} is not a finished status"),
    }
}

/// Registers the run and runs one worker pass whose second fleet write, the
/// one that would finish the entry, is refused: the worker is lost after
/// the outbox write that spent the budget and before the fleet write.
async fn lose_the_worker_before_its_fleet_write(fx: &AuthorityFixture, effect: &AgentRunEffect) {
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    fx.fleet_store.crash_at(2, CrashPoint::BeforeWrite);
    pipeline
        .claim_and_execute()
        .await
        .expect_err("the settle's fleet write is refused");
    fx.fleet_store
        .assert_crash_fired(2, CrashPoint::BeforeWrite);
    fx.fleet_store.survive();
    assert_eq!(row_status(fx, effect).await, Some(OutboxStatus::Exhausted));
    assert_eq!(
        fleet_entry(fx, effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Claimed)
    );
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
}

/// A first attempt whose worker dies after the durable `Started` write,
/// before the target is invoked. The row is left `Dispatching`.
async fn lose_the_first_attempt_after_started(fx: &AuthorityFixture, effect: &AgentRunEffect) {
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let died = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(died.died);
    assert_eq!(
        row_status(fx, effect).await,
        Some(OutboxStatus::Dispatching)
    );
    fx.expire_lease();
}

fn assert_entry_follows_the_row(entry: Option<AgentDispatchEntry>, status: AgentDispatchStatus) {
    let entry = entry.expect("the entry is kept");
    assert_eq!(entry.status, status);
    if status != AgentDispatchStatus::Completed {
        assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-settled"));
    }
}

/// The fleet index entry for one effect's ticket, if it is registered.
async fn fleet_entry(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<AgentDispatchEntry> {
    let record = fx
        .fleet_store
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")?;
    record
        .state
        .entry(&rakka_agent_workflow::agent_dispatch_id(
            &rakka_agent::workflow_run_id(&run_scope()),
            &effect.dispatch_ticket_id(),
        ))
        .cloned()
}

/// A tool that always fails definitively, and counts its calls.
#[derive(Default)]
struct CountingRefusal {
    calls: AtomicUsize,
}

impl CountingRefusal {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn refusing() -> Arc<CountingRefusal> {
    Arc::new(CountingRefusal::default())
}

impl AgentDispatchToolExecutor for CountingRefusal {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Err(AgentDispatchError::Definitive {
                code: "tool-refused".to_string(),
                message: "the tool refused the call".to_string(),
            })
        })
    }
}

// ---------------------------------------------------------------------------
// A claim on a finished row starts nothing and tells the run.
// ---------------------------------------------------------------------------

/// A failed attempt spent the budget and its worker was lost before the
/// fleet write. The next lease finds the row `Exhausted` and the run still
/// waiting. It does not call the tool again: it tells the run what the row
/// says, which is what the twin's worker told its run, less the reason.
#[tokio::test]
async fn a_failed_attempts_exhaustion_lost_before_the_fleet_write_is_told() {
    let spec = idempotent(3);
    let (twin, twin_has_reason) = {
        let (fx, _effect) = ticket(&spec, Some(refusing()), None).await;
        fx.pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the pass runs");
        recorded(&fx).await
    };
    assert_eq!(twin.status, AgentRunEffectStatus::Exhausted);
    assert!(twin_has_reason, "an uncrashed worker delivers the reason");

    let tool = refusing();
    let (fx, effect) = ticket(&spec, Some(tool.clone()), None).await;
    lose_the_worker_before_its_fleet_write(&fx, &effect).await;
    fx.expire_lease();
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(tool.calls(), 1, "the exhausted row was not run again");
    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (1, 0, 1));
    let (told, has_reason) = recorded(&fx).await;
    assert_eq!(told, twin);
    assert!(!has_reason, "no durable record keeps the reason");
    assert_entry_follows_the_row(
        fleet_entry(&fx, &effect).await,
        AgentDispatchStatus::Exhausted,
    );
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
    );
}

/// The same for a reconcileable effect. A failed attempt exhausts it like
/// any other, and its worker tells the run `Exhausted`; so does the repair.
/// The reconciler is not asked: nothing here was ambiguous.
#[tokio::test]
async fn the_same_for_a_reconcileable_effect_is_told_exhausted() {
    let spec = reconcileable(3);
    let twin = {
        let (fx, _effect) = ticket(&spec, Some(refusing()), Some(&ScriptedReconciler::new())).await;
        fx.pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the pass runs");
        recorded(&fx).await.0
    };
    assert_eq!(twin.status, AgentRunEffectStatus::Exhausted);

    let tool = refusing();
    let reconciler = ScriptedReconciler::new();
    let (fx, effect) = ticket(&spec, Some(tool.clone()), Some(&reconciler)).await;
    lose_the_worker_before_its_fleet_write(&fx, &effect).await;
    fx.expire_lease();
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!((tool.calls(), reconciler.queries()), (1, 0));
    assert_eq!(recorded(&fx).await.0, twin);
}

/// The budget was spent recovering an attempt whose worker was lost after
/// `Started`, and the recovering worker was lost before its fleet write.
#[tokio::test]
async fn an_ambiguous_budget_lost_before_the_fleet_write_is_told() {
    let spec = idempotent(1);
    let twin = {
        let (fx, effect) = ticket(&spec, None, None).await;
        lose_the_first_attempt_after_started(&fx, &effect).await;
        fx.pipeline()
            .claim_and_execute()
            .await
            .expect("the pass runs");
        recorded(&fx).await.0
    };
    assert_eq!(
        (twin.status, twin.code.as_deref()),
        (
            AgentRunEffectStatus::Exhausted,
            Some("dispatcher-lost-after-started")
        )
    );

    let (fx, effect) = ticket(&spec, None, None).await;
    lose_the_first_attempt_after_started(&fx, &effect).await;
    // The recovery's claim is its first fleet write, the settle its second.
    fx.fleet_store.crash_at(2, CrashPoint::BeforeWrite);
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect_err("the settle's fleet write is refused");
    fx.fleet_store
        .assert_crash_fired(2, CrashPoint::BeforeWrite);
    fx.fleet_store.survive();
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
    );
    fx.expire_lease();
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!((pass.invoked, pass.delivered), (0, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(recorded(&fx).await.0, twin);
}

/// A reconcileable effect whose first attempt was lost after `Started`,
/// with one attempt in its budget, behind `reconciler`. The recovery asks
/// the reconciler, burns the attempt, and parks.
async fn a_reconciliation_that_spends_the_budget(
    reconciler: &ScriptedReconciler,
) -> (AuthorityFixture, AgentRunEffect) {
    let (fx, effect) = ticket(&reconcileable(1), None, Some(reconciler)).await;
    lose_the_first_attempt_after_started(&fx, &effect).await;
    (fx, effect)
}

/// The same, with the recovering worker lost after the write that spent the
/// budget and before it told the run.
async fn lost_before_the_park(
    reconciler: &ScriptedReconciler,
) -> (AuthorityFixture, AgentRunEffect) {
    let (fx, effect) = a_reconciliation_that_spends_the_budget(reconciler).await;
    fx.fx.runs.crash_at(1, CrashPoint::BeforeWrite);
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect_err("the run's store refuses the park");
    fx.fx.runs.assert_crash_fired(1, CrashPoint::BeforeWrite);
    fx.fx.runs.survive();
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
    );
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    fx.expire_lease();
    (fx, effect)
}

/// A reconciliation established nothing and spent the budget, and the
/// worker was lost before it parked the run. The row's line names the
/// finding, so the run is parked as that worker would have parked it, and
/// the reconciler is not asked again: the budget to ask it is gone.
#[tokio::test]
async fn a_reconciliation_that_established_nothing_lost_before_the_park_is_parked() {
    let twin = {
        let reconciler = ScriptedReconciler::new();
        let (fx, _effect) = a_reconciliation_that_spends_the_budget(&reconciler).await;
        fx.pipeline()
            .claim_and_execute()
            .await
            .expect("the pass runs");
        recorded(&fx).await.0
    };
    assert_eq!(
        (twin.status, twin.code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("reconciliation-exhausted")
        )
    );

    let reconciler = ScriptedReconciler::new();
    let (fx, effect) = lost_before_the_park(&reconciler).await;
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!((pass.invoked, pass.parked_indeterminate), (0, 1));
    assert_eq!(reconciler.queries(), 1, "the reconciler is not asked again");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(recorded(&fx).await.0, twin);
    assert_entry_follows_the_row(
        fleet_entry(&fx, &effect).await,
        AgentDispatchStatus::Exhausted,
    );
}

/// The reconciler answers with an outcome that belongs to another call,
/// which establishes nothing either, under its own code.
#[tokio::test]
async fn the_same_through_the_mismatch_arm_parks_under_its_code() {
    let foreign = || {
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::Executed {
            outcome: Box::new(AgentRunEffectOutcome::Tool {
                call_id: AgentToolCallId::new("another-call").expect("the call id is valid"),
                content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-1" }))
                    .expect("the content is inline-bounded"),
            }),
        })
    };
    let twin = {
        let (fx, _effect) = a_reconciliation_that_spends_the_budget(&foreign()).await;
        fx.pipeline()
            .claim_and_execute()
            .await
            .expect("the pass runs");
        recorded(&fx).await.0
    };
    assert_eq!(
        (twin.status, twin.code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("effect-outcome-mismatch")
        )
    );

    let (fx, _effect) = lost_before_the_park(&foreign()).await;
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(recorded(&fx).await.0, twin);
}

/// A registered ticket whose row was then finished by a worker that was
/// lost, with nothing told to the run.
async fn a_registered_ticket_with_a_finished_row(
    spec: &AgentEffectSpec,
    reconciler: Option<&ScriptedReconciler>,
    status: OutboxStatus,
    line: &str,
) -> (AuthorityFixture, AgentRunEffect) {
    let (fx, effect) = ticket(spec, None, reconciler).await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    finish_row(&fx, &effect, status, line).await;
    (fx, effect)
}

/// A line no exhausting site of this build writes says the budget is spent
/// and not how. Any effect but a reconcileable one is exhausted under the
/// row's own code; a reconcileable one parks.
#[tokio::test]
async fn a_row_whose_line_has_no_known_shape() {
    let (fx, _effect) = a_registered_ticket_with_a_finished_row(
        &idempotent(3),
        None,
        OutboxStatus::Exhausted,
        "something else happened",
    )
    .await;
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");
    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (AgentRunEffectStatus::Exhausted, Some("ticket-row-settled"))
    );

    let reconciler = ScriptedReconciler::new();
    let (fx, _effect) = a_registered_ticket_with_a_finished_row(
        &reconcileable(3),
        Some(&reconciler),
        OutboxStatus::Exhausted,
        "something else happened",
    )
    .await;
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");
    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("reconciliation-exhausted")
        )
    );
    assert_eq!(reconciler.queries(), 0);
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// The row says the call succeeded, and its result reached nobody. Nothing
/// can produce that result now, so the run is parked, and the target is not
/// called a second time.
#[tokio::test]
async fn a_dispatched_row_nobody_delivered_is_parked() {
    let (fx, effect) =
        a_registered_ticket_with_a_finished_row(&idempotent(3), None, OutboxStatus::Dispatched, "")
            .await;

    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!((pass.invoked, pass.parked_indeterminate), (0, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("ticket-row-settled")
        )
    );
    assert_entry_follows_the_row(
        fleet_entry(&fx, &effect).await,
        AgentDispatchStatus::Completed,
    );
}

/// A cancelled row under a live run's waiting effect: the ticket was ended
/// and nothing says with what. Parked, not invoked.
#[tokio::test]
async fn a_cancelled_row_on_a_live_run_is_parked() {
    let (fx, effect) = a_registered_ticket_with_a_finished_row(
        &idempotent(3),
        None,
        OutboxStatus::Cancelled,
        "cancelled",
    )
    .await;

    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!((pass.invoked, pass.parked_indeterminate), (0, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("ticket-row-settled")
        )
    );
    assert_entry_follows_the_row(
        fleet_entry(&fx, &effect).await,
        AgentDispatchStatus::Cancelled,
    );
}

/// An operator cancels the run, which stays `Cancelling` on its ready effect.
async fn cancel_the_run(fx: &AuthorityFixture) {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("cancellation applies");
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelling
    );
}

/// The wind-down branch cancels a ticket that provably never started. An
/// `Exhausted` row is a ticket that was attempted until its budget was
/// spent, so a winding-down run is told that, and not `Cancelled`.
#[tokio::test]
async fn a_cancelling_run_is_told_what_its_exhausted_row_says() {
    let (fx, _effect) = a_registered_ticket_with_a_finished_row(
        &idempotent(3),
        None,
        OutboxStatus::Exhausted,
        "tool-refused: the tool refused the call",
    )
    .await;
    cancel_the_run(&fx).await;

    fx.pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (AgentRunEffectStatus::Exhausted, Some("tool-refused"))
    );
}

/// The worker's lease lapses between its claim and its look at the row. It
/// no longer holds the ticket, and the worker that takes it over may be
/// about to tell the run itself. The fleet answers its settle
/// `claim-fenced`: it tells the run nothing, and its pass does not fail.
/// The next lease tells the run.
#[tokio::test]
async fn a_worker_whose_claim_lapsed_tells_the_run_nothing() {
    let (fx, effect) = a_registered_ticket_with_a_finished_row(
        &idempotent(3),
        None,
        OutboxStatus::Exhausted,
        "tool-refused: the tool refused the call",
    )
    .await;
    // The claim's first read of the run's workflow record is its look at
    // the row. By then the lease has run out.
    let clock = fx.wf_clock.clone();
    fx.workflow_store.before_next_load(Box::new(move || {
        Box::pin(async move { clock.advance(LEASE_MS + 1) })
    }));

    let late = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("a fenced settle does not fail the pass");

    assert_eq!((late.claimed, late.delivered, late.invoked), (1, 0, 0));
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Claimed)
    );

    let next = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");
    assert_eq!((next.claimed, next.delivered), (1, 1));
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Exhausted)
    );
}

/// The row is finished after the claim looked at it and before the claim's
/// `Dispatching` mark: a late worker's failure spent the budget in between.
/// The mark is the write that precedes every invocation, and it sets a
/// row's status whatever the row held, so it looks at the row in the same
/// read it is computed from. A finished row is not marked and not invoked;
/// the claim ends as it does when it finds the row finished at once.
#[tokio::test]
async fn a_row_finished_between_the_claims_read_and_the_mark_is_not_invoked() {
    let (fx, effect) = ticket(&idempotent(3), None, None).await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    // The claim's first load of the run's workflow record is its look at
    // the row, and its second is the mark's. The late worker's write lands
    // before the second.
    let rearm = fx.workflow_store.clone();
    let (store, clock, late) = (
        fx.workflow_store.clone(),
        fx.wf_clock.clone(),
        effect.clone(),
    );
    fx.workflow_store.before_next_load(Box::new(move || {
        Box::pin(async move {
            rearm.before_next_load(Box::new(move || {
                Box::pin(async move {
                    finish_row_in(
                        &store,
                        &clock,
                        &late,
                        OutboxStatus::Exhausted,
                        "tool-refused: the tool refused the call",
                    )
                    .await;
                })
            }));
        })
    }));

    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!((pass.invoked, pass.delivered), (0, 1));
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
    );
    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (AgentRunEffectStatus::Exhausted, Some("tool-refused"))
    );
    assert_entry_follows_the_row(
        fleet_entry(&fx, &effect).await,
        AgentDispatchStatus::Exhausted,
    );
}

// ---------------------------------------------------------------------------
// The other side of the window: no claim will come.
// ---------------------------------------------------------------------------

/// A failed attempt spent the budget, the worker finished the fleet entry,
/// and it was lost before it told the run: the run's store refuses the
/// delivery. The row and the entry are `Exhausted` and the effect is
/// `Ready`. Nothing claims a finished entry.
async fn lose_the_worker_after_its_fleet_write(
    spec: &AgentEffectSpec,
) -> (AuthorityFixture, AgentRunEffect, Arc<CountingRefusal>) {
    let tool = refusing();
    let (fx, effect) = ticket(spec, Some(tool.clone()), None).await;
    fx.fx.runs.crash_at(1, CrashPoint::BeforeWrite);
    fx.pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("the delivery is refused");
    fx.fx.runs.assert_crash_fired(1, CrashPoint::BeforeWrite);
    fx.fx.runs.survive();
    assert_eq!(tool.calls(), 1);
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Exhausted)
    );
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    (fx, effect, tool)
}

/// The per-run half of a pass is what tells this run: no claim will come
/// for a finished entry. It waits until the entry has been finished for a
/// lease, and then tells the run what the row says, once.
#[tokio::test]
async fn a_worker_lost_after_the_fleet_write_is_repaired_after_a_lease() {
    let spec = idempotent(3);
    let twin = {
        let (fx, _effect) = ticket(&spec, Some(refusing()), None).await;
        fx.pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the pass runs");
        recorded(&fx).await.0
    };
    let (fx, effect, tool) = lose_the_worker_after_its_fleet_write(&spec).await;

    fx.expire_lease();
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (0, 0, 1));
    assert_eq!(tool.calls(), 1);
    let (told, has_reason) = recorded(&fx).await;
    assert_eq!(told, twin);
    assert!(!has_reason);
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Exhausted)
    );

    let again = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(again.delivered, 0, "the run is told once");
}

/// A worker that has just finished the entry is about to tell the run
/// itself, with the collaborator's reason the row does not keep. Until the
/// entry has been finished for a lease, the sweep leaves the run to it.
#[tokio::test]
async fn a_finished_entry_is_not_swept_before_a_lease_has_passed() {
    let (fx, _effect, _tool) = lose_the_worker_after_its_fleet_write(&idempotent(3)).await;
    fx.wf_clock.advance(LEASE_MS / 2);

    let early = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");

    assert_eq!(early.delivered, 0);
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));

    fx.wf_clock.advance(LEASE_MS);
    let late = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");
    assert_eq!(late.delivered, 1);
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Exhausted)
    );
}

/// A finished row under an entry a worker holds, or can still claim, is the
/// claim path's. The holder's call may still be in flight under a row a
/// late worker exhausted, and it delivers its own outcome; a claimable
/// entry is told by the worker that claims it. The sweep tells neither,
/// however long the row has been finished.
#[tokio::test]
async fn a_finished_row_under_a_claim_is_left_to_the_claim_path() {
    let (fx, effect) = ticket(&idempotent(3), Some(refusing()), None).await;
    lose_the_worker_before_its_fleet_write(&fx, &effect).await;

    let held = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");
    assert_eq!(held.delivered, 0, "the lease is still running");

    fx.expire_lease();
    fx.expire_lease();
    let claimable = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");
    assert_eq!(claimable.delivered, 0, "a worker can claim the ticket");
    assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Ready));
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Claimed)
    );
}

/// The budget was spent recovering an attempt lost after `Started`, and the
/// recovering worker was lost after its fleet write.
#[tokio::test]
async fn an_ambiguous_budget_lost_after_the_fleet_write_is_repaired() {
    let (fx, effect) = ticket(&idempotent(1), None, None).await;
    lose_the_first_attempt_after_started(&fx, &effect).await;
    fx.fx.runs.crash_at(1, CrashPoint::BeforeWrite);
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect_err("the delivery is refused");
    fx.fx.runs.assert_crash_fired(1, CrashPoint::BeforeWrite);
    fx.fx.runs.survive();
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Exhausted)
    );
    fx.expire_lease();

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!((pass.invoked, pass.delivered), (0, 1));
    let told = recorded(&fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref(), told.attempts),
        (
            AgentRunEffectStatus::Exhausted,
            Some("dispatcher-lost-after-started"),
            1
        )
    );
}

/// The finished entry was pruned before the run was told. The row is what
/// is left, and its own finishing time stands in for the entry's.
#[tokio::test]
async fn a_finished_row_whose_entry_was_pruned_is_repaired_from_the_row() {
    let (fx, effect, _tool) = lose_the_worker_after_its_fleet_write(&idempotent(3)).await;
    fx.pipeline()
        .prune_fleet(rakka_agent_workflow::AgentTimestampMillis::new(u64::MAX))
        .await
        .expect("the prune runs");
    assert!(fleet_entry(&fx, &effect).await.is_none());

    let early = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");
    assert_eq!(early.delivered, 0, "a lease has not passed since the row");

    fx.expire_lease();
    let late = fx
        .pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");
    assert_eq!(late.delivered, 1);
    let told = recorded(&fx).await.0;
    assert_eq!(told.status, AgentRunEffectStatus::Exhausted);
}

/// A fleet handle of its own over the fixture's fleet record.
fn another_fleet_handle(
    fx: &AuthorityFixture,
) -> AgentDispatcherFleet<FleetStore, SharedAtomicWorkflowClock> {
    AgentDispatcherFleet::with_clock_and_metrics(
        fx.fleet_store.clone(),
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
        AgentDispatcherFleetSettings::new(16, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    )
}

/// A live run whose ticket is finished at the fleet, and whose outbox row
/// is then lost with the run's workflow record.
async fn a_finished_ticket_that_lost_its_row() -> (AuthorityFixture, AgentRunEffect) {
    let (fx, effect) = ticket(&AgentEffectSpec::non_idempotent(), None, None).await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let mut fleet = another_fleet_handle(&fx);
    let claim = fleet
        .claim_due(AgentDispatcherWorkerId::new("worker-lost"))
        .await
        .expect("the claim pass runs")
        .claims
        .remove(0);
    fleet
        .record_claim_failure(
            &claim,
            &WorkflowTelemetryEvent::OutboxDispatchExhausted {
                message_id: OutboxMessageId::new(claim.effect_id.as_str()),
                attempts: 1,
                message: "tool-timeout".to_string(),
            },
        )
        .await
        .expect("the entry is exhausted");
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
    (fx, effect)
}

async fn assert_parked_as_missing(fx: &AuthorityFixture) {
    let told = recorded(fx).await.0;
    assert_eq!(
        (told.status, told.code.as_deref()),
        (
            AgentRunEffectStatus::Indeterminate,
            Some("ticket-row-missing")
        )
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// The run settles and puts a row back, as a run does. Registration leaves
/// a finished entry alone, so nothing will ever claim that row, and a live
/// row under a finished entry is always a row put back: an entry is
/// finished only after its row is finished or gone. The effect is parked as
/// a lost row's is, and the row is finished so it does not sit due for good.
#[tokio::test]
async fn a_live_run_whose_finished_ticket_lost_its_row_is_parked() {
    let (fx, effect) = a_finished_ticket_that_lost_its_row().await;
    fx.expire_lease();
    fx.settle().await;
    assert_eq!(row_status(&fx, &effect).await, Some(OutboxStatus::Pending));

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!(
        (pass.claimed, pass.invoked, pass.parked_indeterminate),
        (0, 0, 1)
    );
    assert_parked_as_missing(&fx).await;
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Cancelled)
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Exhausted),
        "the finished entry is left as it finished"
    );

    let again = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!((again.delivered, again.registered), (0, 0));
}

/// Rewrites the fleet record without any entry's row stamp: the record a
/// build before issue #153 wrote.
async fn strip_the_row_stamps(fx: &AuthorityFixture) {
    let id = rakka_agent_workflow::agent_dispatcher_fleet_persistence_id();
    let record = fx
        .fleet_store
        .load(&id)
        .await
        .expect("the fleet store loads")
        .expect("the fleet has a record");
    let mut state = serde_json::to_value(&record.state).expect("the state encodes");
    for entry in state["entries"]
        .as_object_mut()
        .expect("the entries are a map")
        .values_mut()
    {
        assert!(
            entry
                .as_object_mut()
                .expect("an entry is an object")
                .remove("row_scheduled_at")
                .is_some(),
            "the stamp was there to remove"
        );
    }
    fx.fleet_store
        .compare_and_set(
            &id,
            record.revision,
            serde_json::from_value(state).expect("a record with no stamps decodes"),
        )
        .await
        .expect("the earlier record is written");
}

/// The entry's row stamp is not what says the row was put back, so an entry
/// registered by a build that recorded none is reached too.
#[tokio::test]
async fn the_same_for_an_entry_with_no_stamp() {
    let (fx, effect) = a_finished_ticket_that_lost_its_row().await;
    strip_the_row_stamps(&fx).await;
    fx.expire_lease();
    fx.settle().await;

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!(pass.parked_indeterminate, 1);
    assert_parked_as_missing(&fx).await;
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Cancelled)
    );
}

/// The run has not settled since the loss, so there is no row at all.
#[tokio::test]
async fn a_live_run_whose_finished_ticket_lost_its_row_is_parked_before_it_settles() {
    let (fx, effect) = a_finished_ticket_that_lost_its_row().await;

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");

    assert_eq!(
        (pass.claimed, pass.invoked, pass.parked_indeterminate),
        (0, 0, 1)
    );
    assert_parked_as_missing(&fx).await;
    assert!(fx.outbox_row(&effect).await.is_none());
}

// ---------------------------------------------------------------------------
// What the sweep costs a pass that has nothing to repair.
// ---------------------------------------------------------------------------

/// Counts every load of `store` from now on.
fn count_loads<S>(store: &rakka_agent::testkit::CrashingStateStore<S>, loads: &Arc<AtomicUsize>)
where
    S: rakka_persistence::DurableState,
{
    let (rearm, counted) = (store.clone(), loads.clone());
    store.before_next_load(Box::new(move || {
        Box::pin(async move {
            counted.fetch_add(1, Ordering::SeqCst);
            count_loads(&rearm, &counted);
        })
    }));
}

/// A healthy ticket, registered and due again on every pass until it is
/// claimed. The per-run half reads the run's workflow record twice, once to
/// see that something is due and once after the fleet record, and the fleet
/// record once. The sweep looks at the rows and the entries those reads
/// already hold, and reads nothing more.
#[tokio::test]
async fn a_healthy_due_ticket_costs_the_per_run_half_no_more_reads() {
    let (fx, _effect) = ticket(&idempotent(3), None, None).await;
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let (outbox_loads, fleet_loads) =
        (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    count_loads(&fx.workflow_store, &outbox_loads);
    count_loads(&fx.fleet_store, &fleet_loads);

    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the per-run half runs");

    assert_eq!(
        (
            outbox_loads.load(Ordering::SeqCst),
            fleet_loads.load(Ordering::SeqCst)
        ),
        (2, 1)
    );
}
