//! A finished outbox row stays finished (issue #145).
//!
//! The dispatcher's settle writes used to set a ticket's outbox row whatever
//! it held. A call that outlived its lease and then failed wrote `Failed`
//! over a row another worker had already finished as `Dispatched`: the row
//! was due again, for good. While a finished fleet entry stayed in the
//! record it absorbed the re-registration. Once a prune can remove that
//! entry, the row has to stay finished on its own, so a settle that finds
//! its row already finished writes nothing, and the worker's claim follows
//! the row instead.
//!
//! The same file pins the order every finishing site keeps, the outbox row
//! first and the fleet entry second, and the one write this change does not
//! guard (issue #134).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, SharedAtomicWorkflowClock};
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchPass, AgentDispatchToolExecutor,
    AgentDispatchWindow, AgentEffectSpec, AgentModelTurn, AgentRunEffect, AgentRunScope,
    AgentRunStatus, AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{
    AgentDispatchEntry, AgentDispatchStatus, AgentDispatcherFleet, AgentDispatcherFleetSettings,
    AgentEphemeralCredential, AgentTimestampMillis,
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

async fn row_status(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<OutboxStatus> {
    fx.outbox_row(effect).await.map(|row| row.status())
}

// ---------------------------------------------------------------------------
// A call that outlives its lease.
// ---------------------------------------------------------------------------

/// What the overrunning call answers once another worker has finished the
/// ticket.
#[derive(Clone, Copy)]
enum Late {
    RetryableFailure,
    DefinitiveFailure,
    Success,
}

/// A tool whose first call outlives its lease: while it is still running,
/// the lease lapses and another worker takes the ticket over, calls the tool
/// itself, and finishes. Only then does the first call answer.
struct OverrunningTool {
    calls: AtomicUsize,
    clock: SharedAtomicWorkflowClock,
    other: tokio::sync::Mutex<Option<Pipeline>>,
    other_pass: std::sync::Mutex<Option<AgentDispatchPass>>,
    late: Late,
}

impl OverrunningTool {
    fn new(clock: SharedAtomicWorkflowClock, late: Late) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            clock,
            other: tokio::sync::Mutex::new(None),
            other_pass: std::sync::Mutex::new(None),
            late,
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn other_pass(&self) -> AgentDispatchPass {
        let pass = *self
            .other_pass
            .lock()
            .expect("the pass slot is not poisoned");
        pass.expect("the other worker ran")
    }
}

impl AgentDispatchToolExecutor for OverrunningTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                // The other worker's own call, made on time.
                return Ok(
                    AgentTaskContent::inline(serde_json::json!({ "paid": "on-time" }))
                        .expect("the result is inline-bounded"),
                );
            }
            self.clock.advance(LEASE_MS + 1);
            let mut other = self
                .other
                .lock()
                .await
                .take()
                .expect("the other worker is wired before the first call");
            let pass = other
                .claim_and_execute()
                .await
                .expect("the other worker's pass runs");
            *self
                .other_pass
                .lock()
                .expect("the pass slot is not poisoned") = Some(pass);
            match self.late {
                Late::RetryableFailure => Err(AgentDispatchError::Invocation {
                    code: "tool-timeout",
                    message: "the call answered after its lease".to_string(),
                }),
                Late::DefinitiveFailure => Err(AgentDispatchError::Definitive {
                    code: "tool-refused".to_string(),
                    message: "the call answered after its lease".to_string(),
                }),
                Late::Success => Ok(AgentTaskContent::inline(
                    serde_json::json!({ "paid": "late" }),
                )
                .expect("the result is inline-bounded")),
            }
        })
    }
}

/// Runs worker A's pass over a ready tool ticket whose first call overruns
/// its lease, and answers the fixture, the tool, the effect and A's pass.
async fn overrun(
    late: Late,
) -> (
    AuthorityFixture,
    Arc<OverrunningTool>,
    AgentRunEffect,
    Result<AgentDispatchPass, AgentDispatchError>,
) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    let tool = OverrunningTool::new(fx.wf_clock.clone(), late);
    let fx = fx.with_tool_executor(tool.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    *tool.other.lock().await = Some(fx.worker("worker-b", fx.tools.clone(), None));

    let pass = fx
        .worker("worker-a", fx.tools.clone(), None)
        .pump_run(&run_scope())
        .await;
    (fx, tool, effect, pass)
}

/// Prunes every finished entry, as an operator command does.
async fn prune_everything(fx: &AuthorityFixture) -> usize {
    AgentDispatcherFleet::with_clock_and_metrics(
        fx.fleet_store.clone(),
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
        AgentDispatcherFleetSettings::default(),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    )
    .prune_terminal(AgentTimestampMillis::new(u64::MAX))
    .await
    .expect("the prune runs")
    .pruned
}

/// The run goes on from the outcome of the worker that finished the ticket,
/// and ends completed: the late answer changed nothing it holds.
async fn assert_the_run_completes_on_the_on_time_result(
    fx: &AuthorityFixture,
    tool: &OverrunningTool,
) {
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
    assert_eq!(tool.calls(), 2, "the tool was not called again");
}

#[tokio::test]
async fn a_late_failure_does_not_reopen_a_row_another_worker_finished() {
    let (fx, tool, effect, pass) = overrun(Late::RetryableFailure).await;

    // The other worker took the ticket over and finished it.
    let other = tool.other_pass();
    assert_eq!((other.claimed, other.delivered), (1, 1));
    assert_eq!(
        tool.calls(),
        2,
        "the lease's own ambiguity: both workers called"
    );
    // The late failure wrote nothing over the finished row.
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Dispatched)
    );
    let pass = pass.expect("a claim whose row is already finished is over, not failed");
    assert_eq!(pass.failed_attempts, 0, "no attempt was burned");
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Completed)
    );

    // And with the finished entry pruned, nothing brings the ticket back.
    assert!(prune_everything(&fx).await >= 1);
    fx.pipeline()
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    assert!(
        fleet_entry(&fx, &effect).await.is_none(),
        "a finished ticket is not registered again"
    );
    assert_the_run_completes_on_the_on_time_result(&fx, &tool).await;
}

#[tokio::test]
async fn a_late_definitive_failure_does_not_reopen_it_either() {
    let (fx, tool, effect, pass) = overrun(Late::DefinitiveFailure).await;

    assert_eq!(tool.calls(), 2);
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Dispatched)
    );
    let pass = pass.expect("a claim whose row is already finished is over, not failed");
    assert_eq!(pass.failed_attempts, 0);
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Completed)
    );
    assert_the_run_completes_on_the_on_time_result(&fx, &tool).await;
}

#[tokio::test]
async fn a_late_success_leaves_the_row_and_the_fleet_as_they_are() {
    let (fx, tool, effect, pass) = overrun(Late::Success).await;

    assert_eq!(tool.calls(), 2);
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Dispatched)
    );
    pass.expect("a claim whose row is already finished is over, not failed");
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Completed)
    );
    assert_the_run_completes_on_the_on_time_result(&fx, &tool).await;
}

// ---------------------------------------------------------------------------
// Every finishing site writes the outbox row before the fleet entry.
// ---------------------------------------------------------------------------

/// A run whose tool ticket is in the outbox, under `spec`, with `tools`.
async fn ready_tool_ticket(
    spec: &AgentEffectSpec,
    tools: Option<Arc<dyn AgentDispatchToolExecutor>>,
) -> (AuthorityFixture, AgentRunEffect) {
    let mut fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    );
    if let Some(tools) = tools {
        fx = fx.with_tool_executor(tools);
    }
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    (fx, effect)
}

/// A tool that always fails definitively.
struct RefusingTool;

impl AgentDispatchToolExecutor for RefusingTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            Err(AgentDispatchError::Definitive {
                code: "tool-refused".to_string(),
                message: "the tool refused the call".to_string(),
            })
        })
    }
}

/// Registers the run, then runs one claim pass in which the fleet write that
/// would finish the entry is refused by the store, and answers the row's
/// status at that moment.
///
/// The claim is the pass's first fleet write and the settle its second, so
/// the second is the one refused. The row is read after the pass failed:
/// whatever it holds was written before the fleet write was attempted.
async fn row_when_the_fleet_write_is_refused(
    fx: &AuthorityFixture,
    effect: &AgentRunEffect,
) -> Option<OutboxStatus> {
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    fx.fleet_store.crash_at(2, CrashPoint::BeforeWrite);
    let error = pipeline
        .claim_and_execute()
        .await
        .expect_err("the settle's fleet write is refused");
    fx.fleet_store
        .assert_crash_fired(2, CrashPoint::BeforeWrite);
    fx.fleet_store.survive();
    assert_eq!(error.code(), "store-error");
    assert_eq!(
        fleet_entry(fx, effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Claimed),
        "the fleet entry was not finished"
    );
    row_status(fx, effect).await
}

#[tokio::test]
async fn a_success_finishes_the_row_before_the_fleet_entry() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;

    let row = row_when_the_fleet_write_is_refused(&fx, &effect).await;

    assert_eq!(row, Some(OutboxStatus::Dispatched));
}

#[tokio::test]
async fn a_definitive_failure_finishes_the_row_before_the_fleet_entry() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, Some(Arc::new(RefusingTool))).await;

    let row = row_when_the_fleet_write_is_refused(&fx, &effect).await;

    assert_eq!(row, Some(OutboxStatus::Exhausted));
}

#[tokio::test]
async fn a_spent_retry_budget_finishes_the_row_before_the_fleet_entry() {
    // One attempt: the worker dies after its durable `Started`, and the
    // recovery that burns the ambiguous attempt spends the budget.
    let spec = AgentEffectSpec::idempotent(1).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let died = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(died.died);
    fx.expire_lease();

    let row = row_when_the_fleet_write_is_refused(&fx, &effect).await;

    assert_eq!(row, Some(OutboxStatus::Exhausted));
}

#[tokio::test]
async fn a_ticket_whose_intent_is_resolved_finishes_the_row_before_the_fleet_entry() {
    // The worker dies after it delivered the result and before it recorded
    // the success: the intent is resolved and the row is still `Dispatching`.
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    fx.probe.arm(AgentDispatchWindow::AfterResultDelivery);
    let died = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(died.died);
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Dispatching)
    );
    fx.expire_lease();

    let row = row_when_the_fleet_write_is_refused(&fx, &effect).await;

    assert_eq!(row, Some(OutboxStatus::Cancelled));
}

// ---------------------------------------------------------------------------
// The write this change does not guard.
// ---------------------------------------------------------------------------

/// Issue #134, pinned as it stands: a claim on an `Exhausted` row whose
/// intent is still unresolved starts an attempt.
///
/// The settle writes are guarded against a finished row; the `Dispatching`
/// mark is not. Refusing it here would leave the intent unresolved with
/// nothing left to claim it, which issue #134 has to decide together with
/// the park. When #134 lands, this test changes with it.
#[tokio::test]
async fn a_claim_on_an_exhausted_row_with_an_unresolved_intent_still_starts_an_attempt() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let tool = Arc::new(CountingRefusal::default());
    let (fx, effect) = ready_tool_ticket(&spec, Some(tool.clone())).await;
    // The exhausting outbox write lands, and the worker is lost before the
    // fleet write and before it delivers the exhausted outcome.
    let row = row_when_the_fleet_write_is_refused(&fx, &effect).await;
    assert_eq!(row, Some(OutboxStatus::Exhausted));
    assert_eq!(tool.calls(), 1);
    assert!(
        !fx.effect_at(1)
            .await
            .expect("the effect exists")
            .status
            .is_resolved(),
        "the run never learned the outcome"
    );

    fx.expire_lease();
    fx.pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(
        tool.calls(),
        2,
        "issue #134: the exhausted row was run again"
    );
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
