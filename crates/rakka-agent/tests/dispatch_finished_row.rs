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
    AgentDispatchWindow, AgentEffectSpec, AgentModelTurn, AgentRunEffect, AgentRunEffectStatus,
    AgentRunScope, AgentRunStatus, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
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
// A finished row under an effect the run still waits on (issue #134).
// ---------------------------------------------------------------------------

/// A claim on an `Exhausted` row whose intent is still unresolved starts no
/// attempt. The worker that spent the budget was lost before it finished
/// the fleet entry and told the run; the next lease settles the entry to
/// follow the row and tells the run what the row says.
/// `dispatch_owed_outcome.rs` has the cases.
#[tokio::test]
async fn a_claim_on_an_exhausted_row_with_an_unresolved_intent_tells_the_run_and_invokes_nothing() {
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
    let pass = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the pass runs");

    assert_eq!(tool.calls(), 1, "the exhausted row was not run again");
    assert_eq!((pass.invoked, pass.delivered), (0, 1));
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Exhausted)
    );
    let entry = fleet_entry(&fx, &effect).await.expect("the entry is kept");
    assert_eq!(entry.status, AgentDispatchStatus::Exhausted);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-settled"));
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
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

// ---------------------------------------------------------------------------
// The worker that holds the claim tells the run, when the row was exhausted
// under it.
// ---------------------------------------------------------------------------

/// How a gated call answers once it is released.
#[derive(Clone, Copy, Debug)]
enum Answer {
    DefinitiveFailure,
    RetryableFailure,
}

/// A tool whose calls each wait to be released, so a test can order what two
/// workers' calls answer and when.
struct GatedTool {
    calls: AtomicUsize,
    entered: [tokio::sync::Notify; 2],
    answers: [tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<Answer>>>; 2],
}

impl GatedTool {
    fn new() -> (Arc<Self>, [tokio::sync::oneshot::Sender<Answer>; 2]) {
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let (second_tx, second_rx) = tokio::sync::oneshot::channel();
        let tool = Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered: [tokio::sync::Notify::new(), tokio::sync::Notify::new()],
            answers: [
                tokio::sync::Mutex::new(Some(first_rx)),
                tokio::sync::Mutex::new(Some(second_rx)),
            ],
        });
        (tool, [first_tx, second_tx])
    }
}

impl AgentDispatchToolExecutor for GatedTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(call < 2, "the tool is called by two workers, once each");
            self.entered[call].notify_one();
            let released = self.answers[call]
                .lock()
                .await
                .take()
                .expect("each call is released once");
            match released.await.expect("the test releases every call") {
                Answer::DefinitiveFailure => Err(AgentDispatchError::Definitive {
                    code: "tool-refused".to_string(),
                    message: "the tool refused the call".to_string(),
                }),
                Answer::RetryableFailure => Err(AgentDispatchError::Invocation {
                    code: "tool-timeout",
                    message: "the tool timed out".to_string(),
                }),
            }
        })
    }
}

/// A row can be exhausted under the worker that holds the claim. A call
/// outlives its lease; a second worker takes the ticket over and calls the
/// tool itself; the first call then fails for good and its worker, whose
/// claim has lapsed, exhausts the row and is fenced before it can tell the
/// run. When the second worker's own call fails, it finds the row already
/// exhausted. It holds the claim, so it is the one the run hears from: with
/// nothing delivered, the row would be exhausted, the fleet entry finished,
/// and the run would wait on the effect for good.
#[tokio::test]
async fn the_worker_holding_the_claim_delivers_the_exhaustion_of_a_row_exhausted_under_it() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (tool, [release_first, release_second]) = GatedTool::new();
    let (fx, effect) = ready_tool_ticket(&spec, Some(tool.clone())).await;
    let mut late = fx.worker("worker-late", fx.tools.clone(), None);
    let mut current = fx.worker("worker-current", fx.tools.clone(), None);
    let late_done = tokio::sync::Notify::new();

    let late_pass = async {
        let pass = late.pump_run(&run_scope()).await;
        late_done.notify_one();
        pass
    };
    let current_pass = async {
        // Once the first call is in flight its lease lapses, and the second
        // worker takes the ticket over and calls the tool itself.
        tool.entered[0].notified().await;
        fx.wf_clock.advance(LEASE_MS + 1);
        current.claim_and_execute().await
    };
    let order = async {
        // With both calls in flight, the late one fails for good first...
        tool.entered[1].notified().await;
        release_first
            .send(Answer::DefinitiveFailure)
            .expect("the first call is waiting");
        late_done.notified().await;
        // ...and then the call of the worker that holds the claim fails.
        release_second
            .send(Answer::RetryableFailure)
            .expect("the second call is waiting");
    };
    // Bounded: a second worker that never makes its call would leave all
    // three waiting, and the binary with them. A dispatcher that drops the
    // attempt a recovery hands back does exactly that (issue #165).
    let (late_pass, current_pass, ()) =
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            tokio::join!(late_pass, current_pass, order)
        })
        .await
        .expect("both workers call the tool, and both passes end");

    // The late worker exhausted the row and was fenced before it delivered.
    let late_error = late_pass.expect_err("the late worker's fleet write is fenced");
    assert_eq!(late_error.code(), "claim-fenced");
    assert_eq!(
        row_status(&fx, &effect).await,
        Some(OutboxStatus::Exhausted)
    );
    // The worker that holds the claim found the row finished: its pass goes
    // on, the fleet entry follows the row, and the run is told.
    let current_pass = current_pass.expect("a claim whose row is already finished is over");
    assert_eq!(current_pass.claimed, 1);
    assert_eq!(
        fleet_entry(&fx, &effect).await.map(|entry| entry.status),
        Some(AgentDispatchStatus::Exhausted)
    );
    assert!(
        !fx.effect_at(1)
            .await
            .is_some_and(|effect| !effect.status.is_resolved()),
        "the run is not left waiting on an effect nothing will ever claim again"
    );
    assert_eq!(current_pass.delivered, 1, "the exhaustion was delivered");
}
