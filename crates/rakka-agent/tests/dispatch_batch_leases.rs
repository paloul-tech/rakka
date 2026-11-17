//! A pass and the batch it claimed (issue #157).
//!
//! A pass claims a batch from the fleet under one lease instant and runs the
//! claims one after another. Three things follow from that, and each test
//! here pins one of them:
//!
//! - **A settle that loses its write to another handle** used to end the
//!   pass and leave the whole batch leased. It reads the record again
//!   (issue #145), and the pass goes on with its batch.
//! - **A later claim had only what the earlier ones left of the lease.**
//!   Every settle now renews the claims the pass still holds, so a batch
//!   needs a lease that covers its longest call and not their sum, and a
//!   claim whose lease lapsed before the pass reached it is not started.
//! - **A pass that failed left the claims it never started leased.** It now
//!   gives them back before it returns its error.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, SharedAtomicWorkflowClock};
use rakka_agent::{
    AgentDispatchError, AgentDispatchWindow, AgentEffectSpec, AgentModelTurn, AgentRunEffect,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxMessageId;
use rakka_agent_workflow::{
    AgentDispatchConcurrencyLimits, AgentDispatchEntry, AgentDispatchStatus, AgentDispatcherFleet,
    AgentDispatcherFleetSettings, AgentDispatcherWorkerId, AgentRunInbox, AgentTimestampMillis,
};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

const TOOL: &str = "charge-card";
const FUTURE: AgentTimestampMillis = AgentTimestampMillis::new(u64::MAX);

/// Two tool calls in one turn, so one pass claims two tickets in one batch.
fn two_tools_then_proposal() -> DeterministicModelAdapter {
    let call = |id: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("the call id is valid"),
            AgentToolId::new(TOOL).expect("the tool id is valid"),
            serde_json::json!({ "amount": 42 }),
        )
        .expect("the tool call is bounded")
    };
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_tool_call(call("call-1"))
                .with_tool_call(call("call-2")),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                    .expect("the proposal is inline-bounded"),
            ),
        )
}

/// What a [`TimedTool`] does during its first call, given the call's effect.
type DuringFirstCall =
    Box<dyn FnOnce(AgentRunEffect) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// A tool that takes time: every call moves the shared clock on by `millis`.
/// Its first call can also run a piece of work, which is how a test puts
/// another worker between a pass's claim and its settle.
struct TimedTool {
    clock: std::sync::Mutex<Option<SharedAtomicWorkflowClock>>,
    millis: u64,
    during_first: tokio::sync::Mutex<Option<DuringFirstCall>>,
    calls: std::sync::Mutex<Vec<String>>,
}

impl TimedTool {
    fn taking(millis: u64) -> Arc<Self> {
        Arc::new(Self {
            clock: std::sync::Mutex::new(None),
            millis,
            during_first: tokio::sync::Mutex::new(None),
            calls: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// The call ids it was handed, in order.
    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("the log is not poisoned").clone()
    }
}

impl rakka_agent::AgentDispatchToolExecutor for TimedTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
        _credential: Option<&'a rakka_agent_workflow::AgentEphemeralCredential>,
    ) -> rakka_agent::AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("the log is not poisoned")
                .push(call.call_id.as_str().to_string());
            let during_first = self.during_first.lock().await.take();
            if let Some(work) = during_first {
                work(intent.clone()).await;
            }
            let clock = self
                .clock
                .lock()
                .expect("the clock slot is not poisoned")
                .clone()
                .expect("the clock is wired before the call");
            clock.advance(self.millis);
            Ok(
                AgentTaskContent::inline(serde_json::json!({ "paid": true }))
                    .expect("the result is inline-bounded"),
            )
        })
    }
}

/// A run with two tool tickets in its outbox, not yet registered, and the
/// two effects behind them. `tool` replaces the recording executor.
async fn two_tool_tickets(tool: Option<Arc<TimedTool>>) -> (AuthorityFixture, [AgentRunEffect; 2]) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let mut fx = AuthorityFixture::new(
        two_tools_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    if let Some(tool) = tool {
        *tool.clock.lock().expect("the clock slot is not poisoned") = Some(fx.wf_clock.clone());
        fx = fx.with_tool_executor(tool);
    }
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let first = fx.effect_at(1).await.expect("the first call is committed");
    let second = fx.effect_at(2).await.expect("the second call is committed");
    (fx, [first, second])
}

/// The fleet record as the store holds it.
async fn fleet_state(fx: &AuthorityFixture) -> rakka_agent_workflow::AgentDispatcherFleetState {
    fx.fleet_store
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")
        .expect("the fleet record exists")
        .state
}

/// The fleet entries of the two tool tickets.
async fn tool_entries(
    fx: &AuthorityFixture,
    effects: &[AgentRunEffect; 2],
) -> Vec<AgentDispatchEntry> {
    let state = fleet_state(fx).await;
    effects
        .iter()
        .map(|effect| {
            state
                .entry(&rakka_agent_workflow::agent_dispatch_id(
                    &rakka_agent::workflow_run_id(&run_scope()),
                    &effect.dispatch_ticket_id(),
                ))
                .cloned()
                .expect("the ticket is registered")
        })
        .collect()
}

/// A fleet handle of another worker's over the same store.
fn another_handle(
    fx: &AuthorityFixture,
    max_batch_size: usize,
) -> AgentDispatcherFleet<FleetStore, SharedAtomicWorkflowClock> {
    AgentDispatcherFleet::with_clock_and_metrics(
        fx.fleet_store.clone(),
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
        AgentDispatcherFleetSettings::new(max_batch_size, LEASE_MS),
        fx.wf_clock.clone(),
        Arc::new(rakka_core::NoopMetricsRecorder),
    )
}

// ---------------------------------------------------------------------------
// A settle after another handle's write.
// ---------------------------------------------------------------------------

/// The host's proof of the stranded batch
/// (`a_settle_after_another_workers_write_strands_the_batch_until_the_lease_lapses`),
/// inverted. There a registration from another worker wrote the record
/// between the pass's claim and its first settle; here a prune from another
/// handle does, since this fixture has one run. Either is one more
/// compare-and-set of the same record.
#[tokio::test]
async fn a_settle_after_another_handles_write_completes_and_the_batch_goes_on() {
    let tool = TimedTool::taking(0);
    let (fx, effects) = two_tool_tickets(Some(tool.clone())).await;
    let mut other = another_handle(&fx, 16);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    *tool.during_first.lock().await = Some(Box::new(move |_intent| {
        Box::pin(async move {
            let prune = other
                .prune_terminal(FUTURE)
                .await
                .expect("the other handle's prune runs");
            sender.send(prune.pruned).expect("the test is waiting");
        })
    }));

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("another handle's write does not fail the pass");

    assert_eq!(
        receiver.await.expect("the prune ran during the first call"),
        1,
        "the first model turn's finished entry was pruned, so the record moved"
    );
    assert_eq!((pass.claimed, pass.invoked, pass.delivered), (2, 2, 2));
    for entry in tool_entries(&fx, &effects).await {
        assert_eq!(entry.status, AgentDispatchStatus::Completed);
        assert_eq!(
            entry.last_fencing_token, 1,
            "settled under the claim it was run under, not after a lease"
        );
    }
}

// ---------------------------------------------------------------------------
// A batch's leases.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_batch_whose_calls_each_fit_the_lease_outlives_the_instant_it_was_leased_to() {
    // Two calls of 40 s under a lease of 60 s: the second ends 80 s after
    // the claim.
    let tool = TimedTool::taking(40_000);
    let (fx, effects) = two_tool_tickets(Some(tool.clone())).await;

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the second claim settles under a lease the first one's settle renewed");

    assert_eq!(
        (pass.claimed, pass.invoked, pass.delivered, pass.lapsed),
        (2, 2, 2, 0)
    );
    for entry in tool_entries(&fx, &effects).await {
        assert_eq!(entry.status, AgentDispatchStatus::Completed);
        assert_eq!(entry.last_fencing_token, 1);
    }
}

#[tokio::test]
async fn a_claim_whose_lease_lapsed_before_the_pass_reached_it_is_not_started() {
    let tool = TimedTool::taking(0);
    let (fx, effects) = two_tool_tickets(Some(tool.clone())).await;
    // During the first call the lease lapses, and another worker takes that
    // one ticket over and finishes it: its outbox row, then its entry. The
    // pass comes back to a finished row, which ends its claim quietly.
    let mut other = another_handle(&fx, 1);
    let workflow_store = fx.workflow_store.clone();
    let clock = fx.wf_clock.clone();
    *tool.during_first.lock().await = Some(Box::new(move |intent| {
        Box::pin(async move {
            clock.advance(LEASE_MS + 1);
            let mut batch = other
                .claim_due(AgentDispatcherWorkerId::new("worker-other"))
                .await
                .expect("the other worker claims");
            let claim = batch.claims.remove(0);
            assert_eq!(
                claim.effect_id.as_str(),
                intent.dispatch_ticket_id().as_str(),
                "the other worker took the ticket whose call is running"
            );
            let mut inbox = AgentRunInbox::with_clock(
                rakka_agent::workflow_run_id(&run_scope()),
                workflow_store,
                clock,
            );
            inbox.recover().await.expect("the inbox recovers");
            inbox
                .inner_mut()
                .record_outbox_success(&OutboxMessageId::new(claim.effect_id.as_str()))
                .await
                .expect("the other worker finishes the row");
            other
                .complete_claim(&claim)
                .await
                .expect("the other worker finishes the entry");
        })
    }));

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("a lapsed claim is passed over, and the pass goes on");

    assert_eq!(
        (pass.claimed, pass.invoked, pass.lapsed),
        (2, 1, 1),
        "the second claim was taken and never started"
    );
    assert_eq!(tool.calls().len(), 1, "the second call was not made");
    // Nothing was written for the lapsed claim: its entry is as the claim
    // left it, and anyone can take it.
    let statuses: Vec<_> = tool_entries(&fx, &effects)
        .await
        .into_iter()
        .map(|entry| (entry.status, entry.last_fencing_token))
        .collect();
    assert!(
        statuses.contains(&(AgentDispatchStatus::Completed, 2)),
        "the other worker's ticket: {statuses:?}"
    );
    assert!(
        statuses.contains(&(AgentDispatchStatus::Claimed, 1)),
        "the lapsed claim: {statuses:?}"
    );

    let next = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the next pass runs");
    assert_eq!((next.claimed, next.invoked), (1, 1));
    assert_eq!(tool.calls().len(), 2, "each call was made once");
}

// ---------------------------------------------------------------------------
// A pass that stops short.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failed_pass_gives_back_the_claims_it_did_not_start() {
    let (fx, effects) = two_tool_tickets(None).await;
    let mut worker = fx.pipeline();
    let registered = worker
        .register_run(&run_scope())
        .await
        .expect("the tickets register");
    assert_eq!(registered.registered, 2);
    // The first claim's first write to the run's outbox, its `Dispatching`
    // mark, loses every compare-and-set it tries.
    fx.workflow_store.conflict_from(1);

    let error = worker
        .claim_and_execute()
        .await
        .expect_err("the mark loses more often than it is re-applied");
    fx.workflow_store.survive();

    assert_eq!(error.code(), "revision-conflict");
    assert!(matches!(error, AgentDispatchError::Inbox(_)), "{error:?}");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    let entries = tool_entries(&fx, &effects).await;
    let failed: Vec<_> = entries
        .iter()
        .filter(|entry| entry.status == AgentDispatchStatus::Claimed)
        .collect();
    let given_back: Vec<_> = entries
        .iter()
        .filter(|entry| entry.status == AgentDispatchStatus::Pending)
        .collect();
    assert_eq!(failed.len(), 1, "the claim that failed keeps its lease");
    assert!(failed[0].lease.is_some());
    assert_eq!(given_back.len(), 1, "the claim behind it is given back");
    assert_eq!(given_back[0].lease, None);
    assert_eq!(given_back[0].last_fencing_token, 1);

    // No lease has to lapse: the next pass takes the released ticket at
    // once, under the next token, and leaves the failed one to its lease.
    let next = fx
        .pipeline()
        .claim_and_execute()
        .await
        .expect("the next pass runs");
    assert_eq!((next.claimed, next.invoked, next.delivered), (1, 1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

#[tokio::test]
async fn a_failed_pass_returns_its_own_error_when_the_release_fails_too() {
    let (fx, effects) = two_tool_tickets(None).await;
    fx.workflow_store.conflict_from(1);
    // The pass writes the fleet record three times: the registration, the
    // claim, and the release. The third is lost with its store.
    fx.fleet_store
        .crash_at(3, rakka_agent::testkit::CrashPoint::BeforeWrite);

    let error = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("the first claim fails the pass");
    fx.fleet_store
        .assert_crash_fired(3, rakka_agent::testkit::CrashPoint::BeforeWrite);
    fx.workflow_store.survive();
    fx.fleet_store.survive();

    assert!(
        matches!(error, AgentDispatchError::Inbox(_)),
        "the error the pass had, not the release's: {error:?}"
    );
    for entry in tool_entries(&fx, &effects).await {
        assert_eq!(
            entry.status,
            AgentDispatchStatus::Claimed,
            "a release that failed leaves the claims to their lease"
        );
    }
}

#[tokio::test]
async fn a_killed_worker_gives_nothing_back() {
    let (fx, effects) = two_tool_tickets(None).await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the worker dies quietly");

    assert!(pass.died);
    for entry in tool_entries(&fx, &effects).await {
        assert_eq!(entry.status, AgentDispatchStatus::Claimed);
        assert!(entry.lease.is_some(), "a crash releases nothing");
    }
    let next = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the next pass runs");
    assert_eq!(next.claimed, 0, "both tickets wait out the lease");
}

// ---------------------------------------------------------------------------
// What a pass reports.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_pass_reports_the_tickets_its_claim_left_at_a_concurrency_limit() {
    let (fx, _effects) = two_tool_tickets(None).await;
    // One call in flight per class: the batch takes the first ticket and
    // leaves the second at the limit.
    let mut worker = fx.pipeline().with_fleet_settings(
        AgentDispatcherFleetSettings::new(16, LEASE_MS)
            .concurrency_limits(AgentDispatchConcurrencyLimits::new(1)),
    );

    let pass = worker.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!((pass.claimed, pass.concurrency_limited), (1, 1));
}
