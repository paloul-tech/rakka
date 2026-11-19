//! A claim's attempts and its lease (issue #171).
//!
//! One claim makes every attempt a reconcileable effect's budget permits,
//! one after another, for as long as each ends unknown and is found not
//! executed. Nothing renews the lease of the claim being run. So a later
//! attempt is made only while the lease still covers it: a claim whose lease
//! has lapsed, or would lapse before the attempt's own bound is up, ends
//! there and writes nothing, and the claim that follows takes the ticket up
//! from the reconciler's question.
//!
//! The executor here moves the shared clock by the time each call takes, so
//! where the clock stands when a call starts is the evidence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchPass, AgentDispatchToolExecutor,
    AgentEffectSpec, AgentModelTurn, AgentReconciliationFinding, AgentReconciliationProtocolRef,
    AgentRunEffect, AgentRunEffectStatus, AgentRunScope, AgentTaskContent, AgentToolAuthority,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::AgentEphemeralCredential;

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

fn unknown() -> AgentDispatchError {
    AgentDispatchError::outcome_unknown(
        "scripted-outcome-unknown",
        "the call was sent and no answer was read",
    )
}

/// What a [`SlowExecutor`] saw.
#[derive(Clone, Default)]
struct Calls {
    /// How far the executor had moved the clock when each call started.
    started_at: Arc<Mutex<Vec<u64>>>,
    /// How far the executor has moved the clock.
    elapsed: Arc<AtomicU64>,
    /// A second worker, when a test names one. As each call ends, and
    /// before it answers, the rival runs one dispatch pass: what any other
    /// worker of the fleet could do while that call is in flight.
    rival: Arc<tokio::sync::Mutex<Option<Pipeline>>>,
    /// The passes the rival ran.
    rival_passes: Arc<AtomicU64>,
    /// The calls the rival made in them.
    rival_calls: Arc<AtomicU64>,
}

impl Calls {
    fn started_at(&self) -> Vec<u64> {
        self.started_at
            .lock()
            .expect("the calls are readable")
            .clone()
    }

    fn count(&self) -> usize {
        self.started_at().len()
    }
}

/// A tool executor whose every call takes `takes_ms` of the shared clock and
/// ends with an unknown outcome.
struct SlowExecutor {
    clock: SharedAtomicWorkflowClock,
    takes_ms: u64,
    calls: Calls,
}

impl AgentDispatchToolExecutor for SlowExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            // A call the rival makes is made while the first worker's call
            // is still in flight: the rival is out of its slot only then.
            let Ok(mut rival) = self.calls.rival.try_lock() else {
                self.calls.rival_calls.fetch_add(1, Ordering::SeqCst);
                return Err(unknown());
            };
            self.calls
                .started_at
                .lock()
                .expect("the calls are writable")
                .push(self.calls.elapsed.load(Ordering::SeqCst));
            self.clock.advance(self.takes_ms);
            self.calls
                .elapsed
                .fetch_add(self.takes_ms, Ordering::SeqCst);
            if let Some(rival) = rival.as_mut() {
                self.calls.rival_passes.fetch_add(1, Ordering::SeqCst);
                // Whatever the rival's pass answers, what it invoked is the
                // evidence.
                let _ = Box::pin(rival.pump_run(&run_scope())).await;
            }
            Err(unknown())
        })
    }
}

/// A reconcileable tool of `attempts` attempts, each bounded by `timeout_ms`
/// when one is given, behind an executor whose every call takes `takes_ms`
/// and ends unknown, and a reconciler that finds every attempt not executed.
fn world(
    attempts: u32,
    timeout_ms: Option<u64>,
    takes_ms: u64,
) -> (AuthorityFixture, Calls, ScriptedReconciler) {
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let mut spec = AgentEffectSpec::reconcileable(protocol, attempts).expect("the spec is valid");
    if let Some(timeout_ms) = timeout_ms {
        spec = spec.with_timeout_ms(timeout_ms);
    }
    let reconciler =
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted);
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    let calls = Calls::default();
    let executor = SlowExecutor {
        clock: fx.wf_clock.clone(),
        takes_ms,
        calls: calls.clone(),
    };
    let fx = fx
        .with_tool_executor(Arc::new(executor))
        .with_reconciler(Arc::new(reconciler.clone()));
    (fx, calls, reconciler)
}

async fn ready(fx: &AuthorityFixture) -> AgentRunEffect {
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.effect_at(1).await.expect("the tool effect is committed")
}

/// One dispatch pass of `worker`: its report, or the code it failed with.
async fn try_pass_of(fx: &AuthorityFixture, worker: &str) -> Result<AgentDispatchPass, String> {
    fx.settle().await;
    fx.worker(worker, fx.tools.clone(), None)
        .pump_run(&run_scope())
        .await
        .map_err(|error| error.code().to_string())
}

/// One dispatch pass of `worker`, which must not fail.
async fn pass_of(fx: &AuthorityFixture, worker: &str) -> AgentDispatchPass {
    try_pass_of(fx, worker)
        .await
        .unwrap_or_else(|code| panic!("{worker}'s pass failed: {code}"))
}

/// The ticket's row: its status and the attempts it counts.
async fn row_of(fx: &AuthorityFixture, effect: &AgentRunEffect) -> (OutboxStatus, u32) {
    let row = fx.outbox_row(effect).await.expect("the ticket's row");
    (row.status(), row.attempts().attempts())
}

/// The defect, as the host read it: a budget whose attempts together outlast
/// the lease was spent to its end under the one claim. Here four attempts of
/// 25 s under a 60 s lease: the fourth started 75 s into a lease that had
/// lapsed at 60 s, when any worker could have taken the ticket.
///
/// No bound is declared for an attempt, so the claim can only ask whether
/// its lease has lapsed. The third attempt starts inside the lease and is
/// made; the fourth is not.
#[tokio::test]
async fn a_claim_whose_lease_has_lapsed_makes_no_further_attempt() {
    let (fx, calls, reconciler) = world(4, None, 25_000);
    let effect = ready(&fx).await;

    let pass = try_pass_of(&fx, "worker-1").await;

    assert_eq!(
        calls.started_at(),
        vec![0, 25_000, 50_000],
        "an attempt starts only inside the lease of {LEASE_MS} ms"
    );
    let pass = pass.expect("a claim that runs out of lease is not a failed pass");
    assert_eq!((pass.invoked, pass.lapsed), (3, 1));
    assert_eq!(reconciler.queries(), 3, "each attempt's finding was asked");
    assert_eq!(
        row_of(&fx, &effect).await,
        (OutboxStatus::Dispatching, 2),
        "the claim that ended wrote nothing for the attempt it left"
    );
    assert_eq!(
        fx.effect_at(1).await.expect("the effect").status,
        AgentRunEffectStatus::Ready,
        "the run is told nothing: the effect is still out"
    );
}

/// With a bound declared for an attempt, the claim asks whether its lease
/// covers the whole of the next one, not only its first instant. Two
/// attempts of 25 s leave 10 s of a 60 s lease: the third is not started,
/// so no attempt is in flight when the lease lapses.
#[tokio::test]
async fn an_attempt_the_lease_does_not_cover_is_not_started() {
    let (fx, calls, _reconciler) = world(4, Some(25_000), 25_000);
    let effect = ready(&fx).await;

    let pass = try_pass_of(&fx, "worker-1").await;

    assert_eq!(calls.started_at(), vec![0, 25_000]);
    let pass = pass.expect("a claim that runs out of lease is not a failed pass");
    assert!(
        calls.elapsed.load(Ordering::SeqCst) < LEASE_MS,
        "every attempt made had ended before the lease lapsed"
    );
    assert_eq!((pass.invoked, pass.lapsed), (2, 1));
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Dispatching, 1));
}

/// The claim that follows takes the ticket up from the reconciler's
/// question, and the budget is spent exactly: four attempts, four calls. The
/// attempt the first claim left is counted once, by the claim that asks
/// about it.
#[tokio::test]
async fn the_next_claim_goes_on_and_the_budget_is_spent_exactly() {
    let (fx, calls, reconciler) = world(4, Some(25_000), 25_000);
    let effect = ready(&fx).await;
    let first = pass_of(&fx, "worker-1").await;
    assert_eq!((first.invoked, first.lapsed), (2, 1));

    // Nothing takes the ticket while the first claim's lease stands.
    let early = pass_of(&fx, "worker-2").await;
    assert_eq!((early.claimed, early.invoked), (0, 0));

    fx.wf_clock
        .advance(LEASE_MS - calls.elapsed.load(Ordering::SeqCst));
    let second = pass_of(&fx, "worker-2").await;

    assert_eq!((second.claimed, second.invoked, second.lapsed), (1, 2, 0));
    assert_eq!(calls.count(), 4, "one call for each attempt of the budget");
    assert_eq!(
        reconciler.queries(),
        5,
        "the second claim asked again about the attempt the first one left"
    );
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Exhausted, 4));
    let resolved = fx.effect_at(1).await.expect("the effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("scripted-outcome-unknown")
    );
}

/// The last attempt of a budget is settled by the claim that made it,
/// whatever is left of the lease: no attempt follows it, so there is nothing
/// for the lease to cover. Two attempts of 28 s leave 4 s.
#[tokio::test]
async fn a_budgets_last_attempt_is_settled_under_the_claim_that_made_it() {
    let (fx, calls, _reconciler) = world(2, Some(28_000), 28_000);
    let effect = ready(&fx).await;

    let pass = pass_of(&fx, "worker-1").await;

    assert_eq!(calls.started_at(), vec![0, 28_000]);
    assert_eq!((pass.invoked, pass.lapsed), (2, 0));
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Exhausted, 2));
    assert_eq!(
        fx.effect_at(1).await.expect("the effect").status,
        AgentRunEffectStatus::Exhausted
    );
}

/// A claim's first attempt is made whatever its bound: the rule is for the
/// attempts after it. So a tool whose attempt is bounded longer than the
/// lease still spends its budget, one attempt to a claim.
#[tokio::test]
async fn a_claims_first_attempt_is_made_whatever_its_bound() {
    let (fx, calls, _reconciler) = world(2, Some(LEASE_MS + 1), 1_000);
    let effect = ready(&fx).await;

    let first = pass_of(&fx, "worker-1").await;
    assert_eq!((first.invoked, first.lapsed), (1, 1));
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Dispatching, 0));

    fx.expire_lease();
    let second = pass_of(&fx, "worker-2").await;

    assert_eq!((second.invoked, second.lapsed), (1, 0));
    assert_eq!(calls.count(), 2);
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Exhausted, 2));
}

/// A budget that fits its lease is spent under one claim, as before.
#[tokio::test]
async fn a_budget_that_fits_its_lease_is_spent_under_one_claim() {
    let (fx, calls, reconciler) = world(4, Some(10_000), 10_000);
    let effect = ready(&fx).await;

    let pass = pass_of(&fx, "worker-1").await;

    assert_eq!(calls.started_at(), vec![0, 10_000, 20_000, 30_000]);
    assert_eq!((pass.invoked, pass.lapsed), (4, 0));
    assert_eq!(reconciler.queries(), 4);
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Exhausted, 4));
}

/// What the rule is for: no second worker invokes while an attempt is in
/// flight. A rival worker runs a dispatch pass as each of the first worker's
/// calls ends, before the call answers. While the first claim's lease
/// stands the rival can take nothing. The first worker used to start its
/// third attempt 50 s into a 60 s lease; at 75 s, with that attempt still
/// in flight, the rival took the lapsed ticket, was told by the reconciler
/// that nothing had executed, and invoked beside it.
#[tokio::test]
async fn no_second_worker_invokes_while_an_attempt_is_in_flight() {
    let (fx, calls, _reconciler) = world(4, Some(25_000), 25_000);
    let effect = ready(&fx).await;
    *calls.rival.lock().await = Some(fx.worker("worker-2", fx.tools.clone(), None));

    let pass = try_pass_of(&fx, "worker-1").await;

    assert!(
        calls.rival_passes.load(Ordering::SeqCst) >= 2,
        "the rival looked for work during each call"
    );
    assert_eq!(
        calls.rival_calls.load(Ordering::SeqCst),
        0,
        "a second worker invoked while the first worker's attempt was in flight"
    );
    assert_eq!(calls.started_at(), vec![0, 25_000]);
    let pass = pass.expect("a claim that runs out of lease is not a failed pass");
    assert_eq!((pass.invoked, pass.lapsed), (2, 1));
    assert_eq!(row_of(&fx, &effect).await, (OutboxStatus::Dispatching, 1));
}
