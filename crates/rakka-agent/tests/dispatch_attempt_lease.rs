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
//! A claim that ends that way has spent lease and written no settle, so
//! nothing renewed the claims behind it in its batch. Its pass gives those
//! back, to be claimed again under a full lease.
//!
//! The executor here moves the shared clock by the time each call takes, so
//! where the clock stands when a call starts is the evidence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, InProcessRunResultDelivery, ScriptedReconciler,
    SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentCheckpointDecision, AgentCompensationExecutor, AgentCompensationRef,
    AgentDispatchAuthority, AgentDispatchDecision, AgentDispatchError, AgentDispatchFuture,
    AgentDispatchPass, AgentDispatchToolExecutor, AgentDispatchWindow, AgentEffectSpec,
    AgentEntityAuthority, AgentModelResponseDecision, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentReconciliationDecision, AgentReconciliationFinding,
    AgentReconciliationProtocolRef, AgentRunEffect, AgentRunEffectDispatcher,
    AgentRunEffectRequest, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunScope,
    AgentRunState, AgentRunStatus, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, AgentToolRegistry, AgentToolResponseDecision,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{OutboxStatus, WorkflowClock};
use rakka_agent_workflow::{
    agent_dispatcher_fleet_persistence_id, AgentDispatchStatus, AgentDispatcherFleetSettings,
    AgentDispatcherWorkerId, AgentEphemeralCredential, AgentTelemetryContext, AgentTimestampMillis,
    PrincipalRef,
};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

const TOOL: &str = "charge-card";

/// The second tool of a turn that calls two.
const OTHER_TOOL: &str = "refund-card";

fn call(id: &str, tool: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(id).expect("the call id is valid"),
        AgentToolId::new(tool).expect("the tool id is valid"),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the tool call is bounded")
}

/// A model that calls each of `tools` in its first turn, as `call-1`,
/// `call-2` and so on, and proposes in its second.
fn tools_then_proposal(tools: &[&str]) -> DeterministicModelAdapter {
    let mut turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION);
    for (index, tool) in tools.iter().enumerate() {
        turn = turn.with_tool_call(call(&format!("call-{}", index + 1), tool));
    }
    DeterministicModelAdapter::new()
        .with_turn_for(1, turn)
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

/// When a call started, on the shared clock, and the deadline its attempt
/// was handed.
type Bound = (u64, Option<u64>);

/// What a [`SlowExecutor`] saw.
#[derive(Clone, Default)]
struct Calls {
    /// Each call the first worker's side made: its call id, and how far the
    /// executor had moved the clock when it started.
    started: Arc<Mutex<Vec<(String, u64)>>>,
    /// For each of those calls, the shared clock when it started and the
    /// deadline the attempt was handed.
    bounds: Arc<Mutex<Vec<Bound>>>,
    /// How far the executor has moved the clock.
    elapsed: Arc<AtomicU64>,
    /// A second worker, when a test names one. As each call ends, and
    /// before it answers, the rival runs one dispatch pass: what any other
    /// worker of the fleet could do while that call is in flight.
    rival: Arc<tokio::sync::Mutex<Option<Pipeline>>>,
    /// A tool call whose call answers, when a test names one. Every other
    /// call ends unknown.
    answers: Arc<Mutex<Option<String>>>,
    /// When a test sets it: the first call ends with exactly this much of
    /// its claim's lease left, whatever `takes_ms` says.
    first_call_leaves: Arc<Mutex<Option<u64>>>,
    /// The passes the rival ran.
    rival_passes: Arc<AtomicU64>,
    /// The calls the rival made in them.
    rival_calls: Arc<AtomicU64>,
}

impl Calls {
    /// When each call started, in the order they were made.
    fn started_at(&self) -> Vec<u64> {
        self.started
            .lock()
            .expect("the calls are readable")
            .iter()
            .map(|(_call, at)| *at)
            .collect()
    }

    /// When each call of one tool call started.
    fn started_at_of(&self, call_id: &str) -> Vec<u64> {
        self.started
            .lock()
            .expect("the calls are readable")
            .iter()
            .filter(|(call, _at)| call == call_id)
            .map(|(_call, at)| *at)
            .collect()
    }

    fn count(&self) -> usize {
        self.started_at().len()
    }
}

/// A tool executor whose every call takes `takes_ms` of the shared clock and
/// ends with an unknown outcome.
struct SlowExecutor {
    clock: SharedAtomicWorkflowClock,
    fleet_store: FleetStore,
    takes_ms: u64,
    calls: Calls,
}

impl SlowExecutor {
    /// How long this call takes.
    async fn duration_of(&self, intent: &AgentRunEffect) -> u64 {
        let leaves = self
            .calls
            .first_call_leaves
            .lock()
            .expect("the knob is readable")
            .take();
        match leaves {
            Some(leaves) => {
                let expires_at = lease_expiry_in(&self.fleet_store, intent).await;
                expires_at - leaves - self.clock.now().as_millis()
            }
            None => self.takes_ms,
        }
    }
}

impl AgentDispatchToolExecutor for SlowExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
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
                .started
                .lock()
                .expect("the calls are writable")
                .push((
                    call.call_id.as_str().to_string(),
                    self.calls.elapsed.load(Ordering::SeqCst),
                ));
            self.calls
                .bounds
                .lock()
                .expect("the calls are writable")
                .push((
                    self.clock.now().as_millis(),
                    intent.deadline_at.map(|at| at.as_millis()),
                ));
            let takes_ms = self.duration_of(intent).await;
            self.clock.advance(takes_ms);
            self.calls.elapsed.fetch_add(takes_ms, Ordering::SeqCst);
            if let Some(rival) = rival.as_mut() {
                self.calls.rival_passes.fetch_add(1, Ordering::SeqCst);
                // Whatever the rival's pass answers, what it invoked is the
                // evidence.
                let _ = Box::pin(rival.pump_run(&run_scope())).await;
            }
            let answers = self.calls.answers.lock().expect("the knob is readable");
            if answers.as_deref() == Some(call.call_id.as_str()) {
                return Ok(AgentTaskContent::inline(serde_json::json!({ "ok": true }))
                    .expect("the content is inline-bounded"));
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
    world_of(&[TOOL], attempts, timeout_ms, takes_ms)
}

/// [`world`], with a first turn that calls each of `tools`: the tickets of
/// one turn are claimed in one batch.
fn world_of(
    tools: &[&str],
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
    let registry = tools
        .iter()
        .fold(AgentToolRegistry::new(), |registry, tool| {
            registry
                .register(tool_binding_for_spec(tool, &spec))
                .expect("the tool registers")
        });
    let fx = AuthorityFixture::new(
        tools_then_proposal(tools),
        AgentToolAuthority::new(registry),
        None,
    );
    let calls = Calls::default();
    let executor = SlowExecutor {
        clock: fx.wf_clock.clone(),
        fleet_store: fx.fleet_store.clone(),
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

/// The fleet's entry for the ticket: its status, and the worker that holds
/// its lease if it has one.
async fn entry_of(
    fx: &AuthorityFixture,
    effect: &AgentRunEffect,
) -> (AgentDispatchStatus, Option<String>) {
    let record = fx
        .fleet_store
        .load(&agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet record loads")
        .expect("the fleet record exists");
    let ticket = effect.dispatch_ticket_id();
    let entry = record
        .state
        .entries()
        .values()
        .find(|entry| entry.effect_id.as_str() == ticket.as_str())
        .expect("the ticket is registered");
    (
        entry.status,
        entry
            .lease
            .as_ref()
            .map(|lease| lease.worker_id.as_str().to_string()),
    )
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
/// question. Told again that the attempt the first claim left was not
/// executed, it records that attempt once and goes on: four attempts, four
/// calls.
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
/// lease still spends its budget, one attempt to a claim, where one claim
/// used to make both.
#[tokio::test]
async fn a_bound_longer_than_the_lease_means_one_attempt_to_a_claim() {
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

/// The claims behind a claim that ran out of lease are given back.
///
/// Two tickets of one turn are claimed in one batch and leased at one
/// instant. The first claim makes two attempts of 25 s and ends, with 10 s
/// of the 60 s lease left and no settle written, so nothing renewed the
/// second claim. A claim's first attempt is not asked whether the lease
/// covers it, so started there the second ticket's 25 s attempt would have
/// been in flight when its lease lapsed. The pass gives the second claim
/// back instead, and a worker that claims it gets a full lease.
#[tokio::test]
async fn the_claims_behind_a_claim_that_ran_out_of_lease_are_given_back() {
    let (fx, calls, _reconciler) = world_of(&[TOOL, OTHER_TOOL], 4, Some(25_000), 25_000);
    let first_ticket = ready(&fx).await;
    let second_ticket = fx.effect_at(2).await.expect("the second tool effect");

    let pass = pass_of(&fx, "worker-1").await;

    assert_eq!(calls.started_at_of("call-1"), vec![0, 25_000]);
    assert_eq!(
        calls.started_at_of("call-2"),
        Vec::<u64>::new(),
        "not started on what the first claim left of the lease"
    );
    assert_eq!((pass.claimed, pass.invoked, pass.lapsed), (2, 2, 1));
    assert_eq!(
        entry_of(&fx, &first_ticket).await,
        (AgentDispatchStatus::Claimed, Some("worker-1".to_string())),
        "the claim that ended keeps its entry until its lease lapses"
    );
    assert_eq!(
        entry_of(&fx, &second_ticket).await,
        (AgentDispatchStatus::Pending, None),
        "the claim behind it is anyone's, at once"
    );

    // Another worker takes it under a full lease: two attempts of 25 s
    // again, and the first ticket is still its first worker's.
    let second = pass_of(&fx, "worker-2").await;

    assert_eq!((second.claimed, second.invoked), (1, 2));
    assert_eq!(calls.started_at_of("call-2"), vec![50_000, 75_000]);
    assert_eq!(calls.started_at_of("call-1"), vec![0, 25_000]);
}

/// The same batch with a rival worker looking for work as each call ends:
/// it never invokes while a call is in flight. Started on what was left of
/// the lease, the second ticket's first attempt ran from 50 s to 75 s, and
/// the rival took both lapsed tickets at 75 s and invoked beside it.
#[tokio::test]
async fn no_second_worker_invokes_beside_the_claim_behind_one_that_ran_out_of_lease() {
    let (fx, calls, _reconciler) = world_of(&[TOOL, OTHER_TOOL], 4, Some(25_000), 25_000);
    ready(&fx).await;
    *calls.rival.lock().await = Some(fx.worker("worker-2", fx.tools.clone(), None));

    let pass = try_pass_of(&fx, "worker-1").await;

    assert!(calls.rival_passes.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        calls.rival_calls.load(Ordering::SeqCst),
        0,
        "a second worker invoked while the first worker's attempt was in flight"
    );
    pass.expect("a claim that runs out of lease is not a failed pass");
}

/// An authority whose every authorization takes `takes_ms` of the shared
/// clock: time a claim spends between asking whether its lease covers an
/// attempt and starting it.
struct SlowAuthority<Inner> {
    inner: Inner,
    clock: SharedAtomicWorkflowClock,
    takes_ms: u64,
}

impl<Inner: AgentDispatchAuthority> AgentDispatchAuthority for SlowAuthority<Inner> {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        let inner = self.inner.authorize(scope, run, intent, attempt, now);
        Box::pin(async move {
            let decision = inner.await?;
            self.clock.advance(self.takes_ms);
            Ok(decision)
        })
    }

    fn review_tool_response<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        tool: Option<&'a AgentToolId>,
        content: AgentTaskContent,
    ) -> AgentDispatchFuture<'a, AgentToolResponseDecision> {
        self.inner
            .review_tool_response(scope, intent, tool, content)
    }

    fn review_model_response<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        turn: AgentModelTurn,
    ) -> AgentDispatchFuture<'a, AgentModelResponseDecision> {
        self.inner.review_model_response(scope, intent, turn)
    }
}

/// The fixture's worker, behind an authority that takes `takes_ms` to
/// authorize each attempt.
fn worker_behind_a_slow_gate(
    fx: &AuthorityFixture,
    reconciler: &ScriptedReconciler,
    takes_ms: u64,
) -> Pipeline {
    let gate = SlowAuthority {
        inner: AgentEntityAuthority::new(fx.fx.agents.clone(), fx.authority.clone()),
        clock: fx.wf_clock.clone(),
        takes_ms,
    };
    let delivery = InProcessRunResultDelivery::new(
        fx.fx.runs.clone(),
        fx.fx.effects.clone(),
        fx.fx.router.clone(),
        fx.fx.clock.clone(),
    )
    .with_effect_policies(fx.fx.policies.clone());
    AgentRunEffectDispatcher::new(
        AgentDispatcherWorkerId::new("worker-1"),
        fx.workflow_store.clone(),
        fx.fleet_store.clone(),
        fx.fx.runs.clone(),
        fx.wf_clock.clone(),
        Arc::new(fx.adapter.clone()),
        fx.tool_executor.clone().expect("the slow executor"),
        Arc::new(gate),
        Arc::new(delivery),
    )
    .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
    .with_reconciler(Arc::new(reconciler.clone()))
}

/// When the lease of the ticket's entry expires.
async fn lease_expiry_of(fx: &AuthorityFixture, effect: &AgentRunEffect) -> u64 {
    lease_expiry_in(&fx.fleet_store, effect).await
}

/// [`lease_expiry_of`], from the fleet's store.
async fn lease_expiry_in(fleet_store: &FleetStore, effect: &AgentRunEffect) -> u64 {
    let record = fleet_store
        .load(&agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet record loads")
        .expect("the fleet record exists");
    let ticket = effect.dispatch_ticket_id();
    record
        .state
        .entries()
        .values()
        .find(|entry| entry.effect_id.as_str() == ticket.as_str())
        .and_then(|entry| entry.lease.as_ref())
        .expect("the ticket is leased")
        .lease_expires_at
        .as_millis()
}

/// The bound of a later attempt is counted from the moment its claim asked
/// whether the lease covers it, not from the attempt's own start. Between
/// the two the dispatcher records the attempt before, authorizes the next
/// and writes its `Started`, and that takes time: here two seconds at the
/// authority. Counted from its start, the third attempt of 25 s was asked
/// about at 34 s, started at 36 s and was handed a deadline of 61 s, under a
/// lease that ended at 60 s.
#[tokio::test]
async fn a_later_attempts_deadline_falls_inside_the_lease() {
    let (fx, calls, reconciler) = world(4, Some(25_000), 15_000);
    let effect = ready(&fx).await;
    fx.settle().await;

    let pass = worker_behind_a_slow_gate(&fx, &reconciler, 2_000)
        .pump_run(&run_scope())
        .await
        .map_err(|error| error.code().to_string())
        .expect("the pass runs");

    assert_eq!((pass.invoked, pass.lapsed), (3, 1));
    let lease_expires_at = lease_expiry_of(&fx, &effect).await;
    let bounds = calls.bounds.lock().expect("the calls are readable").clone();
    let (first_started_at, first_deadline) = bounds[0];
    assert_eq!(
        first_deadline,
        Some(first_started_at + 25_000),
        "a claim's first attempt is bounded from its own start"
    );
    for (attempt, (started_at, deadline)) in bounds.iter().enumerate().skip(1) {
        let deadline = deadline.expect("the tool declares a bound");
        assert!(
            deadline < lease_expires_at,
            "attempt {} was handed a deadline of {deadline}, past the lease's end at \
             {lease_expires_at}",
            attempt + 1
        );
        assert_eq!(
            deadline,
            started_at - 2_000 + 25_000,
            "attempt {}: the time the authority took came out of the attempt's bound",
            attempt + 1
        );
    }
}

/// A compensation executor whose every call takes `takes_ms` of the shared
/// clock and ends with an unknown outcome.
struct SlowCompensation {
    clock: SharedAtomicWorkflowClock,
    takes_ms: u64,
    calls: Arc<AtomicU64>,
}

impl AgentCompensationExecutor for SlowCompensation {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _compensation: &'a AgentCompensationRef,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.clock.advance(self.takes_ms);
        Box::pin(async move { Err(unknown()) })
    }
}

/// The run's compensation effect, once one is scheduled.
async fn compensation_of(fx: &AuthorityFixture) -> AgentRunEffect {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.state()
        .expect("the run state reads")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .iter()
        .find(|effect| matches!(effect.request, AgentRunEffectRequest::Compensation { .. }))
        .cloned()
        .expect("the compensation is scheduled")
}

/// A run winding down behind a reconcileable compensation of `attempts`
/// attempts, each bounded at 25 s and taking 25 s, every one ending unknown
/// and found not executed. The compensation's ticket is ready to claim.
async fn compensating_world(attempts: u32) -> (AuthorityFixture, Arc<AtomicU64>) {
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let compensating = AgentEffectSpec::reconcileable(protocol, attempts)
        .expect("the spec is valid")
        .with_timeout_ms(25_000);
    let mut fx = AuthorityFixture::new(
        tools_then_proposal(&[TOOL]),
        AgentToolAuthority::new(tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::non_idempotent(),
        )),
        None,
    );
    fx.fx.policies = fx
        .fx
        .policies
        .clone()
        .with_compensation_spec(compensating)
        .expect("the compensation spec is valid");
    let compensations = Arc::new(AtomicU64::new(0));
    let clock = fx.wf_clock.clone();
    let fleet_store = fx.fleet_store.clone();
    let fx = fx
        .with_tool_executor(Arc::new(SlowExecutor {
            clock: clock.clone(),
            fleet_store,
            takes_ms: 1,
            calls: Calls::default(),
        }))
        .with_compensation_executor(Arc::new(SlowCompensation {
            clock,
            takes_ms: 25_000,
            calls: compensations.clone(),
        }))
        .with_reconciler(Arc::new(
            ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
        ));

    // The tool's one call ends unknown and parks; an operator compensates.
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    assert_eq!(fx.one_pass().await.parked_indeterminate, 1);
    let now = fx.fx.now();
    let mut run = fx.fx.run();
    run.recover(now).await.expect("the run recovers");
    let checkpoint_id = run
        .state()
        .expect("the run state reads")
        .loop_state()
        .expect("the loop exists")
        .open_checkpoints()
        .first()
        .expect("the park's checkpoint")
        .checkpoint_id
        .clone();
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "compensate",
            )
            .expect("the decision key derives"),
            checkpoint_id,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::Compensate {
                    compensation: AgentCompensationRef::new("refund-card")
                        .expect("the reference is valid"),
                },
            )),
            telemetry: AgentTelemetryContext::default(),
        },
        &fx.fx.router,
        now,
    )
    .await
    .expect("the compensation decision applies");
    fx.expire_lease();
    (fx, compensations)
}

/// A wind-down does not stop the claim that follows from going on with an
/// effect the wind-down is waiting for. A compensation runs while its run
/// winds down, and is exempt from the wind-down's fence. Its first claim
/// ends for want of lease, and the claim that follows is told by the
/// reconciler that the attempt it left was not executed. That claim used to
/// read "the run is winding down, and nothing executed" as "the run wants
/// nothing further", cancel the compensation's ticket and leave it
/// `Cancelled` with half its budget unspent.
#[tokio::test]
async fn a_compensation_goes_on_under_the_claim_that_follows() {
    let (fx, compensations) = compensating_world(4).await;

    // Two attempts of 25 s under a 60 s lease, and the claim ends.
    let first = pass_of(&fx, "worker-1").await;
    assert_eq!((first.invoked, first.lapsed), (2, 1));
    let compensation = compensation_of(&fx).await;
    assert_eq!(compensation.status, AgentRunEffectStatus::Ready);

    fx.expire_lease();
    let second = pass_of(&fx, "worker-2").await;

    assert_eq!(
        (second.invoked, second.cancelled),
        (2, 0),
        "the claim that follows goes on with the compensation"
    );
    assert_eq!(
        compensations.load(Ordering::SeqCst),
        4,
        "one call for each attempt of the compensation's budget"
    );
    assert_eq!(
        row_of(&fx, &compensation).await,
        (OutboxStatus::Exhausted, 4)
    );
    let resolved = compensation_of(&fx).await;
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("scripted-outcome-unknown")
    );
    fx.settle().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run").status,
        AgentRunStatus::Failed
    );
}

/// The same reading met a compensation whose worker was lost after its
/// durable `Started`, which leaves the state a claim that ran out of lease
/// leaves: the row `Dispatching`, and the attempt found not executed. That
/// compensation was cancelled without ever being called.
#[tokio::test]
async fn a_compensation_whose_worker_was_lost_goes_on_too() {
    let (fx, compensations) = compensating_world(2).await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let lost = pass_of(&fx, "worker-1").await;
    assert!(lost.died, "the worker is lost after `Started`");
    assert_eq!(compensations.load(Ordering::SeqCst), 0);

    fx.expire_lease();
    let second = pass_of(&fx, "worker-2").await;

    assert_eq!((second.invoked, second.cancelled), (1, 0));
    assert_eq!(compensations.load(Ordering::SeqCst), 1);
    assert_eq!(
        compensation_of(&fx).await.status,
        AgentRunEffectStatus::Exhausted,
        "the budget of two is spent: the lost attempt, and the one that was made"
    );
}

/// The rule at its edge. A lease is over at the instant it expires, so an
/// attempt of 25 s is covered only by more than 25 s of lease: with exactly
/// 25 s left the claim ends, and with one millisecond more it goes on.
#[tokio::test]
async fn an_attempt_needs_more_lease_than_its_bound() {
    for (left, calls_made) in [(25_000, 1), (25_001, 2)] {
        let (fx, calls, _reconciler) = world(2, Some(25_000), 1_000);
        ready(&fx).await;
        *calls
            .first_call_leaves
            .lock()
            .expect("the knob is writable") = Some(left);

        pass_of(&fx, "worker-1").await;

        assert_eq!(
            calls.count(),
            calls_made,
            "with {left} ms of lease left after the first call"
        );
    }
}

/// The lease a claim asks about is the one its entry holds, which a renewal
/// moves, and not the expiry the claim was handed. The first ticket of a
/// batch answers at 25 s, and its settle renews the second to 85 s. So the
/// second ticket's second attempt, which ends at 75 s, is made, though the
/// batch was leased to 60 s.
#[tokio::test]
async fn a_renewed_claim_makes_the_attempts_its_renewed_lease_covers() {
    let (fx, calls, _reconciler) = world_of(&[TOOL, OTHER_TOOL], 4, Some(25_000), 25_000);
    ready(&fx).await;
    *calls.answers.lock().expect("the knob is writable") = Some("call-1".to_string());

    let pass = pass_of(&fx, "worker-1").await;

    assert_eq!(calls.started_at_of("call-1"), vec![0]);
    assert_eq!(calls.started_at_of("call-2"), vec![25_000, 50_000]);
    assert_eq!((pass.invoked, pass.lapsed), (3, 1));
}
