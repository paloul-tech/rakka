//! A reconcileable call's failed attempt, recovered (issue #195): the cases
//! the reviews of the fix found.
//!
//! A failed attempt's outbox row counted that attempt when it failed, so the
//! claim that follows asks the reconciler about an attempt already counted,
//! and a finding of "not executed" goes straight to the next attempt with
//! nothing written. These tests hold what that path does around the
//! question:
//!
//! - it asks the dispatch authority first, so a retry the authority defers
//!   spends nothing and asks the reconciler nothing;
//! - after the answer, a claim makes its first attempt only while its lease
//!   is still current, on this path and on a lost attempt's;
//! - it reads the row again, and makes no attempt when another worker has
//!   moved it, telling the run at once of a row that worker finished;
//! - a park made at once names and links the attempt that failed.
//!
//! `effect_dispatch.rs` holds the rest of the issue's cases.

use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, RecordingToolExecutor, ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentDispatchFuture, AgentDispatchPass, AgentDispatchWindow, AgentEffectReconciler,
    AgentEffectSpec, AgentModelTurn, AgentReconciliationFinding, AgentReconciliationProtocolRef,
    AgentRunEffect, AgentRunEffectStatus, AgentRunScope, AgentRunStatus, AgentRunTerminalReason,
    AgentSegmentOperation, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentSegmentSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxMessageId;
use rakka_agent_workflow::substrate::OutboxStatus::{self, Dispatching, Exhausted, Failed};

mod common;

use common::*;

const TOOL: &str = "charge-card";

/// The segment attribute naming the attempt a segment is about.
const ATTEMPT: &str = "rakka.agent.effect.attempt";

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                AgentToolId::new(TOOL).expect("tool id should be valid"),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn closing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A fixture whose run calls a reconcileable tool of `attempts` attempts,
/// every call of which fails with a retryable failure.
fn failing_reconcileable_call(attempts: u32) -> AuthorityFixture {
    let spec = AgentEffectSpec::reconcileable(
        AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid"),
        attempts,
    )
    .expect("the spec is valid");
    let mut fx = AuthorityFixture::over(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn())
            .with_turn_for(2, closing_turn()),
        tool_registry_for_spec(TOOL, &spec),
        None,
    );
    fx.tools = RecordingToolExecutor::new().with_failure(TOOL, "upstream-unavailable", "later");
    fx
}

/// A reconciler that always answers "not executed".
fn not_executed() -> ScriptedReconciler {
    ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted)
}

/// The tool ticket's outbox row: its status, and the attempts it counts.
async fn tool_row(fx: &AuthorityFixture) -> (OutboxStatus, u32) {
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's row exists");
    (row.status(), row.attempts().attempts())
}

/// The effect attempts the run counts as spent.
async fn spent_effect_attempts(fx: &AuthorityFixture) -> u64 {
    rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists")
    .loop_state()
    .expect("the loop is started")
    .budget()
    .consumption()
    .get(rakka_agent::AgentBudgetDimension::EffectAttempts)
}

/// One claim of the tool's ticket: expires the lease and the retry's backoff,
/// then runs one pass on a fresh worker.
async fn claim_round(fx: &AuthorityFixture) -> AgentDispatchPass {
    fx.expire_lease();
    fx.one_pass().await
}

/// One claim of the tool's ticket whose pass must end without an error: the
/// claim taken after the lease, by a fresh worker.
async fn the_late_claim(fx: &AuthorityFixture) -> AgentDispatchPass {
    fx.expire_lease();
    fx.settle().await;
    fx.pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the claim's pass ends without an error")
}

/// The attempt each of the tool's failed attempts was made as, in the order
/// their segments closed.
fn failed_tool_attempts(segments: &InMemoryAgentSegmentSink) -> Vec<String> {
    segments
        .segments()
        .iter()
        .filter(|segment| {
            matches!(
                segment.operation,
                AgentSegmentOperation::EffectDispatch {
                    effect_kind: "tool-call"
                }
            ) && segment.error_code.as_deref() == Some("dispatch-collaborator-failed")
        })
        .filter_map(|segment| segment.attributes.get(ATTEMPT).cloned())
        .collect()
}

/// A retry the dispatch authority defers after a failed call spends none of
/// the budget, and asks the reconciler nothing. The agent is suspended after
/// the call's first attempt failed. Each claim asks the authority before the
/// reconciler, and the authority defers the retry, on as many passes as are
/// made: the deferral is due again at once. The row keeps the one attempt it
/// counted and the run is charged nothing more, so once the agent is resumed
/// the call gets the two attempts left, each asked about first.
///
/// On `main` the finding recorded a failed attempt of its own on each claim,
/// so a suspended agent spent its budget and failed the run under
/// `dispatcher-lost-after-started` after one call. The fix's first version
/// spent nothing but asked the reconciler on every pass.
#[tokio::test]
async fn a_retry_the_authority_defers_after_a_failed_call_spends_nothing_and_asks_nothing() {
    let reconciler = not_executed();
    let fx = failing_reconcileable_call(3).with_reconciler(Arc::new(reconciler.clone()));
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    let spent = spent_effect_attempts(&fx).await;

    fx.set_suspended(true, "suspend-1").await;
    let mut passes = vec![claim_round(&fx).await];
    for _pass in 0..4 {
        passes.push(fx.one_pass().await);
    }
    let deferred: Vec<_> = passes
        .iter()
        .map(|pass| (pass.deferred, pass.failed_attempts))
        .collect();
    assert_eq!(
        deferred,
        vec![(1, 0); 5],
        "(deferred, failed attempts) of each pass, back to back"
    );
    assert_eq!(reconciler.queries(), 0, "the reconciler was asked nothing");
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(spent_effect_attempts(&fx).await, spent);
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForEffect
    );

    fx.set_suspended(false, "resume-1").await;
    claim_round(&fx).await;
    assert_eq!(
        (tool_row(&fx).await, reconciler.queries()),
        ((Failed, 2), 1)
    );
    claim_round(&fx).await;

    assert_eq!(
        fx.tools.invocation_count(TOOL),
        3,
        "the budget of three bought three calls"
    );
    assert_eq!(
        (tool_row(&fx).await, reconciler.queries()),
        ((Exhausted, 3), 2)
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, attempts, .. }) => {
            assert_eq!(code, "dispatch-collaborator-failed");
            assert_eq!(attempts, Some(3));
        }
        other => panic!("the run fails on the call, not {other:?}"),
    }
}

/// What the reconciler does the first time it is asked, before it answers.
enum BeforeTheAnswer {
    /// It takes longer than the asking claim's lease.
    OutlastsTheLease,
    /// It takes longer than the lease, and a rival worker runs a whole pass
    /// meanwhile.
    OutlastsTheLeaseWhileARivalRuns(Box<Pipeline>),
    /// A worker on a lapsed lease of its own writes the ticket's row, as the
    /// attempt it was making would: marks it `Dispatching`, then, if `fails`,
    /// records that attempt's failure. The asking claim's lease stands.
    AStaleWorkerWritesTheRow {
        store: WorkflowStore,
        clock: SharedAtomicWorkflowClock,
        fails: bool,
    },
}

/// A reconciler whose first answer comes after [`BeforeTheAnswer`].
struct HookedReconciler {
    answers: ScriptedReconciler,
    clock: SharedAtomicWorkflowClock,
    first: Mutex<Option<BeforeTheAnswer>>,
}

impl HookedReconciler {
    /// A reconciler answering as `answers` does, with nothing armed.
    fn new(fx: &AuthorityFixture, answers: &ScriptedReconciler) -> Arc<Self> {
        Arc::new(Self {
            answers: answers.clone(),
            clock: fx.wf_clock.clone(),
            first: Mutex::new(None),
        })
    }

    /// Arms what happens before the next answer.
    fn arm(&self, before: BeforeTheAnswer) {
        *self.first.lock().expect("the lock is not poisoned") = Some(before);
    }
}

impl AgentEffectReconciler for HookedReconciler {
    fn reconcile<'a>(
        &'a self,
        protocol: &'a AgentReconciliationProtocolRef,
        scope: &'a AgentRunScope,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding> {
        Box::pin(async move {
            let first = self.first.lock().expect("the lock is not poisoned").take();
            match first {
                None => {}
                Some(BeforeTheAnswer::OutlastsTheLease) => self.clock.advance(LEASE_MS + 1),
                Some(BeforeTheAnswer::OutlastsTheLeaseWhileARivalRuns(mut rival)) => {
                    self.clock.advance(LEASE_MS + 1);
                    rival
                        .pump_run(&run_scope())
                        .await
                        .expect("the rival's pass runs");
                }
                Some(BeforeTheAnswer::AStaleWorkerWritesTheRow {
                    store,
                    clock,
                    fails,
                }) => {
                    let row = OutboxMessageId::new(effect.dispatch_ticket_id().as_str());
                    let mut inbox = rakka_agent_workflow::AgentRunInbox::with_clock(
                        rakka_agent::workflow_run_id(scope),
                        store,
                        clock,
                    );
                    inbox.recover().await.expect("the inbox recovers");
                    inbox
                        .inner_mut()
                        .mark_outbox_dispatching(&row)
                        .await
                        .expect("the stale worker marks the row");
                    if fails {
                        inbox.recover().await.expect("the inbox recovers");
                        inbox
                            .inner_mut()
                            .record_outbox_failure(&row, "upstream-unavailable: later", false)
                            .await
                            .expect("the stale worker records its failure");
                    }
                }
            }
            self.answers.reconcile(protocol, scope, effect).await
        })
    }
}

/// A claim whose ticket another worker moved while the reconciler was
/// answering makes no attempt. The call's first attempt failed. A claim asks
/// the reconciler about it, and the reconciler is slow: the claim's lease
/// lapses, and a rival worker claims the ticket, asks, and makes attempt two,
/// which fails too. When the first claim is told "not executed", its lease is
/// over, so it ends there and writes nothing, as one whose lease ran out
/// does; the row, which now counts two attempts, would have ended it too. The
/// next claim makes attempt three.
///
/// The fix's first version went on with the number it had first read: a
/// second call went out as attempt two, under the same durable span identity
/// as the rival's. `main` made that call too, under a later number.
#[tokio::test]
async fn a_claim_whose_ticket_moved_while_the_reconciler_answered_makes_no_attempt() {
    let answers = not_executed();
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = failing_reconcileable_call(6).with_dispatch_segments(segments.clone());
    let reconciler = HookedReconciler::new(&fx, &answers);
    let fx = fx.with_reconciler(reconciler.clone());
    reconciler.arm(BeforeTheAnswer::OutlastsTheLeaseWhileARivalRuns(Box::new(
        fx.worker("worker-2", fx.tools.clone(), None),
    )));
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));

    let late = the_late_claim(&fx).await;

    assert_eq!(
        (late.invoked, late.lapsed),
        (0, 1),
        "the late claim made no attempt and ended as out of lease"
    );
    assert_eq!(
        (fx.tools.invocation_count(TOOL), answers.queries()),
        (2, 2),
        "the rival's call is the only one since the first"
    );
    assert_eq!(tool_row(&fx).await, (Failed, 2));

    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 3));
    assert_eq!(
        failed_tool_attempts(&segments),
        vec!["1", "2", "3"],
        "each call was made as its own attempt"
    );
}

/// A claim whose lease lapsed while the reconciler answered makes no attempt,
/// though nothing has moved the row yet. Once the lease is over another
/// worker may take the ticket at any moment, and an attempt made now could
/// go out beside that worker's. The claim ends out of lease and writes
/// nothing, and the next claim makes the attempt. Only that the lease is
/// current is asked, not that it covers the attempt: every claim on this
/// path asks the reconciler first, and a cover rule would never retry an
/// effect whose answer and call together outlast the lease.
///
/// The budget is two, so the attempt is the call's last: that one follows is
/// said by the row having counted the failed attempt, and by nothing else.
#[tokio::test]
async fn a_claim_whose_lease_lapsed_while_the_reconciler_answered_makes_no_attempt() {
    let answers = not_executed();
    let fx = failing_reconcileable_call(2);
    let reconciler = HookedReconciler::new(&fx, &answers);
    let fx = fx.with_reconciler(reconciler.clone());
    reconciler.arm(BeforeTheAnswer::OutlastsTheLease);
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));

    let late = the_late_claim(&fx).await;

    assert_eq!((late.invoked, late.lapsed), (0, 1));
    assert_eq!((fx.tools.invocation_count(TOOL), answers.queries()), (1, 1));
    assert_eq!(tool_row(&fx).await, (Failed, 1), "nothing was written");
    claim_round(&fx).await;
    assert_eq!(
        (fx.tools.invocation_count(TOOL), tool_row(&fx).await),
        (2, (Exhausted, 2)),
        "the next claim made the last attempt"
    );
}

/// The same for an attempt whose worker was lost after its durable
/// `Started`: a claim whose lease lapsed while the reconciler answered burns
/// nothing and makes no attempt. The row still marks the lost attempt as
/// started and uncounted, and the next claim asks about it again, counts it,
/// and makes the next attempt.
#[tokio::test]
async fn a_lost_attempts_claim_whose_lease_lapsed_while_the_reconciler_answered_burns_nothing() {
    let answers = not_executed();
    let fx = failing_reconcileable_call(6);
    let reconciler = HookedReconciler::new(&fx, &answers);
    let fx = fx.with_reconciler(reconciler.clone());
    reconciler.arm(BeforeTheAnswer::OutlastsTheLease);
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    assert!(fx.one_pass().await.died, "the worker is lost after Started");
    assert_eq!(tool_row(&fx).await, (Dispatching, 0));

    let late = the_late_claim(&fx).await;

    assert_eq!((late.invoked, late.lapsed), (0, 1));
    assert_eq!((fx.tools.invocation_count(TOOL), answers.queries()), (0, 1));
    assert_eq!(tool_row(&fx).await, (Dispatching, 0), "nothing was burned");
    claim_round(&fx).await;
    assert_eq!(
        (fx.tools.invocation_count(TOOL), tool_row(&fx).await),
        (1, (Failed, 2)),
        "the next claim counted the lost attempt and made the next"
    );
}

/// A failed call's next claim, run while a worker on a lapsed lease of its
/// own writes the ticket's row as the reconciler answers. The claim's own
/// lease stands throughout. Answers that claim's pass.
async fn a_stale_worker_writes_the_row_during_the_question(
    attempts: u32,
    fails: bool,
) -> (AuthorityFixture, AgentDispatchPass) {
    let fx = failing_reconcileable_call(attempts);
    let reconciler = HookedReconciler::new(&fx, &not_executed());
    let fx = fx.with_reconciler(reconciler.clone());
    reconciler.arm(BeforeTheAnswer::AStaleWorkerWritesTheRow {
        store: fx.workflow_store.clone(),
        clock: fx.wf_clock.clone(),
        fails,
    });
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    let pass = claim_round(&fx).await;
    (fx, pass)
}

/// A claim that finds another worker's attempt in flight makes none beside
/// it. The row is `Dispatching` at the count the claim read: what changed is
/// its status alone.
#[tokio::test]
async fn a_claim_that_finds_another_workers_attempt_in_flight_makes_none_beside_it() {
    let (fx, pass) = a_stale_worker_writes_the_row_during_the_question(6, false).await;

    assert_eq!((pass.invoked, pass.lapsed), (0, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(tool_row(&fx).await, (Dispatching, 1));
}

/// A claim whose row another worker's attempt failed meanwhile makes no
/// attempt: the row is `Failed` again, at a count the claim did not read.
#[tokio::test]
async fn a_claim_whose_row_another_worker_failed_meanwhile_makes_no_attempt() {
    let (fx, pass) = a_stale_worker_writes_the_row_during_the_question(6, true).await;

    assert_eq!((pass.invoked, pass.lapsed), (0, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(tool_row(&fx).await, (Failed, 2));
}

/// A row another worker exhausted while the reconciler answered is told to
/// the run at once. That worker's lease had lapsed, so it could not tell the
/// run; the claim that still holds the ticket takes the finished row up as a
/// claim that read it first would. The fix's first version ended such a
/// claim out of lease, and the run heard only once the lease was over.
#[tokio::test]
async fn a_row_another_worker_exhausted_meanwhile_is_told_to_the_run_at_once() {
    let (fx, pass) = a_stale_worker_writes_the_row_during_the_question(2, true).await;

    assert_eq!((pass.invoked, pass.lapsed, pass.delivered), (0, 0, 1));
    assert_eq!(tool_row(&fx).await, (Exhausted, 2));
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { attempts, .. }) => {
            assert_eq!(attempts, Some(2));
        }
        other => panic!("the run fails on the call, not {other:?}"),
    }
}

fn traced() -> rakka_agent_workflow::AgentTelemetryContext {
    rakka_agent_workflow::AgentTelemetryContext {
        trace_parent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        ..rakka_agent_workflow::AgentTelemetryContext::default()
    }
}

/// The span id of a segment's `ambiguous-attempt` link, if it has one.
fn ambiguous_attempt_link(segment: &rakka_agent::AgentTelemetrySegment) -> Option<String> {
    segment
        .telemetry
        .span_links
        .iter()
        .find(|link| {
            link.attributes
                .get(rakka_agent::ATTR_AGENT_TELEMETRY_LINK_KIND)
                .map(String::as_str)
                == Some(rakka_agent::LINK_KIND_AMBIGUOUS_ATTEMPT)
        })
        .map(|link| link.span_id.clone())
}

/// A park made at once after a failed call names the attempt that failed and
/// links to its span. With no reconciler wired, the claim after the failed
/// first attempt parks the call. The dispatcher's park segment and the run's
/// `checkpoint-open` segment both name attempt one, and their
/// `ambiguous-attempt` links point at the span the failed attempt exported.
/// They used to name attempt two, a span no segment exported.
#[tokio::test]
async fn a_park_after_a_failed_call_names_and_links_the_attempt_that_failed() {
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = failing_reconcileable_call(3)
        .with_segments(segments.clone())
        .with_dispatch_segments(segments.clone());
    fx.fx
        .instantiate_agent_with_envelope(fx.envelope.clone())
        .await;
    fx.fx.create_task_traced(traced()).await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));

    claim_round(&fx).await;

    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    assert_eq!(
        (
            effect.status,
            effect.attempts,
            effect.last_error_code.as_deref()
        ),
        (
            AgentRunEffectStatus::Indeterminate,
            1,
            Some("reconciler-missing")
        )
    );
    let failed_span = effect
        .attempt_span_identity(1)
        .expect("a traced effect derives its attempts' spans")
        .span_id;
    let segments = segments.segments();
    let failed_attempt = segments
        .iter()
        .find(|segment| {
            matches!(
                segment.operation,
                AgentSegmentOperation::EffectDispatch {
                    effect_kind: "tool-call"
                }
            ) && segment.error_code.as_deref() == Some("dispatch-collaborator-failed")
        })
        .expect("the failed attempt closed its dispatch segment");
    assert_eq!(
        failed_attempt.span_id.as_deref(),
        Some(failed_span.as_str()),
        "the failed attempt exported attempt one's span"
    );
    let parks: Vec<_> = segments
        .iter()
        .filter(|segment| ambiguous_attempt_link(segment).is_some())
        .map(|segment| {
            (
                segment.operation.as_label(),
                segment.attributes.get(ATTEMPT).cloned(),
                ambiguous_attempt_link(segment),
            )
        })
        .collect();
    assert_eq!(
        parks,
        vec![
            (
                "checkpoint-open",
                Some("1".to_string()),
                Some(failed_span.clone())
            ),
            (
                "effect-dispatch",
                Some("1".to_string()),
                Some(failed_span.clone())
            ),
        ],
        "both park segments name attempt one and link to its span"
    );
}

/// An operator cancels the run.
async fn cancel_the_run(fx: &AuthorityFixture) {
    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        rakka_agent::AgentRunEntityCommand::Cancel {
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
    .expect("the cancellation applies");
}

/// A cancelled run's failed call is cancelled on the reconciler's finding,
/// though its agent is suspended. Where the wind-down fence applies the
/// finding decides with no attempt to make, so the authority is not asked
/// first: a suspended agent's deferral would hold the cancelled run open for
/// as long as the suspension lasts.
#[tokio::test]
async fn a_cancelled_runs_failed_call_is_cancelled_on_the_finding_while_its_agent_is_suspended() {
    let reconciler = not_executed();
    let fx = failing_reconcileable_call(3).with_reconciler(Arc::new(reconciler.clone()));
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    fx.set_suspended(true, "suspend-1").await;
    cancel_the_run(&fx).await;

    let pass = claim_round(&fx).await;

    assert_eq!((pass.deferred, reconciler.queries()), (0, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    assert_eq!(
        (effect.status, effect.attempts),
        (AgentRunEffectStatus::Cancelled, 1)
    );
    assert_eq!(tool_row(&fx).await, (OutboxStatus::Cancelled, 1));
}
