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
//!   spends nothing and asks the reconciler nothing, except under the
//!   wind-down fence or with no reconciler wired; an error from that first
//!   question is left for the attempt;
//! - after the answer, a claim makes its first attempt only while its lease
//!   is still current, on this path and on a lost attempt's;
//! - it reads the row and the run again, and makes no attempt when another
//!   worker has moved the row; a row that worker finished is settled
//!   against the run as it now stands;
//! - a park made at once names and links the attempt that failed;
//! - a refusal whose authority answered once the lease had lapsed is left
//!   to the worker that holds the ticket (issue #196).
//!
//! `effect_dispatch.rs` holds the rest of the issue's cases.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, InProcessRunResultDelivery, RecordingToolExecutor,
    ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentAuthorityRefusal, AgentDispatchAuthority, AgentDispatchDecision, AgentDispatchError,
    AgentDispatchFuture, AgentDispatchPass, AgentDispatchToolExecutor, AgentDispatchWindow,
    AgentEffectReconciler, AgentEffectSpec, AgentEntityAuthority, AgentModelTurn,
    AgentReconciliationFinding, AgentReconciliationProtocolRef, AgentRunEffect,
    AgentRunEffectDispatcher, AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunScope,
    AgentRunState, AgentRunStatus, AgentRunTerminalReason, AgentSegmentOperation, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, InMemoryAgentSegmentSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxMessageId;
use rakka_agent_workflow::substrate::OutboxStatus::{self, Dispatching, Exhausted, Failed};
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherWorkerId, AgentEphemeralCredential,
    AgentTimestampMillis,
};
use tokio::sync::oneshot;

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
    /// A worker on a lapsed lease of its own makes the attempt and it
    /// succeeds: it marks the row `Dispatching`, tells the run the call's
    /// result, and finishes the row as `Dispatched`, the order a worker
    /// keeps. Since issue #207 a worker can be there only by stalling between
    /// its lease check after the authority's answer and its `Started`.
    AStaleWorkerSucceeds {
        store: WorkflowStore,
        clock: SharedAtomicWorkflowClock,
        delivery: Arc<dyn rakka_agent::AgentRunResultDelivery>,
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
                Some(BeforeTheAnswer::AStaleWorkerSucceeds {
                    store,
                    clock,
                    delivery,
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
                    let rakka_agent::AgentRunEffectRequest::Tool { call } = &effect.request else {
                        panic!("the ticket is a tool call");
                    };
                    delivery
                        .deliver(
                            scope,
                            rakka_agent::AgentRunEntityCommand::RecordEffectResult {
                                operation_id: effect
                                    .result_operation_id(scope)
                                    .expect("the operation id derives"),
                                effect_id: effect.effect_id.clone(),
                                generation: effect.generation,
                                attempt: 2,
                                fence: 0,
                                outcome: Box::new(AgentRunEffectOutcome::Tool {
                                    call_id: call.call_id.clone(),
                                    content: AgentTaskContent::inline(
                                        serde_json::json!({ "charged": true }),
                                    )
                                    .expect("the result is inline-bounded"),
                                }),
                            },
                        )
                        .await
                        .expect("the stale worker tells the run");
                    inbox.recover().await.expect("the inbox recovers");
                    inbox
                        .inner_mut()
                        .record_outbox_success(&row)
                        .await
                        .expect("the stale worker finishes the row");
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

// ---------------------------------------------------------------------------
// The third review's cases: workers that run at once, an authority that
// answers per call, and a reconciler that hands off while it is asked.
// ---------------------------------------------------------------------------

/// A tool that fails its first `fail_first` calls and then succeeds.
#[derive(Clone)]
struct FlakyTool {
    calls: Arc<AtomicUsize>,
    fail_first: usize,
}

impl FlakyTool {
    fn failing_first(fail_first: usize) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            fail_first,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentDispatchToolExecutor for FlakyTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) < self.fail_first {
                Err(AgentDispatchError::collaborator(
                    "upstream-unavailable",
                    "later",
                ))
            } else {
                Ok(
                    AgentTaskContent::inline(serde_json::json!({ "receipt": "r-195" }))
                        .expect("the content is inline-bounded"),
                )
            }
        })
    }
}

/// What one `authorize` call of a [`ScriptedAuthority`] answers.
#[derive(Clone)]
enum Answer {
    /// The agent entity authority's own answer.
    Inner,
    /// An error from the authority itself.
    Fail,
}

/// A hold on a [`ScriptedAuthority`]: its first call made once `armed` is
/// set moves the clock past the lease, says so on `held`, and waits for
/// `release`, then answers `refusal` if there is one.
struct AuthorityHold {
    armed: Arc<AtomicBool>,
    clock: SharedAtomicWorkflowClock,
    held: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    refusal: Option<AgentAuthorityRefusal>,
}

/// The agent entity authority, scripted per call, recording the attempt each
/// call was about.
struct ScriptedAuthority {
    inner: Arc<dyn AgentDispatchAuthority>,
    script: Vec<Answer>,
    asked: Mutex<Vec<u32>>,
    hold: Mutex<Option<AuthorityHold>>,
}

impl ScriptedAuthority {
    fn new(fx: &AuthorityFixture, script: Vec<Answer>) -> Self {
        Self {
            inner: Arc::new(AgentEntityAuthority::new(
                fx.fx.agents.clone(),
                fx.authority.clone(),
            )),
            script,
            asked: Mutex::new(Vec::new()),
            hold: Mutex::new(None),
        }
    }

    /// The attempt each call so far was about.
    fn asked(&self) -> Vec<u32> {
        self.asked.lock().expect("the lock is not poisoned").clone()
    }
}

impl AgentDispatchAuthority for ScriptedAuthority {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            let call = {
                let mut asked = self.asked.lock().expect("the lock is not poisoned");
                asked.push(attempt);
                asked.len() - 1
            };
            let hold = {
                let mut hold = self.hold.lock().expect("the lock is not poisoned");
                if hold
                    .as_ref()
                    .is_some_and(|hold| hold.armed.load(Ordering::SeqCst))
                {
                    hold.take()
                } else {
                    None
                }
            };
            if let Some(hold) = hold {
                hold.clock.advance(LEASE_MS + 1);
                let _ = hold.held.send(());
                let _ = hold.release.await;
                if let Some(refusal) = hold.refusal {
                    return Ok(AgentDispatchDecision::Refused(refusal));
                }
            }
            match self.script.get(call).cloned().unwrap_or(Answer::Inner) {
                Answer::Inner => self.inner.authorize(scope, run, intent, attempt, now).await,
                Answer::Fail => Err(AgentDispatchError::collaborator(
                    "authority-unavailable",
                    "the agent store did not answer",
                )),
            }
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

/// A reconciler answering as `answers` does. When asked, it sets `asked` if
/// there is one, and the first time, if a hand-off is set, says so on its
/// sender and waits on its receiver before it answers.
struct HandingOffReconciler {
    answers: ScriptedReconciler,
    asked: Option<Arc<AtomicBool>>,
    handoff: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl HandingOffReconciler {
    fn new(answers: &ScriptedReconciler) -> Self {
        Self {
            answers: answers.clone(),
            asked: None,
            handoff: Mutex::new(None),
        }
    }

    /// Hands off on the first question: answers the receiver that says the
    /// question was asked, and the sender that lets it answer.
    fn handing_off(
        answers: &ScriptedReconciler,
    ) -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (asked_tx, asked_rx) = oneshot::channel();
        let (answer_tx, answer_rx) = oneshot::channel();
        let reconciler = Self::new(answers);
        *reconciler.handoff.lock().expect("the lock is not poisoned") = Some((asked_tx, answer_rx));
        (reconciler, asked_rx, answer_tx)
    }
}

impl AgentEffectReconciler for HandingOffReconciler {
    fn reconcile<'a>(
        &'a self,
        protocol: &'a AgentReconciliationProtocolRef,
        scope: &'a AgentRunScope,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding> {
        Box::pin(async move {
            if let Some(flag) = &self.asked {
                flag.store(true, Ordering::SeqCst);
            }
            let handoff = self
                .handoff
                .lock()
                .expect("the lock is not poisoned")
                .take();
            if let Some((asked, answer)) = handoff {
                let _ = asked.send(());
                let _ = answer.await;
            }
            self.answers.reconcile(protocol, scope, effect).await
        })
    }
}

/// A worker over the fixture's stores with the caller's authority, tool,
/// reconciler and segment sink.
fn worker(
    fx: &AuthorityFixture,
    worker_id: &str,
    authority: Arc<dyn AgentDispatchAuthority>,
    tools: Arc<dyn AgentDispatchToolExecutor>,
    reconciler: Arc<dyn AgentEffectReconciler>,
    segments: Option<Arc<InMemoryAgentSegmentSink>>,
) -> Pipeline {
    let delivery = InProcessRunResultDelivery::new(
        fx.fx.runs.clone(),
        fx.fx.effects.clone(),
        fx.fx.router.clone(),
        fx.fx.clock.clone(),
    )
    .with_effect_policies(fx.fx.policies.clone());
    let mut pipeline = AgentRunEffectDispatcher::new(
        AgentDispatcherWorkerId::new(worker_id),
        fx.workflow_store.clone(),
        fx.fleet_store.clone(),
        fx.fx.runs.clone(),
        fx.wf_clock.clone(),
        Arc::new(fx.adapter.clone()),
        tools,
        authority,
        Arc::new(delivery),
    )
    .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
    .with_probe(Arc::new(fx.probe.clone()))
    .with_reconciler(reconciler);
    if let Some(segments) = segments {
        pipeline = pipeline.with_segments(segments);
    }
    pipeline
}

/// One claim by `pipeline`: expires the lease, settles the entities, runs one
/// pass.
async fn claim_with(
    fx: &AuthorityFixture,
    pipeline: &mut Pipeline,
) -> rakka_agent::AgentDispatchResult<AgentDispatchPass> {
    fx.expire_lease();
    fx.settle().await;
    pipeline.pump_run(&run_scope()).await
}

/// A fixture whose run calls a reconcileable tool of three attempts, its
/// ticket out and no attempt made.
async fn a_reconcileable_call_out() -> AuthorityFixture {
    let fx = failing_reconcileable_call(3);
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx
}

/// The tool's result the reconciler establishes.
fn executed() -> AgentReconciliationFinding {
    AgentReconciliationFinding::Executed {
        outcome: Box::new(AgentRunEffectOutcome::Tool {
            call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
            content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-195" }))
                .expect("the content is inline-bounded"),
        }),
    }
}

/// How many `tool-authorize` segments have closed.
fn authorize_segments(segments: &InMemoryAgentSegmentSink) -> usize {
    segments
        .segments()
        .iter()
        .filter(|segment| {
            matches!(
                segment.operation,
                AgentSegmentOperation::ToolAuthorize { .. }
            )
        })
        .count()
}

/// The claim after a failed call asks the authority before the reconciler,
/// and the attempt it then makes asks it again: two questions about attempt
/// two, and two `tool-authorize` segments, where there was one. Found
/// executed, the claim makes no attempt, and the one question is the one it
/// asked first.
#[tokio::test]
async fn the_claim_after_a_failed_call_asks_the_authority_first_and_its_attempt_asks_again() {
    for (finding, asked, invoked) in [
        (AgentReconciliationFinding::NotExecuted, vec![2, 2], 1),
        (executed(), vec![2], 0),
    ] {
        let fx = a_reconcileable_call_out().await;
        let authority = Arc::new(ScriptedAuthority::new(&fx, Vec::new()));
        let segments = Arc::new(InMemoryAgentSegmentSink::new());
        let mut pipeline = worker(
            &fx,
            "worker-1",
            authority.clone(),
            Arc::new(FlakyTool::failing_first(99)),
            Arc::new(ScriptedReconciler::new().with_finding(finding)),
            Some(segments.clone()),
        );
        claim_with(&fx, &mut pipeline)
            .await
            .expect("the first claim's pass runs");
        assert_eq!(tool_row(&fx).await, (Failed, 1));
        let (before, segments_before) = (authority.asked().len(), authorize_segments(&segments));

        let pass = claim_with(&fx, &mut pipeline)
            .await
            .expect("the claim's pass runs");

        assert_eq!(pass.invoked, invoked);
        assert_eq!(authority.asked()[before..], asked[..]);
        assert_eq!(
            authorize_segments(&segments) - segments_before,
            asked.len(),
            "one tool-authorize segment for each question"
        );
    }
}

/// An authority that fails before the reconciler is asked does not withhold
/// a reconciled outcome. The question to the authority is only there to save
/// the reconciler a question while a retry would be deferred, so its error
/// is left for the attempt to meet: found executed, the claim makes no
/// attempt, and the run takes the outcome.
#[tokio::test]
async fn an_authority_error_before_the_question_does_not_withhold_a_reconciled_outcome() {
    let fx = a_reconcileable_call_out().await;
    let answers = ScriptedReconciler::new().with_finding(executed());
    let mut pipeline = worker(
        &fx,
        "worker-1",
        Arc::new(ScriptedAuthority::new(
            &fx,
            vec![Answer::Inner, Answer::Fail, Answer::Fail],
        )),
        Arc::new(FlakyTool::failing_first(99)),
        Arc::new(answers.clone()),
        None,
    );
    claim_with(&fx, &mut pipeline)
        .await
        .expect("the first claim's pass runs");
    assert_eq!(tool_row(&fx).await, (Failed, 1));

    let pass = claim_with(&fx, &mut pipeline)
        .await
        .expect("the authority's error does not end the pass");

    assert_eq!((pass.invoked, pass.delivered), (0, 1));
    assert_eq!(answers.queries(), 1);
    assert!(
        fx.effect_at(1).await.is_none(),
        "the reconciled result moved the turn on"
    );
}

/// A failed call with no reconciler wired parks at once, though its agent is
/// suspended. With no reconciler there is no question to save, so the
/// authority is not asked first, and the park is not held for the length of
/// the suspension.
#[tokio::test]
async fn a_failed_call_with_no_reconciler_parks_at_once_while_its_agent_is_suspended() {
    let fx = a_reconcileable_call_out().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    fx.set_suspended(true, "suspend-1").await;

    let pass = claim_round(&fx).await;

    assert_eq!((pass.deferred, pass.parked_indeterminate), (0, 1));
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
}

/// A suspension that lands while the reconciler answers is honoured by the
/// attempt's own question to the authority: the first question was granted,
/// and the attempt that follows the finding is deferred, not made.
#[tokio::test]
async fn a_suspension_during_the_question_is_honoured_by_the_attempts_own_question() {
    let fx = a_reconcileable_call_out().await;
    let tool = FlakyTool::failing_first(99);
    let answers = not_executed();
    let mut first = worker(
        &fx,
        "worker-1",
        Arc::new(ScriptedAuthority::new(&fx, Vec::new())),
        Arc::new(tool.clone()),
        Arc::new(answers.clone()),
        None,
    );
    claim_with(&fx, &mut first)
        .await
        .expect("the first claim's pass runs");
    let (reconciler, asked, answer) = HandingOffReconciler::handing_off(&answers);
    let mut second = worker(
        &fx,
        "worker-2",
        Arc::new(ScriptedAuthority::new(&fx, Vec::new())),
        Arc::new(tool.clone()),
        Arc::new(reconciler),
        None,
    );

    let (pass, ()) = tokio::join!(claim_with(&fx, &mut second), async {
        let _ = asked.await;
        fx.set_suspended(true, "suspend-1").await;
        let _ = answer.send(());
    });

    let pass = pass.expect("the claim's pass runs");
    assert_eq!((pass.deferred, pass.invoked), (1, 0));
    assert_eq!(tool.calls(), 1);
    assert_eq!(tool_row(&fx).await, (Failed, 1));
}

/// What [`worker_one_retries_on_a_lapsed_lease_while_worker_two_asks`]
/// leaves: each worker's pass, and the fixture and tool they shared.
struct Race {
    one: rakka_agent::AgentDispatchResult<AgentDispatchPass>,
    two: AgentDispatchPass,
    fx: AuthorityFixture,
    tool: FlakyTool,
}

/// Worker one is told "not executed" about a failed call, and its lease
/// lapses while the authority answers for the retry: with a grant when
/// `refusal` is `None`, else with `refusal`. Worker two claims the ticket,
/// and worker one goes on while worker two's reconciler answers it, which
/// it does once worker one's pass is over.
async fn worker_one_retries_on_a_lapsed_lease_while_worker_two_asks(
    refusal: Option<AgentAuthorityRefusal>,
) -> Race {
    let fx = a_reconcileable_call_out().await;
    let tool = FlakyTool::failing_first(1);
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let mut failing = worker(
        &fx,
        "worker-0",
        Arc::new(ScriptedAuthority::new(&fx, Vec::new())),
        Arc::new(tool.clone()),
        Arc::new(not_executed()),
        Some(segments.clone()),
    );
    claim_with(&fx, &mut failing)
        .await
        .expect("the first claim's pass runs");
    assert_eq!(tool_row(&fx).await, (Failed, 1));

    let one_asked = Arc::new(AtomicBool::new(false));
    let (held_tx, held) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let held_authority = ScriptedAuthority::new(&fx, Vec::new());
    *held_authority
        .hold
        .lock()
        .expect("the lock is not poisoned") = Some(AuthorityHold {
        armed: one_asked.clone(),
        clock: fx.wf_clock.clone(),
        held: held_tx,
        release: release_rx,
        refusal,
    });
    let mut one_reconciler = HandingOffReconciler::new(&not_executed());
    one_reconciler.asked = Some(one_asked);
    let mut one = worker(
        &fx,
        "worker-1",
        Arc::new(held_authority),
        Arc::new(tool.clone()),
        Arc::new(one_reconciler),
        Some(segments.clone()),
    );
    let (two_reconciler, two_asked, two_answer) =
        HandingOffReconciler::handing_off(&not_executed());
    let mut two = worker(
        &fx,
        "worker-2",
        Arc::new(ScriptedAuthority::new(&fx, Vec::new())),
        Arc::new(tool.clone()),
        Arc::new(two_reconciler),
        Some(segments.clone()),
    );
    let (one_done_tx, one_done) = oneshot::channel::<()>();

    fx.expire_lease();
    fx.settle().await;
    let (one, two, ()) = tokio::join!(
        async {
            let pass = one.pump_run(&run_scope()).await;
            let _ = one_done_tx.send(());
            pass
        },
        async {
            let _ = held.await;
            two.pump_run(&run_scope()).await
        },
        async {
            let _ = two_asked.await;
            let _ = release.send(());
            let _ = one_done.await;
            let _ = two_answer.send(());
        }
    );

    Race {
        one,
        two: two.expect("worker two's pass runs"),
        fx,
        tool,
    }
}

/// A late grant leaves the retry to the worker that holds the ticket (issue
/// #207). In the race above, worker one's authority grants the retry in an
/// answer that came after its lease lapsed. Worker one ends out of lease and
/// writes nothing. Worker two, told "not executed" under a current lease,
/// finds the row where it read it and makes the retry, which succeeds. The
/// late grant used to make the retry itself, beside worker two's claim.
#[tokio::test]
async fn a_late_grant_leaves_the_retry_to_the_worker_that_holds_the_ticket() {
    let Race {
        one, two, fx, tool, ..
    } = worker_one_retries_on_a_lapsed_lease_while_worker_two_asks(None).await;

    let one = one.expect("worker one's pass runs");
    assert_eq!(
        (one.lapsed, one.invoked, one.delivered, one.cancelled),
        (1, 0, 0, 0),
        "worker one ends out of lease and writes nothing"
    );
    assert_eq!(
        (two.invoked, two.delivered, two.parked_indeterminate),
        (1, 1, 0)
    );
    assert_eq!(tool.calls(), 2, "the failed call, then worker two's retry");
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed,
        "the run completes on worker two's retry"
    );
}

/// A row another worker finished, after it had told the run, is not parked
/// by a claim that read it again. A failed call's next claim asks the
/// reconciler; meanwhile a worker on a lapsed lease of its own makes the
/// attempt, which succeeds: it tells the run, then finishes the row. Told
/// "not executed" under a current lease, the claim reads the row again and
/// the run with it, finds the call resolved, and settles its ticket. It
/// used to read the finished row against the run as it read it before the
/// question, and parked the call as unknown (issue #195).
///
/// The other worker was worker one of the race above, making the retry on
/// a grant that came after its lease, until issue #207 ended such a claim
/// at the grant. A worker can still be there by stalling between its lease
/// check and its `Started`, which the reconciler's hook stands in for.
#[tokio::test]
async fn a_row_another_worker_finished_with_a_success_it_delivered_is_not_parked() {
    let fx = failing_reconcileable_call(6);
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let reconciler = HookedReconciler::new(&fx, &not_executed());
    let fx = fx
        .with_reconciler(reconciler.clone())
        .with_segments(segments.clone())
        .with_dispatch_segments(segments.clone());
    let delivery = InProcessRunResultDelivery::new(
        fx.fx.runs.clone(),
        fx.fx.effects.clone(),
        fx.fx.router.clone(),
        fx.fx.clock.clone(),
    )
    .with_effect_policies(fx.fx.policies.clone());
    reconciler.arm(BeforeTheAnswer::AStaleWorkerSucceeds {
        store: fx.workflow_store.clone(),
        clock: fx.wf_clock.clone(),
        delivery: Arc::new(delivery),
    });
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    // Read before the claim: a resolved call leaves the run once its turn
    // is recorded.
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");

    let two = claim_round(&fx).await;

    assert_eq!(
        (two.invoked, two.parked_indeterminate),
        (0, 0),
        "the claim made no attempt and parked nothing"
    );
    assert_eq!(
        fx.outbox_row(&effect)
            .await
            .expect("the ticket's row exists")
            .status(),
        OutboxStatus::Dispatched,
        "the other worker finished the row"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "the claim invoked nothing; only the failed call ran here"
    );
    let parked: Vec<_> = segments
        .segments()
        .iter()
        .filter(|segment| {
            segment
                .attributes
                .get("rakka.agent.effect.status")
                .map(String::as_str)
                == Some("indeterminate")
        })
        .map(|segment| segment.operation.as_label())
        .collect();
    assert_eq!(
        parked,
        Vec::<&str>::new(),
        "no segment says the call is unknown"
    );
    assert_ne!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForReconciliation
    );
}

/// A refusal on a lapsed lease leaves the retry to the worker that holds the
/// ticket (issue #196). In the race above, worker one's authority refuses
/// the retry, in an answer that came after its lease lapsed. Worker one
/// tells the run nothing and writes nothing to the row. Worker two, told
/// "not executed" under a current lease, finds the row where it read it and
/// makes the retry, which succeeds. The refusal used to fail the run and
/// cancel the row, and worker two then settled its ticket against them.
#[tokio::test]
async fn a_refusal_on_a_lapsed_lease_leaves_the_retry_to_the_worker_that_holds_the_ticket() {
    let Race {
        one, two, fx, tool, ..
    } = worker_one_retries_on_a_lapsed_lease_while_worker_two_asks(Some(
        AgentAuthorityRefusal::of("tool-revoked", "the tool was revoked"),
    ))
    .await;

    let one = one.expect("worker one's pass runs");
    assert_eq!(
        (one.lapsed, one.delivered, one.cancelled),
        (1, 0, 0),
        "worker one ends out of lease and writes nothing"
    );
    assert_eq!(
        (two.invoked, two.delivered, two.parked_indeterminate),
        (1, 1, 0)
    );
    assert_eq!(tool.calls(), 2, "the failed call, then worker two's retry");
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed,
        "the run completes on worker two's retry"
    );
}
