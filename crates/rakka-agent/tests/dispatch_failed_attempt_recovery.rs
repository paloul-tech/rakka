//! A reconcileable call's failed attempt, recovered (issue #195): the cases
//! the review of the fix found.
//!
//! A failed attempt's outbox row counted that attempt when it failed, so the
//! claim that follows asks the reconciler about an attempt already counted,
//! and a finding of "not executed" goes straight to the next attempt with
//! nothing written. These tests hold what follows from that: a retry the
//! dispatch authority defers spends none of the budget; a claim whose ticket
//! another worker moved while the reconciler was answering makes no attempt;
//! and a park made at once names and links the attempt that failed.
//! `effect_dispatch.rs` holds the rest of the issue's cases.

use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, RecordingToolExecutor, ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentDispatchFuture, AgentEffectReconciler, AgentEffectSpec, AgentModelTurn,
    AgentReconciliationFinding, AgentReconciliationProtocolRef, AgentRunEffect,
    AgentRunEffectStatus, AgentRunScope, AgentRunStatus, AgentRunTerminalReason,
    AgentSegmentOperation, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentSegmentSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus::{self, Exhausted, Failed};

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
async fn claim_round(fx: &AuthorityFixture) -> rakka_agent::AgentDispatchPass {
    fx.expire_lease();
    fx.one_pass().await
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
/// the budget. The agent is suspended after the call's first attempt failed:
/// each claim asks the reconciler, is told "not executed", and the authority
/// defers the retry. The row keeps the one attempt it counted and the run is
/// charged nothing more, so once the agent is resumed the call gets the two
/// attempts left. The finding used to record a failed attempt of its own on
/// each claim, so a suspended agent spent its budget and failed the run under
/// `dispatcher-lost-after-started` after one call.
///
/// The reconciler is asked on every claim while the retry is deferred: what
/// bounds those questions is the claims, paced by the retry backoff, not the
/// budget.
#[tokio::test]
async fn a_retry_the_authority_defers_after_a_failed_call_spends_none_of_the_budget() {
    let reconciler =
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted);
    let fx = failing_reconcileable_call(3).with_reconciler(Arc::new(reconciler.clone()));
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    let spent = spent_effect_attempts(&fx).await;

    fx.set_suspended(true, "suspend-1").await;
    let mut deferred_claims = Vec::new();
    for _claim in 0..5 {
        let pass = claim_round(&fx).await;
        deferred_claims.push((
            pass.deferred,
            pass.failed_attempts,
            reconciler.queries(),
            tool_row(&fx).await,
        ));
    }
    assert_eq!(
        deferred_claims,
        vec![
            (1, 0, 1, (Failed, 1)),
            (1, 0, 2, (Failed, 1)),
            (1, 0, 3, (Failed, 1)),
            (1, 0, 4, (Failed, 1)),
            (1, 0, 5, (Failed, 1)),
        ],
        "(deferred, failed attempts, queries, row) after each claim: one question, nothing spent"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(spent_effect_attempts(&fx).await, spent);
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForEffect
    );

    fx.set_suspended(false, "resume-1").await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 2));
    claim_round(&fx).await;

    assert_eq!(
        fx.tools.invocation_count(TOOL),
        3,
        "the budget of three bought three calls"
    );
    assert_eq!(tool_row(&fx).await, (Exhausted, 3));
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed { code, attempts, .. }) => {
            assert_eq!(code, "dispatch-collaborator-failed");
            assert_eq!(attempts, Some(3));
        }
        other => panic!("the run fails on the call, not {other:?}"),
    }
}

/// A reconciler that, the first time it is asked, has a rival worker run a
/// whole pass before it answers: the asking worker's lease lapses while it
/// waits, and the rival takes the ticket over and makes the next attempt.
struct ReconcilerThatLetsARivalRunFirst {
    answers: ScriptedReconciler,
    rival: Mutex<Option<Pipeline>>,
    clock: SharedAtomicWorkflowClock,
}

impl AgentEffectReconciler for ReconcilerThatLetsARivalRunFirst {
    fn reconcile<'a>(
        &'a self,
        protocol: &'a AgentReconciliationProtocolRef,
        scope: &'a AgentRunScope,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding> {
        Box::pin(async move {
            let rival = self.rival.lock().expect("the lock is not poisoned").take();
            if let Some(mut rival) = rival {
                self.clock.advance(LEASE_MS + 1);
                rival
                    .pump_run(&run_scope())
                    .await
                    .expect("the rival's pass runs");
            }
            self.answers.reconcile(protocol, scope, effect).await
        })
    }
}

/// A claim whose ticket another worker moved while the reconciler was
/// answering makes no attempt. The call's first attempt failed. A claim asks
/// the reconciler about it, and the reconciler is slow: the claim's lease
/// lapses, and a rival worker claims the ticket, asks, and makes attempt two,
/// which fails too. When the first claim is told "not executed", the row
/// counts two attempts where it read one, so the claim ends there and writes
/// nothing, as one whose lease ran out does. The next claim makes attempt
/// three.
///
/// The claim went on with the number it had first read: a second call went
/// out as attempt two, under the same durable span identity as the rival's.
#[tokio::test]
async fn a_claim_whose_ticket_moved_while_the_reconciler_answered_makes_no_attempt() {
    let answers = ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted);
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = failing_reconcileable_call(6).with_dispatch_segments(segments.clone());
    let reconciler = Arc::new(ReconcilerThatLetsARivalRunFirst {
        answers: answers.clone(),
        rival: Mutex::new(None),
        clock: fx.wf_clock.clone(),
    });
    let fx = fx.with_reconciler(reconciler.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    claim_round(&fx).await;
    assert_eq!(tool_row(&fx).await, (Failed, 1));
    *reconciler.rival.lock().expect("the lock is not poisoned") =
        Some(fx.worker("worker-2", fx.tools.clone(), None));

    fx.expire_lease();
    fx.settle().await;
    let late = fx.pipeline().pump_run(&run_scope()).await;

    let late = late.expect("the late claim's pass ends without an error");
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
