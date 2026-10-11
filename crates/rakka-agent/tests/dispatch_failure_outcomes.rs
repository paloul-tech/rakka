//! How the dispatcher settles an attempt whose executor answered a
//! definitive refusal or an unknown outcome (issue #85, asks 7 and 14).
//!
//! A scripted executor answers every call with one error and counts the
//! calls; the run, the outbox row, and the call count are the evidence.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor, AgentEffectSpec,
    AgentFailureReason, AgentModelTurn, AgentRunEffect, AgentRunEffectStatus, AgentRunScope,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
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

/// A tool executor that answers every call with one scripted error and
/// counts the calls.
struct FailingExecutor {
    error: fn() -> AgentDispatchError,
    calls: Arc<AtomicUsize>,
}

impl AgentDispatchToolExecutor for FailingExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let error = (self.error)();
        Box::pin(async move { Err(error) })
    }
}

/// A run whose tool ticket, under `spec`, is ready to claim, behind an
/// executor that answers `error`.
fn fixture(
    spec: &AgentEffectSpec,
    error: fn() -> AgentDispatchError,
) -> (AuthorityFixture, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    )
    .with_tool_executor(Arc::new(FailingExecutor {
        error,
        calls: calls.clone(),
    }));
    (fx, calls)
}

async fn ready(fx: &AuthorityFixture) -> AgentRunEffect {
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.effect_at(1).await.expect("the tool effect is committed")
}

fn definitive() -> AgentDispatchError {
    AgentDispatchError::definitive("egress-denied", "the host refused the destination")
}

fn retryable() -> AgentDispatchError {
    AgentDispatchError::collaborator("egress-denied", "the host refused the destination")
}

#[tokio::test]
async fn a_definitive_refusal_exhausts_the_ticket_on_its_first_attempt() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, calls) = fixture(&spec, definitive);
    let effect = ready(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!((pass.invoked, pass.failed_attempts), (1, 1), "{pass:?}");
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(
        row.status(),
        OutboxStatus::Exhausted,
        "a definitive refusal spends the budget at once"
    );
    assert_eq!(row.attempts().attempts(), 1);
    let resolved = fx
        .effect_at(1)
        .await
        .expect("a failed run keeps its failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("dispatch-collaborator-failed"),
        "the pipeline code a spent retryable refusal records today"
    );
    assert_eq!(
        resolved
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("egress-denied")
    );

    fx.expire_lease();
    fx.pump().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "nothing invokes it again");
}

#[tokio::test]
async fn a_retryable_refusal_still_spends_its_budget_one_attempt_at_a_time() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, calls) = fixture(&spec, retryable);
    let effect = ready(&fx).await;

    fx.one_pass().await;

    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(row.status(), OutboxStatus::Failed, "a retry is scheduled");
    assert_eq!(row.attempts().attempts(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
