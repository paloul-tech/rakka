//! A dispatch refused before its durable `Started` is charged the attempts
//! that started, and no more (issue #196).
//!
//! The dispatcher refuses a dispatch before it writes `Started` at four
//! places: a model call whose policy is weaker than its adapter's
//! declaration, the dispatch authority's refusal, a grant that fails its
//! check, and a tool call pinned to another guardrail chain revision. Each
//! used to tell the run the attempt it was about to make, and the run
//! recorded and charged it, though nothing was invoked. Each test here reads
//! what the run recorded for the call, what it charged, and the attempts its
//! terminal reason reports.
//!
//! One refusal comes after `Started`: no credential resolver for the call's
//! binding. Its attempt did start, and it is still charged.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentCredentialBindingRef, AgentEffectSpec, AgentGuardrail, AgentGuardrailBoundary,
    AgentGuardrailChain, AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage,
    AgentGuardrailStageId, AgentModelRetryPolicy, AgentModelTurn, AgentRevisionNumber,
    AgentRunEffectKind, AgentRunEffectStatus, AgentRunStatus, AgentRunTerminalReason,
    AgentSettingsChange, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use serde_json::Value;

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("tool id should be valid")
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                tool_id(),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

/// An adapter scripted for one tool turn and one closing proposal.
fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn("charged"))
}

/// The run's record of its first effect of `kind` (status and attempts
/// made), and the effect attempts the run counts as spent.
async fn accounts(
    fx: &AuthorityFixture,
    kind: AgentRunEffectKind,
) -> (AgentRunEffectStatus, u32, u64) {
    let state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    let held = state.loop_state().expect("the loop is started");
    let effect = held
        .effects()
        .iter()
        .find(|effect| effect.kind() == kind)
        .expect("the run holds the effect");
    (
        effect.status,
        effect.attempts,
        held.budget()
            .consumption()
            .get(rakka_agent::AgentBudgetDimension::EffectAttempts),
    )
}

/// The effect attempts the run counts as spent, once the model call is in.
async fn spent_before_the_tool(fx: &AuthorityFixture) -> u64 {
    accounts(fx, AgentRunEffectKind::ModelCall).await.2
}

/// The run's terminal failure: its code, the attempts it reports made, and
/// the attempts allowed.
async fn effect_failed(fx: &AuthorityFixture) -> (String, Option<u32>, Option<u32>) {
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    match run.terminal_reason {
        Some(AgentRunTerminalReason::EffectFailed {
            code,
            attempts,
            max_attempts,
            ..
        }) => (code, attempts, max_attempts),
        other => panic!("the run fails on an effect, not {other:?}"),
    }
}

/// A model call whose policy is weaker than its adapter's declaration is
/// refused before `Started` and charged no attempt. The adapter declares its
/// calls non-idempotent with one attempt; the deployment's spec says
/// read-only with three. The run used to record and charge one attempt, and
/// its terminal reason reported one of three.
#[tokio::test]
async fn a_model_call_refused_for_a_policy_conflict_is_charged_no_attempt() {
    let adapter = DeterministicModelAdapter::new()
        .with_turn(proposing_turn("resolved"))
        .with_retry_policy(AgentModelRetryPolicy {
            safety_class: rakka_agent::AgentEffectSafetyClass::NonIdempotent,
            max_attempts: 1,
        })
        .expect("the adapter policy is valid");
    let fx = AuthorityFixture::over(
        adapter,
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        Some(
            AgentEffectSpec::read_only()
                .with_max_attempts(3)
                .expect("the spec is valid"),
        ),
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.adapter.calls(), 0, "nothing was invoked");
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ModelCall).await,
        (AgentRunEffectStatus::Failed, 0, 0),
        "no attempt recorded, none charged"
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("model-policy-conflict".to_string(), Some(0), Some(3))
    );
}

/// A call whose grant fails the dispatcher's check is refused before
/// `Started` and charged no attempt. Every grant is issued already expired;
/// the model call is the first dispatch to meet one.
#[tokio::test]
async fn a_call_whose_grant_fails_its_check_is_charged_no_attempt() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    )
    .with_expired_grants();
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.adapter.calls(), 0);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ModelCall).await,
        (AgentRunEffectStatus::Failed, 0, 0)
    );
    assert_eq!(effect_failed(&fx).await.1, Some(0));
}

/// A call the dispatch authority refuses is charged no attempt. The tool is
/// revoked by a settings update after the run commits its call.
#[tokio::test]
async fn a_call_its_authority_refuses_is_charged_no_attempt() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let spent = spent_before_the_tool(&fx).await;
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;

    fx.pump().await;

    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 0, spent)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("tool-revoked".to_string(), Some(0), Some(1))
    );
}

/// A guardrail stage that rewrites every amount above ten.
struct ClampAmount;

impl AgentGuardrail for ClampAmount {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        match content.get("amount").and_then(Value::as_u64) {
            Some(amount) if amount <= 10 => AgentGuardrailOutcome::Allow,
            _ => AgentGuardrailOutcome::Transform {
                content: serde_json::json!({ "amount": 10 }),
                reason_code: "amount-clamped".to_string(),
            },
        }
    }
}

/// A chain of one transforming stage, at `revision`.
fn transforming(revision: AgentRevisionNumber) -> AgentGuardrailChain {
    AgentGuardrailChain::new(revision)
        .with_stage(
            AgentGuardrailStage::new(
                AgentGuardrailStageId::new("amount-clamp").expect("the stage id is valid"),
                AgentRevisionNumber::INITIAL,
                Arc::new(ClampAmount),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest),
        )
        .expect("the stage registers")
}

/// A tool call pinned to another guardrail chain revision than the one the
/// dispatch gate evaluates is refused before `Started` and charged no
/// attempt.
#[tokio::test]
async fn a_call_refused_for_its_guardrail_revision_is_charged_no_attempt() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry.clone())
            .with_guardrails(transforming(AgentRevisionNumber::INITIAL)),
        None,
    )
    .with_gate_authority(
        AgentToolAuthority::new(registry)
            .with_guardrails(transforming(AgentRevisionNumber::new(2))),
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let spent = spent_before_the_tool(&fx).await;

    fx.pump().await;

    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 0, spent)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("guardrail-revision-mismatch".to_string(), Some(0), Some(1))
    );
}

/// A fixture whose run calls a tool of `spec`, every call of which fails
/// with a retryable failure, with its first attempt made and failed. Returns
/// the effect attempts the run counts as spent before the tool.
async fn a_call_whose_first_attempt_failed(spec: AgentEffectSpec) -> (AuthorityFixture, u64) {
    let mut fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &spec),
        None,
    );
    fx.tools = RecordingToolExecutor::new().with_failure(TOOL, "upstream-unavailable", "later");
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let spent = spent_before_the_tool(&fx).await;
    fx.expire_lease();
    let first = fx.one_pass().await;
    assert_eq!((first.invoked, first.failed_attempts), (1, 1));
    (fx, spent)
}

/// A call refused after one failed attempt is charged that attempt alone.
/// A read-only tool's first call fails and is counted; the tool is then
/// revoked, and its retry is refused before `Started`. One attempt started
/// and one is charged; the run used to record two.
#[tokio::test]
async fn a_call_refused_on_its_retry_is_charged_the_attempt_it_made() {
    let (fx, spent) = a_call_whose_first_attempt_failed(
        AgentEffectSpec::read_only()
            .with_max_attempts(3)
            .expect("the spec is valid"),
    )
    .await;
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;

    fx.expire_lease();
    fx.one_pass().await;

    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 1, spent + 1)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("tool-revoked".to_string(), Some(1), Some(3))
    );
}

/// An idempotent call refused on its retry parks charged the attempt that
/// may have executed. Its first attempt failed and may have reached the
/// target, so the refusal of the retry cannot say nothing was invoked: the
/// call parks for a reconciliation decision. One attempt started, and the
/// park is charged that one; it used to be charged two.
#[tokio::test]
async fn an_idempotent_call_refused_on_its_retry_parks_charged_the_attempt_it_made() {
    let (fx, spent) = a_call_whose_first_attempt_failed(
        AgentEffectSpec::idempotent(3).expect("the spec is valid"),
    )
    .await;
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;

    fx.expire_lease();
    fx.one_pass().await;

    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Indeterminate, 1, spent + 1)
    );
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    assert_eq!(effect.last_error_code.as_deref(), Some("tool-revoked"));
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForReconciliation
    );
}

/// A refusal after `Started` is charged the attempt that started. A tool
/// whose spec names a credential binding, with no credential resolver wired,
/// is refused once its attempt is marked `Dispatching`: that attempt started,
/// so it is recorded and charged.
#[tokio::test]
async fn a_call_refused_after_its_attempt_started_is_charged_that_attempt() {
    let spec = AgentEffectSpec::idempotent(2)
        .expect("the spec is valid")
        .with_credential_binding(
            AgentCredentialBindingRef::new("payments").expect("the binding ref is valid"),
        );
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &spec),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let spent = spent_before_the_tool(&fx).await;

    fx.pump().await;

    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 1, spent + 1)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("credential-resolver-missing".to_string(), Some(1), Some(2))
    );
}
