//! A dispatch refused before its durable `Started` is charged the attempts
//! that started, and no more (issue #196).
//!
//! The dispatcher refuses a dispatch before it writes `Started` at four
//! places: a model call whose policy is weaker than its adapter's
//! declaration, the dispatch authority's definitive refusal, a grant that
//! fails its check, and a tool call pinned to another guardrail chain
//! revision. Each used to tell the run the attempt it was about to make, and
//! the run recorded and charged it, though nothing was invoked. Each test
//! here reads what the run recorded for the call, what it charged, and the
//! attempts its terminal reason reports.
//!
//! One refusal comes after `Started`: no credential resolver for the call's
//! binding. Its attempt did start, and it is still charged.
//!
//! The refusals are met on each path that reaches an attempt: a fresh claim,
//! the recovery of a lost attempt, the retry after a failed attempt (with
//! and without a reconciler), and a later attempt under the same claim.
//!
//! A refusal before `Started` is told to the run only while the claim's
//! lease is still current. The last tests here give the authority an answer
//! that outlasts the lease, with another worker running the ticket
//! meanwhile and without.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{
    DeterministicModelAdapter, InProcessRunResultDelivery, RecordingToolExecutor,
    ScriptedCredentialResolver, ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentAuthorityRefusal, AgentCheckpointDecision, AgentCredentialBindingRef,
    AgentDispatchAuthority, AgentDispatchDecision, AgentDispatchError, AgentDispatchFuture,
    AgentDispatchToolExecutor, AgentDispatchWindow, AgentEffectReconciler, AgentEffectSpec,
    AgentEntityAuthority, AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain,
    AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentModelRetryPolicy, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentReconciliationDecision, AgentReconciliationFinding, AgentReconciliationProtocolRef,
    AgentRevisionNumber, AgentRunEffect, AgentRunEffectDispatcher, AgentRunEffectKind,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunScope, AgentRunState, AgentRunStatus,
    AgentRunTerminalReason, AgentSegmentOperation, AgentSettingsChange, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentSegmentSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherWorkerId, AgentEphemeralCredential,
    AgentTelemetryContext, AgentTimestampMillis, PrincipalRef,
};
use serde_json::Value;
use tokio::sync::oneshot;

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
    assert_eq!(
        effect_failed(&fx).await,
        ("grant-expired".to_string(), Some(0), Some(1))
    );
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
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &spec),
        None,
    );
    a_first_attempt_that_failed(fx).await
}

/// Makes and fails the first attempt of `fx`'s tool call, every call of
/// which fails with a retryable failure. Returns the fixture and the effect
/// attempts the run counts as spent before the tool.
async fn a_first_attempt_that_failed(mut fx: AuthorityFixture) -> (AuthorityFixture, u64) {
    fx.tools = RecordingToolExecutor::new().with_failure(TOOL, "upstream-unavailable", "later");
    fx.fx
        .instantiate_agent_with_envelope(fx.envelope.clone())
        .await;
    fx.fx.create_task_traced(traced()).await;
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

/// A task traced from a caller's span, so the run derives a span identity
/// for each of its effect attempts.
fn traced() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        ..AgentTelemetryContext::default()
    }
}

/// An idempotent tool call whose first attempt failed and whose retry was
/// refused (the tool revoked meanwhile), parked for a reconciliation
/// decision, with every segment in `segments`. Returns the fixture and the
/// effect attempts the run counts as spent before the tool.
async fn an_idempotent_call_parked_on_a_refused_retry(
    segments: Arc<InMemoryAgentSegmentSink>,
) -> (AuthorityFixture, u64) {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::idempotent(3).expect("the spec is valid"),
        ),
        None,
    )
    .with_segments(segments.clone())
    .with_dispatch_segments(segments);
    let (fx, spent) = a_first_attempt_that_failed(fx).await;
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;

    fx.expire_lease();
    fx.one_pass().await;
    (fx, spent)
}

/// An idempotent call refused on its retry parks charged the attempt that
/// may have executed. Its first attempt failed and may have reached the
/// target, so the refusal of the retry cannot say nothing was invoked: the
/// call parks for a reconciliation decision. One attempt started, and the
/// park is charged that one; it used to be charged two.
#[tokio::test]
async fn an_idempotent_call_refused_on_its_retry_parks_charged_the_attempt_it_made() {
    let (fx, spent) =
        an_idempotent_call_parked_on_a_refused_retry(Arc::new(InMemoryAgentSegmentSink::new()))
            .await;

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

/// The park after a refused retry links the attempt that failed, the one
/// that may have executed. Both segments that carry an `ambiguous-attempt`
/// link (the run's park and the checkpoint it opens) name attempt one and
/// link the span attempt one's dispatch exported. They used to name
/// attempt two, which never started and exported no span.
#[tokio::test]
async fn the_park_after_a_refused_retry_links_the_attempt_that_failed() {
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let (fx, _) = an_idempotent_call_parked_on_a_refused_retry(segments.clone()).await;

    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let first = effect
        .attempt_span_identity(1)
        .expect("the run is traced")
        .span_id;
    let exported: Vec<_> = segments
        .segments()
        .into_iter()
        .filter(|segment| {
            matches!(
                segment.operation,
                AgentSegmentOperation::EffectDispatch {
                    effect_kind: "tool-call"
                }
            ) && segment.span_id.is_some()
        })
        .map(|segment| segment.span_id)
        .collect();
    assert_eq!(
        exported,
        vec![Some(first.clone())],
        "attempt one's dispatch is the one exported span"
    );
    let links: Vec<_> = segments
        .segments()
        .iter()
        .filter_map(|segment| {
            let link = segment.telemetry.span_links.iter().find(|link| {
                link.attributes
                    .get(rakka_agent::ATTR_AGENT_TELEMETRY_LINK_KIND)
                    .map(String::as_str)
                    == Some(rakka_agent::LINK_KIND_AMBIGUOUS_ATTEMPT)
            })?;
            Some((
                segment
                    .attributes
                    .get("rakka.agent.effect.attempt")
                    .cloned(),
                link.span_id.clone(),
            ))
        })
        .collect();
    assert_eq!(
        links,
        vec![
            (Some("1".to_string()), first.clone()),
            (Some("1".to_string()), first)
        ]
    );
}

/// A run that abandons the park after a refused retry reports the attempt
/// that started. The operator's `AbandonAndFail` fails the run, and its
/// terminal reason reports one attempt of three; it used to report two.
#[tokio::test]
async fn abandoning_the_park_after_a_refused_retry_reports_the_attempt_it_made() {
    let (fx, spent) =
        an_idempotent_call_parked_on_a_refused_retry(Arc::new(InMemoryAgentSegmentSink::new()))
            .await;
    let checkpoint_id = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists")
    .loop_state()
    .expect("the loop is started")
    .open_checkpoints()
    .first()
    .expect("the park opened its checkpoint")
    .checkpoint_id
    .clone();

    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "abandon",
            )
            .expect("the operation id derives"),
            checkpoint_id,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::AbandonAndFail,
            )),
            telemetry: AgentTelemetryContext::default(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the abandonment applies");
    fx.settle().await;

    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 1, spent + 1)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("reconciliation-abandoned".to_string(), Some(1), Some(3))
    );
}

/// The missing resolver is refused after the attempt's `Started`: a worker
/// that dies right after writing it leaves the row `Dispatching`, so the
/// refusal the guard above meets comes after the attempt started.
#[tokio::test]
async fn a_missing_resolver_is_refused_after_the_attempts_started() {
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
    fx.probe.arm(AgentDispatchWindow::AfterStarted);

    fx.expire_lease();
    let pass = fx.one_pass().await;

    assert!(pass.died, "the worker dies once `Started` is written");
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's row exists");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Dispatching, 0)
    );
}

/// A refusal after a lost attempt is charged the lost attempt alone. A
/// read-only tool call's worker dies once its attempt's `Started` is
/// written; the tool is then revoked. The next claim counts the lost
/// attempt and refuses the next before `Started`: one attempt started and
/// one is charged. It used to be two.
#[tokio::test]
async fn a_call_refused_after_a_lost_attempt_is_charged_that_attempt() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::read_only()
                .with_max_attempts(3)
                .expect("the spec is valid"),
        ),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let spent = spent_before_the_tool(&fx).await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    fx.expire_lease();
    assert!(fx.one_pass().await.died);
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;

    fx.expire_lease();
    let pass = fx.one_pass().await;

    assert_eq!(pass.failed_attempts, 1, "the lost attempt is counted");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 1, spent + 1)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("tool-revoked".to_string(), Some(1), Some(3))
    );
}

/// A model call refused for a policy conflict after a lost attempt is
/// charged the lost attempt alone. The adapter's declaration permits the
/// spec at first; the worker dies once the call's `Started` is written, and
/// the adapter's declaration tightens before the next claim. That claim
/// counts the lost attempt and refuses the next before `Started`. It used to
/// be charged two.
#[tokio::test]
async fn a_model_call_refused_for_a_policy_conflict_after_a_lost_attempt_is_charged_that_attempt() {
    let declared = |safety_class, max_attempts| {
        DeterministicModelAdapter::new()
            .with_turn(proposing_turn("resolved"))
            .with_retry_policy(AgentModelRetryPolicy {
                safety_class,
                max_attempts,
            })
            .expect("the adapter policy is valid")
    };
    let mut fx = AuthorityFixture::over(
        declared(rakka_agent::AgentEffectSafetyClass::ReadOnly, 3),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        Some(
            AgentEffectSpec::read_only()
                .with_max_attempts(3)
                .expect("the spec is valid"),
        ),
    );
    fx.start().await;
    fx.settle().await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    assert!(fx.one_pass().await.died);
    fx.model_adapter = Some(Arc::new(declared(
        rakka_agent::AgentEffectSafetyClass::NonIdempotent,
        1,
    )));

    fx.expire_lease();
    fx.one_pass().await;

    assert_eq!(fx.adapter.calls(), 0, "nothing was invoked");
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ModelCall).await,
        (AgentRunEffectStatus::Failed, 1, 1)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("model-policy-conflict".to_string(), Some(1), Some(3))
    );
}

/// A reconcileable tool's spec: retried only once absence is proven.
fn reconcileable(attempts: u32) -> AgentEffectSpec {
    AgentEffectSpec::reconcileable(
        AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid"),
        attempts,
    )
    .expect("the spec is valid")
}

/// A reconcileable call refused on its retry is charged the attempt it
/// made. Its first call fails; the tool is then revoked. The next claim
/// asks the reconciler, which finds the call not executed, and the retry is
/// refused before `Started` (issue #195's path). It used to be charged two.
#[tokio::test]
async fn a_reconcileable_call_refused_on_its_retry_is_charged_the_attempt_it_made() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &reconcileable(3)),
        None,
    )
    .with_reconciler(Arc::new(
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
    ));
    let (fx, spent) = a_first_attempt_that_failed(fx).await;
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

/// An authority that answers its first `grant` questions as the fixture's
/// own authority would, and refuses `tool-revoked` from then on. It records
/// the attempt each question names.
struct RefusingAfter {
    inner: Arc<dyn AgentDispatchAuthority>,
    grant: usize,
    asked: Mutex<Vec<u32>>,
}

impl AgentDispatchAuthority for RefusingAfter {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            let asked = {
                let mut asked = self.asked.lock().expect("the lock holds");
                asked.push(attempt);
                asked.len()
            };
            if asked > self.grant {
                return Ok(AgentDispatchDecision::Refused(AgentAuthorityRefusal::of(
                    "tool-revoked",
                    "the tool was revoked",
                )));
            }
            self.inner.authorize(scope, run, intent, attempt, now).await
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

/// A tool whose every call ends with an unknown outcome.
#[derive(Clone, Default)]
struct UnknownTool {
    calls: Arc<AtomicUsize>,
}

impl AgentDispatchToolExecutor for UnknownTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(AgentDispatchError::outcome_unknown(
                "upstream-timeout",
                "the answer was lost",
            ))
        })
    }
}

/// A later attempt refused under the same claim is charged the attempt
/// before it. A reconcileable call's first attempt ends with an unknown
/// outcome; the reconciler finds it not executed, the attempt is counted,
/// and the claim goes on to the next attempt (issues #165 and #171), which
/// the authority refuses before `Started`. It used to be charged two.
#[tokio::test]
async fn a_later_attempt_refused_under_the_same_claim_is_charged_the_attempt_before_it() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &reconcileable(3)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let spent = spent_before_the_tool(&fx).await;
    let authority = Arc::new(RefusingAfter {
        inner: Arc::new(AgentEntityAuthority::new(
            fx.fx.agents.clone(),
            fx.authority.clone(),
        )),
        grant: 1,
        asked: Mutex::new(Vec::new()),
    });
    let tool = UnknownTool::default();
    let mut worker = worker(&fx, "worker-1", authority.clone(), Arc::new(tool.clone()))
        .with_reconciler(Arc::new(
            ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
        ));

    fx.expire_lease();
    fx.settle().await;
    let pass = worker.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!((pass.invoked, pass.failed_attempts), (1, 1));
    assert_eq!(
        authority.asked.lock().expect("the lock holds").clone(),
        vec![1, 2],
        "one claim asks about both attempts"
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        accounts(&fx, AgentRunEffectKind::ToolCall).await,
        (AgentRunEffectStatus::Failed, 1, spent + 1)
    );
    assert_eq!(
        effect_failed(&fx).await,
        ("tool-revoked".to_string(), Some(1), Some(3))
    );
}

/// A dispatcher worker over `fx`'s stores, asking `authority` and calling
/// `tools`, with the fixture's lease.
fn worker(
    fx: &AuthorityFixture,
    worker_id: &str,
    authority: Arc<dyn AgentDispatchAuthority>,
    tools: Arc<dyn AgentDispatchToolExecutor>,
) -> Pipeline {
    let delivery = InProcessRunResultDelivery::new(
        fx.fx.runs.clone(),
        fx.fx.effects.clone(),
        fx.fx.router.clone(),
        fx.fx.clock.clone(),
    )
    .with_effect_policies(fx.fx.policies.clone());
    AgentRunEffectDispatcher::new(
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
}

/// The fixture's own dispatch authority.
fn the_fixtures_authority(fx: &AuthorityFixture) -> Arc<dyn AgentDispatchAuthority> {
    Arc::new(AgentEntityAuthority::new(
        fx.fx.agents.clone(),
        fx.authority.clone(),
    ))
}

/// An authority whose first answer outlasts the claim's lease. It moves the
/// clock past the lease and, when given a hand-off, signals it and waits to
/// be released; then it refuses, `tool-revoked` unless told another refusal
/// ([`Self::refusing`]). Every later question it answers as the fixture's
/// own authority would.
struct OutlastingAuthority {
    inner: Arc<dyn AgentDispatchAuthority>,
    clock: SharedAtomicWorkflowClock,
    answered: AtomicBool,
    hand_off: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    refusal: AgentAuthorityRefusal,
}

impl OutlastingAuthority {
    fn new(fx: &AuthorityFixture) -> Self {
        Self {
            inner: the_fixtures_authority(fx),
            clock: fx.wf_clock.clone(),
            answered: AtomicBool::new(false),
            hand_off: Mutex::new(None),
            refusal: AgentAuthorityRefusal::of("tool-revoked", "the tool was revoked"),
        }
    }

    /// Refuses with `refusal` instead of `tool-revoked`.
    fn refusing(mut self, refusal: AgentAuthorityRefusal) -> Self {
        self.refusal = refusal;
        self
    }

    /// Signals `held` once the lease has lapsed, and refuses once `release`
    /// fires.
    fn handing_off(self, held: oneshot::Sender<()>, release: oneshot::Receiver<()>) -> Self {
        *self.hand_off.lock().expect("the lock holds") = Some((held, release));
        self
    }
}

impl AgentDispatchAuthority for OutlastingAuthority {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            if self.answered.swap(true, Ordering::SeqCst) {
                return self.inner.authorize(scope, run, intent, attempt, now).await;
            }
            self.clock.advance(LEASE_MS + 1);
            let hand_off = self.hand_off.lock().expect("the lock holds").take();
            if let Some((held, release)) = hand_off {
                let _ = held.send(());
                let _ = release.await;
            }
            Ok(AgentDispatchDecision::Refused(self.refusal.clone()))
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

/// A tool whose first call signals `entered` and waits for `release`, and
/// whose every call succeeds.
struct HeldTool {
    calls: AtomicUsize,
    gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl AgentDispatchToolExecutor for HeldTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate.lock().expect("the lock holds").take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
            Ok(
                AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                    .expect("the result is inline-bounded"),
            )
        })
    }
}

/// A refusal whose lease lapsed while another worker's call was out is not
/// delivered. Worker one's authority answers after the lease lapsed; worker
/// two claims the ticket meanwhile and its call of a non-idempotent tool is
/// out when worker one's refusal comes. Worker one tells the run nothing and
/// cancels nothing; worker two's call succeeds and the run completes on it.
/// The refusal used to fail the run `tool-revoked` and cancel the row, and
/// the call that had executed was lost.
#[tokio::test]
async fn a_refusal_whose_lease_lapsed_while_another_worker_called_the_tool_is_not_delivered() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let (held, lapsed) = oneshot::channel();
    let (release_one, refuse) = oneshot::channel();
    let (entered, out) = oneshot::channel();
    let (release_two, answer) = oneshot::channel();
    let tool = Arc::new(HeldTool {
        calls: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered, answer))),
    });
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(OutlastingAuthority::new(&fx).handing_off(held, refuse)),
        tool.clone(),
    );
    let mut two = worker(&fx, "worker-two", the_fixtures_authority(&fx), tool.clone());
    let (one_done, one_ended) = oneshot::channel::<()>();

    fx.expire_lease();
    fx.settle().await;
    let (one_pass, two_pass, ()) = tokio::join!(
        async {
            let pass = one.pump_run(&run_scope()).await;
            let _ = one_done.send(());
            pass
        },
        async {
            let _ = lapsed.await;
            two.pump_run(&run_scope()).await
        },
        async {
            let _ = out.await;
            let _ = release_one.send(());
            let _ = one_ended.await;
            let _ = release_two.send(());
        }
    );

    let one_pass = one_pass.expect("worker one's pass runs");
    assert_eq!(
        (one_pass.lapsed, one_pass.delivered, one_pass.cancelled),
        (1, 0, 0),
        "worker one ends out of lease and writes nothing"
    );
    let two_pass = two_pass.expect("worker two's pass runs");
    assert_eq!((two_pass.invoked, two_pass.delivered), (1, 1));
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

/// A refusal whose lease lapsed after another worker failed the attempt is
/// not delivered. A read-only tool's first call fails. Worker one's
/// authority answers about the retry after the lease lapsed; worker two
/// claims the ticket meanwhile, and its retry fails too. Worker one tells
/// the run nothing, and the row keeps both failures for the next claim. The
/// refusal used to fail the run, which this fix would have charged one of
/// the two attempts that started.
#[tokio::test]
async fn a_refusal_whose_lease_lapsed_after_another_worker_failed_the_attempt_is_not_delivered() {
    let (fx, _) = a_call_whose_first_attempt_failed(
        AgentEffectSpec::read_only()
            .with_max_attempts(3)
            .expect("the spec is valid"),
    )
    .await;
    let (held, lapsed) = oneshot::channel();
    let (release_one, refuse) = oneshot::channel();
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(OutlastingAuthority::new(&fx).handing_off(held, refuse)),
        Arc::new(fx.tools.clone()),
    );
    let mut two = worker(
        &fx,
        "worker-two",
        the_fixtures_authority(&fx),
        Arc::new(fx.tools.clone()),
    );

    fx.expire_lease();
    fx.settle().await;
    let scope = run_scope();
    let (one_pass, two_pass) = tokio::join!(one.pump_run(&scope), async {
        let _ = lapsed.await;
        let pass = two.pump_run(&run_scope()).await;
        let _ = release_one.send(());
        pass
    });

    let one_pass = one_pass.expect("worker one's pass runs");
    assert_eq!(
        (one_pass.lapsed, one_pass.delivered, one_pass.cancelled),
        (1, 0, 0)
    );
    let two_pass = two_pass.expect("worker two's pass runs");
    assert_eq!((two_pass.invoked, two_pass.failed_attempts), (1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 2);
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's row exists");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Failed, 2)
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.terminal_reason, None, "the run is not failed");
}

/// A late refusal leaves the call to the claim that follows. The
/// authority's first answer, a refusal, comes after the lease lapsed, and
/// no other worker claims the ticket: the claim ends out of lease and tells
/// the run nothing. The tool is then revoked, and the next claim's
/// authority refuses it at once, under a current lease: that refusal is
/// delivered.
#[tokio::test]
async fn a_late_refusal_leaves_the_call_to_the_claim_that_follows() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(OutlastingAuthority::new(&fx)),
        Arc::new(fx.tools.clone()),
    );
    fx.expire_lease();
    fx.settle().await;

    let first = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!((first.lapsed, first.delivered, first.cancelled), (1, 0, 0));
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.terminal_reason, None);

    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;
    fx.expire_lease();
    let second = fx.one_pass().await;

    assert_eq!((second.delivered, second.cancelled), (1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        effect_failed(&fx).await,
        ("tool-revoked".to_string(), Some(0), Some(1))
    );
}

/// An idempotent retry refused after the lease lapsed parks nothing. Its
/// first call failed and may have reached the target, so a refusal of the
/// retry under a current lease parks the call for a reconciliation
/// decision. Made once the lease has lapsed, the claim ends out of lease
/// and parks nothing.
#[tokio::test]
async fn an_idempotent_retry_refused_after_the_lease_lapsed_parks_nothing() {
    let (fx, _) = a_call_whose_first_attempt_failed(
        AgentEffectSpec::idempotent(3).expect("the spec is valid"),
    )
    .await;
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(OutlastingAuthority::new(&fx)),
        Arc::new(fx.tools.clone()),
    );
    fx.expire_lease();
    fx.settle().await;

    let pass = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!(
        (pass.lapsed, pass.parked_indeterminate, pass.delivered),
        (1, 0, 0)
    );
    assert_ne!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForReconciliation
    );
}

/// A policy conflict met after the lease lapsed is not delivered. The
/// claim's first read of the run's workflow record moves the clock past the
/// lease, and the policy check that follows refuses the model call: the
/// claim ends out of lease and the run is not told.
#[tokio::test]
async fn a_policy_conflict_met_after_the_lease_lapsed_is_not_delivered() {
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
    fx.settle().await;
    let mut pipeline = fx.pipeline();
    pipeline
        .register_run(&run_scope())
        .await
        .expect("the registration runs");
    let clock = fx.wf_clock.clone();
    fx.workflow_store.before_next_load(Box::new(move || {
        Box::pin(async move { clock.advance(LEASE_MS + 1) })
    }));

    let pass = pipeline
        .claim_and_execute()
        .await
        .expect("a claim out of lease does not fail the pass");

    assert_eq!((pass.claimed, pass.lapsed, pass.delivered), (1, 1, 0));
    assert_eq!(fx.adapter.calls(), 0);
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.terminal_reason, None, "the run is not told");
}

/// An authority that answers its first question as the fixture's own
/// authority would, and refuses every later one `tool-revoked`, moving the
/// clock `then_advance` ms first.
struct GrantThenRefuse {
    inner: Arc<dyn AgentDispatchAuthority>,
    clock: SharedAtomicWorkflowClock,
    asked: AtomicUsize,
    then_advance: u64,
}

impl AgentDispatchAuthority for GrantThenRefuse {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            if self.asked.fetch_add(1, Ordering::SeqCst) == 0 {
                return self.inner.authorize(scope, run, intent, attempt, now).await;
            }
            self.clock.advance(self.then_advance);
            Ok(AgentDispatchDecision::Refused(AgentAuthorityRefusal::of(
                "tool-revoked",
                "the tool was revoked",
            )))
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

/// A tool whose every call moves the clock `advance` ms and succeeds.
struct SlowTool {
    clock: SharedAtomicWorkflowClock,
    advance: u64,
}

impl AgentDispatchToolExecutor for SlowTool {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(async move {
            self.clock.advance(self.advance);
            Ok(
                AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                    .expect("the result is inline-bounded"),
            )
        })
    }
}

/// A refusal inside a renewed lease is delivered: the check asks the
/// claim's lease as the batch's settles renewed it, not the expiry the
/// claim was handed. The model asks for two tool calls, both claimed in one
/// batch. The first call takes most of the lease, and its settle renews the
/// second claim. The second call's refusal comes after the batch's first
/// expiry and inside its renewed lease.
#[tokio::test]
async fn a_refusal_inside_a_renewed_lease_is_delivered() {
    let call = |id: &str, amount: u64| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("call id should be valid"),
            tool_id(),
            serde_json::json!({ "amount": amount }),
        )
        .expect("the tool call is bounded")
    };
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Two of them.")
                .with_tool_call(call("call-1", 1))
                .with_tool_call(call("call-2", 2)),
        )
        .with_turn_for(2, proposing_turn("charged"));
    let fx = AuthorityFixture::over(
        adapter,
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(GrantThenRefuse {
            inner: the_fixtures_authority(&fx),
            clock: fx.wf_clock.clone(),
            asked: AtomicUsize::new(0),
            then_advance: 10,
        }),
        Arc::new(SlowTool {
            clock: fx.wf_clock.clone(),
            advance: LEASE_MS - 5,
        }),
    );
    fx.expire_lease();
    fx.settle().await;

    let pass = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!(pass.claimed, 2, "both calls are claimed in one batch");
    assert_eq!((pass.invoked, pass.lapsed), (1, 0));
    assert_eq!(effect_failed(&fx).await.0, "tool-revoked");
}

/// An authority that answers as the fixture's own authority would, and
/// whose `hold_on`th answer (counting from one) comes after the lease: it
/// moves the clock past the lease, signals `held` and waits for `release`
/// before giving it.
struct LateAnswer {
    inner: Arc<dyn AgentDispatchAuthority>,
    clock: SharedAtomicWorkflowClock,
    hold_on: usize,
    asked: AtomicUsize,
    hand_off: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl AgentDispatchAuthority for LateAnswer {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            let decision = self.inner.authorize(scope, run, intent, attempt, now).await;
            if self.asked.fetch_add(1, Ordering::SeqCst) + 1 == self.hold_on {
                self.clock.advance(LEASE_MS + 1);
                let hand_off = self.hand_off.lock().expect("the lock holds").take();
                if let Some((held, release)) = hand_off {
                    let _ = held.send(());
                    let _ = release.await;
                }
            }
            decision
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

/// A reconciler that answers "not executed", signalling `asked` and waiting
/// for `answer` first.
struct HandingOffReconciler {
    answers: ScriptedReconciler,
    hand_off: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

impl AgentEffectReconciler for HandingOffReconciler {
    fn reconcile<'a>(
        &'a self,
        protocol: &'a AgentReconciliationProtocolRef,
        scope: &'a AgentRunScope,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding> {
        Box::pin(async move {
            let hand_off = self.hand_off.lock().expect("the lock holds").take();
            if let Some((asked, answer)) = hand_off {
                let _ = asked.send(());
                let _ = answer.await;
            }
            self.answers.reconcile(protocol, scope, effect).await
        })
    }
}

/// A late grant ends the claim before a missing resolver is met (issue
/// #207). `credential-resolver-missing` is refused after `Started`, so the
/// claim checks its lease before then, right after the authority's answer.
///
/// A reconcileable call with a credential binding fails its first attempt.
/// Worker one, which has no resolver wired, is told "not executed", and the
/// authority's grant for its retry comes after the lease. Worker two claims
/// the ticket and asks its reconciler. Worker one's late grant ends its claim
/// out of lease: it writes no `Started`, meets no missing resolver, and tells
/// the run nothing. Worker two, told "not executed", finds the row as it read
/// it and makes the retry, which fails as every call of the tool does. The
/// late grant used to go on to `Started` and the missing resolver, failing
/// the run and cancelling the row beside worker two's claim.
#[tokio::test]
async fn a_late_grant_ends_the_claim_before_a_missing_resolver_is_met() {
    let spec = reconcileable(3).with_credential_binding(
        AgentCredentialBindingRef::new("payments").expect("the binding ref is valid"),
    );
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &spec),
        None,
    )
    .with_credential_resolver("token")
    .with_reconciler(Arc::new(
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
    ));
    let (fx, _) = a_first_attempt_that_failed(fx).await;
    let (held, lapsed) = oneshot::channel();
    let (release_one, granted) = oneshot::channel();
    // Worker one's first question is the one asked before the reconciler
    // (issue #195); its second is its retry's.
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(LateAnswer {
            inner: the_fixtures_authority(&fx),
            clock: fx.wf_clock.clone(),
            hold_on: 2,
            asked: AtomicUsize::new(0),
            hand_off: Mutex::new(Some((held, granted))),
        }),
        Arc::new(fx.tools.clone()),
    )
    .with_reconciler(Arc::new(
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
    ));
    let (asked, two_asked) = oneshot::channel();
    let (release_two, answer) = oneshot::channel();
    let mut two = worker(
        &fx,
        "worker-two",
        the_fixtures_authority(&fx),
        Arc::new(fx.tools.clone()),
    )
    .with_credential_resolver(Arc::new(ScriptedCredentialResolver::new("token")))
    .with_reconciler(Arc::new(HandingOffReconciler {
        answers: ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
        hand_off: Mutex::new(Some((asked, answer))),
    }));
    let (one_done, one_ended) = oneshot::channel::<()>();

    fx.expire_lease();
    fx.settle().await;
    let (one_pass, two_pass, ()) = tokio::join!(
        async {
            let pass = one.pump_run(&run_scope()).await;
            let _ = one_done.send(());
            pass
        },
        async {
            let _ = lapsed.await;
            two.pump_run(&run_scope()).await
        },
        async {
            let _ = two_asked.await;
            let _ = release_one.send(());
            let _ = one_ended.await;
            let _ = release_two.send(());
        }
    );

    let one_pass = one_pass.expect("worker one's pass runs");
    assert_eq!(
        (one_pass.lapsed, one_pass.delivered, one_pass.cancelled),
        (1, 0, 0),
        "worker one ends out of lease and writes nothing"
    );
    let two_pass = two_pass.expect("worker two's pass runs");
    assert_eq!(
        (
            two_pass.invoked,
            two_pass.failed_attempts,
            two_pass.parked_indeterminate
        ),
        (1, 1, 0),
        "worker two makes the retry"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 2);
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's row exists");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Failed, 2)
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.terminal_reason, None, "the run is not failed");
}

// ---------------------------------------------------------------------------
// Every late answer (issue #207). A claim asks whether its lease is still
// current right after the dispatch authority answers, whatever the answer,
// and ends out of lease, writing nothing more, when it is not.
// ---------------------------------------------------------------------------

/// The fixture's own authority, answering its `hold_on`th question (counting
/// from one) after the claim's lease has lapsed.
fn late_answer(fx: &AuthorityFixture, hold_on: usize) -> LateAnswer {
    LateAnswer {
        inner: the_fixtures_authority(fx),
        clock: fx.wf_clock.clone(),
        hold_on,
        asked: AtomicUsize::new(0),
        hand_off: Mutex::new(None),
    }
}

/// A grant that comes after the lease makes no attempt beside another
/// worker's call. Worker one's authority grants the call after the lease
/// lapsed; worker two claims the ticket meanwhile, and its call of a
/// non-idempotent tool is out when worker one's grant comes. Worker one ends
/// out of lease and calls nothing; worker two's call succeeds and the run
/// completes on it. The late grant used to write `Started` over worker two's
/// row and call the tool a second time.
#[tokio::test]
async fn a_late_grant_makes_no_attempt_beside_another_workers_call() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let (held, lapsed) = oneshot::channel();
    let (release_one, grant) = oneshot::channel();
    let (entered, out) = oneshot::channel();
    let (release_two, answer) = oneshot::channel();
    let tool = Arc::new(HeldTool {
        calls: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered, answer))),
    });
    let late = late_answer(&fx, 1);
    *late.hand_off.lock().expect("the lock holds") = Some((held, grant));
    let mut one = worker(&fx, "worker-one", Arc::new(late), tool.clone());
    let mut two = worker(&fx, "worker-two", the_fixtures_authority(&fx), tool.clone());
    let (one_done, one_ended) = oneshot::channel::<()>();

    fx.expire_lease();
    fx.settle().await;
    let (one_pass, two_pass, ()) = tokio::join!(
        async {
            let pass = one.pump_run(&run_scope()).await;
            let _ = one_done.send(());
            pass
        },
        async {
            let _ = lapsed.await;
            two.pump_run(&run_scope()).await
        },
        async {
            let _ = out.await;
            let _ = release_one.send(());
            let _ = one_ended.await;
            let _ = release_two.send(());
        }
    );

    let one_pass = one_pass.expect("worker one's pass runs");
    assert_eq!(
        (
            one_pass.lapsed,
            one_pass.invoked,
            one_pass.delivered,
            one_pass.cancelled
        ),
        (1, 0, 0, 0),
        "worker one ends out of lease and writes nothing"
    );
    let two_pass = two_pass.expect("worker two's pass runs");
    assert_eq!((two_pass.invoked, two_pass.delivered), (1, 1));
    assert_eq!(
        tool.calls.load(Ordering::SeqCst),
        1,
        "the tool is called once"
    );
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

/// A late grant with no other worker about leaves the attempt to the claim
/// that follows: the claim ends out of lease and calls nothing, and the next
/// claim's grant, under a current lease, makes the attempt.
#[tokio::test]
async fn a_late_grant_alone_leaves_the_attempt_to_the_claim_that_follows() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(late_answer(&fx, 1)),
        Arc::new(fx.tools.clone()),
    );
    fx.expire_lease();
    fx.settle().await;

    let first = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!((first.lapsed, first.invoked), (1, 0));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    fx.expire_lease();
    let second = fx.one_pass().await;
    assert_eq!(second.invoked, 1);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

/// A transient refusal that comes after the lease ends the claim out of
/// lease, as every late answer does. It used to end the pass in a
/// `claim-fenced` error, with no report: the deferral's fleet write was
/// fenced.
#[tokio::test]
async fn a_late_transient_refusal_ends_the_claim_out_of_lease_not_in_an_error() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent()),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(
            OutlastingAuthority::new(&fx).refusing(AgentAuthorityRefusal::transient(
                "agent-suspended",
                "the agent is suspended",
            )),
        ),
        Arc::new(fx.tools.clone()),
    );
    fx.expire_lease();
    fx.settle().await;

    let pass = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!((pass.lapsed, pass.deferred, pass.delivered), (1, 0, 0));
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// A late answer to the question asked before the reconciler (issue #195)
/// ends the claim before the reconciler is asked. A reconcileable call's
/// first attempt failed; the claim that follows asks the authority about the
/// retry first, and the answer comes after the lease. The reconciler used to
/// be asked anyway, and the claim then ended out of lease at the check after
/// its answer.
#[tokio::test]
async fn a_late_answer_before_the_reconciler_ends_the_claim_unasked() {
    let (fx, _) = a_call_whose_first_attempt_failed(reconcileable(3)).await;
    let reconciler =
        Arc::new(ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted));
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(late_answer(&fx, 1)),
        Arc::new(fx.tools.clone()),
    )
    .with_reconciler(reconciler.clone());
    fx.expire_lease();
    fx.settle().await;

    let pass = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!(pass.lapsed, 1);
    assert_eq!(reconciler.queries(), 0, "the reconciler is not asked");
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "only the first attempt ran"
    );
}

/// A late grant on the retry of a counted lost attempt leaves the retry to
/// the claim that follows. An idempotent call's first attempt is lost after
/// `Started`. The next claim counts the loss and asks about the retry, and
/// the grant comes after the lease: the loss stays counted and no retry is
/// made. The claim after that makes the retry, once.
#[tokio::test]
async fn a_late_grant_on_the_retry_of_a_counted_lost_attempt_leaves_it_to_the_next_claim() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::idempotent(3).expect("the spec is valid"),
        ),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    fx.expire_lease();
    assert!(fx.one_pass().await.died, "the first attempt is lost");
    let mut one = worker(
        &fx,
        "worker-one",
        Arc::new(late_answer(&fx, 1)),
        Arc::new(fx.tools.clone()),
    );
    fx.expire_lease();
    fx.settle().await;

    let pass = one.pump_run(&run_scope()).await.expect("the pass runs");

    assert_eq!(
        (pass.lapsed, pass.failed_attempts, pass.invoked),
        (1, 1, 0),
        "the loss is counted, and no retry is made"
    );
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's row exists");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Failed, 1)
    );
    fx.expire_lease();
    let next = fx.one_pass().await;
    assert_eq!(next.invoked, 1);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Every late finding (issue #212). A claim asks whether its lease is still
// current right after the reconciler answers, whatever the finding, and
// ends out of lease, writing nothing more, when it is not.
// ---------------------------------------------------------------------------

/// A hand-off between a held collaborator and the test: the collaborator
/// signals the sender, then waits on the receiver.
type HandOff = (oneshot::Sender<()>, oneshot::Receiver<()>);

/// A reconciler whose first answer comes after the claim's lease: it moves
/// the clock past the lease and, when a hand-off is armed, signals `held`
/// and waits for `release`. Every answer is `answers`'.
struct LateReconciler {
    answers: ScriptedReconciler,
    clock: SharedAtomicWorkflowClock,
    late: AtomicBool,
    hand_off: Mutex<Option<HandOff>>,
}

/// A [`LateReconciler`] answering as `answers` does, with no hand-off armed.
fn late_reconciler(fx: &AuthorityFixture, answers: &ScriptedReconciler) -> LateReconciler {
    LateReconciler {
        answers: answers.clone(),
        clock: fx.wf_clock.clone(),
        late: AtomicBool::new(true),
        hand_off: Mutex::new(None),
    }
}

impl AgentEffectReconciler for LateReconciler {
    fn reconcile<'a>(
        &'a self,
        protocol: &'a AgentReconciliationProtocolRef,
        scope: &'a AgentRunScope,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding> {
        Box::pin(async move {
            if self.late.swap(false, Ordering::SeqCst) {
                self.clock.advance(LEASE_MS + 1);
                let hand_off = self.hand_off.lock().expect("the lock holds").take();
                if let Some((held, release)) = hand_off {
                    let _ = held.send(());
                    let _ = release.await;
                }
            }
            self.answers.reconcile(protocol, scope, effect).await
        })
    }
}

/// An authority whose first answer is an error that comes after the
/// claim's lease: it moves the clock past the lease, signals `held`, waits
/// for `release`, then fails as an unavailable authority does. Every later
/// question is `inner`'s.
struct LateErrorAuthority {
    inner: Arc<dyn AgentDispatchAuthority>,
    clock: SharedAtomicWorkflowClock,
    hand_off: Mutex<Option<HandOff>>,
}

impl AgentDispatchAuthority for LateErrorAuthority {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            let hand_off = self.hand_off.lock().expect("the lock holds").take();
            if let Some((held, release)) = hand_off {
                self.clock.advance(LEASE_MS + 1);
                let _ = held.send(());
                let _ = release.await;
                return Err(AgentDispatchError::collaborator(
                    "authority-unavailable",
                    "the authority timed out",
                ));
            }
            self.inner.authorize(scope, run, intent, attempt, now).await
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

/// What [`worker_two_retries_while_worker_one_waits_for_an_answer`] leaves:
/// each worker's pass, the calls of worker two's tool, the fixture and the
/// tool call's effect as it was before the passes.
struct LateRace {
    one: rakka_agent::AgentDispatchResult<rakka_agent::AgentDispatchPass>,
    two: rakka_agent::AgentDispatchResult<rakka_agent::AgentDispatchPass>,
    calls: usize,
    fx: AuthorityFixture,
    effect: AgentRunEffect,
}

/// A reconcileable call with a budget of two fails its first attempt. Worker
/// one, built by `worker_one` with the race's hand-off, claims the ticket and
/// is held past its lease at the hand-off. Worker two claims the ticket
/// meanwhile, is told "not executed", and its retry is out when worker one
/// goes on; the retry succeeds once worker one's pass is over.
async fn worker_two_retries_while_worker_one_waits_for_an_answer(
    worker_one: impl FnOnce(&AuthorityFixture, HandOff) -> Pipeline,
) -> LateRace {
    let (fx, _) = a_call_whose_first_attempt_failed(reconcileable(2)).await;
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let (held, lapsed) = oneshot::channel();
    let (release_one, answered) = oneshot::channel();
    let (entered, out) = oneshot::channel();
    let (release_two, answer) = oneshot::channel();
    let tool = Arc::new(HeldTool {
        calls: AtomicUsize::new(0),
        gate: Mutex::new(Some((entered, answer))),
    });
    let mut one = worker_one(&fx, (held, answered));
    let mut two = worker(&fx, "worker-two", the_fixtures_authority(&fx), tool.clone())
        .with_reconciler(Arc::new(
            ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted),
        ));
    let (one_done, one_ended) = oneshot::channel::<()>();

    fx.expire_lease();
    fx.settle().await;
    let (one, two, ()) = tokio::join!(
        async {
            let pass = one.pump_run(&run_scope()).await;
            let _ = one_done.send(());
            pass
        },
        async {
            let _ = lapsed.await;
            two.pump_run(&run_scope()).await
        },
        async {
            let _ = out.await;
            let _ = release_one.send(());
            let _ = one_ended.await;
            let _ = release_two.send(());
        }
    );

    LateRace {
        one,
        two,
        calls: tool.calls.load(Ordering::SeqCst),
        fx,
        effect,
    }
}

/// Worker one ended out of lease and wrote nothing, and the run completed
/// on worker two's call.
async fn the_run_completes_on_worker_twos_call(race: LateRace) {
    let one = race.one.expect("worker one's pass runs");
    assert_eq!(
        (
            one.lapsed,
            one.failed_attempts,
            one.parked_indeterminate,
            one.delivered
        ),
        (1, 0, 0, 0),
        "worker one ends out of lease and writes nothing"
    );
    let two = race.two.expect("worker two's pass runs");
    assert_eq!((two.invoked, two.delivered), (1, 1));
    assert_eq!(race.calls, 1, "worker two's retry is the one call made");
    let row = race
        .fx
        .outbox_row(&race.effect)
        .await
        .expect("the ticket's row exists");
    assert_eq!(
        row.status(),
        OutboxStatus::Dispatched,
        "worker two finished the row"
    );
    race.fx.pump().await;
    assert_eq!(
        race.fx
            .fx
            .run_snapshot()
            .await
            .expect("the run exists")
            .status,
        AgentRunStatus::Completed,
        "the run completes on worker two's call"
    );
}

/// A late `Unknown` is not acted on beside another worker's call (the
/// review's probe A). Worker one's reconciler answers `Unknown` after its
/// lease lapsed, while worker two's retry is out. Worker one ends out of
/// lease, and the run completes on worker two's call. The late `Unknown`
/// used to record a failure over worker two's call, spending the budget,
/// and park the call: the run waited for a decision about a call that had
/// run, and its result was lost.
#[tokio::test]
async fn a_late_unknown_finding_is_not_acted_on_beside_another_workers_call() {
    let race = worker_two_retries_while_worker_one_waits_for_an_answer(|fx, hand_off| {
        let late = late_reconciler(
            fx,
            &ScriptedReconciler::new().with_finding(AgentReconciliationFinding::Unknown),
        );
        *late.hand_off.lock().expect("the lock holds") = Some(hand_off);
        worker(
            fx,
            "worker-one",
            the_fixtures_authority(fx),
            Arc::new(fx.tools.clone()),
        )
        .with_reconciler(Arc::new(late))
    })
    .await;

    the_run_completes_on_worker_twos_call(race).await;
}

/// An `Unknown` met after a late authority error is not acted on beside
/// another worker's call (the review's probe B). Worker one asks the
/// authority before the reconciler (issue #195), and the authority's error
/// comes after the lease. An error keeps its handling, so worker one goes
/// on to the reconciler, which answers `Unknown` at once: asked once, and
/// its answer is not acted on, as the lease has lapsed. The run completes
/// on worker two's call.
#[tokio::test]
async fn an_unknown_finding_after_a_late_authority_error_is_not_acted_on_beside_another_workers_call(
) {
    let asked =
        Arc::new(ScriptedReconciler::new().with_finding(AgentReconciliationFinding::Unknown));
    let reconciler = asked.clone();
    let race = worker_two_retries_while_worker_one_waits_for_an_answer(move |fx, hand_off| {
        worker(
            fx,
            "worker-one",
            Arc::new(LateErrorAuthority {
                inner: the_fixtures_authority(fx),
                clock: fx.wf_clock.clone(),
                hand_off: Mutex::new(Some(hand_off)),
            }),
            Arc::new(fx.tools.clone()),
        )
        .with_reconciler(reconciler)
    })
    .await;

    assert_eq!(
        asked.queries(),
        1,
        "the reconciler is asked once, on the lapsed lease"
    );
    the_run_completes_on_worker_twos_call(race).await;
}

/// A claim's own attempt that ends unknown and is answered `Unknown` after
/// its lease is counted once, by the claim that follows. Worker one's call
/// ends with an unknown outcome, and its reconciler answers after the
/// lease: worker one ends out of lease and counts nothing. Worker two asks
/// again and burns the attempt. It used to be burned by worker one under
/// the lapsed lease, which then ended its pass in `claim-fenced`.
#[tokio::test]
async fn a_claims_own_unknown_attempt_answered_after_the_lease_is_counted_once() {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &reconcileable(3)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let tool = UnknownTool::default();
    let unknown = ScriptedReconciler::new().with_finding(AgentReconciliationFinding::Unknown);
    let mut one = worker(
        &fx,
        "worker-one",
        the_fixtures_authority(&fx),
        Arc::new(tool.clone()),
    )
    .with_reconciler(Arc::new(late_reconciler(&fx, &unknown)));
    fx.expire_lease();
    fx.settle().await;

    let first = one
        .pump_run(&run_scope())
        .await
        .expect("worker one's pass runs");

    assert_eq!(
        (first.invoked, first.lapsed, first.failed_attempts),
        (1, 1, 0)
    );
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Dispatching, 0),
        "nothing was counted"
    );
    let mut two = worker(
        &fx,
        "worker-two",
        the_fixtures_authority(&fx),
        Arc::new(tool.clone()),
    )
    .with_reconciler(Arc::new(unknown.clone()));
    fx.expire_lease();
    fx.settle().await;
    let second = two
        .pump_run(&run_scope())
        .await
        .expect("worker two's pass runs");
    assert_eq!((second.invoked, second.failed_attempts), (0, 1));
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Failed, 1),
        "the attempt is counted once"
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
}

/// A claim's own last attempt, found "not executed" after the lease, fails
/// the run under the lost worker's code. A budget of one: worker one's call
/// ends with an unknown outcome (`upstream-timeout`), and its reconciler
/// answers after the lease, so worker one writes nothing and the row still
/// marks the attempt `Dispatching`. Worker two recovers the row as an
/// attempt whose worker was lost, is told "not executed", and with no
/// budget left settles it: the run fails under
/// `dispatcher-lost-after-started`. Answered inside the lease, the run
/// fails under the tool's own code. Before issue #212 worker one settled
/// the row under its lapsed lease (issue #171's exception), with that code.
/// Every earlier attempt whose claim ran out of lease has been burned under
/// the lost worker's code since issue #171.
#[tokio::test]
async fn a_claims_own_last_attempt_found_not_executed_after_the_lease_fails_under_the_lost_worker_code(
) {
    let fx = AuthorityFixture::over(
        tool_then_proposal(),
        tool_registry_for_spec(TOOL, &reconcileable(1)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the run holds the tool call");
    let tool = UnknownTool::default();
    let not_executed =
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted);
    let mut one = worker(
        &fx,
        "worker-one",
        the_fixtures_authority(&fx),
        Arc::new(tool.clone()),
    )
    .with_reconciler(Arc::new(late_reconciler(&fx, &not_executed)));
    fx.expire_lease();
    fx.settle().await;

    let first = one
        .pump_run(&run_scope())
        .await
        .expect("worker one's pass runs");

    assert_eq!((first.invoked, first.lapsed), (1, 1));
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(
        (row.status(), row.attempts().attempts()),
        (OutboxStatus::Dispatching, 0)
    );
    let mut two = worker(
        &fx,
        "worker-two",
        the_fixtures_authority(&fx),
        Arc::new(tool.clone()),
    )
    .with_reconciler(Arc::new(not_executed.clone()));
    fx.expire_lease();
    fx.settle().await;
    let second = two
        .pump_run(&run_scope())
        .await
        .expect("worker two's pass runs");
    assert_eq!((second.invoked, second.delivered), (0, 1));
    fx.pump().await;
    assert_eq!(
        effect_failed(&fx).await,
        (
            "dispatcher-lost-after-started".to_string(),
            Some(1),
            Some(1)
        )
    );
    assert_eq!(
        (tool.calls.load(Ordering::SeqCst), not_executed.queries()),
        (1, 2)
    );
}
