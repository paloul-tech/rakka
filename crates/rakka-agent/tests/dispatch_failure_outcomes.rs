//! How the dispatcher settles an attempt whose executor answered a
//! definitive refusal or an unknown outcome (issue #85, asks 7 and 14).
//!
//! A scripted executor answers every call with one error and counts the
//! calls; the run, the outbox row, and the call count are the evidence.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedReconciler};
use rakka_agent::{
    agent_operational_snapshot, AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor,
    AgentEffectSpec, AgentFailureReason, AgentModelTurn, AgentReconciliationFinding,
    AgentReconciliationProtocolRef, AgentRunEffect, AgentRunEffectOutcome, AgentRunEffectStatus,
    AgentRunScope, AgentRunStatus, AgentSchemaPolicy, AgentTaskContent, AgentToolAuthority,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, AgentUnknownOutcomeEvidence,
    AgentUnknownOutcomeReport, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};

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
    // Issue #100 ask 4: the effect settles `Exhausted` exactly as a spent
    // budget does, and the reason says the collaborator refused for good.
    assert!(
        resolved
            .last_error_reason
            .as_ref()
            .is_some_and(AgentFailureReason::is_definitive),
        "the effect record marks the refusal definitive"
    );
    assert!(
        fx.terminal_failure_reason()
            .await
            .is_some_and(|reason| reason.is_definitive()),
        "so does the run's terminal reason"
    );
    // Issue #100 ask 3: one attempt made of the three allowed.
    assert_eq!(fx.terminal_failure_attempts().await, (Some(1), Some(3)));

    fx.expire_lease();
    fx.pump().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "nothing invokes it again");
}

/// Issue #100 asks 3 and 4: a retryable refusal that spends its whole budget
/// ends in the same `Exhausted` status, under the same reason code, without
/// the marker — and its terminal reason counts every attempt.
#[tokio::test]
async fn a_spent_budget_is_not_marked_definitive() {
    let spec = AgentEffectSpec::idempotent(2).expect("the spec is valid");
    let (fx, calls) = fixture(&spec, retryable);
    ready(&fx).await;

    fx.one_pass().await;
    fx.expire_lease();
    fx.one_pass().await;

    assert_eq!(calls.load(Ordering::SeqCst), 2, "both attempts ran");
    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    let reason = resolved
        .last_error_reason
        .expect("the collaborator's code is the reason");
    assert_eq!(reason.code(), "egress-denied");
    assert!(!reason.is_definitive(), "a spent budget is not definitive");
    assert!(
        fx.terminal_failure_reason()
            .await
            .is_some_and(|reason| !reason.is_definitive()),
        "nor is the run's terminal reason"
    );
    assert_eq!(fx.terminal_failure_attempts().await, (Some(2), Some(2)));
}

fn definitive_without_a_code() -> AgentDispatchError {
    AgentDispatchError::definitive("", "the host refused the destination")
}

/// Issue #100 ask 4: a blank code identifies nothing, but a definitive
/// refusal under one still says it was definitive.
#[tokio::test]
async fn a_definitive_refusal_under_a_blank_code_still_carries_the_marker() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, _calls) = fixture(&spec, definitive_without_a_code);
    ready(&fx).await;

    fx.one_pass().await;

    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert!(
        resolved
            .last_error_reason
            .as_ref()
            .is_some_and(AgentFailureReason::is_definitive),
        "the marker survives a blank code: {:?}",
        resolved.last_error_reason
    );
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

fn unknown() -> AgentDispatchError {
    AgentDispatchError::outcome_unknown(
        "scripted-outcome-unknown",
        "the call was sent and no answer was read",
    )
}

#[tokio::test]
async fn an_unknown_outcome_parks_a_non_idempotent_effect_for_reconciliation() {
    let (fx, calls) = fixture(&AgentEffectSpec::non_idempotent(), unknown);
    ready(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1, "{pass:?}");
    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("scripted-outcome-unknown"),
        "the executor's code, not the worker-loss code"
    );
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run").status,
        AgentRunStatus::WaitingForReconciliation
    );

    fx.expire_lease();
    fx.pump().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a non-idempotent target is never invoked again"
    );
}

#[tokio::test]
async fn an_unknown_outcome_retries_a_read_only_or_idempotent_effect_under_its_budget() {
    for spec in [
        AgentEffectSpec::read_only()
            .with_max_attempts(3)
            .expect("the spec is valid"),
        AgentEffectSpec::idempotent(3).expect("the spec is valid"),
    ] {
        let class = spec.safety_class;
        let (fx, calls) = fixture(&spec, unknown);
        let effect = ready(&fx).await;

        let pass = fx.one_pass().await;

        assert_eq!(pass.failed_attempts, 1, "{class:?}: {pass:?}");
        let row = fx.outbox_row(&effect).await.expect("the ticket's row");
        assert_eq!(
            row.status(),
            OutboxStatus::Failed,
            "{class:?}: retry scheduled"
        );
        assert!(
            row.attempts()
                .last_error()
                .is_some_and(|line| line.starts_with("scripted-outcome-unknown")),
            "{class:?}: the executor's code leads the line: {:?}",
            row.attempts().last_error()
        );
        fx.expire_lease();
        fx.one_pass().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "{class:?}: retried");
    }
}

#[tokio::test]
async fn an_unknown_outcome_on_an_idempotent_effects_last_attempt_exhausts_it() {
    let spec = AgentEffectSpec::idempotent(1).expect("the spec is valid");
    let (fx, _calls) = fixture(&spec, unknown);
    ready(&fx).await;

    fx.one_pass().await;

    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("scripted-outcome-unknown")
    );
}

#[tokio::test]
async fn an_unknown_outcome_asks_the_reconciler_of_a_reconcileable_effect_in_the_same_pass() {
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let spec = AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid");
    let reconciler = ScriptedReconciler::new();
    let (fx, calls) = fixture(&spec, unknown);
    let fx = fx.with_reconciler(Arc::new(reconciler.clone()));
    ready(&fx).await;

    fx.one_pass().await;

    assert_eq!(
        reconciler.queries(),
        1,
        "queried in the pass that saw the unknown outcome"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A reconcileable effect whose reconciler answers `finding`, behind an
/// executor whose every outcome is unknown.
fn reconcileable_fixture(
    finding: AgentReconciliationFinding,
) -> (AuthorityFixture, Arc<AtomicUsize>, ScriptedReconciler) {
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let spec = AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid");
    let reconciler = ScriptedReconciler::new().with_finding(finding);
    let (fx, calls) = fixture(&spec, unknown);
    (
        fx.with_reconciler(Arc::new(reconciler.clone())),
        calls,
        reconciler,
    )
}

#[tokio::test]
async fn an_unknown_outcome_the_reconciler_finds_not_executed_is_reinvoked_under_the_executors_code(
) {
    // The scripted reconciler repeats its last answer, so every lap of the
    // recovery finds the effect not executed and spends an attempt.
    let (fx, calls, reconciler) = reconcileable_fixture(AgentReconciliationFinding::NotExecuted);
    let effect = ready(&fx).await;

    fx.one_pass().await;

    assert_eq!(
        (calls.load(Ordering::SeqCst), reconciler.queries()),
        (3, 3),
        "re-invoked once per not-executed finding, until the budget of three is spent"
    );
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(row.status(), OutboxStatus::Exhausted);
    assert_eq!(row.attempts().attempts(), 3);
    assert!(
        row.attempts()
            .last_error()
            .is_some_and(|line| line.starts_with("scripted-outcome-unknown")),
        "the executor's code, not a lost dispatcher: {:?}",
        row.attempts().last_error()
    );
    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("scripted-outcome-unknown"),
        "the run's record names what happened"
    );
}

#[tokio::test]
async fn an_unknown_outcome_the_reconciler_finds_executed_delivers_the_established_outcome() {
    let established = AgentTaskContent::inline(serde_json::json!({ "receipt": "r-77" }))
        .expect("the content is inline-bounded");
    let (fx, calls, reconciler) = reconcileable_fixture(AgentReconciliationFinding::Executed {
        outcome: Box::new(AgentRunEffectOutcome::Tool {
            call_id: AgentToolCallId::new("call-1").expect("the call id is valid"),
            content: established,
        }),
    });
    let effect = ready(&fx).await;

    fx.one_pass().await;

    assert_eq!((calls.load(Ordering::SeqCst), reconciler.queries()), (1, 1));
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(
        row.status(),
        OutboxStatus::Dispatched,
        "the reconciled outcome settles the ticket without a second invocation"
    );
}

/// An executor that answers every call with an unknown outcome carrying
/// `report`, and records the shared clock when it is called. The fixture's
/// dispatcher reads that clock without advancing it, and nothing advances it
/// between the invocation's start and the call, so the recorded value is the
/// invocation's start. The call then takes ten seconds of that clock, so a
/// send time anchored anywhere after the invocation began — at the park, say
/// — reads later than the start plus the reported offset.
struct ReportingExecutor {
    clock: Arc<std::sync::atomic::AtomicU64>,
    called_at: Arc<std::sync::atomic::AtomicU64>,
    report: fn() -> AgentUnknownOutcomeReport,
}

impl AgentDispatchToolExecutor for ReportingExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.called_at
            .store(self.clock.load(Ordering::SeqCst), Ordering::SeqCst);
        self.clock.fetch_add(10_000, Ordering::SeqCst);
        let error = AgentDispatchError::outcome_unknown(
            "scripted-outcome-unknown",
            "the call was handed over and no answer was read",
        )
        .with_report((self.report)());
        Box::pin(async move { Err(error) })
    }
}

/// A run whose tool ticket, under `spec`, is ready to claim, behind an
/// executor that reports `report`; and the cell its call time lands in.
fn reporting_fixture(
    spec: &AgentEffectSpec,
    report: fn() -> AgentUnknownOutcomeReport,
) -> (AuthorityFixture, Arc<std::sync::atomic::AtomicU64>) {
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    );
    let called_at = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let executor = ReportingExecutor {
        clock: fx.fx.clock.clone(),
        called_at: called_at.clone(),
        report,
    };
    (fx.with_tool_executor(Arc::new(executor)), called_at)
}

fn deadline_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new()
        .with_reason("scripted-deadline-awaiting-answer")
        .with_handed_over_after(Duration::from_millis(250))
        .with_cancellation_issued(true)
}

/// Issue #100 ask 5: a non-idempotent park records the executor's reason, a
/// send time on the dispatcher's clock (the attempt's start plus the
/// reported offset), and the cancellation; the snapshot reports them.
#[tokio::test]
async fn a_parked_effect_records_what_its_executor_knew() {
    let (fx, called_at) = reporting_fixture(&AgentEffectSpec::non_idempotent(), deadline_report);
    ready(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1, "{pass:?}");
    let started = called_at.load(Ordering::SeqCst);
    assert!(started > 0, "the executor ran and read the clock");
    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("scripted-outcome-unknown"),
        "the pipeline code is unchanged"
    );
    assert_eq!(
        parked
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("scripted-deadline-awaiting-answer")
    );
    let expected =
        AgentUnknownOutcomeEvidence::new(Some(AgentTimestampMillis::new(started + 250)), true);
    assert_eq!(parked.unknown_outcome, Some(expected));

    let snapshot = agent_operational_snapshot(
        &fx.fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(started + 1_000),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    let view = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.effect_id == parked.effect_id)
        .expect("the parked effect is reported");
    assert_eq!(view.unknown_outcome, Some(expected));
}

fn reason_only_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new().with_reason("scripted-call-not-handed-over")
}

/// Review focus 1: a report that knows only why records the reason and
/// writes no evidence object.
#[tokio::test]
async fn a_report_with_only_a_reason_records_no_evidence() {
    let (fx, _) = reporting_fixture(&AgentEffectSpec::non_idempotent(), reason_only_report);
    ready(&fx).await;
    fx.one_pass().await;

    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(
        parked
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("scripted-call-not-handed-over")
    );
    assert_eq!(parked.unknown_outcome, None);
}

fn absurd_offset_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new().with_handed_over_after(Duration::MAX)
}

/// Review focus 2: an offset past any representable time saturates.
#[tokio::test]
async fn an_absurd_offset_saturates_rather_than_panicking() {
    let (fx, _) = reporting_fixture(&AgentEffectSpec::non_idempotent(), absurd_offset_report);
    ready(&fx).await;
    fx.one_pass().await;

    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(
        parked.unknown_outcome,
        Some(AgentUnknownOutcomeEvidence::new(
            Some(AgentTimestampMillis::new(u64::MAX)),
            false
        ))
    );
}

fn blank_reason_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new()
        .with_reason("   ")
        .with_cancellation_issued(true)
}

/// Review focus 3: a blank reason identifies nothing, so none is recorded;
/// the pipeline code and the evidence still are.
#[tokio::test]
async fn a_blank_reason_code_records_no_reason_but_keeps_the_evidence() {
    let (fx, _) = reporting_fixture(&AgentEffectSpec::non_idempotent(), blank_reason_report);
    ready(&fx).await;
    fx.one_pass().await;

    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("scripted-outcome-unknown")
    );
    assert_eq!(parked.last_error_reason, None);
    assert_eq!(
        parked.unknown_outcome,
        Some(AgentUnknownOutcomeEvidence::new(None, true))
    );
}

/// Issue #100 ask 5: only the park records evidence. An idempotent effect
/// whose last attempt's outcome is unknown exhausts as before, carrying
/// neither the reason nor the evidence.
#[tokio::test]
async fn an_exhausted_idempotent_effect_records_no_evidence() {
    let spec = AgentEffectSpec::idempotent(1).expect("the spec is valid");
    let (fx, _) = reporting_fixture(&spec, deadline_report);
    ready(&fx).await;
    fx.one_pass().await;

    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(resolved.last_error_reason, None);
    assert_eq!(resolved.unknown_outcome, None);
}

/// The report rides only an unknown outcome, and reads back as set.
#[test]
fn a_report_attaches_only_to_an_unknown_outcome() {
    let unknown = AgentDispatchError::outcome_unknown("c", "m").with_report(deadline_report());
    assert_eq!(unknown.unknown_outcome_report(), Some(&deadline_report()));
    let other = AgentDispatchError::collaborator("c", "m").with_report(deadline_report());
    assert_eq!(other.unknown_outcome_report(), None);
}
