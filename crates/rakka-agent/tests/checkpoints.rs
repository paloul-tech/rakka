//! The durable checkpoint substrate: the three kinds, digest-bound grants, the
//! reconciliation decision set, decision deduplication, and timers.
//!
//! Specification: section 12. Scenarios 3, 11, 12, and 57 (its reconciliation
//! half) exercise this substrate. A checkpoint is plain durable state: it
//! round-trips through serialization, so a run passivates behind it and resumes
//! on the next decision with no live execution task. Duplicate decisions never
//! resolve it twice, a timer never auto-approves, and a changed argument digest
//! invalidates a stale grant.

use rakka_agent::{
    AgentApprovalDecision, AgentCheckpoint, AgentCheckpointDecision, AgentCheckpointEffectBinding,
    AgentCheckpointError, AgentCheckpointKind, AgentCheckpointOutcome, AgentCheckpointStatus,
    AgentCheckpointTimerOutcome, AgentCompensationRef, AgentEffectResolution, AgentEffectSpec,
    AgentId, AgentOperationId, AgentOperationKind, AgentReconciliationDecision, AgentRecordKind,
    AgentRevisionNumber, AgentRunEffect, AgentRunEffectOutcome, AgentRunEffectRequest,
    AgentRunScope, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId, TenantId,
    VersionedAgentRecord, CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, HumanCheckpointId, PrincipalRef};

fn scope() -> AgentRunScope {
    AgentRunScope::new(
        TenantId::new("acme"),
        AgentId::new("support").expect("the agent id is valid"),
        rakka_agent::AgentRunId::new("run-1").expect("the run id is valid"),
    )
    .expect("the scope is valid")
}

fn resolver() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "approver".to_string(),
        display_name: None,
    }
}

fn tool_intent(amount: i64) -> AgentRunEffect {
    let call = AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("the call id is valid"),
        AgentToolId::new("charge-card").expect("the tool id is valid"),
        serde_json::json!({ "amount": amount }),
    )
    .expect("the call is bounded");
    AgentRunEffect::new(
        &scope(),
        1,
        0,
        AgentRunEffectRequest::Tool {
            call: Box::new(call),
        },
        &AgentEffectSpec::non_idempotent(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the effect derives")
}

fn decision_key(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::CheckpointResolution,
        ["acme", "support", "run-1", discriminator],
    )
    .expect("the decision key derives")
}

fn open(kind: AgentCheckpointKind, intent: &AgentRunEffect) -> AgentCheckpoint {
    AgentCheckpoint::open(
        HumanCheckpointId::new("ck-1"),
        kind,
        scope(),
        intent,
        "Decide whether to charge the card.",
        resolver(),
        AgentTimestampMillis::new(1),
    )
    .expect("the checkpoint opens")
}

fn approve() -> AgentCheckpointDecision {
    AgentCheckpointDecision::Approval(AgentApprovalDecision::Approve {
        credential_binding: None,
        expires_at: AgentTimestampMillis::new(1_000),
        allowed_use_count: 1,
    })
}

#[test]
fn an_approval_binds_a_cryptographic_grant_to_the_exact_intent() {
    let intent = tool_intent(42);
    let mut checkpoint = open(AgentCheckpointKind::Approval, &intent);
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Open);
    // The record carries the cryptographic digest, never the FNV fingerprint.
    assert!(checkpoint
        .bound_effect
        .argument_digest
        .algorithm
        .is_cryptographic());

    let report = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(5),
        )
        .expect("the approval resolves");
    assert!(!report.deduplicated);
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Resolved);

    let AgentCheckpointOutcome::Granted(grant) = report.outcome else {
        panic!("an approval yields a grant");
    };
    // The grant validates against the exact approved intent.
    grant
        .validate_for(&scope(), &intent, 1, AgentTimestampMillis::new(500))
        .expect("the grant covers the approved intent");
    // A changed argument under the same effect identity invalidates it.
    let changed = tool_intent(99);
    assert_eq!(grant.effect_id, changed.effect_id);
    assert_eq!(
        grant
            .validate_for(&scope(), &changed, 1, AgentTimestampMillis::new(500))
            .expect_err("a changed digest is refused")
            .code(),
        "checkpoint-argument-digest-mismatch"
    );
    // Spent and expired grants fail closed.
    assert_eq!(
        grant
            .validate_for(&scope(), &intent, 2, AgentTimestampMillis::new(500))
            .expect_err("a spent grant is refused")
            .code(),
        "checkpoint-grant-uses-exhausted"
    );
    assert_eq!(
        grant
            .validate_for(&scope(), &intent, 1, AgentTimestampMillis::new(1_001))
            .expect_err("an expired grant is refused")
            .code(),
        "checkpoint-grant-expired"
    );
}

#[test]
fn a_duplicate_decision_does_not_resolve_twice() {
    // Scenario 11: a replayed decision returns the original outcome and makes
    // no second transition; a *different* decision against a resolved
    // checkpoint is refused.
    let intent = tool_intent(42);
    let mut checkpoint = open(AgentCheckpointKind::Approval, &intent);

    let first = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(5),
        )
        .expect("the approval resolves");
    assert!(!first.deduplicated);

    let replay = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(6),
        )
        .expect("the replay is accepted");
    assert!(replay.deduplicated, "the same decision key is deduplicated");
    assert_eq!(
        replay.outcome, first.outcome,
        "the original grant is returned"
    );

    let conflict = checkpoint
        .resolve(
            decision_key("d2"),
            resolver(),
            AgentCheckpointDecision::Approval(AgentApprovalDecision::Deny {
                reason: "changed my mind".to_string(),
            }),
            AgentTimestampMillis::new(7),
        )
        .expect_err("a different decision against a resolved checkpoint is refused");
    assert_eq!(conflict.code(), "checkpoint-already-resolved");
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Resolved);
}

#[test]
fn the_checkpoint_round_trips_and_resolves_after_passivation() {
    // Scenario 3: a checkpoint is plain durable state. It survives a
    // serialization round-trip — a passivated, later-recovered run — and
    // resolves on the next command with no live execution task.
    let intent = tool_intent(42);
    let checkpoint = open(AgentCheckpointKind::Approval, &intent);

    let encoded = serde_json::to_vec(&checkpoint).expect("the checkpoint serializes");
    let mut recovered: AgentCheckpoint =
        serde_json::from_slice(&encoded).expect("the checkpoint deserializes");
    assert_eq!(recovered.status, AgentCheckpointStatus::Open);

    let report = recovered
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(5),
        )
        .expect("the recovered checkpoint resolves");
    assert!(matches!(report.outcome, AgentCheckpointOutcome::Granted(_)));
    assert_eq!(recovered.status, AgentCheckpointStatus::Resolved);
}

#[test]
fn a_timer_escalates_and_expires_but_never_approves() {
    // Scenario 3 / spec 12.6: SLA and expiration are durable-timer driven, and
    // a timeout on non-idempotent work fails closed — it never auto-approves.
    let intent = tool_intent(42);
    let mut checkpoint = open(AgentCheckpointKind::Approval, &intent).with_deadlines(
        Some(AgentTimestampMillis::new(100)),
        Some(AgentTimestampMillis::new(200)),
        Some("secops-oncall".to_string()),
    );

    // Before the SLA deadline nothing is due.
    assert_eq!(
        checkpoint.on_timer(AgentTimestampMillis::new(50)),
        AgentCheckpointTimerOutcome::Pending
    );
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Open);

    // At the SLA deadline it escalates, still waiting.
    assert_eq!(
        checkpoint.on_timer(AgentTimestampMillis::new(150)),
        AgentCheckpointTimerOutcome::Escalated
    );
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Escalated);

    // At expiration it expires — never grants.
    assert_eq!(
        checkpoint.on_timer(AgentTimestampMillis::new(250)),
        AgentCheckpointTimerOutcome::Expired
    );
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Expired);

    // An expired checkpoint cannot be resolved into a grant afterward.
    let refused = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(300),
        )
        .expect_err("an expired checkpoint refuses a late approval");
    assert_eq!(refused.code(), "checkpoint-already-resolved");
}

#[test]
fn a_decision_must_fit_the_checkpoint_kind() {
    let intent = tool_intent(42);
    let mut approval = open(AgentCheckpointKind::Approval, &intent);
    let mismatch = approval
        .resolve(
            decision_key("d1"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedNotExecuted,
            ),
            AgentTimestampMillis::new(5),
        )
        .expect_err("a reconciliation decision does not fit an approval checkpoint");
    assert_eq!(mismatch.code(), "checkpoint-decision-kind-mismatch");

    let mut reconciliation = open(
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        &intent,
    );
    let mismatch = reconciliation
        .resolve(
            decision_key("d2"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(5),
        )
        .expect_err("an approval decision does not fit a reconciliation checkpoint");
    assert_eq!(mismatch.code(), "checkpoint-decision-kind-mismatch");
}

#[test]
fn the_reconciliation_decision_set_maps_each_decision() {
    // Scenario 57 (reconciliation half): the full decision set, with no plain
    // `Retry`. Each decision reaches its distinct outcome.
    let intent = tool_intent(42);

    // ConfirmedNotExecuted authorizes a new generation through the effect layer.
    let mut checkpoint = open(
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        &intent,
    );
    let report = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedNotExecuted,
            ),
            AgentTimestampMillis::new(5),
        )
        .expect("confirmed-not-executed resolves");
    assert!(matches!(
        report.outcome,
        AgentCheckpointOutcome::EffectResolution(resolution)
            if matches!(*resolution, AgentEffectResolution::ConfirmedNotExecuted)
    ));
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Resolved);

    // ConfirmedCompleted carries an authoritative outcome.
    let mut checkpoint = open(
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        &intent,
    );
    let outcome = AgentRunEffectOutcome::Tool {
        call_id: AgentToolCallId::new("call-1").expect("the call id is valid"),
        content: AgentTaskContent::inline(serde_json::json!({ "charged": true }))
            .expect("the content is bounded"),
    };
    let report = checkpoint
        .resolve(
            decision_key("d2"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedCompleted {
                    resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                        outcome: Box::new(outcome),
                    }),
                },
            ),
            AgentTimestampMillis::new(5),
        )
        .expect("confirmed-completed resolves");
    assert!(matches!(
        report.outcome,
        AgentCheckpointOutcome::EffectResolution(resolution)
            if matches!(*resolution, AgentEffectResolution::ConfirmedExecuted { .. })
    ));

    // Compensate schedules an explicit compensation.
    let mut checkpoint = open(
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        &intent,
    );
    let compensation = AgentCompensationRef::new("refund-charge").expect("the ref is valid");
    let report = checkpoint
        .resolve(
            decision_key("d3"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::Compensate {
                compensation: compensation.clone(),
            }),
            AgentTimestampMillis::new(5),
        )
        .expect("compensate resolves");
    assert_eq!(
        report.outcome,
        AgentCheckpointOutcome::Compensate { compensation }
    );
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Compensated);

    // AbandonAndFail abandons the effect terminally.
    let mut checkpoint = open(
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        &intent,
    );
    let report = checkpoint
        .resolve(
            decision_key("d4"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::AbandonAndFail),
            AgentTimestampMillis::new(5),
        )
        .expect("abandon resolves");
    assert_eq!(report.outcome, AgentCheckpointOutcome::Abandoned);
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Abandoned);
}

#[test]
fn escalation_stays_nonterminal_until_a_resolving_decision() {
    // Scenario 57: an escalated reconciliation is still owed. A later resolving
    // decision, under a new key, converges it.
    let intent = tool_intent(42);
    let mut checkpoint = open(
        AgentCheckpointKind::IndeterminateEffectReconciliation,
        &intent,
    );

    let escalated = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(AgentReconciliationDecision::Escalate),
            AgentTimestampMillis::new(5),
        )
        .expect("escalate is accepted");
    assert_eq!(escalated.outcome, AgentCheckpointOutcome::Escalated);
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Escalated);
    assert!(checkpoint.status.is_waiting(), "escalation is nonterminal");

    let resolved = checkpoint
        .resolve(
            decision_key("d2"),
            resolver(),
            AgentCheckpointDecision::Reconciliation(
                AgentReconciliationDecision::ConfirmedNotExecuted,
            ),
            AgentTimestampMillis::new(6),
        )
        .expect("a later decision resolves the escalated checkpoint");
    assert!(matches!(
        resolved.outcome,
        AgentCheckpointOutcome::EffectResolution(_)
    ));
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Resolved);
}

#[test]
fn the_checkpoint_record_declares_its_schema_and_kind() {
    let intent = tool_intent(42);
    let checkpoint = open(AgentCheckpointKind::Approval, &intent);
    assert_eq!(
        checkpoint.schema_version(),
        CURRENT_AGENT_CHECKPOINT_SCHEMA_VERSION
    );
    assert_eq!(AgentCheckpoint::RECORD_KIND, AgentRecordKind::Checkpoint);
}

#[test]
fn a_cancelled_run_cancels_a_waiting_checkpoint() {
    let intent = tool_intent(42);
    let mut checkpoint = open(AgentCheckpointKind::Approval, &intent);
    checkpoint.cancel(AgentTimestampMillis::new(9));
    assert_eq!(checkpoint.status, AgentCheckpointStatus::Cancelled);

    // A cancelled checkpoint no longer resolves into a grant.
    let refused = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(10),
        )
        .expect_err("a cancelled checkpoint refuses a late approval");
    assert!(matches!(
        refused,
        AgentCheckpointError::AlreadyResolved { .. }
    ));
}

#[test]
fn the_binding_validation_path_agrees_with_the_effect_validation_path() {
    // The claim promotion gate (slice 2.3) validates grants through
    // `validate_for_binding`; this parity proof is what lets one code path
    // serve both gates without drift. For the same grant, the effect path and
    // the binding path derived from that same effect agree on the pass case
    // and on every single-defect refusal.
    let intent = tool_intent(42);
    let mut checkpoint = open(AgentCheckpointKind::Approval, &intent);
    let report = checkpoint
        .resolve(
            decision_key("d1"),
            resolver(),
            approve(),
            AgentTimestampMillis::new(5),
        )
        .expect("the approval resolves");
    let AgentCheckpointOutcome::Granted(grant) = report.outcome else {
        panic!("an approval yields a grant");
    };
    let binding = AgentCheckpointEffectBinding::of_effect(&intent).expect("the binding derives");

    // Pass case.
    grant
        .validate_for(&scope(), &intent, 1, AgentTimestampMillis::new(500))
        .expect("the effect path accepts");
    grant
        .validate_for_binding(&binding, 1, AgentTimestampMillis::new(500))
        .expect("the binding path accepts");

    // A changed argument refuses identically on both paths.
    let changed = tool_intent(99);
    let changed_binding =
        AgentCheckpointEffectBinding::of_effect(&changed).expect("the binding derives");
    assert_eq!(
        grant
            .validate_for(&scope(), &changed, 1, AgentTimestampMillis::new(500))
            .expect_err("a changed digest is refused")
            .code(),
        grant
            .validate_for_binding(&changed_binding, 1, AgentTimestampMillis::new(500))
            .expect_err("a changed digest is refused")
            .code(),
    );

    // Spent and expired grants refuse identically on both paths.
    for (attempt, now) in [(2, 500), (1, 1_001)] {
        assert_eq!(
            grant
                .validate_for(&scope(), &intent, attempt, AgentTimestampMillis::new(now))
                .expect_err("the effect path refuses")
                .code(),
            grant
                .validate_for_binding(&binding, attempt, AgentTimestampMillis::new(now))
                .expect_err("the binding path refuses")
                .code(),
        );
    }

    // The binding path's target comparison is the one strengthening: a
    // tampered target refuses even though every other field still matches.
    let mut tampered = binding.clone();
    tampered.target = "tool:other-tool".to_string();
    assert_eq!(
        grant
            .validate_for_binding(&tampered, 1, AgentTimestampMillis::new(500))
            .expect_err("a tampered target is refused")
            .code(),
        "checkpoint-grant-intent-mismatch"
    );
}
