//! Phase 5 autonomy target catalog and policy tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    agent_autonomy_policy_audit_event, AgentAuditEventId, AgentAuditEventKind, AgentAutonomyPolicy,
    AgentAutonomyPolicyDecisionStatus, AgentAutonomyTargetClass, AgentAutonomyUsage,
    AgentCausationId, AgentCorrelationId, AgentDeduplicationKey, AgentDispatchTargetClass,
    AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectStatus, AgentEffectTarget,
    AgentEffectTargetCatalog, AgentIdempotencyKey, AgentRunId, AgentTelemetryContext,
    AgentTenantId, AgentTimestampMillis, AgentWorkflowId, ArtifactKind, ArtifactRef,
    RedactionStatus, WorkflowDefinitionVersion, AGENT_AUTONOMY_DECISION_STATUS_ATTRIBUTE,
    AGENT_AUTONOMY_POLICY_VERSION_ATTRIBUTE, AGENT_AUTONOMY_REASON_CODE_ATTRIBUTE,
    AGENT_AUTONOMY_TARGET_CLASS_ATTRIBUTE,
};

#[test]
fn phase5_catalog_classifies_and_requires_artifacts_for_large_effect_classes() {
    let catalog = AgentEffectTargetCatalog::phase5_default();
    let policy = AgentAutonomyPolicy::phase5_default("phase5.v1");
    let usage = AgentAutonomyUsage::new();
    let now = AgentTimestampMillis::new(100);

    let missing_prompt = effect(
        "model-no-artifact",
        AgentEffectKind::ModelCall,
        "model",
        "reasoning-model",
        [],
        None,
    );
    let denied = catalog
        .validate_effect(&policy, &usage, &missing_prompt, None, now)
        .expect("policy decision");
    assert_eq!(denied.status, AgentAutonomyPolicyDecisionStatus::Denied);
    assert_eq!(denied.reason_code, "input-artifact-required");
    assert_eq!(denied.target_class, Some(AgentAutonomyTargetClass::Model));

    let with_prompt = effect(
        "model-with-artifact",
        AgentEffectKind::ModelCall,
        "model",
        "reasoning-model",
        [],
        Some(artifact("prompt")),
    );
    let allowed = catalog
        .validate_effect(&policy, &usage, &with_prompt, None, now)
        .expect("policy decision");
    assert!(allowed.is_allowed());
}

#[test]
fn skill_and_workflow_policy_reject_disallowed_targets_before_scheduling() {
    let catalog = AgentEffectTargetCatalog::phase5_default().with_skill_targets(
        "research",
        [
            AgentAutonomyTargetClass::Model,
            AgentAutonomyTargetClass::Tool,
        ],
    );
    let policy = AgentAutonomyPolicy::fail_closed("phase5.v1")
        .allow_target_class(AgentAutonomyTargetClass::Model)
        .allow_target_class(AgentAutonomyTargetClass::Tool)
        .allow_tool("search");
    let usage = AgentAutonomyUsage::new();
    let now = AgentTimestampMillis::new(200);

    let disallowed_peer = effect(
        "peer-call",
        AgentEffectKind::HttpCall,
        "a2a-peer",
        "billing-agent",
        [("target_class", "a2a-peer")],
        Some(artifact("peer-request")),
    );
    let denied = catalog
        .validate_effect(&policy, &usage, &disallowed_peer, Some("research"), now)
        .expect("policy decision");
    assert_eq!(denied.reason_code, "target-class-disallowed");

    let disallowed_tool = effect(
        "tool-call",
        AgentEffectKind::ToolCall,
        "tool",
        "shell",
        [],
        Some(artifact("tool-input")),
    );
    let denied = catalog
        .validate_effect(&policy, &usage, &disallowed_tool, Some("research"), now)
        .expect("policy decision");
    assert_eq!(denied.reason_code, "tool-disallowed");
}

#[test]
fn budgets_approval_and_cancellation_are_durable_policy_decisions() {
    let catalog = AgentEffectTargetCatalog::phase5_default();
    let effect = effect(
        "webhook-call",
        AgentEffectKind::HttpCall,
        "webhook",
        "customer-callback",
        [("target_class", "webhook")],
        Some(artifact("webhook-payload")),
    );
    let now = AgentTimestampMillis::new(1_100);

    let budget_policy = AgentAutonomyPolicy::phase5_default("phase5.v1").max_external_calls(3);
    let budget_denied = catalog
        .validate_effect(
            &budget_policy,
            &AgentAutonomyUsage::new().external_calls(3),
            &effect,
            None,
            now,
        )
        .expect("budget decision");
    assert_eq!(budget_denied.reason_code, "max-external-calls-exceeded");

    let approval_policy = AgentAutonomyPolicy::phase5_default("phase5.v2")
        .require_approval_for(AgentAutonomyTargetClass::Webhook);
    let approval = catalog
        .validate_effect(
            &approval_policy,
            &AgentAutonomyUsage::new(),
            &effect,
            None,
            now,
        )
        .expect("approval decision");
    assert_eq!(
        approval.status,
        AgentAutonomyPolicyDecisionStatus::ApprovalRequired
    );

    let cancelled_policy =
        AgentAutonomyPolicy::phase5_default("phase5.v3").cancellation_requested();
    let cancelled = catalog
        .validate_effect(
            &cancelled_policy,
            &AgentAutonomyUsage::new(),
            &effect,
            None,
            now,
        )
        .expect("cancel decision");
    assert_eq!(cancelled.reason_code, "cancellation-requested");
    assert!(cancelled.is_rejected());
}

#[test]
fn autonomy_classification_agrees_with_dispatch_classification() {
    let cases = [
        // Name-based heuristics no longer classify: a notification whose name
        // merely contains "webhook" is not policy-classified as a webhook.
        (
            effect(
                "notify-named-webhook",
                AgentEffectKind::Notification,
                "notification",
                "slack-webhook",
                [],
                None,
            ),
            AgentAutonomyTargetClass::Other,
        ),
        // Explicit target types and target_class attributes still classify.
        (
            effect(
                "notify-webhook",
                AgentEffectKind::Notification,
                "webhook",
                "customer-callback",
                [],
                None,
            ),
            AgentAutonomyTargetClass::Webhook,
        ),
        (
            effect(
                "peer-call",
                AgentEffectKind::HttpCall,
                "http",
                "billing-agent",
                [("target_class", "a2a-peer")],
                None,
            ),
            AgentAutonomyTargetClass::A2aPeer,
        ),
        // A kind-incompatible target_class label cannot reroute the effect.
        (
            effect(
                "notify-peer-label",
                AgentEffectKind::Notification,
                "notification",
                "ops-alert",
                [("target_class", "a2a-peer")],
                None,
            ),
            AgentAutonomyTargetClass::Other,
        ),
    ];

    for (effect, expected) in cases {
        let autonomy_class = AgentAutonomyTargetClass::for_effect(&effect);
        assert_eq!(autonomy_class, expected, "effect {}", effect.effect_id);
        let dispatch_class = AgentDispatchTargetClass::classify(effect.kind, &effect.target);
        assert_eq!(
            autonomy_class,
            AgentAutonomyTargetClass::from_dispatch_class(dispatch_class),
            "policy and dispatch must agree for effect {}",
            effect.effect_id
        );
    }
}

#[test]
fn policy_decisions_carry_bounded_audit_attributes() {
    let decision = AgentAutonomyPolicy::phase5_default("phase5.v1")
        .max_autonomous_steps(1)
        .evaluate_target(
            AgentAutonomyTargetClass::Tool,
            &AgentAutonomyUsage::new().autonomous_steps(1),
            AgentTimestampMillis::new(500),
        );

    let event = agent_autonomy_policy_audit_event(
        AgentAuditEventId::new("audit-policy-denial"),
        AgentWorkflowId::new("workflow-a"),
        AgentRunId::new("run-a"),
        WorkflowDefinitionVersion::new("v1"),
        Some(AgentTenantId::new("tenant-a")),
        decision,
        AgentCausationId::new("cause-a"),
        AgentCorrelationId::new("corr-a"),
        AgentTelemetryContext::default(),
    );

    assert_eq!(event.kind, AgentAuditEventKind::PolicyOverride);
    assert_eq!(
        event
            .attributes
            .get(AGENT_AUTONOMY_POLICY_VERSION_ATTRIBUTE)
            .map(String::as_str),
        Some("phase5.v1")
    );
    assert_eq!(
        event
            .attributes
            .get(AGENT_AUTONOMY_DECISION_STATUS_ATTRIBUTE)
            .map(String::as_str),
        Some("denied")
    );
    assert_eq!(
        event
            .attributes
            .get(AGENT_AUTONOMY_REASON_CODE_ATTRIBUTE)
            .map(String::as_str),
        Some("max-autonomous-steps-exceeded")
    );
    assert_eq!(
        event
            .attributes
            .get(AGENT_AUTONOMY_TARGET_CLASS_ATTRIBUTE)
            .map(String::as_str),
        Some("tool")
    );
    assert_eq!(event.artifact_refs, Vec::<ArtifactRef>::new());
}

fn effect(
    id: &str,
    kind: AgentEffectKind,
    target_type: &str,
    name: &str,
    attributes: impl IntoIterator<Item = (&'static str, &'static str)>,
    payload_ref: Option<ArtifactRef>,
) -> AgentEffect {
    AgentEffect {
        effect_id: AgentEffectId::new(id),
        deduplication_key: AgentDeduplicationKey::new(format!("dedupe-{id}")),
        kind,
        target: AgentEffectTarget {
            target_type: target_type.to_string(),
            name: name.to_string(),
            address: Some(format!("https://example.com/{name}")),
            attributes: attrs(attributes),
        },
        status: AgentEffectStatus::Scheduled,
        payload_ref,
        result_ref: None,
        timeout_ms: Some(30_000),
        idempotency_key: AgentIdempotencyKey::new(format!("idem-{id}")),
        expected_result_type: Some("phase5.result".to_string()),
        causation_id: AgentCausationId::new(format!("cause-{id}")),
        correlation_id: AgentCorrelationId::new(format!("corr-{id}")),
        telemetry_context: AgentTelemetryContext::default(),
        attempt: 0,
        created_at: AgentTimestampMillis::new(10),
        due_at: None,
        last_error_code: None,
    }
}

fn artifact(id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind: ArtifactKind::Input,
        uri: format!("memory://phase5/{id}"),
        checksum: None,
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("test".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(1),
        metadata: attrs([("purpose", "phase5-test")]),
    }
}

fn attrs(
    pairs: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> BTreeMap<String, String> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
