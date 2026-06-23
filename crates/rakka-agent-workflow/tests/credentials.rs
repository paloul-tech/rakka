//! Credential resolver contract tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use rakka_agent_workflow::{
    credential_binding_ref_from_effect, AgentAdapterFailureClass, AgentCausationId,
    AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCorrelationId, AgentCredentialBindingRef, AgentCredentialError,
    AgentCredentialResolutionRequest, AgentCredentialResolver, AgentCredentialResolverFuture,
    AgentCredentialUse, AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffect, AgentEffectId,
    AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget,
    AgentEphemeralCredential, AgentEphemeralCredentialMaterial, AgentGraphNodeState,
    AgentGraphNodeStatus, AgentGraphRunProjection, AgentGraphRunState, AgentGraphWaitReason,
    AgentIdempotencyKey, AgentRunId, AgentTelemetryContext, AgentTenantId, AgentTimestampMillis,
    AgentWorkflowId, ArtifactKind, ArtifactRef, RedactionStatus,
    AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE,
};
use rakka_workflow::OutboxDispatchResult;

#[tokio::test]
async fn fake_resolver_returns_ephemeral_credential_for_tool_effect() {
    let resolver = FakeCredentialResolver::default()
        .with_credential("cred-slack", credential("slack-secret-v1"));
    let effect = credential_effect("tool-effect", "cred-slack");
    let request = resolution_request(&effect);

    let resolved = resolver
        .resolve(request.clone())
        .await
        .expect("credential should resolve");

    assert_eq!(request.tenant, Some(AgentTenantId::new("tenant-a")));
    assert_eq!(request.workflow_id, AgentWorkflowId::new("workflow-slack"));
    assert_eq!(request.run_id, AgentRunId::new("run-slack"));
    assert_eq!(
        request.plan_fingerprint,
        AgentCompiledPlanFingerprint::new("sha256:slack-plan")
    );
    assert_eq!(request.node_id, AgentCompiledNodeId::new("send-slack"));
    assert_eq!(
        request.credential_binding_ref,
        AgentCredentialBindingRef::new("cred-slack")
    );
    assert_eq!(request.credential_use, AgentCredentialUse::ToolAdapter);
    assert_eq!(
        request.causation_id,
        AgentCausationId::new("cause-tool-effect")
    );
    assert_eq!(
        request.correlation_id,
        AgentCorrelationId::new("corr-tool-effect")
    );
    assert_eq!(
        request.telemetry_context.trace_parent.as_deref(),
        Some("00-00000000000000000000000000000001-0000000000000001-01")
    );

    match resolved.material() {
        AgentEphemeralCredentialMaterial::BearerToken { token } => {
            assert_eq!(token, "slack-secret-v1");
        }
        other => panic!("unexpected credential material: {other:?}"),
    }
    assert_eq!(
        resolved
            .attributes()
            .get("secret_version")
            .map(String::as_str),
        Some("v1")
    );
}

#[test]
fn resolver_failures_map_to_stable_dispatch_failures() {
    let missing = AgentCredentialError::MissingBinding {
        binding_ref: AgentCredentialBindingRef::new("cred-missing"),
    };
    assert_eq!(missing.code(), "credential-binding-missing");
    assert_eq!(missing.failure_class(), AgentAdapterFailureClass::Permanent);
    assert_eq!(
        missing.to_outbox_dispatch_result(),
        OutboxDispatchResult::failure("permanent:credential-binding-missing")
    );

    let revoked = AgentCredentialError::RevokedBinding {
        binding_ref: AgentCredentialBindingRef::new("cred-revoked"),
    };
    assert_eq!(revoked.code(), "credential-binding-revoked");
    assert_eq!(revoked.failure_class(), AgentAdapterFailureClass::Permanent);
    assert_eq!(
        revoked.to_outbox_dispatch_result(),
        OutboxDispatchResult::failure("permanent:credential-binding-revoked")
    );

    let unavailable = AgentCredentialError::Unavailable {
        binding_ref: AgentCredentialBindingRef::new("cred-store"),
        reason: "timeout".to_string(),
        retry_after: Some(AgentTimestampMillis::new(1_000)),
    };
    assert_eq!(unavailable.code(), "credential-resolver-unavailable");
    assert_eq!(
        unavailable.failure_class(),
        AgentAdapterFailureClass::Retryable
    );
    assert_eq!(
        unavailable.to_outbox_dispatch_result(),
        OutboxDispatchResult::failure("retryable:credential-resolver-unavailable")
    );
}

#[tokio::test]
async fn resolver_missing_and_revoked_bindings_surface_bounded_errors() {
    let resolver = FakeCredentialResolver::default().with_error(
        "cred-revoked",
        AgentCredentialError::RevokedBinding {
            binding_ref: AgentCredentialBindingRef::new("cred-revoked"),
        },
    );

    let missing = resolver
        .resolve(resolution_request(&credential_effect(
            "missing-effect",
            "cred-missing",
        )))
        .await
        .expect_err("missing binding should fail");
    assert_eq!(missing.code(), "credential-binding-missing");
    assert!(!missing.to_string().contains("secret"));

    let revoked = resolver
        .resolve(resolution_request(&credential_effect(
            "revoked-effect",
            "cred-revoked",
        )))
        .await
        .expect_err("revoked binding should fail");
    assert_eq!(revoked.code(), "credential-binding-revoked");
    assert!(!revoked.to_string().contains("secret"));
}

#[tokio::test]
async fn credential_rotation_does_not_change_compiled_plan_reference() {
    let resolver = FakeCredentialResolver::default()
        .with_credential("cred-slack", credential("slack-secret-v1"));
    let effect = credential_effect("rotated-effect", "cred-slack");
    let request = resolution_request(&effect);
    let plan_fingerprint = request.plan_fingerprint.clone();

    let first = resolver
        .resolve(request.clone())
        .await
        .expect("initial credential should resolve");
    resolver.rotate("cred-slack", credential("slack-secret-v2"));
    let second = resolver
        .resolve(request.clone())
        .await
        .expect("rotated credential should resolve");

    assert_eq!(request.plan_fingerprint, plan_fingerprint);
    assert_eq!(
        credential_secret(&first),
        Some("slack-secret-v1".to_string())
    );
    assert_eq!(
        credential_secret(&second),
        Some("slack-secret-v2".to_string())
    );
    assert_eq!(
        credential_binding_ref_from_effect(&effect),
        Some(AgentCredentialBindingRef::new("cred-slack"))
    );
}

#[tokio::test]
async fn resolved_secret_is_absent_from_durable_serialized_records() {
    let resolver = FakeCredentialResolver::default()
        .with_credential("cred-slack", credential("super-secret-token"));
    let effect = credential_effect("safe-effect", "cred-slack");
    let request = resolution_request(&effect);
    let credential = resolver
        .resolve(request)
        .await
        .expect("credential should resolve");
    let graph_state = AgentGraphRunState::new(
        AgentCompiledPlanId::new("plan-slack"),
        AgentCompiledPlanFingerprint::new("sha256:slack-plan"),
    )
    .node_state(
        AgentGraphNodeState::new(
            AgentCompiledNodeId::new("send-slack"),
            AgentCompiledNodeKind::ToolCall,
            AgentTimestampMillis::new(100),
        )
        .status(AgentGraphNodeStatus::Waiting)
        .wait_reason(AgentGraphWaitReason::Effect)
        .scheduled_effect_id(effect.effect_id.clone()),
    );
    let projection = AgentGraphRunProjection::from_graph_state(&graph_state);

    let effect_json = serde_json::to_string(&effect).expect("effect should serialize");
    let graph_json = serde_json::to_string(&graph_state).expect("graph should serialize");
    let projection_json = serde_json::to_string(&projection).expect("projection should serialize");
    let credential_debug = format!("{credential:?}");

    for serialized in [effect_json, graph_json, projection_json, credential_debug] {
        assert!(
            !serialized.contains("super-secret-token"),
            "secret leaked into serialized/debug output: {serialized}"
        );
    }
    assert!(format!("{credential:?}").contains("<redacted>"));
}

#[derive(Default)]
struct FakeCredentialResolver {
    bindings: Mutex<BTreeMap<String, FakeCredentialResponse>>,
}

impl FakeCredentialResolver {
    fn with_credential(self, binding_ref: &str, credential: AgentEphemeralCredential) -> Self {
        self.bindings.lock().expect("resolver lock").insert(
            binding_ref.to_string(),
            FakeCredentialResponse::Credential(credential),
        );
        self
    }

    fn with_error(self, binding_ref: &str, error: AgentCredentialError) -> Self {
        self.bindings.lock().expect("resolver lock").insert(
            binding_ref.to_string(),
            FakeCredentialResponse::Error(error),
        );
        self
    }

    fn rotate(&self, binding_ref: &str, credential: AgentEphemeralCredential) {
        self.bindings.lock().expect("resolver lock").insert(
            binding_ref.to_string(),
            FakeCredentialResponse::Credential(credential),
        );
    }
}

impl AgentCredentialResolver for FakeCredentialResolver {
    fn resolve<'a>(
        &'a self,
        request: AgentCredentialResolutionRequest,
    ) -> AgentCredentialResolverFuture<'a> {
        let response = self
            .bindings
            .lock()
            .expect("resolver lock")
            .get(request.credential_binding_ref.as_str())
            .cloned();
        Box::pin(async move {
            match response {
                Some(FakeCredentialResponse::Credential(credential)) => Ok(credential),
                Some(FakeCredentialResponse::Error(error)) => Err(error),
                None => Err(AgentCredentialError::MissingBinding {
                    binding_ref: request.credential_binding_ref,
                }),
            }
        })
    }
}

#[derive(Clone)]
enum FakeCredentialResponse {
    Credential(AgentEphemeralCredential),
    Error(AgentCredentialError),
}

fn resolution_request(effect: &AgentEffect) -> AgentCredentialResolutionRequest {
    AgentCredentialResolutionRequest::from_effect(
        Some(AgentTenantId::new("tenant-a")),
        AgentWorkflowId::new("workflow-slack"),
        AgentRunId::new("run-slack"),
        AgentCompiledPlanFingerprint::new("sha256:slack-plan"),
        AgentCompiledNodeId::new("send-slack"),
        AgentCredentialUse::for_effect_kind(effect.kind)
            .expect("tool effect should use credential"),
        effect,
    )
    .expect("effect should contain credential binding ref")
}

fn credential(secret: &str) -> AgentEphemeralCredential {
    AgentEphemeralCredential::bearer_token(secret)
        .expires_at(AgentTimestampMillis::new(10_000))
        .attribute(
            "secret_version",
            if secret.ends_with("v2") { "v2" } else { "v1" },
        )
}

fn credential_secret(credential: &AgentEphemeralCredential) -> Option<String> {
    match credential.material() {
        AgentEphemeralCredentialMaterial::BearerToken { token } => Some(token.clone()),
        AgentEphemeralCredentialMaterial::ApiKey { value, .. }
        | AgentEphemeralCredentialMaterial::Basic {
            password: value, ..
        }
        | AgentEphemeralCredentialMaterial::Custom { value, .. } => Some(value.clone()),
    }
}

fn credential_effect(effect_id: &str, binding_ref: &str) -> AgentEffect {
    let target = AgentEffectTarget {
        target_type: "tool".to_string(),
        name: "slack.chat.postMessage".to_string(),
        address: Some("tool://slack.chat.postMessage".to_string()),
        attributes: BTreeMap::from([
            (
                AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE.to_string(),
                binding_ref.to_string(),
            ),
            ("target_class".to_string(), "messaging".to_string()),
        ]),
    };
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("dedupe-{effect_id}")),
        AgentCausationId::new(format!("cause-{effect_id}")),
        AgentCorrelationId::new(format!("corr-{effect_id}")),
    )
    .telemetry_context(telemetry_context());
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id),
        durability,
        AgentIdempotencyKey::new(format!("idem-{effect_id}")),
        AgentTimestampMillis::new(100),
    )
    .expect("metadata should validate");

    AgentEffectSchedule::new(AgentEffectKind::ToolCall, target, metadata)
        .expect("schedule should validate")
        .payload_ref(artifact("artifact:tool-input"))
        .expected_result_type("tool.result")
        .expect("expected result type should validate")
        .into_effect()
        .expect("effect should validate")
}

fn telemetry_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some("00-00000000000000000000000000000001-0000000000000001-01".to_string()),
        trace_state: None,
        baggage: BTreeMap::from([("tenant_tier".to_string(), "test".to_string())]),
        span_links: Vec::new(),
    }
}

fn artifact(artifact_id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind: ArtifactKind::Input,
        uri: format!("object://bucket/{artifact_id}"),
        checksum: Some("sha256:abc".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("standard".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(100),
        metadata: BTreeMap::new(),
    }
}
