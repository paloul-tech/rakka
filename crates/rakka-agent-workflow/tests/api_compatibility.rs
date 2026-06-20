//! API review and rolling-update compatibility tests for agent workflows.

use std::any::type_name;
use std::collections::BTreeMap;

use rakka_agent_workflow::{
    validate_agent_telemetry_context, validate_command, validate_effect_schedule, AgentAuditEvent,
    AgentAuditEventId, AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind,
    AgentCommandMetadata, AgentCorrelationId, AgentDeduplicationKey, AgentDispatchEntry,
    AgentDispatchId, AgentDispatchIndexEntry, AgentDispatchLease, AgentDispatchQuery,
    AgentDispatchStatus, AgentDispatchTargetClass, AgentDispatcherWorkerId, AgentDueEffect,
    AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectTarget, AgentFacadeResult, AgentIdempotencyKey,
    AgentMigrationDecision, AgentRunId, AgentRunIndexEntry, AgentRunQueryWaitingReason,
    AgentRunState, AgentRunStatus, AgentSpanLink, AgentStatePayload, AgentStepId,
    AgentTelemetryContext, AgentTenantId, AgentTimerEntry, AgentTimerId, AgentTimerIndexEntry,
    AgentTimerPolicy, AgentTimerQuery, AgentTimerStatus, AgentTimestampMillis, AgentWorkflowId,
    AgentWorkflowIndexSchemaVersion, AgentWorkflowMigrationPolicy, AgentWorkflowQueryIndex,
    AgentWorkflowRunQuery, AgentWorkflowShardOwnership, ArtifactKind, ArtifactRef, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, InMemoryAgentWorkflowQueryIndex,
    PrincipalRef, RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion,
    CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION,
};
use serde_json::json;

const CARGO_TOML: &str = include_str!("../Cargo.toml");
const KUBERNETES_REFERENCE_TOPOLOGY: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-reference-topology.yaml");
const ROOT_TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[test]
fn public_root_exports_cover_stable_default_api_surface() {
    let exported_types = [
        type_name::<AgentCommand>(),
        type_name::<AgentEffectSchedule>(),
        type_name::<AgentRunState>(),
        type_name::<AgentTelemetryContext>(),
        type_name::<AgentAuditEvent>(),
        type_name::<AgentRunIndexEntry>(),
        type_name::<AgentTimerIndexEntry>(),
        type_name::<AgentDispatchIndexEntry>(),
        type_name::<AgentWorkflowMigrationPolicy>(),
        type_name::<InMemoryAgentWorkflowQueryIndex>(),
    ];
    for exported in exported_types {
        assert!(
            exported.starts_with("rakka_agent_workflow::"),
            "public type should be re-exported from crate root: {exported}"
        );
    }

    let _validate_command: fn(&AgentCommand) -> AgentFacadeResult<()> = validate_command;
    let _validate_effect_schedule: fn(&AgentEffectSchedule) -> AgentFacadeResult<()> =
        validate_effect_schedule;
    let _query = AgentWorkflowRunQuery::new()
        .waiting_reason(AgentRunQueryWaitingReason::Timer)
        .limit(10);
    let _timer_query = AgentTimerQuery::new()
        .status(AgentTimerStatus::Pending)
        .limit(10);
    let _dispatch_query = AgentDispatchQuery::new()
        .status(AgentDispatchStatus::Claimed)
        .limit(10);
    let _due_effect_type = type_name::<AgentDueEffect>();
}

#[cfg(feature = "k8s")]
#[test]
fn public_root_exports_cover_kubernetes_api_surface_when_enabled() {
    use rakka_agent_workflow::{
        AgentWorkflowKubernetesStartup, AgentWorkflowStartupStep,
        DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS,
    };
    use rakka_k8s::KubernetesNodeHealth;

    let _startup_type = type_name::<AgentWorkflowKubernetesStartup>();
    assert!(DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS.contains(&AgentWorkflowStartupStep::Postgres));
    let _constructor: fn(KubernetesNodeHealth) -> AgentWorkflowKubernetesStartup =
        AgentWorkflowKubernetesStartup::new;
}

#[test]
fn command_and_effect_wire_contract_accepts_additive_fields_and_rejects_breaking_removals() {
    let command = start_command("command-api-compat", telemetry_context());
    let mut command_json = serde_json::to_value(&command).expect("command should serialize");
    assert_eq!(command_json["kind"]["type"], "start-run");
    assert_eq!(
        command_json["metadata"]["command_id"],
        json!("command-api-compat")
    );
    command_json["future_additive_field"] = json!("ignored-by-current-reader");
    command_json["metadata"]["future_metadata"] = json!({"next": true});
    let decoded: AgentCommand =
        serde_json::from_value(command_json.clone()).expect("additive command should deserialize");
    assert_eq!(decoded.kind.type_name(), "StartRun");
    decoded.validate().expect("decoded command should validate");

    let mut missing_command_id = command_json;
    missing_command_id["metadata"]
        .as_object_mut()
        .expect("metadata should be object")
        .remove("command_id");
    assert!(
        serde_json::from_value::<AgentCommand>(missing_command_id).is_err(),
        "removing command_id is a breaking command schema change"
    );

    let effect = effect_schedule("effect-api-compat", telemetry_context())
        .into_effect()
        .expect("effect should convert to persistable contract");
    let mut effect_json = serde_json::to_value(&effect).expect("effect should serialize");
    assert_eq!(effect_json["kind"], json!("model-call"));
    assert_eq!(effect_json["target"]["target_type"], json!("model"));
    effect_json["future_additive_field"] = json!({"compatible": true});
    effect_json["telemetry_context"]["future_trace_field"] = json!("ignored");
    let decoded: AgentEffect =
        serde_json::from_value(effect_json.clone()).expect("additive effect should deserialize");
    assert_eq!(decoded.effect_id, AgentEffectId::new("effect-api-compat"));
    assert_eq!(decoded.telemetry_context, telemetry_context());

    let mut missing_idempotency = effect_json;
    missing_idempotency
        .as_object_mut()
        .expect("effect should be object")
        .remove("idempotency_key");
    assert!(
        serde_json::from_value::<AgentEffect>(missing_idempotency).is_err(),
        "removing idempotency_key is a breaking effect schema change"
    );
}

#[tokio::test]
async fn durable_state_trace_and_query_contracts_are_versioned_for_rolling_updates() {
    let policy = AgentWorkflowMigrationPolicy::n_plus_one(
        StateSchemaVersion::new(2),
        AgentWorkflowIndexSchemaVersion::new(2),
    )
    .support_definition_version("v1")
    .support_definition_version("v2");
    let previous = run_state("run-api-compat-v1", "v1", 1, AgentRunStatus::Running);
    let current = run_state(
        "run-api-compat-v2",
        "v2",
        2,
        AgentRunStatus::WaitingForTimer,
    );

    assert!(matches!(
        policy.assess_run_state(&previous).decision,
        AgentMigrationDecision::Current | AgentMigrationDecision::CompatiblePrevious
    ));
    assert!(matches!(
        policy.assess_run_state(&current).decision,
        AgentMigrationDecision::Current | AgentMigrationDecision::CompatiblePrevious
    ));
    assert!(matches!(
        policy
            .assess_index_schema(CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION)
            .decision,
        AgentMigrationDecision::Current | AgentMigrationDecision::CompatiblePrevious
    ));

    let mut run_json = serde_json::to_value(&current).expect("run should serialize");
    assert_eq!(run_json["definition_version"], json!("v2"));
    assert_eq!(run_json["state_schema_version"], json!(2));
    run_json["future_state_field"] = json!({"reader": "n"});
    let decoded_run: AgentRunState =
        serde_json::from_value(run_json.clone()).expect("additive run state should deserialize");
    assert_eq!(
        decoded_run.definition_version,
        WorkflowDefinitionVersion::new("v2")
    );
    assert_eq!(decoded_run.state_schema_version, StateSchemaVersion::new(2));

    let mut missing_schema = run_json;
    missing_schema
        .as_object_mut()
        .expect("run should be object")
        .remove("state_schema_version");
    assert!(
        serde_json::from_value::<AgentRunState>(missing_schema).is_err(),
        "removing state_schema_version is a breaking durable-state change"
    );

    let mut trace_json = serde_json::to_value(telemetry_context()).expect("trace should serialize");
    trace_json["future_trace_field"] = json!("ignored");
    trace_json["span_links"][0]["future_link_field"] = json!("ignored");
    let decoded_trace: AgentTelemetryContext =
        serde_json::from_value(trace_json).expect("additive trace context should deserialize");
    validate_agent_telemetry_context(&decoded_trace).expect("trace context should validate");
    assert_eq!(decoded_trace.span_links.len(), 1);

    let mut run_projection =
        AgentRunIndexEntry::from_run_state(&current, "compat-workflow").namespace("prod");
    run_projection =
        run_projection.shard_ownership(AgentWorkflowShardOwnership::new("AgentRun", "7", "node-a"));
    let timer_projection =
        AgentTimerIndexEntry::from_timer_entry(&timer_entry("timer-api-compat", &current.run_id))
            .namespace("prod");
    let dispatch_projection = AgentDispatchIndexEntry::from_dispatch_entry(&dispatch_entry(
        "dispatch-api-compat",
        &current.run_id,
    ));

    let mut projection_json =
        serde_json::to_value(&run_projection).expect("projection should serialize");
    projection_json["future_projection_field"] = json!("ignored");
    let decoded_projection: AgentRunIndexEntry = serde_json::from_value(projection_json)
        .expect("additive run projection should deserialize");
    assert_eq!(decoded_projection.workflow_type, "compat-workflow");

    let mut index = InMemoryAgentWorkflowQueryIndex::new();
    index
        .upsert_run(decoded_projection)
        .await
        .expect("run should upsert");
    index
        .upsert_timer(timer_projection)
        .await
        .expect("timer should upsert");
    index
        .upsert_dispatch(dispatch_projection)
        .await
        .expect("dispatch should upsert");
}

#[tokio::test]
async fn query_index_compatibility_accepts_additive_projection_shapes() {
    let run = run_state(
        "run-query-api-compat",
        "v2",
        2,
        AgentRunStatus::WaitingForTimer,
    );
    let run_projection = AgentRunIndexEntry::from_run_state(&run, "compat-workflow")
        .namespace("prod")
        .shard_ownership(AgentWorkflowShardOwnership::new("AgentRun", "11", "node-b"));
    let timer_projection =
        AgentTimerIndexEntry::from_timer_entry(&timer_entry("timer-query-api-compat", &run.run_id))
            .namespace("prod");
    let dispatch_projection = AgentDispatchIndexEntry::from_dispatch_entry(&dispatch_entry(
        "dispatch-query-api-compat",
        &run.run_id,
    ));

    let mut index = InMemoryAgentWorkflowQueryIndex::new();
    index
        .upsert_run(run_projection)
        .await
        .expect("run projection should upsert");
    index
        .upsert_timer(timer_projection)
        .await
        .expect("timer projection should upsert");
    index
        .upsert_dispatch(dispatch_projection)
        .await
        .expect("dispatch projection should upsert");

    let due_runs = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .workflow_type("compat-workflow")
                .namespace("prod")
                .due_timer_at_or_before(ts(2_000))
                .limit(10),
        )
        .await
        .expect("run query should remain compatible");
    assert_eq!(run_ids(&due_runs), vec!["run-query-api-compat"]);

    let due_timers = index
        .query_timers(
            AgentTimerQuery::new()
                .status(AgentTimerStatus::Pending)
                .due_at_or_before(ts(2_000)),
        )
        .await
        .expect("timer query should remain compatible");
    assert_eq!(
        due_timers[0].timer_id,
        AgentTimerId::new("timer-query-api-compat")
    );

    let stuck_dispatches = index
        .query_dispatches(
            AgentDispatchQuery::new()
                .status(AgentDispatchStatus::Claimed)
                .stuck_at_or_before(ts(2_000)),
        )
        .await
        .expect("dispatch query should remain compatible");
    assert_eq!(
        stuck_dispatches[0].dispatch_id,
        AgentDispatchId::new("dispatch-query-api-compat")
    );
}

#[test]
fn feature_flags_are_additive_and_match_api_review_boundaries() {
    assert!(CARGO_TOML.contains("[features]\ndefault = []"));
    for expected in [
        "grpc = [\"dep:rakka-grpc\"]",
        "http = [\"dep:rakka-http\"]",
        "k8s = [\"dep:rakka-k8s\"]",
        "otel = []",
        "postgres = [\"dep:rakka-persistence-postgres\", \"dep:rakka-sharding-postgres\", \"dep:tokio-postgres\"]",
        "process-tools = [\"dep:rakka-process\"]",
        "sharding = [\"dep:rakka-sharding\"]",
        "testkit = [\"dep:rakka-testkit\"]",
    ] {
        assert!(CARGO_TOML.contains(expected), "missing feature line {expected}");
    }
    assert!(CARGO_TOML.contains("rakka-core = { path = \"../rakka-core\", version = \"0.1.0\" }"));
    assert!(CARGO_TOML
        .contains("rakka-workflow = { path = \"../rakka-workflow\", version = \"0.1.0\" }"));
}

#[test]
fn kubernetes_reference_manifest_carries_agent_workflow_compatibility_contract() {
    for expected in [
        "namespace: rakka-system",
        "RAKKA_AGENT_WORKFLOW_CURRENT_STATE_SCHEMA_VERSION: \"1\"",
        "RAKKA_AGENT_WORKFLOW_CURRENT_INDEX_SCHEMA_VERSION: \"1\"",
        "RAKKA_AGENT_WORKFLOW_COMPAT_POLICY: n-to-n-plus-one",
        "RAKKA_AGENT_WORKFLOW_EXPECTED_DEFINITION_VERSIONS: v1",
        "RAKKA_MANIFEST_VERSION: \"1.0\"",
        "RAKKA_GENERATED_API_VERSION: \"1.0\"",
        "RAKKA_PROTOCOL_VERSION: \"1.0\"",
        "RAKKA_COMPAT_MIN: \"1.0\"",
        "RAKKA_COMPAT_MAX: \"1.1\"",
        "rakka.rs/agent-workflow-spec-version: \"1.0\"",
        "rakka.rs/compat-policy: n-to-n-plus-one",
        "maxUnavailable: 0",
        "RAKKA_REQUIRED_SERVICES: telemetry-resource,otlp-exporter,postgres,durable-state,query-index,artifact-store,actor-system,remoting,sharding,workflow-registry,operational-snapshots",
    ] {
        assert!(
            KUBERNETES_REFERENCE_TOPOLOGY.contains(expected),
            "reference topology missing compatibility marker {expected}"
        );
    }
}

fn start_command(command_id: &str, telemetry_context: AgentTelemetryContext) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            workflow_id(),
            run_id("run-api-compat"),
            AgentCommandId::new(command_id),
            durability(command_id, telemetry_context),
            tenant_id(),
            ts(1_000),
        )
        .expect("command metadata should validate"),
    )
    .expect("command should validate")
}

fn effect_schedule(
    effect_id: &str,
    telemetry_context: AgentTelemetryContext,
) -> AgentEffectSchedule {
    AgentEffectSchedule::new(
        AgentEffectKind::ModelCall,
        target("model", "primary-chat"),
        AgentEffectMetadata::new(
            AgentEffectId::new(effect_id),
            durability(effect_id, telemetry_context),
            AgentIdempotencyKey::new(format!("{effect_id}:idempotency")),
            ts(1_000),
        )
        .expect("effect metadata should validate")
        .due_at(ts(1_100))
        .timeout_ms(30_000),
    )
    .expect("effect schedule should validate")
    .expected_result_type("model.response")
    .expect("expected result type should validate")
}

fn run_state(
    run_id: &str,
    definition_version: &str,
    state_schema_version: u32,
    status: AgentRunStatus,
) -> AgentRunState {
    AgentRunState {
        run_id: AgentRunId::new(run_id),
        workflow_id: workflow_id(),
        tenant: Some(tenant_id()),
        definition_version: WorkflowDefinitionVersion::new(definition_version),
        state_schema_version: StateSchemaVersion::new(state_schema_version),
        status,
        current_step_id: Some(AgentStepId::new("step-review")),
        current_attempt: 1,
        inputs_ref: Some(artifact("artifact:input", ArtifactKind::Input)),
        state_payload: AgentStatePayload::Empty,
        checkpoints: vec![checkpoint()],
        pending_effects: vec![effect_schedule("effect-run-state", telemetry_context())
            .into_effect()
            .expect("effect should convert")],
        pending_human_checkpoint: (status == AgentRunStatus::WaitingForHuman)
            .then(|| HumanCheckpointId::new("checkpoint-review")),
        cancellation: None,
        created_at: ts(1_000),
        updated_at: ts(1_200),
        completed_at: matches!(
            status,
            AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
        )
        .then(|| ts(1_200)),
    }
}

fn timer_entry(timer_id: &str, run_id: &AgentRunId) -> AgentTimerEntry {
    AgentTimerEntry::new(
        AgentTimerId::new(timer_id),
        workflow_id(),
        run_id.clone(),
        tenant_id(),
        ts(1_500),
        durability(timer_id, telemetry_context()),
        ts(1_000),
    )
    .expect("timer should validate")
    .policy(AgentTimerPolicy::new().policy_name("api-compat-timeout"))
    .expect("timer policy should validate")
}

fn dispatch_entry(dispatch_id: &str, run_id: &AgentRunId) -> AgentDispatchEntry {
    AgentDispatchEntry {
        dispatch_id: AgentDispatchId::new(dispatch_id),
        workflow_id: Some(workflow_id()),
        run_id: run_id.clone(),
        effect_id: AgentEffectId::new(format!("{dispatch_id}:effect")),
        effect_kind: AgentEffectKind::ToolCall,
        target: target("tool", "api-review-tool"),
        target_class: AgentDispatchTargetClass::Tool,
        due_at: ts(1_100),
        status: AgentDispatchStatus::Claimed,
        lease: Some(AgentDispatchLease {
            worker_id: AgentDispatcherWorkerId::new("worker-api-compat"),
            fencing_token: 1,
            claimed_at: ts(1_100),
            lease_expires_at: ts(1_400),
        }),
        last_fencing_token: 1,
        attempts: 1,
        last_error_code: None,
        created_at: ts(1_000),
        updated_at: ts(1_100),
        completed_at: None,
        exhausted_at: None,
        attributes: attributes([("redaction", "reference-only")]),
    }
}

fn checkpoint() -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new("checkpoint-review"),
        status: HumanCheckpointStatus::Open,
        summary: "Review compatibility boundary".to_string(),
        available_decisions: vec![HumanDecisionOption {
            value: "approve".to_string(),
            label: "Approve".to_string(),
            requires_comment: false,
        }],
        required_roles: vec!["reviewer".to_string()],
        due_at: Some(ts(2_000)),
        escalation_target: Some("workflow-ops".to_string()),
        context_artifacts: vec![artifact("artifact:context", ArtifactKind::Prompt)],
        created_by: Some(principal()),
        resolved_by: None,
        created_at: ts(1_000),
        resolved_at: None,
        audit_event_ids: vec![AgentAuditEventId::new("audit-compat")],
    }
}

fn telemetry_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(ROOT_TRACE_PARENT.to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: attributes([("tenant_tier", "internal")]),
        span_links: vec![AgentSpanLink {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            trace_state: Some("vendor=value".to_string()),
            attributes: attributes([("resume_kind", "compatibility")]),
        }],
    }
}

fn durability(id: &str, telemetry_context: AgentTelemetryContext) -> AgentDurabilityMetadata {
    AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("{id}:dedupe")),
        AgentCausationId::new(format!("{id}:cause")),
        AgentCorrelationId::new("corr-api-compat"),
    )
    .telemetry_context(telemetry_context)
}

fn artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind,
        uri: format!("object://agent-workflow/{artifact_id}"),
        checksum: Some("sha256:compat".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(64),
        retention_class: Some("standard".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: ts(1_000),
        metadata: attributes([("schema", "compat")]),
    }
}

fn target(target_type: &str, name: &str) -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: target_type.to_string(),
        name: name.to_string(),
        address: Some(format!("{target_type}://{name}")),
        attributes: attributes([("target_class", target_type)]),
    }
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "service".to_string(),
        principal_id: "compatibility-suite".to_string(),
        display_name: Some("Compatibility Suite".to_string()),
    }
}

fn workflow_id() -> AgentWorkflowId {
    AgentWorkflowId::new("workflow-api-compat")
}

fn run_id(value: &str) -> AgentRunId {
    AgentRunId::new(value)
}

fn tenant_id() -> AgentTenantId {
    AgentTenantId::new("tenant-api-compat")
}

fn run_ids(entries: &[AgentRunIndexEntry]) -> Vec<&str> {
    entries.iter().map(|entry| entry.run_id.as_str()).collect()
}

fn attributes<const N: usize>(items: [(&str, &str); N]) -> BTreeMap<String, String> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

const fn ts(value: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(value)
}
