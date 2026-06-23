//! Agent workflow definition registry tests.

use rakka_agent_workflow::{
    AgentCompiledExecutionPlan, AgentCompiledNodeKind, AgentCompiledPlanCompatibility,
    AgentCompiledPlanEdge, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPlanSchemaVersion,
    AgentCompiledPortDirection, AgentCompiledWorkflowRegistration, AgentPayload,
    AgentPayloadDescriptor, AgentRunStatus, AgentStep, AgentStepId, AgentStepKind, AgentWorkflow,
    AgentWorkflowId, AgentWorkflowRegistry, AgentWorkflowRegistryError, StateSchemaVersion,
    WorkflowDefinitionVersion, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};

#[test]
fn workflow_registry_registers_and_queries_by_type_and_version() {
    let mut registry = AgentWorkflowRegistry::new();
    let workflow = workflow("research", "v1");

    let registered = registry
        .register(workflow.clone())
        .expect("workflow should register");

    assert_eq!(registered.workflow_id, workflow.workflow_id);
    assert!(registry.contains("research", &WorkflowDefinitionVersion::new("v1")));
    assert_eq!(
        registry
            .get("research", &WorkflowDefinitionVersion::new("v1"))
            .expect("workflow should be queryable")
            .definition_version,
        WorkflowDefinitionVersion::new("v1")
    );
    assert_eq!(registry.definitions_for_type("research").len(), 1);
    assert_eq!(registry.len(), 1);
}

#[test]
fn workflow_registry_rejects_duplicate_type_and_version() {
    let mut registry = AgentWorkflowRegistry::new();
    registry
        .register(workflow("research", "v1"))
        .expect("first workflow should register");

    let error = registry
        .register(workflow("research", "v1"))
        .expect_err("duplicate workflow should fail");

    match error {
        AgentWorkflowRegistryError::DuplicateDefinition {
            workflow_type,
            definition_version,
            ..
        } => {
            assert_eq!(workflow_type, "research");
            assert_eq!(definition_version, WorkflowDefinitionVersion::new("v1"));
        }
        other => panic!("expected duplicate definition error, got {other:?}"),
    }
}

#[test]
fn workflow_registry_validates_required_metadata() {
    let mut registry = AgentWorkflowRegistry::new();
    let mut workflow = workflow("research", "v1");
    workflow.command_types.clear();

    let error = registry
        .register(workflow)
        .expect_err("workflow with no commands should fail");

    assert!(error.to_string().contains("at least one command type"));
}

#[test]
fn workflow_registry_validates_duplicate_step_ids() {
    let mut registry = AgentWorkflowRegistry::new();
    let mut workflow = workflow("research", "v1");
    workflow.steps.push(workflow.steps[0].clone());

    let error = registry
        .register(workflow)
        .expect_err("workflow with duplicate steps should fail");

    assert!(error.to_string().contains("duplicate step id"));
}

#[test]
fn workflow_registry_registers_compiled_plan_pair() {
    let mut registry = AgentWorkflowRegistry::new();
    let workflow = workflow("research", "v1");
    let plan = compiled_plan("research", "v1");

    let registered = registry
        .register_compiled(workflow.clone(), plan.clone())
        .expect("compiled workflow should register");

    assert_eq!(registered.workflow.workflow_id, workflow.workflow_id);
    assert_eq!(registered.plan.plan_id, plan.plan_id);
    assert_eq!(registered.plan_fingerprint(), &plan.plan_fingerprint);
    assert_eq!(
        registered.plan_schema_version(),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION
    );
    assert!(registry.contains("research", &WorkflowDefinitionVersion::new("v1")));
    assert!(registry.contains_compiled("research", &WorkflowDefinitionVersion::new("v1")));
    assert_eq!(
        registry
            .get_compiled("research", &WorkflowDefinitionVersion::new("v1"))
            .expect("compiled workflow should be queryable")
            .plan_id(),
        &AgentCompiledPlanId::new("plan-research-v1")
    );
    assert_eq!(registry.compiled_len(), 1);
}

#[test]
fn workflow_registry_attaches_compiled_plan_to_existing_definition() {
    let mut registry = AgentWorkflowRegistry::new();
    registry
        .register(workflow("research", "v1"))
        .expect("workflow should register");

    let registered = registry
        .register_compiled_plan(compiled_plan("research", "v1"))
        .expect("compiled plan should attach to existing workflow");

    assert_eq!(registered.workflow_type(), "research");
    assert_eq!(
        registered.definition_version(),
        &WorkflowDefinitionVersion::new("v1")
    );
    assert_eq!(registry.len(), 1);
    assert_eq!(registry.compiled_len(), 1);
}

#[test]
fn workflow_registry_rejects_duplicate_compiled_type_and_version() {
    let mut registry = AgentWorkflowRegistry::new();
    registry
        .register_compiled(workflow("research", "v1"), compiled_plan("research", "v1"))
        .expect("first compiled workflow should register");

    let error = registry
        .register_compiled(workflow("research", "v1"), compiled_plan("research", "v1"))
        .expect_err("duplicate compiled workflow should fail");

    match error {
        AgentWorkflowRegistryError::DuplicateCompiledPlan {
            workflow_type,
            definition_version,
            ..
        } => {
            assert_eq!(workflow_type, "research");
            assert_eq!(definition_version, WorkflowDefinitionVersion::new("v1"));
        }
        other => panic!("expected duplicate compiled plan error, got {other:?}"),
    }
}

#[test]
fn workflow_registry_accepts_multiple_compiled_versions() {
    let mut registry = AgentWorkflowRegistry::new();
    registry
        .register_compiled(workflow("research", "v1"), compiled_plan("research", "v1"))
        .expect("v1 compiled workflow should register");
    registry
        .register_compiled(workflow("research", "v2"), compiled_plan("research", "v2"))
        .expect("v2 compiled workflow should register");

    assert_eq!(registry.definitions_for_type("research").len(), 2);
    assert_eq!(
        registry.compiled_registrations_for_type("research").len(),
        2
    );
    assert!(registry.contains_compiled("research", &WorkflowDefinitionVersion::new("v1")));
    assert!(registry.contains_compiled("research", &WorkflowDefinitionVersion::new("v2")));
}

#[test]
fn workflow_registry_rejects_incompatible_compiled_schema_version() {
    let mut registry = AgentWorkflowRegistry::new();
    let mut plan = compiled_plan("research", "v1");
    plan.plan_schema_version =
        AgentCompiledPlanSchemaVersion::new(CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION.get() + 1);

    let error = registry
        .register_compiled(workflow("research", "v1"), plan)
        .expect_err("unsupported schema should fail");

    assert!(matches!(
        error,
        AgentWorkflowRegistryError::IncompatibleCompiledPlan { .. }
    ));
    assert!(error.to_string().contains("schema version"));
}

#[test]
fn workflow_registry_rejects_mismatched_compiled_plan_metadata() {
    let mut registry = AgentWorkflowRegistry::new();
    let plan = compiled_plan("other", "v1");

    let error = registry
        .register_compiled(workflow("research", "v1"), plan)
        .expect_err("mismatched workflow metadata should fail");

    assert!(matches!(
        error,
        AgentWorkflowRegistryError::IncompatibleCompiledPlan { .. }
    ));
    assert!(error.to_string().contains("workflow id mismatch"));
}

#[test]
fn compiled_registration_preserves_fingerprint_across_serde_round_trip() {
    let mut registry = AgentWorkflowRegistry::new();
    let registration = registry
        .register_compiled(workflow("research", "v1"), compiled_plan("research", "v1"))
        .expect("compiled workflow should register")
        .clone();

    let json =
        serde_json::to_string(&registration).expect("compiled registration should serialize");
    let decoded: AgentCompiledWorkflowRegistration =
        serde_json::from_str(&json).expect("compiled registration should deserialize");

    assert_eq!(
        decoded.plan_fingerprint(),
        &AgentCompiledPlanFingerprint::new("sha256:research-v1")
    );
    assert_eq!(
        decoded.plan_schema_version(),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION
    );
}

#[test]
fn payload_descriptors_support_traits_and_schema_refs() {
    struct ResearchInput;
    impl AgentPayload for ResearchInput {}

    let typed = ResearchInput::payload_descriptor().content_type("application/json");
    let opaque = AgentPayloadDescriptor::new("opaque.bytes")
        .content_type("application/octet-stream")
        .attribute("owner", "application");

    assert!(typed.type_name.contains("ResearchInput"));
    assert_eq!(typed.content_type.as_deref(), Some("application/json"));
    assert_eq!(
        opaque.attributes.get("owner").map(String::as_str),
        Some("application")
    );
}

fn compiled_plan(workflow_type: &str, version: &str) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Output,
            format!("{workflow_type}.input"),
        ),
    );
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            format!("{workflow_type}.input"),
        ));

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new(format!("plan-{workflow_type}-{version}")),
        AgentWorkflowId::new(format!("{workflow_type}-{version}")),
        workflow_type,
        WorkflowDefinitionVersion::new(version),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new(format!("sha256:{workflow_type}-{version}")),
    )
    .entry_node("input")
    .node(input)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-terminal",
        "input",
        "payload",
        "terminal",
        "result",
    ))
    .with_compatibility(
        AgentCompiledPlanCompatibility::new()
            .min_runtime_version("0.1.0")
            .required_capability("compiled-graph-v1"),
    )
}

fn workflow(workflow_type: &str, version: &str) -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new(format!("{workflow_type}-{version}")),
        workflow_type: workflow_type.to_string(),
        definition_version: WorkflowDefinitionVersion::new(version),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some(format!("{workflow_type} {version}")),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec!["StartRun".to_string(), "HumanDecisionSubmitted".to_string()],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: Default::default(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("research.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: Default::default(),
    }
}

trait CompiledPlanTestExt {
    fn with_compatibility(self, compatibility: AgentCompiledPlanCompatibility) -> Self;
}

impl CompiledPlanTestExt for AgentCompiledExecutionPlan {
    fn with_compatibility(mut self, compatibility: AgentCompiledPlanCompatibility) -> Self {
        self.compatibility = compatibility;
        self
    }
}
