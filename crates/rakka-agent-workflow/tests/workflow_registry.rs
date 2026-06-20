//! Agent workflow definition registry tests.

use rakka_agent_workflow::{
    AgentPayload, AgentPayloadDescriptor, AgentRunStatus, AgentStep, AgentStepId, AgentStepKind,
    AgentWorkflow, AgentWorkflowId, AgentWorkflowRegistry, AgentWorkflowRegistryError,
    StateSchemaVersion, WorkflowDefinitionVersion,
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
