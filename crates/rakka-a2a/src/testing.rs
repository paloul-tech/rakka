//! Fixtures and in-memory helpers for testing A2A adapters.
//!
//! Available to applications through the `testkit` feature and used by this
//! crate's own tests. Nothing here is intended for production wiring.

use std::collections::BTreeMap;

use a2a::{AgentCapabilities, AgentCard, AgentInterface, TRANSPORT_PROTOCOL_HTTP_JSON};
use rakka_agent_workflow::{
    AgentCommandKind, AgentPayloadDescriptor, AgentRunId, AgentRunState, AgentRunStatus,
    AgentStatePayload, AgentStep, AgentStepId, AgentStepKind, AgentTenantId, AgentTimestampMillis,
    AgentWorkflow, AgentWorkflowId, StateSchemaVersion, WorkflowDefinitionVersion,
};

/// Stable workflow id used by [`fixture_workflow`].
pub const FIXTURE_WORKFLOW_ID: &str = "workflow-rakka-a2a-fixture";
/// Stable workflow type used by [`fixture_workflow`].
pub const FIXTURE_WORKFLOW_TYPE: &str = "rakka-a2a-fixture";
/// Stable definition version used by [`fixture_workflow`].
pub const FIXTURE_DEFINITION_VERSION: &str = "v1";

/// A minimal single-step workflow definition for adapter tests.
#[must_use]
pub fn fixture_workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new(FIXTURE_WORKFLOW_ID),
        workflow_type: FIXTURE_WORKFLOW_TYPE.to_string(),
        definition_version: WorkflowDefinitionVersion::new(FIXTURE_DEFINITION_VERSION),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Rakka A2A fixture workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![
            AgentCommandKind::StartRun.type_name().to_string(),
            AgentCommandKind::SubmitSignal {
                signal_type: "a2a.message".to_string(),
            }
            .type_name()
            .to_string(),
            AgentCommandKind::CancelRun.type_name().to_string(),
        ],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("receive-a2a-message"),
            kind: AgentStepKind::Planner,
            display_name: Some("Receive A2A message".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(30_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("a2a.message").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            FIXTURE_WORKFLOW_TYPE.to_string(),
        )]),
    }
}

/// A minimal agent card for adapter tests.
#[must_use]
pub fn fixture_agent_card() -> AgentCard {
    AgentCard {
        name: "Rakka A2A fixture agent".to_string(),
        description: "In-memory A2A adapter fixture for tests.".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        supported_interfaces: vec![AgentInterface::new(
            "http://127.0.0.1:0/a2a",
            TRANSPORT_PROTOCOL_HTTP_JSON,
        )],
        capabilities: AgentCapabilities {
            streaming: Some(true),
            push_notifications: Some(false),
            extensions: None,
            extended_agent_card: Some(false),
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: Vec::new(),
        provider: None,
        documentation_url: None,
        icon_url: None,
        security_schemes: None,
        security_requirements: None,
        signatures: None,
    }
}

/// A minimal durable run state for projection tests.
#[must_use]
pub fn fixture_run_state(run_id: &str, status: AgentRunStatus) -> AgentRunState {
    AgentRunState {
        run_id: AgentRunId::new(run_id),
        workflow_id: AgentWorkflowId::new(FIXTURE_WORKFLOW_ID),
        tenant: Some(AgentTenantId::new("tenant-a")),
        definition_version: WorkflowDefinitionVersion::new(FIXTURE_DEFINITION_VERSION),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: None,
        status,
        current_step_id: None,
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(10),
        updated_at: AgentTimestampMillis::new(20),
        completed_at: None,
    }
}
