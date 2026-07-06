//! Demo workflow definition advertised by the clustered A2A agent card.

use std::collections::BTreeMap;

use rakka::agent_workflow::{
    AgentCommandKind, AgentPayloadDescriptor, AgentRunStatus, AgentStep, AgentStepId,
    AgentStepKind, AgentWorkflow, AgentWorkflowId, StateSchemaVersion, WorkflowDefinitionVersion,
};

use crate::support::WORKFLOW_TYPE;

const WORKFLOW_ID: &str = "workflow-a2a-phase-2-demo";
const DEFINITION_VERSION: &str = "v1";

/// The single demo workflow definition hosted by this example.
#[must_use]
pub fn demo_workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new(WORKFLOW_ID),
        workflow_type: WORKFLOW_TYPE.to_string(),
        definition_version: WorkflowDefinitionVersion::new(DEFINITION_VERSION),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("A2A clustered demo workflow".to_string()),
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
            WORKFLOW_TYPE.to_string(),
        )]),
    }
}
