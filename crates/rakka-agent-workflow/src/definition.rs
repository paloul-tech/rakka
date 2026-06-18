//! Agent workflow definition registration API.
//!
//! ```
//! # use rakka_agent_workflow::{
//! #     AgentPayloadDescriptor, AgentRunStatus, AgentStep, AgentStepKind,
//! #     AgentWorkflow, AgentWorkflowId, AgentWorkflowRegistry, StateSchemaVersion,
//! #     WorkflowDefinitionVersion,
//! # };
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! struct ResearchInput;
//!
//! let workflow = AgentWorkflow {
//!     workflow_id: AgentWorkflowId::new("research"),
//!     workflow_type: "research".to_string(),
//!     definition_version: WorkflowDefinitionVersion::new("v1"),
//!     state_schema_version: StateSchemaVersion::new(1),
//!     display_name: Some("Research".to_string()),
//!     status_labels: vec![AgentRunStatus::Accepted.as_label().to_string()],
//!     command_types: vec!["StartRun".to_string()],
//!     steps: vec![AgentStep {
//!         step_id: "plan".into(),
//!         kind: AgentStepKind::Planner,
//!         display_name: Some("Plan".to_string()),
//!         next_step_ids: Vec::new(),
//!         timeout_ms: None,
//!         config_ref: None,
//!         observability_labels: Default::default(),
//!     }],
//!     payload_types: vec![AgentPayloadDescriptor::for_type::<ResearchInput>()],
//!     retry_policy_ref: None,
//!     timeout_policy_ref: None,
//!     approval_policy_ref: None,
//!     observability_labels: Default::default(),
//! };
//!
//! let mut registry = AgentWorkflowRegistry::new();
//! registry.register(workflow)?;
//! assert!(registry
//!     .get("research", &WorkflowDefinitionVersion::new("v1"))
//!     .is_some());
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{AgentPayloadDescriptor, AgentWorkflow, AgentWorkflowId, WorkflowDefinitionVersion};

/// Application payload marker for typed workflow payload descriptors.
pub trait AgentPayload: 'static {
    /// Returns the payload descriptor for this application-owned type.
    #[must_use]
    fn payload_descriptor() -> AgentPayloadDescriptor
    where
        Self: Sized,
    {
        AgentPayloadDescriptor::for_type::<Self>()
    }
}

/// Registry key for one workflow type and definition version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentWorkflowKey {
    workflow_type: String,
    definition_version: WorkflowDefinitionVersion,
}

impl AgentWorkflowKey {
    /// Creates a registry key.
    #[must_use]
    pub fn new(
        workflow_type: impl Into<String>,
        definition_version: WorkflowDefinitionVersion,
    ) -> Self {
        Self {
            workflow_type: workflow_type.into(),
            definition_version,
        }
    }

    /// Workflow type.
    #[must_use]
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// Workflow definition version.
    #[must_use]
    pub const fn definition_version(&self) -> &WorkflowDefinitionVersion {
        &self.definition_version
    }
}

/// Errors returned by workflow definition registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentWorkflowRegistryError {
    /// Workflow definition failed validation.
    InvalidDefinition {
        /// Workflow id associated with the rejected definition.
        workflow_id: Option<AgentWorkflowId>,
        /// Stable reason.
        reason: String,
    },
    /// Workflow type and version were already registered.
    DuplicateDefinition {
        /// Workflow type.
        workflow_type: String,
        /// Definition version.
        definition_version: WorkflowDefinitionVersion,
        /// Existing workflow id.
        existing_workflow_id: AgentWorkflowId,
        /// Attempted workflow id.
        attempted_workflow_id: AgentWorkflowId,
    },
}

impl Display for AgentWorkflowRegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDefinition {
                workflow_id,
                reason,
            } => match workflow_id {
                Some(workflow_id) => {
                    write!(f, "invalid workflow definition {workflow_id}: {reason}")
                }
                None => write!(f, "invalid workflow definition: {reason}"),
            },
            Self::DuplicateDefinition {
                workflow_type,
                definition_version,
                existing_workflow_id,
                attempted_workflow_id,
            } => write!(
                f,
                "duplicate workflow definition {workflow_type}@{definition_version}: \
                 existing workflow {existing_workflow_id}, attempted workflow {attempted_workflow_id}"
            ),
        }
    }
}

impl Error for AgentWorkflowRegistryError {}

/// Result type for workflow definition registration APIs.
pub type AgentWorkflowRegistryResult<T> = Result<T, AgentWorkflowRegistryError>;

/// In-memory workflow definition registry.
#[derive(Debug, Clone, Default)]
pub struct AgentWorkflowRegistry {
    definitions: BTreeMap<AgentWorkflowKey, AgentWorkflow>,
}

impl AgentWorkflowRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one workflow definition.
    pub fn register(
        &mut self,
        workflow: AgentWorkflow,
    ) -> AgentWorkflowRegistryResult<&AgentWorkflow> {
        validate_workflow(&workflow)?;
        let key = AgentWorkflowKey::new(
            workflow.workflow_type.clone(),
            workflow.definition_version.clone(),
        );

        if let Some(existing) = self.definitions.get(&key) {
            return Err(AgentWorkflowRegistryError::DuplicateDefinition {
                workflow_type: key.workflow_type.clone(),
                definition_version: key.definition_version.clone(),
                existing_workflow_id: existing.workflow_id.clone(),
                attempted_workflow_id: workflow.workflow_id,
            });
        }

        self.definitions.insert(key.clone(), workflow);
        Ok(self
            .definitions
            .get(&key)
            .expect("workflow was just inserted"))
    }

    /// Returns a workflow definition by type and version.
    #[must_use]
    pub fn get(
        &self,
        workflow_type: &str,
        definition_version: &WorkflowDefinitionVersion,
    ) -> Option<&AgentWorkflow> {
        self.definitions.get(&AgentWorkflowKey::new(
            workflow_type.to_string(),
            definition_version.clone(),
        ))
    }

    /// Returns true when the workflow type and version are registered.
    #[must_use]
    pub fn contains(
        &self,
        workflow_type: &str,
        definition_version: &WorkflowDefinitionVersion,
    ) -> bool {
        self.get(workflow_type, definition_version).is_some()
    }

    /// Returns all registered definitions for a workflow type.
    #[must_use]
    pub fn definitions_for_type(&self, workflow_type: &str) -> Vec<&AgentWorkflow> {
        self.definitions
            .iter()
            .filter_map(|(key, workflow)| {
                (key.workflow_type() == workflow_type).then_some(workflow)
            })
            .collect()
    }

    /// Returns all registered definitions.
    #[must_use]
    pub fn definitions(&self) -> Vec<&AgentWorkflow> {
        self.definitions.values().collect()
    }

    /// Number of registered definitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    /// Returns true when the registry has no definitions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }
}

fn validate_workflow(workflow: &AgentWorkflow) -> AgentWorkflowRegistryResult<()> {
    if workflow.workflow_id.as_str().trim().is_empty() {
        return invalid(None, "workflow id must not be empty");
    }
    if workflow.workflow_type.trim().is_empty() {
        return invalid(
            Some(workflow.workflow_id.clone()),
            "workflow type must not be empty",
        );
    }
    if workflow.definition_version.as_str().trim().is_empty() {
        return invalid(
            Some(workflow.workflow_id.clone()),
            "definition version must not be empty",
        );
    }
    if workflow.status_labels.is_empty() {
        return invalid(
            Some(workflow.workflow_id.clone()),
            "at least one status label is required",
        );
    }
    if workflow.command_types.is_empty() {
        return invalid(
            Some(workflow.workflow_id.clone()),
            "at least one command type is required",
        );
    }
    if workflow.steps.is_empty() {
        return invalid(
            Some(workflow.workflow_id.clone()),
            "at least one step definition is required",
        );
    }

    let mut command_types = BTreeSet::new();
    for command_type in &workflow.command_types {
        if command_type.trim().is_empty() {
            return invalid(
                Some(workflow.workflow_id.clone()),
                "command type must not be empty",
            );
        }
        if !command_types.insert(command_type) {
            return invalid(
                Some(workflow.workflow_id.clone()),
                format!("duplicate command type {command_type}"),
            );
        }
    }

    let mut step_ids = BTreeSet::new();
    for step in &workflow.steps {
        if step.step_id.as_str().trim().is_empty() {
            return invalid(
                Some(workflow.workflow_id.clone()),
                "step id must not be empty",
            );
        }
        if !step_ids.insert(&step.step_id) {
            return invalid(
                Some(workflow.workflow_id.clone()),
                format!("duplicate step id {}", step.step_id),
            );
        }
    }

    let mut payload_types = BTreeSet::new();
    for payload_type in &workflow.payload_types {
        if payload_type.type_name.trim().is_empty() {
            return invalid(
                Some(workflow.workflow_id.clone()),
                "payload type name must not be empty",
            );
        }
        if !payload_types.insert(&payload_type.type_name) {
            return invalid(
                Some(workflow.workflow_id.clone()),
                format!("duplicate payload type {}", payload_type.type_name),
            );
        }
    }

    Ok(())
}

fn invalid<T>(
    workflow_id: Option<AgentWorkflowId>,
    reason: impl Into<String>,
) -> AgentWorkflowRegistryResult<T> {
    Err(AgentWorkflowRegistryError::InvalidDefinition {
        workflow_id,
        reason: reason.into(),
    })
}
