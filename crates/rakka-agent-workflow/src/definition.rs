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

use serde::{Deserialize, Serialize};

use crate::compiled_plan::{
    validate_compiled_execution_plan_with_catalog, AgentCompiledExecutionPlan,
    AgentCompiledNodeKindCatalog, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCompiledPlanSchemaVersion, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
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
    /// Compiled execution plan failed validation.
    InvalidCompiledPlan {
        /// Compiled plan id associated with the rejected plan, when available.
        plan_id: Option<AgentCompiledPlanId>,
        /// Stable validation error code.
        code: &'static str,
        /// Stable bounded reason.
        reason: String,
    },
    /// Compiled plan metadata does not match the workflow definition or runtime.
    IncompatibleCompiledPlan {
        /// Workflow id associated with the registration attempt.
        workflow_id: AgentWorkflowId,
        /// Compiled plan id associated with the registration attempt.
        plan_id: AgentCompiledPlanId,
        /// Stable bounded reason.
        reason: String,
    },
    /// A compiled plan is already registered for this workflow type and version.
    DuplicateCompiledPlan {
        /// Workflow type.
        workflow_type: String,
        /// Definition version.
        definition_version: WorkflowDefinitionVersion,
        /// Existing compiled plan id.
        existing_plan_id: AgentCompiledPlanId,
        /// Attempted compiled plan id.
        attempted_plan_id: AgentCompiledPlanId,
    },
    /// A compiled plan was attached without a matching workflow definition.
    MissingDefinitionForCompiledPlan {
        /// Workflow type.
        workflow_type: String,
        /// Definition version.
        definition_version: WorkflowDefinitionVersion,
        /// Compiled plan id.
        plan_id: AgentCompiledPlanId,
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
            Self::InvalidCompiledPlan {
                plan_id,
                code,
                reason,
            } => match plan_id {
                Some(plan_id) => {
                    write!(f, "invalid compiled plan {plan_id} ({code}): {reason}")
                }
                None => write!(f, "invalid compiled plan ({code}): {reason}"),
            },
            Self::IncompatibleCompiledPlan {
                workflow_id,
                plan_id,
                reason,
            } => write!(
                f,
                "compiled plan {plan_id} is incompatible with workflow {workflow_id}: {reason}"
            ),
            Self::DuplicateCompiledPlan {
                workflow_type,
                definition_version,
                existing_plan_id,
                attempted_plan_id,
            } => write!(
                f,
                "duplicate compiled plan {workflow_type}@{definition_version}: \
                 existing plan {existing_plan_id}, attempted plan {attempted_plan_id}"
            ),
            Self::MissingDefinitionForCompiledPlan {
                workflow_type,
                definition_version,
                plan_id,
            } => write!(
                f,
                "compiled plan {plan_id} references missing workflow definition \
                 {workflow_type}@{definition_version}"
            ),
        }
    }
}

impl Error for AgentWorkflowRegistryError {}

/// Result type for workflow definition registration APIs.
pub type AgentWorkflowRegistryResult<T> = Result<T, AgentWorkflowRegistryError>;

/// Registered workflow definition paired with its compiled execution plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledWorkflowRegistration {
    /// Workflow metadata registered for the application-facing workflow type.
    pub workflow: AgentWorkflow,
    /// Product-neutral compiled plan Rakka interprets for this workflow version.
    pub plan: AgentCompiledExecutionPlan,
}

impl AgentCompiledWorkflowRegistration {
    /// Creates a registration record.
    #[must_use]
    pub fn new(workflow: AgentWorkflow, plan: AgentCompiledExecutionPlan) -> Self {
        Self { workflow, plan }
    }

    /// Workflow type used as the registry key.
    #[must_use]
    pub fn workflow_type(&self) -> &str {
        &self.workflow.workflow_type
    }

    /// Workflow definition version used as the registry key.
    #[must_use]
    pub const fn definition_version(&self) -> &WorkflowDefinitionVersion {
        &self.workflow.definition_version
    }

    /// Registered compiled plan id.
    #[must_use]
    pub const fn plan_id(&self) -> &AgentCompiledPlanId {
        &self.plan.plan_id
    }

    /// Immutable compiled plan fingerprint.
    #[must_use]
    pub const fn plan_fingerprint(&self) -> &AgentCompiledPlanFingerprint {
        &self.plan.plan_fingerprint
    }

    /// Compiled plan schema version.
    #[must_use]
    pub const fn plan_schema_version(&self) -> AgentCompiledPlanSchemaVersion {
        self.plan.plan_schema_version
    }
}

/// In-memory workflow definition registry.
#[derive(Debug, Clone, Default)]
pub struct AgentWorkflowRegistry {
    definitions: BTreeMap<AgentWorkflowKey, AgentWorkflow>,
    compiled_registrations: BTreeMap<AgentWorkflowKey, AgentCompiledWorkflowRegistration>,
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

    /// Registers one workflow definition together with its compiled execution plan.
    pub fn register_compiled(
        &mut self,
        workflow: AgentWorkflow,
        plan: AgentCompiledExecutionPlan,
    ) -> AgentWorkflowRegistryResult<&AgentCompiledWorkflowRegistration> {
        validate_workflow(&workflow)?;
        validate_compiled_registration(&workflow, &plan)?;
        let key = AgentWorkflowKey::new(
            workflow.workflow_type.clone(),
            workflow.definition_version.clone(),
        );

        if let Some(existing) = self.compiled_registrations.get(&key) {
            return Err(AgentWorkflowRegistryError::DuplicateCompiledPlan {
                workflow_type: key.workflow_type.clone(),
                definition_version: key.definition_version.clone(),
                existing_plan_id: existing.plan.plan_id.clone(),
                attempted_plan_id: plan.plan_id,
            });
        }
        if let Some(existing) = self.definitions.get(&key) {
            return Err(AgentWorkflowRegistryError::DuplicateDefinition {
                workflow_type: key.workflow_type.clone(),
                definition_version: key.definition_version.clone(),
                existing_workflow_id: existing.workflow_id.clone(),
                attempted_workflow_id: workflow.workflow_id,
            });
        }

        self.definitions.insert(key.clone(), workflow.clone());
        self.compiled_registrations.insert(
            key.clone(),
            AgentCompiledWorkflowRegistration::new(workflow, plan),
        );
        Ok(self
            .compiled_registrations
            .get(&key)
            .expect("compiled workflow was just inserted"))
    }

    /// Attaches a compiled execution plan to an already registered workflow definition.
    pub fn register_compiled_plan(
        &mut self,
        plan: AgentCompiledExecutionPlan,
    ) -> AgentWorkflowRegistryResult<&AgentCompiledWorkflowRegistration> {
        let key =
            AgentWorkflowKey::new(plan.workflow_type.clone(), plan.definition_version.clone());
        if let Some(existing) = self.compiled_registrations.get(&key) {
            return Err(AgentWorkflowRegistryError::DuplicateCompiledPlan {
                workflow_type: key.workflow_type.clone(),
                definition_version: key.definition_version.clone(),
                existing_plan_id: existing.plan.plan_id.clone(),
                attempted_plan_id: plan.plan_id,
            });
        }
        let workflow = self.definitions.get(&key).cloned().ok_or_else(|| {
            AgentWorkflowRegistryError::MissingDefinitionForCompiledPlan {
                workflow_type: key.workflow_type.clone(),
                definition_version: key.definition_version.clone(),
                plan_id: plan.plan_id.clone(),
            }
        })?;

        validate_compiled_registration(&workflow, &plan)?;
        self.compiled_registrations.insert(
            key.clone(),
            AgentCompiledWorkflowRegistration::new(workflow, plan),
        );
        Ok(self
            .compiled_registrations
            .get(&key)
            .expect("compiled workflow was just inserted"))
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

    /// Returns a compiled workflow registration by type and version.
    #[must_use]
    pub fn get_compiled(
        &self,
        workflow_type: &str,
        definition_version: &WorkflowDefinitionVersion,
    ) -> Option<&AgentCompiledWorkflowRegistration> {
        self.compiled_registrations.get(&AgentWorkflowKey::new(
            workflow_type.to_string(),
            definition_version.clone(),
        ))
    }

    /// Returns true when a compiled workflow registration exists.
    #[must_use]
    pub fn contains_compiled(
        &self,
        workflow_type: &str,
        definition_version: &WorkflowDefinitionVersion,
    ) -> bool {
        self.get_compiled(workflow_type, definition_version)
            .is_some()
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

    /// Returns all compiled workflow registrations for a workflow type.
    #[must_use]
    pub fn compiled_registrations_for_type(
        &self,
        workflow_type: &str,
    ) -> Vec<&AgentCompiledWorkflowRegistration> {
        self.compiled_registrations
            .iter()
            .filter_map(|(key, registration)| {
                (key.workflow_type() == workflow_type).then_some(registration)
            })
            .collect()
    }

    /// Returns all registered definitions.
    #[must_use]
    pub fn definitions(&self) -> Vec<&AgentWorkflow> {
        self.definitions.values().collect()
    }

    /// Returns all compiled workflow registrations.
    #[must_use]
    pub fn compiled_registrations(&self) -> Vec<&AgentCompiledWorkflowRegistration> {
        self.compiled_registrations.values().collect()
    }

    /// Number of registered definitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    /// Number of registered compiled workflow definitions.
    #[must_use]
    pub fn compiled_len(&self) -> usize {
        self.compiled_registrations.len()
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

fn validate_compiled_registration(
    workflow: &AgentWorkflow,
    plan: &AgentCompiledExecutionPlan,
) -> AgentWorkflowRegistryResult<()> {
    validate_compiled_execution_plan_with_catalog(plan, &AgentCompiledNodeKindCatalog::current())
        .map_err(|error| AgentWorkflowRegistryError::InvalidCompiledPlan {
        plan_id: Some(plan.plan_id.clone()),
        code: error.code(),
        reason: error.to_string(),
    })?;

    if plan.plan_schema_version != CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION {
        return incompatible_compiled(
            workflow,
            plan,
            format!(
                "compiled plan schema version {} is not supported by this runtime; expected {}",
                plan.plan_schema_version.get(),
                CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION.get()
            ),
        );
    }
    if workflow.workflow_id != plan.workflow_id {
        return incompatible_compiled(
            workflow,
            plan,
            format!(
                "workflow id mismatch: definition has {}, plan has {}",
                workflow.workflow_id, plan.workflow_id
            ),
        );
    }
    if workflow.workflow_type != plan.workflow_type {
        return incompatible_compiled(
            workflow,
            plan,
            format!(
                "workflow type mismatch: definition has {}, plan has {}",
                workflow.workflow_type, plan.workflow_type
            ),
        );
    }
    if workflow.definition_version != plan.definition_version {
        return incompatible_compiled(
            workflow,
            plan,
            format!(
                "definition version mismatch: definition has {}, plan has {}",
                workflow.definition_version, plan.definition_version
            ),
        );
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

fn incompatible_compiled<T>(
    workflow: &AgentWorkflow,
    plan: &AgentCompiledExecutionPlan,
    reason: impl Into<String>,
) -> AgentWorkflowRegistryResult<T> {
    Err(AgentWorkflowRegistryError::IncompatibleCompiledPlan {
        workflow_id: workflow.workflow_id.clone(),
        plan_id: plan.plan_id.clone(),
        reason: reason.into(),
    })
}
