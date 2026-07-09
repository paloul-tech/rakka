//! Workflow catalog: which workflows this A2A service hosts.

use rakka_agent_workflow::AgentWorkflow;

use crate::mapping::A2AWorkflowSelection;

/// Catalog of workflows an A2A service hosts, keyed by stable workflow id,
/// workflow type, and definition version.
///
/// Selection policy is deterministic: an explicit selection must match every
/// present field; among multiple matches the first workflow in registration
/// order wins; an empty selection resolves to the default workflow.
pub trait A2AWorkflowCatalog: Send + Sync + 'static {
    /// Workflow used when a request carries no selection metadata.
    fn default_workflow(&self) -> &AgentWorkflow;

    /// Resolves an explicit selection; `None` when nothing matches.
    fn resolve(&self, selection: &A2AWorkflowSelection) -> Option<&AgentWorkflow> {
        if selection.is_empty() {
            return Some(self.default_workflow());
        }
        self.workflows()
            .into_iter()
            .find(|workflow| selection.matches(workflow))
    }

    /// Resolves the workflow of record for an existing run.
    ///
    /// Existing runs pin their workflow id (and definition version) in
    /// durable run state; continuations and cancellations must use that
    /// workflow, not a request-supplied one.
    fn resolve_by_id(
        &self,
        workflow_id: &str,
        definition_version: Option<&str>,
    ) -> Option<&AgentWorkflow> {
        self.workflows().into_iter().find(|workflow| {
            workflow.workflow_id.as_str() == workflow_id
                && definition_version
                    .is_none_or(|version| workflow.definition_version.as_str() == version)
        })
    }

    /// Every hosted workflow, in registration order. Used for agent-card
    /// skill projection.
    fn workflows(&self) -> Vec<&AgentWorkflow>;
}

/// Static catalog over a fixed workflow list.
#[derive(Debug, Clone)]
pub struct A2AStaticWorkflowCatalog {
    workflows: Vec<AgentWorkflow>,
    default_index: usize,
}

impl A2AStaticWorkflowCatalog {
    /// Creates a single-workflow catalog (local/example convenience).
    #[must_use]
    pub fn single(workflow: AgentWorkflow) -> Self {
        Self {
            workflows: vec![workflow],
            default_index: 0,
        }
    }

    /// Creates a catalog whose first workflow is the default.
    ///
    /// Returns `None` when `workflows` is empty.
    #[must_use]
    pub fn new(workflows: Vec<AgentWorkflow>) -> Option<Self> {
        if workflows.is_empty() {
            return None;
        }
        Some(Self {
            workflows,
            default_index: 0,
        })
    }

    /// Selects the default workflow by workflow id.
    ///
    /// Returns `None` when no hosted workflow has that id.
    #[must_use]
    pub fn with_default(mut self, workflow_id: &str) -> Option<Self> {
        let index = self
            .workflows
            .iter()
            .position(|workflow| workflow.workflow_id.as_str() == workflow_id)?;
        self.default_index = index;
        Some(self)
    }
}

impl A2AWorkflowCatalog for A2AStaticWorkflowCatalog {
    fn default_workflow(&self) -> &AgentWorkflow {
        &self.workflows[self.default_index]
    }

    fn workflows(&self) -> Vec<&AgentWorkflow> {
        self.workflows.iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::fixture_workflow;
    use rakka_agent_workflow::{AgentWorkflowId, WorkflowDefinitionVersion};

    fn second_workflow() -> AgentWorkflow {
        let mut workflow = fixture_workflow();
        workflow.workflow_id = AgentWorkflowId::new("workflow-second");
        workflow.workflow_type = "second-type".to_string();
        workflow.definition_version = WorkflowDefinitionVersion::new("v2");
        workflow
    }

    #[test]
    fn empty_selection_resolves_to_default_and_ids_resolve_exactly() {
        let catalog = A2AStaticWorkflowCatalog::new(vec![fixture_workflow(), second_workflow()])
            .expect("catalog");

        let resolved = catalog
            .resolve(&A2AWorkflowSelection::default())
            .expect("default");
        assert_eq!(
            resolved.workflow_id.as_str(),
            fixture_workflow().workflow_id.as_str()
        );

        let selected = catalog
            .resolve(&A2AWorkflowSelection {
                workflow_type: Some("second-type".to_string()),
                ..Default::default()
            })
            .expect("by type");
        assert_eq!(selected.workflow_id.as_str(), "workflow-second");

        assert!(catalog
            .resolve(&A2AWorkflowSelection {
                workflow_id: Some("missing".to_string()),
                ..Default::default()
            })
            .is_none());

        assert!(catalog
            .resolve_by_id("workflow-second", Some("v2"))
            .is_some());
        assert!(catalog
            .resolve_by_id("workflow-second", Some("v9"))
            .is_none());
        assert!(catalog.resolve_by_id("workflow-second", None).is_some());
    }

    #[test]
    fn default_can_be_re_pointed_by_workflow_id() {
        let catalog = A2AStaticWorkflowCatalog::new(vec![fixture_workflow(), second_workflow()])
            .expect("catalog")
            .with_default("workflow-second")
            .expect("default exists");
        assert_eq!(
            catalog.default_workflow().workflow_id.as_str(),
            "workflow-second"
        );
    }
}
