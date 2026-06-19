//! Migration and backfill policy for durable agent workflow indexes.
//!
//! Durable run state is the source of truth. Query indexes are operational
//! projections that can be rebuilt from that state, so this module keeps
//! compatibility decisions and repair planning explicit instead of hiding them
//! in a particular index backend.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    AgentRunId, AgentRunIndexEntry, AgentRunState, AgentWorkflowId, AgentWorkflowQueryIndex,
    AgentWorkflowQueryResult, AgentWorkflowShardOwnership, StateSchemaVersion,
    WorkflowDefinitionVersion,
};

/// Current query index schema version for agent workflow projections.
pub const CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION: AgentWorkflowIndexSchemaVersion =
    AgentWorkflowIndexSchemaVersion::new(1);

/// Version of the operational query index schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentWorkflowIndexSchemaVersion(u32);

impl AgentWorkflowIndexSchemaVersion {
    /// Creates an index schema version from a positive integer.
    #[must_use]
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// Returns the version number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Compatibility policy for workflow definitions, durable state, and indexes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowMigrationPolicy {
    /// Current durable run-state schema version understood by this binary.
    pub current_state_schema_version: StateSchemaVersion,
    /// Oldest durable run-state schema version this binary can read.
    pub minimum_supported_state_schema_version: StateSchemaVersion,
    /// Current query index schema version expected by this binary.
    pub current_index_schema_version: AgentWorkflowIndexSchemaVersion,
    /// Oldest query index schema version this binary can repair in place.
    pub minimum_supported_index_schema_version: AgentWorkflowIndexSchemaVersion,
    /// Optional allow-list of workflow definition versions for this deployment.
    pub supported_definition_versions: BTreeSet<WorkflowDefinitionVersion>,
}

impl AgentWorkflowMigrationPolicy {
    /// Creates an explicit migration policy.
    #[must_use]
    pub fn new(
        current_state_schema_version: StateSchemaVersion,
        minimum_supported_state_schema_version: StateSchemaVersion,
        current_index_schema_version: AgentWorkflowIndexSchemaVersion,
        minimum_supported_index_schema_version: AgentWorkflowIndexSchemaVersion,
    ) -> Self {
        Self {
            current_state_schema_version,
            minimum_supported_state_schema_version,
            current_index_schema_version,
            minimum_supported_index_schema_version,
            supported_definition_versions: BTreeSet::new(),
        }
    }

    /// Creates the default N/N+1 compatibility policy.
    ///
    /// The current version N and the immediately previous version N-1 are
    /// accepted. Older versions are rejected so operators can backfill them
    /// before rolling the next deployment through a Kubernetes fleet.
    #[must_use]
    pub fn n_plus_one(
        current_state_schema_version: StateSchemaVersion,
        current_index_schema_version: AgentWorkflowIndexSchemaVersion,
    ) -> Self {
        Self::new(
            current_state_schema_version,
            previous_state_schema_version(current_state_schema_version),
            current_index_schema_version,
            previous_index_schema_version(current_index_schema_version),
        )
    }

    /// Adds one workflow definition version to the deployment allow-list.
    ///
    /// If no versions are added, all definition versions are considered
    /// readable and only state/index schema compatibility is enforced.
    #[must_use]
    pub fn support_definition_version(
        mut self,
        version: impl Into<WorkflowDefinitionVersion>,
    ) -> Self {
        self.supported_definition_versions.insert(version.into());
        self
    }

    /// Returns the compatibility assessment for a durable run state.
    #[must_use]
    pub fn assess_run_state(&self, run: &AgentRunState) -> AgentMigrationAssessment {
        if !self.supported_definition_versions.is_empty()
            && !self
                .supported_definition_versions
                .contains(&run.definition_version)
        {
            return AgentMigrationAssessment::unsupported(
                AgentMigrationReason::DefinitionVersionUnsupported,
            );
        }

        assess_version(
            run.state_schema_version.get(),
            self.current_state_schema_version.get(),
            self.minimum_supported_state_schema_version.get(),
            AgentMigrationReason::StateSchemaPrevious,
            AgentMigrationReason::StateSchemaTooOld,
            AgentMigrationReason::StateSchemaAhead,
        )
    }

    /// Returns the compatibility assessment for an observed index schema.
    #[must_use]
    pub fn assess_index_schema(
        &self,
        observed: AgentWorkflowIndexSchemaVersion,
    ) -> AgentMigrationAssessment {
        assess_version(
            observed.get(),
            self.current_index_schema_version.get(),
            self.minimum_supported_index_schema_version.get(),
            AgentMigrationReason::IndexSchemaPrevious,
            AgentMigrationReason::IndexSchemaTooOld,
            AgentMigrationReason::IndexSchemaAhead,
        )
    }
}

/// High-level migration decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentMigrationDecision {
    /// The observed schema matches this binary's current schema.
    Current,
    /// The observed schema is supported but should be backfilled to current.
    CompatiblePrevious,
    /// The observed schema or definition cannot be handled by this binary.
    Unsupported,
}

impl AgentMigrationDecision {
    /// Returns true when work may proceed safely.
    #[must_use]
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Current | Self::CompatiblePrevious)
    }

    /// Returns true when a repair or migration pass should rewrite projection data.
    #[must_use]
    pub const fn requires_backfill(self) -> bool {
        matches!(self, Self::CompatiblePrevious)
    }
}

/// Machine-readable reason attached to a migration assessment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentMigrationReason {
    /// Durable run state is on a supported previous schema.
    StateSchemaPrevious,
    /// Durable run state is older than the supported compatibility window.
    StateSchemaTooOld,
    /// Durable run state was written by a newer binary.
    StateSchemaAhead,
    /// Query index data is on a supported previous schema.
    IndexSchemaPrevious,
    /// Query index data is older than the supported compatibility window.
    IndexSchemaTooOld,
    /// Query index data was written by a newer binary.
    IndexSchemaAhead,
    /// Workflow definition version is not enabled in this deployment.
    DefinitionVersionUnsupported,
}

/// Compatibility assessment for one migration subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMigrationAssessment {
    /// High-level decision.
    pub decision: AgentMigrationDecision,
    /// Stable reason for non-current decisions.
    pub reason: Option<AgentMigrationReason>,
}

impl AgentMigrationAssessment {
    /// Creates a current-schema assessment.
    #[must_use]
    pub const fn current() -> Self {
        Self {
            decision: AgentMigrationDecision::Current,
            reason: None,
        }
    }

    /// Creates a supported previous-schema assessment.
    #[must_use]
    pub const fn compatible_previous(reason: AgentMigrationReason) -> Self {
        Self {
            decision: AgentMigrationDecision::CompatiblePrevious,
            reason: Some(reason),
        }
    }

    /// Creates an unsupported assessment.
    #[must_use]
    pub const fn unsupported(reason: AgentMigrationReason) -> Self {
        Self {
            decision: AgentMigrationDecision::Unsupported,
            reason: Some(reason),
        }
    }

    /// Returns true when work may proceed safely.
    #[must_use]
    pub const fn is_supported(self) -> bool {
        self.decision.is_supported()
    }

    /// Returns true when the subject should be rewritten to the current schema.
    #[must_use]
    pub const fn requires_backfill(self) -> bool {
        self.decision.requires_backfill()
    }
}

/// Durable run plus projection metadata used by index repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowBackfillSource {
    /// Durable run state to project.
    pub run_state: AgentRunState,
    /// Bounded workflow type label for query projections.
    pub workflow_type: String,
    /// Application namespace or operational partition, when known.
    pub namespace: Option<String>,
    /// Shard ownership metadata, when known.
    pub shard_ownership: Option<AgentWorkflowShardOwnership>,
}

impl AgentWorkflowBackfillSource {
    /// Creates a repair source from durable run state and workflow type.
    #[must_use]
    pub fn from_run_state(run_state: AgentRunState, workflow_type: impl Into<String>) -> Self {
        Self {
            run_state,
            workflow_type: workflow_type.into(),
            namespace: None,
            shard_ownership: None,
        }
    }

    /// Sets an explicit namespace for repaired projections.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sets shard ownership metadata for repaired projections.
    #[must_use]
    pub fn shard_ownership(mut self, ownership: AgentWorkflowShardOwnership) -> Self {
        self.shard_ownership = Some(ownership);
        self
    }

    /// Builds the run projection that can be upserted into a query index.
    #[must_use]
    pub fn run_index_entry(&self) -> AgentRunIndexEntry {
        let mut entry =
            AgentRunIndexEntry::from_run_state(&self.run_state, self.workflow_type.clone());
        if let Some(namespace) = &self.namespace {
            entry = entry.namespace(namespace.clone());
        }
        if let Some(ownership) = &self.shard_ownership {
            entry = entry.shard_ownership(ownership.clone());
        }
        entry
    }
}

/// Repair action selected for one durable run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentWorkflowBackfillAction {
    /// Upsert the run projection into a current-schema index.
    UpsertRunProjection,
    /// Rebuild the run projection because the index schema is supported but old.
    RebuildRunProjection,
    /// Skip the run because the current binary cannot safely read it.
    SkipUnsupported,
}

impl AgentWorkflowBackfillAction {
    /// Returns true when the action writes a projection into the index.
    #[must_use]
    pub const fn writes_index(self) -> bool {
        matches!(self, Self::UpsertRunProjection | Self::RebuildRunProjection)
    }
}

/// Planned or completed repair action for one durable run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowBackfillItem {
    /// Stable run id.
    pub run_id: AgentRunId,
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Workflow definition version selected by the run.
    pub definition_version: WorkflowDefinitionVersion,
    /// Durable run-state schema observed for the run.
    pub state_schema_version: StateSchemaVersion,
    /// Target query index schema version for repaired projections.
    pub target_index_schema_version: AgentWorkflowIndexSchemaVersion,
    /// Action selected by the repair planner.
    pub action: AgentWorkflowBackfillAction,
    /// Run-state compatibility assessment.
    pub run_assessment: AgentMigrationAssessment,
    /// Index-schema compatibility assessment used for this item.
    pub index_assessment: AgentMigrationAssessment,
}

impl AgentWorkflowBackfillItem {
    fn from_source(
        source: &AgentWorkflowBackfillSource,
        target_index_schema_version: AgentWorkflowIndexSchemaVersion,
        action: AgentWorkflowBackfillAction,
        run_assessment: AgentMigrationAssessment,
        index_assessment: AgentMigrationAssessment,
    ) -> Self {
        Self {
            run_id: source.run_state.run_id.clone(),
            workflow_id: source.run_state.workflow_id.clone(),
            definition_version: source.run_state.definition_version.clone(),
            state_schema_version: source.run_state.state_schema_version,
            target_index_schema_version,
            action,
            run_assessment,
            index_assessment,
        }
    }
}

/// Backfill plan for an index repair pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowBackfillPlan {
    /// Target index schema version.
    pub target_index_schema_version: AgentWorkflowIndexSchemaVersion,
    /// Planned or executed item actions.
    pub items: Vec<AgentWorkflowBackfillItem>,
}

impl AgentWorkflowBackfillPlan {
    /// Creates an empty backfill plan.
    #[must_use]
    pub const fn new(target_index_schema_version: AgentWorkflowIndexSchemaVersion) -> Self {
        Self {
            target_index_schema_version,
            items: Vec::new(),
        }
    }

    /// Adds one plan item.
    pub fn push(&mut self, item: AgentWorkflowBackfillItem) {
        self.items.push(item);
    }

    /// Returns the number of items in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true when the plan has no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the number of items that will write index records.
    #[must_use]
    pub fn write_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.action.writes_index())
            .count()
    }

    /// Returns the number of unsupported items.
    #[must_use]
    pub fn skipped_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| item.action == AgentWorkflowBackfillAction::SkipUnsupported)
            .count()
    }
}

/// Builds a deterministic repair plan without writing to an index.
#[must_use]
pub fn plan_agent_workflow_index_backfill(
    sources: impl IntoIterator<Item = AgentWorkflowBackfillSource>,
    policy: &AgentWorkflowMigrationPolicy,
    observed_index_schema_version: AgentWorkflowIndexSchemaVersion,
) -> AgentWorkflowBackfillPlan {
    let index_assessment = policy.assess_index_schema(observed_index_schema_version);
    let mut plan = AgentWorkflowBackfillPlan::new(policy.current_index_schema_version);

    for source in sources {
        let run_assessment = policy.assess_run_state(&source.run_state);
        let action = backfill_action(run_assessment, index_assessment);
        plan.push(AgentWorkflowBackfillItem::from_source(
            &source,
            policy.current_index_schema_version,
            action,
            run_assessment,
            index_assessment,
        ));
    }

    plan
}

/// Repairs an agent workflow query index from durable run state.
///
/// This is intentionally small enough to use as an admin API handler body or a
/// Kubernetes Job loop: page durable run states, wrap them as
/// [`AgentWorkflowBackfillSource`] values, and call this function for each page.
pub async fn repair_agent_workflow_index<I>(
    index: &mut I,
    sources: impl IntoIterator<Item = AgentWorkflowBackfillSource>,
    policy: &AgentWorkflowMigrationPolicy,
    observed_index_schema_version: AgentWorkflowIndexSchemaVersion,
) -> AgentWorkflowQueryResult<AgentWorkflowBackfillPlan>
where
    I: AgentWorkflowQueryIndex,
{
    let index_assessment = policy.assess_index_schema(observed_index_schema_version);
    let mut plan = AgentWorkflowBackfillPlan::new(policy.current_index_schema_version);

    for source in sources {
        let run_assessment = policy.assess_run_state(&source.run_state);
        let action = backfill_action(run_assessment, index_assessment);
        let item = AgentWorkflowBackfillItem::from_source(
            &source,
            policy.current_index_schema_version,
            action,
            run_assessment,
            index_assessment,
        );
        if action.writes_index() {
            index.upsert_run(source.run_index_entry()).await?;
        }
        plan.push(item);
    }

    Ok(plan)
}

fn assess_version(
    observed: u32,
    current: u32,
    minimum_supported: u32,
    previous_reason: AgentMigrationReason,
    too_old_reason: AgentMigrationReason,
    ahead_reason: AgentMigrationReason,
) -> AgentMigrationAssessment {
    if observed == current {
        AgentMigrationAssessment::current()
    } else if observed > current {
        AgentMigrationAssessment::unsupported(ahead_reason)
    } else if observed >= minimum_supported {
        AgentMigrationAssessment::compatible_previous(previous_reason)
    } else {
        AgentMigrationAssessment::unsupported(too_old_reason)
    }
}

fn backfill_action(
    run_assessment: AgentMigrationAssessment,
    index_assessment: AgentMigrationAssessment,
) -> AgentWorkflowBackfillAction {
    if !run_assessment.is_supported() || !index_assessment.is_supported() {
        AgentWorkflowBackfillAction::SkipUnsupported
    } else if run_assessment.requires_backfill() || index_assessment.requires_backfill() {
        AgentWorkflowBackfillAction::RebuildRunProjection
    } else {
        AgentWorkflowBackfillAction::UpsertRunProjection
    }
}

const fn previous_state_schema_version(current: StateSchemaVersion) -> StateSchemaVersion {
    let version = current.get();
    if version > 1 {
        StateSchemaVersion::new(version - 1)
    } else {
        StateSchemaVersion::new(version)
    }
}

const fn previous_index_schema_version(
    current: AgentWorkflowIndexSchemaVersion,
) -> AgentWorkflowIndexSchemaVersion {
    let version = current.get();
    if version > 1 {
        AgentWorkflowIndexSchemaVersion::new(version - 1)
    } else {
        AgentWorkflowIndexSchemaVersion::new(version)
    }
}
