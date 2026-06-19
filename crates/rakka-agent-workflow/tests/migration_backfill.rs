//! Migration and index backfill policy tests.

use rakka_agent_workflow::{
    plan_agent_workflow_index_backfill, repair_agent_workflow_index, AgentMigrationDecision,
    AgentMigrationReason, AgentRunId, AgentRunState, AgentRunStatus, AgentStatePayload,
    AgentStepId, AgentTimestampMillis, AgentWorkflowBackfillAction, AgentWorkflowBackfillSource,
    AgentWorkflowId, AgentWorkflowIndexSchemaVersion, AgentWorkflowMigrationPolicy,
    AgentWorkflowQueryIndex, AgentWorkflowRunQuery, AgentWorkflowShardOwnership,
    InMemoryAgentWorkflowQueryIndex, StateSchemaVersion, WorkflowDefinitionVersion,
};

#[test]
fn n_plus_one_policy_accepts_current_and_previous_state_and_index_versions() {
    let policy = AgentWorkflowMigrationPolicy::n_plus_one(
        StateSchemaVersion::new(2),
        AgentWorkflowIndexSchemaVersion::new(3),
    );

    let current_run = run_state("run-current", "v1", 2);
    let previous_run = run_state("run-previous", "v1", 1);
    let old_run = run_state("run-old", "v1", 0);
    let future_run = run_state("run-future", "v1", 3);

    assert_eq!(
        policy.assess_run_state(&current_run).decision,
        AgentMigrationDecision::Current
    );
    let previous_assessment = policy.assess_run_state(&previous_run);
    assert_eq!(
        previous_assessment.decision,
        AgentMigrationDecision::CompatiblePrevious
    );
    assert_eq!(
        previous_assessment.reason,
        Some(AgentMigrationReason::StateSchemaPrevious)
    );
    assert_eq!(
        policy.assess_run_state(&old_run).reason,
        Some(AgentMigrationReason::StateSchemaTooOld)
    );
    assert_eq!(
        policy.assess_run_state(&future_run).reason,
        Some(AgentMigrationReason::StateSchemaAhead)
    );

    assert_eq!(
        policy
            .assess_index_schema(AgentWorkflowIndexSchemaVersion::new(3))
            .decision,
        AgentMigrationDecision::Current
    );
    assert_eq!(
        policy
            .assess_index_schema(AgentWorkflowIndexSchemaVersion::new(2))
            .reason,
        Some(AgentMigrationReason::IndexSchemaPrevious)
    );
    assert_eq!(
        policy
            .assess_index_schema(AgentWorkflowIndexSchemaVersion::new(1))
            .reason,
        Some(AgentMigrationReason::IndexSchemaTooOld)
    );
    assert_eq!(
        policy
            .assess_index_schema(AgentWorkflowIndexSchemaVersion::new(4))
            .reason,
        Some(AgentMigrationReason::IndexSchemaAhead)
    );
}

#[test]
fn definition_version_allow_list_blocks_disabled_runs() {
    let policy = AgentWorkflowMigrationPolicy::n_plus_one(
        StateSchemaVersion::new(2),
        AgentWorkflowIndexSchemaVersion::new(2),
    )
    .support_definition_version("v2");

    let disabled = policy.assess_run_state(&run_state("run-v1", "v1", 2));

    assert_eq!(disabled.decision, AgentMigrationDecision::Unsupported);
    assert_eq!(
        disabled.reason,
        Some(AgentMigrationReason::DefinitionVersionUnsupported)
    );
}

#[test]
fn versioned_run_state_serialization_preserves_schema_and_definition_versions() {
    let run_v1 = run_state("run-v1", "v1", 1);
    let run_v2 = run_state("run-v2", "v2", 2);

    let json_v1 = serde_json::to_string(&run_v1).expect("v1 run should serialize");
    let json_v2 = serde_json::to_string(&run_v2).expect("v2 run should serialize");

    assert!(json_v1.contains("\"definition_version\":\"v1\""));
    assert!(json_v1.contains("\"state_schema_version\":1"));
    assert!(json_v2.contains("\"definition_version\":\"v2\""));
    assert!(json_v2.contains("\"state_schema_version\":2"));

    let decoded_v1: AgentRunState =
        serde_json::from_str(&json_v1).expect("v1 run should deserialize");
    let decoded_v2: AgentRunState =
        serde_json::from_str(&json_v2).expect("v2 run should deserialize");

    assert_eq!(
        decoded_v1.definition_version,
        WorkflowDefinitionVersion::new("v1")
    );
    assert_eq!(decoded_v1.state_schema_version, StateSchemaVersion::new(1));
    assert_eq!(
        decoded_v2.definition_version,
        WorkflowDefinitionVersion::new("v2")
    );
    assert_eq!(decoded_v2.state_schema_version, StateSchemaVersion::new(2));
}

#[tokio::test]
async fn repair_index_rebuilds_supported_sources_and_skips_unsupported_runs() {
    let policy = AgentWorkflowMigrationPolicy::n_plus_one(
        StateSchemaVersion::new(2),
        AgentWorkflowIndexSchemaVersion::new(3),
    )
    .support_definition_version("v1");
    let sources = vec![
        AgentWorkflowBackfillSource::from_run_state(run_state("run-current", "v1", 2), "research")
            .namespace("prod")
            .shard_ownership(AgentWorkflowShardOwnership::new("AgentRun", "7", "node-a")),
        AgentWorkflowBackfillSource::from_run_state(run_state("run-previous", "v1", 1), "research")
            .namespace("prod"),
        AgentWorkflowBackfillSource::from_run_state(run_state("run-old", "v1", 0), "research"),
        AgentWorkflowBackfillSource::from_run_state(
            run_state("run-disabled", "v-disabled", 2),
            "research",
        ),
    ];

    let dry_run = plan_agent_workflow_index_backfill(
        sources.clone(),
        &policy,
        AgentWorkflowIndexSchemaVersion::new(3),
    );
    assert_eq!(dry_run.len(), 4);
    assert_eq!(
        dry_run.items[0].action,
        AgentWorkflowBackfillAction::UpsertRunProjection
    );
    assert_eq!(
        dry_run.items[1].action,
        AgentWorkflowBackfillAction::RebuildRunProjection
    );
    assert_eq!(
        dry_run.items[2].action,
        AgentWorkflowBackfillAction::SkipUnsupported
    );
    assert_eq!(
        dry_run.items[3].run_assessment.reason,
        Some(AgentMigrationReason::DefinitionVersionUnsupported)
    );

    let mut index = InMemoryAgentWorkflowQueryIndex::new();
    let executed = repair_agent_workflow_index(
        &mut index,
        sources,
        &policy,
        AgentWorkflowIndexSchemaVersion::new(2),
    )
    .await
    .expect("index repair should succeed");

    assert_eq!(executed.len(), 4);
    assert_eq!(executed.write_count(), 2);
    assert_eq!(executed.skipped_count(), 2);
    assert!(executed
        .items
        .iter()
        .take(2)
        .all(|item| item.action == AgentWorkflowBackfillAction::RebuildRunProjection));

    let indexed = index
        .query_runs(AgentWorkflowRunQuery::new().workflow_type("research"))
        .await
        .expect("repaired runs should be queryable");
    let run_ids = indexed
        .iter()
        .map(|entry| entry.run_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(run_ids, vec!["run-current", "run-previous"]);
    assert_eq!(indexed[0].namespace.as_deref(), Some("prod"));
    assert_eq!(
        indexed[0]
            .shard_ownership
            .as_ref()
            .map(|owner| owner.shard_id.as_str()),
        Some("7")
    );
}

fn run_state(run_id: &str, definition_version: &str, state_schema_version: u32) -> AgentRunState {
    AgentRunState {
        run_id: AgentRunId::new(run_id),
        workflow_id: AgentWorkflowId::new("workflow-research"),
        tenant: None,
        definition_version: WorkflowDefinitionVersion::new(definition_version),
        state_schema_version: StateSchemaVersion::new(state_schema_version),
        status: AgentRunStatus::Running,
        current_step_id: Some(AgentStepId::new("step-plan")),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(1_000),
        updated_at: AgentTimestampMillis::new(1_100),
        completed_at: None,
    }
}
