//! The communal read path, end to end: real sharded agent, task and run
//! entities, the production dispatcher, the knowledge graph's own adapters
//! and the grant read from the agent entity
//! ([specification 13.4, 13.5 and 16](../../../docs/plans/rakka-agent/spec.md);
//! issue #69).
//!
//! One verified claim sits in one knowledge space and requires one read
//! capability. Three agents run the same task, and what each run's persisted
//! context snapshot holds is decided by that agent's own definition and
//! nothing else.

use std::collections::BTreeSet;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    registered_agent_task_entity_ref, run_id_for_assignment, AgentAssignmentGeneration,
    AgentAuthorityEnvelope, AgentCapabilityId, AgentCommunalClaimTrust, AgentContextSnapshotRef,
    AgentId, AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunScope,
    AgentSchemaPolicy, AgentScope, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition,
    AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityReply, AgentTaskId,
    AgentTaskScope, CommunalClaimObject, ContextSnapshotStore, KnowledgeSpaceId,
    MemoryClassification, MemoryContextSnapshot, TenantId, VersionedAgentRecord,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimAccess, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimPromotionPolicy, ClaimProvenance, ClaimTrustStatus, ClaimTrustTransitionRequest,
    KnowledgeGraphStore, KnowledgeSpaceScope,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use rakka_example_multi_agent_goal_acceptance::flow::{
    ask_retrying, instantiate, mission_definition, pump,
};
use rakka_example_multi_agent_goal_acceptance::wiring::{World, TENANT};

const SHARED_SPACE: &str = "shared-findings";
const READ_CAPABILITY: &str = "findings.read";
const CLAIM_TEXT: &str = "the zanzibar shipment is held at the mombasa depot";
const TASK_INPUT: &str = "where is the zanzibar shipment";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn shared_space() -> KnowledgeSpaceId {
    KnowledgeSpaceId::new(SHARED_SPACE).expect("the space id is valid")
}

fn read_capability() -> AgentCapabilityId {
    AgentCapabilityId::new(READ_CAPABILITY).expect("the capability id is valid")
}

fn reviewer() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "findings-reviewer".to_string(),
        display_name: None,
    }
}

/// Appends the one claim of this test to the shared space, requiring the
/// read capability, and promotes it to `Verified`.
async fn verified_claim(world: &World) -> Claim {
    let scope = KnowledgeSpaceScope::new(tenant(), shared_space()).expect("the scope is valid");
    let claim = Claim::new(
        &scope,
        ClaimOperationId::derive_append(&scope, "shipment-status")
            .expect("the operation id derives"),
        ClaimNodeId::new("shipment-zanzibar").expect("the node id is valid"),
        ClaimPredicate::new("status").expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!(CLAIM_TEXT)).expect("inline"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        8_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim constructs")
    .with_access(ClaimAccess {
        required_read_capabilities: BTreeSet::from([read_capability()]),
    })
    .expect("the access is bounded");
    let claim = world
        .graph
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    world
        .graph
        .transition(
            &scope,
            &ClaimTrustTransitionRequest::new(
                claim.claim_id.clone(),
                ClaimOperationId::derive_transition(&scope, &claim.claim_id, "verify")
                    .expect("the operation id derives"),
                ClaimTrustStatus::Verified,
                reviewer(),
                AgentTimestampMillis::new(2),
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(2),
        )
        .await
        .expect("the claim is verified");
    claim
}

/// Instantiates `agent` under a definition that grants `spaces` and
/// `capabilities` for communal reads, runs one task assigned to it through
/// one model turn, and returns the context snapshot that turn was computed
/// from, as the snapshot store holds it.
async fn snapshot_of_one_turn(
    world: &World,
    agent: &str,
    spaces: &[KnowledgeSpaceId],
    capabilities: &[AgentCapabilityId],
) -> MemoryContextSnapshot {
    let definition: AgentTaskDefinition = mission_definition();
    let agent_id = AgentId::new(agent).expect("the agent id is valid");
    let agent_scope = AgentScope::new(tenant(), agent_id.clone()).expect("the scope is valid");
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(definition.definition_id.clone());
    envelope.knowledge_spaces.extend(spaces.iter().cloned());
    envelope
        .read_capabilities
        .extend(capabilities.iter().cloned());
    instantiate(world, &agent_scope, &format!("{agent}-v1"), envelope).await;

    let task_id = AgentTaskId::new(format!("task-{agent}")).expect("the task id is valid");
    let task_scope = AgentTaskScope::new(tenant(), task_id.clone()).expect("the scope is valid");
    let task = registered_agent_task_entity_ref(&world.task_registration, &task_scope);
    let created = ask_retrying(
        &task,
        |reply_to| AgentTaskEntityMessage::Command {
            command: Box::new(AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, task_id.as_str(), "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTaskCreation {
                    definition: definition.clone(),
                    input: AgentTaskContent::inline(serde_json::json!(TASK_INPUT))
                        .expect("the input is inline-bounded"),
                    assignee: Some(agent_id.clone()),
                    team: None,
                    goal: None,
                    goal_mode: Default::default(),
                    goal_spec: None,
                    parent: None,
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    delegation: None,
                    telemetry: Default::default(),
                }),
            }),
            reply_to,
        },
        "create-task",
    )
    .await;
    assert!(
        matches!(created, AgentTaskEntityReply::Applied { .. }),
        "the task creates, got {created:?}"
    );

    let run_scope = AgentRunScope::new(
        tenant(),
        agent_id,
        run_id_for_assignment(&task_id, AgentAssignmentGeneration::new(1))
            .expect("the run id derives"),
    )
    .expect("the run scope is valid");
    let adapter = DeterministicModelAdapter::new().with_turn_for(
        1,
        AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
            .with_text("Answering from what I was given.")
            .with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "at the depot" }))
                    .expect("the proposal is inline-bounded"),
            ),
    );
    pump(world, &task_scope, &run_scope, &adapter, true).await;

    let status =
        rakka_agent::load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .and_then(|state| state.status());
    assert!(
        status.is_some_and(rakka_agent::AgentRunStatus::is_terminal),
        "the run of {agent} finishes its one turn, got {status:?}"
    );

    world
        .snapshots
        .load(
            &run_scope,
            &AgentContextSnapshotRef::for_turn(&run_scope, 1).expect("the reference derives"),
        )
        .await
        .expect("the snapshot store answers")
        .expect("the turn's snapshot was persisted before its model call")
}

#[tokio::test]
async fn a_run_reads_only_the_communal_claims_it_is_granted() {
    let world = World::with_communal_read_path();
    let claim = verified_claim(&world).await;

    // Granted the space and the capability the claim requires: the turn is
    // computed from the claim, as the graph holds it.
    let granted = snapshot_of_one_turn(
        &world,
        "reader-granted",
        &[shared_space()],
        &[read_capability()],
    )
    .await;
    assert_eq!(granted.schema_version().get(), 2);
    assert_eq!(granted.communal_claims.len(), 1);
    let selected = &granted.communal_claims[0];
    assert_eq!(selected.claim_id.as_str(), claim.claim_id.as_str());
    assert_eq!(selected.space, shared_space());
    assert_eq!(selected.trust, AgentCommunalClaimTrust::Verified);
    assert_eq!(selected.trust.as_label(), "verified");
    assert_eq!(selected.statement.subject, "shipment-zanzibar");
    assert_eq!(
        selected.statement.object,
        CommunalClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!(CLAIM_TEXT)).expect("inline")
        )
    );
    assert_eq!(selected.statement_digest, claim.content_digest);
    assert_eq!(selected.provenance.agent.as_str(), "scout");
    assert_eq!(selected.confidence_bps, 8_000);
    let search = granted
        .communal
        .as_ref()
        .expect("a version 2 snapshot records its communal search");
    assert_eq!(search.spaces, vec![shared_space()]);
    assert_eq!(search.retriever, "in-memory");
    assert_eq!(search.query, TASK_INPUT);
    assert_eq!(granted.content_digest, granted.compute_digest());

    // Not granted the space: nothing is searched, and the snapshot is the
    // version 1 record a run without the read path writes.
    let no_space = snapshot_of_one_turn(&world, "reader-no-space", &[], &[read_capability()]).await;
    assert_eq!(no_space.schema_version().get(), 1);
    assert!(no_space.communal_claims.is_empty());
    assert!(no_space.communal.is_none());

    // Granted the space without the capability: the space is searched, and
    // the claim is not this run's to read.
    let no_capability =
        snapshot_of_one_turn(&world, "reader-no-capability", &[shared_space()], &[]).await;
    assert_eq!(no_capability.schema_version().get(), 2);
    assert!(no_capability.communal_claims.is_empty());
    assert_eq!(
        no_capability
            .communal
            .as_ref()
            .expect("the search is recorded")
            .spaces,
        vec![shared_space()]
    );

    // And a space the agent is granted that is not the claim's reads nothing
    // from the claim's space.
    let other_space = snapshot_of_one_turn(
        &world,
        "reader-other-space",
        &[KnowledgeSpaceId::new("unrelated-space").expect("the space id is valid")],
        &[read_capability()],
    )
    .await;
    assert_eq!(other_space.schema_version().get(), 2);
    assert!(other_space.communal_claims.is_empty());
}
