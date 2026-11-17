//! The communal read path ranked by meaning, end to end: real sharded agent,
//! task and run entities, the production dispatcher, and the knowledge
//! graph's own adapters with an embedder wired into two of them
//! ([specification 13.4, 13.5 and 16](../../../docs/plans/rakka-agent/spec.md);
//! issue #140).
//!
//! One agent appends a claim through its own run, as a committed effect the
//! dispatcher executes. The append executor embeds it. A reviewer verifies
//! it. Another agent, granted the claim's space, then asks about the same
//! thing in words the claim does not use, and its turn is computed from the
//! claim.
//!
//! The same walk in a world that ranks by shared words selects nothing,
//! which is the whole reason this path exists.

use std::collections::BTreeSet;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    claim_append_operation_id, registered_agent_run_entity_ref, registered_agent_task_entity_ref,
    run_id_for_assignment, AgentAssignmentGeneration, AgentAuthorityEnvelope,
    AgentClaimAppendRequest, AgentClaimObjectRequest, AgentCommunalClaimTrust,
    AgentContextSnapshotRef, AgentId, AgentMemoryEmbedder, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentRunEntityCommand, AgentRunEntityMessage,
    AgentRunEntityReply, AgentRunScope, AgentSchemaPolicy, AgentScope, AgentTaskContent,
    AgentTaskCreation, AgentTaskDefinition, AgentTaskEntityCommand, AgentTaskEntityMessage,
    AgentTaskEntityReply, AgentTaskId, AgentTaskScope, CommunalClaimObject, ContextSnapshotStore,
    KnowledgeSpaceId, MemoryClassification, MemoryContextSnapshot, TenantId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimCursor, ClaimFilter, ClaimOperationId, ClaimPromotionPolicy, ClaimTrustStatus,
    ClaimTrustTransitionRequest, KnowledgeGraphStore, KnowledgeSpaceScope,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use rakka_example_multi_agent_goal_acceptance::flow::{
    ask_retrying, instantiate, mission_definition, pump,
};
use rakka_example_multi_agent_goal_acceptance::wiring::{World, TENANT};

const SHARED_SPACE: &str = "fleet-notes";
/// What the writer states. It shares no word with [`READER_INPUT`].
const CLAIM_TEXT: &str = "the car is at the depot";
/// The writer's own task.
const WRITER_INPUT: &str = "note the fleet";
/// What the reader asks. "automobile" and "car" are one concept to the
/// world's embedder, and no word here is a word of the claim.
const READER_INPUT: &str = "automobile whereabouts";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn shared_space() -> KnowledgeSpaceId {
    KnowledgeSpaceId::new(SHARED_SPACE).expect("the space id is valid")
}

fn space_scope() -> KnowledgeSpaceScope {
    KnowledgeSpaceScope::new(tenant(), shared_space()).expect("the scope is valid")
}

fn principal(id: &str) -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: id.to_string(),
        display_name: None,
    }
}

/// Instantiates `agent` under a definition that grants `spaces`, runs one task with `input` assigned to it through one model turn to
/// its end, and returns the task and run that did it.
async fn run_one_turn(
    world: &World,
    agent: &str,
    input: &str,
    spaces: &[KnowledgeSpaceId],
) -> (AgentTaskScope, AgentRunScope, DeterministicModelAdapter) {
    let definition: AgentTaskDefinition = mission_definition();
    let agent_id = AgentId::new(agent).expect("the agent id is valid");
    let agent_scope = AgentScope::new(tenant(), agent_id.clone()).expect("the scope is valid");
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(definition.definition_id.clone());
    envelope.knowledge_spaces.extend(spaces.iter().cloned());
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
                    input: AgentTaskContent::inline(serde_json::json!(input))
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
                AgentTaskContent::inline(serde_json::json!({ "answer": "noted" }))
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
    (task_scope, run_scope, adapter)
}

/// The context snapshot a run's first turn was computed from, as the
/// snapshot store holds it.
async fn first_turn_snapshot(world: &World, run_scope: &AgentRunScope) -> MemoryContextSnapshot {
    world
        .snapshots
        .load(
            run_scope,
            &AgentContextSnapshotRef::for_turn(run_scope, 1).expect("the reference derives"),
        )
        .await
        .expect("the snapshot store answers")
        .expect("the turn's snapshot was persisted before its model call")
}

/// The writer agent states [`CLAIM_TEXT`] through its own run: a committed
/// `ClaimAppend` effect that the production dispatcher executes. A reviewer
/// then verifies the claim on the store, as the world's read policy admits
/// verified claims only. Returns the claim as the graph holds it.
async fn written_and_verified_claim(world: &World) -> Claim {
    // The writer's definition grants the space: the dispatcher refuses an
    // append into a space its agent was never given
    // (`widened-knowledge-access`).
    let (task_scope, run_scope, adapter) =
        run_one_turn(world, "writer", WRITER_INPUT, &[shared_space()]).await;
    let run = registered_agent_run_entity_ref(&world.run_registration, &run_scope);
    // The run has ended. It still accepts a claim append inside its
    // post-terminal window.
    let appended = ask_retrying(
        &run,
        |reply_to| AgentRunEntityMessage::Command {
            command: Box::new(AgentRunEntityCommand::AppendClaim {
                operation_id: claim_append_operation_id(&run_scope, "fleet-note-1")
                    .expect("the operation id derives"),
                append: Box::new(AgentClaimAppendRequest {
                    space: shared_space(),
                    subject: "fleet-note".to_string(),
                    predicate: "reports".to_string(),
                    object: AgentClaimObjectRequest::Value(
                        AgentTaskContent::inline(serde_json::json!(CLAIM_TEXT))
                            .expect("the claim text is inline-bounded"),
                    ),
                    confidence_bps: 8_000,
                    classification: MemoryClassification::Unclassified,
                    evidence: Vec::new(),
                    requested_by: principal("writer-runtime"),
                }),
            }),
            reply_to,
        },
        "append-claim",
    )
    .await;
    assert!(
        matches!(appended, AgentRunEntityReply::Applied { .. }),
        "the claim append applies, got {appended:?}"
    );
    // The dispatcher executes the committed effect.
    pump(world, &task_scope, &run_scope, &adapter, true).await;

    let scope = space_scope();
    let stored = world
        .graph
        .query(&scope, &ClaimFilter::matching_all(), ClaimCursor::start())
        .await
        .expect("the graph answers");
    assert_eq!(
        stored.claims.len(),
        1,
        "the append effect was executed against the graph"
    );
    let claim = stored.claims[0].clone();
    assert_eq!(claim.provenance.agent.as_str(), "writer");

    world
        .graph
        .transition(
            &scope,
            &ClaimTrustTransitionRequest::new(
                claim.claim_id.clone(),
                ClaimOperationId::derive_transition(&scope, &claim.claim_id, "verify")
                    .expect("the operation id derives"),
                ClaimTrustStatus::Verified,
                principal("fleet-reviewer"),
                AgentTimestampMillis::new(2),
            ),
            &ClaimPromotionPolicy::ungated(),
            AgentTimestampMillis::new(2),
        )
        .await
        .expect("the claim is verified");
    claim
}

#[tokio::test]
async fn a_run_reads_a_claim_another_agent_wrote_in_other_words() {
    let world = World::with_semantic_read_path();
    let embedder = world
        .embedder
        .clone()
        .expect("the semantic world wires an embedder");
    let claim = written_and_verified_claim(&world).await;

    // The append effect embedded the claim. No pass ran, and none is owed.
    assert!(
        world.graph.has_embedding(&space_scope(), &claim.claim_id),
        "the append executor embedded the fresh claim"
    );
    let owed = world
        .graph
        .unembedded_claims(
            &space_scope(),
            &embedder.embedding_ref(),
            &BTreeSet::from([MemoryClassification::Unclassified]),
            None,
            8,
        )
        .await
        .expect("the page answers");
    assert!(owed.claims.is_empty());
    // Everything the embedder was sent so far, in order: the query of the
    // writer's own turn, which searched the space it is granted, and then
    // the claim's text. Wiring an embedder sends both kinds of text.
    assert_eq!(
        embedder.calls(),
        vec![
            WRITER_INPUT.to_string(),
            format!("fleet-note reports {CLAIM_TEXT}")
        ]
    );

    // The reader asks about the same thing in words the claim does not use.
    let (_, reader_run, _) = run_one_turn(&world, "reader", READER_INPUT, &[shared_space()]).await;
    let snapshot = first_turn_snapshot(&world, &reader_run).await;
    assert_eq!(snapshot.communal_claims.len(), 1);
    let selected = &snapshot.communal_claims[0];
    assert_eq!(selected.claim_id.as_str(), claim.claim_id.as_str());
    assert_eq!(selected.space, shared_space());
    assert_eq!(selected.trust, AgentCommunalClaimTrust::Verified);
    assert_eq!(selected.statement.subject, "fleet-note");
    assert_eq!(
        selected.statement.object,
        CommunalClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!(CLAIM_TEXT)).expect("inline")
        )
    );
    assert_eq!(selected.provenance.agent.as_str(), "writer");
    let search = snapshot
        .communal
        .as_ref()
        .expect("a snapshot that searched records its communal search");
    assert_eq!(search.query, READER_INPUT);
    assert_eq!(search.retriever, "in-memory");
    assert_eq!(
        search.retriever_version,
        AgentRevisionNumber::new(2),
        "the snapshot records that the ranking was by meaning"
    );
    assert!(!search.index_incomplete);
    assert_eq!(snapshot.content_digest, snapshot.compute_digest());
    assert!(
        embedder.calls().contains(&READER_INPUT.to_string()),
        "the reader's query was embedded"
    );
}

#[tokio::test]
async fn the_same_walk_ranked_by_words_selects_nothing() {
    // The control: the same claim, the same question, a world whose
    // retriever ranks by shared words. The two share none.
    let world = World::with_communal_read_path();
    assert!(world.embedder.is_none());
    let claim = written_and_verified_claim(&world).await;
    assert!(!world.graph.has_embedding(&space_scope(), &claim.claim_id));

    let (_, reader_run, _) = run_one_turn(&world, "reader", READER_INPUT, &[shared_space()]).await;
    let snapshot = first_turn_snapshot(&world, &reader_run).await;
    assert!(
        snapshot.communal_claims.is_empty(),
        "a word search cannot find a claim that shares no word with the query"
    );
    let search = snapshot
        .communal
        .as_ref()
        .expect("a snapshot that searched records its communal search");
    assert_eq!(search.retriever_version, AgentRevisionNumber::new(1));
}
