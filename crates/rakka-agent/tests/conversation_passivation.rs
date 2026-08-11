//! The turn protocol is durable data, not a resident coordinator
//! ([specification 8.11 and 15](../../../docs/plans/rakka-agent/spec.md),
//! scenario 43 over real sharded entities): moderation recovers participant,
//! round, turn owner, transcript reference, and budgets after passivation
//! and shard-entity reactivation without duplicating a turn.

mod common;

use std::time::Duration;

use common::{ConversationStore, ShardedWorld};
use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    conversation_turn_content_digest, conversation_turn_operation_id,
    passivate_agent_conversation_entity, AgentBudgetConsumption, AgentConversationCompletionRule,
    AgentConversationCreation, AgentConversationDirection, AgentConversationEntityCommand,
    AgentConversationEntityMessage, AgentConversationEntityReply, AgentConversationId,
    AgentConversationMode, AgentConversationScope, AgentConversationStatus,
    AgentConversationTurnSubmit, AgentId, AgentModerationPolicy, AgentRevisionNumber, AgentTaskId,
    TenantId,
};

const TENANT: &str = "acme";
const MODERATOR: &str = "moderator";
const ASK_TIMEOUT: Duration = Duration::from_secs(5);

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
}

fn conversation_scope(name: &str) -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(name).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn create_command(
    name: &str,
    mode: AgentConversationMode,
    participants: &[&str],
) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(
            &tenant(),
            &AgentConversationId::new(name).expect("the conversation id is valid"),
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentConversationCreation {
            moderator: agent(MODERATOR),
            participants: participants.iter().map(|name| agent(name)).collect(),
            mode,
            completion: AgentConversationCompletionRule::ModeratorDecides,
            policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
            task: AgentTaskId::new("moderated-task").expect("the task id is valid"),
            tokens: Some(500),
            max_wall_clock_millis: Some(1_000_000),
            transcript_ref: Some("artifact://transcripts/moderated".to_string()),
        }),
    }
}

fn submit(
    name: &str,
    round: u64,
    turn: u32,
    participant: &str,
    body: &str,
    tokens: u64,
    direction: Option<AgentConversationDirection>,
) -> AgentConversationEntityCommand {
    let mut usage = AgentBudgetConsumption::zero();
    usage.tokens = tokens;
    AgentConversationEntityCommand::SubmitTurn {
        operation_id: conversation_turn_operation_id(
            &tenant(),
            &AgentConversationId::new(name).expect("the conversation id is valid"),
            round,
            turn,
            &agent(participant),
            &conversation_turn_content_digest(body, direction.as_ref()),
        )
        .expect("the operation id derives"),
        submit: Box::new(AgentConversationTurnSubmit {
            round,
            turn,
            participant: agent(participant),
            body: body.to_string(),
            direction,
            usage,
        }),
    }
}

async fn apply(
    world: &ShardedWorld,
    scope: &AgentConversationScope,
    command: AgentConversationEntityCommand,
) -> AgentConversationEntityReply {
    world
        .conversation_ref(scope)
        .ask(
            |reply_to| AgentConversationEntityMessage::Command {
                command: Box::new(command),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded conversation replies")
}

async fn describe(
    world: &ShardedWorld,
    scope: &AgentConversationScope,
) -> rakka_agent::AgentConversationSnapshot {
    let reply = apply(world, scope, AgentConversationEntityCommand::Describe).await;
    let AgentConversationEntityReply::Snapshot(Some(snapshot)) = reply else {
        panic!("the conversation snapshots, got {reply:?}");
    };
    *snapshot
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moderation_recovers_the_protocol_across_passivation_without_duplicating_a_turn() {
    let world = ShardedWorld::new(
        "conversation-passivation",
        Duration::from_secs(60),
        ScriptedDispatcher::new(),
        None,
    );
    let scope = conversation_scope("design-review");

    // Trusted wiring creates the conversation; two turns land with reported
    // usage.
    let reply = apply(
        &world,
        &scope,
        create_command(
            "design-review",
            AgentConversationMode::RoundRobin,
            &["p1", "p2", "p3"],
        ),
    )
    .await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let first = submit("design-review", 0, 0, "p1", "the proposal", 10, None);
    let reply = apply(&world, &scope, first.clone()).await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let second = submit("design-review", 0, 1, "p2", "a concern", 15, None);
    let reply = apply(&world, &scope, second.clone()).await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    // Everything passivates: the mid-round protocol holds no actor, future,
    // or timer resident — the protocol is data.
    assert!(passivate_agent_conversation_entity(
        &world.sharding,
        world.conversation_registration.key(),
        &scope
    )
    .expect("the conversation passivates"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        world.resident_entities(),
        0,
        "the mid-round conversation keeps nothing resident"
    );

    // Scenario 43's five nouns, recovered from durable state alone:
    // participant roster, round cursor, turn owner, transcript reference,
    // and budgets.
    let snapshot = describe(&world, &scope).await;
    assert_eq!(snapshot.status, AgentConversationStatus::Active);
    assert_eq!(
        snapshot.participants,
        vec![agent("p1"), agent("p2"), agent("p3")],
        "the roster recovers in order"
    );
    assert_eq!(snapshot.round, 0);
    assert_eq!(snapshot.turn_in_round, 2);
    assert_eq!(
        snapshot.current_speaker,
        Some(agent("p3")),
        "the turn owner re-derives from the durable cursor"
    );
    assert_eq!(
        snapshot.transcript_ref.as_deref(),
        Some("artifact://transcripts/moderated")
    );
    assert_eq!(snapshot.budgets.tokens, Some(500));
    assert_eq!(snapshot.budgets.consumed.tokens, 25);
    assert!(snapshot.budgets.deadline.is_some());
    assert_eq!(snapshot.turns.len(), 2);

    // The redelivered turn converges without a second record — scenario
    // 43's "without duplicating a turn" — and its usage is not re-charged.
    let replay = apply(&world, &scope, second).await;
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));
    let snapshot = describe(&world, &scope).await;
    assert_eq!(snapshot.turns.len(), 2, "the replay recorded nothing");
    assert_eq!(snapshot.budgets.consumed.tokens, 25);

    // And the protocol resumes: the recovered owner's fresh turn lands.
    let reply = apply(
        &world,
        &scope,
        submit("design-review", 0, 2, "p3", "a resolution", 5, None),
    )
    .await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let snapshot = describe(&world, &scope).await;
    assert_eq!(snapshot.round, 1, "the recovered round closed normally");
    assert_eq!(snapshot.budgets.consumed.tokens, 30);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_designation_survives_passivation() {
    // The moderator-directed owner is the one owner fact that cannot be
    // re-derived: it is a durable decision, and it must survive the round
    // trip like every other.
    let world = ShardedWorld::new(
        "conversation-designation",
        Duration::from_secs(60),
        ScriptedDispatcher::new(),
        None,
    );
    let scope = conversation_scope("interview");

    let reply = apply(
        &world,
        &scope,
        create_command(
            "interview",
            AgentConversationMode::ModeratorDirected,
            &["p1", "p2"],
        ),
    )
    .await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let reply = apply(
        &world,
        &scope,
        submit(
            "interview",
            0,
            0,
            MODERATOR,
            "p2, your view",
            0,
            Some(AgentConversationDirection::Designate(agent("p2"))),
        ),
    )
    .await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    assert!(passivate_agent_conversation_entity(
        &world.sharding,
        world.conversation_registration.key(),
        &scope
    )
    .expect("the conversation passivates"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(world.resident_entities(), 0);

    let snapshot = describe(&world, &scope).await;
    assert_eq!(snapshot.designated, Some(agent("p2")));
    assert_eq!(snapshot.current_speaker, Some(agent("p2")));

    // The recovered designation is the fence: the wrong participant still
    // refuses, and the designated one proceeds.
    let reply = apply(
        &world,
        &scope,
        submit("interview", 0, 1, "p1", "interjecting", 0, None),
    )
    .await;
    let AgentConversationEntityReply::Rejected { code, .. } = reply else {
        panic!("the undesignated participant is refused, got {reply:?}");
    };
    assert_eq!(code, "conversation-not-your-turn");
    let reply = apply(
        &world,
        &scope,
        submit("interview", 0, 1, "p2", "my view", 0, None),
    )
    .await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_conversation_passivates_on_its_own() {
    // A short idle window: the created conversation must leave residency
    // without any explicit command — no resident coordinator, ever.
    let world = ShardedWorld::new(
        "conversation-idle-passivation",
        Duration::from_millis(50),
        ScriptedDispatcher::new(),
        None,
    );
    let scope = conversation_scope("idle-review");
    let reply = apply(
        &world,
        &scope,
        create_command("idle-review", AgentConversationMode::RoundRobin, &["p1"]),
    )
    .await;
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    assert!(world.resident_entities() >= 1);

    let mut waited = Duration::ZERO;
    while world.resident_entities() > 0 && waited < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(25)).await;
        waited += Duration::from_millis(25);
    }
    assert_eq!(
        world.resident_entities(),
        0,
        "the idle conversation passivated on its own"
    );

    // And it answers again from durable state alone.
    let snapshot = describe(&world, &scope).await;
    assert_eq!(snapshot.moderator, agent(MODERATOR));
}

// Keep the unused-import lint honest: the sharded world's conversation store
// type is part of the fixture surface this file exercises.
#[allow(dead_code)]
fn _store_type(_: &ConversationStore) {}
