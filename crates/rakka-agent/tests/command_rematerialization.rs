//! The command path re-answers a refusal from durable state.
//!
//! Every sharded entity has two writers by construction — a resident facade
//! holding its cache for the whole residency, and the A2A service (or a
//! peer's courier transport) reaching the same durable record through its
//! *own* store handle. A command that **commits** is fenced by its own
//! compare-and-set: it is written at the cached record's revision, so a
//! writer that moved the record first makes the write lose, drops the cache,
//! and the retry re-reads. A command that is **refused** writes nothing, so
//! that fence never engages — and the refusal it hands back carries a
//! definitive-looking code the caller is entitled to rely on.
//!
//! These are the parities for that second class: a fence answered from a
//! cache the other writer has already moved past must be re-answered against
//! a re-materialized record, not stand for the rest of the residency. The
//! settle-pass halves live in `settle_rematerialization.rs`.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{tenant, Fixture, TENANT};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_turn_content_digest, conversation_turn_operation_id, AgentBudgetConsumption,
    AgentCapabilityId, AgentConversationCompletionRule, AgentConversationCreation,
    AgentConversationEntityCommand, AgentConversationEntityReply, AgentConversationId,
    AgentConversationMode, AgentConversationScope, AgentConversationTurnSubmit, AgentGoalId,
    AgentId, AgentModerationPolicy, AgentOperationId, AgentOperationKind, AgentRevisionNumber,
    AgentTaskId, AgentTeamCreation, AgentTeamEntityCommand, AgentTeamEntityReply, AgentTeamId,
    AgentTeamPolicy, AgentTeamScope,
};

const CONVERSATION: &str = "panel-debate";
const MODERATOR: &str = "moderator";
const TEAM: &str = "support-team";
const LEADER: &str = "lead";
const MEMBER: &str = "worker-a";
const BOARD_TASK: &str = "board-ticket";

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
}

fn conversation_scope() -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(
        tenant(),
        AgentTeamId::new(TEAM).expect("the team id is valid"),
    )
    .expect("the team scope is valid")
}

fn create_conversation() -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentConversationCreation {
            moderator: agent(MODERATOR),
            participants: vec![agent("p1"), agent("p2")],
            mode: AgentConversationMode::RoundRobin,
            completion: AgentConversationCompletionRule::ModeratorDecides,
            policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
            task: AgentTaskId::new("debate-task").expect("the task id is valid"),
            tokens: None,
            max_wall_clock_millis: None,
            transcript_ref: None,
        }),
    }
}

fn submit(round: u64, turn: u32, participant: &str, body: &str) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::SubmitTurn {
        operation_id: conversation_turn_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
            round,
            turn,
            &agent(participant),
            &conversation_turn_content_digest(body, None),
        )
        .expect("the operation id derives"),
        submit: Box::new(AgentConversationTurnSubmit {
            round,
            turn,
            participant: agent(participant),
            body: body.to_string(),
            direction: None,
            usage: AgentBudgetConsumption::zero(),
        }),
    }
}

fn team_op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::TeamOperation,
        [tenant().as_str(), TEAM, discriminator],
    )
    .expect("the operation id derives")
}

fn create_team() -> AgentTeamEntityCommand {
    let mut members: BTreeMap<AgentId, BTreeSet<AgentCapabilityId>> = BTreeMap::new();
    members.insert(agent(MEMBER), BTreeSet::new());
    AgentTeamEntityCommand::Create {
        operation_id: team_op("create"),
        creation: Box::new(AgentTeamCreation {
            leader: agent(LEADER),
            root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
            policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
            members,
        }),
    }
}

fn post_task() -> AgentTeamEntityCommand {
    AgentTeamEntityCommand::PostTask {
        operation_id: team_op("post"),
        task: AgentTaskId::new(BOARD_TASK).expect("the task id is valid"),
        posted_by: agent(LEADER),
    }
}

fn claim() -> AgentTeamEntityCommand {
    AgentTeamEntityCommand::Claim {
        operation_id: team_op("claim"),
        task: AgentTaskId::new(BOARD_TASK).expect("the task id is valid"),
        member: agent(MEMBER),
        expected_epoch: 0,
    }
}

/// The conversation's cursor fence: a wire turn commits through the service's
/// own store handle while a resident facade idles on a stale cursor. The
/// rightful next speaker's turn reaches the resident, whose cache still
/// expects the previous speaker — and before the fix that refusal, having
/// written nothing, never corrected itself: the correct turn was refused
/// `conversation-turn-out-of-order` for the rest of the residency, with a
/// code the caller reads as definitive.
#[tokio::test]
async fn a_stale_cursor_cannot_refuse_the_rightful_next_turn() {
    let fx = fixture();
    fx.instantiate_conversation_participants(&[MODERATOR, "p1", "p2"])
        .await;
    fx.apply_conversation_command_at(&conversation_scope(), create_conversation())
        .await
        .expect("the conversation creates");

    // The resident materializes and goes idle holding a clean cache:
    // cursor (0, 0), p1 to speak.
    let mut resident = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.agents.clone(),
        fx.conversation_history.clone(),
    );
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // The other writer commits p1's turn: the durable cursor is now (0, 1),
    // p2 to speak. Nothing tells the resident.
    fx.apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "position"))
        .await
        .expect("the wire turn commits");

    // The rightful next speaker reaches the resident directly — no sweep, no
    // passivation, no intervening command to heal the cache. The stale cursor
    // says it is p1's turn 0; durable state says otherwise, and durable state
    // is the answer the caller must get.
    let reply = resident
        .apply(submit(0, 1, "p2", "response"), &fx.router, fx.now())
        .await
        .expect("the rightful turn is admitted against the durable cursor");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.turns.len(), 2, "both turns stand durably");
}

/// The same conversation fence, in the direction that must *not* change: a
/// turn the durable record genuinely refuses is still refused after the
/// re-materialized second look. The re-verification is a correction, never an
/// admission — and it costs exactly one extra read before answering.
#[tokio::test]
async fn a_genuinely_out_of_order_turn_is_still_refused() {
    let fx = fixture();
    fx.instantiate_conversation_participants(&[MODERATOR, "p1", "p2"])
        .await;
    fx.apply_conversation_command_at(&conversation_scope(), create_conversation())
        .await
        .expect("the conversation creates");

    let mut resident = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.agents.clone(),
        fx.conversation_history.clone(),
    );
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // p2 speaks out of turn against a cache that is perfectly fresh: the
    // durable record agrees with it, so the refusal stands.
    let refused = resident
        .apply(submit(0, 1, "p2", "jumping in"), &fx.router, fx.now())
        .await;
    assert!(
        refused.is_err(),
        "a fresh record still refuses an out-of-order turn: {refused:?}"
    );
}

/// The board's parity: a task is posted through the service's own store
/// handle while a resident board idles on a cache that predates the entry. A
/// member's claim reaches the resident, whose stale board holds no such entry
/// — before the fix the claim was refused against a board that durably had
/// the work, and the entry sat unclaimed with the refusal writing nothing to
/// correct it.
#[tokio::test]
async fn a_stale_board_cannot_refuse_a_claim_for_a_posted_task() {
    let fx = fixture();
    fx.apply_team_command_at(&team_scope(), create_team())
        .await
        .expect("the team creates");

    // The resident board materializes and goes idle: no entries.
    let mut resident = rakka_agent::AgentTeamEntityStore::new(
        team_scope(),
        fx.teams.clone(),
        fx.team_history.clone(),
    );
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // The other writer posts the task; the resident's cache never hears.
    fx.apply_team_command_at(&team_scope(), post_task())
        .await
        .expect("the wire post commits");

    // The claim reaches the resident, whose stale board holds no entry for
    // this task. Durable state does, so the claim must be arbitrated.
    let reply = resident
        .apply(claim(), &fx.router, fx.now())
        .await
        .expect("the claim is arbitrated against the durable board");
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));

    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the board snapshots");
    assert!(
        snapshot
            .board
            .iter()
            .any(|entry| entry.task.as_str() == BOARD_TASK),
        "the posted entry stands on the durable board"
    );
}

/// The task's parity: a creation commits through the service's own store
/// handle while a resident facade holds a cache from before the task
/// existed. A cancellation reaches the resident, whose stale cache holds no
/// task at all — before the fix it was refused `task-not-created` against a
/// task that durably exists, and the refusal, writing nothing, left that
/// answer standing for the rest of the residency.
#[tokio::test]
async fn a_stale_task_facade_cannot_refuse_a_command_for_a_created_task() {
    let fx = fixture();
    fx.instantiate_agent().await;

    // The resident materializes before the task exists: its cache holds no
    // task at all.
    let mut resident = rakka_agent::AgentTaskEntityStore::new(
        common::task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    let _ = resident.recover(fx.now()).await;

    // The other writer creates the task; nothing tells the resident.
    fx.apply_task_command_at(&common::task_scope(), create_task_command("1"))
        .await
        .expect("the wire creation commits");

    // A cancellation reaches the resident, whose stale cache says there is
    // no such task. Durable state says there is, and that is the record the
    // command must be answered against.
    let reply = resident
        .apply(
            rakka_agent::AgentTaskEntityCommand::Cancel {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::Cancellation,
                    [TENANT, common::task_scope().task().as_str(), "cancel-1"],
                )
                .expect("the operation id derives"),
                reason: "withdrawn upstream".to_string(),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the cancellation is answered against the durable record");
    assert!(matches!(
        reply,
        rakka_agent::AgentTaskEntityReply::Applied { .. }
    ));
}

fn create_task_command(discriminator: &str) -> rakka_agent::AgentTaskEntityCommand {
    rakka_agent::AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(
            AgentOperationKind::TaskCreation,
            [TENANT, common::task_scope().task().as_str(), discriminator],
        )
        .expect("the operation id derives"),
        creation: Box::new(rakka_agent::AgentTaskCreation {
            definition: common::task_definition(),
            input: rakka_agent::AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(common::agent_id()),
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
    }
}
