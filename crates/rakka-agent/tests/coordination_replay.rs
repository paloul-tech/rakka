//! Replayable coordination events: the scoped cursor and its explicit
//! retention-gap answer
//! ([specification 17.13](../../../docs/plans/rakka-agent/spec.md), scenario
//! 45).
//!
//! Scenario 45 has two halves and this file proves both. A cursor *resumes*:
//! paged one event at a time, the concatenation is the whole log in order,
//! once each, with no gap — across every scope class that keeps one, including
//! the team and conversation scopes the substrate's `<task-id>:<sequence>`
//! cursor cannot name. And a cursor that cannot resume says so: a window that
//! moved under a reader answers an explicit floor to resynchronize from, never
//! a short page the reader would mistake for the truth.
//!
//! The events themselves are not new. Every coordination transition already
//! wrote its durable record, after its own compare-and-set, on a sequence that
//! transition consumed; what is proven here is the read contract over them.

mod common;

use common::{tenant, Fixture};
use rakka_agent::testkit::{
    assert_conversation_history_store_contract, assert_task_history_store_contract,
    assert_team_history_store_contract, DeterministicModelAdapter, HistoryRetention,
    ScriptedDispatcher,
};
use rakka_agent::{
    conversation_turn_content_digest, conversation_turn_operation_id, AgentBudgetConsumption,
    AgentCapabilityId, AgentConversationCompletionRule, AgentConversationCreation,
    AgentConversationEntityCommand, AgentConversationEntityReply, AgentConversationHistoryKind,
    AgentConversationId, AgentConversationMode, AgentConversationScope,
    AgentConversationTurnSubmit, AgentCoordinationCursor, AgentCoordinationEventKind,
    AgentCoordinationReplay, AgentCoordinationSources, AgentEntityAddress, AgentGoalId, AgentId,
    AgentModerationPolicy, AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentScope,
    AgentTaskHistoryKind, AgentTaskId, AgentTaskScope, AgentTeamCreation, AgentTeamEntityCommand,
    AgentTeamEntityReply, AgentTeamHistoryKind, AgentTeamId, AgentTeamPolicy, AgentTeamScope,
    InMemoryAgentConversationHistoryStore, InMemoryAgentTaskHistoryStore,
    InMemoryAgentTeamHistoryStore,
};
use std::collections::{BTreeMap, BTreeSet};

const TEAM: &str = "support-team";
const CONVERSATION: &str = "panel-debate";
const LEADER: &str = "lead";
const MEMBER: &str = "worker";
const MODERATOR: &str = "moderator";

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
}

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(tenant(), AgentTeamId::new(TEAM).expect("the team id")).expect("the scope")
}

fn conversation_scope() -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(CONVERSATION).expect("the conversation id"),
    )
    .expect("the scope")
}

fn team_op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::TeamOperation,
        [tenant().as_str(), TEAM, discriminator],
    )
    .expect("the operation id derives")
}

fn team_creation() -> AgentTeamEntityCommand {
    let mut members: BTreeMap<AgentId, BTreeSet<AgentCapabilityId>> = BTreeMap::new();
    members.insert(agent(MEMBER), BTreeSet::new());
    AgentTeamEntityCommand::Create {
        operation_id: team_op("create"),
        creation: Box::new(AgentTeamCreation {
            leader: agent(LEADER),
            root_goal: AgentGoalId::new("quarterly-support").expect("the goal id"),
            policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
            members,
        }),
    }
}

fn conversation_creation() -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id"),
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentConversationCreation {
            moderator: agent(MODERATOR),
            participants: vec![agent("p1"), agent("p2")],
            mode: AgentConversationMode::RoundRobin,
            completion: AgentConversationCompletionRule::ModeratorDecides,
            policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
            task: AgentTaskId::new("debate-task").expect("the task id"),
            tokens: None,
            max_wall_clock_millis: None,
            transcript_ref: None,
        }),
    }
}

fn turn(round: u64, index: u32, participant: &str, body: &str) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::SubmitTurn {
        operation_id: conversation_turn_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id"),
            round,
            index,
            &agent(participant),
            &conversation_turn_content_digest(body, None),
        )
        .expect("the operation id derives"),
        submit: Box::new(AgentConversationTurnSubmit {
            round,
            turn: index,
            participant: agent(participant),
            body: body.to_string(),
            direction: None,
            usage: AgentBudgetConsumption::zero(),
        }),
    }
}

/// Drives a world that has recorded real coordination history on three scopes:
/// a task that was created and assigned, a team that was created and had a
/// member join, and a conversation that took two turns.
async fn coordinated_world() -> Fixture {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_task().await;

    let reply = fx
        .apply_team_command_at(&team_scope(), team_creation())
        .await
        .expect("the team creates");
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));
    let reply = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::AddMember {
                operation_id: team_op("join-second"),
                member: agent("second"),
                capability_scopes: BTreeSet::new(),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(common::provenance(10)),
            },
        )
        .await
        .expect("the member joins");
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));

    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), conversation_creation())
        .await
        .expect("the conversation creates");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    for (index, speaker) in ["p1", "p2"].into_iter().enumerate() {
        fx.apply_conversation_command_at(
            &conversation_scope(),
            turn(0, index as u32, speaker, "a position"),
        )
        .await
        .expect("the turn records");
    }

    // History reaches its sink on the settle pass after the transition, so
    // drive every owed flush before reading — the read contract is about what
    // the log holds, not about what an entity still owes it.
    let _ = fx.settle_task_at(&common::task_scope()).await;
    let _ = fx.settle_team_at(&team_scope()).await;
    let _ = fx.settle_conversation_at(&conversation_scope()).await;
    fx
}

fn sources(
    fx: &Fixture,
) -> AgentCoordinationSources<
    '_,
    InMemoryAgentTaskHistoryStore,
    InMemoryAgentTeamHistoryStore,
    InMemoryAgentConversationHistoryStore,
> {
    AgentCoordinationSources::new(&fx.history, &fx.team_history, &fx.conversation_history)
}

/// Pages one scope a single event at a time and returns everything the cursor
/// delivered, asserting nothing was skipped or repeated along the way.
async fn page_everything(fx: &Fixture, scope: &AgentEntityAddress) -> Vec<(u64, String)> {
    let sources = sources(fx);
    let mut cursor: Option<String> = None;
    let mut seen: Vec<(u64, String)> = Vec::new();
    for _ in 0..64 {
        let replay = sources
            .replay(&tenant(), scope, cursor.as_deref(), 1)
            .await
            .expect("the scope replays");
        let AgentCoordinationReplay::Page(page) = replay else {
            panic!("an untrimmed log never answers an expired window: {scope}");
        };
        assert_eq!(&page.scope, scope, "a page answers the scope it was asked");
        assert!(
            page.events.len() <= 1,
            "the page honors the limit it was given"
        );
        for event in &page.events {
            assert_eq!(
                event.scope, *scope,
                "every event carries the scope it was read from"
            );
            seen.push((event.sequence, event.kind.as_label()));
        }
        assert_eq!(
            page.complete_through,
            seen.last().map_or(0, |(sequence, _)| *sequence),
            "the page reports how far the reader has now got"
        );
        if !page.has_more {
            assert!(
                page.next_cursor.is_none(),
                "a final page offers no next cursor"
            );
            break;
        }
        cursor = Some(page.next_cursor.expect("more history offers a cursor"));
    }
    seen
}

fn sequences(seen: &[(u64, String)]) -> Vec<u64> {
    seen.iter().map(|(sequence, _)| *sequence).collect()
}

/// Scenario 45's positive half, across every scope class that keeps a log: a
/// cursor paged one event at a time reconstructs the whole log in order, once
/// each, with no gap — and the team and conversation scopes prove the cursor
/// generalized past the `<task-id>:<sequence>` shape that could not name them.
#[tokio::test]
async fn a_cursor_resumes_exactly_where_it_left_off_across_every_scope() {
    let fx = coordinated_world().await;

    let task = AgentEntityAddress::Task(common::task_scope());
    let team = AgentEntityAddress::Team(team_scope());
    let conversation = AgentEntityAddress::Conversation(conversation_scope());

    let task_events = page_everything(&fx, &task).await;
    let team_events = page_everything(&fx, &team).await;
    let conversation_events = page_everything(&fx, &conversation).await;

    // `expected` is anchored to what the store actually holds, never to how
    // many events the cursor happened to deliver — an expectation derived
    // from `observed.len()` would pass for any contiguous prefix, so a
    // paging bug that stopped early would ship green claiming completeness.
    for (scope, events, in_store) in [
        ("task", &task_events, fx.history.len(&common::task_scope())),
        ("team", &team_events, fx.team_history.len(&team_scope())),
        (
            "conversation",
            &conversation_events,
            fx.conversation_history.len(&conversation_scope()),
        ),
    ] {
        let observed = sequences(events);
        assert_eq!(
            observed.len(),
            in_store,
            "the cursor delivered every entry the {scope} store holds"
        );
        let expected: Vec<u64> = (1..=in_store as u64).collect();
        assert_eq!(
            observed, expected,
            "the {scope} log paged in order, once each, with no gap"
        );
        assert!(
            observed.len() > 1,
            "the {scope} world must record more than one event to prove the cursor"
        );
    }

    // The labels are the coordination vocabulary an operator filters on, and
    // they are scope-qualified precisely so the two sides of one fact stay
    // distinguishable.
    assert!(
        task_events.iter().any(|(_, label)| label == "task/created"),
        "the task log records its creation: {task_events:?}"
    );
    assert!(
        team_events
            .iter()
            .any(|(_, label)| label == "team/team-member-joined"),
        "the team log records the join: {team_events:?}"
    );
    assert_eq!(
        conversation_events
            .iter()
            .filter(|(_, label)| label == "conversation/conversation-turn-recorded")
            .count(),
        2,
        "the conversation log records both turns: {conversation_events:?}"
    );
}

/// The same cursor twice is the same page. Deduplication is proven on the write
/// side by the idempotent append; this is the read side of specification
/// 17.13's "duplicate processing MUST NOT create two logical runtime events".
#[tokio::test]
async fn a_replayed_page_is_identical_to_its_first_answer() {
    let fx = coordinated_world().await;
    let scope = AgentEntityAddress::Conversation(conversation_scope());
    let sources = sources(&fx);

    let first = sources
        .replay(&tenant(), &scope, None, 2)
        .await
        .expect("the first read succeeds");
    let again = sources
        .replay(&tenant(), &scope, None, 2)
        .await
        .expect("the replayed read succeeds");
    assert_eq!(first, again, "the same cursor answers the same page");

    // And re-driving the entity's own flush — the crash-recovery path, which
    // re-appends to the slots the transition assigned — changes nothing a
    // reader can observe.
    let _ = fx.settle_conversation_at(&conversation_scope()).await;
    let after_reflush = sources
        .replay(&tenant(), &scope, None, 2)
        .await
        .expect("the read after a re-driven flush succeeds");
    assert_eq!(
        first, after_reflush,
        "an idempotent re-flush is invisible to the cursor"
    );
}

/// A cursor is not a bearer token for whatever it names. It carries its own
/// tenant and entity, so one naming a different scope than the read addresses
/// is refused — including the substrate's own `<task-id>:<sequence>` shape,
/// which is a different vocabulary and must not be followed by accident.
#[tokio::test]
async fn a_cursor_naming_another_scope_is_refused() {
    let fx = coordinated_world().await;
    let scope = AgentEntityAddress::Task(common::task_scope());
    let sources = sources(&fx);

    let other_task = AgentTaskScope::new(
        tenant(),
        AgentTaskId::new("some-other-task").expect("the task id"),
    )
    .expect("the scope");
    let foreign = AgentCoordinationCursor::new(AgentEntityAddress::Task(other_task), 1).encode();
    let error = sources
        .replay(&tenant(), &scope, Some(&foreign), 8)
        .await
        .expect_err("a cursor naming another entity is refused");
    assert_eq!(error.code(), "coordination-cursor-scope-mismatch");

    let cross_tenant = AgentCoordinationCursor::new(
        AgentEntityAddress::Task(
            AgentTaskScope::new(
                rakka_agent::TenantId::new("other-tenant"),
                common::task_scope().task().clone(),
            )
            .expect("the scope"),
        ),
        1,
    )
    .encode();
    let error = sources
        .replay(&tenant(), &scope, Some(&cross_tenant), 8)
        .await
        .expect_err("a cursor naming another tenant is refused");
    assert_eq!(error.code(), "coordination-cursor-scope-mismatch");

    // The substrate's public task-event cursor is `<task-id>:<sequence>` with
    // no class segment, so it cannot be mistaken for a scoped one.
    let substrate_shaped = format!("{}:3", common::task_scope().task().as_str());
    let error = sources
        .replay(&tenant(), &scope, Some(&substrate_shaped), 8)
        .await
        .expect_err("the substrate's cursor shape does not cross over");
    assert_eq!(error.code(), "coordination-cursor-malformed");
}

/// The tenant fence is the shared entry point's own, not each surface's: a
/// scope key carries its own tenant, and a caller authenticated for another
/// tenant is refused before any log is consulted — whichever surface forgot
/// to pre-check.
#[tokio::test]
async fn a_scope_outside_the_authenticated_tenant_is_refused_by_the_entry_point() {
    let fx = coordinated_world().await;
    let scope = AgentEntityAddress::Task(common::task_scope());
    let sources = sources(&fx);

    let error = sources
        .replay(&rakka_agent::TenantId::new("other-tenant"), &scope, None, 8)
        .await
        .expect_err("a foreign-tenant read is refused, never served");
    assert_eq!(error.code(), "coordination-scope-foreign-tenant");
}

/// An agent entity records its lifecycle in settings revisions and audit, not
/// in a sequenced log. Answering an empty page would claim it has done nothing,
/// which is a different statement entirely, so the scope is refused by name.
#[tokio::test]
async fn a_scope_with_no_log_is_refused_rather_than_answered_empty() {
    let fx = coordinated_world().await;
    let sources = sources(&fx);

    let agent_scope = AgentEntityAddress::Agent(
        AgentScope::new(tenant(), agent("assistant")).expect("the agent scope"),
    );
    let error = sources
        .replay(&tenant(), &agent_scope, None, 8)
        .await
        .expect_err("the agent scope keeps no replayable log");
    assert_eq!(error.code(), "coordination-scope-not-replayable");

    // A run scope with no sink wired is a *different* refusal: the class does
    // keep a log, this deployment just did not wire one.
    let run_scope = AgentEntityAddress::Run(common::run_scope());
    let error = sources
        .replay(&tenant(), &run_scope, None, 8)
        .await
        .expect_err("an unwired run scope is refused explicitly");
    assert_eq!(error.code(), "coordination-run-events-unavailable");
}

/// Scenario 45's negative half. Under a bounded window the log evicts its
/// oldest entries, and a reader whose cursor predates what is left is told so
/// — with the floor to resume from — instead of being handed a page that
/// silently starts in the middle. The store conformance harness is the proof,
/// so the owed PostgreSQL backends inherit it rather than reimplement it.
#[tokio::test]
async fn an_evicted_cursor_answers_window_expired_with_a_resync_floor() {
    let scope = common::task_scope();
    assert_task_history_store_contract(
        &InMemoryAgentTaskHistoryStore::new(),
        &scope,
        HistoryRetention::Unbounded,
    )
    .await;
    assert_task_history_store_contract(
        &InMemoryAgentTaskHistoryStore::new().with_retention(10),
        &scope,
        HistoryRetention::Bounded(10),
    )
    .await;

    let team = team_scope();
    assert_team_history_store_contract(
        &InMemoryAgentTeamHistoryStore::new(),
        &team,
        HistoryRetention::Unbounded,
    )
    .await;
    assert_team_history_store_contract(
        &InMemoryAgentTeamHistoryStore::new().with_retention(6),
        &team,
        HistoryRetention::Bounded(6),
    )
    .await;

    let conversation = conversation_scope();
    assert_conversation_history_store_contract(
        &InMemoryAgentConversationHistoryStore::new(),
        &conversation,
        HistoryRetention::Unbounded,
    )
    .await;
    assert_conversation_history_store_contract(
        &InMemoryAgentConversationHistoryStore::new().with_retention(4),
        &conversation,
        HistoryRetention::Bounded(4),
    )
    .await;
}

/// The facade's own half of the retention gap: the expired-window answer names
/// a cursor, and resuming from that cursor works. A floor a reader cannot act
/// on would be no better than a short page.
#[tokio::test]
async fn the_reported_floor_is_a_cursor_the_reader_can_resume_from() {
    let fx = coordinated_world().await;
    let scope = AgentEntityAddress::Conversation(conversation_scope());

    // A store that kept only the tail of what this conversation recorded.
    let trimmed = InMemoryAgentConversationHistoryStore::new().with_retention(1);
    let whole = sources(&fx)
        .replay(&tenant(), &scope, None, 64)
        .await
        .expect("the untrimmed log reads");
    let AgentCoordinationReplay::Page(page) = whole else {
        panic!("the fixture's log is untrimmed");
    };
    assert!(page.events.len() > 1, "there is a window to lose");

    let sources = AgentCoordinationSources::new(&fx.history, &fx.team_history, &trimmed);
    for entry in rakka_agent::AgentConversationHistoryStore::read(
        &fx.conversation_history,
        &conversation_scope(),
        rakka_agent::AgentConversationHistoryCursor::start().with_limit(64),
    )
    .await
    .expect("the source log reads")
    .entries
    {
        rakka_agent::AgentConversationHistoryStore::append(&trimmed, &conversation_scope(), &entry)
            .await
            .expect("the trimmed store accepts the append");
    }

    let replay = sources
        .replay(&tenant(), &scope, None, 8)
        .await
        .expect("the trimmed log answers");
    let AgentCoordinationReplay::WindowExpired {
        oldest_retained,
        resume_from,
        ..
    } = replay
    else {
        panic!("a reader starting before a trimmed window must be told");
    };
    let floor = oldest_retained.expect("a trimmed log still retains something");
    assert_eq!(
        floor,
        page.events.last().expect("events").sequence,
        "the floor is the one entry the window kept"
    );

    let resumed = sources
        .replay(&tenant(), &scope, Some(&resume_from), 8)
        .await
        .expect("the reported floor is a legal resume point");
    let AgentCoordinationReplay::Page(resumed) = resumed else {
        panic!("resuming at the floor is not itself an expired window");
    };
    assert_eq!(
        sequences(
            &resumed
                .events
                .iter()
                .map(|event| (event.sequence, event.kind.as_label()))
                .collect::<Vec<_>>()
        ),
        vec![floor],
        "resuming at the floor delivers the floor"
    );
}

/// The merged kind vocabulary must stay injective. A task records
/// `team-claim-recorded` when it takes a board claim and a team records
/// `team-claim-recorded` when it makes one — the same fact from two sides, in
/// two logs, at two sequences. A reader filtering by label that could not tell
/// them apart would merge two different events into one.
#[test]
fn the_kind_vocabulary_is_injective_across_the_scopes_that_share_a_label() {
    let task_side = AgentCoordinationEventKind::Task(AgentTaskHistoryKind::TeamClaimRecorded);
    let team_side = AgentCoordinationEventKind::Team(AgentTeamHistoryKind::ClaimRecorded);
    assert_eq!(
        task_side.source_label(),
        team_side.source_label(),
        "the collision this guards against is real, not hypothetical"
    );
    assert_ne!(task_side.as_label(), team_side.as_label());

    // Every kind's label is scope-qualified, so no two of them can collide
    // whatever their source vocabularies do.
    let created = [
        AgentCoordinationEventKind::Task(AgentTaskHistoryKind::Created),
        AgentCoordinationEventKind::Team(AgentTeamHistoryKind::Created),
        AgentCoordinationEventKind::Conversation(AgentConversationHistoryKind::Created),
    ];
    let labels: BTreeSet<String> = created.iter().map(|kind| kind.as_label()).collect();
    assert_eq!(labels.len(), created.len(), "labels: {labels:?}");

    // The terminal-notice rows both sides of slice 5.5b record stay
    // scope-qualified too, so a replay reader tells the task's provenance
    // row from the board's close row.
    assert_eq!(
        AgentCoordinationEventKind::Task(AgentTaskHistoryKind::ConversationTerminalRecorded)
            .as_label(),
        "task/conversation-terminal-recorded"
    );
    assert_eq!(
        AgentCoordinationEventKind::Team(AgentTeamHistoryKind::TaskClosed).as_label(),
        "team/team-task-closed"
    );
}
