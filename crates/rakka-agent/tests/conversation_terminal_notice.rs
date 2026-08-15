//! The conversation → task terminal notice
//! ([specification 8.11 and 9.8](../../../docs/plans/rakka-agent/spec.md)):
//! every terminal flip — rounds complete, moderator early end, lazy expiry —
//! owes the governing task a notice in its own compare-and-set, and the task
//! records the bounded conversation provenance cell that makes the
//! terminated conversation observable from it.
//!
//! The sweeps copy the conversation-recovery discipline: each iteration
//! builds a fresh world, arms exactly one store at one write, drives to the
//! loss, survives, and re-drives the same operation ids.

mod common;

use std::sync::atomic::Ordering;

use common::{task_scope, tenant, Fixture, TENANT};
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_end_operation_id, conversation_turn_content_digest,
    conversation_turn_operation_id, AgentBudgetConsumption, AgentConversationCompletionRule,
    AgentConversationCreation, AgentConversationEntityCommand, AgentConversationId,
    AgentConversationMode, AgentConversationScope, AgentConversationStatus,
    AgentConversationTerminalReason, AgentConversationTurnSubmit, AgentId, AgentModerationPolicy,
    AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentTaskHistoryKind,
    AgentTaskStatus,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

const CONVERSATION: &str = "standup-review";
const MODERATOR: &str = "moderator";

fn conversation_scope() -> AgentConversationScope {
    scope_for(CONVERSATION)
}

fn scope_for(conversation: &str) -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(conversation).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
}

fn provenance(at: u64) -> rakka_agent::AgentRevisionProvenance {
    rakka_agent::AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator-7".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

/// A one-round, two-participant creation whose final turn completes it.
fn create_command(
    conversation: &str,
    completion: AgentConversationCompletionRule,
    max_wall_clock_millis: Option<u64>,
) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(
            &tenant(),
            &AgentConversationId::new(conversation).expect("the conversation id is valid"),
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentConversationCreation {
            moderator: agent(MODERATOR),
            participants: vec![agent("p1"), agent("p2")],
            mode: AgentConversationMode::RoundRobin,
            completion,
            policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_rounds(1),
            task: task_scope().task().clone(),
            tokens: Some(1_000),
            max_wall_clock_millis,
            transcript_ref: None,
        }),
    }
}

fn submit(
    conversation: &str,
    round: u64,
    turn: u32,
    participant: &str,
    body: &str,
) -> AgentConversationEntityCommand {
    let mut usage = AgentBudgetConsumption::zero();
    usage.tokens = 10;
    AgentConversationEntityCommand::SubmitTurn {
        operation_id: conversation_turn_operation_id(
            &tenant(),
            &AgentConversationId::new(conversation).expect("the conversation id is valid"),
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
            usage,
        }),
    }
}

fn end_command(conversation: &str, expected_round: u64) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::EndEarly {
        operation_id: conversation_end_operation_id(
            &tenant(),
            &AgentConversationId::new(conversation).expect("the conversation id is valid"),
            expected_round,
            "resolved early",
        )
        .expect("the operation id derives"),
        moderator: agent(MODERATOR),
        expected_round,
        reason: "resolved early".to_string(),
        provenance: Box::new(provenance(1)),
    }
}

/// Builds the world: the governing task exists, and the conversation is
/// created against it under the given completion rule.
async fn world(
    completion: AgentConversationCompletionRule,
    max_wall_clock_millis: Option<u64>,
) -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.apply_conversation_command_at(
        &conversation_scope(),
        create_command(CONVERSATION, completion, max_wall_clock_millis),
    )
    .await
    .expect("the conversation creates");
    fx
}

/// Submits round 0's two turns — under `AllRounds` with one permitted
/// round, the second completes the conversation — and settles the passes
/// that deliver what the flips owed, tolerating injected crashes.
async fn drive_completion(fx: &Fixture) {
    let _ = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(CONVERSATION, 0, 0, "p1", "opening"),
        )
        .await;
    let _ = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(CONVERSATION, 0, 1, "p2", "closing"),
        )
        .await;
    for _round in 0..3 {
        let _ = fx.settle_conversation_at(&conversation_scope()).await;
        let _ = fx.settle_task_at(&task_scope()).await;
    }
}

/// How many task-history rows of one kind the sink holds.
async fn task_history_count(fx: &Fixture, kind: AgentTaskHistoryKind) -> usize {
    let mut count = 0;
    let mut cursor = Some(rakka_agent::AgentTaskHistoryCursor::start());
    while let Some(position) = cursor {
        let page = rakka_agent::AgentTaskHistoryStore::read(&fx.history, &task_scope(), position)
            .await
            .expect("the task history reads");
        count += page
            .entries
            .iter()
            .filter(|entry| entry.kind == kind)
            .count();
        cursor = page.next;
    }
    count
}

/// Re-drives after survival until quiescent, then asserts the converged
/// truth: the conversation ended once, the cell recorded once, both markers
/// durable.
async fn assert_converged(fx: &Fixture) {
    // The retried commands either apply fresh (the crash preceded their
    // commit) or answer from the operation log and the ledger — both
    // converge.
    drive_completion(fx).await;
    drive_completion(fx).await;

    let conversation = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(conversation.status, AgentConversationStatus::Ended);
    assert!(
        conversation.terminal_notice_settled,
        "the notice marker settled"
    );

    let task = fx.task_snapshot().await;
    assert_eq!(task.conversations, 1, "exactly one notice recorded");
    let cell = task.conversation.expect("the provenance cell stands");
    assert_eq!(cell.conversation.as_str(), CONVERSATION);
    assert_eq!(cell.status, AgentConversationStatus::Ended);
    assert_eq!(
        cell.terminal_reason,
        AgentConversationTerminalReason::RoundsComplete
    );
    assert_eq!(
        task_history_count(fx, AgentTaskHistoryKind::ConversationTerminalRecorded).await,
        1,
        "exactly one provenance row across the loss"
    );
}

#[tokio::test]
async fn a_completed_conversation_is_observable_from_its_governing_task() {
    // The done-when, on the AllRounds completion flip.
    let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
    drive_completion(&fx).await;

    let conversation = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(conversation.status, AgentConversationStatus::Ended);
    assert!(conversation.terminal_notice_settled);

    let task = fx.task_snapshot().await;
    assert_eq!(task.conversations, 1);
    let cell = task.conversation.expect("the provenance cell stands");
    assert_eq!(cell.conversation.as_str(), CONVERSATION);
    assert_eq!(cell.status, AgentConversationStatus::Ended);
    assert_eq!(
        cell.terminal_reason,
        AgentConversationTerminalReason::RoundsComplete
    );
    assert_eq!(cell.turns, 2, "the coordinates rode the notice");
    assert_eq!(
        task_history_count(&fx, AgentTaskHistoryKind::ConversationTerminalRecorded).await,
        1
    );
}

#[tokio::test]
async fn a_moderator_ended_conversation_reports_its_own_reason() {
    let fx = world(AgentConversationCompletionRule::ModeratorDecides, None).await;
    fx.apply_conversation_command_at(&conversation_scope(), end_command(CONVERSATION, 0))
        .await
        .expect("the early end applies");
    for _round in 0..3 {
        let _ = fx.settle_conversation_at(&conversation_scope()).await;
    }

    let task = fx.task_snapshot().await;
    let cell = task.conversation.expect("the provenance cell stands");
    assert_eq!(cell.status, AgentConversationStatus::Ended);
    assert_eq!(
        cell.terminal_reason,
        AgentConversationTerminalReason::ModeratorEnded
    );
}

#[tokio::test]
async fn an_expired_conversation_reports_through_the_lazy_flip() {
    // The expiry flip commits in the settle pass's own compare-and-set; the
    // notice rides that same commit and delivers in the same pass.
    let fx = world(
        AgentConversationCompletionRule::ModeratorDecides,
        Some(1_000),
    )
    .await;
    fx.clock.fetch_add(2_000, Ordering::SeqCst);
    for _round in 0..3 {
        let _ = fx.settle_conversation_at(&conversation_scope()).await;
    }

    let conversation = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(conversation.status, AgentConversationStatus::Expired);
    assert!(conversation.terminal_notice_settled);

    let cell = fx
        .task_snapshot()
        .await
        .conversation
        .expect("the provenance cell stands");
    assert_eq!(cell.status, AgentConversationStatus::Expired);
    assert_eq!(
        cell.terminal_reason,
        AgentConversationTerminalReason::Expired
    );
}

#[tokio::test]
async fn an_already_terminal_task_still_records_the_provenance_cell() {
    // The user-approved posture: the cell is observational provenance, not
    // new work, and the common race — the conversation ending beside the
    // task's own terminalization — must not lose the observability this
    // exchange exists for.
    let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
    fx.apply_task_command_at(
        &task_scope(),
        rakka_agent::AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, task_scope().task().as_str(), "operator"],
            )
            .expect("the operation id derives"),
            reason: "no longer needed".to_string(),
        },
    )
    .await
    .expect("the cancel applies");
    // The default task carries a live assignment; the pump drives the
    // cancellation through the run's wind-down to the terminal commit.
    let _ = fx.pump().await;
    for _round in 0..3 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }
    assert!(fx.task_snapshot().await.status.is_terminal());

    drive_completion(&fx).await;

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Cancelled, "still terminal");
    assert_eq!(task.conversations, 1);
    let cell = task.conversation.expect("the terminal task recorded it");
    assert_eq!(cell.conversation.as_str(), CONVERSATION);
}

#[tokio::test]
async fn a_notice_racing_its_tasks_creation_stays_outstanding_then_converges() {
    // The dependency-registration posture: `task-not-created` is neither
    // settled nor memoized, so the notice waits out a racing creation
    // instead of replaying the miss for the whole applied window.
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.apply_conversation_command_at(
        &conversation_scope(),
        create_command(
            CONVERSATION,
            AgentConversationCompletionRule::AllRounds,
            None,
        ),
    )
    .await
    .expect("the conversation creates");
    drive_completion(&fx).await;

    let progress = fx
        .settle_conversation_at(&conversation_scope())
        .await
        .expect("the settle pass runs");
    assert_eq!(
        progress.outstanding, 1,
        "the notice stays outstanding against the missing task"
    );
    let conversation = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert!(
        !conversation.terminal_notice_settled,
        "an unanswerable notice never settles"
    );

    // The task appears; the next drive converges.
    fx.instantiate_agent().await;
    fx.create_task().await;
    for _round in 0..3 {
        let _ = fx.settle_conversation_at(&conversation_scope()).await;
    }
    assert!(
        fx.conversation_snapshot_at(&conversation_scope())
            .await
            .expect("the conversation snapshots")
            .terminal_notice_settled
    );
    assert_eq!(fx.task_snapshot().await.conversations, 1);
}

#[tokio::test]
async fn a_second_conversation_overwrites_the_cell_and_the_chain_is_history() {
    // Latest-only, the handoff precedent: the materialized cell holds the
    // newest terminated conversation, the counter and the history rows hold
    // the chain.
    let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
    drive_completion(&fx).await;
    assert_eq!(fx.task_snapshot().await.conversations, 1);

    let second = "retro-review";
    fx.apply_conversation_command_at(
        &scope_for(second),
        create_command(second, AgentConversationCompletionRule::AllRounds, None),
    )
    .await
    .expect("the second conversation creates");
    let _ = fx
        .apply_conversation_command_at(&scope_for(second), submit(second, 0, 0, "p1", "opening"))
        .await;
    let _ = fx
        .apply_conversation_command_at(&scope_for(second), submit(second, 0, 1, "p2", "closing"))
        .await;
    for _round in 0..3 {
        let _ = fx.settle_conversation_at(&scope_for(second)).await;
    }

    let task = fx.task_snapshot().await;
    assert_eq!(task.conversations, 2, "the counter carries the chain");
    assert_eq!(
        task.conversation
            .expect("the cell stands")
            .conversation
            .as_str(),
        second,
        "the cell holds the latest conversation"
    );
    assert_eq!(
        task_history_count(&fx, AgentTaskHistoryKind::ConversationTerminalRecorded).await,
        2,
        "the chain is history"
    );
}

#[tokio::test]
async fn a_settled_notice_burns_no_revision_on_later_sweeps() {
    let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
    drive_completion(&fx).await;
    assert!(
        fx.conversation_snapshot_at(&conversation_scope())
            .await
            .expect("the conversation snapshots")
            .terminal_notice_settled
    );

    fx.conversations.reset_writes();
    let _ = fx.settle_conversation_at(&conversation_scope()).await;
    let _ = fx.settle_conversation_at(&conversation_scope()).await;
    assert_eq!(
        fx.conversations.writes(),
        0,
        "a healthy sweep over a settled notice burns no revision"
    );
}

/// Counts the durable writes one crash-free completion flow attempts on
/// each store, so the sweeps below cover every real write and know when
/// they have run past the flow's end.
async fn reference_writes() -> (usize, usize) {
    let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
    fx.conversations.reset_writes();
    fx.tasks.reset_writes();
    drive_completion(&fx).await;
    (fx.conversations.writes(), fx.tasks.writes())
}

#[tokio::test]
async fn the_notice_converges_across_every_conversation_store_crash_point() {
    let (conversation_writes, _) = reference_writes().await;
    assert!(
        conversation_writes >= 2,
        "the completion flow writes the conversation store at least twice (turns, notice settle)"
    );
    for point in 1..=conversation_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
            fx.conversations.reset_writes();
            fx.conversations.crash_at(point, window);
            drive_completion(&fx).await;
            fx.conversations.assert_crash_fired(point, window);
            fx.conversations.survive();
            assert_converged(&fx).await;
        }
    }
}

#[tokio::test]
async fn the_notice_converges_across_every_task_store_crash_point() {
    let (_, task_writes) = reference_writes().await;
    assert!(
        task_writes >= 1,
        "the completion flow writes the task store at least once (the provenance cell)"
    );
    for point in 1..=task_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world(AgentConversationCompletionRule::AllRounds, None).await;
            fx.tasks.reset_writes();
            fx.tasks.crash_at(point, window);
            drive_completion(&fx).await;
            fx.tasks.assert_crash_fired(point, window);
            fx.tasks.survive();
            assert_converged(&fx).await;
        }
    }
}
