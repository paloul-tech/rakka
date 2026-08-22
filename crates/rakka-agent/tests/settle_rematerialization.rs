//! The settle pass re-materializes before it decides: every sharded entity
//! has two writers by construction — a resident facade holding its cache for
//! the whole residency, and the A2A service (or a peer's courier transport)
//! reaching the same durable record through its *own* store handle. A
//! resident that concludes it owes nothing from a stale cache performs zero
//! writes, never conflicts, and so would never re-read — the wedge the team
//! sweep closed in the slice-5.6 hardening
//! (`a_wire_claim_committed_past_a_resident_board_is_driven_by_its_next_settle_sweep`).
//! These are the owed conversation, task, and run parities.

mod common;

use common::{agent_id, run_scope, task_scope, tenant, Fixture, TENANT};
use rakka_agent::testkit::{run_entity, CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_turn_content_digest, conversation_turn_operation_id, AgentBudgetConsumption,
    AgentConversationCompletionRule, AgentConversationCreation, AgentConversationEntityCommand,
    AgentConversationEntityReply, AgentConversationId, AgentConversationMode,
    AgentConversationScope, AgentConversationTurnSubmit, AgentId, AgentModerationPolicy,
    AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentRunStatus, AgentTaskContent,
    AgentTaskCreation, AgentTaskEntityCommand, AgentTaskId,
};

fn create_task_command() -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(
            AgentOperationKind::TaskCreation,
            [TENANT, task_scope().task().as_str(), "1"],
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentTaskCreation {
            definition: common::task_definition(),
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
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

const CONVERSATION: &str = "panel-debate";
const MODERATOR: &str = "moderator";

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
}

fn conversation_scope() -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
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

/// The conversation parity: a wire turn commits through the service's own
/// store handle while a resident facade idles on a stale cursor. The
/// rightful next speaker's turn is refused off that stale cursor; the
/// resident's own settle sweep must re-materialize so the retry is admitted
/// — before the fix, the refusal wrote nothing, the cache never re-read, and
/// the wrong refusal stood for the whole residency.
#[tokio::test]
async fn a_wire_turn_committed_past_a_resident_conversation_heals_on_its_next_sweep() {
    let fx = fixture();
    fx.instantiate_conversation_participants(&[MODERATOR, "p1", "p2"])
        .await;
    fx.apply_conversation_command_at(&conversation_scope(), create_conversation())
        .await
        .expect("the conversation creates");

    // The resident facade materializes and goes idle holding a clean cache:
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

    // The other writer — the A2A service's own store handle — commits p1's
    // turn: the durable cursor is now (0, 1), p2 to speak.
    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "position"))
        .await
        .expect("the wire turn commits");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    // The rightful next speaker reaches the resident, whose stale cursor
    // still expects p1's turn 0: refused, and the refusal writes nothing.
    let refused = resident
        .apply(submit(0, 1, "p2", "response"), &fx.router, fx.now())
        .await;
    assert!(
        refused.is_err(),
        "the stale cursor refuses the in-order turn: {refused:?}"
    );

    // The resident's own settle sweep — no passivation, no lost race, no
    // mutating command in between — must re-materialize the durable record.
    let _ = resident
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the resident sweep settles");

    // The same submission now lands: the refusal was never memoized (a
    // non-committing refusal writes nothing), so the retry re-runs the door
    // against the re-materialized cursor.
    let reply = resident
        .apply(submit(0, 1, "p2", "response"), &fx.router, fx.now())
        .await
        .expect("an inability must not outlive the sweep: the in-order turn is admitted");
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

/// The task parity: the creation — assignee and all — commits through the
/// service's own store handle, and the writer dies before its tail settle
/// could decide the assignment the creation owed. A resident facade idles on
/// a cache that says the task does not exist; its own settle sweep must
/// re-materialize and decide that assignment — before the fix, the sweep
/// concluded it owed nothing from the stale cache, wrote nothing, and the
/// offer stalled until an unrelated command lost a race.
#[tokio::test]
async fn a_wire_creation_committed_past_a_resident_task_is_decided_by_its_next_sweep() {
    let fx = fixture();
    fx.instantiate_agent().await;

    // The resident facade materializes on the not-yet-created task and goes
    // idle holding that emptiness as its cache.
    let mut resident = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // The other writer commits the creation — which mints the assignment
    // offer in the same compare-and-set — and dies right after the write:
    // the offered assignment exists durably, its offer exchange unsent.
    fx.tasks.reset_writes();
    fx.tasks.crash_at(1, CrashPoint::AfterWrite);
    let died = fx
        .apply_task_command_at(&task_scope(), create_task_command())
        .await;
    assert!(died.is_err(), "the writer dies after the commit: {died:?}");
    fx.tasks.assert_crash_fired(1, CrashPoint::AfterWrite);
    fx.tasks.survive();
    let task = fx.task_snapshot().await;
    let assignment = task.assignment.expect("the creation committed the offer");
    assert_eq!(
        assignment.status,
        rakka_agent::AgentAssignmentStatus::Offered,
        "the offer committed with its exchange unsent"
    );

    // The resident's own settle sweeps — no passivation, no lost race, no
    // mutating command in between — must observe the wire-committed offer
    // and drive the exchange the writer died owing.
    let mut settled = 0;
    for _round in 0..3 {
        let progress = resident
            .settle_side_effects(&fx.router, fx.now())
            .await
            .expect("the resident sweep settles");
        settled += progress.settled;
    }
    assert!(
        settled >= 1,
        "the sweeps drove the offer their cache never saw"
    );
    let task = fx.task_snapshot().await;
    let assignment = task.assignment.expect("the assignment stands");
    assert_eq!(
        assignment.status,
        rakka_agent::AgentAssignmentStatus::Accepted,
        "the offer round-tripped to acceptance"
    );
}

/// The run parity: the task's courier — a transport holding its own store
/// handle — durably accepts the run's assignment and dies before advancing
/// the accepted run's loop. A resident run facade idles on a cache that says
/// no run exists; its own settle sweep must re-materialize and advance the
/// loop — before the fix, the sweep concluded it owed nothing and the
/// freshly accepted run sat unadvanced behind the stale cache.
#[tokio::test]
async fn an_assignment_accepted_past_a_resident_run_advances_on_its_next_sweep() {
    let fx = fixture();
    fx.instantiate_agent().await;

    // The resident run facade materializes before the run exists.
    let mut resident = run_entity(&run_scope(), &fx.runs, &fx.effects);
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // The courier's run entity commits the acceptance and dies right after
    // the write: the accepted run exists durably, its loop unadvanced. The
    // creating writer's own settle delivers the offer inside its apply, so
    // no further task settles run — the run store's first write is the
    // acceptance, and the armed crash kills its writer there.
    fx.runs.reset_writes();
    fx.runs.crash_at(1, CrashPoint::AfterWrite);
    let _ = fx
        .apply_task_command_at(&task_scope(), create_task_command())
        .await;
    fx.runs.assert_crash_fired(1, CrashPoint::AfterWrite);
    fx.runs.survive();
    let snapshot = fx.run_snapshot().await.expect("the run exists durably");
    assert_eq!(
        snapshot.status,
        AgentRunStatus::Accepted,
        "the acceptance committed with the loop unadvanced"
    );

    // The resident's own settle sweep must observe the accepted run and
    // advance its loop.
    let progress = resident
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the resident sweep settles");
    assert!(
        progress.transitions >= 1,
        "the sweep advanced the run its cache never saw: {progress:?}"
    );
    let status = fx.run_snapshot().await.expect("the run snapshots").status;
    assert_ne!(
        status,
        AgentRunStatus::Accepted,
        "the accepted run advanced past acceptance"
    );
}
