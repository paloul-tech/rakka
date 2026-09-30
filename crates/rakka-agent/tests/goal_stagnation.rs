//! Stagnation detection at the epoch settlement, and its deterministic policy.
//!
//! Specification: section 8.3 — progress evaluation detects bounded
//! repetition and lack of material state change through repeated result
//! fingerprints and no-progress epochs, then continues, waits, escalates, or
//! terminates under deterministic per-goal policy; exceeding a limit never
//! silently resets the counter. Every entity here is rebuilt from durable
//! state per call — the `Fixture` convention — so every settlement already
//! arrives after a restart.

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    epoch_result_operation_id, epoch_task_id_for_wake, load_agent_task_state,
    wake_admission_command, AgentBudgetConsumption, AgentContentDigest, AgentEntityAddress,
    AgentEpochResult, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentGoalLifecycleStatus, AgentGoalStagnationAction, AgentGoalStagnationPolicy,
    AgentGoalStatus, AgentGoalWaitReason, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentSchemaPolicy, AgentStagnationTrigger, AgentTaskEntityCommand,
    AgentTaskScope, AgentTaskStatus, AgentWakeControllerState, ScheduleRevision,
    AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};

mod common;

use common::{
    continuous_goal_control_creation_command, continuous_goal_mode, goal_spec_draft,
    goal_spec_with_stagnation, provenance, scheduled_wake_binding, task_scope, wake_policy,
    Fixture, TASK, TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
}

fn digest_of(answer: &str) -> AgentContentDigest {
    AgentContentDigest::of_json(&serde_json::json!({ "answer": answer }))
}

async fn create_stagnation_goal(fx: &Fixture, repeated: u32, action: AgentGoalStagnationAction) {
    fx.instantiate_agent().await;
    let reply = fx
        .apply_task_command(continuous_goal_control_creation_command(
            continuous_goal_mode(wake_policy()),
            goal_spec_draft(goal_spec_with_stagnation(repeated, action), true),
        ))
        .await
        .expect("the goal-bearing control task creates");
    assert!(matches!(
        reply,
        rakka_agent::AgentTaskEntityReply::Applied { .. }
    ));
}

async fn root_state(fx: &Fixture) -> rakka_agent::AgentTaskState {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists")
}

async fn controller(fx: &Fixture) -> AgentWakeControllerState {
    root_state(fx)
        .await
        .task()
        .expect("the root is created")
        .wake_controller
        .clone()
        .expect("the controller exists")
}

async fn goal_status(fx: &Fixture) -> (AgentGoalStatus, Option<AgentGoalWaitReason>) {
    let state = root_state(fx).await;
    let goal = state
        .task()
        .expect("the root is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .clone();
    (goal.status(), goal.wait().cloned())
}

/// Every audit entry of the root task, as `(kind, detail)` in sequence order.
async fn history_entries(fx: &Fixture) -> Vec<(rakka_agent::AgentTaskHistoryKind, String)> {
    let mut entries = Vec::new();
    let mut cursor = Some(rakka_agent::AgentTaskHistoryCursor::start());
    while let Some(position) = cursor {
        let page = rakka_agent::AgentTaskHistoryStore::read(&fx.history, &task_scope(), position)
            .await
            .expect("the history reads");
        entries.extend(
            page.entries
                .iter()
                .map(|entry| (entry.kind, entry.detail.clone())),
        );
        cursor = page.next;
    }
    entries
}

/// A legitimate epoch-result envelope for one admitted wake, carrying the
/// result fingerprint the epoch's accepted result produced.
fn epoch_result(
    binding: &rakka_agent::AgentWakeBinding,
    status: AgentTaskStatus,
    result_digest: Option<AgentContentDigest>,
) -> AgentExchangeEnvelope {
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(common::tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status,
        consumed: AgentBudgetConsumption::zero(),
        result_digest,
    };
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::EpochResult,
        AgentEntityAddress::Task(epoch_scope),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds")
}

async fn accept_on_root(fx: &Fixture, envelope: &AgentExchangeEnvelope) {
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    let reply = root
        .accept(envelope, &fx.router, fx.now())
        .await
        .expect("the result is answered");
    assert!(reply.result().is_accepted(), "the epoch result lands");
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
}

/// Admits one occurrence and settles its epoch with the given outcome.
async fn settle_epoch(
    fx: &Fixture,
    due_at: u64,
    status: AgentTaskStatus,
    result_digest: Option<AgentContentDigest>,
) {
    let binding = scheduled_wake_binding(due_at, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    accept_on_root(fx, &epoch_result(&binding, status, result_digest)).await;
}

#[tokio::test]
async fn repetition_trips_exactly_at_the_threshold_and_not_before() {
    let fx = fixture();
    create_stagnation_goal(&fx, 3, AgentGoalStagnationAction::Wait).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;

    // Two identical completions: the streak is durable, and nothing tripped.
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().repeated_result_epochs(), 2);
    assert_eq!(state.counters().stagnation_repeated, 0);
    assert_eq!(goal_status(&fx).await.0, AgentGoalStatus::Active);

    // The third identical completion trips exactly at the threshold: the
    // goal parks on the structured trigger, and admission suspends in the
    // same settlement so nothing further spends.
    settle_epoch(&fx, 15, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().repeated_result_epochs(), 3);
    assert_eq!(state.counters().stagnation_repeated, 1);
    assert_eq!(
        state.lifecycle().status(),
        AgentGoalLifecycleStatus::Suspended
    );
    assert_eq!(state.lifecycle().suspended_reason(), Some("stagnant"));
    let (status, wait) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Waiting);
    assert_eq!(
        wait,
        Some(AgentGoalWaitReason::Stagnant {
            trigger: AgentStagnationTrigger::RepeatedResult,
        })
    );

    // A recovery sweep over the settled world detects nothing twice: the
    // counters are the durable record, and replay never moves them.
    fx.settle_task_at(&task_scope())
        .await
        .expect("the sweep settles");
    let state = controller(&fx).await;
    assert_eq!(state.counters().stagnation_repeated, 1);
}

#[tokio::test]
async fn a_new_result_digest_resets_the_streaks() {
    let fx = fixture();
    create_stagnation_goal(&fx, 3, AgentGoalStagnationAction::Wait).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 15, AgentTaskStatus::Completed, Some(digest_of("new"))).await;

    // Progress is the definitional reset: three completions, no trip.
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().repeated_result_epochs(), 1);
    assert_eq!(state.lifecycle().no_progress_epochs(), 0);
    assert_eq!(state.counters().stagnation_repeated, 0);
    assert_eq!(goal_status(&fx).await.0, AgentGoalStatus::Active);
}

#[tokio::test]
async fn failed_epochs_neither_grow_nor_reset_the_detector() {
    let fx = fixture();
    create_stagnation_goal(&fx, 3, AgentGoalStagnationAction::Wait).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Failed, None).await;

    // The failure moved the failure streak and only the failure streak: the
    // two families never overlap in one settlement.
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().consecutive_failures(), 1);
    assert_eq!(state.lifecycle().repeated_result_epochs(), 1);
    assert_eq!(state.counters().stagnation_repeated, 0);
    assert_eq!(goal_status(&fx).await.0, AgentGoalStatus::Active);
}

#[tokio::test]
async fn a_terminate_action_fails_goal_task_and_gate_together() {
    let fx = fixture();
    create_stagnation_goal(&fx, 2, AgentGoalStagnationAction::Terminate).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;

    let state = root_state(&fx).await;
    let task = state.task().expect("the root exists");
    assert_eq!(task.status, AgentTaskStatus::Failed);
    assert_eq!(
        task.terminal_reason.as_ref().map(|reason| reason.code()),
        Some("goal-stagnant")
    );
    let goal = task.goal_state.as_deref().expect("the goal record exists");
    assert_eq!(goal.status(), AgentGoalStatus::Failed);
    assert_eq!(
        goal.terminal().map(|decision| decision.reason.clone()),
        Some(rakka_agent::AgentGoalTerminalReason::Stagnant {
            trigger: AgentStagnationTrigger::RepeatedResult,
            epochs: 2,
        })
    );
    assert_eq!(
        task.wake_controller
            .as_ref()
            .expect("the controller exists")
            .lifecycle()
            .status(),
        AgentGoalLifecycleStatus::Retired
    );
}

#[tokio::test]
async fn an_escalate_action_parks_with_the_escalation_on_record() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let mut spec = goal_spec_with_stagnation(2, AgentGoalStagnationAction::Escalate);
    spec.escalation =
        Some(rakka_agent::AgentPolicyRef::new("page-the-owner").expect("the policy ref is valid"));
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the creation applies");

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;

    let (status, wait) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Waiting);
    assert_eq!(
        wait,
        Some(AgentGoalWaitReason::Stagnant {
            trigger: AgentStagnationTrigger::RepeatedResult,
        })
    );
    // The escalation is durably on record in the audit trail.
    let history = history_entries(&fx).await;
    assert!(
        history.iter().any(|(kind, detail)| {
            *kind == rakka_agent::AgentTaskHistoryKind::GoalParked
                && detail.contains("escalation page-the-owner")
        }),
        "the park records the escalation reference"
    );
}

#[tokio::test]
async fn resume_goal_lifts_the_park_and_resets_the_detector() {
    let fx = fixture();
    create_stagnation_goal(&fx, 2, AgentGoalStagnationAction::Wait).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    let (status, _) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Waiting);
    let expected = root_state(&fx)
        .await
        .task()
        .expect("the root exists")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .status_revision();

    // The authorized resume is the one deliberate non-progress reset: it
    // lifts the park, reopens the gate, and clears the detector — with
    // provenance on the contract, never silently.
    fx.apply_task_command(AgentTaskEntityCommand::ResumeGoal {
        operation_id: operation("resume"),
        expected_status_revision: expected,
        top_up: None,
        provenance: Box::new(provenance(90)),
    })
    .await
    .expect("the resume applies");

    let (status, wait) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Active);
    assert_eq!(wait, None);
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    assert_eq!(state.lifecycle().repeated_result_epochs(), 0);
    assert!(state.lifecycle().last_result_digest().is_none());

    // The next identical epoch starts a fresh streak: it takes the full
    // threshold again to re-trip, not one late settlement.
    settle_epoch(&fx, 20, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().repeated_result_epochs(), 1);
    assert_eq!(goal_status(&fx).await.0, AgentGoalStatus::Active);
}

#[tokio::test]
async fn the_doors_are_fenced_both_ways() {
    let fx = fixture();
    create_stagnation_goal(&fx, 2, AgentGoalStagnationAction::Wait).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;

    // The gate's own resume does not own a stagnation park: lifting the
    // suspension while the contract says parked would re-admit spending.
    let lifecycle_revision = controller(&fx).await.lifecycle().lifecycle_revision();
    let refused = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: operation("gate-resume"),
            expected_lifecycle_revision: lifecycle_revision,
            provenance: Box::new(provenance(95)),
        })
        .await;
    assert_eq!(
        refused.expect_err("the gate door is fenced").code(),
        "task-goal-wait-owned-elsewhere"
    );
    let (status, _) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Waiting, "nothing lifted");
}

#[tokio::test]
async fn continue_action_detects_without_parking() {
    let fx = fixture();
    create_stagnation_goal(&fx, 2, AgentGoalStagnationAction::Continue).await;

    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    settle_epoch(&fx, 15, AgentTaskStatus::Completed, Some(digest_of("same"))).await;

    // Observe-only: the trips are durably counted and audited, and nothing
    // parks — the goal keeps working under its owner's policy.
    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().repeated_result_epochs(), 3);
    assert_eq!(state.counters().stagnation_repeated, 2);
    assert_eq!(state.lifecycle().status(), AgentGoalLifecycleStatus::Active);
    assert_eq!(goal_status(&fx).await.0, AgentGoalStatus::Active);
    let history = history_entries(&fx).await;
    assert_eq!(
        history
            .iter()
            .filter(|(kind, _)| *kind == rakka_agent::AgentTaskHistoryKind::GoalStagnationDetected)
            .count(),
        2,
        "each trip is audited once"
    );
}

#[tokio::test]
async fn a_trip_the_goal_never_saw_accounts_its_streak_but_counts_nothing() {
    // The streaks are facts about the epochs, so they account whatever the
    // contract does. The *trip counter* is not: it backs
    // `rakka.agent.goal.stagnation`, and a count with no detection row behind
    // it would report an event nobody observed.
    //
    // The reachable suppression is a schedule expiry projected in the same
    // settlement: the trip is measured against the goal as it was on entry
    // (`Active`), the gate's expiry then ends the contract, and
    // `apply_goal_stagnation` finds a terminal goal and records nothing.
    use rakka_agent::AgentWakeLifecyclePolicy;

    let fx = fixture();
    fx.instantiate_agent().await;
    let policy = wake_policy()
        .with_lifecycle(AgentWakeLifecyclePolicy {
            expires_at: Some(AgentTimestampMillis::new(50_000)),
            ..AgentWakeLifecyclePolicy::DEFAULT
        })
        .expect("the lifecycle policy is valid");
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(policy),
        goal_spec_draft(
            goal_spec_with_stagnation(2, AgentGoalStagnationAction::Wait),
            true,
        ),
    ))
    .await
    .expect("the creation applies");

    // One identical completion before the expiry: the streak starts, nothing
    // trips at a threshold of two.
    settle_epoch(&fx, 5, AgentTaskStatus::Completed, Some(digest_of("same"))).await;
    assert_eq!(controller(&fx).await.counters().stagnation_repeated, 0);

    // The second settles past the expiry. It would trip — and the streak
    // proves it did reach the threshold — but the projection ended the goal in
    // this same transition, so no detection was recorded and none is counted.
    let binding = scheduled_wake_binding(10, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    fx.clock.store(60_000, std::sync::atomic::Ordering::SeqCst);
    accept_on_root(
        &fx,
        &epoch_result(
            &binding,
            AgentTaskStatus::Completed,
            Some(digest_of("same")),
        ),
    )
    .await;

    let state = controller(&fx).await;
    assert_eq!(
        state.lifecycle().repeated_result_epochs(),
        2,
        "the streak accounted: a durable fact about the epochs"
    );
    assert_eq!(
        state.counters().stagnation_repeated,
        0,
        "and nothing counted: the goal never saw the trip"
    );
    let (status, wait) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Expired);
    assert_eq!(wait, None, "an expired goal parks on nothing");
    assert!(
        !history_entries(&fx)
            .await
            .iter()
            .any(|(kind, _)| *kind == rakka_agent::AgentTaskHistoryKind::GoalStagnationDetected),
        "no detection row, so the counter must agree"
    );
}

#[tokio::test]
async fn a_no_progress_threshold_trips_on_missing_fingerprints() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let mut spec = goal_spec_with_stagnation(5, AgentGoalStagnationAction::Wait);
    spec.stagnation_policy = AgentGoalStagnationPolicy {
        repeated_result_epochs: None,
        no_progress_epochs: Some(2),
        default: AgentGoalStagnationAction::Wait,
        overrides: Default::default(),
    };
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the creation applies");

    // Two completions without any result fingerprint: no material state
    // change the detector can see.
    settle_epoch(&fx, 5, AgentTaskStatus::Completed, None).await;
    settle_epoch(&fx, 10, AgentTaskStatus::Completed, None).await;

    let state = controller(&fx).await;
    assert_eq!(state.lifecycle().no_progress_epochs(), 2);
    assert_eq!(state.counters().stagnation_no_progress, 1);
    let (status, wait) = goal_status(&fx).await;
    assert_eq!(status, AgentGoalStatus::Waiting);
    assert_eq!(
        wait,
        Some(AgentGoalWaitReason::Stagnant {
            trigger: AgentStagnationTrigger::NoProgress,
        })
    );
}

#[tokio::test]
async fn a_replan_selecting_policy_is_refused_at_the_spec_door() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let spec = goal_spec_with_stagnation(2, AgentGoalStagnationAction::Replan);
    let refused = fx
        .apply_task_command(continuous_goal_control_creation_command(
            continuous_goal_mode(wake_policy()),
            goal_spec_draft(spec, true),
        ))
        .await;
    assert_eq!(
        refused.expect_err("replan cannot be selected yet").code(),
        "goal-stagnation-replan-unsupported"
    );

    // A threshold below its minimum is equally refused: a repeat count of
    // one is every completion, not a repetition.
    let mut low = goal_spec_with_stagnation(1, AgentGoalStagnationAction::Wait);
    low.stagnation_policy.repeated_result_epochs = Some(1);
    let refused = fx
        .apply_task_command(continuous_goal_control_creation_command(
            continuous_goal_mode(wake_policy()),
            goal_spec_draft(low, true),
        ))
        .await;
    assert_eq!(
        refused
            .expect_err("a threshold below its minimum is refused")
            .code(),
        "goal-stagnation-threshold-too-low"
    );
    let _ = AgentRevisionNumber::INITIAL;
}
