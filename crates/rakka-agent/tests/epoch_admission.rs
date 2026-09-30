//! One admitted wake is one finite child epoch task and run — exactly one.
//!
//! Specification: sections 8.2 ("Each admitted epoch MUST create one finite
//! child `AgentTaskId` and one finite `AgentRunId`. The epoch MUST carry the
//! goal/root task, `AgentWakeId`, schedule, definition/setup/settings/policy
//! revisions, input observation scope, budget, deadline, and result/evidence
//! contract") and 9.7 (the epoch's allocation is debited from the parent
//! scope inside the parent's own creating transition and carried on the
//! deduplicated creation command); the "at most one admitted child epoch
//! task/run" clause of scenario 48, now provable end to end. The epoch's
//! identities are derived from the wake, so every duplicate path — a replayed
//! admission command, a re-driven creation exchange, an owner lost mid-flow —
//! converges on the same child.

use rakka_agent::testkit::{sweep_crash_points, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, load_agent_task_state, wake_admission_command, AgentBudgetDimension,
    AgentSchemaPolicy, AgentTaskEntityReply, AgentTaskStatus, ScheduleRevision,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent::{AgentModelTurn, AgentTaskContent};

mod common;

use common::{epoch_scopes_for, scheduled_wake_binding, task_scope, Fixture};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "observed" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn()))
}

#[tokio::test]
async fn an_admitted_wake_creates_exactly_one_epoch_task_and_run() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(common::continuous_goal_mode(common::wake_policy()))
        .await;

    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let (epoch_scope, run_scope) = epoch_scopes_for(binding.wake_id());
    let command = wake_admission_command(binding.clone()).expect("the admission command derives");

    let reply = fx
        .apply_task_command(command.clone())
        .await
        .expect("the admission applies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));

    // The creation exchange the admission owed has been driven; the epoch
    // task exists, carrying everything specification 8.2 binds to it.
    let epoch = load_agent_task_state(&fx.tasks, &epoch_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the epoch state loads")
        .expect("the epoch task exists");
    let epoch_task = epoch.task().expect("the epoch task is created");
    assert_eq!(epoch_task.wake.as_ref(), Some(binding.wake_id()));
    assert_eq!(epoch_task.parent.as_ref(), Some(task_scope().task()));
    assert_eq!(epoch_task.goal.as_ref(), Some(binding.goal()));
    assert!(!epoch_task.goal_mode.is_continuous(), "an epoch is finite");
    assert_eq!(
        epoch_task
            .escrow
            .allocation()
            .get(AgentBudgetDimension::ModelCalls),
        Some(8),
        "the epoch holds the escrow its parent debited, not its ceilings"
    );
    let input = serde_json::to_value(&epoch_task.input).expect("the input serializes");
    assert!(
        input.to_string().contains(binding.wake_id().as_str()),
        "the epoch input names the wake it observes"
    );

    // The root debited the epoch's allocation in the admitting transition.
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let root_task = root.task().expect("the root is created");
    assert_eq!(root_task.escrow.outstanding().count(), 1);
    let controller = root_task
        .wake_controller
        .as_ref()
        .expect("the controller exists");
    let epoch_ref = controller.active()[0]
        .epoch()
        .expect("the epoch is attached");
    assert_eq!(&epoch_ref.task, epoch_scope.task());
    assert_eq!(&epoch_ref.run, run_scope.run());

    // The epoch's assignment machinery ran on the existing rails: one run.
    fx.pump_epoch(&epoch_scope, &run_scope)
        .await
        .expect("the epoch converges");
    let run = load_agent_run_state(&fx.runs, &run_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads");
    assert!(run.is_some(), "the epoch's derived run exists");

    // Every duplicate path converges on the same child: a replayed admission
    // answers from the record, and the world still holds one epoch.
    let replay = fx
        .apply_task_command(command)
        .await
        .expect("the replayed admission is answered");
    assert!(matches!(replay, AgentTaskEntityReply::Duplicate { .. }));
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    assert_eq!(
        root.task()
            .expect("the root is created")
            .wake_controller
            .as_ref()
            .expect("the controller exists")
            .counters()
            .admitted,
        1
    );
}

#[tokio::test]
async fn an_admission_without_an_epoch_contract_fails_closed() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(common::continuous_goal_mode_with_epoch(
        common::wake_policy(),
        None,
    ))
    .await;

    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let error = fx
        .apply_task_command(wake_admission_command(binding).expect("the command derives"))
        .await
        .expect_err("an admission with no epoch contract is refused");
    assert_eq!(error.code(), "task-epoch-undefined");

    // The refusal rolled the whole transition back: nothing was admitted,
    // nothing was debited, nothing is owed.
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let root_task = root.task().expect("the root is created");
    assert!(root_task
        .wake_controller
        .as_ref()
        .is_none_or(|controller| controller.active().is_empty()));
    assert_eq!(root_task.escrow.outstanding().count(), 0);
}

#[tokio::test]
async fn the_epoch_admission_survives_any_owner_loss() {
    // Reference run: task-store writes of the create-admit-converge flow.
    let reference = fixture();
    reference.instantiate_agent().await;
    reference
        .create_continuous_control_task(common::continuous_goal_mode(common::wake_policy()))
        .await;
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let (epoch_scope, run_scope) = epoch_scopes_for(binding.wake_id());
    let command = wake_admission_command(binding.clone()).expect("the command derives");
    reference
        .apply_task_command(command.clone())
        .await
        .expect("the reference admission applies");
    reference
        .pump_epoch(&epoch_scope, &run_scope)
        .await
        .expect("the reference converges");
    let writes = reference.tasks.writes();

    sweep_crash_points(writes, |nth, point| {
        let command = command.clone();
        let epoch_scope = epoch_scope.clone();
        let run_scope = run_scope.clone();
        async move {
            let fx = fixture();
            fx.instantiate_agent().await;
            let create = common::continuous_control_creation_command(common::continuous_goal_mode(
                common::wake_policy(),
            ));
            fx.tasks.crash_at(nth, point);
            let _ = fx.apply_task_command(create.clone()).await;
            let _ = fx.apply_task_command(command.clone()).await;
            let _ = fx.pump_epoch(&epoch_scope, &run_scope).await;
            fx.tasks.assert_crash_fired(nth, point);
            fx.tasks.survive();

            // The next owner re-drives the identical flow from durable state.
            let _ = fx.apply_task_command(create).await;
            let _ = fx.apply_task_command(command).await;
            fx.pump_epoch(&epoch_scope, &run_scope)
                .await
                .unwrap_or_else(|error| {
                    panic!("the re-driven flow converges at write {nth} {point:?}: {error}")
                });

            let root =
                load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
                    .await
                    .expect("the root state loads")
                    .expect("the root exists");
            let controller = root
                .task()
                .expect("the root is created")
                .wake_controller
                .as_ref()
                .expect("the controller exists")
                .clone();
            assert_eq!(
                controller.counters().admitted,
                1,
                "exactly one admission at write {nth} {point:?}"
            );
            let epoch =
                load_agent_task_state(&fx.tasks, &epoch_scope, &AgentSchemaPolicy::default())
                    .await
                    .expect("the epoch state loads")
                    .expect("the epoch exists at every window");
            assert!(
                matches!(
                    epoch.status(),
                    Some(
                        AgentTaskStatus::Created
                            | AgentTaskStatus::Assigned
                            | AgentTaskStatus::InProgress
                            | AgentTaskStatus::Completed
                    )
                ),
                "one live epoch at write {nth} {point:?}, got {:?}",
                epoch.status()
            );
        }
    })
    .await;
}
