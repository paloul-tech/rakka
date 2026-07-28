//! A continuous goal completes one bounded epoch, persists its next durable
//! wake condition, passivates, and later resumes without an immortal poller.
//!
//! Specification: sections 8.2 ("Epoch completion returns evidence to the
//! controller and MUST NOT by itself terminate the continuous goal"; "Between
//! every wake, admission, epoch transition, and result, the controller, task,
//! and run MAY passivate") and 9.7 (settlement and return flow upward through
//! the deduplicated inter-entity command path after a known terminal child
//! outcome); scenario 36 of section 18. The first test drives the full cycle
//! on the direct-drive harness, where every call is already a restart; the
//! second runs it on real sharded actors under an idle policy and proves the
//! world holds zero resident entities between epochs.

use std::time::Duration;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_run_state, load_agent_task_state, AgentBudgetDimension, AgentRunStatus,
    AgentSchemaPolicy, AgentTaskEntityReply, AgentTaskStatus, AgentWakeControllerState,
    AgentWakeDisposition, AgentWakeScanner, AgentWakeTimerEntry, AgentWakeTimerStore,
    ScheduleRevision, ShardedWakeDelivery, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent::{AgentModelTurn, AgentTaskContent};

mod common;

use common::{
    continuous_control_creation_command, continuous_goal_mode, epoch_scopes_for,
    scheduled_wake_binding, task_scope, wake_policy, Fixture, ShardedWorld, WakeStore,
};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Observed the nightly state.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

async fn controller_of(fx: &Fixture) -> AgentWakeControllerState {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    state
        .task()
        .expect("the root is created")
        .wake_controller
        .clone()
        .expect("the controller exists")
}

#[tokio::test]
async fn a_continuous_goal_completes_one_epoch_and_resumes_from_its_next_durable_wake() {
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(proposing_turn())
            .with_turn(proposing_turn()),
    );
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    // One durable occurrence, delivered by the scanner.
    let binding = fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    let (epoch_scope, run_scope) = epoch_scopes_for(binding.wake_id());
    fx.clock
        .fetch_add(1_000, std::sync::atomic::Ordering::SeqCst);
    let scan = fx.wake_scanner().scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);

    // The epoch executes to its accepted result, its ledger settles and
    // returns, and its result flows back to the controller — all on the
    // re-drivable couriers, every entity rebuilt from durable state each
    // round.
    fx.pump_epoch(&epoch_scope, &run_scope)
        .await
        .expect("the epoch cycle converges");

    let epoch = load_agent_task_state(&fx.tasks, &epoch_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the epoch state loads")
        .expect("the epoch exists");
    assert_eq!(epoch.status(), Some(AgentTaskStatus::Completed));
    let run = load_agent_run_state(&fx.runs, &run_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the epoch run exists");
    assert!(run.status().is_some_and(|status| status.is_terminal()));

    // Epoch completion returned evidence without terminating the goal: the
    // wake released, the escrow settled and returned, the consumption is on
    // the root's ledger, and the root task is still nonterminal.
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let root_task = root.task().expect("the root is created");
    assert!(!root_task.status.is_terminal(), "the goal stays active");
    assert_eq!(root_task.escrow.outstanding().count(), 0, "escrow returned");
    assert!(
        root_task
            .escrow
            .consumed()
            .get(AgentBudgetDimension::ModelCalls)
            >= 1,
        "the epoch's consumption settled upward"
    );
    let controller = root_task
        .wake_controller
        .as_ref()
        .expect("the controller exists");
    assert_eq!(controller.counters().admitted, 1);
    assert_eq!(controller.counters().released, 1);
    assert!(controller.active().is_empty());

    // The next durable wake condition: the schedule layer parks the next
    // occurrence, durably. Nothing is resident on its behalf — every entity
    // in this harness is rebuilt per call, which is the passivation claim.
    let next = fx.schedule_wake(2_000_000, ScheduleRevision::INITIAL).await;
    fx.clock
        .store(2_000_100, std::sync::atomic::Ordering::SeqCst);

    // A fresh scanner — a new pod — resumes the goal from durable state
    // alone: the next occurrence admits a fresh epoch.
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the resume pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Admitted { .. },
            ..
        }
    ));
    let (next_epoch, next_run) = epoch_scopes_for(next.wake_id());
    fx.pump_epoch(&next_epoch, &next_run)
        .await
        .expect("the second epoch converges");
    let controller = controller_of(&fx).await;
    assert_eq!(controller.counters().admitted, 2);
    assert_eq!(controller.counters().released, 2);
}

#[tokio::test]
async fn the_controller_passivates_between_epochs_and_a_durable_wake_resumes_it() {
    const IDLE: Duration = Duration::from_millis(200);
    let world = ShardedWorld::new(
        "EpochLifecycle",
        IDLE,
        ScriptedDispatcher::new()
            .with_turn(proposing_turn())
            .with_turn(proposing_turn()),
        None,
    );

    // The agent that runs epochs, and the human-owned controller.
    instantiate_agent(&world).await;
    let task = world.task_ref(&task_scope());
    let created = task
        .ask(
            |reply_to| rakka_agent::AgentTaskEntityMessage::Command {
                command: Box::new(continuous_control_creation_command(continuous_goal_mode(
                    wake_policy(),
                ))),
                reply_to,
            },
            ShardedWorld::ASK_TIMEOUT,
        )
        .await
        .expect("the sharded controller replies");
    assert!(matches!(created, AgentTaskEntityReply::Applied { .. }));

    // One durable occurrence through the production delivery.
    let wakes = WakeStore::new();
    let delivery = ShardedWakeDelivery::new(
        world.sharding.clone(),
        world.task_registration.key().clone(),
        ShardedWorld::ASK_TIMEOUT,
    );
    let mut scanner = AgentWakeScanner::new(AgentWakeTimerStore::new(wakes.clone()), delivery);
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let (epoch_scope, run_scope) = epoch_scopes_for(binding.wake_id());
    scanner
        .timers_mut()
        .schedule_occurrence(AgentWakeTimerEntry::new(
            binding,
            task_scope().task().clone(),
            rakka_agent_workflow::AgentTimestampMillis::new(5),
        ))
        .await
        .expect("the occurrence parks");
    let scan = scanner.scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);

    // Drive the sharded world until the epoch completes and its result flows
    // back: settle the controller, the epoch task, and the epoch run; answer
    // what became dispatchable.
    pump_sharded_epoch(&world, &epoch_scope, &run_scope).await;
    let released = |state: &rakka_agent::AgentTaskState| {
        state
            .task()
            .and_then(|task| task.wake_controller.as_ref())
            .map(|controller| controller.counters().released)
            .unwrap_or(0)
    };
    let root = load_agent_task_state(&world.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    assert_eq!(released(&root), 1, "the epoch result released the wake");

    // No command of any kind: the idle policy alone evicts every actor while
    // the goal stays logically active with its next wake parked durably.
    scanner
        .timers_mut()
        .schedule_occurrence(AgentWakeTimerEntry::new(
            scheduled_wake_binding(2_000, ScheduleRevision::INITIAL),
            task_scope().task().clone(),
            rakka_agent_workflow::AgentTimestampMillis::new(2_000),
        ))
        .await
        .expect("the next occurrence parks");
    drain_to_zero_residents(&world).await;

    // The durable wake — and nothing else — resumes the goal.
    let scan = scanner.scan_due().await.expect("the resume pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Admitted { .. },
            ..
        }
    ));
    assert!(
        world.resident_entities() > 0,
        "the durable wake reactivated the controller"
    );
}

async fn instantiate_agent(world: &ShardedWorld) {
    use rakka_agent::{
        AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId, AgentEntityCommand,
        AgentEntityMessage, AgentEntityReply, AgentOperationId, AgentOperationKind, AgentSettings,
    };
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(common::task_definition_id());
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
        "Runs nightly reconciliation epochs.",
        envelope,
    )
    .expect("the agent definition should be valid");
    let reply = world
        .agent_ref(&common::agent_scope())
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::Instantiate {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::DefinitionUpdate,
                        &common::agent_scope(),
                        "1",
                    )
                    .expect("operation id should be derivable"),
                    definition: Box::new(definition),
                    settings: Box::new(AgentSettings::default()),
                    provenance: Box::new(common::provenance(1)),
                },
                reply_to,
            },
            ShardedWorld::ASK_TIMEOUT,
        )
        .await
        .expect("the sharded agent replies");
    assert!(matches!(reply, AgentEntityReply::Applied { .. }));
}

/// Settles the controller, the epoch task, and the epoch run, answering ready
/// effects, until the epoch's result has flowed back to the controller.
async fn pump_sharded_epoch(
    world: &ShardedWorld,
    epoch_scope: &rakka_agent::AgentTaskScope,
    run_scope: &rakka_agent::AgentRunScope,
) {
    use rakka_agent::{
        AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityMessage, AgentTaskEntityMessage,
    };
    for _round in 0..24 {
        for scope in [task_scope(), epoch_scope.clone()] {
            let _ = world
                .task_ref(&scope)
                .ask(
                    |reply_to| AgentTaskEntityMessage::Settle { reply_to },
                    ShardedWorld::ASK_TIMEOUT,
                )
                .await
                .expect("the sharded task settles");
        }
        let run = world.run_ref(run_scope);
        let _ = run
            .ask(
                |reply_to| AgentRunEntityMessage::Settle { reply_to },
                ShardedWorld::ASK_TIMEOUT,
            )
            .await
            .expect("the sharded run settles");

        // Answer every Ready effect from the durable record, as the
        // dispatcher would.
        if let Some(state) =
            load_agent_run_state(&world.runs, run_scope, &AgentSchemaPolicy::default())
                .await
                .expect("the run state loads")
        {
            if let Some(loop_state) = state.loop_state() {
                let ready: Vec<_> = loop_state
                    .effects()
                    .iter()
                    .filter(|effect| effect.status == AgentRunEffectStatus::Ready)
                    .cloned()
                    .collect();
                for effect in ready {
                    let outcome = world.dispatcher.answer(&effect).await;
                    let command = AgentRunEntityCommand::RecordEffectResult {
                        operation_id: effect
                            .result_operation_id(run_scope)
                            .expect("the result operation id derives"),
                        effect_id: effect.effect_id.clone(),
                        generation: effect.generation,
                        attempt: effect.attempts.saturating_add(1),
                        fence: 0,
                        outcome: Box::new(outcome),
                    };
                    let _ = run
                        .ask(
                            |reply_to| AgentRunEntityMessage::Command {
                                command: Box::new(command),
                                reply_to,
                            },
                            ShardedWorld::ASK_TIMEOUT,
                        )
                        .await
                        .expect("the sharded run answers the result");
                }
            }
        }

        let root =
            load_agent_task_state(&world.tasks, &task_scope(), &AgentSchemaPolicy::default())
                .await
                .expect("the root state loads")
                .expect("the root exists");
        let released = root
            .task()
            .and_then(|task| task.wake_controller.as_ref())
            .map(|controller| controller.counters().released)
            .unwrap_or(0);
        let run_terminal =
            load_agent_run_state(&world.runs, run_scope, &AgentSchemaPolicy::default())
                .await
                .expect("the run state loads")
                .and_then(|state| state.status())
                .is_some_and(AgentRunStatus::is_terminal);
        if released >= 1 && run_terminal {
            return;
        }
    }
    panic!("the sharded epoch cycle did not converge");
}

async fn drain_to_zero_residents(world: &ShardedWorld) {
    let deadline = 100;
    let mut polls = 0;
    loop {
        let resident = world.resident_entities();
        if resident == 0 {
            break;
        }
        polls += 1;
        assert!(
            polls < deadline,
            "the idle policy never evicted every actor; {resident} still resident"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
