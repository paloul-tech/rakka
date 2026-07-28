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

/// A forged epoch result — a sender other than the very task the wake
/// derives — is refused closed and changes nothing (the review-claimed
/// `task-epoch-forged` behavior, now pinned).
#[tokio::test]
async fn a_forged_epoch_result_is_refused_and_changes_nothing() {
    use rakka_agent::{
        epoch_result_operation_id, AgentBudgetConsumption, AgentEntityAddress, AgentEpochResult,
        AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentTaskScope,
        AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    };
    use rakka_agent_workflow::AgentCorrelationId;

    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn()));
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let binding = common::scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(
        rakka_agent::wake_admission_command(binding.clone()).expect("the command derives"),
    )
    .await
    .expect("the admission applies");
    let before = controller_of(&fx).await;

    // The claimed epoch task does not match what the wake derives.
    let impostor = AgentTaskScope::new(
        common::tenant(),
        rakka_agent::AgentTaskId::new("impostor-epoch").expect("the id is valid"),
    )
    .expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: impostor.task().clone(),
        status: rakka_agent::AgentTaskStatus::Completed,
        consumed: AgentBudgetConsumption::zero(),
        result_digest: None,
    };
    let forged = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::EpochResult,
        AgentEntityAddress::Task(impostor),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        rakka_agent_workflow::AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds");

    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    root.recover(rakka_agent_workflow::AgentTimestampMillis::new(9_001))
        .await
        .expect("the root recovers");
    let reply = root
        .accept(
            &forged,
            &fx.router,
            rakka_agent_workflow::AgentTimestampMillis::new(9_002),
        )
        .await
        .expect("the delivery is answered");
    assert_eq!(
        reply.result().status().rejection_code(),
        Some("task-epoch-forged"),
        "the forged sender is refused"
    );
    assert_eq!(
        controller_of(&fx).await,
        before,
        "a forged result changes no controller state"
    );
}

/// An explicit release racing ahead of the epoch's own result: the late
/// `EpochResult` still settles the escrow, tolerates the missing active
/// occurrence, and promotes nothing twice (the review-claimed crossing).
#[tokio::test]
async fn a_raced_manual_release_and_late_epoch_result_converge() {
    use rakka_agent::{
        epoch_result_operation_id, epoch_task_id_for_wake, AgentBudgetConsumption,
        AgentBudgetDimension, AgentEntityAddress, AgentEpochResult, AgentExchangeEnvelope,
        AgentExchangeKind, AgentExchangePayload, AgentOperationId, AgentOperationKind,
        AgentTaskEntityCommand, AgentTaskScope, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    };
    use rakka_agent_workflow::AgentCorrelationId;

    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn()));
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let binding = common::scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(
        rakka_agent::wake_admission_command(binding.clone()).expect("the command derives"),
    )
    .await
    .expect("the admission applies");

    // An operator releases the occurrence explicitly, ahead of the result.
    fx.apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Command,
            [common::TENANT, common::TASK, "manual-release"],
        )
        .expect("the operation id derives"),
        wake: binding.wake_id().clone(),
    })
    .await
    .expect("the manual release applies");
    assert_eq!(controller_of(&fx).await.counters().released, 1);

    // The epoch's own result arrives late, from the legitimate sender.
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(common::tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let mut consumed = AgentBudgetConsumption::zero();
    consumed.add(AgentBudgetDimension::ModelCalls, 3);
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status: rakka_agent::AgentTaskStatus::Completed,
        consumed,
        result_digest: None,
    };
    let late = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::EpochResult,
        AgentEntityAddress::Task(epoch_scope),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        rakka_agent_workflow::AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds");

    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    root.recover(rakka_agent_workflow::AgentTimestampMillis::new(9_001))
        .await
        .expect("the root recovers");
    let reply = root
        .accept(
            &late,
            &fx.router,
            rakka_agent_workflow::AgentTimestampMillis::new(9_002),
        )
        .await
        .expect("the late result is answered");
    assert!(reply.result().is_accepted(), "the settlement still lands");

    let root_state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let root_task = root_state.task().expect("the root is created");
    assert_eq!(
        root_task
            .escrow
            .consumed()
            .get(AgentBudgetDimension::ModelCalls),
        3,
        "the late result's consumption settled"
    );
    assert_eq!(root_task.escrow.outstanding().count(), 0, "escrow returned");
    let controller = root_task
        .wake_controller
        .as_ref()
        .expect("the controller exists");
    assert_eq!(controller.counters().released, 1, "no second release");
    assert!(controller.active().is_empty(), "nothing was re-promoted");
}

/// A cancelled epoch with a closed ledger owes its terminal result to the
/// controller from the cancel transition itself (the review-claimed owing).
#[tokio::test]
async fn a_cancelled_epoch_owes_its_result_to_the_controller() {
    // No agent is instantiated: the epoch's assignment is refused, so its
    // ledger never opens a run child and the cancel finds it closed.
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let binding = common::scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let (epoch_scope, run_scope) = epoch_scopes_for(binding.wake_id());
    fx.apply_task_command(
        rakka_agent::wake_admission_command(binding.clone()).expect("the command derives"),
    )
    .await
    .expect("the admission applies");

    // Cancel the epoch task directly, then let its courier deliver what the
    // cancellation owed.
    let mut epoch = rakka_agent::AgentTaskEntityStore::new(
        epoch_scope.clone(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    epoch
        .recover(rakka_agent_workflow::AgentTimestampMillis::new(8_000))
        .await
        .expect("the epoch recovers");
    epoch
        .apply(
            rakka_agent::AgentTaskEntityCommand::Cancel {
                operation_id: rakka_agent::AgentOperationId::new(
                    rakka_agent::AgentOperationKind::Cancellation,
                    [common::TENANT, "epoch", "cancel-1"],
                )
                .expect("the operation id derives"),
                reason: "operator abort".to_string(),
            },
            &fx.router,
            rakka_agent_workflow::AgentTimestampMillis::new(8_001),
        )
        .await
        .expect("the cancel applies");
    // No run ever existed, so the settle passes alone drive the owed result
    // home: the epoch's courier delivers, the root applies.
    let _ = run_scope;
    for _round in 0..4 {
        fx.settle_task_at(&epoch_scope)
            .await
            .expect("the epoch settles");
        fx.settle_task_at(&task_scope())
            .await
            .expect("the root settles");
    }

    let controller = controller_of(&fx).await;
    assert_eq!(
        controller.counters().released,
        1,
        "the cancelled epoch's result released its wake"
    );
    assert!(controller.active().is_empty());
    let root_state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    assert_eq!(
        root_state
            .task()
            .expect("the root is created")
            .escrow
            .outstanding()
            .count(),
        0,
        "the cancelled epoch's escrow settled and returned"
    );
}
