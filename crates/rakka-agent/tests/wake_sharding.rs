//! The continuous controller passivates between wakes, and the production
//! delivery reactivates it only for a durable occurrence.
//!
//! Specification: section 8.2 ("Between every wake, admission, epoch
//! transition, and result, the controller, task, and run MAY passivate") and
//! the section 15 continuous clauses; the sharded arm of scenario 47. The
//! entities are real sharded actors under a short idle policy, and the
//! delivery is the production [`rakka_agent::ShardedWakeDelivery`] over the
//! same registration production wires — the scanner reaches the controller
//! exactly the way a shared pod service would.

use std::time::Duration;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, AgentOperationId, AgentOperationKind, AgentSchemaPolicy,
    AgentTaskContent, AgentTaskCreation, AgentTaskEntityCommand, AgentTaskEntityMessage,
    AgentTaskEntityReply, AgentTaskOwnership, AgentTaskStatus, AgentWakeDisposition,
    AgentWakeScanner, AgentWakeTimerEntry, AgentWakeTimerStore, ScheduleRevision,
    ShardedWakeDelivery,
};

mod common;

use common::{
    continuous_goal_mode, goal_id, scheduled_wake_binding, task_definition, task_scope,
    wake_policy, ShardedWorld, WakeStore, TASK, TENANT,
};

const IDLE: Duration = Duration::from_millis(200);

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

#[tokio::test]
async fn the_controller_passivates_between_wakes_and_a_durable_wake_reactivates_it() {
    let world = ShardedWorld::new("WakeSharding", IDLE, ScriptedDispatcher::new(), None);
    let task = world.task_ref(&task_scope());

    // The continuous root control task. Human ownership keeps the assignment
    // machinery out of this test: the controller's own behavior is what is
    // under proof.
    let created = task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, TASK, "1"],
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(AgentTaskCreation {
                        definition: task_definition().with_ownership(AgentTaskOwnership::Human),
                        input: AgentTaskContent::inline(serde_json::json!({ "goal": 1 }))
                            .expect("the input is inline-bounded"),
                        assignee: None,
                        goal: Some(goal_id()),
                        goal_mode: continuous_goal_mode(wake_policy()),
                        parent: None,
                        dependencies: Vec::new(),
                        escrow: None,
                        wake: None,
                        telemetry: Default::default(),
                    }),
                }),
                reply_to,
            },
            ShardedWorld::ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task replies");
    let AgentTaskEntityReply::Applied { outcome } = created else {
        panic!("the continuous root task is created, got {created:?}");
    };
    assert_eq!(outcome.status, AgentTaskStatus::WaitingForInput);

    // No command of any kind: the idle policy alone evicts the controller
    // while its goal stays logically active.
    drain_to_zero_residents(&world).await;

    // A scanner pass over an empty store wakes nothing: scanner uptime is
    // not an occurrence.
    let wakes = WakeStore::new();
    let delivery = ShardedWakeDelivery::new(
        world.sharding.clone(),
        world.task_registration.key().clone(),
        ShardedWorld::ASK_TIMEOUT,
    );
    let mut scanner = AgentWakeScanner::new(AgentWakeTimerStore::new(wakes.clone()), delivery);
    let scan = scanner.scan_due().await.expect("the empty pass runs");
    assert!(scan.outcomes.is_empty());
    assert_eq!(
        world.resident_entities(),
        0,
        "an empty scan reactivates nothing"
    );

    // One durable occurrence, parked and due. Its delivery — the production
    // sharded ask — is what reactivates the controller.
    scanner
        .timers_mut()
        .schedule_occurrence(AgentWakeTimerEntry::new(
            scheduled_wake_binding(5, ScheduleRevision::INITIAL),
            task_scope().task().clone(),
            rakka_agent_workflow::AgentTimestampMillis::new(5),
        ))
        .await
        .expect("the occurrence parks");
    let scan = scanner.scan_due().await.expect("the pass runs");
    assert_eq!(scan.outcomes.len(), 1);
    assert!(matches!(
        &scan.outcomes[0],
        rakka_agent::AgentWakeScanOutcome::Dispositioned {
            disposition: AgentWakeDisposition::Admitted { .. },
            ..
        }
    ));

    // The controller passivates again after the admission, and the admission
    // is durable — readable without waking anything.
    drain_to_zero_residents(&world).await;
    let state = load_agent_task_state(&world.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let controller = state
        .task()
        .expect("the task is created")
        .wake_controller
        .as_ref()
        .expect("the controller state exists");
    assert_eq!(controller.counters().admitted, 1);
    assert_eq!(controller.active().len(), 1);

    // A rescan finds the entry terminal and wakes nothing: at most one
    // admission per occurrence, however often the shared scanner runs.
    let scan = scanner.scan_due().await.expect("the rescan runs");
    assert!(scan.outcomes.is_empty());
    assert_eq!(world.resident_entities(), 0);
}
