//! Pruning the dispatcher fleet record while a claim is in flight, end to
//! end: real sharded agent, task and run entities, and the production
//! dispatcher over one shared fleet record (issue #145).
//!
//! Two runs finish, and their finished entries are what the fleet record
//! would otherwise hold for good. A third run's claim is then in flight,
//! under a live lease, when a second dispatcher prunes the record at a
//! horizon in the future, which removes every finished entry. The claim in
//! flight is left exactly as it was, and its run goes on to complete.

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    registered_agent_task_entity_ref, run_id_for_assignment, AgentAssignmentGeneration,
    AgentAuthorityEnvelope, AgentDispatchWindow, AgentId, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRunScope, AgentRunStatus, AgentSchemaPolicy, AgentScope,
    AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskEntityCommand,
    AgentTaskEntityMessage, AgentTaskEntityReply, AgentTaskId, AgentTaskScope, TenantId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentDispatchStatus, AgentDispatcherFleetState, AgentTimestampMillis};
use rakka_example_multi_agent_goal_acceptance::flow::{
    ask_retrying, instantiate, mission_definition, pump,
};
use rakka_example_multi_agent_goal_acceptance::wiring::{World, LEASE_MS, TENANT};
use rakka_persistence::DurableStateStore;

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

/// One agent with one task assigned to it, answered in one model turn.
struct OneTurn {
    task_scope: AgentTaskScope,
    run_scope: AgentRunScope,
    adapter: DeterministicModelAdapter,
}

impl OneTurn {
    /// Instantiates `agent`, and creates the one task assigned to it.
    async fn create(world: &World, agent: &str) -> Self {
        let definition: AgentTaskDefinition = mission_definition();
        let agent_id = AgentId::new(agent).expect("the agent id is valid");
        let agent_scope = AgentScope::new(tenant(), agent_id.clone()).expect("the scope is valid");
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(definition.definition_id.clone());
        instantiate(world, &agent_scope, &format!("{agent}-v1"), envelope).await;

        let task_id = AgentTaskId::new(format!("task-{agent}")).expect("the task id is valid");
        let task_scope =
            AgentTaskScope::new(tenant(), task_id.clone()).expect("the scope is valid");
        let task = registered_agent_task_entity_ref(&world.task_registration, &task_scope);
        let created = ask_retrying(
            &task,
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, task_id.as_str(), "1"],
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(AgentTaskCreation {
                        definition: definition.clone(),
                        input: AgentTaskContent::inline(serde_json::json!("what is owed"))
                            .expect("the input is inline-bounded"),
                        assignee: Some(agent_id.clone()),
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
                }),
                reply_to,
            },
            "create-task",
        )
        .await;
        assert!(
            matches!(created, AgentTaskEntityReply::Applied { .. }),
            "the task creates, got {created:?}"
        );

        let run_scope = AgentRunScope::new(
            tenant(),
            agent_id,
            run_id_for_assignment(&task_id, AgentAssignmentGeneration::new(1))
                .expect("the run id derives"),
        )
        .expect("the run scope is valid");
        let adapter = DeterministicModelAdapter::new().with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Answering.")
                .with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": "nothing" }))
                        .expect("the proposal is inline-bounded"),
                ),
        );
        Self {
            task_scope,
            run_scope,
            adapter,
        }
    }

    /// Pumps the task and its run until the run ends or nothing moves.
    async fn pump(&self, world: &World) {
        pump(
            world,
            &self.task_scope,
            &self.run_scope,
            &self.adapter,
            true,
        )
        .await;
    }

    async fn status(&self, world: &World) -> Option<AgentRunStatus> {
        rakka_agent::load_agent_run_state(
            &world.runs,
            &self.run_scope,
            &AgentSchemaPolicy::default(),
        )
        .await
        .expect("the run state loads")
        .and_then(|state| state.status())
    }
}

async fn fleet_state(world: &World) -> AgentDispatcherFleetState {
    world
        .fleet_store
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")
        .expect("the fleet record exists")
        .state
}

#[tokio::test]
async fn a_prune_while_a_claim_is_in_flight_costs_nothing() {
    let world = World::new();

    // Two runs finish. Every entry they leave in the fleet record is finished.
    for agent in ["first", "second"] {
        let turn = OneTurn::create(&world, agent).await;
        turn.pump(&world).await;
        assert_eq!(
            turn.status(&world).await,
            Some(AgentRunStatus::Completed),
            "the run of {agent} completes"
        );
    }
    let finished = fleet_state(&world).await.entries().len();
    assert!(finished >= 2, "each finished run left an entry behind");
    assert!(fleet_state(&world)
        .await
        .entries()
        .values()
        .all(|entry| entry.status.is_terminal()));

    // A third run's dispatcher is lost after it claimed the model call's
    // ticket and before it wrote the durable `Started`: the claim is in
    // flight, under a lease that is still live, and nothing was invoked.
    let third = OneTurn::create(&world, "third").await;
    world.probe.arm(AgentDispatchWindow::BeforeStarted);
    third.pump(&world).await;
    let now = AgentTimestampMillis::new(world.clock.load(std::sync::atomic::Ordering::SeqCst));
    let in_flight: Vec<_> = fleet_state(&world)
        .await
        .entries()
        .values()
        .filter(|entry| entry.is_in_flight_at(now))
        .cloned()
        .collect();
    assert_eq!(in_flight.len(), 1, "one claim is in flight");

    // A second dispatcher over the same stores prunes at a horizon in the
    // future: every finished entry goes, and the claim in flight stays.
    let prune = world
        .pipeline(DeterministicModelAdapter::new())
        .prune_fleet(AgentTimestampMillis::new(u64::MAX))
        .await
        .expect("the prune runs");
    assert_eq!(prune.pruned, finished);
    assert_eq!(prune.remaining, 1);
    let after = fleet_state(&world).await;
    assert_eq!(
        after.entry(&in_flight[0].dispatch_id),
        Some(&in_flight[0]),
        "the claim in flight is exactly as it was"
    );

    // The lease lapses, a dispatcher takes the claim over, and the run ends.
    world.wf_clock.advance(LEASE_MS + 1);
    third.pump(&world).await;

    assert_eq!(
        third.status(&world).await,
        Some(AgentRunStatus::Completed),
        "the third run completes"
    );
    assert_eq!(third.adapter.calls(), 1, "its model was invoked once");
    let entry = fleet_state(&world)
        .await
        .entry(&in_flight[0].dispatch_id)
        .cloned()
        .expect("the claim's entry is still in the record");
    assert_eq!(entry.status, AgentDispatchStatus::Completed);
    // And pumping the finished runs again brings nothing back.
    let held = fleet_state(&world).await.entries().len();
    third.pump(&world).await;
    assert_eq!(fleet_state(&world).await.entries().len(), held);
}
