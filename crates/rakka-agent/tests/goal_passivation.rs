//! Goal-active work passivates to zero and one durable trigger resumes it.
//!
//! Specification: sections 6.11, 8.1, and 15; scenario 35 of section 18. The
//! first two tests keep the M1 reading they were written under — "an `Active`
//! goal" as a non-terminal root task carrying an `AgentGoalId`, its run
//! parked on a durable approval wait — because that reading stays true and
//! the scenario stays owed. Slice 4.1 filled `goal.rs` with the full
//! contract, so the last test adds the 8.1 half: a root task holding a real
//! `AgentGoalState` passivates to zero resident entities, the goal record is
//! read back from durable state alone without waking anything, and one
//! durable goal command reactivates the correct owner and transitions the
//! contract exactly once — the "remains durably addressable through
//! passivation until an authorized terminal transition" clause, proven
//! against the record the entity itself transitions. All entities are real
//! sharded actors; the wait is durable state (the no-open-span half is
//! scenario 22's proof in `trace_scenarios.rs`), not a resident actor, task,
//! or timer. Timer-driven wakes are the phase 3 wake controller's; nothing
//! here invents one.

use std::time::Duration;

use rakka_agent::testkit::{CrashPoint, CrashingStateStore, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, passivate_agent_entity, passivate_agent_run_entity,
    passivate_agent_task_entity, run_id_for_assignment, AgentApprovalDecision,
    AgentAssignmentGeneration, AgentAuthorityEnvelope, AgentCheckpointDecision, AgentDefinition,
    AgentDefinitionId, AgentEffectPolicies, AgentEffectSpec, AgentEntityCommand,
    AgentEntityMessage, AgentEntityReply, AgentGoalId, AgentId, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentRevisionProvenance, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunEntityMessage, AgentRunEntityRef, AgentRunEntityReply,
    AgentRunScope, AgentRunState, AgentRunStatus, AgentSchemaId, AgentSchemaPolicy, AgentSchemaRef,
    AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition,
    AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityRef,
    AgentTaskEntityReply, AgentTaskId, AgentTaskScope, AgentTaskStatus, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, HumanCheckpointId, PrincipalRef,
};
use rakka_core::ActorSystem;
use rakka_sharding::ClusterSharding;

mod common;

use common::ShardedWorld;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "goal-ticket-1";
const TOOL: &str = "charge-card";
const ASK_TIMEOUT: Duration = Duration::from_secs(5);

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid")
}

fn goal_id() -> AgentGoalId {
    AgentGoalId::new("customer-goal-1").expect("goal id should be valid")
}

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
}

fn run_scope() -> AgentRunScope {
    let run = run_id_for_assignment(task_scope().task(), AgentAssignmentGeneration::new(1))
        .expect("the run id should be derivable");
    AgentRunScope::new(tenant(), agent_id(), run).expect("run scope should be valid")
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new("resolve-ticket").expect("task definition id should be valid"),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
}

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "ingress".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me charge that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id is valid"),
                AgentToolId::new(TOOL).expect("tool id is valid"),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// Scenario 35's world over the shared [`common::ShardedWorld`] wiring: the
/// checkpoint-gated dispatcher script, the goal task's scopes and refs, and
/// the drive helpers.
struct Sharded {
    system: ActorSystem,
    sharding: ClusterSharding,
    tasks: CrashingStateStore<rakka_agent::AgentTaskState>,
    runs: CrashingStateStore<AgentRunState>,
    dispatcher: ScriptedDispatcher,
    agent: rakka_agent::AgentEntityRef,
    task: AgentTaskEntityRef,
    run: AgentRunEntityRef,
    agent_registration: rakka_agent::AgentEntityRegistration,
    task_registration: rakka_agent::AgentTaskEntityRegistration,
    run_registration: rakka_agent::AgentRunEntityRegistration,
}

impl Sharded {
    fn new(name: &str, idle: Duration) -> Self {
        let dispatcher = ScriptedDispatcher::new()
            .with_turn(tool_calling_turn())
            .with_turn(proposing_turn())
            .with_tool_result(
                TOOL,
                AgentTaskContent::inline(serde_json::json!({ "charged": true }))
                    .expect("the tool result is inline-bounded"),
            );
        let policies = AgentEffectPolicies::new()
            .with_tool_spec(
                AgentToolId::new(TOOL).expect("tool id is valid"),
                AgentEffectSpec::non_idempotent().with_checkpoint_required(),
            )
            .expect("the checkpoint-required tool spec is valid");

        let world = ShardedWorld::new(name, idle, dispatcher, Some(policies));
        let agent = world.agent_ref(&agent_scope());
        let task = world.task_ref(&task_scope());
        let run = world.run_ref(&run_scope());
        let ShardedWorld {
            system,
            sharding,
            tasks,
            runs,
            dispatcher,
            agent_registration,
            task_registration,
            run_registration,
            ..
        } = world;

        Self {
            system,
            sharding,
            tasks,
            runs,
            dispatcher,
            agent,
            task,
            run,
            agent_registration,
            task_registration,
            run_registration,
        }
    }

    async fn instantiate_agent(&self) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(task_definition().definition_id.clone());
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
            "Resolves customer support tickets end to end.",
            envelope,
        )
        .expect("the agent definition should be valid");
        let reply = self
            .agent
            .ask(
                |reply_to| AgentEntityMessage {
                    command: AgentEntityCommand::Instantiate {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::DefinitionUpdate,
                            &agent_scope(),
                            "1",
                        )
                        .expect("operation id should be derivable"),
                        definition: Box::new(definition),
                        settings: Box::new(AgentSettings::default()),
                        provenance: Box::new(provenance(1)),
                    },
                    reply_to,
                },
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded agent replies");
        assert!(
            matches!(reply, AgentEntityReply::Applied { .. }),
            "the agent instantiates, got {reply:?}"
        );
    }

    async fn create_goal_task(&self) -> AgentTaskEntityReply {
        self.create_goal_task_with(None).await
    }

    async fn create_goal_task_with(
        &self,
        goal_spec: Option<Box<rakka_agent::AgentGoalSpecDraft>>,
    ) -> AgentTaskEntityReply {
        self.task
            .ask(
                |reply_to| AgentTaskEntityMessage::Command {
                    command: Box::new(AgentTaskEntityCommand::Create {
                        operation_id: AgentOperationId::new(
                            AgentOperationKind::TaskCreation,
                            [TENANT, TASK, "1"],
                        )
                        .expect("operation id should be derivable"),
                        creation: Box::new(AgentTaskCreation {
                            definition: task_definition(),
                            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                                .expect("the input is inline-bounded"),
                            assignee: Some(agent_id()),
                            team: None,
                            goal: Some(goal_id()),
                            goal_mode: Default::default(),
                            goal_spec: goal_spec.clone(),
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
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded task replies")
    }

    async fn settle_task(&self) {
        let _reply = self
            .task
            .ask(
                |reply_to| AgentTaskEntityMessage::Settle { reply_to },
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded task settles");
    }

    async fn settle_run(&self) -> AgentRunEntityReply {
        self.run
            .ask(
                |reply_to| AgentRunEntityMessage::Settle { reply_to },
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded run settles")
    }

    /// Answers every `Ready` effect from the durable record, through the
    /// sharded command surface — what the dispatcher does, minus the fleet.
    async fn answer_ready_effects(&self) -> usize {
        let Some(state) =
            load_agent_run_state(&self.runs, &run_scope(), &AgentSchemaPolicy::default())
                .await
                .expect("the run state loads")
        else {
            return 0;
        };
        let Some(loop_state) = state.loop_state() else {
            return 0;
        };
        let ready: Vec<_> = loop_state
            .effects()
            .iter()
            .filter(|effect| effect.status == AgentRunEffectStatus::Ready)
            .cloned()
            .collect();
        let mut answered = 0;
        for effect in ready {
            let outcome = self.dispatcher.answer(&effect).await;
            let command = AgentRunEntityCommand::RecordEffectResult {
                operation_id: effect
                    .result_operation_id(&run_scope())
                    .expect("the result operation id derives"),
                effect_id: effect.effect_id.clone(),
                generation: effect.generation,
                attempt: effect.attempts.saturating_add(1),
                fence: 0,
                outcome: Box::new(outcome),
            };
            let _reply = self
                .run
                .ask(
                    |reply_to| AgentRunEntityMessage::Command {
                        command: Box::new(command),
                        reply_to,
                    },
                    ASK_TIMEOUT,
                )
                .await
                .expect("the sharded run replies to the result");
            answered += 1;
        }
        answered
    }

    /// Drives the sharded world until the run parks or completes: settle the
    /// task, settle the run, answer what became dispatchable.
    async fn pump(&self) {
        for _round in 0..16 {
            self.settle_task().await;
            self.settle_run().await;
            let answered = self.answer_ready_effects().await;
            let status = self.run_status().await;
            let parked = status == Some(AgentRunStatus::WaitingForApproval);
            let terminal = status.is_some_and(|status| status.is_terminal());
            if terminal || (parked && answered == 0) {
                return;
            }
        }
        panic!("the sharded world did not converge");
    }

    async fn run_status(&self) -> Option<AgentRunStatus> {
        load_agent_run_state(&self.runs, &run_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .and_then(|state| state.status())
    }

    async fn open_checkpoint(&self) -> Option<HumanCheckpointId> {
        load_agent_run_state(&self.runs, &run_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")?
            .loop_state()?
            .open_checkpoints()
            .first()
            .map(|checkpoint| checkpoint.checkpoint_id.clone())
    }

    /// Passivates all three entities and asserts the whole world holds zero
    /// resident entities — the scenario 35 resource claim.
    fn assert_no_resident_entities(&self, context: &str) {
        for (key_name, count) in [
            (
                "agent",
                self.sharding
                    .registration_state(self.agent_registration.key())
                    .expect("the agent registration exists")
                    .local_entity_count(),
            ),
            (
                "task",
                self.sharding
                    .registration_state(self.task_registration.key())
                    .expect("the task registration exists")
                    .local_entity_count(),
            ),
            (
                "run",
                self.sharding
                    .registration_state(self.run_registration.key())
                    .expect("the run registration exists")
                    .local_entity_count(),
            ),
        ] {
            assert_eq!(
                count, 0,
                "{context}: the {key_name} entity still holds a resident actor"
            );
        }
    }

    async fn resolve_checkpoint(&self, checkpoint_id: HumanCheckpointId) -> AgentRunEntityReply {
        self.run
            .ask(
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::CheckpointResolution,
                            &agent_scope(),
                            "d1",
                        )
                        .expect("the decision key derives"),
                        checkpoint_id,
                        resolver: PrincipalRef {
                            principal_type: "user".to_string(),
                            principal_id: "approver".to_string(),
                            display_name: None,
                        },
                        decision: Box::new(AgentCheckpointDecision::Approval(
                            AgentApprovalDecision::Approve {
                                credential_binding: None,
                                expires_at: AgentTimestampMillis::new(1_000_000),
                                allowed_use_count: 1,
                            },
                        )),
                        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
                    }),
                    reply_to,
                },
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded run replies to the decision")
    }
}

#[tokio::test]
async fn goal_active_work_passivates_to_zero_and_one_trigger_advances_once() {
    let world = Sharded::new("GoalPassivation", Duration::from_secs(60));
    world.instantiate_agent().await;

    // The root task carries the goal; its run parks on the approval wait.
    let created = world.create_goal_task().await;
    assert!(
        matches!(created, AgentTaskEntityReply::Applied { .. }),
        "the goal task is created, got {created:?}"
    );
    world.pump().await;
    assert_eq!(
        world.run_status().await,
        Some(AgentRunStatus::WaitingForApproval),
        "the run parks on the checkpoint"
    );
    let checkpoint_id = world
        .open_checkpoint()
        .await
        .expect("the approval checkpoint is open");

    // Everything passivates. The goal is still logically active — the task is
    // nonterminal and carries the goal id, durably — while no per-agent actor
    // exists anywhere.
    assert!(
        passivate_agent_entity(
            &world.sharding,
            world.agent_registration.key(),
            &agent_scope()
        )
        .expect("agent passivation routes"),
        "the agent was resident"
    );
    assert!(
        passivate_agent_task_entity(
            &world.sharding,
            world.task_registration.key(),
            &task_scope()
        )
        .expect("task passivation routes"),
        "the task was resident"
    );
    assert!(
        passivate_agent_run_entity(&world.sharding, world.run_registration.key(), &run_scope())
            .expect("run passivation routes"),
        "the run was resident"
    );
    world.assert_no_resident_entities("while the goal waits");

    // One durable trigger reactivates the correct owner and the work advances
    // exactly once: approval, gated tool, closing turn, acceptance.
    let decided = world.resolve_checkpoint(checkpoint_id.clone()).await;
    assert!(
        matches!(decided, AgentRunEntityReply::Applied { .. }),
        "the decision applies, got {decided:?}"
    );
    world.pump().await;

    let state = load_agent_run_state(&world.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let run = state.snapshot().expect("the run accepted");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2, "the resumed work took its two turns once");
    assert_eq!(run.goal, Some(goal_id()), "the run carries the goal id");
    assert_eq!(world.dispatcher.tool_calls(), 1, "the gated tool ran once");

    let task_reply = world
        .task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Describe),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task replies");
    let AgentTaskEntityReply::Snapshot(Some(task_snapshot)) = task_reply else {
        panic!("expected a task snapshot, got {task_reply:?}");
    };
    assert_eq!(task_snapshot.status, AgentTaskStatus::Completed);
    assert_eq!(
        task_snapshot.goal,
        Some(goal_id()),
        "the task carries the goal id"
    );

    // The duplicate trigger is answered from the record: no second advance.
    let replay = world.resolve_checkpoint(checkpoint_id).await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "a duplicate trigger must not advance, got {replay:?}"
    );
    let after = load_agent_run_state(&world.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
        .snapshot()
        .expect("the run accepted");
    assert_eq!(after.turn, 2, "the duplicate trigger advanced the run");
    assert_eq!(world.dispatcher.tool_calls(), 1);

    world.system.shutdown();
}

#[tokio::test]
async fn a_trigger_redelivered_after_an_owner_kill_still_advances_once() {
    // The targeted crash of the scenario: the owner dies at the first durable
    // write after the trigger, the trigger is redelivered, and the advance
    // still happens exactly once. The exhaustive write-by-write sweeps live at
    // the store level (`checkpoint_run.rs`); through-actor asks get one
    // targeted window so the sharded surface is proven without ask-timeout
    // flakiness.
    let world = Sharded::new("GoalPassivationCrash", Duration::from_secs(60));
    world.instantiate_agent().await;
    let created = world.create_goal_task().await;
    assert!(matches!(created, AgentTaskEntityReply::Applied { .. }));
    world.pump().await;
    let checkpoint_id = world
        .open_checkpoint()
        .await
        .expect("the approval checkpoint is open");

    // Kill the owner at the decision's own write: the decision is lost before
    // it commits.
    world.runs.crash_at(1, CrashPoint::BeforeWrite);
    let lost = world.resolve_checkpoint(checkpoint_id.clone()).await;
    assert!(
        matches!(lost, AgentRunEntityReply::Rejected { .. }),
        "the killed owner surfaced the loss, got {lost:?}"
    );
    world.runs.survive();

    // The approver redelivers the same decision; the new owner applies it
    // once and the work completes once.
    let redelivered = world.resolve_checkpoint(checkpoint_id).await;
    assert!(
        matches!(redelivered, AgentRunEntityReply::Applied { .. }),
        "the redelivered trigger applies, got {redelivered:?}"
    );
    world.pump().await;

    let run = load_agent_run_state(&world.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
        .snapshot()
        .expect("the run accepted");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2, "the redelivered trigger advanced exactly once");
    assert_eq!(world.dispatcher.tool_calls(), 1, "the gated tool ran once");

    world.system.shutdown();
}

#[tokio::test]
async fn the_goal_contract_stays_addressable_while_fully_passivated() {
    // The slice 4.1 half of the scenario: the root task holds the full
    // `AgentGoalState`, everything passivates to zero, the goal record is
    // read from durable state alone, and one durable goal command
    // reactivates the correct owner and transitions the contract exactly
    // once (specification 8.1's addressability clause).
    let world = Sharded::new("GoalContractPassivation", Duration::from_secs(60));
    world.instantiate_agent().await;

    let created = world
        .create_goal_task_with(Some(Box::new(common::goal_spec_draft(
            common::goal_spec(),
            true,
        ))))
        .await;
    assert!(
        matches!(created, AgentTaskEntityReply::Applied { .. }),
        "the goal-bearing task is created, got {created:?}"
    );
    world.pump().await;
    assert_eq!(
        world.run_status().await,
        Some(AgentRunStatus::WaitingForApproval),
        "the run parks on the checkpoint"
    );

    // Everything passivates while the goal contract is Active.
    passivate_agent_entity(
        &world.sharding,
        world.agent_registration.key(),
        &agent_scope(),
    )
    .expect("agent passivation routes");
    passivate_agent_task_entity(
        &world.sharding,
        world.task_registration.key(),
        &task_scope(),
    )
    .expect("task passivation routes");
    passivate_agent_run_entity(&world.sharding, world.run_registration.key(), &run_scope())
        .expect("run passivation routes");
    world.assert_no_resident_entities("while the goal contract is active");

    // (a) The goal record answers from durable state alone, and reading it
    // wakes nothing.
    let state = rakka_agent::load_agent_task_state(
        &world.tasks,
        &task_scope(),
        &AgentSchemaPolicy::default(),
    )
    .await
    .expect("the task state loads")
    .expect("the task exists");
    let goal = state
        .task()
        .expect("the task is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .clone();
    assert_eq!(goal.status(), rakka_agent::AgentGoalStatus::Active);
    assert_eq!(goal.spec().revision(), AgentRevisionNumber::INITIAL);
    world.assert_no_resident_entities("after the durable point read");

    // (b) One durable goal command reactivates the correct owner and makes
    // the authorized terminal transition exactly once.
    let decide = |step: &str| {
        Box::new(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
                .expect("the operation id derives"),
            decision: Box::new(rakka_agent::AgentGoalDecision {
                reason: rakka_agent::AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(common::goal_evaluation())),
                provenance: Some(Box::new(provenance(100))),
                expected_status_revision: goal.status_revision(),
            }),
        })
    };
    let reply = world
        .task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: decide("satisfy"),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task replies");
    let AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("expected the decision to apply, got {reply:?}");
    };
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        rakka_agent::AgentGoalStatus::Satisfied
    );

    // The duplicate trigger answers from the record: no second transition.
    let replay = world
        .task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: decide("satisfy"),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task replies");
    assert!(
        matches!(replay, AgentTaskEntityReply::Duplicate { .. }),
        "a duplicate decision must not transition again, got {replay:?}"
    );
    let after = rakka_agent::load_agent_task_state(
        &world.tasks,
        &task_scope(),
        &AgentSchemaPolicy::default(),
    )
    .await
    .expect("the task state loads")
    .expect("the task exists");
    let record = after
        .task()
        .expect("the task is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .clone();
    assert_eq!(record.status(), rakka_agent::AgentGoalStatus::Satisfied);
    assert_eq!(
        record.status_revision(),
        goal.status_revision().next(),
        "the terminal transition happened exactly once"
    );

    world.system.shutdown();
}
