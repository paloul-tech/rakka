//! Goal-active work passivates to zero and one durable trigger resumes it.
//!
//! Specification: sections 6.11 and 15; scenario 35 of section 18. The M1
//! reading, pinned here deliberately: the goal types of `goal.rs` are filled
//! by the later goal slices, so at M1 "an `Active` goal" is a non-terminal
//! root task carrying an `AgentGoalId`, and "its waiting runs" is the task's
//! run parked on a durable approval wait. All three entities — agent, task,
//! run — are real sharded actors here, and all three passivate to a local
//! entity count of zero while the goal remains logically active: the wait is
//! the durable checkpoint record (the no-open-span half is scenario 22's
//! proof in `trace_scenarios.rs`), not a resident actor, task, or timer.
//! One durable trigger — the checkpoint decision command — reactivates the
//! correct owner and advances the work exactly once: a duplicate decision is
//! answered from the record, and an owner killed mid-resume converges on the
//! same single advance after the trigger is redelivered. Timer-driven wakes
//! are the phase 3 wake controller's; nothing here invents one.

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
                            goal: Some(goal_id()),
                            goal_mode: Default::default(),
                            parent: None,
                            dependencies: Vec::new(),
                            escrow: None,
                            wake: None,
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
