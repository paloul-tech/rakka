//! An idle agent with blocked future work auto-passivates and reactivates.
//!
//! Specification: sections 6.11 and 15 ("Waiting and otherwise quiescent
//! agent/task/run ... entities SHOULD passivate under normal idle policy even
//! when ... future tasks remain assigned/blocked"); scenario 46 of section
//! 18, which is open decision 20's proof: no Akka-style idle residency —
//! auto-passivation needs no `terminate` or `suspend`, because suspend
//! controls admission and terminate controls lifecycle, never memory-resource
//! release. The entities here are real sharded actors with a short idle
//! policy: after the blocked task is created, *nothing* is commanded, and the
//! sharding's own idle timer evicts every resident actor while the agent
//! stays `Active` and the task stays `Blocked` with its assignee — durably,
//! readable without waking anything. Work becomes eligible through one
//! durable trigger: the dependency-outcome command flips `Blocked` to
//! eligible and the next settle decides the assignment. The upstream task
//! notifying its dependents is the later coordination slices' courier; at M1
//! the command *is* the durable trigger, injected exactly as that courier
//! will inject it. A replayed trigger is answered from the record: one
//! assignment generation, one run, one turn.

use std::time::Duration;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_entity_state, load_agent_run_state, load_agent_task_state, run_id_for_assignment,
    AgentAssignmentGeneration, AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId,
    AgentEntityCommand, AgentEntityMessage, AgentEntityReply, AgentId, AgentLifecycleStatus,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityMessage,
    AgentRunScope, AgentRunStatus, AgentSchemaId, AgentSchemaPolicy, AgentSchemaRef, AgentScope,
    AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome, AgentTaskEntityCommand,
    AgentTaskEntityMessage, AgentTaskEntityReply, AgentTaskId, AgentTaskScope, AgentTaskStatus,
    TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

mod common;

use common::ShardedWorld;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "future-ticket-1";
const UPSTREAM: &str = "upstream-report";
const ASK_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE: Duration = Duration::from_millis(200);

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid")
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

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

#[tokio::test]
async fn an_idle_agent_with_a_blocked_task_auto_passivates_and_reactivates() {
    let world = ShardedWorld::new(
        "IdleAgentReactivation",
        IDLE,
        ScriptedDispatcher::new().with_turn(proposing_turn()),
        None,
    );
    let agent = world.agent_ref(&agent_scope());
    let task = world.task_ref(&task_scope());
    let run = world.run_ref(&run_scope());

    // Instantiate the agent and create the future task: blocked behind an
    // upstream dependency, assignee already set. No admission gate applies —
    // the definition's default operation class is attended.
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
    let instantiated = agent
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
    assert!(matches!(instantiated, AgentEntityReply::Applied { .. }));

    let created = task
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
                        goal: None,
                        goal_mode: Default::default(),
                        goal_spec: None,
                        parent: None,
                        dependencies: vec![AgentTaskDependencyDeclaration::new(
                            AgentTaskId::new(UPSTREAM).expect("task id should be valid"),
                        )],
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
        .expect("the sharded task replies");
    let AgentTaskEntityReply::Applied { outcome } = created else {
        panic!("the blocked task is created, got {created:?}");
    };
    assert_eq!(outcome.status, AgentTaskStatus::Blocked);

    // No `terminate`, no `suspend`, no further command of any kind: the idle
    // policy alone must evict every resident actor.
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

    // The passivated world is still logically available, durably: the agent
    // is `Active`, the task is `Blocked` with its assignee — read without
    // waking anything.
    let durable_agent =
        load_agent_entity_state(&world.agents, &agent_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the agent state loads")
            .expect("the agent exists");
    assert_eq!(durable_agent.status(), AgentLifecycleStatus::Active);
    let durable_task =
        load_agent_task_state(&world.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the task state loads")
            .expect("the task exists");
    assert_eq!(durable_task.status(), Some(AgentTaskStatus::Blocked));

    // Work becomes eligible: the durable dependency-outcome trigger flips the
    // task, and the entity's own settle decides the assignment.
    let resolve = |discriminator: &'static str| AgentTaskEntityCommand::RecordDependencyOutcome {
        operation_id: AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, "resolve"])
            .expect("operation id should be derivable"),
        dependency: AgentTaskId::new(UPSTREAM)
            .unwrap_or_else(|_| panic!("{discriminator}: task id should be valid")),
        outcome: AgentTaskDependencyOutcome::Completed,
    };
    let resolved = task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(resolve("first")),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the reactivated task replies");
    assert!(
        matches!(resolved, AgentTaskEntityReply::Applied { .. }),
        "the trigger applies, got {resolved:?}"
    );

    // Drive to completion through the sharded surface. A stall must name
    // itself rather than fall through to an opaque status assertion.
    let mut drive_converged = false;
    for _round in 0..16 {
        let _task_settled = task
            .ask(
                |reply_to| AgentTaskEntityMessage::Settle { reply_to },
                ASK_TIMEOUT,
            )
            .await
            .expect("the task settles");
        let _run_settled = run
            .ask(
                |reply_to| AgentRunEntityMessage::Settle { reply_to },
                ASK_TIMEOUT,
            )
            .await
            .expect("the run settles");

        let state = load_agent_run_state(&world.runs, &run_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads");
        let mut answered = 0;
        if let Some(loop_state) = state.as_ref().and_then(|state| state.loop_state()) {
            let ready: Vec<_> = loop_state
                .effects()
                .iter()
                .filter(|effect| effect.status == AgentRunEffectStatus::Ready)
                .cloned()
                .collect();
            for effect in ready {
                let outcome = world.dispatcher.answer(&effect).await;
                let _reply = run
                    .ask(
                        |reply_to| AgentRunEntityMessage::Command {
                            command: Box::new(AgentRunEntityCommand::RecordEffectResult {
                                operation_id: effect
                                    .result_operation_id(&run_scope())
                                    .expect("the result operation id derives"),
                                effect_id: effect.effect_id.clone(),
                                generation: effect.generation,
                                attempt: effect.attempts.saturating_add(1),
                                fence: 0,
                                outcome: Box::new(outcome),
                            }),
                            reply_to,
                        },
                        ASK_TIMEOUT,
                    )
                    .await
                    .expect("the run replies to the result");
                answered += 1;
            }
        }
        let terminal = state
            .and_then(|state| state.status())
            .is_some_and(|status| status.is_terminal());
        if terminal && answered == 0 {
            drive_converged = true;
            break;
        }
    }
    assert!(drive_converged, "the reactivated flow did not converge");

    let run_state = load_agent_run_state(&world.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
        .snapshot()
        .expect("the run accepted");
    assert_eq!(run_state.status, AgentRunStatus::Completed);
    assert_eq!(run_state.turn, 1, "the eligible work advanced exactly once");

    // The replayed trigger — a duplicate timer scan, a redelivered courier
    // exchange — is answered from the record: one assignment generation, one
    // run, no second advance.
    let replay = task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(resolve("replay")),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the task replies to the replay");
    assert!(
        matches!(replay, AgentTaskEntityReply::Duplicate { .. }),
        "a replayed trigger must not advance, got {replay:?}"
    );
    let durable_task =
        load_agent_task_state(&world.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the task state loads")
            .expect("the task exists");
    let converged = durable_task.task().expect("the task is created");
    assert_eq!(durable_task.status(), Some(AgentTaskStatus::Completed));
    assert_eq!(converged.assignment_generation.get(), 1);

    world.system.shutdown();
}
