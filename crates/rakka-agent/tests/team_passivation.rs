//! The board is durable data, not a resident coordinator
//! ([specification 8.10 and 15](../../../docs/plans/rakka-agent/spec.md),
//! scenario 42 over real sharded entities): a team with a pending claim
//! passivates to zero resident actors, recovers from durable state alone,
//! and the courier's settle passes drive the claim to its one accepted
//! owner across passivation.

mod common;

use std::time::Duration;

use common::{ShardedWorld, TeamStore};
use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    passivate_agent_entity, passivate_agent_task_entity, passivate_agent_team_entity,
    AgentAssignmentStatus, AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId,
    AgentEntityCommand, AgentEntityMessage, AgentEntityReply, AgentGoalId, AgentId,
    AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentScope, AgentSettings,
    AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityReply, AgentTaskId,
    AgentTaskScope, AgentTaskTeamClaimStatus, AgentTeamBoardEntryStatus, AgentTeamCreation,
    AgentTeamEntityCommand, AgentTeamEntityMessage, AgentTeamEntityReply, AgentTeamId,
    AgentTeamPolicy, AgentTeamScope, TenantId,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};
use std::collections::{BTreeMap, BTreeSet};

const TENANT: &str = "acme";
const TEAM: &str = "support-team";
const MEMBER: &str = "worker-a";
const TASK: &str = "board-ticket-1";
const ASK_TIMEOUT: Duration = Duration::from_secs(5);

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn member() -> AgentId {
    AgentId::new(MEMBER).expect("the member id is valid")
}

fn member_scope() -> AgentScope {
    AgentScope::new(tenant(), member()).expect("the member scope is valid")
}

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(
        tenant(),
        AgentTeamId::new(TEAM).expect("the team id is valid"),
    )
    .expect("the team scope is valid")
}

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(TASK).expect("the task id is valid"),
    )
    .expect("the task scope is valid")
}

fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new("resolve-ticket").expect("the definition id is valid"),
        "Resolve one customer support ticket.",
        common::schema("ticket-input"),
        common::schema("ticket-result"),
    )
    .expect("the task definition is valid")
}

fn provenance(at: u64) -> rakka_agent::AgentRevisionProvenance {
    rakka_agent::AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "wiring".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn team_op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::TeamClaim, [TENANT, TEAM, discriminator])
        .expect("the operation id derives")
}

async fn apply_team(world: &ShardedWorld, command: AgentTeamEntityCommand) -> AgentTeamEntityReply {
    world
        .team_ref(&team_scope())
        .ask(
            |reply_to| AgentTeamEntityMessage::Command {
                command: Box::new(command),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded team replies")
}

async fn settle_team(world: &ShardedWorld) {
    let _ = world
        .team_ref(&team_scope())
        .ask(
            |reply_to| AgentTeamEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded team settles");
}

async fn settle_task(world: &ShardedWorld) {
    let _ = world
        .task_ref(&task_scope())
        .ask(
            |reply_to| AgentTaskEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task settles");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_board_survives_passivation_and_the_claim_activates_across_it() {
    let world = ShardedWorld::new(
        "team-passivation",
        Duration::from_secs(60),
        ScriptedDispatcher::new(),
        None,
    );

    // The member agent, instantiated on the real sharded entity.
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(AgentTaskDefinitionId::new("resolve-ticket").expect("the definition id is valid"));
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
        "Resolves customer support tickets end to end.",
        envelope,
    )
    .expect("the agent definition is valid");
    let reply = world
        .agent_ref(&member_scope())
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::Instantiate {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::DefinitionUpdate,
                        &member_scope(),
                        "1",
                    )
                    .expect("the operation id derives"),
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
    assert!(matches!(reply, AgentEntityReply::Applied { .. }));

    // The board task: team provenance, no assignee — it waits for a claim.
    let reply = world
        .task_ref(&task_scope())
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, TASK, "1"],
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(AgentTaskCreation {
                        definition: task_definition(),
                        input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                            .expect("the input is inline-bounded"),
                        assignee: None,
                        team: Some(AgentTeamId::new(TEAM).expect("the team id is valid")),
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
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task replies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));

    // Team, board entry, claim — all durable decisions on the sharded team.
    let mut members: BTreeMap<AgentId, BTreeSet<rakka_agent::AgentCapabilityId>> = BTreeMap::new();
    members.insert(member(), BTreeSet::new());
    let reply = apply_team(
        &world,
        AgentTeamEntityCommand::Create {
            operation_id: team_op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: member(),
                root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                members,
            }),
        },
    )
    .await;
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));
    let reply = apply_team(
        &world,
        AgentTeamEntityCommand::PostTask {
            operation_id: team_op("post"),
            task: task_scope().task().clone(),
            posted_by: member(),
        },
    )
    .await;
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));
    let reply = apply_team(
        &world,
        AgentTeamEntityCommand::Claim {
            operation_id: team_op("claim"),
            task: task_scope().task().clone(),
            member: member(),
            expected_epoch: 0,
        },
    )
    .await;
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));

    // Everything passivates: the pending claim holds no actor, future, or
    // timer resident — the board is data.
    assert!(passivate_agent_team_entity(
        &world.sharding,
        world.team_registration.key(),
        &team_scope()
    )
    .expect("the team passivates"));
    assert!(passivate_agent_task_entity(
        &world.sharding,
        world.task_registration.key(),
        &task_scope()
    )
    .expect("the task passivates"));
    assert!(passivate_agent_entity(
        &world.sharding,
        world.agent_registration.key(),
        &member_scope()
    )
    .expect("the agent passivates"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        world.resident_entities(),
        0,
        "the pending claim keeps nothing resident"
    );

    // Recovery is the courier: every settle pass rebuilds its entity from
    // durable state and drives what the board decision owed.
    settle_team(&world).await;
    settle_task(&world).await;
    settle_task(&world).await;
    settle_team(&world).await;

    let reply = world
        .task_ref(&task_scope())
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Describe),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task describes");
    let AgentTaskEntityReply::Snapshot(Some(task)) = reply else {
        panic!("the task snapshots, got {reply:?}");
    };
    let assignment = task.assignment.expect("the assignment stands");
    assert_eq!(assignment.agent, member());
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    let claim = task.team_claim.expect("the claim provenance stands");
    assert_eq!(claim.status, AgentTaskTeamClaimStatus::Accepted);

    let reply = apply_team(&world, AgentTeamEntityCommand::Describe).await;
    let AgentTeamEntityReply::Snapshot(Some(team)) = reply else {
        panic!("the team snapshots, got {reply:?}");
    };
    let entry = team
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the task");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    assert_eq!(
        entry.claim.as_ref().expect("the echo stands").member,
        member()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_team_passivates_on_its_own() {
    // A short idle window: the created team must leave residency without
    // any explicit command — no resident coordinator, ever.
    let world = ShardedWorld::new(
        "team-idle-passivation",
        Duration::from_millis(50),
        ScriptedDispatcher::new(),
        None,
    );
    let reply = apply_team(
        &world,
        AgentTeamEntityCommand::Create {
            operation_id: team_op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: member(),
                root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                members: BTreeMap::new(),
            }),
        },
    )
    .await;
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));
    assert!(world.resident_entities() >= 1);

    let mut waited = Duration::ZERO;
    while world.resident_entities() > 0 && waited < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(25)).await;
        waited += Duration::from_millis(25);
    }
    assert_eq!(
        world.resident_entities(),
        0,
        "the idle team passivated on its own"
    );

    // And it answers again from durable state alone.
    let reply = apply_team(&world, AgentTeamEntityCommand::Describe).await;
    let AgentTeamEntityReply::Snapshot(Some(team)) = reply else {
        panic!("the team snapshots, got {reply:?}");
    };
    assert_eq!(team.leader, member());
}

// Keep the unused-import lint honest: the sharded world's team store type is
// part of the fixture surface this file exercises.
#[allow(dead_code)]
fn _store_type(_: &TeamStore) {}
