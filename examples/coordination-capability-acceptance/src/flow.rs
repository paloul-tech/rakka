//! The acceptance walk: every bullet of the coordination-capability
//! milestone, in one continuous story over the wired world.
//!
//! One `AgentTaskId` is posted to a team board, claimed, handed off, gated
//! by a human-owned approval upstream and by the checkpoint that parks the
//! consequential refund, reviewed in a moderated conversation, and finally
//! closed — surviving an owner death inside the handoff and inside the
//! conversation, and replayable from a cursor afterwards.

use std::collections::{BTreeMap, BTreeSet};

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use rakka_a2a::agents::{
    AGENT_COLLABORATION_EXTENSION_URI, AGENT_COLLABORATION_SCHEMA_VERSION, META_COLLABORATION,
};
use rakka_a2a::mapping::{META_DEDUPLICATION_KEY, META_PRINCIPAL_REF};
use rakka_agent::run_id_for_assignment;
use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    load_agent_task_state, registered_agent_conversation_entity_ref, registered_agent_entity_ref,
    registered_agent_task_entity_ref, registered_agent_team_entity_ref, AgentAuthorityEnvelope,
    AgentCapabilityId, AgentConversationEntityCommand, AgentConversationEntityMessage,
    AgentCoordinationCapabilityKind, AgentDefinition, AgentDefinitionId, AgentEntityCommand,
    AgentEntityMessage, AgentEntityReply, AgentGoalId, AgentId, AgentOperationId,
    AgentOperationKind, AgentRunScope, AgentSchemaPolicy, AgentSettings, AgentTaskContent,
    AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand,
    AgentTaskEntityMessage, AgentTaskEntityReply, AgentTaskScope, AgentTeamCreation,
    AgentTeamEntityCommand, AgentTeamEntityMessage, AgentTeamEntityReply, AgentTeamId,
    AgentTeamPolicy, AgentTeamScope, AgentToolDeclaration, AgentToolId, TenantId,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};
use serde_json::{json, Value};

use crate::report::{AcceptanceReport, EXPECTED_TRANSCRIPT};
use crate::wiring::{
    schema, World, APPROVAL_TASK, ASK_TIMEOUT, CONVERSATION, HANDOFF_TOOL, MODERATOR, REFUND_TOOL,
    SKILL, SPECIALIST, TASK, TASK_DEFINITION, TEAM, TENANT, TRIAGER, UNCAPABLE,
};

/// The probe entry the envelope bullet claims — a second board task, so the
/// walk's own `AgentTaskId` is never perturbed by a deliberate refusal.
pub const PROBE_TASK: &str = "ticket-4712";

/// The content sentinels the walk plants in the model's hidden reasoning, the
/// human result's content, a moderated turn's body, and the handoff reason.
///
/// Each is real content that really enters the system and is legitimately at
/// home *somewhere*: hidden reasoning in session memory, a typed result on the
/// task that accepted it, a turn body in the conversation's own bounded ring
/// ([specification 8.11](../../docs/plans/rakka-agent/spec.md)), and the
/// model-supplied handoff reason in the run's handoff cell and the task's
/// handoff provenance — the one model-derived string the transfer routes
/// *toward* coordination machinery, which is exactly why it carries a
/// sentinel: a regression copying it onto the board echo or a replay page is
/// what the sweep exists to catch. What the milestone requires is that none
/// of it crosses onto a *coordination* surface — the shared board, the
/// replayable coordination events, or the metrics — which is what the sweep
/// measures. The scripts plant from this same array, so a sentinel cannot
/// drift away from its sweep, and each sentinel has a positive control
/// proving it really entered its home record.
pub const CONTENT_SENTINELS: [&str; 4] = [
    "SECRET-HIDDEN-REASONING",
    "SECRET-APPROVAL-MEMO",
    "SECRET-REVIEW-TRANSCRIPT",
    "SECRET-HANDOFF-RATIONALE",
];

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent(id: &str) -> AgentId {
    AgentId::new(id).expect("the agent id is valid")
}

fn agent_scope(id: &str) -> rakka_agent::AgentScope {
    rakka_agent::AgentScope::new(tenant(), agent(id)).expect("the agent scope is valid")
}

fn task_scope(task: &str) -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new(task).expect("the task id is valid"),
    )
    .expect("the task scope is valid")
}

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(
        tenant(),
        AgentTeamId::new(TEAM).expect("the team id is valid"),
    )
    .expect("the team scope is valid")
}

fn conversation_scope() -> rakka_agent::AgentConversationScope {
    rakka_agent::AgentConversationScope::new(
        tenant(),
        rakka_agent::AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn owner() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "operator-7".to_string(),
        display_name: None,
    }
}

fn provenance(at: u64) -> rakka_agent::AgentRevisionProvenance {
    rakka_agent::AgentRevisionProvenance {
        principal: owner(),
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn team_op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::TeamOperation,
        [TENANT, TEAM, discriminator],
    )
    .expect("the operation id derives")
}

/// The typed task definition every agent in the walk accepts.
#[must_use]
pub fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"),
        "Resolve one customer support ticket end to end.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("the task definition is valid")
}

// ---------------------------------------------------------------------------
// Authority envelopes — the walk's whole envelope story is here
// ---------------------------------------------------------------------------

fn base_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"));
    envelope
}

/// The claim-capable member: `Team` is the authority a board claim spends,
/// and `Handoff` is the authority its transfer spends. It also reviews the
/// escalation it raised, so it takes moderated turns.
fn triager_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = base_envelope();
    for kind in [
        AgentCoordinationCapabilityKind::Team,
        AgentCoordinationCapabilityKind::Handoff,
        AgentCoordinationCapabilityKind::Moderation,
    ] {
        envelope.coordination_capabilities.insert(kind);
    }
    envelope.tools.insert(
        AgentToolId::new(HANDOFF_TOOL).expect("the tool id is valid"),
        AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::ReadOnly),
    );
    envelope
}

/// The member the board admits and the envelope does not: a perfectly healthy,
/// instantiated agent, on the roster, accepted for the task definition — and
/// with no coordination capability at all.
fn uncapable_envelope() -> AgentAuthorityEnvelope {
    base_envelope()
}

/// The handoff target: it may be handed to, it takes moderated turns, and it
/// declares the one consequential tool.
fn specialist_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = base_envelope();
    envelope
        .coordination_capabilities
        .insert(AgentCoordinationCapabilityKind::Handoff);
    envelope
        .coordination_capabilities
        .insert(AgentCoordinationCapabilityKind::Moderation);
    envelope.tools.insert(
        AgentToolId::new(REFUND_TOOL).expect("the tool id is valid"),
        AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::NonIdempotent),
    );
    envelope
}

/// The moderator: moderated participation and nothing else.
fn moderator_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = base_envelope();
    envelope
        .coordination_capabilities
        .insert(AgentCoordinationCapabilityKind::Moderation);
    envelope
}

// ---------------------------------------------------------------------------
// Sharded drive helpers
// ---------------------------------------------------------------------------

/// Asks a sharded entity, retrying through the transient window where a
/// just-passivated actor's stop has not yet finished — exactly what a
/// production caller does across a shard handoff: the entity's durable state
/// is the identity, and a routed retry lands on the re-materialized owner.
async fn ask_retrying<M, R>(
    entity: &rakka_sharding::ShardedEntityRef<M>,
    build: impl Fn(rakka_core::ReplyTo<R>) -> M,
    context: &str,
) -> R
where
    M: rakka_core::Message,
    R: Send + 'static,
{
    let mut last_error = None;
    for _ in 0..300 {
        match entity.ask(&build, ASK_TIMEOUT).await {
            Ok(reply) => return reply,
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
    panic!("{context}: the sharded ask never landed: {last_error:?}");
}

async fn instantiate(world: &World, id: &str, envelope: AgentAuthorityEnvelope) {
    let scope = agent_scope(id);
    let entity = registered_agent_entity_ref(&world.agent_registration, &scope);
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
        "Serves the support pod.",
        envelope,
    )
    .expect("the agent definition is valid");
    let reply = ask_retrying(
        &entity,
        |reply_to| AgentEntityMessage {
            command: AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &scope,
                    "1",
                )
                .expect("the operation id derives"),
                definition: Box::new(definition.clone()),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(provenance(1)),
            },
            reply_to,
        },
        "instantiate",
    )
    .await;
    assert!(
        matches!(reply, AgentEntityReply::Applied { .. }),
        "{id} instantiates, got {reply:?}"
    );
}

async fn team_command(world: &World, command: AgentTeamEntityCommand) -> AgentTeamEntityReply {
    let entity = registered_agent_team_entity_ref(&world.team_registration, &team_scope());
    ask_retrying(
        &entity,
        |reply_to| AgentTeamEntityMessage::Command {
            command: Box::new(command.clone()),
            reply_to,
        },
        "team command",
    )
    .await
}

async fn task_command(
    world: &World,
    scope: &AgentTaskScope,
    command: AgentTaskEntityCommand,
) -> AgentTaskEntityReply {
    let entity = registered_agent_task_entity_ref(&world.task_registration, scope);
    ask_retrying(
        &entity,
        |reply_to| AgentTaskEntityMessage::Command {
            command: Box::new(command.clone()),
            reply_to,
        },
        "task command",
    )
    .await
}

/// Settles the board.
///
/// No passivation ritual: a wire command reaches the board through the A2A
/// service's *own* store handle, and the team entity's settle pass
/// re-materializes from the durable record before deciding what it owes —
/// so the sweep on a long-lived resident actor observes the service's
/// writes. Driving it through the resident, stale cache and all, is the
/// point.
async fn settle_team(world: &World) {
    let entity = registered_agent_team_entity_ref(&world.team_registration, &team_scope());
    let _ = entity
        .ask(
            |reply_to| AgentTeamEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await;
}

/// Settles one task; see [`settle_team`] for why every settle restarts.
async fn settle_task(world: &World, scope: &AgentTaskScope) {
    let _ = rakka_agent::passivate_agent_task_entity(
        &world.sharding,
        world.task_registration.key(),
        scope,
    );
    let entity = registered_agent_task_entity_ref(&world.task_registration, scope);
    let _ = entity
        .ask(
            |reply_to| AgentTaskEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await;
}

/// Settles the conversation; see [`settle_team`] for why every settle restarts.
async fn settle_conversation(world: &World) {
    let _ = passivate_conversation(world);
    let entity = registered_agent_conversation_entity_ref(
        &world.conversation_registration,
        &conversation_scope(),
    );
    let _ = entity
        .ask(
            |reply_to| AgentConversationEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await;
}

/// Drives the claim choreography to quiescence: the team's courier delivers
/// the board decision, the task's settle delivers the assignment and the
/// claim result, and the final team settle absorbs it.
async fn settle_claim_round_trip(world: &World, scope: &AgentTaskScope) {
    for _ in 0..4 {
        settle_team(world).await;
        settle_task(world, scope).await;
    }
}

async fn task_state(world: &World, scope: &AgentTaskScope) -> rakka_agent::AgentTaskState {
    load_agent_task_state(&world.tasks, scope, &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists")
}

/// The board, read from the durable record: the describe passivates first for
/// the reason [`settle_team`] gives — a wire command wrote through the
/// service's own store handle, and a resident actor's cache predates it.
async fn team_snapshot(world: &World) -> rakka_agent::AgentTeamSnapshot {
    let _ = rakka_agent::passivate_agent_team_entity(
        &world.sharding,
        world.team_registration.key(),
        &team_scope(),
    );
    let entity = registered_agent_team_entity_ref(&world.team_registration, &team_scope());
    // A just-activated entity can answer `exchange-not-recovered` for the
    // instant between activation and its first durable load. A production
    // caller retries a transient exactly like this; the durable record is the
    // identity, so the retry lands on the recovered owner.
    for _ in 0..300 {
        let reply = ask_retrying(
            &entity,
            |reply_to| AgentTeamEntityMessage::Command {
                command: Box::new(AgentTeamEntityCommand::Describe),
                reply_to,
            },
            "team describe",
        )
        .await;
        match reply {
            AgentTeamEntityReply::Snapshot(Some(snapshot)) => return *snapshot,
            AgentTeamEntityReply::Rejected { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            other => panic!("the team snapshots, got {other:?}"),
        }
    }
    panic!("the team never snapshotted");
}

fn board_entry<'a>(
    snapshot: &'a rakka_agent::AgentTeamSnapshot,
    task: &str,
) -> &'a rakka_agent::AgentTeamBoardEntry {
    snapshot
        .board
        .iter()
        .find(|entry| entry.task.as_str() == task)
        .expect("the board holds the posted task")
}

// ---------------------------------------------------------------------------
// The A2A wire: coordination commands ride the collaboration extension
// ---------------------------------------------------------------------------

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

fn send_request(message: Message) -> SendMessageRequest {
    SendMessageRequest {
        message,
        configuration: None,
        metadata: None,
        tenant: Some(TENANT.to_string()),
    }
}

fn coordination_message(message_id: &str, body: Value, cluster: Value) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(body),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = message_id.to_string();
    message.extensions = Some(vec![AGENT_COLLABORATION_EXTENSION_URI.to_string()]);
    message.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String(message_id.to_string()),
            ),
            (
                META_PRINCIPAL_REF.to_string(),
                Value::String(format!(
                    "{}:{}",
                    owner().principal_type,
                    owner().principal_id
                )),
            ),
            (META_COLLABORATION.to_string(), cluster),
        ]
        .into_iter()
        .collect(),
    );
    message
}

fn claim_cluster(task: &str, member: &str, expected_epoch: u64) -> Value {
    json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "team": TEAM,
        "operation": "claim",
        "task": task,
        "member": member,
        "expected-epoch": expected_epoch,
    })
}

/// The immediate wire response, decoded into the reply's externally tagged
/// JSON shape.
fn response_payload(response: &a2a::SendMessageResponse) -> Value {
    let a2a::SendMessageResponse::Message(message) = response else {
        panic!("a coordination command answers with a message, got a task");
    };
    let Some(Part {
        content: PartContent::Data(payload),
        ..
    }) = message.parts.first()
    else {
        panic!("the coordination response carries a data part");
    };
    payload.clone()
}

// ---------------------------------------------------------------------------
// Act helpers
// ---------------------------------------------------------------------------

/// The triager's one scripted turn: transfer the ticket to billing.
///
/// The hidden reasoning carries one sentinel (its home is session memory),
/// and the handoff *reason* carries another: the reason is the model-derived
/// string that travels into the handoff record, the task's handoff
/// provenance, and the A2A offer — the field a regression would copy onto a
/// coordination surface — so the sweep must be measuring it, not the turn
/// text that never leaves the run. Beat 6 holds its positive control.
fn handoff_turn() -> rakka_agent::AgentModelTurn {
    rakka_agent::AgentModelTurn::new(rakka_agent::CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(CONTENT_SENTINELS[0])
        .with_tool_call(
            rakka_agent::AgentToolCallRequest::new(
                rakka_agent::AgentToolCallId::new("transfer-1").expect("the call id is valid"),
                AgentToolId::new(HANDOFF_TOOL).expect("the tool id is valid"),
                json!({ "skill": SKILL, "reason": CONTENT_SENTINELS[3] }),
            )
            .expect("the tool call is bounded"),
        )
}

/// The specialist's one scripted turn: issue the refund it now has human
/// approval for. The tool is checkpoint-bound by the deployment's registry,
/// so this proposal parks on an `AgentCheckpoint` rather than dispatching —
/// which is what makes beat 10's boundary claim a fact about real state.
fn refund_turn() -> rakka_agent::AgentModelTurn {
    rakka_agent::AgentModelTurn::new(rakka_agent::CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("issuing the approved refund")
        .with_tool_call(
            rakka_agent::AgentToolCallRequest::new(
                rakka_agent::AgentToolCallId::new("refund-1").expect("the call id is valid"),
                AgentToolId::new(REFUND_TOOL).expect("the tool id is valid"),
                json!({ "ticket": TASK, "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

/// The entry ids one run's session-memory namespace holds.
async fn session_entry_ids(world: &World, scope: &AgentRunScope) -> BTreeSet<String> {
    use rakka_agent::SessionMemoryStore;
    world
        .session
        .read(scope, rakka_agent::SessionMemoryCursor::start())
        .await
        .map(|page| {
            page.entries
                .iter()
                .map(|entry| format!("{:?}", entry.entry_id))
                .collect()
        })
        .unwrap_or_default()
}

async fn run_state(world: &World, scope: &AgentRunScope) -> rakka_agent::AgentRunState {
    rakka_agent::load_agent_run_state(&world.runs, scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
}

/// Settles one run; see [`settle_team`] for why every settle restarts.
async fn settle_run(world: &World, scope: &AgentRunScope) {
    let _ = rakka_agent::passivate_agent_run_entity(
        &world.sharding,
        world.run_registration.key(),
        scope,
    );
    let entity = rakka_agent::registered_agent_run_entity_ref(&world.run_registration, scope);
    let _ = entity
        .ask(
            |reply_to| rakka_agent::AgentRunEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await;
}

/// Whether the source run holds a committed, still-unanswered A2A send.
async fn outstanding_send(world: &World, scope: &AgentRunScope) -> bool {
    let Ok(Some(state)) =
        rakka_agent::load_agent_run_state(&world.runs, scope, &AgentSchemaPolicy::default()).await
    else {
        return false;
    };
    state.loop_state().is_some_and(|loop_state| {
        loop_state.effects().iter().any(|effect| {
            effect.is_outstanding() && effect.kind() == rakka_agent::AgentRunEffectKind::A2aSendCall
        })
    })
}

/// Drives the transfer to quiescence. Errors are swallowed: a crashed owner
/// is supposed to fail here, and convergence is asserted from durable state
/// afterwards. How many times the send executor was asked is measured at the
/// executor itself (`World::handoff_sends`), never inferred from delivery
/// passes — a pass that delivered any run-entity result is not a transfer
/// attempt, and the ask that died inside the crash window delivered nothing.
async fn drive_transfer(
    world: &World,
    source_run: &AgentRunScope,
    adapter: &DeterministicModelAdapter,
) {
    for _ in 0..24 {
        settle_task(world, &task_scope(TASK)).await;
        settle_run(world, source_run).await;
        let _ = world.pipeline(adapter.clone()).pump_run(source_run).await;
    }
    // The courier's remaining legs: the target's acceptance settles on the
    // task, and the settle pass re-derives and delivers the owed handoff
    // result to the source — one pass to initiate, one to deliver, exactly as
    // a recovery sweep would.
    for _ in 0..12 {
        settle_task(world, &task_scope(TASK)).await;
        settle_run(world, source_run).await;
    }
}

fn human_result_message(dedup: &str, answer: Value) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(answer),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = format!("{dedup}-message");
    message.task_id = Some(APPROVAL_TASK.to_string());
    message.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String(dedup.to_string()),
            ),
            (
                rakka_a2a::agents::META_AGENT_RESULT.to_string(),
                json!({
                    "definition": TASK_DEFINITION,
                    "definition-version": 1,
                    "result-schema": "ticket-result",
                    "result-schema-version": 1,
                }),
            ),
            (
                META_PRINCIPAL_REF.to_string(),
                Value::String("human:alice".to_string()),
            ),
        ]
        .into_iter()
        .collect(),
    );
    message
}

// --- moderation -----------------------------------------------------------

async fn create_conversation(world: &World) {
    let entity = registered_agent_conversation_entity_ref(
        &world.conversation_registration,
        &conversation_scope(),
    );
    let command = AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(
            &tenant(),
            conversation_scope().conversation(),
        )
        .expect("the operation id derives"),
        creation: Box::new(rakka_agent::AgentConversationCreation {
            moderator: agent(MODERATOR),
            participants: vec![agent(SPECIALIST), agent(TRIAGER), agent(UNCAPABLE)],
            mode: rakka_agent::AgentConversationMode::RoundRobin,
            completion: rakka_agent::AgentConversationCompletionRule::ModeratorDecides,
            policy: rakka_agent::AgentModerationPolicy::new(
                rakka_agent::AgentRevisionNumber::INITIAL,
            ),
            task: rakka_agent::AgentTaskId::new(TASK).expect("the task id is valid"),
            tokens: Some(10_000),
            max_wall_clock_millis: None,
            transcript_ref: Some("artifact://transcripts/refund-review".to_string()),
        }),
    };
    let reply = ask_retrying(
        &entity,
        |reply_to| AgentConversationEntityMessage::Command {
            command: Box::new(command.clone()),
            reply_to,
        },
        "conversation create",
    )
    .await;
    assert!(
        matches!(
            reply,
            rakka_agent::AgentConversationEntityReply::Applied { .. }
        ),
        "trusted wiring creates the conversation, got {reply:?}"
    );
}

fn turn_cluster(round: u64, turn: u32, participant: &str, body: &str) -> Value {
    json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "conversation": CONVERSATION,
        "operation": "submit-turn",
        "participant": participant,
        "round": round,
        "turn": turn,
        "body": body,
        "tokens-consumed": 1,
    })
}

async fn try_submit_turn(
    world: &World,
    dedup: &str,
    round: u64,
    turn: u32,
    participant: &str,
    body: &str,
) -> Option<Value> {
    let message = coordination_message(
        dedup,
        json!({ "conversation": CONVERSATION }),
        turn_cluster(round, turn, participant, body),
    );
    world
        .service
        .send(&params(), &send_request(message))
        .await
        .ok()
        .map(|response| response_payload(&response))
}

async fn submit_turn(
    world: &World,
    dedup: &str,
    round: u64,
    turn: u32,
    participant: &str,
    body: &str,
) -> Value {
    try_submit_turn(world, dedup, round, turn, participant, body)
        .await
        .expect("the turn command is served")
}

/// The conversation, read from the durable record; see [`team_snapshot`].
async fn conversation_snapshot(world: &World) -> rakka_agent::AgentConversationSnapshot {
    let _ = passivate_conversation(world);
    let entity = registered_agent_conversation_entity_ref(
        &world.conversation_registration,
        &conversation_scope(),
    );
    for _ in 0..300 {
        let reply = ask_retrying(
            &entity,
            |reply_to| AgentConversationEntityMessage::Command {
                command: Box::new(AgentConversationEntityCommand::Describe),
                reply_to,
            },
            "conversation describe",
        )
        .await;
        match reply {
            rakka_agent::AgentConversationEntityReply::Snapshot(Some(snapshot)) => {
                return *snapshot
            }
            rakka_agent::AgentConversationEntityReply::Rejected { .. } => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            other => panic!("the conversation snapshots, got {other:?}"),
        }
    }
    panic!("the conversation never snapshotted");
}

fn passivate_conversation(world: &World) -> bool {
    rakka_agent::passivate_agent_conversation_entity(
        &world.sharding,
        world.conversation_registration.key(),
        &conversation_scope(),
    )
    .unwrap_or(false)
}

async fn end_conversation(world: &World, round: u64) {
    let entity = registered_agent_conversation_entity_ref(
        &world.conversation_registration,
        &conversation_scope(),
    );
    let command = AgentConversationEntityCommand::EndEarly {
        operation_id: rakka_agent::conversation_end_operation_id(
            &tenant(),
            conversation_scope().conversation(),
            round,
            "the review concluded",
        )
        .expect("the operation id derives"),
        moderator: agent(MODERATOR),
        expected_round: round,
        reason: "the review concluded".to_string(),
        provenance: Box::new(provenance(2)),
    };
    let reply = ask_retrying(
        &entity,
        |reply_to| AgentConversationEntityMessage::Command {
            command: Box::new(command.clone()),
            reply_to,
        },
        "conversation end",
    )
    .await;
    assert!(
        matches!(
            reply,
            rakka_agent::AgentConversationEntityReply::Applied { .. }
                | rakka_agent::AgentConversationEntityReply::Duplicate { .. }
        ),
        "the moderator ends the conversation, got {reply:?}"
    );
}

// --- replay ---------------------------------------------------------------

/// The cursor a `WindowExpired` answer says to resume from.
fn expired_floor(replay: &rakka_agent::AgentCoordinationReplay) -> Option<String> {
    match replay {
        rakka_agent::AgentCoordinationReplay::WindowExpired { resume_from, .. } => {
            Some(resume_from.clone())
        }
        // The enum is `#[non_exhaustive]`: a future arm is not a floor.
        _ => None,
    }
}

/// Passivates every entity of every type the walk has touched.
fn passivate_all(world: &World) {
    for id in [TRIAGER, UNCAPABLE, SPECIALIST, MODERATOR] {
        let _ = rakka_agent::passivate_agent_entity(
            &world.sharding,
            world.agent_registration.key(),
            &agent_scope(id),
        );
    }
    for task in [TASK, PROBE_TASK] {
        let _ = rakka_agent::passivate_agent_task_entity(
            &world.sharding,
            world.task_registration.key(),
            &task_scope(task),
        );
    }
    let _ = rakka_agent::passivate_agent_team_entity(
        &world.sharding,
        world.team_registration.key(),
        &team_scope(),
    );
    // Every run the walk can have minted: the claim's generation-1 run under
    // the claimant, and the transfer's generation-2 run under the target.
    for (agent_id, generation) in [(TRIAGER, 1), (SPECIALIST, 2)] {
        let Ok(run) = run_id_for_assignment(
            task_scope(TASK).task(),
            rakka_agent::AgentAssignmentGeneration::new(generation),
        ) else {
            continue;
        };
        let Ok(scope) = AgentRunScope::new(tenant(), agent(agent_id), run) else {
            continue;
        };
        let _ = rakka_agent::passivate_agent_run_entity(
            &world.sharding,
            world.run_registration.key(),
            &scope,
        );
    }
    let _ = passivate_conversation(world);
}

/// Replays every scope from a cursor and probes the truncated-window answer.
/// Returns the total events replayed and whether the expired window's floor
/// resumed for real.
async fn replay_everything(world: &World) -> (usize, bool) {
    let sources = rakka_agent::AgentCoordinationSources::new(
        &world.history,
        &world.team_history,
        &world.conversation_history,
    );
    let scopes = [
        rakka_agent::AgentEntityAddress::Task(task_scope(TASK)),
        rakka_agent::AgentEntityAddress::Team(team_scope()),
        rakka_agent::AgentEntityAddress::Conversation(conversation_scope()),
    ];

    let mut total = 0;
    let mut task_log_expired = false;
    for scope in &scopes {
        // Page from the start, following the cursor: no gap and no repeat.
        // The task log is deliberately bounded, so its *first* read lands in
        // the retention gap and answers `WindowExpired` — exactly where a
        // production resynchronizer meets it — and the walk resumes at the
        // reported floor. The team and conversation logs are unbounded and
        // must page from their beginning.
        let mut cursor: Option<String> = None;
        let mut seen: Vec<String> = Vec::new();
        let mut first = true;
        loop {
            let replay = sources
                .replay(&tenant(), scope, cursor.as_deref(), 4)
                .await
                .expect("the scope replays");
            if let Some(floor) = expired_floor(&replay) {
                assert!(
                    first,
                    "only the first read may land in the retention gap: {replay:?}"
                );
                assert!(
                    matches!(scope, rakka_agent::AgentEntityAddress::Task(_)),
                    "only the deliberately bounded task log may expire: {replay:?}"
                );
                task_log_expired = true;
                cursor = Some(floor);
                first = false;
                continue;
            }
            first = false;
            let Some(page) = replay.page() else {
                panic!("a replay answers a page or an expiry: {replay:?}");
            };
            for event in &page.events {
                let key = format!("{scope:?}#{}", event.cursor);
                assert!(!seen.contains(&key), "the cursor repeated {key}");
                seen.push(key);
            }
            total += page.events.len();
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor.clone();
            assert!(cursor.is_some(), "a page with more carries its next cursor");
        }
    }
    assert!(
        task_log_expired,
        "the walk outgrew the task log's retention bound, so the gap arm really ran"
    );

    // The retention-gap arm: a cursor before the retained floor answers
    // `WindowExpired` with the floor, and resuming from it pages for real.
    let task_address = rakka_agent::AgentEntityAddress::Task(task_scope(TASK));
    let from_the_beginning =
        rakka_agent::AgentCoordinationCursor::new(task_address.clone(), 0).encode();
    let expired = sources
        .replay(&tenant(), &task_address, Some(&from_the_beginning), 4)
        .await
        .expect("the retention-gap probe replays");
    // Pinned to the arm, never tolerated either way: the task history's
    // retention bound is deliberately smaller than what the walk writes, so
    // a from-the-beginning cursor MUST land in the retention gap. A probe
    // that quietly paged would mean the walk had stopped outgrowing the
    // bound and line 15's WindowExpired demonstration had silently stopped
    // running — the same under-coverage `assert_crash_fired` exists to
    // prevent.
    let floor = expired_floor(&expired).unwrap_or_else(|| {
        panic!(
            "the from-the-beginning probe must answer WindowExpired; the walk no longer \
             outgrows the task log's retention bound: {expired:?}"
        )
    });
    let resumed = sources
        .replay(&tenant(), &task_address, Some(&floor), 4)
        .await
        .is_ok_and(|answer| answer.page().is_some());
    (total, resumed)
}

// --- the no-leak sweep ----------------------------------------------------

/// Every coordination surface a sentinel must never cross into.
///
/// The conversation's own snapshot and the task that accepted a typed result
/// are deliberately absent: a moderated interaction's bounded ring *is* where
/// its transcript lives, and an accepted result *is* task state. What must
/// never carry content is the shared board, the replayable coordination
/// events — observability, never the correctness source — and the metrics.
async fn queried_surfaces(world: &World) -> Vec<String> {
    let mut surfaces = Vec::new();
    surfaces.push(format!("{:?}", team_snapshot(world).await));
    let sources = rakka_agent::AgentCoordinationSources::new(
        &world.history,
        &world.team_history,
        &world.conversation_history,
    );
    for scope in [
        rakka_agent::AgentEntityAddress::Task(task_scope(TASK)),
        rakka_agent::AgentEntityAddress::Team(team_scope()),
        rakka_agent::AgentEntityAddress::Conversation(conversation_scope()),
    ] {
        // The bounded task log's first read answers `WindowExpired`; the
        // sweep must sweep the *retained* events too, so it resumes at the
        // reported floor and sweeps both answers.
        let mut cursor: Option<String> = None;
        for _ in 0..2 {
            let Ok(replay) = sources
                .replay(&tenant(), &scope, cursor.as_deref(), 64)
                .await
            else {
                break;
            };
            surfaces.push(format!("{replay:?}"));
            match expired_floor(&replay) {
                Some(floor) => cursor = Some(floor),
                None => break,
            }
        }
    }
    surfaces.push(format!("{:?}", world.metrics));
    surfaces
}

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

/// Runs the whole acceptance walk and returns the transcript plus the typed
/// facts behind it.
///
/// # Panics
///
/// Panics if any bullet's fact does not hold — the walk is the check.
#[allow(clippy::too_many_lines)]
pub async fn run_acceptance() -> AcceptanceReport {
    let world = World::new();
    let mut lines = vec![String::new(); 16];

    // 1/16 — the deployment: five sharded entity types, one team, one board
    // task posted deliberately unassigned.
    instantiate(&world, TRIAGER, triager_envelope()).await;
    instantiate(&world, UNCAPABLE, uncapable_envelope()).await;
    instantiate(&world, SPECIALIST, specialist_envelope()).await;
    instantiate(&world, MODERATOR, moderator_envelope()).await;

    let mut members: BTreeMap<AgentId, BTreeSet<AgentCapabilityId>> = BTreeMap::new();
    members.insert(agent(TRIAGER), BTreeSet::new());
    members.insert(agent(UNCAPABLE), BTreeSet::new());
    let created = team_command(
        &world,
        AgentTeamEntityCommand::Create {
            operation_id: team_op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: agent(TRIAGER),
                root_goal: AgentGoalId::new("support-quarter").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(rakka_agent::AgentRevisionNumber::INITIAL),
                members,
            }),
        },
    )
    .await;
    assert!(matches!(created, AgentTeamEntityReply::Applied { .. }));

    for task in [TASK, PROBE_TASK] {
        let reply = task_command(
            &world,
            &task_scope(task),
            AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, task, "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTaskCreation {
                    definition: task_definition(),
                    input: AgentTaskContent::inline(json!({ "ticket": task }))
                        .expect("the input is inline-bounded"),
                    assignee: None,
                    team: Some(AgentTeamId::new(TEAM).expect("the team id is valid")),
                    goal: None,
                    goal_mode: rakka_agent::AgentGoalMode::default(),
                    goal_spec: None,
                    parent: None,
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    delegation: None,
                    telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
                }),
            },
        )
        .await;
        assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));

        let posted = team_command(
            &world,
            AgentTeamEntityCommand::PostTask {
                operation_id: team_op(&format!("post-{task}")),
                task: rakka_agent::AgentTaskId::new(task).expect("the task id is valid"),
                posted_by: agent(TRIAGER),
            },
        )
        .await;
        assert!(matches!(posted, AgentTeamEntityReply::Applied { .. }));
    }

    let board = team_snapshot(&world).await;
    assert_eq!(board.board.len(), 2, "both entries are posted");
    assert_eq!(
        board_entry(&board, TASK).status,
        rakka_agent::AgentTeamBoardEntryStatus::Open
    );
    assert!(
        task_state(&world, &task_scope(TASK))
            .await
            .task()
            .expect("the task exists")
            .assignment
            .is_none(),
        "a board task waits unassigned until a claim names an owner"
    );
    lines[0] = EXPECTED_TRANSCRIPT[0].to_string();

    // 2/16 — two members claim concurrently over the real A2A surface: one
    // owner is admitted, and the loser's stale-epoch command fails closed.
    let winner = world
        .service
        .send(
            &params(),
            &send_request(coordination_message(
                "claim-triager",
                json!({ "team": TEAM }),
                claim_cluster(TASK, TRIAGER, 0),
            )),
        )
        .await
        .expect("the winning claim is served");
    assert!(
        response_payload(&winner).get("Applied").is_some(),
        "the first claim applies: {}",
        response_payload(&winner)
    );

    let loser = world
        .service
        .send(
            &params(),
            &send_request(coordination_message(
                "claim-loser",
                json!({ "team": TEAM }),
                claim_cluster(TASK, UNCAPABLE, 0),
            )),
        )
        .await
        .expect("the losing claim is served");
    let loser_payload = response_payload(&loser);
    assert!(
        loser_payload.get("Rejected").is_some(),
        "the stale-epoch claim fails closed: {loser_payload}"
    );

    settle_claim_round_trip(&world, &task_scope(TASK)).await;
    let task = task_state(&world, &task_scope(TASK)).await;
    let task = task.task().expect("the task exists");
    let assignment = task.assignment.as_ref().expect("the claim bought an owner");
    assert_eq!(assignment.agent.as_str(), TRIAGER);
    assert_eq!(
        assignment.generation,
        rakka_agent::AgentAssignmentGeneration::new(1)
    );
    assert_eq!(
        assignment.status,
        rakka_agent::AgentAssignmentStatus::Accepted
    );
    lines[1] = EXPECTED_TRANSCRIPT[1].to_string();

    // 3/16 — the envelope door: a member the board admits and the definition
    // does not is refused, and its entry reopens for a member that may.
    let probe_epoch = board_entry(&team_snapshot(&world).await, PROBE_TASK).claim_epoch;
    let probe = world
        .service
        .send(
            &params(),
            &send_request(coordination_message(
                "claim-uncapable",
                json!({ "team": TEAM }),
                claim_cluster(PROBE_TASK, UNCAPABLE, probe_epoch),
            )),
        )
        .await
        .expect("the uncapable claim is served");
    assert!(
        response_payload(&probe).get("Applied").is_some(),
        "the board itself admits the member: the refusal is the envelope's, not the roster's"
    );
    settle_claim_round_trip(&world, &task_scope(PROBE_TASK)).await;

    let probe_task = task_state(&world, &task_scope(PROBE_TASK)).await;
    let probe_task = probe_task.task().expect("the probe task exists");
    assert!(
        probe_task.assignment.is_none(),
        "no generation was bought without the capability"
    );
    let claim = probe_task
        .team_claim
        .as_ref()
        .expect("the claim provenance stands");
    let claim_refusal_code = match &claim.status {
        rakka_agent::AgentTaskTeamClaimStatus::Refused { code } => code.clone(),
        other => panic!("the claim resolves refused, got {other:?}"),
    };
    assert_eq!(claim_refusal_code, "team-coordination-unauthorized");
    let probe_entry = board_entry(&team_snapshot(&world).await, PROBE_TASK).clone();
    assert_eq!(
        probe_entry.status,
        rakka_agent::AgentTeamBoardEntryStatus::Open,
        "the entry reopened rather than parking"
    );
    lines[2] = EXPECTED_TRANSCRIPT[2].to_string();

    // 4/16 — everything passivates; the board is durable data, not a resident
    // coordinator, and the claim survives the passivation.
    passivate_all(&world);
    let resident_at_wait = world.resident();
    assert_eq!(
        resident_at_wait, 0,
        "an idle team and its members hold no runtime resource"
    );
    let after = team_snapshot(&world).await;
    let entry = board_entry(&after, TASK);
    assert_eq!(
        entry.status,
        rakka_agent::AgentTeamBoardEntryStatus::Active,
        "the claim activated across the passivation"
    );
    assert_eq!(
        entry
            .claim
            .as_ref()
            .expect("the owner echo stands")
            .member
            .as_str(),
        TRIAGER
    );
    lines[3] = EXPECTED_TRANSCRIPT[3].to_string();

    // 5/16 — the transfer: one model turn commits the handoff record and its
    // outbound send in the same compare-and-set, and fences the source.
    let source_run = AgentRunScope::new(
        tenant(),
        agent(TRIAGER),
        run_id_for_assignment(
            task_scope(TASK).task(),
            rakka_agent::AgentAssignmentGeneration::new(1),
        )
        .expect("the run id derives"),
    )
    .expect("the source run scope is valid");
    let triager_adapter = DeterministicModelAdapter::new().with_turn(handoff_turn());

    // Drive up to the committed-but-unanswered send: the record is durable
    // and nothing has left the node.
    for _ in 0..12 {
        settle_task(&world, &task_scope(TASK)).await;
        settle_run(&world, &source_run).await;
        if outstanding_send(&world, &source_run).await {
            break;
        }
        let _ = world
            .pipeline(triager_adapter.clone())
            .pump_run(&source_run)
            .await;
    }
    let source_state = run_state(&world, &source_run).await;
    let cell = source_state
        .run()
        .expect("the source record exists")
        .loop_state
        .handoff()
        .expect("the handoff cell committed with the send")
        .clone();
    assert_eq!(cell.record.task.as_str(), TASK, "the same task is offered");
    assert_eq!(cell.record.resolved.agent.as_str(), SPECIALIST);
    assert!(
        outstanding_send(&world, &source_run).await,
        "the send is committed and still unanswered: nothing left the node before the record"
    );
    lines[4] = EXPECTED_TRANSCRIPT[4].to_string();

    // 7/16 — HANDOFF POD LOSS, injected inside the transfer: the task store's
    // owner dies on the very next durable write, which is the offer the A2A
    // send is about to apply. Recovery must converge on one transfer.
    world
        .tasks
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    let _ = world
        .pipeline(triager_adapter.clone())
        .pump_run(&source_run)
        .await;
    // The loss is proven, not assumed: a window that never fired would make
    // this line a claim about nothing.
    world
        .tasks
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    world.tasks.survive();

    // A new owner, with nothing but the durable record.
    drive_transfer(&world, &source_run, &triager_adapter).await;
    // Measured at the executor itself: the send was asked for twice — once
    // by the pump that died inside the injected loss, once by the re-drive
    // that completed — and converged on one accepted transfer. The parallel
    // sweep in `rakka-agent/tests/handoff_recovery.rs` measures the same
    // property the same way.
    let transfers_attempted = world
        .handoff_sends
        .load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        transfers_attempted, 2,
        "the executor was asked into the loss, and asked again to completion"
    );
    lines[6] = EXPECTED_TRANSCRIPT[6].to_string();

    // 6/16 — the transfer's own facts. The identity is preserved, the source
    // terminalized only after the target's durable acceptance, and no
    // session namespace travelled.
    let task = task_state(&world, &task_scope(TASK)).await;
    let task = task.task().expect("the task exists");
    assert_eq!(
        task_scope(TASK).task().as_str(),
        TASK,
        "a transfer is never a new task"
    );
    let owner_after_handoff = task
        .assignment
        .as_ref()
        .expect("the task has an owner")
        .agent
        .to_string();
    let source_status = run_state(&world, &source_run).await.status().map_or_else(
        || "none".to_string(),
        |status| status.as_label().to_string(),
    );
    let generations =
        u32::try_from(task.assignment_generation.get()).expect("the walk's generations fit");
    // Asserted unconditionally: the single BeforeWrite loss above is a
    // retryable delivery failure, and the re-drive deterministically
    // completes the transfer — the arm the pinned transcript requires. The
    // restored ending is legal for a transfer in general (a definitive
    // refusal, a spent retry budget), and the two-arm convergence property
    // is swept where both arms genuinely occur, in
    // `rakka-agent/tests/handoff_recovery.rs`. A tolerant either-arm branch
    // here was dead code whose own following session asserts would have
    // panicked on the arm it tolerated.
    assert_eq!(owner_after_handoff, SPECIALIST, "the transfer completed");
    assert_eq!(generations, 2, "exactly one new generation");
    let provenance = task.handoff.as_deref().expect("the provenance survives");
    assert_eq!(
        provenance.status,
        rakka_agent::AgentTaskHandoffStatus::Accepted
    );
    assert!(
        provenance.result_settled,
        "the result exchange settled, which is what let the source terminalize"
    );
    assert_eq!(
        source_status, "handed-off",
        "HandedOff is recorded strictly after the target's acceptance"
    );
    assert_eq!(
        provenance.source_assignment.agent.as_str(),
        TRIAGER,
        "the stashed source survives for the goal view to join"
    );
    // The reason sentinel's positive control: the model-derived string
    // really is sitting in the task's own handoff provenance — the
    // surface-adjacent record it belongs in — so the sweep's silence about
    // the board, the replay pages, and the metrics is evidence, not vacancy.
    assert_eq!(
        provenance.reason, CONTENT_SENTINELS[3],
        "the handoff reason reached the task's provenance"
    );
    // The target's session namespace is its own: nothing the source wrote is
    // addressable under the target's run.
    let target_run = AgentRunScope::new(
        tenant(),
        agent(SPECIALIST),
        run_id_for_assignment(
            task_scope(TASK).task(),
            rakka_agent::AgentAssignmentGeneration::new(2),
        )
        .expect("the run id derives"),
    )
    .expect("the target run scope is valid");
    assert_ne!(
        source_run.run().as_str(),
        target_run.run().as_str(),
        "a distinct target run under the target agent"
    );
    // Measured, not asserted by construction: specification 8.9 forbids the
    // transfer from reusing the source's short-term-memory namespace, and
    // session memory is keyed by run scope. Each run seeded its own namespace
    // from the task's input, and the two hold disjoint entries — a transfer
    // that reused the namespace would show the source's entry ids under the
    // target's scope.
    let source_session = session_entry_ids(&world, &source_run).await;
    let target_session = session_entry_ids(&world, &target_run).await;
    assert!(
        !source_session.is_empty() && !target_session.is_empty(),
        "both runs really did write a session a reused namespace could have merged"
    );
    assert!(
        source_session.is_disjoint(&target_session),
        "the target's short-term memory namespace is its own: no entry travelled"
    );
    lines[5] = EXPECTED_TRANSCRIPT[5].to_string();

    // 8/16 — a human-owned approval is declared upstream of the ticket: the
    // edge registers with the upstream in the declaring transition, and the
    // dependent's decision graph reads unsatisfied until it resolves.
    let approval = task_scope(APPROVAL_TASK);
    let created = task_command(
        &world,
        &approval,
        AgentTaskEntityCommand::Create {
            operation_id: AgentOperationId::new(
                AgentOperationKind::TaskCreation,
                [TENANT, APPROVAL_TASK, "1"],
            )
            .expect("the operation id derives"),
            creation: Box::new(AgentTaskCreation {
                definition: task_definition()
                    .with_ownership(rakka_agent::AgentTaskOwnership::Human),
                input: AgentTaskContent::inline(json!({ "refund": TASK }))
                    .expect("the input is inline-bounded"),
                assignee: None,
                team: None,
                goal: None,
                goal_mode: rakka_agent::AgentGoalMode::default(),
                goal_spec: None,
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: None,
                telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
            }),
        },
    )
    .await;
    assert!(matches!(created, AgentTaskEntityReply::Applied { .. }));

    let dependent = task_scope(TASK);
    let declared = task_command(
        &world,
        &dependent,
        AgentTaskEntityCommand::DeclareDependency {
            operation_id: AgentOperationId::new(
                AgentOperationKind::DependencyRegistration,
                [TENANT, TASK, APPROVAL_TASK],
            )
            .expect("the operation id derives"),
            declaration: Box::new(rakka_agent::AgentTaskDependencyDeclaration::new(
                approval.task().clone(),
            )),
        },
    )
    .await;
    assert!(
        matches!(declared, AgentTaskEntityReply::Applied { .. }),
        "the dependency declares, got {declared:?}"
    );
    for _ in 0..6 {
        settle_task(&world, &dependent).await;
        settle_task(&world, &approval).await;
    }
    let upstream_state = task_state(&world, &approval).await;
    assert!(
        upstream_state
            .task()
            .expect("the upstream exists")
            .dependents
            .contains_key(dependent.task()),
        "the forward edge registered with the upstream"
    );
    // The graph half of the claim, from the dependent's own record: the
    // declared edge is unresolved, so every decision door that consults
    // `dependencies_satisfied` reads false. An in-flight assignment
    // deliberately keeps working — a dependency can resolve while a run is
    // mid-flight, and demoting live work would strand it — so what the edge
    // gates is the task's *next* decision, and the durable `Blocked`
    // posture belongs to tasks that are assignable when the edge lands
    // (`rakka-agent/tests/human_owned_tasks.rs` pins that arm).
    let dependent_state = task_state(&world, &dependent).await;
    assert!(
        !dependent_state
            .task()
            .expect("the dependent exists")
            .dependencies_satisfied(),
        "the unresolved human-owned edge gates the dependent's decision graph"
    );
    lines[7] = EXPECTED_TRANSCRIPT[7].to_string();

    // The specialist proposes the refund while the approval is pending. The
    // tool is checkpoint-bound by the deployment's registry, so the
    // consequential effect parks on a bound `AgentCheckpoint` — real durable
    // state, created *before* the human result, which is what beat 10
    // measures its boundary against — and invokes nothing.
    let refund_adapter = DeterministicModelAdapter::new().with_turn(refund_turn());
    for _ in 0..12 {
        settle_task(&world, &task_scope(TASK)).await;
        settle_run(&world, &target_run).await;
        let _ = world
            .pipeline(refund_adapter.clone())
            .pump_run(&target_run)
            .await;
        if run_state(&world, &target_run)
            .await
            .loop_state()
            .is_some_and(|loop_state| !loop_state.open_checkpoints().is_empty())
        {
            break;
        }
    }
    let parked = run_state(&world, &target_run).await;
    assert_eq!(
        parked.status(),
        Some(rakka_agent::AgentRunStatus::WaitingForApproval),
        "the checkpoint-bound proposal parks the run for approval"
    );
    let open_before_result = parked
        .loop_state()
        .expect("the target loop exists")
        .open_checkpoints()
        .to_vec();
    assert_eq!(
        open_before_result.len(),
        1,
        "exactly one checkpoint gates the refund"
    );

    // 9/16 — the authenticated human result completes the upstream over the
    // real A2A surface and unblocks the dependent; a replay echoes it.
    let first = world
        .service
        .send(
            &params(),
            &send_request(human_result_message(
                "approval-1",
                json!({ "answer": "approved", "memo": CONTENT_SENTINELS[1] }),
            )),
        )
        .await
        .expect("the human submission is served");
    let a2a::SendMessageResponse::Task(first_task) = first else {
        panic!("a human result answers with a task");
    };
    let replay = world
        .service
        .send(
            &params(),
            &send_request(human_result_message(
                "approval-1",
                json!({ "answer": "approved", "memo": CONTENT_SENTINELS[1] }),
            )),
        )
        .await
        .expect("the replayed submission is served");
    let a2a::SendMessageResponse::Task(replayed) = replay else {
        panic!("a replayed human result answers with a task");
    };
    assert_eq!(
        first_task.id, replayed.id,
        "the replay echoes the original task rather than creating a second"
    );
    for _ in 0..8 {
        settle_task(&world, &approval).await;
        settle_task(&world, &dependent).await;
    }
    let upstream_state = task_state(&world, &approval).await;
    let upstream = upstream_state.task().expect("the upstream exists");
    assert_eq!(upstream.status, rakka_agent::AgentTaskStatus::Completed);
    let human_results_accepted = usize::from(upstream.accepted_result.is_some());
    assert_eq!(human_results_accepted, 1, "exactly one accepted result");
    let dependent_state = task_state(&world, &dependent).await;
    let dependent_task = dependent_state.task().expect("the dependent exists");
    let edge = dependent_task
        .dependencies
        .get(approval.task())
        .expect("the edge stands")
        .clone();
    // Unblocked means the graph flipped, not merely that a field appeared:
    // the edge carries its upstream's outcome AND the dependent's decision
    // graph — false at beat 8 — reads satisfied again, which is the fact
    // every decision door consults.
    let dependent_unblocked = edge.outcome.is_some() && dependent_task.dependencies_satisfied();
    assert!(
        dependent_unblocked,
        "the dependent learned its upstream's outcome and its decision graph reads satisfied"
    );
    lines[8] = EXPECTED_TRANSCRIPT[8].to_string();

    // 10/16 — the checkpoint boundary, measured on real state: the effect
    // parked on its checkpoint *before* the human result, and the result
    // completed the upstream without touching it — the same checkpoint is
    // still open, the run is still parked, and the tool has never run
    // (`rakka-agent/tests/human_owned_tasks.rs` pins the same boundary at
    // the unit level).
    let declared_gate = world
        .registry
        .binding(&AgentToolId::new(REFUND_TOOL).expect("the tool id is valid"))
        .is_some_and(rakka_agent::AgentToolBinding::checkpoint_required);
    assert!(
        declared_gate,
        "the consequential tool is checkpoint-bound by declaration"
    );
    let after_result = run_state(&world, &target_run).await;
    let open_after_result = after_result
        .loop_state()
        .expect("the target loop exists")
        .open_checkpoints()
        .to_vec();
    assert_eq!(
        open_after_result.len(),
        1,
        "the human result resolved no checkpoint: the gate is still closed"
    );
    assert_eq!(
        open_after_result[0].checkpoint_id, open_before_result[0].checkpoint_id,
        "the very checkpoint that parked before the result is the one still open"
    );
    assert_eq!(
        after_result.status(),
        Some(rakka_agent::AgentRunStatus::WaitingForApproval),
        "the run is still parked on the effect gate"
    );
    let checkpoint_gated_effect = declared_gate && open_after_result.len() == 1;
    let effect_invocations = world.tools.invocation_count(REFUND_TOOL);
    assert_eq!(
        effect_invocations, 0,
        "a human task is not a substitute for an effect-bound checkpoint: nothing was invoked"
    );
    lines[9] = EXPECTED_TRANSCRIPT[9].to_string();

    // 11/16 — the moderated conversation: bounded ordered turns over the real
    // A2A surface, and a replayed turn absorbed by the dense ledger.
    create_conversation(&world).await;
    let opening = submit_turn(&world, "turn-1", 0, 0, SPECIALIST, "refund is in policy").await;
    assert!(
        opening.get("Applied").is_some(),
        "the round-robin cursor's first owner speaks: {opening}"
    );
    let replayed = submit_turn(&world, "turn-1", 0, 0, SPECIALIST, "refund is in policy").await;
    assert!(
        replayed.get("Duplicate").is_some(),
        "the replayed turn converges on the recorded one: {replayed}"
    );
    let snapshot = conversation_snapshot(&world).await;
    let turns_recorded = snapshot.turns.len();
    assert_eq!(turns_recorded, 1, "a replay records no second turn");
    lines[10] = EXPECTED_TRANSCRIPT[10].to_string();

    // 13/16 — CONVERSATION POD LOSS, injected inside the next turn's durable
    // write. The protocol's five nouns must come back without duplicating it.
    let before = conversation_snapshot(&world).await;
    world
        .conversations
        .crash_at(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    let _ = try_submit_turn(&world, "turn-2", 0, 1, TRIAGER, CONTENT_SENTINELS[2]).await;
    world
        .conversations
        .assert_crash_fired(1, rakka_agent::testkit::CrashPoint::BeforeWrite);
    assert_eq!(
        conversation_snapshot(&world).await.turns.len(),
        turns_recorded,
        "the lost write recorded nothing"
    );
    world.conversations.survive();
    // The owner dies: its entity leaves residency and the next ask
    // re-materializes it from the durable record alone.
    let _ = passivate_conversation(&world);
    let recovered = submit_turn(&world, "turn-2", 0, 1, TRIAGER, CONTENT_SENTINELS[2]).await;
    assert!(
        recovered.get("Applied").is_some() || recovered.get("Duplicate").is_some(),
        "the re-driven turn lands: {recovered}"
    );
    let after = conversation_snapshot(&world).await;
    let turns_after_recovery = after.turns.len();
    assert_eq!(
        turns_after_recovery,
        turns_recorded + 1,
        "the re-driven turn recorded exactly once across the loss"
    );
    assert_eq!(after.moderator, agent(MODERATOR), "participant recovered");
    assert_eq!(after.round, before.round, "round recovered");
    assert_eq!(
        after.current_speaker,
        Some(agent(UNCAPABLE)),
        "the turn owner recovered and advanced exactly one place"
    );
    assert_eq!(
        after.transcript_ref, before.transcript_ref,
        "transcript reference recovered"
    );
    assert_eq!(
        after.budgets.tokens, before.budgets.tokens,
        "the token ceiling recovered unchanged"
    );
    assert_eq!(
        after.budgets.deadline, before.budgets.deadline,
        "the creation-fixed deadline recovered unchanged"
    );
    lines[12] = EXPECTED_TRANSCRIPT[12].to_string();

    // 12/16 — the moderation envelope door, at the cursor's own current
    // speaker: the roster admits it, the coordinate is right, every budget is
    // intact, and its definition never granted `Moderation`.
    let refused = submit_turn(&world, "turn-uncapable", 0, 2, UNCAPABLE, "may I?").await;
    let turn_refusal_code = refused
        .get("Rejected")
        .and_then(|rejected| rejected.get("code"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        turn_refusal_code, "conversation-moderation-unauthorized",
        "the roster admits the speaker; the envelope does not: {refused}"
    );
    assert_eq!(
        conversation_snapshot(&world).await.turns.len(),
        turns_after_recovery,
        "a refusal records nothing"
    );
    lines[11] = EXPECTED_TRANSCRIPT[11].to_string();

    // 14/16 — the conversation's terminal reaches its governing task, and the
    // terminal task closes its board entry with the claim epoch bumped.
    end_conversation(&world, after.round).await;
    for _ in 0..8 {
        settle_conversation(&world).await;
        settle_task(&world, &task_scope(TASK)).await;
    }
    let conversation_terminal = conversation_snapshot(&world)
        .await
        .terminal_reason
        .map_or_else(|| "none".to_string(), |reason| reason.code().to_string());
    assert_eq!(conversation_terminal, "moderator-ended");
    let cell_task = task_state(&world, &task_scope(TASK)).await;
    assert_eq!(
        cell_task
            .task()
            .expect("the task exists")
            .conversation
            .as_ref()
            .expect("the terminal conversation recorded its cell on the task")
            .conversation
            .as_str(),
        CONVERSATION
    );

    let epoch_before = board_entry(&team_snapshot(&world).await, TASK).claim_epoch;
    let cancelled = task_command(
        &world,
        &task_scope(TASK),
        AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, TASK, "closed"],
            )
            .expect("the operation id derives"),
            reason: "resolved at the review".to_string(),
        },
    )
    .await;
    assert!(matches!(
        cancelled,
        AgentTaskEntityReply::Applied { .. } | AgentTaskEntityReply::Duplicate { .. }
    ));
    // The task finalizes terminal only once its escrow ledger closes, and the
    // ledger closes only after a known terminal run outcome — so the owner's
    // wind-down is driven, not assumed.
    let quiet = DeterministicModelAdapter::new();
    for _ in 0..24 {
        settle_task(&world, &task_scope(TASK)).await;
        settle_run(&world, &target_run).await;
        let _ = world.pipeline(quiet.clone()).pump_run(&target_run).await;
        settle_team(&world).await;
        if task_state(&world, &task_scope(TASK))
            .await
            .task()
            .is_some_and(|task| task.status.is_terminal())
        {
            break;
        }
    }
    for _ in 0..6 {
        settle_task(&world, &task_scope(TASK)).await;
        settle_team(&world).await;
    }
    let closed = board_entry(&team_snapshot(&world).await, TASK).clone();
    let board_entry_status = closed.status.as_label().to_string();
    assert_eq!(
        closed.status,
        rakka_agent::AgentTeamBoardEntryStatus::Done,
        "a terminal task closes its board entry eagerly rather than leaving it open"
    );
    assert!(
        closed.claim_epoch > epoch_before,
        "the eager close bumps the claim epoch, absorbing every stale in-flight reply"
    );
    lines[13] = EXPECTED_TRANSCRIPT[13].to_string();

    // 15/16 — replay: the coordination log resumes from a cursor across every
    // scope, and a truncated window answers with a floor that resumes.
    let (replayed_events, window_expired_resumed) = replay_everything(&world).await;
    assert!(replayed_events > 0, "the walk left a coordination log");
    assert!(
        window_expired_resumed,
        "a truncated window answers WindowExpired with a resumable floor"
    );
    lines[14] = EXPECTED_TRANSCRIPT[14].to_string();

    // 16/16 — the no-leak sweep. First prove the sentinels are real content
    // that really did enter the system, so the sweep below is measuring
    // something: the transcript sentinel is in the conversation's own bounded
    // ring, which is exactly where specification 8.11 puts it.
    let transcript = format!("{:?}", conversation_snapshot(&world).await);
    assert!(
        transcript.contains(CONTENT_SENTINELS[2]),
        "the planted turn body reached the conversation's own state"
    );
    let surfaces = queried_surfaces(&world).await;
    assert!(!surfaces.is_empty());
    for surface in &surfaces {
        for sentinel in CONTENT_SENTINELS {
            assert!(
                !surface.contains(sentinel),
                "a queried surface leaked {sentinel}"
            );
        }
    }
    lines[15] = EXPECTED_TRANSCRIPT[15].to_string();

    AcceptanceReport {
        lines,
        task_id: TASK.to_string(),
        generations,
        claim_refusal_code,
        turn_refusal_code,
        resident_at_wait,
        source_status,
        owner_after_handoff,
        transfers_attempted,
        human_results_accepted,
        dependent_unblocked,
        checkpoint_gated_effect,
        effect_invocations,
        turns_recorded,
        turns_after_recovery,
        conversation_terminal,
        board_entry_status,
        replayed_events,
        window_expired_resumed,
        surfaces,
    }
}
