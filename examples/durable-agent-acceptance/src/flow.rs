//! The acceptance walk: every bullet of the spec 22 initial statement, in
//! order, over the wired world — and, below it, the drive the gated
//! live-provider walk of `crate::provider` reuses: the same agent, task
//! definition, tool, and envelope, driven to a terminal run and swept for the
//! provider key.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use rakka_a2a::agents::A2AAgentTarget;
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter};
use rakka_agent::AgentRevisionNumber;
use rakka_agent::{
    agent_operational_snapshot, assemble_agent_session_view, load_agent_run_state,
    load_agent_task_state, passivate_agent_entity, passivate_agent_run_entity,
    passivate_agent_task_entity, registered_agent_entity_ref, registered_agent_run_entity_ref,
    registered_agent_task_entity_ref, run_id_for_assignment, AgentAdmissionEvaluator,
    AgentAdmissionRequirement, AgentApprovalDecision, AgentAssignmentGeneration,
    AgentAssignmentRefusalReason, AgentAuthorityEnvelope, AgentBudgetCeilings,
    AgentCancellationProgress, AgentCheckpointDecision, AgentDefinition, AgentDefinitionId,
    AgentDispatchWindow, AgentEffectResolution, AgentEntityCommand, AgentEntityMessage,
    AgentEntityReply, AgentId, AgentModelTurn, AgentOperationClass, AgentOperationId,
    AgentOperationKind, AgentPolicyRef, AgentPolicyRefs, AgentReconciliationDecision,
    AgentRevisionProvenance, AgentRunEffectOutcome, AgentRunEntityCommand, AgentRunEntityMessage,
    AgentRunEntityRef, AgentRunEntityReply, AgentRunScope, AgentRunSettlementStatus,
    AgentRunStatus, AgentSchemaPolicy, AgentScope, AgentSettings, AgentTaskContent,
    AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand,
    AgentTaskEntityMessage, AgentTaskEntityRef, AgentTaskEntityReply, AgentTaskId,
    AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskStatus,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, AutonomyAdmissionDecision,
    SessionMemoryCursor, SessionMemoryStore, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, HumanCheckpointId, PrincipalRef,
};

use crate::report::AcceptanceReport;
use crate::wiring::{World, ASK_TIMEOUT, TOOL};

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const AUDITOR: &str = "auditor-agent";
const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

/// The one agent both walks instantiate.
pub(crate) fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("the agent id is valid")
}

/// That agent's scope under the walk's tenant.
pub(crate) fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("the agent scope is valid")
}

fn auditor_id() -> AgentId {
    AgentId::new(AUDITOR).expect("the auditor id is valid")
}

fn auditor_scope() -> AgentScope {
    AgentScope::new(tenant(), auditor_id()).expect("the auditor scope is valid")
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

/// The public task: one deterministic result rule, so the typed result is
/// validated before the task may complete.
pub fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new("resolve-ticket").expect("the definition id is valid"),
        "Resolve one customer support ticket.",
        crate::wiring::schema("ticket-input"),
        crate::wiring::schema("ticket-result"),
    )
    .expect("the task definition is valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("the rule id is valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
}

/// The auditor's unattended task definition, for the admission bullet.
fn unattended_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new("audit-report").expect("the definition id is valid"),
        "Produce one unattended audit report.",
        crate::wiring::schema("audit-input"),
        crate::wiring::schema("audit-result"),
    )
    .expect("the task definition is valid")
    .with_operation_class(AgentOperationClass::BoundedAsync)
}

/// The auditor's admittable envelope: budget-bounded, no tools, the
/// unattended class declared.
fn auditor_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(unattended_definition().definition_id.clone());
    envelope
        .operation_classes
        .insert(AgentOperationClass::BoundedAsync);
    envelope.budgets = AgentBudgetCeilings {
        max_loop_iterations: Some(8),
        max_model_calls: Some(8),
        max_tool_calls: Some(8),
        max_effects: Some(8),
        max_effect_attempts: Some(16),
        max_tokens: Some(100_000),
        max_cost_micros: Some(1_000_000),
        max_wall_clock_millis: Some(600_000),
        max_concurrent_effects: Some(2),
    };
    envelope
}

fn auditor_policies() -> AgentPolicyRefs {
    let policy = |name: &str| AgentPolicyRef::new(name).expect("the policy reference is valid");
    AgentPolicyRefs {
        approval: Some(policy("approval-v1")),
        authorization: Some(policy("authorization-v1")),
        escalation: Some(policy("escalation-v1")),
        guardrail: None,
        retention: None,
    }
}

/// The content sentinels the walk plants in model text, tool arguments, and
/// the proposed result: any telemetry surface containing one has leaked
/// content that default telemetry must never carry. The scripted adapter
/// plants from this same array, so a sentinel cannot drift away from its
/// sweep.
pub const CONTENT_SENTINELS: [&str; 3] = [
    "SENSITIVE-REASONING",
    "SECRET-TOKEN",
    "charged and resolved",
];

/// The two scripted model turns: ask for the gated tool, then propose.
pub fn scripted_adapter() -> DeterministicModelAdapter {
    let tool_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(format!("{} about the charge.", CONTENT_SENTINELS[0]))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                AgentToolId::new(TOOL).expect("the tool id is valid"),
                serde_json::json!({ "amount": 42, "card_token": CONTENT_SENTINELS[1] }),
            )
            .expect("the tool call is bounded"),
        );
    let proposing_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(format!("{} toward the answer.", CONTENT_SENTINELS[0]))
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": CONTENT_SENTINELS[2] }))
                .expect("the proposal is inline-bounded"),
        );
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_turn)
        .with_turn_for(2, proposing_turn)
}

fn task_message(message_id: &str) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(serde_json::json!({ "ticket": 1 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = message_id.to_string();
    message
}

fn send_request(message: &Message) -> SendMessageRequest {
    let mut metadata = HashMap::new();
    metadata.insert(
        "traceparent".to_string(),
        serde_json::Value::String(INGRESS_PARENT.to_string()),
    );
    SendMessageRequest {
        message: message.clone(),
        configuration: None,
        metadata: Some(metadata),
        tenant: Some(TENANT.to_string()),
    }
}

// ---------------------------------------------------------------------------
// The pieces both walks drive: one agent, one task, one run, one pass.
// ---------------------------------------------------------------------------

/// The support agent's authority envelope: its one task definition, and every
/// tool the deployment's registry declares.
///
/// The gated provider walk widens exactly two dimensions of this — the model
/// profile it selected and that profile's credential binding — and nothing
/// else, so both walks run the same agent under the same authority.
pub(crate) fn support_envelope(world: &World) -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(task_definition().definition_id.clone());
    for (tool, declaration) in world.registry.tool_declarations() {
        envelope.tools.insert(tool, declaration);
    }
    envelope
}

/// Instantiates the support agent under `envelope` and `settings`, and answers
/// the settings revision the sharded entity accepted.
pub(crate) async fn instantiate_support_agent(
    world: &World,
    envelope: AgentAuthorityEnvelope,
    settings: AgentSettings,
) -> AgentRevisionNumber {
    let agent = registered_agent_entity_ref(&world.agent_registration, &agent_scope());
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
        "Resolves customer support tickets end to end.",
        envelope,
    )
    .expect("the agent definition is valid");
    let reply = agent
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::Instantiate {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::DefinitionUpdate,
                        &agent_scope(),
                        "1",
                    )
                    .expect("the operation id derives"),
                    definition: Box::new(definition),
                    settings: Box::new(settings),
                    provenance: Box::new(provenance(1)),
                },
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded agent replies");
    let AgentEntityReply::Applied { outcome } = reply else {
        panic!("the agent instantiates, got {reply:?}");
    };
    outcome.settings_revision
}

/// Sends one A2A message into the in-process service core and answers the
/// durable task identity it mapped to.
pub(crate) async fn send_task(world: &World, message: &Message) -> String {
    world
        .service
        .send_message(&a2a_server::ServiceParams::new(), &send_request(message))
        .await
        .expect("the send is accepted")
        .id
}

/// The scope of the task's initial run: derived, never read back from a
/// record, so both walks name the run before anything has written one.
pub(crate) fn initial_run_scope(task_scope: &AgentTaskScope) -> AgentRunScope {
    let run = run_id_for_assignment(task_scope.task(), AgentAssignmentGeneration::new(1))
        .expect("the run id derives");
    AgentRunScope::new(tenant(), agent_id(), run).expect("the run scope is valid")
}

/// Settles the sharded task entity: its owed cross-entity work, drained.
pub(crate) async fn settle_task_entity(task: &AgentTaskEntityRef) {
    let _reply = task
        .ask(
            |reply_to| AgentTaskEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded task settles");
}

/// Settles the sharded run entity.
pub(crate) async fn settle_run_entity(run: &AgentRunEntityRef) {
    let _reply = run
        .ask(
            |reply_to| AgentRunEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded run settles");
}

/// Passivates the run actor, so store-level result delivery and the actor
/// never hold two copies of one revision.
pub(crate) fn park_run_actor(world: &World, run_scope: &AgentRunScope) {
    let _was_resident =
        passivate_agent_run_entity(&world.sharding, world.run_registration.key(), run_scope)
            .expect("run passivation routes");
}

/// The run's durable status, read from the store rather than the actor.
pub(crate) async fn run_status(world: &World, run_scope: &AgentRunScope) -> Option<AgentRunStatus> {
    load_agent_run_state(&world.runs, run_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .and_then(|state| state.status())
}

/// The first surface in `surfaces` that carries `needle`, when one does.
///
/// One needle, one sweep: the acceptance walk runs it over its planted content
/// sentinels, and the gated provider walk over the resolved provider key.
pub(crate) fn surface_carrying<'a>(
    surfaces: &'a [(&'static str, String)],
    needle: &str,
) -> Option<&'a (&'static str, String)> {
    surfaces
        .iter()
        .find(|(_label, surface)| surface.contains(needle))
}

/// Runs the whole acceptance walk and returns the transcript plus the typed
/// facts behind it.
///
/// # Panics
///
/// Panics if any bullet's fact does not hold — the walk is the check.
#[allow(clippy::too_many_lines)]
pub async fn run_acceptance() -> AcceptanceReport {
    let world = World::new(
        scripted_adapter(),
        A2AAgentTarget::new(agent_id(), task_definition()),
    );
    let mut lines = vec![String::new(); 18];

    // 1/18 — instantiate with versioned settings.
    let settings_revision =
        instantiate_support_agent(&world, support_envelope(&world), AgentSettings::default()).await;
    lines[0] = format!(
        "ok  1/18 instantiated with versioned settings: revision {}",
        settings_revision.get()
    );

    // 2/18 — one deduplicated A2A task, one initial run. The run half of the
    // line is proven before anything prints: the first pump below finds this
    // *derived* initial-run scope parked WaitingForApproval, so the identity
    // named here is the one the durable record answers for.
    let message = task_message("msg-1");
    let first = send_task(&world, &message).await;
    let duplicate = send_task(&world, &message).await;
    assert_eq!(first, duplicate, "one durable task identity");
    let task_id = first;
    let task_scope = AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(&task_id).expect("the task id is valid"),
    )
    .expect("the task scope is valid");
    let run_scope = initial_run_scope(&task_scope);
    let task = registered_agent_task_entity_ref(&world.task_registration, &task_scope);
    let run = registered_agent_run_entity_ref(&world.run_registration, &run_scope);
    lines[1] =
        format!("ok  2/18 duplicate A2A sends mapped to one task {task_id} and its initial run");

    // Local drivers over the sharded surface. The run actor is passivated
    // before every dispatcher pass, so the store-level result delivery and
    // the actor never hold two copies of one revision.
    let settle_task = || settle_task_entity(&task);
    let settle_run = || settle_run_entity(&run);
    let park_run_actor = || park_run_actor(&world, &run_scope);
    let run_status = || run_status(&world, &run_scope);
    let pump = || async {
        for _round in 0..16 {
            settle_task().await;
            settle_run().await;
            park_run_actor();
            let pass = world
                .pipeline()
                .pump_run(&run_scope)
                .await
                .expect("the dispatch pass runs");
            let status = run_status().await;
            let waiting = matches!(
                status,
                Some(AgentRunStatus::WaitingForApproval)
                    | Some(AgentRunStatus::WaitingForReconciliation)
            );
            let terminal = status.is_some_and(|status| status.is_terminal());
            let moved = pass.registered + pass.claimed + pass.delivered + pass.cancelled > 0;
            if terminal {
                settle_run().await;
                settle_task().await;
                return;
            }
            if waiting && !moved {
                return;
            }
        }
        panic!("the acceptance flow did not converge");
    };

    // 12/18 (and 7's first half) — the model turn runs through the
    // dispatcher, asks for the gated tool, and the run parks.
    pump().await;
    assert_eq!(
        run_status().await,
        Some(AgentRunStatus::WaitingForApproval),
        "the checkpoint-required tool parks the run"
    );
    assert_eq!(world.tools.invocation_count(TOOL), 0, "nothing invoked yet");
    lines[11] = "ok 12/18 the checkpoint-required tool parked the run WaitingForApproval, \
                 passivated"
        .to_string();

    // 11/18 — the model call and the tool call are separate durable effects.
    let effect_count = load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .len();
    assert_eq!(effect_count, 2, "one model effect, one gated tool effect");
    lines[10] = "ok 11/18 each effectful call is its own durable effect: model and tool \
                 ticketed separately"
        .to_string();

    // 6/18 — fully passivated, still addressable.
    let _agent_was_resident = passivate_agent_entity(
        &world.sharding,
        world.agent_registration.key(),
        &agent_scope(),
    )
    .expect("agent passivation routes");
    let _task_was_resident =
        passivate_agent_task_entity(&world.sharding, world.task_registration.key(), &task_scope)
            .expect("task passivation routes");
    park_run_actor();
    let resident: usize = [
        world
            .sharding
            .registration_state(world.agent_registration.key())
            .expect("the agent registration exists")
            .local_entity_count(),
        world
            .sharding
            .registration_state(world.task_registration.key())
            .expect("the task registration exists")
            .local_entity_count(),
        world
            .sharding
            .registration_state(world.run_registration.key())
            .expect("the run registration exists")
            .local_entity_count(),
    ]
    .into_iter()
    .sum();
    assert_eq!(resident, 0, "no per-agent runtime resources remain");
    let describe = task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Describe),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the passivated task is still addressable");
    assert!(matches!(describe, AgentTaskEntityReply::Snapshot(Some(_))));
    lines[5] = "ok  6/18 fully passivated (0 resident entities) and still addressable: the \
                describe ask re-materialized the owner"
        .to_string();

    // The approval: the human decision resumes the gated tool.
    let checkpoint_id: HumanCheckpointId = {
        let state = load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .expect("the run exists");
        state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .first()
            .expect("the approval checkpoint is open")
            .checkpoint_id
            .clone()
    };
    let resolve = |discriminator: &'static str, decision: Box<AgentCheckpointDecision>| {
        let checkpoint_id = checkpoint_id.clone();
        let run = &run;
        async move {
            run.ask(
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::CheckpointResolution,
                            &agent_scope(),
                            discriminator,
                        )
                        .expect("the decision key derives"),
                        checkpoint_id,
                        resolver: PrincipalRef {
                            principal_type: "user".to_string(),
                            principal_id: "approver".to_string(),
                            display_name: None,
                        },
                        decision,
                        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
                    }),
                    reply_to,
                },
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded run replies to the decision")
        }
    };
    let approve = || {
        Box::new(AgentCheckpointDecision::Approval(
            AgentApprovalDecision::Approve {
                credential_binding: None,
                expires_at: AgentTimestampMillis::new(10_000_000),
                allowed_use_count: 1,
            },
        ))
    };
    let approved = resolve("approve-1", approve()).await;
    assert!(
        matches!(approved, AgentRunEntityReply::Applied { .. }),
        "the approval applies, got {approved:?}"
    );

    // 14/18 — the worker invokes the tool, then dies: dispatcher pod loss.
    // The external system committed, so the outcome is ambiguous, and a
    // non-idempotent effect must park one Indeterminate — never re-invoke.
    settle_run().await;
    park_run_actor();
    world.probe.arm(AgentDispatchWindow::AfterInvocation);
    let dying = world
        .pipeline()
        .pump_run(&run_scope)
        .await
        .expect("the dying pass runs");
    assert!(dying.died, "the worker died after the invocation");
    world.expire_lease();
    let recovering = world
        .pipeline()
        .pump_run(&run_scope)
        .await
        .expect("the recovery pass runs");
    assert!(recovering.parked_indeterminate > 0);
    assert_eq!(
        run_status().await,
        Some(AgentRunStatus::WaitingForReconciliation),
        "the ambiguity parks the run"
    );
    assert_eq!(
        world.tools.invocation_count(TOOL),
        1,
        "invoked exactly once, never re-invoked"
    );
    lines[13] = "ok 14/18 the ambiguous non-idempotent tool parked one Indeterminate outcome; \
                 invoked exactly once, never re-invoked"
        .to_string();

    // 13/18 — owner pod loss: the reconciliation decision's own write is
    // killed; the redelivered decision converges. (The dispatcher half was
    // the probe kill above.)
    let confirmed = || {
        Box::new(AgentCheckpointDecision::Reconciliation(
            AgentReconciliationDecision::ConfirmedCompleted {
                resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                    outcome: Box::new(AgentRunEffectOutcome::Tool {
                        call_id: AgentToolCallId::new("call-1").expect("the call id is valid"),
                        content: AgentTaskContent::inline(
                            serde_json::json!({ "charged": true, "receipt": "r-42" }),
                        )
                        .expect("the content is inline-bounded"),
                    }),
                }),
            },
        ))
    };
    let reconciliation_checkpoint: HumanCheckpointId = {
        let state = load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .expect("the run exists");
        state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .first()
            .expect("the reconciliation checkpoint is open")
            .checkpoint_id
            .clone()
    };
    let reconcile = |discriminator: &'static str| {
        let checkpoint_id = reconciliation_checkpoint.clone();
        let run = &run;
        async move {
            run.ask(
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::CheckpointResolution,
                            &agent_scope(),
                            discriminator,
                        )
                        .expect("the decision key derives"),
                        checkpoint_id,
                        resolver: PrincipalRef {
                            principal_type: "user".to_string(),
                            principal_id: "operator".to_string(),
                            display_name: None,
                        },
                        decision: confirmed(),
                        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
                    }),
                    reply_to,
                },
                ASK_TIMEOUT,
            )
            .await
            .expect("the sharded run replies to the reconciliation")
        }
    };
    world.runs.crash_at(1, CrashPoint::BeforeWrite);
    let lost = reconcile("reconcile-1").await;
    assert!(
        matches!(lost, AgentRunEntityReply::Rejected { .. }),
        "the killed owner surfaced the loss, got {lost:?}"
    );
    world.runs.survive();
    lines[12] = "ok 13/18 recovered after dispatcher loss (worker died mid-attempt) and owner \
                 loss (decision write killed, redelivered)"
        .to_string();

    // 15/18 — the redelivered, deduplicated reconciliation decision resumes.
    let redelivered = reconcile("reconcile-1").await;
    assert!(
        matches!(redelivered, AgentRunEntityReply::Applied { .. }),
        "the redelivered decision applies, got {redelivered:?}"
    );
    let replay = reconcile("reconcile-1").await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "the replay is answered from the record, got {replay:?}"
    );
    lines[14] = "ok 15/18 resumed only after the deduplicated reconciliation decision; its \
                 replay answered Duplicate"
        .to_string();

    // The closing turn: the second model call through the dispatcher, the
    // typed proposal, the task's validation and acceptance.
    pump().await;
    assert_eq!(run_status().await, Some(AgentRunStatus::Completed));
    assert_eq!(world.adapter.calls(), 2, "both turns went through workers");
    lines[6] = "ok  7/18 both model turns executed through dispatcher worker-1".to_string();

    // 3/18 — the typed result was validated before completion.
    let durable_task =
        load_agent_task_state(&world.tasks, &task_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the task state loads")
            .expect("the task exists");
    assert_eq!(durable_task.status(), Some(AgentTaskStatus::Completed));
    let record = durable_task.task().expect("the task is created");
    assert!(record.accepted_result.is_some(), "the result was accepted");
    assert_eq!(record.rejection_count, 0, "the rule passed first try");
    lines[2] = "ok  3/18 the typed result passed rule answer-present before the task completed"
        .to_string();

    // 5/18 — the escrow settled durably: what the run consumed, exactly once.
    let consumed = record.escrow.consumed();
    let run_record = load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists")
        .snapshot()
        .expect("the run accepted");
    assert_eq!(run_record.settlement, AgentRunSettlementStatus::Returned);
    lines[4] = format!(
        "ok  5/18 budgets settled durably: {} loop iterations, {} model calls, {} effect \
         attempts, escrow returned",
        consumed.loop_iterations, consumed.model_calls, consumed.effect_attempts
    );

    // 4/18 — autonomy admission fails closed, and a widening definition
    // stops what the stale admission no longer covers. A second agent runs
    // this half so its envelope can satisfy every structural requirement.
    let auditor = registered_agent_entity_ref(&world.agent_registration, &auditor_scope());
    let auditor_definition = || {
        let mut definition = AgentDefinition::new(
            AgentDefinitionId::new("auditor-v1").expect("the definition id is valid"),
            "Produces audit reports unattended.",
            auditor_envelope(),
        )
        .expect("the auditor definition is valid");
        definition.policies = auditor_policies();
        definition
    };
    let instantiated = auditor
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::Instantiate {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::DefinitionUpdate,
                        &auditor_scope(),
                        "1",
                    )
                    .expect("the operation id derives"),
                    definition: Box::new(auditor_definition()),
                    settings: Box::new(AgentSettings::default()),
                    provenance: Box::new(provenance(2)),
                },
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the auditor instantiates");
    assert!(matches!(instantiated, AgentEntityReply::Applied { .. }));
    let audit_scope = AgentTaskScope::new(
        tenant(),
        AgentTaskId::new("audit-1").expect("the task id is valid"),
    )
    .expect("the audit task scope is valid");
    let audit_task = registered_agent_task_entity_ref(&world.task_registration, &audit_scope);
    let created = audit_task
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, "audit-1", "1"],
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(AgentTaskCreation {
                        definition: unattended_definition(),
                        input: AgentTaskContent::inline(serde_json::json!({ "quarter": "Q3" }))
                            .expect("the input is inline-bounded"),
                        assignee: Some(auditor_id()),
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
            ASK_TIMEOUT,
        )
        .await
        .expect("the audit task is created");
    assert!(matches!(created, AgentTaskEntityReply::Applied { .. }));
    let audit_refusal = || async {
        load_agent_task_state(&world.tasks, &audit_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the audit task state loads")
            .expect("the audit task exists")
            .task()
            .expect("the audit task is created")
            .last_refusal
            .clone()
    };
    let refusal = audit_refusal().await.expect("a refusal is recorded");
    assert_eq!(refusal.reason, AgentAssignmentRefusalReason::NotAdmitted);

    let decision = AutonomyAdmissionDecision::new(
        [AgentOperationClass::BoundedAsync].into_iter().collect(),
        AgentRevisionNumber::INITIAL,
        AgentRevisionNumber::INITIAL,
        auditor_envelope(),
        AgentAdmissionEvaluator::Service("risk-policy-service".to_string()),
        AgentAdmissionRequirement::ALL.into_iter().collect(),
        provenance(3).accepted_at,
    )
    .expect("a complete admission");
    let admitted = auditor
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::Admit {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::Command,
                        &auditor_scope(),
                        "admit-1",
                    )
                    .expect("the operation id derives"),
                    decision: Box::new(decision),
                },
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the admission is decided");
    assert!(matches!(admitted, AgentEntityReply::Applied { .. }));

    // Widen the definition past the admitted envelope, then re-settle: the
    // stale admission no longer covers the agent, so assignment stays
    // refused without anything having to notice the update.
    let mut widened = auditor_envelope();
    widened
        .operation_classes
        .insert(AgentOperationClass::Continuous);
    let mut widened_definition = AgentDefinition::new(
        AgentDefinitionId::new("auditor-v1").expect("the definition id is valid"),
        "Produces audit reports unattended, now continuously too.",
        widened,
    )
    .expect("the widened definition is valid");
    widened_definition.policies = auditor_policies();
    let republished = auditor
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::PublishDefinition {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::DefinitionUpdate,
                        &auditor_scope(),
                        "2",
                    )
                    .expect("the operation id derives"),
                    definition: Box::new(widened_definition),
                    provenance: Box::new(provenance(4)),
                },
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the widened definition publishes");
    assert!(matches!(republished, AgentEntityReply::Applied { .. }));
    let _settled = audit_task
        .ask(
            |reply_to| AgentTaskEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("the audit task settles");
    let refusal = audit_refusal().await.expect("a refusal is recorded");
    assert_eq!(refusal.reason, AgentAssignmentRefusalReason::NotAdmitted);
    lines[3] = "ok  4/18 admission fails closed: not-admitted before the decision, \
                not-admitted again after a widening definition"
        .to_string();

    // 8/18 — the session view assembles the correlated trace segments.
    let view = assemble_agent_session_view(
        &world.runs,
        &run_scope,
        &AgentSchemaPolicy::default(),
        None,
        AgentTimestampMillis::new(world.clock.load(Ordering::SeqCst)),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");
    assert!(
        !view.trace_segments.is_empty(),
        "the durable records carry trace segments"
    );
    lines[7] = format!(
        "ok  8/18 the session view assembled {} correlated trace segments by run id",
        view.trace_segments.len()
    );

    // 9/18 — bounded metric names, no high-cardinality identifiers: the
    // whole observation stream, attributes included, must never carry the
    // task or run identity.
    let snapshot = world.metrics.snapshot();
    let mut metric_names: Vec<String> = snapshot
        .observations()
        .iter()
        .map(|observation| observation.name().to_string())
        .collect();
    metric_names.sort();
    metric_names.dedup();
    assert!(metric_names
        .iter()
        .all(|name| name.starts_with("rakka.agent.")));
    let observation_stream = format!("{:?}", snapshot.observations());
    assert!(
        !observation_stream.contains(task_id.as_str()),
        "a metric observation carries the task id"
    );
    assert!(
        !observation_stream.contains(run_scope.run().as_str()),
        "a metric observation carries the run id"
    );
    lines[8] = format!(
        "ok  9/18 bounded metrics observed: {}",
        metric_names.join(", ")
    );

    // 10/18 — short-term session context persisted by the sharded runs.
    let session_page = world
        .session
        .read(&run_scope, SessionMemoryCursor::start())
        .await
        .expect("the session reads");
    let session_entries = session_page.entries.len();
    let context_snapshots = world.snapshots.len(&run_scope);
    assert!(session_entries > 0, "session context persisted");
    assert!(context_snapshots > 0, "immutable snapshots persisted");
    lines[9] = format!(
        "ok 10/18 short-term session context persisted: {session_entries} entries, \
         {context_snapshots} immutable snapshots"
    );

    // 16/18 — the authoritative snapshot needs no telemetry.
    let operational = agent_operational_snapshot(
        &world.runs,
        &run_scope,
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(world.clock.load(Ordering::SeqCst)),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(
        operational.run.as_ref().expect("the run accepted").status,
        AgentRunStatus::Completed
    );
    assert_eq!(operational.wait_reason, None);
    assert!(operational.pending_effects.is_empty());
    assert_eq!(
        operational.cancellation,
        AgentCancellationProgress::NotRequested
    );
    lines[15] = "ok 16/18 the authoritative snapshot answered from durable state with no \
                 telemetry wired into the query"
        .to_string();

    // 17/18 — every default telemetry surface, swept for the planted content
    // sentinels here in the walk itself: `cargo run` fails on a leak, not
    // only the test.
    let labeled_surfaces = vec![
        (
            "metric observations",
            format!("{:?}", snapshot.observations()),
        ),
        (
            "operational snapshot",
            serde_json::to_string(&operational).expect("the snapshot serializes"),
        ),
        (
            "session view",
            serde_json::to_string(&view).expect("the view serializes"),
        ),
    ];
    for sentinel in CONTENT_SENTINELS {
        assert!(
            surface_carrying(&labeled_surfaces, sentinel).is_none(),
            "{sentinel} leaked into a default telemetry surface"
        );
    }
    let telemetry_surfaces: Vec<String> = labeled_surfaces
        .into_iter()
        .map(|(_label, surface)| surface)
        .collect();
    lines[16] = "ok 17/18 default telemetry carries no prompt, tool payload, memory content, \
                 or credential material"
        .to_string();

    // 18/18 — the decision sink was down the whole time; correctness never
    // noticed, and the loss is bounded and visible.
    let flush_failures = snapshot
        .observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_FLUSH_FAILURES)
        .len();
    assert!(flush_failures > 0, "the loss is a bounded counter");
    assert!(
        operational.decisions_owed > 0,
        "the snapshot reports what the sink has not accepted"
    );
    lines[17] = "ok 18/18 the unavailable decision sink blocked nothing: the run completed, \
                 flush failures are a bounded metric, owed events are visible"
        .to_string();

    let tool_idempotency_keys = world
        .tools
        .invocations()
        .into_iter()
        .filter(|invocation| invocation.tool == TOOL)
        .map(|invocation| invocation.idempotency_key)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let report = AcceptanceReport {
        lines,
        task_id,
        tool_invocations: world.tools.invocation_count(TOOL),
        tool_idempotency_keys,
        session_entries,
        context_snapshots,
        metric_names,
        decisions_owed: operational.decisions_owed,
        trace_segments: view.trace_segments.len(),
        telemetry_surfaces,
    };
    world.system.shutdown();
    report
}

// ---------------------------------------------------------------------------
// The gated provider walk's drive.
// ---------------------------------------------------------------------------

/// How many dispatch passes the gated walk drives before it gives up.
const PROVIDER_WALK_MAX_PASSES: usize = 20;

/// Every durable record the world holds for one run, serialized to JSON.
///
/// This is the haystack the gated walk's secret-exclusion sweep scans. It is
/// the whole of what the walk writes: the three sharded entity records, the
/// task's append-only history, the workflow outbox the effects ticket through,
/// the dispatcher fleet's index, the run's session memory, and the immutable
/// context snapshot the run's durable state still names.
async fn durable_records(
    world: &World,
    task_scope: &AgentTaskScope,
    run_scope: &AgentRunScope,
) -> Vec<(&'static str, String)> {
    use rakka_agent::{AgentTaskHistoryStore, ContextSnapshotStore, SessionMemoryStore};
    use rakka_persistence::{DurableStateStore, PersistenceId};

    let mut records = Vec::new();
    let mut push = |label: &'static str, json: String| records.push((label, json));

    let agents = world
        .agents
        .load(&agent_scope().persistence_id())
        .await
        .expect("the agent record loads");
    push(
        "agents",
        serde_json::to_string(&agents.map(|record| record.state)).expect("the record serializes"),
    );
    let tasks = world
        .tasks
        .load(&task_scope.persistence_id())
        .await
        .expect("the task record loads");
    push(
        "tasks",
        serde_json::to_string(&tasks.map(|record| record.state)).expect("the record serializes"),
    );
    let runs = world
        .runs
        .load(&run_scope.persistence_id())
        .await
        .expect("the run record loads");
    push(
        "runs",
        serde_json::to_string(&runs.as_ref().map(|record| &record.state))
            .expect("the record serializes"),
    );
    let workflow = world
        .workflow_store
        .load(&PersistenceId::new(
            rakka_agent::workflow_run_id(run_scope).as_str().to_string(),
        ))
        .await
        .expect("the workflow record loads");
    push(
        "workflow",
        serde_json::to_string(&workflow.map(|record| record.state)).expect("the record serializes"),
    );
    let fleet = world
        .fleet_store
        .load(&PersistenceId::new(
            rakka_agent_workflow::agent_dispatcher_fleet_persistence_id()
                .as_str()
                .to_string(),
        ))
        .await
        .expect("the fleet record loads");
    push(
        "fleet",
        serde_json::to_string(&fleet.map(|record| record.state)).expect("the record serializes"),
    );

    let mut cursor = rakka_agent::AgentTaskHistoryCursor::start();
    let mut history = Vec::new();
    // Bounded: a walk's log is short, and a runaway page loop would hang the
    // sweep rather than fail it.
    for _page in 0..64 {
        let Ok(page) = world.history.read(task_scope, cursor).await else {
            break;
        };
        history.extend(page.entries);
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    push(
        "task-history",
        serde_json::to_string(&history).expect("the history entries serialize"),
    );

    let session = world
        .session
        .read(run_scope, SessionMemoryCursor::start())
        .await
        .expect("the session reads");
    push(
        "session",
        serde_json::to_string(&session.entries).expect("the session entries serialize"),
    );

    let snapshot_ref = runs
        .as_ref()
        .and_then(|record| record.state.loop_state())
        .and_then(|loop_state| loop_state.context_snapshot().cloned());
    if let Some(reference) = snapshot_ref {
        let snapshot = world
            .snapshots
            .load(run_scope, &reference)
            .await
            .expect("the context snapshot loads");
        push(
            "snapshots",
            serde_json::to_string(&snapshot).expect("the snapshot serializes"),
        );
    }

    records
}

/// One checkpoint id, escaped into a legal operation-id segment.
///
/// A checkpoint id is derived from the run scope, so it carries the `/` that
/// separates a scope's parts — and an operation id refuses both that and the
/// persistence separator. The escape is injective (`~` doubles), so two
/// checkpoints can never derive one decision key, which is what makes the key
/// a deduplication key rather than a name.
fn operation_segment(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '~' => escaped.push_str("~~"),
            '/' => escaped.push_str("~s"),
            '|' => escaped.push_str("~p"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// Resolves every open approval checkpoint the run is parked on.
///
/// The gated walk is unattended, so the approval a checkpoint-required tool
/// needs is the walk's own: the same `ResolveCheckpoint` command a human
/// decision arrives as, so the run's durable path is the acceptance walk's.
async fn approve_open_checkpoints(
    world: &World,
    run: &AgentRunEntityRef,
    run_scope: &AgentRunScope,
) -> Result<usize, String> {
    let checkpoints: Vec<HumanCheckpointId> =
        load_agent_run_state(&world.runs, run_scope, &AgentSchemaPolicy::default())
            .await
            .map_err(|error| format!("the run state does not load: {error}"))?
            .and_then(|state| {
                state.loop_state().map(|loop_state| {
                    loop_state
                        .open_checkpoints()
                        .iter()
                        .filter(|checkpoint| {
                            checkpoint.kind == rakka_agent::AgentCheckpointKind::Approval
                        })
                        .map(|checkpoint| checkpoint.checkpoint_id.clone())
                        .collect()
                })
            })
            .unwrap_or_default();
    let mut approved = 0;
    for checkpoint_id in checkpoints {
        let discriminator = format!("approve-{}", operation_segment(checkpoint_id.as_str()));
        let reply = run
            .ask(
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::CheckpointResolution,
                            &agent_scope(),
                            &discriminator,
                        )
                        .expect("the decision key derives"),
                        checkpoint_id,
                        resolver: PrincipalRef {
                            principal_type: "service".to_string(),
                            principal_id: "provider-walk".to_string(),
                            display_name: None,
                        },
                        decision: Box::new(AgentCheckpointDecision::Approval(
                            AgentApprovalDecision::Approve {
                                credential_binding: None,
                                expires_at: AgentTimestampMillis::new(10_000_000),
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
            .map_err(|error| format!("the sharded run does not answer the approval: {error}"))?;
        match reply {
            AgentRunEntityReply::Applied { .. } | AgentRunEntityReply::Duplicate { .. } => {
                approved += 1;
            }
            other => return Err(format!("the approval was refused: {other:?}")),
        }
    }
    Ok(approved)
}

/// Drives one run of the walk's task through the world, under `profile`.
///
/// The same agent, task definition, tool, and envelope the acceptance walk
/// instantiates — widened by exactly the profile the environment described and
/// its credential binding — driven until the run is terminal. What it asserts
/// is structural, because a live model cannot reproduce a scripted transcript:
/// the run terminates, the provider reported the model that answered, and no
/// byte of the resolved provider key reached a durable record or a line of
/// output.
///
/// # Errors
///
/// A run that does not terminate within [`PROVIDER_WALK_MAX_PASSES`] passes, a
/// refused approval, or a leaked key.
pub async fn drive_one_run_with_profile(
    world: &World,
    profile: &rakka_agent::AgentModelProfile,
) -> Result<crate::provider::ProviderWalkReport, String> {
    let mut envelope = support_envelope(world);
    envelope.model_profiles.insert(profile.profile_id.clone());
    if let Some(binding) = &profile.credential_binding {
        envelope.credential_bindings.insert(binding.clone());
    }
    let settings = AgentSettings {
        model_profile: Some(profile.profile_id.clone()),
        ..AgentSettings::default()
    };
    let _revision = instantiate_support_agent(world, envelope, settings).await;

    let task_id = send_task(world, &task_message("provider-1")).await;
    let task_scope = AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(&task_id).map_err(|error| format!("the task id is invalid: {error}"))?,
    )
    .map_err(|error| format!("the task scope is invalid: {error}"))?;
    let run_scope = initial_run_scope(&task_scope);
    let task = registered_agent_task_entity_ref(&world.task_registration, &task_scope);
    let run = registered_agent_run_entity_ref(&world.run_registration, &run_scope);

    // The dispatcher's own segments are where a provider's reported response
    // model lands: the durable run record keeps the loop's decisions, not the
    // provider's answer about itself.
    let segments = Arc::new(rakka_agent::InMemoryAgentSegmentSink::new());

    let mut passes = 0;
    let mut terminal = None;
    while passes < PROVIDER_WALK_MAX_PASSES {
        settle_task_entity(&task).await;
        settle_run_entity(&run).await;
        park_run_actor(world, &run_scope);
        passes += 1;
        let pass = world
            .pipeline()
            .with_segments(segments.clone())
            .pump_run(&run_scope)
            .await
            .map_err(|error| format!("the dispatch pass failed: {error}"))?;
        let status = run_status(world, &run_scope).await;
        if status.is_some_and(AgentRunStatus::is_terminal) {
            settle_run_entity(&run).await;
            settle_task_entity(&task).await;
            terminal = status;
            break;
        }
        let moved = pass.registered + pass.claimed + pass.delivered + pass.cancelled > 0;
        if status == Some(AgentRunStatus::WaitingForApproval)
            && approve_open_checkpoints(world, &run, &run_scope).await? > 0
        {
            continue;
        }
        if !moved && status != Some(AgentRunStatus::WaitingForApproval) {
            return Err(format!(
                "the run stalled at {} after {passes} passes",
                status.map_or("no status", AgentRunStatus::as_label)
            ));
        }
    }
    let Some(terminal) = terminal else {
        return Err(format!(
            "the run did not terminate within {PROVIDER_WALK_MAX_PASSES} passes"
        ));
    };

    // The reason the run ended is part of the fact: a live model that never
    // proposes a result spends its budget and fails, and a line that only said
    // "terminal" would read as a success.
    let terminal_reason =
        load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
            .await
            .map_err(|error| format!("the run state does not load: {error}"))?
            .and_then(|state| state.snapshot())
            .and_then(|snapshot| snapshot.terminal_reason)
            .map_or_else(
                || "no reason recorded".to_string(),
                |reason| reason.code().to_string(),
            );

    let response_model = segments
        .segments()
        .into_iter()
        .find_map(|segment| segment.model_response.and_then(|response| response.model));
    let tool_invocations = world.tools.invocations().len();

    let mut lines = vec![
        format!(
            "ok  profile: {} via {}",
            profile.profile_id,
            profile.provider.as_label()
        ),
        format!(
            "ok  terminal: {} ({terminal_reason}) after {passes} passes",
            terminal.as_label()
        ),
        format!(
            "ok  response model: {}",
            response_model.as_deref().unwrap_or("unreported")
        ),
        format!("ok  tool invocations: {tool_invocations}"),
    ];

    // The key is read here only to prove its absence, and is never kept: the
    // walk holds no field for it, and no line it prints carries it.
    let records = durable_records(world, &task_scope, &run_scope).await;
    let key = std::env::var(crate::provider::PROVIDER_KEY_VAR).unwrap_or_default();
    if !key.is_empty() {
        if let Some((label, _record)) = surface_carrying(&records, &key) {
            return Err(format!("the provider key reached the {label} record"));
        }
        if lines.iter().any(|line| line.contains(&key)) {
            return Err("the provider key reached the walk's own transcript".to_string());
        }
    }
    lines.push(format!(
        "ok  secret exclusion: {} durable records scanned, none carries the key",
        records.len()
    ));

    Ok(crate::provider::ProviderWalkReport {
        lines,
        provider: profile.provider.clone(),
        response_model,
        tool_invocations,
    })
}
