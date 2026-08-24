//! The acceptance walk: every bullet of the multi-agent goal milestone, in
//! order, over the wired world.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter};
use rakka_agent::{
    authorized_agent_goal_view, claim_append_operation_id, evaluation_operation_id,
    load_agent_run_state, load_agent_task_state, passivate_agent_entity,
    passivate_agent_run_entity, passivate_agent_task_entity, registered_agent_entity_ref,
    registered_agent_run_entity_ref, registered_agent_task_entity_ref, run_id_for_assignment,
    AgentA2aSendExecutor, AgentA2aSendFinding, AgentAuthorityEnvelope, AgentCapabilityId,
    AgentCheckpointDecision, AgentClaimAppendRequest, AgentClaimObjectRequest, AgentDefinition,
    AgentDefinitionId, AgentDelegationStatus, AgentDispatchWindow, AgentEffectResolution,
    AgentEffectSpec, AgentEntityCommand, AgentEntityMessage, AgentEntityReply, AgentGoalCriteria,
    AgentGoalCriteriaSource, AgentGoalDecision, AgentGoalDelegationBudget,
    AgentGoalEvaluationMethod, AgentGoalEvaluationRequest, AgentGoalEvidenceRef, AgentGoalId,
    AgentGoalObjective, AgentGoalSpec, AgentGoalSpecDraft, AgentGoalStatus,
    AgentGoalTerminalReason, AgentId, AgentLoopPhase, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentPolicyRef, AgentReconciliationDecision, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunEntityCommand, AgentRunEntityMessage, AgentRunEntityReply,
    AgentRunScope, AgentRunSettlementStatus, AgentRunStatus, AgentSchemaPolicy, AgentScope,
    AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityReply, AgentTaskResultCheck,
    AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskStatus, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, AgentWorkflowStartExecutor, AgentWorkflowStartFinding,
    AgentWorkflowTerminalStatus, KnowledgeSpaceId, MemoryClassification, TenantId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_knowledge_graph::{
    ClaimCursor, ClaimFilter, KnowledgeGraphGoalClaimSource, KnowledgeGraphStore,
    KnowledgeSpaceScope,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, HumanCheckpointId, PrincipalRef,
};

use crate::report::{AcceptanceReport, EXPECTED_TRANSCRIPT};
use crate::wiring::{
    World, ASK_TIMEOUT, COORDINATOR, SKILL_SUMMARIZATION, SKILL_TRANSLATION, SPACE, SUMMARIZER,
    TENANT, TOOL, TRANSLATOR, WORKFLOW_TOOL,
};

/// The root task — and, by the resolved open-decision-14 default, the goal
/// identity itself.
pub const ROOT_TASK: &str = "mission-1";

/// The content sentinels the walk plants in model text, delegated input and
/// tool arguments, and every proposed result: any queried or serialized
/// surface containing one has leaked content the collaboration surfaces must
/// never carry. The scripted adapters plant from this same array, so a
/// sentinel cannot drift away from its sweep.
pub const CONTENT_SENTINELS: [&str; 3] = [
    "SENSITIVE-REASONING",
    "SECRET-MISSION-BRIEF",
    "SECRET-RESULT-TEXT",
];

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent(id: &str) -> AgentId {
    AgentId::new(id).expect("the agent id is valid")
}

fn scope_of(id: &str) -> AgentScope {
    AgentScope::new(tenant(), agent(id)).expect("the agent scope is valid")
}

fn root_task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new(ROOT_TASK).expect("the task id is valid"),
    )
    .expect("the task scope is valid")
}

fn root_run_scope() -> AgentRunScope {
    let run = run_id_for_assignment(
        root_task_scope().task(),
        rakka_agent::AgentAssignmentGeneration::new(1),
    )
    .expect("the run id derives");
    AgentRunScope::new(tenant(), agent(COORDINATOR), run).expect("the run scope is valid")
}

fn goal() -> AgentGoalId {
    AgentGoalId::for_root_task(root_task_scope().task())
}

fn owner() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "mission-owner".to_string(),
        display_name: None,
    }
}

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: owner(),
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn definition(id: &str, description: &str) -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(id).expect("the definition id is valid"),
        description,
        crate::wiring::schema(&format!("{id}-input")),
        crate::wiring::schema(&format!("{id}-result")),
    )
    .expect("the task definition is valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("the rule id is valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
}

/// The coordinator's root task definition.
pub fn mission_definition() -> AgentTaskDefinition {
    definition("coordinate-mission", "Coordinate one mission end to end.")
}

/// The translator's typed task definition, advertised through the A2A
/// catalog.
pub fn translate_definition() -> AgentTaskDefinition {
    definition("translate-document", "Translate one mission document.")
}

/// The summarizer's typed task definition, advertised through the A2A
/// catalog.
pub fn summarize_definition() -> AgentTaskDefinition {
    definition("summarize-document", "Summarize one mission document.")
}

/// The mission goal: evaluator-gated satisfaction, two specialist skills,
/// one versioned workflow tool, one shared knowledge space, bounded
/// delegation ceilings, and an all-members fan-in rule.
fn goal_spec() -> AgentGoalSpec {
    AgentGoalSpec {
        owner: owner(),
        objective: AgentGoalObjective {
            artifact: None,
            summary: CONTENT_SENTINELS[1].to_string(),
        },
        criteria: AgentGoalCriteria {
            source: AgentGoalCriteriaSource::Policy(
                AgentPolicyRef::new("mission-complete").expect("the policy ref is valid"),
            ),
            revision: AgentRevisionNumber::INITIAL,
            digest: None,
        },
        priority: None,
        deadline: None,
        cancellation: None,
        allocation: rakka_agent::AgentBudgetAllocation::unbounded(),
        limits: rakka_agent::AgentBudgetLimits::unbounded(),
        delegation: Some(AgentGoalDelegationBudget {
            max_depth: Some(2),
            max_fan_out: Some(4),
            max_descendants: Some(8),
            max_concurrent: Some(4),
        }),
        fan_in: Some(rakka_agent::AgentFanInPolicy::All),
        exhaustion: rakka_agent::AgentGoalExhaustionPolicy::default(),
        allowed_skills: [
            AgentCapabilityId::new(SKILL_TRANSLATION).expect("the skill id is valid"),
            AgentCapabilityId::new(SKILL_SUMMARIZATION).expect("the skill id is valid"),
        ]
        .into_iter()
        .collect(),
        allowed_tools: Default::default(),
        allowed_workflows: [
            rakka_agent::AgentWorkflowToolId::new(WORKFLOW_TOOL).expect("the tool id is valid")
        ]
        .into_iter()
        .collect(),
        knowledge_spaces: [KnowledgeSpaceId::new(SPACE).expect("the space id is valid")]
            .into_iter()
            .collect(),
        environments: Default::default(),
        evaluator: Some(AgentPolicyRef::new("mission-evaluator").expect("the ref is valid")),
        required_evidence: ["artifact".to_string()].into_iter().collect(),
        escalation: None,
        terminal_decision: None,
        stagnation: None,
        stagnation_policy: Default::default(),
        settings_revision: None,
        policy_revision: None,
    }
}

/// The coordinator's scripted turns: the fan-out, then the synthesis.
fn coordinator_adapter() -> DeterministicModelAdapter {
    let delegate = |call: &str, skill: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(call).expect("the call id is valid"),
            AgentToolId::new(crate::wiring::DELEGATE_TOOL).expect("the tool id is valid"),
            serde_json::json!({ "skill": skill, "input": { "text": CONTENT_SENTINELS[1] } }),
        )
        .expect("the tool call is bounded")
    };
    let fan_out = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(format!("{} about the mission plan.", CONTENT_SENTINELS[0]))
        .with_tool_call(delegate("delegate-1", SKILL_TRANSLATION))
        .with_tool_call(delegate("delegate-2", SKILL_SUMMARIZATION))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("invoke-1").expect("the call id is valid"),
                AgentToolId::new(WORKFLOW_TOOL).expect("the tool id is valid"),
                serde_json::json!({ "order": "o-1" }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("await-1").expect("the call id is valid"),
                AgentToolId::new(crate::wiring::AWAIT_TOOL).expect("the tool id is valid"),
                serde_json::json!({}),
            )
            .expect("the tool call is bounded"),
        );
    let synthesis = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(format!("{} toward the synthesis.", CONTENT_SENTINELS[0]))
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": CONTENT_SENTINELS[2] }))
                .expect("the proposal is inline-bounded"),
        );
    DeterministicModelAdapter::new()
        .with_turn_for(1, fan_out)
        .with_turn_for(2, synthesis)
}

/// The translator's scripted turns: one working turn — during which the
/// commanded claim append resolves — then the typed proposal.
fn translator_adapter() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text(format!("{} about the translation.", CONTENT_SENTINELS[0])),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text(format!("{} toward the translation.", CONTENT_SENTINELS[0]))
                .with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": CONTENT_SENTINELS[2] }))
                        .expect("the proposal is inline-bounded"),
                ),
        )
}

/// The summarizer's scripted turns: the non-idempotent payment, then the
/// proposal.
fn summarizer_adapter() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text(format!("{} about the payment.", CONTENT_SENTINELS[0]))
                .with_tool_call(
                    AgentToolCallRequest::new(
                        AgentToolCallId::new("pay-1").expect("the call id is valid"),
                        AgentToolId::new(TOOL).expect("the tool id is valid"),
                        serde_json::json!({ "amount": 9, "brief": CONTENT_SENTINELS[1] }),
                    )
                    .expect("the tool call is bounded"),
                ),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text(format!("{} toward the summary.", CONTENT_SENTINELS[0]))
                .with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": CONTENT_SENTINELS[2] }))
                        .expect("the proposal is inline-bounded"),
                ),
        )
}

/// A synthetic committed effect for the executor-level replay beats: the
/// executors read only the persisted record they are handed, so the intent's
/// own shape is immaterial — exactly as a re-driven generation's would be.
fn replay_intent(scope: &AgentRunScope) -> AgentRunEffect {
    AgentRunEffect::new(
        scope,
        99,
        0,
        AgentRunEffectRequest::Tool {
            call: Box::new(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("replay-1").expect("the call id is valid"),
                    AgentToolId::new("replay").expect("the tool id is valid"),
                    serde_json::json!({}),
                )
                .expect("the tool call is bounded"),
            ),
        },
        &AgentEffectSpec::idempotent(1).expect("the spec is valid"),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(999),
    )
    .expect("the synthetic effect constructs")
}

/// Asks a sharded entity, retrying through the transient window where a
/// just-passivated actor's stop has not yet finished — exactly what a
/// production caller does across a shard handoff: the entity's durable
/// state is the identity, and a routed retry lands on the re-materialized
/// owner.
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

/// Instantiates one agent under an explicit authority envelope.
async fn instantiate(
    world: &World,
    scope: &AgentScope,
    definition_id: &str,
    envelope: AgentAuthorityEnvelope,
) {
    let entity = registered_agent_entity_ref(&world.agent_registration, scope);
    let definition = AgentDefinition::new(
        AgentDefinitionId::new(definition_id).expect("the definition id is valid"),
        "Serves the mission.",
        envelope,
    )
    .expect("the agent definition is valid");
    let reply = ask_retrying(
        &entity,
        |reply_to| AgentEntityMessage {
            command: AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    scope,
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
        "the agent instantiates, got {reply:?}"
    );
}

/// One settle-and-dispatch round over a task/run pair; returns whether
/// anything moved.
async fn round(
    world: &World,
    task_scope: &AgentTaskScope,
    run_scope: &AgentRunScope,
    adapter: &DeterministicModelAdapter,
    settle_task: bool,
) -> bool {
    let mut moved = false;
    if settle_task {
        let task = registered_agent_task_entity_ref(&world.task_registration, task_scope);
        if let Ok(AgentTaskEntityReply::Progressed { progress }) = task
            .ask(
                |reply_to| AgentTaskEntityMessage::Settle { reply_to },
                ASK_TIMEOUT,
            )
            .await
        {
            moved |= progress.assigned || progress.settled > 0;
        }
    }
    let run = registered_agent_run_entity_ref(&world.run_registration, run_scope);
    if let Ok(AgentRunEntityReply::Progressed { progress }) = run
        .ask(
            |reply_to| AgentRunEntityMessage::Settle { reply_to },
            ASK_TIMEOUT,
        )
        .await
    {
        moved |= progress.settled > 0;
    }
    let _was_resident =
        passivate_agent_run_entity(&world.sharding, world.run_registration.key(), run_scope)
            .expect("run passivation routes");
    let pass = world
        .pipeline(adapter.clone())
        .pump_run(run_scope)
        .await
        .expect("the dispatch pass runs");
    moved |= pass.registered + pass.claimed + pass.delivered + pass.cancelled > 0;
    moved
}

/// Pumps one task/run pair until the run terminates or nothing moves.
async fn pump(
    world: &World,
    task_scope: &AgentTaskScope,
    run_scope: &AgentRunScope,
    adapter: &DeterministicModelAdapter,
    settle_task: bool,
) {
    let mut quiet = 0;
    for _ in 0..48 {
        let moved = round(world, task_scope, run_scope, adapter, settle_task).await;
        let status = load_agent_run_state(&world.runs, run_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .and_then(|state| state.status());
        if status.is_some_and(rakka_agent::AgentRunStatus::is_terminal) {
            // Final settles: the settlement and any owed exchanges drain.
            let _ = round(world, task_scope, run_scope, adapter, settle_task).await;
            let _ = round(world, task_scope, run_scope, adapter, settle_task).await;
            return;
        }
        quiet = if moved { 0 } else { quiet + 1 };
        if quiet >= 2 {
            return;
        }
    }
    panic!("the walk did not converge for {run_scope:?}");
}

async fn root_run_state(world: &World) -> rakka_agent::AgentRunState {
    load_agent_run_state(
        &world.runs,
        &root_run_scope(),
        &AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists")
}

/// Runs the whole acceptance walk and returns the transcript plus the typed
/// facts behind it.
///
/// # Panics
///
/// Panics if any bullet's fact does not hold — the walk is the check.
#[allow(clippy::too_many_lines)]
pub async fn run_acceptance() -> AcceptanceReport {
    let world = World::new();
    let mut lines = vec![String::new(); 18];
    let policy = AgentSchemaPolicy::default();

    // 1/18 — three agents, one goal-bearing root.
    let mut coordinator_envelope = AgentAuthorityEnvelope::empty();
    coordinator_envelope
        .task_definitions
        .insert(mission_definition().definition_id.clone());
    coordinator_envelope.workflow_tools.insert(
        rakka_agent::AgentWorkflowToolId::new(WORKFLOW_TOOL).expect("the tool id is valid"),
    );
    instantiate(
        &world,
        &scope_of(COORDINATOR),
        "coordinator-v1",
        coordinator_envelope,
    )
    .await;
    let mut translator_envelope = AgentAuthorityEnvelope::empty();
    translator_envelope
        .task_definitions
        .insert(translate_definition().definition_id.clone());
    translator_envelope
        .knowledge_spaces
        .insert(KnowledgeSpaceId::new(SPACE).expect("the space id is valid"));
    instantiate(
        &world,
        &scope_of(TRANSLATOR),
        "translator-v1",
        translator_envelope,
    )
    .await;
    let mut summarizer_envelope = AgentAuthorityEnvelope::empty();
    summarizer_envelope
        .task_definitions
        .insert(summarize_definition().definition_id.clone());
    for (tool, declaration) in world.registry.tool_declarations() {
        summarizer_envelope.tools.insert(tool, declaration);
    }
    instantiate(
        &world,
        &scope_of(SUMMARIZER),
        "summarizer-v1",
        summarizer_envelope,
    )
    .await;

    let root_task = registered_agent_task_entity_ref(&world.task_registration, &root_task_scope());
    let creation = ask_retrying(
        &root_task,
        |reply_to| AgentTaskEntityMessage::Command {
            command: Box::new(AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, ROOT_TASK, "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTaskCreation {
                    definition: mission_definition(),
                    input: AgentTaskContent::inline(serde_json::json!({ "mission": 1 }))
                        .expect("the input is inline-bounded"),
                    assignee: Some(agent(COORDINATOR)),
                    team: None,
                    goal: None,
                    goal_mode: Default::default(),
                    goal_spec: Some(Box::new(AgentGoalSpecDraft {
                        spec: goal_spec(),
                        provenance: provenance(2),
                        activate_on_creation: true,
                    })),
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
        "create-root",
    )
    .await;
    assert!(
        matches!(creation, AgentTaskEntityReply::Applied { .. }),
        "the goal-bearing root creates, got {creation:?}"
    );
    let root_snapshot = load_agent_task_state(&world.tasks, &root_task_scope(), &policy)
        .await
        .expect("the task state loads")
        .expect("the task exists")
        .snapshot()
        .expect("the snapshot derives");
    let goal_state = root_snapshot
        .goal_state
        .as_ref()
        .expect("the goal record exists");
    assert_eq!(goal_state.status, AgentGoalStatus::Active);
    assert!(goal_state.evaluator.is_some());
    lines[0] = EXPECTED_TRANSCRIPT[0].to_string();

    // 2/18 + 3/18 + 5/18 — the fan-out turn, driven through the production
    // dispatcher: two real A2A sends, one workflow start, the closed group.
    let adapter = coordinator_adapter();
    pump(
        &world,
        &root_task_scope(),
        &root_run_scope(),
        &adapter,
        true,
    )
    .await;

    let state = root_run_state(&world).await;
    let loop_state = state.loop_state().expect("the loop is running").clone();
    assert_eq!(loop_state.delegation_count(), 2);
    assert_eq!(loop_state.workflow_invocation_count(), 1);
    let fan_in = loop_state.fan_in().expect("the group exists");
    assert!(fan_in.closed);
    assert_eq!(fan_in.members.len(), 3);
    assert!(fan_in.resolution.is_none());
    assert_eq!(loop_state.phase(), AgentLoopPhase::AwaitingChildren);
    lines[1] = EXPECTED_TRANSCRIPT[1].to_string();

    // The two created children, with their delegation records.
    let mut children = Vec::new();
    for (id, cell) in loop_state.delegations() {
        let AgentDelegationStatus::ChildCreated { child_task, .. } = &cell.status else {
            panic!("the send settled ChildCreated, got {:?}", cell.status);
        };
        children.push((id.clone(), child_task.clone(), cell.record.clone()));
    }
    assert_eq!(children.len(), 2);
    assert_ne!(children[0].1, children[1].1, "two distinct children");
    let mut child_scopes = Vec::new();
    for (delegation, child_task, record) in &children {
        let child_scope =
            AgentTaskScope::new(tenant(), child_task.clone()).expect("the child scope is valid");
        let child_state = load_agent_task_state(&world.tasks, &child_scope, &policy)
            .await
            .expect("the child state loads")
            .expect("the child exists");
        let child = child_state.task().expect("the child task exists");
        let child_provenance = child
            .delegation
            .as_deref()
            .expect("the provenance recorded");
        assert_eq!(child_provenance.delegation, *delegation);
        assert_eq!(child_provenance.parent_task, *root_task_scope().task());
        assert_eq!(child.goal.as_ref(), Some(&goal()));
        let assignment = child.assignment.as_ref().expect("the child is assigned");
        let derived = run_id_for_assignment(child_task, assignment.generation)
            .expect("the child run derives");
        assert_eq!(assignment.run, derived);
        child_scopes.push((
            child_scope,
            AgentRunScope::new(
                tenant(),
                record.resolved.agent.clone(),
                assignment.run.clone(),
            )
            .expect("the child run scope is valid"),
        ));
    }
    lines[2] = EXPECTED_TRANSCRIPT[2].to_string();

    // 4/18 — a replayed send converges on the same child.
    let translator_record = children
        .iter()
        .find(|(_, _, record)| record.requested_skill.as_str() == SKILL_TRANSLATION)
        .map(|(_, _, record)| record.clone())
        .expect("the translation delegation exists");
    let send_executor =
        rakka_a2a::agents::A2AAgentDelegationSendExecutor::new(world.service.clone());
    let finding = send_executor
        .execute(
            &root_run_scope(),
            &replay_intent(&root_run_scope()),
            &translator_record,
            None,
        )
        .await
        .expect("the replayed send answers");
    let AgentA2aSendFinding::Sent { child_task, .. } = finding else {
        panic!("the replay converges as Sent, got {finding:?}");
    };
    let translator_child = children
        .iter()
        .find(|(_, _, record)| record.requested_skill.as_str() == SKILL_TRANSLATION)
        .map(|(_, task, _)| task.clone())
        .expect("the translation child exists");
    assert_eq!(child_task, translator_child, "no second child");
    lines[3] = EXPECTED_TRANSCRIPT[3].to_string();

    // 5/18 — the workflow start reached the compiled child's durable inbox.
    let invocation = loop_state
        .workflow_invocations()
        .values()
        .next()
        .expect("the invocation cell exists")
        .record
        .clone();
    assert_eq!(
        invocation.child_run.as_str(),
        invocation.invocation.as_str()
    );
    assert_eq!(invocation.deduplication_key, invocation.invocation.as_str());
    let inbox_entries = world.run_refund_step(&invocation.child_run).await;
    assert_eq!(inbox_entries, 1, "one durable StartRun accepted");
    lines[4] = EXPECTED_TRANSCRIPT[4].to_string();

    // 6/18 — everything passivates; the fan-out waits with nothing resident.
    for scope in [
        scope_of(COORDINATOR),
        scope_of(TRANSLATOR),
        scope_of(SUMMARIZER),
    ] {
        let _ = passivate_agent_entity(&world.sharding, world.agent_registration.key(), &scope)
            .expect("agent passivation routes");
    }
    let mut task_scopes = vec![root_task_scope()];
    task_scopes.extend(child_scopes.iter().map(|(task, _)| task.clone()));
    for scope in &task_scopes {
        let _ = passivate_agent_task_entity(&world.sharding, world.task_registration.key(), scope)
            .expect("task passivation routes");
    }
    let mut run_scopes = vec![root_run_scope()];
    run_scopes.extend(child_scopes.iter().map(|(_, run)| run.clone()));
    for scope in &run_scopes {
        let _ = passivate_agent_run_entity(&world.sharding, world.run_registration.key(), scope)
            .expect("run passivation routes");
    }
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
    let state = root_run_state(&world).await;
    assert_eq!(state.status(), Some(AgentRunStatus::Running));
    let waiting_loop = state.loop_state().expect("the loop is retained");
    assert_eq!(waiting_loop.phase(), AgentLoopPhase::AwaitingChildren);
    assert_eq!(waiting_loop.outstanding_effects().count(), 0);
    lines[5] = EXPECTED_TRANSCRIPT[5].to_string();

    // 7/18 first half — the passivated root re-materializes on the next ask.
    let describe = ask_retrying(
        &root_task,
        |reply_to| AgentTaskEntityMessage::Command {
            command: Box::new(AgentTaskEntityCommand::Describe),
            reply_to,
        },
        "describe-root",
    )
    .await;
    assert!(matches!(describe, AgentTaskEntityReply::Snapshot(Some(_))));

    // 9/18 (driven now; the claim must precede the translator's terminal) —
    // the translator appends one communal claim under its delegated grant.
    let (translator_task_scope, translator_run_scope) = child_scopes
        .iter()
        .find(|(task, _)| *task.task() == translator_child)
        .cloned()
        .expect("the translator scopes exist");
    let translator_run =
        registered_agent_run_entity_ref(&world.run_registration, &translator_run_scope);
    let append = |step: &'static str| {
        let translator_run = &translator_run;
        let translator_run_scope = translator_run_scope.clone();
        async move {
            ask_retrying(
                translator_run,
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(AgentRunEntityCommand::AppendClaim {
                        operation_id: claim_append_operation_id(&translator_run_scope, step)
                            .expect("the operation id derives"),
                        append: Box::new(AgentClaimAppendRequest {
                            space: KnowledgeSpaceId::new(SPACE).expect("the space is valid"),
                            subject: "mission-finding".to_string(),
                            predicate: "informs".to_string(),
                            object: AgentClaimObjectRequest::Node("mission".to_string()),
                            confidence_bps: 8_000,
                            classification: MemoryClassification::Unclassified,
                            evidence: Vec::new(),
                            requested_by: PrincipalRef {
                                principal_type: "service".to_string(),
                                principal_id: "translator-runtime".to_string(),
                                display_name: None,
                            },
                        }),
                    }),
                    reply_to,
                },
                "append-claim",
            )
            .await
        }
    };
    let appended = append("claim-1").await;
    assert!(
        matches!(appended, AgentRunEntityReply::Applied { .. }),
        "the claim append applies, got {appended:?}"
    );

    // The translator runs to its terminal — claim effect and proposal alike —
    // with its own task deliberately unsettled, so the owed delegation result
    // stays parked for the crash window below.
    let translator_model = translator_adapter();
    pump(
        &world,
        &translator_task_scope,
        &translator_run_scope,
        &translator_model,
        false,
    )
    .await;
    let replayed_claim = append("claim-1").await;
    assert!(
        matches!(replayed_claim, AgentRunEntityReply::Duplicate { .. }),
        "the replayed append answers from the record, got {replayed_claim:?}"
    );
    let space_scope = KnowledgeSpaceScope::new(
        tenant(),
        KnowledgeSpaceId::new(SPACE).expect("the space is valid"),
    )
    .expect("the space scope is valid");
    let claims = world
        .graph
        .query(
            &space_scope,
            &ClaimFilter::matching_all().with_goal(goal()),
            ClaimCursor::start(),
        )
        .await
        .expect("the graph answers");
    assert_eq!(claims.claims.len(), 1, "one attributable claim landed");
    let claim = &claims.claims[0];
    assert_eq!(claim.provenance.goal.as_ref(), Some(&goal()));
    assert_eq!(
        claim.provenance.task.as_ref(),
        Some(translator_task_scope.task())
    );
    let claim_has_delegation = claim.provenance.delegation.is_some();
    assert!(claim_has_delegation, "the delegation identity is stamped");
    lines[8] = EXPECTED_TRANSCRIPT[8].to_string();

    // 7/18 second half — ROOT pod loss at the result write: the child's
    // courier delivery dies before the root run's record commits, the child
    // re-drives its settle, and the redelivery converges on one result.
    let translator_task =
        registered_agent_task_entity_ref(&world.task_registration, &translator_task_scope);
    let settle_translator_task = || async {
        let _reply = translator_task
            .ask(
                |reply_to| AgentTaskEntityMessage::Settle { reply_to },
                std::time::Duration::from_secs(10),
            )
            .await
            .expect("the sharded task settles");
    };
    let _ = passivate_agent_run_entity(
        &world.sharding,
        world.run_registration.key(),
        &root_run_scope(),
    )
    .expect("run passivation routes");
    world.runs.crash_at(1, CrashPoint::BeforeWrite);
    settle_translator_task().await;
    let state = root_run_state(&world).await;
    let translator_delegation = translator_record.delegation.clone();
    assert!(
        state
            .loop_state()
            .expect("the loop is retained")
            .delegation(&translator_delegation)
            .expect("the cell exists")
            .result
            .is_none(),
        "the killed write recorded nothing"
    );
    world.runs.survive();
    settle_translator_task().await;
    let state = root_run_state(&world).await;
    let recorded = state
        .loop_state()
        .expect("the loop is retained")
        .delegation(&translator_delegation)
        .expect("the cell exists")
        .result
        .clone()
        .expect("the redelivered result recorded");
    assert_eq!(recorded.status, AgentTaskStatus::Completed);
    lines[6] = EXPECTED_TRANSCRIPT[6].to_string();

    // 8/18 — recorded without resolving the three-member group.
    let state = root_run_state(&world).await;
    let waiting_loop = state.loop_state().expect("the loop is retained");
    assert!(waiting_loop
        .fan_in()
        .expect("the group exists")
        .resolution
        .is_none());
    assert_eq!(waiting_loop.phase(), AgentLoopPhase::AwaitingChildren);
    lines[7] = EXPECTED_TRANSCRIPT[7].to_string();

    // 10/18 — CHILD pod loss: the summarizer's worker dies after invoking the
    // non-idempotent tool; recovery parks one Indeterminate.
    let (summarizer_task_scope, summarizer_run_scope) = child_scopes
        .iter()
        .find(|(task, _)| *task.task() != translator_child)
        .cloned()
        .expect("the summarizer scopes exist");
    let summarizer_model = summarizer_adapter();
    // Advance until the payment tool's effect is committed and outstanding.
    for _ in 0..16 {
        let _ = round(
            &world,
            &summarizer_task_scope,
            &summarizer_run_scope,
            &summarizer_model,
            true,
        )
        .await;
        let pending = load_agent_run_state(&world.runs, &summarizer_run_scope, &policy)
            .await
            .expect("the run state loads")
            .and_then(|state| {
                state.loop_state().map(|loop_state| {
                    loop_state.effects().iter().any(|effect| {
                        effect.kind() == AgentRunEffectKind::ToolCall && effect.is_outstanding()
                    })
                })
            })
            .unwrap_or(false);
        if pending {
            break;
        }
    }
    let _ = passivate_agent_run_entity(
        &world.sharding,
        world.run_registration.key(),
        &summarizer_run_scope,
    )
    .expect("run passivation routes");
    world.probe.arm(AgentDispatchWindow::AfterInvocation);
    let dying = world
        .pipeline(summarizer_model.clone())
        .pump_run(&summarizer_run_scope)
        .await
        .expect("the dying pass runs");
    assert!(dying.died, "the worker died after the invocation");
    world.expire_lease();
    let recovering = world
        .pipeline(summarizer_model.clone())
        .pump_run(&summarizer_run_scope)
        .await
        .expect("the recovery pass runs");
    assert!(recovering.parked_indeterminate > 0);
    let summarizer_status = load_agent_run_state(&world.runs, &summarizer_run_scope, &policy)
        .await
        .expect("the run state loads")
        .and_then(|state| state.status());
    assert_eq!(
        summarizer_status,
        Some(AgentRunStatus::WaitingForReconciliation)
    );
    assert_eq!(
        world.tools.invocation_count(TOOL),
        1,
        "invoked exactly once"
    );
    lines[9] = EXPECTED_TRANSCRIPT[9].to_string();

    // 11/18 — the deduplicated reconciliation decision resolves it, and the
    // summarizer completes; its result flows to the root through the fabric.
    let summarizer_run =
        registered_agent_run_entity_ref(&world.run_registration, &summarizer_run_scope);
    let reconciliation_checkpoint: HumanCheckpointId = {
        let state = load_agent_run_state(&world.runs, &summarizer_run_scope, &policy)
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
        let summarizer_run = &summarizer_run;
        async move {
            ask_retrying(
                summarizer_run,
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::CheckpointResolution,
                            &scope_of(SUMMARIZER),
                            discriminator,
                        )
                        .expect("the decision key derives"),
                        checkpoint_id: checkpoint_id.clone(),
                        resolver: PrincipalRef {
                            principal_type: "user".to_string(),
                            principal_id: "operator".to_string(),
                            display_name: None,
                        },
                        decision: Box::new(AgentCheckpointDecision::Reconciliation(
                            AgentReconciliationDecision::ConfirmedCompleted {
                                resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                                    outcome: Box::new(AgentRunEffectOutcome::Tool {
                                        call_id: AgentToolCallId::new("pay-1")
                                            .expect("the call id is valid"),
                                        content: AgentTaskContent::inline(
                                            serde_json::json!({ "paid": true }),
                                        )
                                        .expect("the content is inline-bounded"),
                                    }),
                                }),
                            },
                        )),
                    }),
                    reply_to,
                },
                "reconcile",
            )
            .await
        }
    };
    let resolved = reconcile("reconcile-1").await;
    assert!(
        matches!(resolved, AgentRunEntityReply::Applied { .. }),
        "the reconciliation applies, got {resolved:?}"
    );
    let replayed = reconcile("reconcile-1").await;
    assert!(
        matches!(replayed, AgentRunEntityReply::Duplicate { .. }),
        "the replay answers from the record, got {replayed:?}"
    );
    pump(
        &world,
        &summarizer_task_scope,
        &summarizer_run_scope,
        &summarizer_model,
        true,
    )
    .await;
    let state = root_run_state(&world).await;
    let summarizer_delegation = children
        .iter()
        .find(|(_, task, _)| *task != translator_child)
        .map(|(id, _, _)| id.clone())
        .expect("the summarizer delegation exists");
    assert!(
        state
            .loop_state()
            .expect("the loop is retained")
            .delegation(&summarizer_delegation)
            .expect("the cell exists")
            .result
            .is_some(),
        "the summarizer's result recorded on the root"
    );
    lines[10] = EXPECTED_TRANSCRIPT[10].to_string();

    // 12/18 — the workflow start replayed after both losses adopts the SAME
    // child run; the compiled step executed exactly once.
    let adoption = world
        .workflow_starts
        .execute(
            &root_run_scope(),
            &replay_intent(&root_run_scope()),
            &invocation,
            None,
        )
        .await
        .expect("the replayed start answers");
    assert!(
        matches!(adoption, AgentWorkflowStartFinding::Adopted),
        "the replayed start adopts, got {adoption:?}"
    );
    let entries_after_replay = world.run_refund_step(&invocation.child_run).await;
    assert_eq!(entries_after_replay, 1, "one durable StartRun ever");
    let refund_steps = world.refund_step_executions.load(Ordering::SeqCst) as usize;
    assert_eq!(refund_steps, 1, "the compiled step executed exactly once");
    lines[11] = EXPECTED_TRANSCRIPT[11].to_string();

    // 13/18 — a direct criteria decision has no door under an evaluator.
    let declared = ask_retrying(
        &root_task,
        |reply_to| AgentTaskEntityMessage::Command {
            command: Box::new(AgentTaskEntityCommand::RecordGoalDecision {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::Command,
                    [TENANT, ROOT_TASK, "declare"],
                )
                .expect("the operation id derives"),
                decision: Box::new(AgentGoalDecision {
                    reason: AgentGoalTerminalReason::CriteriaSatisfied,
                    evaluation: Some(Box::new(rakka_agent::AgentGoalEvaluationRef {
                        evaluator: AgentPolicyRef::new("mission-evaluator")
                            .expect("the ref is valid"),
                        criteria_revision: AgentRevisionNumber::INITIAL,
                        evidence: None,
                        digest: None,
                        evaluation_id: None,
                        method: None,
                        evidence_items: Vec::new(),
                    })),
                    provenance: Some(Box::new(provenance(50))),
                    expected_status_revision: AgentRevisionNumber::INITIAL,
                }),
            }),
            reply_to,
        },
        "declare-decision",
    )
    .await;
    let AgentTaskEntityReply::Rejected { code, .. } = declared else {
        panic!("the declaration is refused, got {declared:?}");
    };
    assert_eq!(code, "task-goal-decision-unattested");
    lines[12] = EXPECTED_TRANSCRIPT[12].to_string();

    // 14/18 — the configured evaluator judges durable evidence; the exchange
    // records Satisfied.
    let evidence_digest = recorded.result_digest.clone();
    let root_run = registered_agent_run_entity_ref(&world.run_registration, &root_run_scope());
    let evaluated = ask_retrying(
        &root_run,
        |reply_to| AgentRunEntityMessage::Command {
            command: Box::new(AgentRunEntityCommand::EvaluateGoal {
                operation_id: evaluation_operation_id(&root_run_scope(), "evaluate-1")
                    .expect("the operation id derives"),
                evaluation: Box::new(AgentGoalEvaluationRequest {
                    goal: goal(),
                    evaluator: AgentPolicyRef::new("mission-evaluator").expect("the ref is valid"),
                    criteria_revision: AgentRevisionNumber::INITIAL,
                    method: AgentGoalEvaluationMethod::DeterministicAssertion {
                        assertion: AgentPolicyRef::new("mission-complete")
                            .expect("the ref is valid"),
                    },
                    evidence: vec![AgentGoalEvidenceRef {
                        class: "artifact".to_string(),
                        artifact: None,
                        digest: evidence_digest.clone(),
                    }],
                    requested_by: owner(),
                }),
            }),
            reply_to,
        },
        "evaluate-goal",
    )
    .await;
    assert!(
        matches!(evaluated, AgentRunEntityReply::Applied { .. }),
        "the evaluation commits, got {evaluated:?}"
    );
    pump(
        &world,
        &root_task_scope(),
        &root_run_scope(),
        &adapter,
        true,
    )
    .await;
    let goal_after = load_agent_task_state(&world.tasks, &root_task_scope(), &policy)
        .await
        .expect("the task state loads")
        .expect("the task exists")
        .snapshot()
        .expect("the snapshot derives")
        .goal_state
        .expect("the goal record exists");
    assert_eq!(goal_after.status, AgentGoalStatus::Satisfied);
    lines[13] = EXPECTED_TRANSCRIPT[13].to_string();

    // 15/18 — the application relays the deduplicated workflow result.
    let relay = |discriminator: &'static str| {
        let root_run = &root_run;
        let invocation = invocation.clone();
        async move {
            let _ = discriminator;
            ask_retrying(
                root_run,
                |reply_to| AgentRunEntityMessage::Command {
                    command: Box::new(
                        AgentRunEntityCommand::record_workflow_result(
                            &tenant(),
                            invocation.invocation.clone(),
                            invocation.child_run.clone(),
                            AgentWorkflowTerminalStatus::Completed,
                            None,
                            None,
                            Some(
                                AgentTaskContent::inline(serde_json::json!({ "refund": "issued" }))
                                    .expect("the content is inline-bounded")
                                    .digest(),
                            ),
                        )
                        .expect("the relay command derives"),
                    ),
                    reply_to,
                },
                "relay-workflow-result",
            )
            .await
        }
    };
    let relayed = relay("relay-1").await;
    assert!(
        matches!(relayed, AgentRunEntityReply::Applied { .. }),
        "the relay applies, got {relayed:?}"
    );
    let replayed_relay = relay("relay-2").await;
    assert!(
        matches!(replayed_relay, AgentRunEntityReply::Duplicate { .. }),
        "the replayed relay answers from the record, got {replayed_relay:?}"
    );
    let state = root_run_state(&world).await;
    let resolution = state
        .loop_state()
        .expect("the loop is retained")
        .fan_in()
        .expect("the group exists")
        .resolution
        .clone()
        .expect("the group resolved");
    assert!(resolution.satisfied);
    assert_eq!(resolution.code, "all-settled");
    lines[14] = EXPECTED_TRANSCRIPT[14].to_string();

    // 16/18 — the root proposes its own validated result and completes.
    pump(
        &world,
        &root_task_scope(),
        &root_run_scope(),
        &adapter,
        true,
    )
    .await;
    let state = root_run_state(&world).await;
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    assert_eq!(
        state.run().expect("the run exists").settlement,
        AgentRunSettlementStatus::Returned
    );
    let final_snapshot = load_agent_task_state(&world.tasks, &root_task_scope(), &policy)
        .await
        .expect("the task state loads")
        .expect("the task exists")
        .snapshot()
        .expect("the snapshot derives");
    assert_eq!(final_snapshot.status, AgentTaskStatus::Completed);
    assert_eq!(final_snapshot.outstanding_escrow, 0);
    lines[15] = EXPECTED_TRANSCRIPT[15].to_string();

    // 17/18 — the authorized goal view reconstructs the whole tree.
    let claim_source = KnowledgeGraphGoalClaimSource::new(world.graph.clone())
        .with_space(KnowledgeSpaceId::new(SPACE).expect("the space is valid"));
    let view = authorized_agent_goal_view(
        &world.tasks,
        &world.runs,
        &tenant(),
        &goal(),
        &owner(),
        &policy,
        Some(&claim_source),
        AgentTimestampMillis::new(world.clock.fetch_add(1, Ordering::SeqCst)),
    )
    .await
    .expect("the view assembles")
    .expect("the owner reads the goal");
    assert_eq!(view.tasks.len(), 3);
    assert_eq!(view.runs.len(), 3);
    let root_node = view
        .runs
        .iter()
        .find(|run| run.scope == root_run_scope())
        .expect("the root run node exists");
    assert_eq!(root_node.collaboration.delegations.len(), 2);
    assert_eq!(root_node.collaboration.workflow_invocations.len(), 1);
    let evaluations = view
        .runs
        .iter()
        .filter(|run| run.collaboration.evaluation.is_some())
        .count();
    assert_eq!(evaluations, 1);
    let terminal = view.contract.terminal.as_ref().expect("the goal decided");
    let evaluation_ref = terminal
        .evaluation
        .as_ref()
        .expect("the decision carries its evaluation");
    assert!(!evaluation_ref.evidence_items.is_empty());
    assert!(view.claims_available);
    assert_eq!(view.claims.len(), 1);
    lines[16] = EXPECTED_TRANSCRIPT[16].to_string();

    // 18/18 — the sentinel sweep over every queried surface.
    let mut surfaces = Vec::new();
    surfaces.push(serde_json::to_string(&view).expect("the view serializes"));
    surfaces.push(serde_json::to_string(&claims.claims).expect("the claims serialize"));
    for run_scope in &run_scopes {
        if let Some(snapshot) = rakka_agent::agent_operational_snapshot(
            &world.runs,
            run_scope,
            &policy,
            AgentTimestampMillis::new(999_999),
        )
        .await
        .expect("the query succeeds")
        {
            surfaces.push(serde_json::to_string(&snapshot).expect("the snapshot serializes"));
        }
    }
    surfaces.push(format!("{:?}", world.metrics));
    for surface in &surfaces {
        for sentinel in CONTENT_SENTINELS {
            assert!(
                !surface.contains(sentinel),
                "a queried surface leaked {sentinel}"
            );
        }
    }
    lines[17] = EXPECTED_TRANSCRIPT[17].to_string();

    AcceptanceReport {
        lines,
        child_tasks: children
            .iter()
            .map(|(_, task, _)| task.to_string())
            .collect(),
        invocation_id: invocation.invocation.as_str().to_string(),
        inbox_start_entries: entries_after_replay,
        refund_step_executions: refund_steps,
        tool_invocations: world.tools.invocation_count(TOOL),
        tool_idempotency_keys: world
            .tools
            .invocations()
            .into_iter()
            .filter(|invocation| invocation.tool == TOOL)
            .map(|invocation| invocation.idempotency_key)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        resident_at_wait: resident,
        unattested_code: code,
        goal_status: goal_after.status.as_label().to_string(),
        claim_provenance_has_delegation: claim_has_delegation,
        view_tasks: view.tasks.len(),
        view_runs: view.runs.len(),
        view_delegations: root_node.collaboration.delegations.len(),
        view_workflow_links: root_node.collaboration.workflow_invocations.len(),
        view_evaluations: evaluations,
        view_evidence: evaluation_ref.evidence_items.len(),
        view_claims: view.claims.len(),
        escrow_outstanding: final_snapshot.outstanding_escrow,
        surfaces,
    }
}
