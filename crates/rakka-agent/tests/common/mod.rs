//! The shared task-and-run fixture the integration tests drive entities with.
//!
//! One durable store per entity class, one clock, and the router that carries
//! the assignment from the task to the run and the proposal back again.
//! Entities are created on demand and thrown away, because that is what a
//! sharded entity does: it is materialized on its owner, transitions, and
//! passivates. Nothing but the stores survives between calls — so every call
//! is already a restart.
//!
//! The fixture is generic over the model adapter its [`ScriptedDispatcher`]
//! answers with, and every durable store — task, agent, and run — is a
//! [`CrashingStateStore`], which behaves as a plain in-memory store until a
//! test arms a [`CrashPoint`] (`fx.runs.crash_at(..)`, `fx.tasks.crash_at(..)`,
//! `fx.agents.crash_at(..)`) — one fixture serves the happy path, the adapter
//! matrix, and the crash matrix alike.

// Each integration-test binary compiles this module independently and uses a
// different subset of it; what one binary leaves unused is not dead code.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    run_entity, CrashingStateStore, DeferredExchangeRouter, InProcessRunEntityTransport,
    InProcessTaskEntityTransport, InProcessWakeDelivery, ScriptedDispatcher,
    SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentBudgetAllocation, AgentBudgetCeilings, AgentBudgetDimension,
    AgentContinuousGoalSpec, AgentDefinition, AgentDefinitionId, AgentEffectPolicies,
    AgentEffectSpec, AgentEntityClass, AgentEntityCommand, AgentEntityState, AgentEntityStore,
    AgentEpochSpec, AgentExchangeRouter, AgentGoalId, AgentGoalMode, AgentId, AgentModelAdapter,
    AgentOperationId, AgentOperationKind, AgentPolicyRef, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunEffectSink, AgentRunEntityStore, AgentRunMemory,
    AgentRunScope, AgentRunSnapshot, AgentRunState, AgentRunStatus, AgentSchemaId, AgentSchemaRef,
    AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition,
    AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityStore, AgentTaskResultCheck,
    AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskSnapshot, AgentTaskState,
    AgentToolBinding, AgentToolDeclaration, AgentToolDescriptor, AgentToolKind, AgentToolRegistry,
    AgentWakeBinding, AgentWakeOccurrence, AgentWakePolicy, AgentWakePolicyRevision,
    AgentWakeScanner, AgentWakeScannerSettings, AgentWakeTimerEntry, AgentWakeTimerStore,
    AgentWakeTimerStoreState, AgentWakeTriggerKind, InMemoryAgentRunEffectSink,
    InMemoryAgentTaskHistoryStore, ScheduleRevision, TenantId,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

/// Durable store for the task entity class; a pass-through until a crash
/// point is armed.
pub type TaskStore = CrashingStateStore<AgentTaskState>;
/// Durable store for the agent entity class; a pass-through until a crash
/// point is armed.
pub type AgentStore = CrashingStateStore<AgentEntityState>;
/// Durable store for the run entity class; a pass-through until a crash point
/// is armed.
pub type RunStore = CrashingStateStore<AgentRunState>;
/// Durable store for the shared wake-timer index; a pass-through until a
/// crash point is armed.
pub type WakeStore = CrashingStateStore<AgentWakeTimerStoreState>;

/// The crash-armable team store every fixture wires.
pub type TeamStore = CrashingStateStore<rakka_agent::AgentTeamState>;
pub type ConversationStore = CrashingStateStore<rakka_agent::AgentConversationState>;
/// The wake delivery the scanner injects admission commands through.
pub type WakeDelivery = InProcessWakeDelivery<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore>;
/// The scanner the wake tests drive.
pub type WakeScanner = AgentWakeScanner<WakeStore, WakeDelivery, SharedAtomicWorkflowClock>;

pub const TENANT: &str = "acme";
pub const AGENT: &str = "support-agent";
pub const TASK: &str = "ticket-1";
pub const TASK_DEFINITION: &str = "resolve-ticket";
pub const GOAL: &str = "nightly-reconciliation";

pub fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

pub fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

pub fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid")
}

pub fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
}

pub fn run_scope() -> AgentRunScope {
    let run = rakka_agent::run_id_for_assignment(
        task_scope().task(),
        rakka_agent::AgentAssignmentGeneration::new(1),
    )
    .expect("the run id should be derivable");
    AgentRunScope::new(tenant(), agent_id(), run).expect("run scope should be valid")
}

pub fn task_definition_id() -> AgentTaskDefinitionId {
    AgentTaskDefinitionId::new(TASK_DEFINITION).expect("task definition id should be valid")
}

pub fn goal_id() -> AgentGoalId {
    AgentGoalId::new(GOAL).expect("goal id should be valid")
}

/// A minimal valid goal contract for the fixture goal: policy-sourced
/// criteria at the initial revision, unbounded budgets, and the default
/// (park) exhaustion policy.
pub fn goal_spec() -> rakka_agent::AgentGoalSpec {
    rakka_agent::AgentGoalSpec {
        owner: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "goal-owner".to_string(),
            display_name: None,
        },
        objective: rakka_agent::AgentGoalObjective {
            artifact: None,
            summary: "resolve the fixture ticket to the owner's satisfaction".to_string(),
        },
        criteria: rakka_agent::AgentGoalCriteria {
            source: rakka_agent::AgentGoalCriteriaSource::Policy(
                AgentPolicyRef::new("ticket-resolved").expect("the policy ref is valid"),
            ),
            revision: AgentRevisionNumber::INITIAL,
            digest: None,
        },
        priority: None,
        deadline: None,
        cancellation: None,
        allocation: AgentBudgetAllocation::unbounded(),
        limits: rakka_agent::AgentBudgetLimits::unbounded(),
        delegation: None,
        fan_in: None,
        exhaustion: rakka_agent::AgentGoalExhaustionPolicy::default(),
        allowed_skills: Default::default(),
        allowed_tools: Default::default(),
        allowed_workflows: Default::default(),
        knowledge_spaces: Default::default(),
        environments: Default::default(),
        evaluator: None,
        required_evidence: Default::default(),
        escalation: None,
        terminal_decision: None,
        stagnation: None,
        stagnation_policy: Default::default(),
        settings_revision: None,
        policy_revision: None,
    }
}

/// The fixture goal contract with a configured completion evaluator and one
/// required evidence class: under it, a criteria decision may only arrive
/// through the goal-evaluation exchange.
pub fn goal_spec_with_evaluator() -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec();
    spec.evaluator =
        Some(AgentPolicyRef::new("ticket-evaluator").expect("the policy ref is valid"));
    spec.required_evidence = ["artifact".to_string()].into_iter().collect();
    spec
}

/// The fixture goal contract with a stagnation policy: repeated-result trips
/// at `repeated` consecutive identical completions under `action`.
pub fn goal_spec_with_stagnation(
    repeated: u32,
    action: rakka_agent::AgentGoalStagnationAction,
) -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec();
    spec.stagnation = Some(AgentPolicyRef::new("no-repeats").expect("the policy ref is valid"));
    spec.stagnation_policy = rakka_agent::AgentGoalStagnationPolicy {
        repeated_result_epochs: Some(repeated),
        no_progress_epochs: None,
        default: action,
        overrides: Default::default(),
    };
    spec
}

/// The creation draft instituting the fixture goal.
pub fn goal_spec_draft(
    spec: rakka_agent::AgentGoalSpec,
    activate: bool,
) -> rakka_agent::AgentGoalSpecDraft {
    rakka_agent::AgentGoalSpecDraft {
        spec,
        provenance: provenance(1),
        activate_on_creation: activate,
    }
}

/// The evaluation reference a criteria decision on the fixture goal rests on,
/// assessed at the initial criteria revision.
pub fn goal_evaluation() -> rakka_agent::AgentGoalEvaluationRef {
    rakka_agent::AgentGoalEvaluationRef {
        evaluator: AgentPolicyRef::new("ticket-evaluator").expect("the policy ref is valid"),
        criteria_revision: AgentRevisionNumber::INITIAL,
        evidence: None,
        digest: None,
        evaluation_id: None,
        method: None,
        evidence_items: Vec::new(),
    }
}

/// The evaluation request the fixture's coordinator commits: a deterministic
/// assertion as the configured evaluator, presenting one classed evidence
/// artifact at `criteria_revision`.
pub fn goal_evaluation_request(
    criteria_revision: AgentRevisionNumber,
) -> rakka_agent::AgentGoalEvaluationRequest {
    rakka_agent::AgentGoalEvaluationRequest {
        // The goal-bearing root task derives its goal identity from its own
        // value (open decision 14's resolved default), and the run is bound
        // to that derived id.
        goal: AgentGoalId::for_root_task(task_scope().task()),
        evaluator: AgentPolicyRef::new("ticket-evaluator").expect("the policy ref is valid"),
        criteria_revision,
        method: rakka_agent::AgentGoalEvaluationMethod::DeterministicAssertion {
            assertion: AgentPolicyRef::new("ticket-resolved").expect("the policy ref is valid"),
        },
        evidence: vec![rakka_agent::AgentGoalEvidenceRef {
            class: "artifact".to_string(),
            artifact: None,
            digest: None,
        }],
        requested_by: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "goal-orchestrator".to_string(),
            display_name: None,
        },
    }
}

/// The creation command of a goal-bearing agent-owned root task: the goal id
/// is deliberately omitted, so creation derives it from the root task's own
/// value (open decision 14's resolved default).
pub fn goal_task_creation_command(
    definition: AgentTaskDefinition,
    draft: rakka_agent::AgentGoalSpecDraft,
) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(AgentOperationKind::TaskCreation, [TENANT, TASK, "1"])
            .expect("operation id should be derivable"),
        creation: Box::new(AgentTaskCreation {
            definition,
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
            team: None,
            goal: None,
            goal_mode: Default::default(),
            goal_spec: Some(Box::new(draft)),
            parent: None,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        }),
    }
}

/// The coordination tool the delegation fixture declares.
pub const DELEGATION_TOOL: &str = "delegate";

/// The skill the fixture goal may delegate.
pub const SKILL: &str = "translation";

/// The specialist agent the fixture catalog resolves the skill to.
pub const SPECIALIST: &str = "translator";

/// The specialist's typed task definition.
pub const SPECIALIST_DEFINITION: &str = "translate-document";

pub fn delegation_tool_id() -> rakka_agent::AgentToolId {
    rakka_agent::AgentToolId::new(DELEGATION_TOOL).expect("tool id should be valid")
}

pub fn skill_id() -> rakka_agent::AgentCapabilityId {
    rakka_agent::AgentCapabilityId::new(SKILL).expect("capability id should be valid")
}

/// The target the fixture catalog resolves [`skill_id`] to.
pub fn delegation_target() -> rakka_agent::AgentDelegationTarget {
    rakka_agent::AgentDelegationTarget::new(
        AgentId::new(SPECIALIST).expect("agent id should be valid"),
        AgentTaskDefinitionId::new(SPECIALIST_DEFINITION).expect("definition id should be valid"),
    )
}

/// The delegation wiring the fixture run entity serves: the declared
/// coordination tool, a static catalog serving [`skill_id`], and the
/// delegation capability.
pub fn delegation_config() -> rakka_agent::AgentRunDelegationConfig {
    rakka_agent::AgentRunDelegationConfig::new(
        delegation_tool_id(),
        Arc::new(
            rakka_agent::StaticAgentDelegationCatalog::new()
                .with_target(skill_id(), delegation_target()),
        ),
        std::collections::BTreeSet::from([
            rakka_agent::AgentCoordinationCapabilityKind::Delegation,
        ]),
    )
    .expect("the delegation configuration declares the capability")
}

/// The coordination tool the handoff fixture declares.
pub const HANDOFF_TOOL: &str = "transfer";

/// The skill the fixture goal may hand off to.
pub const HANDOFF_SKILL: &str = "billing";

/// The agent the fixture catalog resolves the handoff skill to.
pub const HANDOFF_TARGET: &str = "billing-agent";

pub fn handoff_tool_id() -> rakka_agent::AgentToolId {
    rakka_agent::AgentToolId::new(HANDOFF_TOOL).expect("tool id should be valid")
}

pub fn handoff_skill_id() -> rakka_agent::AgentCapabilityId {
    rakka_agent::AgentCapabilityId::new(HANDOFF_SKILL).expect("capability id should be valid")
}

pub fn handoff_target_id() -> AgentId {
    AgentId::new(HANDOFF_TARGET).expect("agent id should be valid")
}

pub fn handoff_target_scope() -> AgentScope {
    AgentScope::new(tenant(), handoff_target_id()).expect("agent scope should be valid")
}

/// The target the fixture catalog resolves [`handoff_skill_id`] to: another
/// agent serving the *same* task definition — specification 8.9's contract
/// validation requires it.
pub fn handoff_target() -> rakka_agent::AgentDelegationTarget {
    rakka_agent::AgentDelegationTarget::new(handoff_target_id(), task_definition_id())
}

/// The run scope of the handoff target's generation-two run: the same task,
/// one generation later, under the target agent.
pub fn handoff_target_run_scope() -> AgentRunScope {
    let run = rakka_agent::run_id_for_assignment(
        task_scope().task(),
        rakka_agent::AgentAssignmentGeneration::new(2),
    )
    .expect("the run id should be derivable");
    AgentRunScope::new(tenant(), handoff_target_id(), run).expect("run scope should be valid")
}

/// The delegation-and-handoff wiring the fixture run entity serves: the
/// declared coordination tools, a static catalog serving both skills, and
/// both capabilities.
pub fn handoff_config() -> rakka_agent::AgentRunDelegationConfig {
    rakka_agent::AgentRunDelegationConfig::new(
        delegation_tool_id(),
        Arc::new(
            rakka_agent::StaticAgentDelegationCatalog::new()
                .with_target(skill_id(), delegation_target())
                .with_target(handoff_skill_id(), handoff_target()),
        ),
        std::collections::BTreeSet::from([
            rakka_agent::AgentCoordinationCapabilityKind::Delegation,
            rakka_agent::AgentCoordinationCapabilityKind::Handoff,
        ]),
    )
    .expect("the delegation configuration declares the capability")
    .with_handoff(rakka_agent::AgentHandoffPolicy::new(
        handoff_tool_id(),
        AgentRevisionNumber::INITIAL,
    ))
    .expect("the handoff configuration declares the capability")
}

/// The fixture goal contract allowing the handoff skill.
pub fn goal_spec_with_handoff() -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec();
    spec.allowed_skills = std::collections::BTreeSet::from([skill_id(), handoff_skill_id()]);
    spec
}

/// The await verb the fan-in fixture declares.
pub const FAN_IN_TOOL: &str = "await_children";

/// The second skill the fan-out fixture may delegate, resolved to a second
/// specialist so scenario 27's "multiple specialist agents" is two distinct
/// targets, not one twice.
pub const SKILL_2: &str = "summarization";

/// The second specialist agent.
pub const SPECIALIST_2: &str = "summarizer";

pub fn fan_in_tool_id() -> rakka_agent::AgentToolId {
    rakka_agent::AgentToolId::new(FAN_IN_TOOL).expect("tool id should be valid")
}

pub fn skill_2_id() -> rakka_agent::AgentCapabilityId {
    rakka_agent::AgentCapabilityId::new(SKILL_2).expect("capability id should be valid")
}

/// The delegation wiring with the await verb declared and both specialist
/// skills resolvable.
pub fn delegation_config_with_fan_in() -> rakka_agent::AgentRunDelegationConfig {
    rakka_agent::AgentRunDelegationConfig::new(
        delegation_tool_id(),
        Arc::new(
            rakka_agent::StaticAgentDelegationCatalog::new()
                .with_target(skill_id(), delegation_target())
                .with_target(
                    skill_2_id(),
                    rakka_agent::AgentDelegationTarget::new(
                        AgentId::new(SPECIALIST_2).expect("agent id should be valid"),
                        AgentTaskDefinitionId::new("summarize-document")
                            .expect("definition id should be valid"),
                    ),
                ),
        ),
        std::collections::BTreeSet::from([
            rakka_agent::AgentCoordinationCapabilityKind::Delegation,
        ]),
    )
    .expect("the delegation configuration declares the capability")
    .with_fan_in_tool(fan_in_tool_id())
    .expect("the fan-in tool id does not collide")
}

/// The fixture goal contract narrowed to delegating [`skill_id`] only.
pub fn goal_spec_with_delegation() -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec();
    spec.allowed_skills = std::collections::BTreeSet::from([skill_id()]);
    spec
}

/// The fixture goal contract allowing both specialist skills, with an
/// explicit fan-in policy and delegation ceilings for the fan-out tests.
pub fn goal_spec_with_fan_out(
    fan_in: Option<rakka_agent::AgentFanInPolicy>,
    delegation: Option<rakka_agent::AgentGoalDelegationBudget>,
) -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec();
    spec.allowed_skills = std::collections::BTreeSet::from([skill_id(), skill_2_id()]);
    spec.fan_in = fan_in;
    spec.delegation = delegation;
    spec
}

/// The workflow tool the workflow fixture declares.
pub const WORKFLOW_TOOL: &str = "refund-flow";

/// The workflow type the descriptor pins.
pub const WORKFLOW_TYPE: &str = "refund";

/// The workflow definition version the descriptor pins.
pub const WORKFLOW_VERSION: &str = "v1";

pub fn workflow_tool_id() -> rakka_agent::AgentWorkflowToolId {
    rakka_agent::AgentWorkflowToolId::new(WORKFLOW_TOOL).expect("workflow tool id should be valid")
}

/// The capability the workflow fixture's descriptor declares.
pub const WORKFLOW_CAPABILITY: &str = "issue-refunds";

/// The versioned descriptor under which the fixture's compiled workflow
/// appears in the agent's toolset. Declares one required capability, so
/// tests can observe the descriptor's capability surface copied onto the
/// invocation record at commit.
pub fn workflow_tool_descriptor() -> rakka_agent::AgentWorkflowToolDescriptor {
    rakka_agent::AgentWorkflowToolDescriptor::new(
        workflow_tool_id(),
        WORKFLOW_TYPE,
        rakka_agent_workflow::WorkflowDefinitionVersion::new(WORKFLOW_VERSION),
        "Runs the compiled refund workflow.",
        schema("refund-input"),
        schema("refund-output"),
    )
    .expect("the workflow-tool descriptor should be valid")
    .with_capability(
        rakka_agent::AgentCapabilityId::new(WORKFLOW_CAPABILITY)
            .expect("the capability id should be valid"),
    )
    .expect("the descriptor should accept the capability")
}

/// The workflow-tool wiring the fixture run entity serves.
pub fn workflow_config() -> rakka_agent::AgentRunWorkflowConfig {
    rakka_agent::AgentRunWorkflowConfig::new()
        .with_descriptor(workflow_tool_descriptor())
        .expect("the workflow configuration should accept the descriptor")
}

/// The fixture goal contract narrowed to the declared workflow tool, over the
/// fan-out spec so mixed delegation-and-workflow turns stay authorized.
pub fn goal_spec_with_workflow(
    fan_in: Option<rakka_agent::AgentFanInPolicy>,
) -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec_with_fan_out(fan_in, None);
    spec.allowed_workflows = std::collections::BTreeSet::from([workflow_tool_id()]);
    spec
}

/// The default continuous wake policy: durable-timer trigger, a bounded
/// per-epoch budget, and a one-minute epoch deadline — the resolved defaults
/// everywhere else.
pub fn wake_policy() -> AgentWakePolicy {
    let mut budget = AgentBudgetAllocation::unbounded();
    budget.set(AgentBudgetDimension::ModelCalls, Some(8));
    AgentWakePolicy::new([AgentWakeTriggerKind::DurableTimer], budget, Some(60_000))
        .expect("the wake policy should be valid")
}

/// A continuous goal mode over `policy` at the initial schedule revision,
/// with the standard epoch contract: each admitted occurrence runs the
/// fixture's task definition, assigned to the fixture agent.
pub fn continuous_goal_mode(policy: AgentWakePolicy) -> AgentGoalMode {
    continuous_goal_mode_with_epoch(
        policy,
        Some(AgentEpochSpec {
            definition: task_definition(),
            assignee: agent_id(),
            observation_scope: None,
        }),
    )
}

/// A continuous goal mode with an explicit — possibly absent — epoch
/// contract.
pub fn continuous_goal_mode_with_epoch(
    policy: AgentWakePolicy,
    epoch: Option<AgentEpochSpec>,
) -> AgentGoalMode {
    AgentGoalMode::Continuous(Box::new(AgentContinuousGoalSpec {
        schedule_revision: ScheduleRevision::INITIAL,
        wake_policy: AgentWakePolicyRevision::initial(policy, provenance(1))
            .expect("the wake policy revision should be valid"),
        health_condition: AgentPolicyRef::new("nightly-health")
            .expect("the policy ref should be valid"),
        epoch: epoch.map(Box::new),
    }))
}

/// The creation command of the fixture's human-owned continuous root control
/// task, for tests that drive it through fault windows themselves.
pub fn continuous_control_creation_command(goal_mode: AgentGoalMode) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(AgentOperationKind::TaskCreation, [TENANT, TASK, "1"])
            .expect("operation id should be derivable"),
        creation: Box::new(AgentTaskCreation {
            definition: task_definition().with_ownership(rakka_agent::AgentTaskOwnership::Human),
            input: AgentTaskContent::inline(serde_json::json!({ "goal": 1 }))
                .expect("the input is inline-bounded"),
            assignee: None,
            team: None,
            goal: Some(goal_id()),
            goal_mode,
            goal_spec: None,
            parent: None,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        }),
    }
}

/// The creation command of a human-owned continuous root control task that
/// also institutes the full goal contract.
pub fn continuous_goal_control_creation_command(
    goal_mode: AgentGoalMode,
    draft: rakka_agent::AgentGoalSpecDraft,
) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(AgentOperationKind::TaskCreation, [TENANT, TASK, "1"])
            .expect("operation id should be derivable"),
        creation: Box::new(AgentTaskCreation {
            definition: task_definition().with_ownership(rakka_agent::AgentTaskOwnership::Human),
            input: AgentTaskContent::inline(serde_json::json!({ "goal": 1 }))
                .expect("the input is inline-bounded"),
            assignee: None,
            team: None,
            goal: Some(goal_id()),
            goal_mode,
            goal_spec: Some(Box::new(draft)),
            parent: None,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        }),
    }
}

/// The epoch task and run scopes one wake derives, under the fixture's tenant
/// and epoch assignee.
pub fn epoch_scopes_for(wake: &rakka_agent::AgentWakeId) -> (AgentTaskScope, AgentRunScope) {
    let task = rakka_agent::epoch_task_id_for_wake(wake).expect("the epoch task derives");
    let run =
        rakka_agent::run_id_for_assignment(&task, rakka_agent::AgentAssignmentGeneration::new(1))
            .expect("the epoch run derives");
    (
        AgentTaskScope::new(tenant(), task).expect("the epoch task scope is valid"),
        AgentRunScope::new(tenant(), agent_id(), run).expect("the epoch run scope is valid"),
    )
}

/// A scheduled wake binding for the fixture's goal, due at `due_at` under
/// `revision`.
pub fn scheduled_wake_binding(due_at: u64, revision: ScheduleRevision) -> AgentWakeBinding {
    AgentWakeBinding::new(
        tenant(),
        goal_id(),
        revision,
        AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(due_at),
        },
        AgentWakeTriggerKind::DurableTimer,
        AgentTimestampMillis::new(due_at),
        AgentRevisionNumber::INITIAL,
    )
    .expect("the wake binding should be valid")
}

pub fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

/// The task requires a non-empty answer, and permits at most three autonomous
/// iterations. Both are deterministic facts the run must satisfy; neither is
/// something the model gets to decide.
pub fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(3),
        ..AgentBudgetCeilings::unbounded()
    })
}

/// A bounded model-visible descriptor for one test tool.
pub fn tool_descriptor(tool: &str) -> AgentToolDescriptor {
    AgentToolDescriptor::new(
        rakka_agent::AgentToolId::new(tool).expect("tool id should be valid"),
        AgentToolKind::Function,
        "A test tool.",
        schema("tool-input"),
        schema("tool-output"),
    )
    .expect("the descriptor should be valid")
}

/// Binds one test tool exactly as an effect spec classifies it, so the
/// registry, the commit-time policies, and the dispatch-time authority all
/// speak from the same declaration.
pub fn tool_binding_for_spec(tool: &str, spec: &AgentEffectSpec) -> AgentToolBinding {
    let mut declaration = AgentToolDeclaration::new(spec.safety_class);
    if let Some(credential) = &spec.credential_binding {
        declaration = declaration.with_credential_binding(credential.clone());
    }
    if let Some(policy) = &spec.execution_policy {
        declaration = declaration.with_execution_policy(policy.clone());
    }
    let mut binding = AgentToolBinding::new(tool_descriptor(tool), declaration, spec.max_attempts);
    if let Some(protocol) = &spec.reconciliation_protocol {
        binding = binding.with_reconciliation_protocol(protocol.clone());
    }
    if let Some(timeout) = spec.timeout_ms {
        binding = binding.with_timeout_ms(timeout);
    }
    binding
}

/// A registry holding one test tool under the given spec.
pub fn tool_registry_for_spec(tool: &str, spec: &AgentEffectSpec) -> AgentToolRegistry {
    AgentToolRegistry::new()
        .register(tool_binding_for_spec(tool, spec))
        .expect("the tool should register")
}

/// The definition envelope one registry's declarations imply: every registered
/// tool declared exactly as bound, with its credential binding authorized.
pub fn envelope_for_registry(registry: &AgentToolRegistry) -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.task_definitions.insert(task_definition_id());
    for (tool, declaration) in registry.tool_declarations() {
        if let Some(credential) = &declaration.credential_binding {
            envelope.credential_bindings.insert(credential.clone());
        }
        envelope.tools.insert(tool, declaration);
    }
    envelope
}

pub fn provenance(at: u64) -> AgentRevisionProvenance {
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

/// The task-and-run fixture, generic over the model adapter the dispatcher
/// answers model calls with and the durable sink the run's effects flush to.
pub struct Fixture<
    A: AgentModelAdapter = rakka_agent::testkit::DeterministicModelAdapter,
    S: AgentRunEffectSink = InMemoryAgentRunEffectSink,
> {
    pub tasks: TaskStore,
    pub agents: AgentStore,
    pub runs: RunStore,
    /// The shared wake-timer index the scanner recovers occurrences from.
    pub wakes: WakeStore,
    /// The delivery the scanner injects admission commands through; its fault
    /// queue injects the wake failure windows.
    pub wake_delivery: WakeDelivery,
    /// The parker every task entity parks controller-originated re-wakes
    /// through — over the same durable wake index the scanner scans.
    pub rewake_parker: std::sync::Arc<dyn rakka_agent::AgentWakeRewakeParker>,
    pub history: InMemoryAgentTaskHistoryStore,
    /// The team entity's durable store, crash-armable like every other.
    pub teams: TeamStore,
    /// The team history sink the team entities flush to.
    pub team_history: rakka_agent::InMemoryAgentTeamHistoryStore,
    /// The conversation entity's durable store, crash-armable like every
    /// other.
    pub conversations: ConversationStore,
    /// The conversation history sink the conversation entities flush to.
    pub conversation_history: rakka_agent::InMemoryAgentConversationHistoryStore,
    pub effects: S,
    pub policies: AgentEffectPolicies,
    pub router: AgentExchangeRouter,
    pub task_transport:
        InProcessTaskEntityTransport<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore>,
    /// The transport the router delivers team-bound exchanges through.
    pub team_transport: rakka_agent::testkit::InProcessTeamEntityTransport<
        TeamStore,
        rakka_agent::InMemoryAgentTeamHistoryStore,
    >,
    /// The transport the router delivers conversation-bound exchanges
    /// through.
    pub conversation_transport: rakka_agent::testkit::InProcessConversationEntityTransport<
        ConversationStore,
        rakka_agent::InMemoryAgentConversationHistoryStore,
    >,
    /// The transport the router delivers run-bound exchanges through. Held so a
    /// test's memory wiring reaches the run entities the transport builds — the
    /// acceptance path advances the loop on those, not on the entity the test
    /// drives directly.
    pub run_transport: InProcessRunEntityTransport<RunStore, S>,
    pub dispatcher: ScriptedDispatcher<A>,
    pub clock: Arc<AtomicU64>,
    /// The session-memory backend the run entity is wired with, when a test
    /// enables it. Absent by default, so the run keeps only the opaque context
    /// reference and retains no session memory — the pre-slice-1.11 behavior.
    pub memory: Option<AgentRunMemory>,
    /// The decision-event sink the run entity is wired with, when a test
    /// enables it. Absent by default, so the run records no decision events —
    /// the pre-slice-1.13 behavior.
    pub decisions: Option<Arc<dyn rakka_agent::AgentDecisionEventSink>>,
    /// The metrics recorder the run entity is wired with, when a test enables
    /// it. Absent by default, so the run records no metrics.
    pub metrics: Option<Arc<dyn rakka_core::MetricsRecorder>>,
    /// The delegation wiring the run entity serves, when a test enables it.
    /// Absent by default, so the run refuses the coordination tool.
    pub delegation: Option<rakka_agent::AgentRunDelegationConfig>,
    /// The workflow-tool wiring the run entity serves, when a test enables
    /// it. Absent by default, so workflow-tool calls take the generic path.
    pub workflow_tools: Option<rakka_agent::AgentRunWorkflowConfig>,
}

impl<A: AgentModelAdapter> Fixture<A> {
    pub fn new(dispatcher: ScriptedDispatcher<A>) -> Self {
        Self::with_sink(
            dispatcher,
            InMemoryAgentRunEffectSink::new(),
            AgentEffectPolicies::default(),
            Arc::new(AtomicU64::new(1)),
        )
    }
}

impl<A: AgentModelAdapter, S: AgentRunEffectSink> Fixture<A, S> {
    /// Builds the fixture over an explicit effect sink, effect policies, and a
    /// shared clock counter — what the dispatch-pipeline tests need.
    pub fn with_sink(
        dispatcher: ScriptedDispatcher<A>,
        effects: S,
        policies: AgentEffectPolicies,
        clock: Arc<AtomicU64>,
    ) -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let wakes = WakeStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let teams = TeamStore::new();
        let team_history = rakka_agent::InMemoryAgentTeamHistoryStore::new();
        let conversations = ConversationStore::new();
        let conversation_history = rakka_agent::InMemoryAgentConversationHistoryStore::new();

        // The task and the run exchange with each other, so each transport needs
        // the router the other lives in. The deferred router is that late binding
        // and nothing more; the durable path is unchanged.
        let deferred = DeferredExchangeRouter::new();
        let task_transport = InProcessTaskEntityTransport::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let run_transport = InProcessRunEntityTransport::new(
            runs.clone(),
            effects.clone(),
            deferred.as_router(),
            clock.clone(),
        )
        .with_effect_policies(policies.clone());
        let team_transport = rakka_agent::testkit::InProcessTeamEntityTransport::new(
            teams.clone(),
            team_history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let conversation_transport =
            rakka_agent::testkit::InProcessConversationEntityTransport::new(
                conversations.clone(),
                conversation_history.clone(),
                deferred.as_router(),
                clock.clone(),
            );
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport.clone()))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport.clone()))
            .with_route(AgentEntityClass::Team, Arc::new(team_transport.clone()))
            .with_route(
                AgentEntityClass::Conversation,
                Arc::new(conversation_transport.clone()),
            );
        deferred.install(router.clone());
        let rewake_parker: std::sync::Arc<dyn rakka_agent::AgentWakeRewakeParker> =
            std::sync::Arc::new(rakka_agent::SharedWakeTimerParker::new(wakes.clone()));
        let wake_delivery = InProcessWakeDelivery::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            router.clone(),
            clock.clone(),
        )
        .with_wake_timers(rewake_parker.clone());

        Self {
            tasks,
            agents,
            runs,
            wakes,
            wake_delivery,
            rewake_parker,
            history,
            teams,
            team_history,
            conversations,
            conversation_history,
            effects,
            policies,
            router,
            task_transport,
            team_transport,
            conversation_transport,
            run_transport,
            dispatcher,
            clock,
            memory: None,
            decisions: None,
            metrics: None,
            delegation: None,
            workflow_tools: None,
        }
    }

    /// Wires the run entity with a session-memory backend, so the loop persists
    /// context snapshots and appends session memory as it cranks.
    ///
    /// The wiring reaches both the entities the test drives directly and the
    /// ones the router's transport builds — a run must be wired identically by
    /// every driver that advances its loop.
    #[must_use]
    pub fn with_memory(mut self, memory: AgentRunMemory) -> Self {
        self.run_transport.install_memory(memory.clone());
        self.memory = Some(memory);
        self
    }

    /// Wires the run entity to serve delegation, under the same every-driver
    /// rule as [`Self::with_memory`]: an entity that advances the loop
    /// unwired refuses the coordination tool.
    #[must_use]
    pub fn with_delegation(mut self, config: rakka_agent::AgentRunDelegationConfig) -> Self {
        self.run_transport.install_delegation(config.clone());
        self.delegation = Some(config);
        self
    }

    /// Wires the run entity to serve workflow tools, under the same
    /// every-driver rule as [`Self::with_memory`].
    #[must_use]
    pub fn with_workflow_tools(mut self, config: rakka_agent::AgentRunWorkflowConfig) -> Self {
        self.run_transport.install_workflow_tools(config.clone());
        self.workflow_tools = Some(config);
        self
    }

    /// Wires the run entity with a decision-event sink, under the same
    /// every-driver rule as [`Self::with_memory`].
    #[must_use]
    pub fn with_decision_events(
        mut self,
        sink: Arc<dyn rakka_agent::AgentDecisionEventSink>,
    ) -> Self {
        self.run_transport.install_decisions(sink.clone());
        self.decisions = Some(sink);
        self
    }

    /// Wires the run entity with a metrics recorder, under the same
    /// every-driver rule as [`Self::with_memory`].
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn rakka_core::MetricsRecorder>) -> Self {
        self.run_transport.install_metrics(metrics.clone());
        self.metrics = Some(metrics);
        self
    }

    pub fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn instantiate_agent(&self) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.task_definitions.insert(task_definition_id());
        self.instantiate_agent_with_envelope(envelope).await;
    }

    /// Instantiates the agent under an explicit authority envelope, for tests
    /// whose dispatches must pass the slice 1.8 authority gate.
    pub async fn instantiate_agent_with_envelope(&self, envelope: AgentAuthorityEnvelope) {
        self.instantiate_agent_with_envelope_at(agent_scope(), envelope)
            .await;
    }

    /// Instantiates an agent at an explicit scope — the handoff target the
    /// transfer tests offer the task to.
    pub async fn instantiate_agent_at(&self, scope: AgentScope) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.task_definitions.insert(task_definition_id());
        self.instantiate_agent_with_envelope_at(scope, envelope)
            .await;
    }

    /// Instantiates an agent at an explicit scope under an explicit envelope.
    pub async fn instantiate_agent_with_envelope_at(
        &self,
        scope: AgentScope,
        envelope: AgentAuthorityEnvelope,
    ) {
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
            "Resolves customer support tickets end to end.",
            envelope,
        )
        .expect("the agent definition should be valid");

        let mut agent = AgentEntityStore::new(scope.clone(), self.agents.clone());
        agent.recover().await.expect("the agent should recover");
        agent
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &scope,
                    "1",
                )
                .expect("operation id should be derivable"),
                definition: Box::new(definition),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(provenance(1)),
            })
            .await
            .expect("the agent should instantiate");
    }

    /// Creates the task. Its assignment decision commits with the creation, and
    /// the run-creation exchange it owes is driven to the run entity.
    pub async fn create_task(&self) {
        self.create_task_with(task_definition()).await;
    }

    /// Creates the task with an ingress trace context, the way the A2A surface
    /// stamps a traced send's creation.
    pub async fn create_task_traced(&self, telemetry: rakka_agent_workflow::AgentTelemetryContext) {
        self.create_task_inner(task_definition(), telemetry).await;
    }

    /// Creates the task under an explicit definition, for tests that need their
    /// own budget ceilings.
    pub async fn create_task_with(&self, definition: AgentTaskDefinition) {
        self.create_task_inner(definition, Default::default()).await;
    }

    async fn create_task_inner(
        &self,
        definition: AgentTaskDefinition,
        telemetry: rakka_agent_workflow::AgentTelemetryContext,
    ) {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        task = task.with_wake_timers(self.rewake_parker.clone());
        if let Some(metrics) = &self.metrics {
            task = task.with_metrics(metrics.clone());
        }
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        let _reply = task
            .apply(
                AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, TASK, "1"],
                    )
                    .expect("operation id should be derivable"),
                    creation: Box::new(AgentTaskCreation {
                        definition,
                        input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                            .expect("the input is inline-bounded"),
                        assignee: Some(agent_id()),
                        team: None,
                        goal: None,
                        goal_mode: Default::default(),
                        goal_spec: None,
                        parent: None,
                        dependencies: Vec::new(),
                        escrow: None,
                        wake: None,
                        telemetry,
                        delegation: None,
                    }),
                },
                &self.router,
                now,
            )
            .await;
    }

    /// Creates the continuous root control task: the fixture goal, the default
    /// continuous wake policy, and the standard agent-owned definition.
    pub async fn create_continuous_task(&self) {
        self.create_continuous_task_with_mode(continuous_goal_mode(wake_policy()))
            .await;
    }

    /// Creates the continuous root control task as a human-owned controller:
    /// no assignee and no assignment machinery of its own, so the only runs
    /// in the world are the epochs its controller admits.
    pub async fn create_continuous_control_task(&self, goal_mode: AgentGoalMode) {
        let reply = self
            .apply_task_command(continuous_control_creation_command(goal_mode))
            .await
            .expect("the control task creation applies");
        assert!(
            matches!(reply, rakka_agent::AgentTaskEntityReply::Applied { .. }),
            "the control task is created, got {reply:?}"
        );
    }

    /// Creates the continuous root control task under an explicit goal mode.
    pub async fn create_continuous_task_with_mode(&self, goal_mode: AgentGoalMode) {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        task = task.with_wake_timers(self.rewake_parker.clone());
        if let Some(metrics) = &self.metrics {
            task = task.with_metrics(metrics.clone());
        }
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        let _reply = task
            .apply(
                AgentTaskEntityCommand::Create {
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
                        goal_mode,
                        goal_spec: None,
                        parent: None,
                        dependencies: Vec::new(),
                        escrow: None,
                        wake: None,
                        delegation: None,
                        telemetry: Default::default(),
                    }),
                },
                &self.router,
                now,
            )
            .await;
    }

    /// Applies one command to the fixture task through a freshly materialized
    /// entity — every call is already a restart.
    pub async fn apply_task_command(
        &self,
        command: AgentTaskEntityCommand,
    ) -> Result<rakka_agent::AgentTaskEntityReply, rakka_agent::AgentTaskError> {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        task = task.with_wake_timers(self.rewake_parker.clone());
        if let Some(metrics) = &self.metrics {
            task = task.with_metrics(metrics.clone());
        }
        let now = self.now();
        task.recover(now).await?;
        task.apply(command, &self.router, self.now()).await
    }

    /// A wake scanner over the fixture's durable wake index and its in-process
    /// delivery, on the fixture's shared clock.
    pub fn wake_scanner(&self) -> WakeScanner {
        AgentWakeScanner::with_clock_and_metrics(
            AgentWakeTimerStore::new(self.wakes.clone()),
            self.wake_delivery.clone(),
            SharedAtomicWorkflowClock::new(self.clock.clone()),
            AgentWakeScannerSettings::default(),
            Arc::new(rakka_core::NoopMetricsRecorder),
        )
    }

    /// Durably parks one scheduled occurrence for the fixture's goal and root
    /// control task, as the application's schedule layer would.
    pub async fn schedule_wake(&self, due_at: u64, revision: ScheduleRevision) -> AgentWakeBinding {
        let binding = scheduled_wake_binding(due_at, revision);
        let entry = AgentWakeTimerEntry::new(
            binding.clone(),
            task_scope().task().clone(),
            AgentTimestampMillis::new(due_at),
        );
        AgentWakeTimerStore::new(self.wakes.clone())
            .schedule_occurrence(entry)
            .await
            .expect("the occurrence should park");
        binding
    }

    /// A run entity over an explicit scope — the epoch runs the continuous
    /// tests drive.
    pub fn run_at(&self, scope: &AgentRunScope) -> AgentRunEntityStore<RunStore, S> {
        let mut entity = run_entity(scope, &self.runs, &self.effects)
            .with_effect_policies(self.policies.clone());
        if let Some(memory) = &self.memory {
            entity = entity.with_memory(memory.clone());
        }
        if let Some(decisions) = &self.decisions {
            entity = entity.with_decision_events(decisions.clone());
        }
        if let Some(metrics) = &self.metrics {
            entity = entity.with_metrics(metrics.clone());
        }
        if let Some(delegation) = &self.delegation {
            entity = entity.with_delegation(delegation.clone());
        }
        if let Some(workflow_tools) = &self.workflow_tools {
            entity = entity.with_workflow_tools(workflow_tools.clone());
        }
        entity
    }

    /// Applies one command to a task entity at an explicit scope — the
    /// delegated child tasks the fan-out tests create and cancel.
    pub async fn apply_task_command_at(
        &self,
        scope: &AgentTaskScope,
        command: AgentTaskEntityCommand,
    ) -> Result<rakka_agent::AgentTaskEntityReply, rakka_agent::AgentTaskError> {
        let mut task = AgentTaskEntityStore::new(
            scope.clone(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        task = task.with_wake_timers(self.rewake_parker.clone());
        if let Some(metrics) = &self.metrics {
            task = task.with_metrics(metrics.clone());
        }
        let now = self.now();
        task.recover(now).await?;
        task.apply(command, &self.router, self.now()).await
    }

    /// Settles one task entity at an explicit scope, the way a recovery sweep
    /// would.
    pub async fn settle_task_at(
        &self,
        scope: &AgentTaskScope,
    ) -> Result<rakka_agent::AgentTaskProgress, String> {
        let mut task = AgentTaskEntityStore::new(
            scope.clone(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        task = task.with_wake_timers(self.rewake_parker.clone());
        if let Some(metrics) = &self.metrics {
            task = task.with_metrics(metrics.clone());
        }
        let now = self.now();
        task.recover(now)
            .await
            .map_err(|error| error.code().to_string())?;
        task.settle_side_effects(&self.router, self.now())
            .await
            .map_err(|error| error.code().to_string())
    }

    /// Applies one command to a team entity at an explicit scope, rebuilding
    /// the entity from durable state — every call is already a restart.
    pub async fn apply_team_command_at(
        &self,
        scope: &rakka_agent::AgentTeamScope,
        command: rakka_agent::AgentTeamEntityCommand,
    ) -> Result<rakka_agent::AgentTeamEntityReply, rakka_agent::AgentTeamError> {
        let mut team = rakka_agent::AgentTeamEntityStore::new(
            scope.clone(),
            self.teams.clone(),
            self.team_history.clone(),
        );
        if let Some(metrics) = &self.metrics {
            team = team.with_metrics(metrics.clone());
        }
        let now = self.now();
        team.recover(now).await?;
        team.apply(command, &self.router, self.now()).await
    }

    /// Settles one team entity at an explicit scope, the way a recovery
    /// sweep would: expiry observation, history flush, and the courier.
    pub async fn settle_team_at(
        &self,
        scope: &rakka_agent::AgentTeamScope,
    ) -> Result<rakka_agent::AgentTeamProgress, String> {
        let mut team = rakka_agent::AgentTeamEntityStore::new(
            scope.clone(),
            self.teams.clone(),
            self.team_history.clone(),
        );
        if let Some(metrics) = &self.metrics {
            team = team.with_metrics(metrics.clone());
        }
        let now = self.now();
        team.recover(now)
            .await
            .map_err(|error| error.code().to_string())?;
        team.settle_side_effects(&self.router, self.now())
            .await
            .map_err(|error| error.code().to_string())
    }

    /// The bounded projection of one team entity, rebuilt from durable state.
    pub async fn team_snapshot_at(
        &self,
        scope: &rakka_agent::AgentTeamScope,
    ) -> Option<rakka_agent::AgentTeamSnapshot> {
        let mut team = rakka_agent::AgentTeamEntityStore::new(
            scope.clone(),
            self.teams.clone(),
            self.team_history.clone(),
        );
        let now = self.now();
        team.recover(now).await.ok()?;
        team.snapshot().ok().flatten()
    }

    /// Applies one command to a conversation entity at an explicit scope,
    /// rebuilding the entity from durable state — every call is already a
    /// restart.
    pub async fn apply_conversation_command_at(
        &self,
        scope: &rakka_agent::AgentConversationScope,
        command: rakka_agent::AgentConversationEntityCommand,
    ) -> Result<rakka_agent::AgentConversationEntityReply, rakka_agent::AgentConversationError>
    {
        let mut conversation = rakka_agent::AgentConversationEntityStore::new(
            scope.clone(),
            self.conversations.clone(),
            self.conversation_history.clone(),
        );
        if let Some(metrics) = &self.metrics {
            conversation = conversation.with_metrics(metrics.clone());
        }
        let now = self.now();
        conversation.recover(now).await?;
        conversation.apply(command, &self.router, self.now()).await
    }

    /// Settles one conversation entity at an explicit scope, the way a
    /// recovery sweep would: expiry observation, history flush, and the
    /// courier.
    pub async fn settle_conversation_at(
        &self,
        scope: &rakka_agent::AgentConversationScope,
    ) -> Result<rakka_agent::AgentConversationProgress, String> {
        let mut conversation = rakka_agent::AgentConversationEntityStore::new(
            scope.clone(),
            self.conversations.clone(),
            self.conversation_history.clone(),
        );
        if let Some(metrics) = &self.metrics {
            conversation = conversation.with_metrics(metrics.clone());
        }
        let now = self.now();
        conversation
            .recover(now)
            .await
            .map_err(|error| error.code().to_string())?;
        conversation
            .settle_side_effects(&self.router, self.now())
            .await
            .map_err(|error| error.code().to_string())
    }

    /// The bounded projection of one conversation entity, rebuilt from
    /// durable state.
    pub async fn conversation_snapshot_at(
        &self,
        scope: &rakka_agent::AgentConversationScope,
    ) -> Option<rakka_agent::AgentConversationSnapshot> {
        let mut conversation = rakka_agent::AgentConversationEntityStore::new(
            scope.clone(),
            self.conversations.clone(),
            self.conversation_history.clone(),
        );
        let now = self.now();
        conversation.recover(now).await.ok()?;
        conversation.snapshot().ok().flatten()
    }

    /// Drives the root controller, one epoch task, and that epoch's run until
    /// the epoch run terminates and every owed exchange settles — the
    /// recovery sweep of the continuous world. Every entity is rebuilt from
    /// durable state each round, so each round is already a restart.
    pub async fn pump_epoch(
        &self,
        epoch: &AgentTaskScope,
        run: &AgentRunScope,
    ) -> Result<(), String> {
        for _round in 0..64 {
            let mut outstanding = 0;
            for scope in [task_scope(), epoch.clone()] {
                let progress = self.settle_task_at(&scope).await?;
                outstanding += progress.outstanding;
            }

            let now = self.now();
            let mut entity = self.run_at(run);
            let (progress, answered, terminal) = match entity.recover(now).await {
                // The epoch's run may not exist yet: the creation and
                // assignment exchanges are still in flight.
                Err(_) => (Default::default(), 0, false),
                Ok(_) => {
                    let progress = entity
                        .settle_side_effects(&self.router, self.now())
                        .await
                        .map_err(|error| error.code().to_string())?;
                    let answered = self
                        .dispatcher
                        .drive(&mut entity, &self.router, self.now())
                        .await
                        .map_err(|error| error.code().to_string())?;
                    let terminal = entity
                        .state()
                        .ok()
                        .and_then(|state| state.status())
                        .is_some_and(rakka_agent::AgentRunStatus::is_terminal);
                    (progress, answered, terminal)
                }
            };

            if terminal && outstanding == 0 && progress.outstanding == 0 && answered == 0 {
                return Ok(());
            }
        }
        Err("the continuous world did not converge".to_string())
    }

    pub fn run(&self) -> AgentRunEntityStore<RunStore, S> {
        let mut entity = run_entity(&run_scope(), &self.runs, &self.effects)
            .with_effect_policies(self.policies.clone());
        if let Some(memory) = &self.memory {
            entity = entity.with_memory(memory.clone());
        }
        if let Some(decisions) = &self.decisions {
            entity = entity.with_decision_events(decisions.clone());
        }
        if let Some(metrics) = &self.metrics {
            entity = entity.with_metrics(metrics.clone());
        }
        if let Some(delegation) = &self.delegation {
            entity = entity.with_delegation(delegation.clone());
        }
        if let Some(workflow_tools) = &self.workflow_tools {
            entity = entity.with_workflow_tools(workflow_tools.clone());
        }
        entity
    }

    /// Drives everything the task and the run owe until nothing moves.
    ///
    /// This is what a recovery sweep does, and what the entity does to itself
    /// after every transition. It reads only durable state, so calling it after a
    /// crash is the same operation as calling it after a success.
    ///
    /// It returns the first error it hits, which is how an injected crash
    /// surfaces.
    pub async fn pump(&self) -> Result<(), String> {
        for _round in 0..64 {
            let now = self.now();
            let mut task = AgentTaskEntityStore::new(
                task_scope(),
                self.tasks.clone(),
                self.agents.clone(),
                self.history.clone(),
            );
            task = task.with_wake_timers(self.rewake_parker.clone());
            if let Some(metrics) = &self.metrics {
                task = task.with_metrics(metrics.clone());
            }
            task.recover(now)
                .await
                .map_err(|error| error.code().to_string())?;
            task.settle_side_effects(&self.router, now)
                .await
                .map_err(|error| error.code().to_string())?;

            let now = self.now();
            let mut run = self.run();
            run.recover(now)
                .await
                .map_err(|error| error.code().to_string())?;
            let progress = run
                .settle_side_effects(&self.router, now)
                .await
                .map_err(|error| error.code().to_string())?;
            let answered = self
                .dispatcher
                .drive(&mut run, &self.router, self.now())
                .await
                .map_err(|error| error.code().to_string())?;

            let terminal = run
                .state()
                .ok()
                .and_then(|state| state.status())
                .is_some_and(AgentRunStatus::is_terminal);
            if terminal {
                return Ok(());
            }
            if progress.transitions == 0
                && progress.effects_dispatched == 0
                && progress.settled == 0
                && progress.failed == 0
                && answered == 0
            {
                return Ok(());
            }
        }
        Err("the loop did not quiesce".to_string())
    }

    pub async fn run_snapshot(&self) -> Option<AgentRunSnapshot> {
        let mut run = self.run();
        let now = self.now();
        run.recover(now).await.expect("the run should recover");
        run.snapshot().expect("the snapshot should read")
    }

    pub async fn task_snapshot(&self) -> AgentTaskSnapshot {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        task = task.with_wake_timers(self.rewake_parker.clone());
        if let Some(metrics) = &self.metrics {
            task = task.with_metrics(metrics.clone());
        }
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        task.snapshot()
            .expect("the snapshot should read")
            .expect("the task exists")
    }
}

impl<A: AgentModelAdapter> Fixture<A, InMemoryAgentRunEffectSink> {
    pub fn dispatched_effects(&self) -> usize {
        self.effects.len(&run_scope())
    }
}

/// The whole sharded world: real agent, task, and run entity types registered
/// on one node's `ClusterSharding`, exchanging through the testkit's
/// `LocalShardedExchangeRoute` — the production sharded route's own local
/// arm, so the durable path is production's minus only the TCP transport.
///
/// Every durable store is a crash-armable pass-through, so a sharded test can
/// inject owner kills exactly as the direct-drive fixtures do. The world is
/// deliberately scope-free: a test derives its entity refs from its own
/// scopes via [`Self::agent_ref`], [`Self::task_ref`], and [`Self::run_ref`].
pub struct ShardedWorld {
    /// The actor system that owns every resident entity.
    pub system: rakka_core::ActorSystem,
    /// The sharding fabric all three entity types are registered on.
    pub sharding: rakka_sharding::ClusterSharding,
    /// Durable agent-entity store.
    pub agents: AgentStore,
    /// Durable task-entity store.
    pub tasks: TaskStore,
    /// Durable run-entity store.
    pub runs: RunStore,
    /// Durable team-entity store.
    pub teams: TeamStore,
    /// The team history sink the sharded team entities flush to.
    pub team_history: rakka_agent::InMemoryAgentTeamHistoryStore,
    /// Durable conversation-entity store.
    pub conversations: ConversationStore,
    /// The conversation history sink the sharded conversation entities
    /// flush to.
    pub conversation_history: rakka_agent::InMemoryAgentConversationHistoryStore,
    /// The scripted model/tool answers a test drives ready effects with.
    pub dispatcher: ScriptedDispatcher,
    /// The agent entity type's sharding registration.
    pub agent_registration: rakka_agent::AgentEntityRegistration,
    /// The task entity type's sharding registration.
    pub task_registration: rakka_agent::AgentTaskEntityRegistration,
    /// The run entity type's sharding registration.
    pub run_registration: rakka_agent::AgentRunEntityRegistration,
    /// The team entity type's sharding registration.
    pub team_registration: rakka_agent::AgentTeamEntityRegistration,
    /// The conversation entity type's sharding registration.
    pub conversation_registration: rakka_agent::AgentConversationEntityRegistration,
}

impl ShardedWorld {
    /// The ask timeout of the local sharded exchange routes.
    pub const ASK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// Wires the three sharded entity types over fresh stores.
    ///
    /// `idle` is every type's idle-passivation window; `policies`, when
    /// given, become the run entity's effect policies.
    pub fn new(
        name: &str,
        idle: std::time::Duration,
        dispatcher: ScriptedDispatcher,
        policies: Option<AgentEffectPolicies>,
    ) -> Self {
        use rakka_agent::testkit::LocalShardedExchangeRoute;
        use rakka_agent::{
            agent_conversation_entity_type_key, agent_entity_type_key, agent_run_entity_type_key,
            agent_task_entity_type_key, agent_team_entity_type_key,
            init_agent_conversation_entity_sharding, init_agent_entity_sharding,
            init_agent_run_entity_sharding, init_agent_task_entity_sharding,
            init_agent_team_entity_sharding, AgentConversationEntityMessage,
            AgentConversationEntityShardingSettings, AgentEntityShardingSettings,
            AgentRunEntityMessage, AgentRunEntityShardingSettings, AgentTaskEntityMessage,
            AgentTaskEntityShardingSettings, AgentTeamEntityMessage,
            AgentTeamEntityShardingSettings,
        };

        let system = rakka_core::ActorSystem::new(name);
        let sharding = rakka_sharding::ClusterSharding::get(&system);
        let agents = AgentStore::new();
        let tasks = TaskStore::new();
        let runs = RunStore::new();
        let teams = TeamStore::new();
        let team_history = rakka_agent::InMemoryAgentTeamHistoryStore::new();
        let conversations = ConversationStore::new();
        let conversation_history = rakka_agent::InMemoryAgentConversationHistoryStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let effects = InMemoryAgentRunEffectSink::new();
        let clock = Arc::new(AtomicU64::new(1));

        // The routes need the registrations and the registrations need the
        // router: the deferred router is that late binding and nothing more.
        let deferred = DeferredExchangeRouter::new();
        let entity_clock = {
            let clock = clock.clone();
            Arc::new(move || AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst)))
        };

        let agent_registration = init_agent_entity_sharding(
            &sharding,
            agents.clone(),
            AgentEntityShardingSettings::new(agent_entity_type_key()).with_idle_passivation(idle),
        )
        .expect("agent entity sharding initializes");
        let task_registration = init_agent_task_entity_sharding(
            &sharding,
            tasks.clone(),
            agents.clone(),
            history,
            deferred.as_router(),
            AgentTaskEntityShardingSettings::new(agent_task_entity_type_key())
                .with_idle_passivation(idle)
                .with_clock(entity_clock.clone()),
        )
        .expect("task entity sharding initializes");
        let mut run_settings = AgentRunEntityShardingSettings::new(agent_run_entity_type_key())
            .with_idle_passivation(idle)
            .with_clock(entity_clock);
        if let Some(policies) = policies {
            run_settings = run_settings.with_effect_policies(policies);
        }
        let run_registration = init_agent_run_entity_sharding(
            &sharding,
            runs.clone(),
            effects,
            deferred.as_router(),
            run_settings,
        )
        .expect("run entity sharding initializes");
        let team_registration = init_agent_team_entity_sharding(
            &sharding,
            teams.clone(),
            team_history.clone(),
            deferred.as_router(),
            AgentTeamEntityShardingSettings::new(agent_team_entity_type_key())
                .with_idle_passivation(idle)
                .with_clock({
                    let clock = clock.clone();
                    Arc::new(move || {
                        AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst))
                    })
                }),
        )
        .expect("team entity sharding initializes");
        let conversation_registration = init_agent_conversation_entity_sharding(
            &sharding,
            conversations.clone(),
            conversation_history.clone(),
            deferred.as_router(),
            AgentConversationEntityShardingSettings::new(agent_conversation_entity_type_key())
                .with_idle_passivation(idle)
                .with_clock({
                    let clock = clock.clone();
                    Arc::new(move || {
                        AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst))
                    })
                }),
        )
        .expect("conversation entity sharding initializes");

        let router = AgentExchangeRouter::new()
            .with_route(
                AgentEntityClass::Task,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    task_registration.key().clone(),
                    Self::ASK_TIMEOUT,
                    |envelope, reply_to| AgentTaskEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            )
            .with_route(
                AgentEntityClass::Run,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    run_registration.key().clone(),
                    Self::ASK_TIMEOUT,
                    |envelope, reply_to| AgentRunEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            )
            .with_route(
                AgentEntityClass::Team,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    team_registration.key().clone(),
                    Self::ASK_TIMEOUT,
                    |envelope, reply_to| AgentTeamEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            )
            .with_route(
                AgentEntityClass::Conversation,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    conversation_registration.key().clone(),
                    Self::ASK_TIMEOUT,
                    |envelope, reply_to| AgentConversationEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            );
        deferred.install(router);

        Self {
            system,
            sharding,
            agents,
            tasks,
            runs,
            teams,
            team_history,
            conversations,
            conversation_history,
            dispatcher,
            agent_registration,
            task_registration,
            run_registration,
            team_registration,
            conversation_registration,
        }
    }

    /// The sharded ref for one agent scope.
    pub fn agent_ref(&self, scope: &AgentScope) -> rakka_agent::AgentEntityRef {
        rakka_agent::registered_agent_entity_ref(&self.agent_registration, scope)
    }

    /// The sharded ref for one task scope.
    pub fn task_ref(&self, scope: &AgentTaskScope) -> rakka_agent::AgentTaskEntityRef {
        rakka_agent::registered_agent_task_entity_ref(&self.task_registration, scope)
    }

    /// The sharded ref for one run scope.
    pub fn run_ref(&self, scope: &AgentRunScope) -> rakka_agent::AgentRunEntityRef {
        rakka_agent::registered_agent_run_entity_ref(&self.run_registration, scope)
    }

    /// The sharded ref for one team scope.
    pub fn team_ref(&self, scope: &rakka_agent::AgentTeamScope) -> rakka_agent::AgentTeamEntityRef {
        rakka_agent::registered_agent_team_entity_ref(&self.team_registration, scope)
    }

    /// The sharded ref for one conversation scope.
    pub fn conversation_ref(
        &self,
        scope: &rakka_agent::AgentConversationScope,
    ) -> rakka_agent::AgentConversationEntityRef {
        rakka_agent::registered_agent_conversation_entity_ref(
            &self.conversation_registration,
            scope,
        )
    }

    /// How many entity actors of any class are resident on this node.
    pub fn resident_entities(&self) -> usize {
        let agent = self
            .sharding
            .registration_state(self.agent_registration.key())
            .expect("the agent registration exists")
            .local_entity_count();
        let task = self
            .sharding
            .registration_state(self.task_registration.key())
            .expect("the task registration exists")
            .local_entity_count();
        let run = self
            .sharding
            .registration_state(self.run_registration.key())
            .expect("the run registration exists")
            .local_entity_count();
        let team = self
            .sharding
            .registration_state(self.team_registration.key())
            .expect("the team registration exists")
            .local_entity_count();
        let conversation = self
            .sharding
            .registration_state(self.conversation_registration.key())
            .expect("the conversation registration exists")
            .local_entity_count();
        agent + task + run + team + conversation
    }
}
