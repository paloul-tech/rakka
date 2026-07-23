//! Agent definition, settings revisions, and run setup envelopes.
//!
//! Owns [`AgentDefinitionRevision`], [`SettingsRevision`] with its three timing
//! classes — turn-bound, immediate safety, and run-pinned — and
//! [`AgentSetupRevision`], whose envelope may only narrow what the definition
//! already permits.
//!
//! Specification: sections 7.1 through 7.3. Dispatch-time envelope enforcement
//! lands with the tool authority layers in slice 1.8; this module is where a
//! widening setup is rejected in the first place.
//!
//! # The authority envelope
//!
//! Section 7.3 states six things a setup or later settings revision must never
//! do: introduce an undeclared tool, weaken a mandatory guardrail, choose an
//! unapproved model, widen credential or knowledge access, add an unauthorized
//! peer, or downgrade effect safety. Rather than scatter those checks, the
//! definition publishes one [`AgentAuthorityEnvelope`], and a setup publishes
//! another that must be a *narrowing* of it. Each rule becomes a dimension of
//! [`AgentEnvelopeDimension`] with a stable reason code, and
//! [`AgentAuthorityEnvelope::validate_narrowing`] is the single place that
//! decides whether one envelope is contained by another.
//!
//! Guardrails and budgets invert the containment direction, which is the whole
//! point of keeping them in the same check: a narrowing setup may *add* a
//! mandatory guardrail but never drop one, and may *lower* a budget ceiling but
//! never raise it or replace a bounded ceiling with an unbounded one.
//!
//! A tool's execution-policy routing is pinned outright. The reference is
//! opaque to Rakka, so two policies cannot be ordered by strictness, and
//! neither substituting another policy nor dropping the declared one can be
//! proven narrower — a narrowing envelope must keep the routing the definition
//! declared.
//!
//! # Settings timing
//!
//! A settings update is a set of field-level changes, and each change declares
//! when it applies ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
//! Turn-bound changes take effect at the next model turn, immediate-safety
//! changes before any further dispatch, and run-pinned changes only for a new run
//! or an explicit migration. [`effective_settings_for_turn`] is the resolution
//! that makes that concrete: a run pins the revision it started with, and each
//! turn reads the current revision for everything except the run-pinned fields.
//!
//! Note also what the [`AgentSettingsChange`] enum makes *unrepresentable*: there
//! is no change that adds a tool, a credential binding, a knowledge space, or a
//! peer. Settings can only revoke. Widening authority requires a new definition
//! revision, which is re-admitted from scratch.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, ArtifactRef, PrincipalRef,
    StateSchemaVersion,
};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::identity::{
    validated_id, AgentEnvironmentRef, AgentId, AgentIdentityError, KnowledgeSpaceId,
};
use crate::schema::{
    AgentRecordKind, VersionedAgentRecord, CURRENT_AGENT_DEFINITION_SCHEMA_VERSION,
    CURRENT_AGENT_SETTINGS_SCHEMA_VERSION, CURRENT_AGENT_SETUP_SCHEMA_VERSION,
};

/// Maximum length, in bytes, of the mandatory agent description.
///
/// The description is bounded because it is the single source used for A2A
/// discovery, specialist selection, model-facing purpose, and documentation. It
/// is user-provided display text and never becomes a metric label.
pub const AGENT_DESCRIPTION_MAX_LENGTH: usize = 1024;

/// Maximum number of field-level changes one settings revision may carry.
pub const AGENT_SETTINGS_MAX_CHANGES: usize = 32;

/// Result type for definition, settings, and setup construction.
pub type AgentDefinitionResult<T> = Result<T, AgentDefinitionError>;

validated_id! {
    /// Stable identity of one published agent definition.
    pub AgentDefinitionId, "agent_definition_id"
}

validated_id! {
    /// Identity of one registered tool the definition declares.
    ///
    /// The tool *descriptor* — schema, capabilities, safety declaration — is the
    /// registry surface of slice 1.8. The definition only needs the identity and
    /// the declaration it authorizes, which is what [`AgentToolDeclaration`]
    /// carries.
    pub AgentToolId, "agent_tool_id"
}

validated_id! {
    /// Identity of one scoped capability a tool may exercise.
    pub AgentCapabilityId, "agent_capability_id"
}

validated_id! {
    /// Identity of one approved model or provider profile.
    pub AgentModelProfileId, "agent_model_profile_id"
}

validated_id! {
    /// Identity of one compiled workflow that may be invoked as a tool.
    pub AgentWorkflowToolId, "agent_workflow_tool_id"
}

validated_id! {
    /// Identity of one typed task definition an agent accepts.
    pub AgentTaskDefinitionId, "agent_task_definition_id"
}

validated_id! {
    /// Identity of one ordered guardrail stage.
    pub AgentGuardrailStageId, "agent_guardrail_stage_id"
}

validated_id! {
    /// Logical reference to an application-owned credential binding.
    ///
    /// It names *which* credential a dispatch may resolve; it never contains
    /// credential material. Resolution happens once, inside the bounded dispatcher
    /// attempt, and the resolved value is never persisted.
    pub AgentCredentialBindingRef, "agent_credential_binding_ref"
}

validated_id! {
    /// Reference to an application-owned execution policy or trust class used to
    /// route tool dispatch.
    pub AgentExecutionPolicyRef, "agent_execution_policy_ref"
}

validated_id! {
    /// Reference to an application-owned approval, authorization, escalation, or
    /// guardrail policy.
    pub AgentPolicyRef, "agent_policy_ref"
}

/// Monotonic revision counter carried by definition, settings, and setup records
/// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentRevisionNumber(u64);

impl AgentRevisionNumber {
    /// First revision of any versioned record.
    pub const INITIAL: Self = Self(1);

    /// Creates a revision number.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw revision number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next revision.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl Display for AgentRevisionNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Who accepted a revision, when, and where the immutable audit record lives
/// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRevisionProvenance {
    /// Authenticated principal that accepted the revision.
    pub principal: PrincipalRef,
    /// Time the revision was accepted.
    pub accepted_at: AgentTimestampMillis,
    /// Command or event that caused the revision.
    pub causation_id: AgentCausationId,
    /// Immutable audit reference for the revision.
    pub audit_ref: AgentAuditEventId,
}

/// Effect-safety class a tool declares
/// ([specification 11.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Only the class discriminant lives here. The full effect-safety record, with
/// its external idempotency key and reconciliation protocol reference, is part of
/// the effect model in slice 1.7. The class is needed this early because setup
/// must not be able to *downgrade* it: re-labelling a non-idempotent tool as
/// idempotent would make an unsafe retry look legal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEffectSafetyClass {
    /// The effect does not change external state.
    ReadOnly,
    /// Re-invocation with the same external idempotency key is safe.
    Idempotent,
    /// The outcome of an ambiguous attempt can be established by a
    /// reconciliation protocol.
    Reconcileable,
    /// An ambiguous attempt cannot be safely retried or reconciled.
    NonIdempotent,
}

impl AgentEffectSafetyClass {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Idempotent => "idempotent",
            Self::Reconcileable => "reconcileable",
            Self::NonIdempotent => "non-idempotent",
        }
    }

    /// How much protection this class demands, ascending.
    ///
    /// A higher rank means a stricter dispatch path: more gating, less automatic
    /// retry. A narrowing setup may raise a tool's rank but never lower it.
    #[must_use]
    pub const fn strictness(self) -> u8 {
        match self {
            Self::ReadOnly => 0,
            Self::Idempotent => 1,
            Self::Reconcileable => 2,
            Self::NonIdempotent => 3,
        }
    }
}

impl Display for AgentEffectSafetyClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Autonomy operation class an agent may run under
/// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentOperationClass {
    /// Attended execution with a human in the loop of the session.
    Interactive,
    /// Unattended execution bounded by an explicit completion condition.
    BoundedAsync,
    /// Unattended execution admitted from recurring durable wake occurrences.
    Continuous,
}

impl AgentOperationClass {
    /// Whether the class runs unattended, and so must be admitted before it may
    /// run at all ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// This is the line the fail-closed rule is drawn on, and it is drawn here
    /// rather than at each enforcement point so that adding a class cannot
    /// quietly add an unattended one that nothing gates.
    ///
    /// Attended execution is not autonomy: a human is in the loop of the
    /// session, which is what [`Self::Interactive`] means, so it needs neither
    /// an admission decision nor an autonomy declaration in the definition's
    /// envelope. Everything else does, and does not run without both.
    #[must_use]
    pub const fn is_unattended(self) -> bool {
        !matches!(self, Self::Interactive)
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::BoundedAsync => "bounded-async",
            Self::Continuous => "continuous",
        }
    }
}

impl Display for AgentOperationClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Coordination capability an agent definition may authorize
/// ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// The capability *descriptors* are M5 surface. The envelope needs the kinds now
/// so a definition can bound them and a setup cannot add one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCoordinationCapabilityKind {
    /// Transfer of one task to another agent, preserving the task identity.
    Handoff,
    /// Assignment of a child task to a specialist agent.
    Delegation,
    /// Membership of a team with a shared durable task board.
    Team,
    /// Participation in a moderated multi-agent conversation.
    Moderation,
}

impl AgentCoordinationCapabilityKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Handoff => "handoff",
            Self::Delegation => "delegation",
            Self::Team => "team",
            Self::Moderation => "moderation",
        }
    }
}

impl Display for AgentCoordinationCapabilityKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// What a definition authorizes one tool to do.
///
/// The capability set and safety class are trusted definition data. Model output
/// can never produce or widen either.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentToolDeclaration {
    /// Declared effect-safety class of the tool.
    pub safety: AgentEffectSafetyClass,
    /// Scoped capabilities the tool may exercise.
    pub capabilities: BTreeSet<AgentCapabilityId>,
    /// Logical credential binding the tool's dispatch may resolve.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// Execution policy or trust class the tool's dispatch is routed through.
    ///
    /// Pinned under narrowing: the reference is opaque, so a substitute — or
    /// its absence — cannot be proven stricter, and a setup must keep the
    /// routing the definition declared.
    pub execution_policy: Option<AgentExecutionPolicyRef>,
}

impl AgentToolDeclaration {
    /// Declares a tool with a safety class and no capabilities.
    #[must_use]
    pub fn new(safety: AgentEffectSafetyClass) -> Self {
        Self {
            safety,
            capabilities: BTreeSet::new(),
            credential_binding: None,
            execution_policy: None,
        }
    }

    /// Adds a scoped capability.
    #[must_use]
    pub fn with_capability(mut self, capability: AgentCapabilityId) -> Self {
        self.capabilities.insert(capability);
        self
    }

    /// Binds the tool to a logical credential reference.
    #[must_use]
    pub fn with_credential_binding(mut self, binding: AgentCredentialBindingRef) -> Self {
        self.credential_binding = Some(binding);
        self
    }

    /// Routes the tool through an execution policy or trust class.
    #[must_use]
    pub fn with_execution_policy(mut self, policy: AgentExecutionPolicyRef) -> Self {
        self.execution_policy = Some(policy);
        self
    }
}

/// Hard ceilings a definition sets and a setup may only lower
/// ([specification 7.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// `None` means unbounded. A narrowing envelope may bound a dimension the
/// definition left unbounded, but it may never unbound a dimension the definition
/// bounded, and it may never raise a ceiling.
///
/// Cost is denominated in integer micro-units of the tenant's currency so a
/// durable ceiling never depends on floating-point rounding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetCeilings {
    /// Maximum durable loop iterations.
    pub max_loop_iterations: Option<u32>,
    /// Maximum model calls.
    pub max_model_calls: Option<u32>,
    /// Maximum tool calls.
    pub max_tool_calls: Option<u32>,
    /// Maximum durable effects.
    pub max_effects: Option<u32>,
    /// Maximum external dispatch attempts.
    ///
    /// An effect and its attempts are two ceilings, not one: an effect the
    /// dispatch layer retries costs one effect and several attempts, and an
    /// attempt that reached durable `Started` counts even when its outcome
    /// became `Indeterminate`
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    pub max_effect_attempts: Option<u32>,
    /// Maximum model tokens.
    pub max_tokens: Option<u64>,
    /// Maximum cost, in micro-units of currency.
    pub max_cost_micros: Option<u64>,
    /// Maximum wall-clock duration, in milliseconds.
    pub max_wall_clock_millis: Option<u64>,
    /// Maximum concurrently dispatched effects.
    pub max_concurrent_effects: Option<u32>,
}

impl AgentBudgetCeilings {
    /// Unbounded ceilings.
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            max_loop_iterations: None,
            max_model_calls: None,
            max_tool_calls: None,
            max_effects: None,
            max_effect_attempts: None,
            max_tokens: None,
            max_cost_micros: None,
            max_wall_clock_millis: None,
            max_concurrent_effects: None,
        }
    }

    fn narrowing_violations(&self, candidate: &Self) -> Vec<AgentEnvelopeViolation> {
        let mut violations = Vec::new();
        let dimensions: [(&str, Option<u64>, Option<u64>); 9] = [
            (
                "max_loop_iterations",
                self.max_loop_iterations.map(u64::from),
                candidate.max_loop_iterations.map(u64::from),
            ),
            (
                "max_model_calls",
                self.max_model_calls.map(u64::from),
                candidate.max_model_calls.map(u64::from),
            ),
            (
                "max_tool_calls",
                self.max_tool_calls.map(u64::from),
                candidate.max_tool_calls.map(u64::from),
            ),
            (
                "max_effects",
                self.max_effects.map(u64::from),
                candidate.max_effects.map(u64::from),
            ),
            (
                "max_effect_attempts",
                self.max_effect_attempts.map(u64::from),
                candidate.max_effect_attempts.map(u64::from),
            ),
            ("max_tokens", self.max_tokens, candidate.max_tokens),
            (
                "max_cost_micros",
                self.max_cost_micros,
                candidate.max_cost_micros,
            ),
            (
                "max_wall_clock_millis",
                self.max_wall_clock_millis,
                candidate.max_wall_clock_millis,
            ),
            (
                "max_concurrent_effects",
                self.max_concurrent_effects.map(u64::from),
                candidate.max_concurrent_effects.map(u64::from),
            ),
        ];

        for (name, ceiling, proposed) in dimensions {
            let Some(ceiling) = ceiling else {
                continue;
            };
            match proposed {
                None => violations.push(AgentEnvelopeViolation::new(
                    AgentEnvelopeDimension::Budget,
                    format!(
                        "{name} is bounded at {ceiling} but the narrowed envelope is unbounded"
                    ),
                )),
                Some(proposed) if proposed > ceiling => {
                    violations.push(AgentEnvelopeViolation::new(
                        AgentEnvelopeDimension::Budget,
                        format!("{name} {proposed} exceeds the ceiling {ceiling}"),
                    ));
                }
                Some(_) => {}
            }
        }
        violations
    }
}

/// Dimension of authority a narrowing check can reject.
///
/// Each variant maps to one clause of
/// [specification 7.3](../../../docs/plans/rakka-agent/spec.md) and carries a
/// stable reason code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEnvelopeDimension {
    /// A tool the definition never declared.
    Tool,
    /// A capability the definition never granted to that tool.
    ToolCapability,
    /// A safety class weaker than the one the definition declared.
    EffectSafety,
    /// A credential binding the definition never authorized for that tool.
    ToolCredentialBinding,
    /// An execution policy differing from the one the definition routes that
    /// tool's dispatch through.
    ToolExecutionPolicy,
    /// A model or provider profile the definition never approved.
    ModelProfile,
    /// A workflow tool the definition never allowed.
    WorkflowTool,
    /// A typed task definition the definition never accepted.
    TaskDefinition,
    /// A peer agent the definition never authorized as a collaborator.
    Collaborator,
    /// A knowledge space the definition never granted access to.
    KnowledgeSpace,
    /// A shared environment the definition never granted access to.
    Environment,
    /// A credential binding the definition never authorized.
    CredentialBinding,
    /// A coordination capability the definition never authorized.
    CoordinationCapability,
    /// An autonomy operation class the definition never admitted.
    OperationClass,
    /// A mandatory guardrail stage the definition requires but the narrowed
    /// envelope dropped.
    MandatoryGuardrail,
    /// A budget ceiling raised or unbounded beyond the definition's.
    Budget,
}

impl AgentEnvelopeDimension {
    /// Stable kebab-case reason code.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Tool => "undeclared-tool",
            Self::ToolCapability => "widened-tool-capability",
            Self::EffectSafety => "downgraded-effect-safety",
            Self::ToolCredentialBinding => "widened-tool-credential-binding",
            Self::ToolExecutionPolicy => "rerouted-tool-execution-policy",
            Self::ModelProfile => "unapproved-model-profile",
            Self::WorkflowTool => "undeclared-workflow-tool",
            Self::TaskDefinition => "unaccepted-task-definition",
            Self::Collaborator => "unauthorized-collaborator",
            Self::KnowledgeSpace => "widened-knowledge-access",
            Self::Environment => "widened-environment-access",
            Self::CredentialBinding => "widened-credential-access",
            Self::CoordinationCapability => "unauthorized-coordination-capability",
            Self::OperationClass => "unadmitted-operation-class",
            Self::MandatoryGuardrail => "weakened-mandatory-guardrail",
            Self::Budget => "widened-budget",
        }
    }
}

impl Display for AgentEnvelopeDimension {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One way a candidate envelope widened the authority it was supposed to narrow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEnvelopeViolation {
    /// Dimension of authority that was widened.
    pub dimension: AgentEnvelopeDimension,
    /// Bounded detail naming the offending value.
    pub detail: String,
}

impl AgentEnvelopeViolation {
    /// Records a widening violation.
    #[must_use]
    pub fn new(dimension: AgentEnvelopeDimension, detail: impl Into<String>) -> Self {
        Self {
            dimension,
            detail: detail.into(),
        }
    }

    /// Stable kebab-case reason code for this violation.
    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        self.dimension.as_label()
    }
}

impl Display for AgentEnvelopeViolation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.dimension.as_label(), self.detail)
    }
}

/// The complete authority a definition grants, or that a setup narrows it to
/// ([specification 7.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAuthorityEnvelope {
    /// Declared tools and what each is authorized to do.
    pub tools: BTreeMap<AgentToolId, AgentToolDeclaration>,
    /// Approved model and provider profiles.
    pub model_profiles: BTreeSet<AgentModelProfileId>,
    /// Compiled workflows invocable as tools.
    pub workflow_tools: BTreeSet<AgentWorkflowToolId>,
    /// Typed task definitions the agent accepts.
    pub task_definitions: BTreeSet<AgentTaskDefinitionId>,
    /// Peer agents the agent may collaborate with.
    pub collaborators: BTreeSet<AgentId>,
    /// Knowledge spaces the agent may read or contribute to.
    pub knowledge_spaces: BTreeSet<KnowledgeSpaceId>,
    /// Shared environments the agent may reach through declared tools.
    pub environments: BTreeSet<AgentEnvironmentRef>,
    /// Logical credential bindings the agent's dispatches may resolve.
    pub credential_bindings: BTreeSet<AgentCredentialBindingRef>,
    /// Coordination capabilities the agent may exercise.
    pub coordination_capabilities: BTreeSet<AgentCoordinationCapabilityKind>,
    /// Autonomy operation classes the agent is admitted for.
    pub operation_classes: BTreeSet<AgentOperationClass>,
    /// Guardrail stages that must run and that no setup or settings update may
    /// remove.
    pub mandatory_guardrails: BTreeSet<AgentGuardrailStageId>,
    /// Hard budget ceilings.
    pub budgets: AgentBudgetCeilings,
}

impl AgentAuthorityEnvelope {
    /// Creates an envelope granting nothing.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Every way `candidate` widens the authority this envelope grants.
    ///
    /// An empty result means `candidate` is a legal narrowing of `self`. All
    /// violations are collected rather than short-circuited: an operator fixing a
    /// rejected setup should see every problem at once.
    #[must_use]
    pub fn narrowing_violations(&self, candidate: &Self) -> Vec<AgentEnvelopeViolation> {
        let mut violations = Vec::new();

        for (tool, declaration) in &candidate.tools {
            let Some(declared) = self.tools.get(tool) else {
                violations.push(AgentEnvelopeViolation::new(
                    AgentEnvelopeDimension::Tool,
                    format!("tool {tool} is not declared by the definition"),
                ));
                continue;
            };

            for capability in declaration.capabilities.difference(&declared.capabilities) {
                violations.push(AgentEnvelopeViolation::new(
                    AgentEnvelopeDimension::ToolCapability,
                    format!("tool {tool} is not granted the capability {capability}"),
                ));
            }

            if declaration.safety.strictness() < declared.safety.strictness() {
                violations.push(AgentEnvelopeViolation::new(
                    AgentEnvelopeDimension::EffectSafety,
                    format!(
                        "tool {tool} is declared {} but the narrowed envelope claims {}",
                        declared.safety, declaration.safety
                    ),
                ));
            }

            if let Some(binding) = &declaration.credential_binding {
                if declared.credential_binding.as_ref() != Some(binding) {
                    violations.push(AgentEnvelopeViolation::new(
                        AgentEnvelopeDimension::ToolCredentialBinding,
                        format!("tool {tool} is not bound to the credential {binding}"),
                    ));
                }
            }

            // The execution policy is an opaque application reference, so the
            // check cannot rank a substitute — or the policy's absence — as
            // stricter or weaker. The only safe narrowing keeps the declared
            // routing exactly.
            if declaration.execution_policy != declared.execution_policy {
                violations.push(AgentEnvelopeViolation::new(
                    AgentEnvelopeDimension::ToolExecutionPolicy,
                    format!(
                        "tool {tool} is routed through {} and the narrowed envelope may not reroute it through {}",
                        execution_policy_label(declared.execution_policy.as_ref()),
                        execution_policy_label(declaration.execution_policy.as_ref()),
                    ),
                ));
            }
        }

        collect_subset_violations(
            &self.model_profiles,
            &candidate.model_profiles,
            AgentEnvelopeDimension::ModelProfile,
            "model profile",
            &mut violations,
        );
        collect_subset_violations(
            &self.workflow_tools,
            &candidate.workflow_tools,
            AgentEnvelopeDimension::WorkflowTool,
            "workflow tool",
            &mut violations,
        );
        collect_subset_violations(
            &self.task_definitions,
            &candidate.task_definitions,
            AgentEnvelopeDimension::TaskDefinition,
            "task definition",
            &mut violations,
        );
        collect_subset_violations(
            &self.collaborators,
            &candidate.collaborators,
            AgentEnvelopeDimension::Collaborator,
            "collaborator",
            &mut violations,
        );
        collect_subset_violations(
            &self.knowledge_spaces,
            &candidate.knowledge_spaces,
            AgentEnvelopeDimension::KnowledgeSpace,
            "knowledge space",
            &mut violations,
        );
        collect_subset_violations(
            &self.environments,
            &candidate.environments,
            AgentEnvelopeDimension::Environment,
            "environment",
            &mut violations,
        );
        collect_subset_violations(
            &self.credential_bindings,
            &candidate.credential_bindings,
            AgentEnvelopeDimension::CredentialBinding,
            "credential binding",
            &mut violations,
        );
        collect_subset_violations(
            &self.coordination_capabilities,
            &candidate.coordination_capabilities,
            AgentEnvelopeDimension::CoordinationCapability,
            "coordination capability",
            &mut violations,
        );
        collect_subset_violations(
            &self.operation_classes,
            &candidate.operation_classes,
            AgentEnvelopeDimension::OperationClass,
            "operation class",
            &mut violations,
        );

        // Guardrails invert: the narrowed envelope must keep every mandatory
        // stage and may add more.
        for stage in self
            .mandatory_guardrails
            .difference(&candidate.mandatory_guardrails)
        {
            violations.push(AgentEnvelopeViolation::new(
                AgentEnvelopeDimension::MandatoryGuardrail,
                format!("mandatory guardrail stage {stage} was dropped"),
            ));
        }

        violations.extend(self.budgets.narrowing_violations(&candidate.budgets));
        violations
    }

    /// Accepts `candidate` as a narrowing of this envelope, or fails closed.
    pub fn validate_narrowing(&self, candidate: &Self) -> AgentDefinitionResult<()> {
        let violations = self.narrowing_violations(candidate);
        if violations.is_empty() {
            Ok(())
        } else {
            Err(AgentDefinitionError::EnvelopeWidened { violations })
        }
    }
}

fn execution_policy_label(policy: Option<&AgentExecutionPolicyRef>) -> String {
    policy.map_or_else(
        || "no execution policy".to_string(),
        |policy| format!("execution policy {policy}"),
    )
}

fn collect_subset_violations<T>(
    granted: &BTreeSet<T>,
    candidate: &BTreeSet<T>,
    dimension: AgentEnvelopeDimension,
    noun: &str,
    violations: &mut Vec<AgentEnvelopeViolation>,
) where
    T: Ord + Display,
{
    for value in candidate.difference(granted) {
        violations.push(AgentEnvelopeViolation::new(
            dimension,
            format!("{noun} {value} is not granted by the definition"),
        ));
    }
}

/// References to the application-owned policies an agent runs under.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPolicyRefs {
    /// Business approval policy for consequential operations.
    pub approval: Option<AgentPolicyRef>,
    /// Security-authorization policy.
    pub authorization: Option<AgentPolicyRef>,
    /// Escalation policy for expired or rejected checkpoints.
    pub escalation: Option<AgentPolicyRef>,
    /// Guardrail policy revision applied at the model, tool, A2A, and memory
    /// boundaries.
    pub guardrail: Option<AgentPolicyRef>,
    /// Retention and classification policy.
    pub retention: Option<AgentPolicyRef>,
}

/// The content of one agent definition
/// ([specification 7.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The fields are public so a definition can be assembled directly, which
/// means construction alone cannot guarantee the bounded invariants.
/// [`AgentDefinition::validate`] therefore runs at three points: inside
/// [`AgentDefinition::new`], on deserialization — so an out-of-bounds
/// definition can neither arrive over the wire nor load from a durable
/// record — and again at the agent entity's accept path before anything is
/// persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentDefinition {
    /// Stable definition identity.
    pub definition_id: AgentDefinitionId,
    /// Mandatory bounded, outcome-oriented description.
    ///
    /// This is the single source used for A2A discovery, specialist selection,
    /// model-facing purpose, documentation, and observability class.
    pub description: String,
    /// System instructions, held out of line as an immutable artifact.
    pub instructions: Option<ArtifactRef>,
    /// Everything the definition authorizes.
    pub envelope: AgentAuthorityEnvelope,
    /// Application-owned policies the agent runs under.
    pub policies: AgentPolicyRefs,
}

impl AgentDefinition {
    /// Creates a definition, rejecting a missing or oversized description.
    pub fn new(
        definition_id: AgentDefinitionId,
        description: impl Into<String>,
        envelope: AgentAuthorityEnvelope,
    ) -> AgentDefinitionResult<Self> {
        let definition = Self {
            definition_id,
            description: description.into(),
            instructions: None,
            envelope,
            policies: AgentPolicyRefs::default(),
        };
        definition.validate()?;
        Ok(definition)
    }

    /// Accepts the definition's bounded invariants, or fails closed.
    ///
    /// This is the single check behind construction, deserialization, and the
    /// agent entity's accept path, so a definition assembled field by field is
    /// held to the same bounds as one built through [`AgentDefinition::new`].
    pub fn validate(&self) -> AgentDefinitionResult<()> {
        if self.description.is_empty() {
            return Err(AgentDefinitionError::EmptyDescription);
        }
        if self.description.len() > AGENT_DESCRIPTION_MAX_LENGTH {
            return Err(AgentDefinitionError::DescriptionTooLong {
                length: self.description.len(),
                maximum: AGENT_DESCRIPTION_MAX_LENGTH,
            });
        }
        Ok(())
    }

    /// Points the definition at its out-of-line system instructions.
    #[must_use]
    pub fn with_instructions(mut self, instructions: ArtifactRef) -> Self {
        self.instructions = Some(instructions);
        self
    }

    /// Sets the application-owned policy references.
    #[must_use]
    pub fn with_policies(mut self, policies: AgentPolicyRefs) -> Self {
        self.policies = policies;
        self
    }
}

impl<'de> Deserialize<'de> for AgentDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            definition_id: AgentDefinitionId,
            description: String,
            instructions: Option<ArtifactRef>,
            envelope: AgentAuthorityEnvelope,
            policies: AgentPolicyRefs,
        }

        let record = Record::deserialize(deserializer)?;
        let definition = Self {
            definition_id: record.definition_id,
            description: record.description,
            instructions: record.instructions,
            envelope: record.envelope,
            policies: record.policies,
        };
        definition.validate().map_err(DeserializeError::custom)?;
        Ok(definition)
    }
}

/// One published, versioned agent definition
/// ([specification 7.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinitionRevision {
    schema_version: StateSchemaVersion,
    revision: AgentRevisionNumber,
    definition: AgentDefinition,
    provenance: AgentRevisionProvenance,
}

impl AgentDefinitionRevision {
    /// Publishes the first revision of a definition.
    #[must_use]
    pub fn initial(definition: AgentDefinition, provenance: AgentRevisionProvenance) -> Self {
        Self {
            schema_version: CURRENT_AGENT_DEFINITION_SCHEMA_VERSION,
            revision: AgentRevisionNumber::INITIAL,
            definition,
            provenance,
        }
    }

    /// Publishes the next revision of this definition.
    ///
    /// A definition revision may widen authority — that is what distinguishes it
    /// from a setup or a settings update — but doing so re-triggers autonomy
    /// admission in slice 1.9.
    #[must_use]
    pub fn succeed(
        &self,
        definition: AgentDefinition,
        provenance: AgentRevisionProvenance,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_DEFINITION_SCHEMA_VERSION,
            revision: self.revision.next(),
            definition,
            provenance,
        }
    }

    /// Monotonic revision number.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// Definition content of this revision.
    #[must_use]
    pub const fn definition(&self) -> &AgentDefinition {
        &self.definition
    }

    /// Authority this revision grants.
    #[must_use]
    pub const fn envelope(&self) -> &AgentAuthorityEnvelope {
        &self.definition.envelope
    }

    /// Who published this revision, when, and under which audit reference.
    #[must_use]
    pub const fn provenance(&self) -> &AgentRevisionProvenance {
        &self.provenance
    }
}

impl VersionedAgentRecord for AgentDefinitionRevision {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::DefinitionRevision;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// When an accepted settings change takes effect
/// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SettingsTimingClass {
    /// Applies at the next model turn.
    TurnBound,
    /// Applies before any further dispatch, and may invalidate an existing
    /// approval or authorization.
    ImmediateSafety,
    /// Applies only to a new run or an explicit migration.
    RunPinned,
}

impl SettingsTimingClass {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::TurnBound => "turn-bound",
            Self::ImmediateSafety => "immediate-safety",
            Self::RunPinned => "run-pinned",
        }
    }

    /// How soon this class must be honored, ascending.
    ///
    /// A revision's application point is the soonest class among its changes: a
    /// revision carrying both a prompt edit and a suspension must be honored
    /// before the next dispatch, not at the next turn.
    #[must_use]
    const fn urgency(self) -> u8 {
        match self {
            Self::ImmediateSafety => 2,
            Self::TurnBound => 1,
            Self::RunPinned => 0,
        }
    }
}

impl Display for SettingsTimingClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Bounded sampling parameters for a model turn.
///
/// Values are integer thousandths so a durable setting never depends on
/// floating-point rounding: `temperature_milli: Some(700)` is a temperature of
/// 0.7.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSamplingSettings {
    /// Sampling temperature, in thousandths.
    pub temperature_milli: Option<u32>,
    /// Nucleus sampling cutoff, in thousandths.
    pub top_p_milli: Option<u32>,
    /// Maximum output tokens for one model turn.
    pub max_output_tokens: Option<u32>,
}

/// One field-level settings change and, implicitly, when it applies
/// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The enum is deliberately incapable of widening authority: nothing here can
/// add a tool, a credential binding, a knowledge space, or a peer. Settings only
/// select within the definition's envelope, or revoke.
///
/// Suspension is the one immediate-safety control that is *not* here. It is the
/// agent's lifecycle status ([`crate::agent::AgentLifecycleStatus`]) so that
/// "may this agent dispatch" has exactly one durable answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentSettingsChange {
    /// Replace or clear the system instructions artifact.
    ///
    /// The artifact reference is boxed so one bulky variant does not inflate
    /// every change that travels through a mailbox or a remote envelope.
    Instructions(Option<Box<ArtifactRef>>),
    /// Select an approved model or provider profile.
    ModelProfile(AgentModelProfileId),
    /// Replace the sampling parameters.
    Sampling(AgentSamplingSettings),
    /// Bound how many memories a retrieval may return.
    RetrievalLimit(u32),
    /// Revoke a declared tool for this agent.
    RevokeTool(AgentToolId),
    /// Revoke a logical credential binding for this agent.
    RevokeCredentialBinding(AgentCredentialBindingRef),
    /// Move to a new guardrail policy revision.
    GuardrailPolicy(AgentPolicyRef),
    /// Move to a new durable loop-state schema.
    LoopStateSchemaVersion(StateSchemaVersion),
    /// Move to a new memory schema.
    MemorySchemaVersion(StateSchemaVersion),
}

impl AgentSettingsChange {
    /// When this change takes effect.
    #[must_use]
    pub const fn timing_class(&self) -> SettingsTimingClass {
        match self {
            Self::Instructions(_)
            | Self::ModelProfile(_)
            | Self::Sampling(_)
            | Self::RetrievalLimit(_) => SettingsTimingClass::TurnBound,
            Self::RevokeTool(_) | Self::RevokeCredentialBinding(_) | Self::GuardrailPolicy(_) => {
                SettingsTimingClass::ImmediateSafety
            }
            Self::LoopStateSchemaVersion(_) | Self::MemorySchemaVersion(_) => {
                SettingsTimingClass::RunPinned
            }
        }
    }
}

/// The materialized settings of one agent.
///
/// Fields are grouped by the timing class that governs them, because
/// [`effective_settings_for_turn`] resolves them group by group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSettings {
    /// Selected system instructions. Turn-bound.
    pub instructions: Option<ArtifactRef>,
    /// Selected model or provider profile. Turn-bound.
    pub model_profile: Option<AgentModelProfileId>,
    /// Sampling parameters. Turn-bound.
    pub sampling: AgentSamplingSettings,
    /// Maximum memories one retrieval may return. Turn-bound.
    pub retrieval_limit: Option<u32>,
    /// Tools revoked for this agent. Immediate safety.
    pub revoked_tools: BTreeSet<AgentToolId>,
    /// Credential bindings revoked for this agent. Immediate safety.
    pub revoked_credential_bindings: BTreeSet<AgentCredentialBindingRef>,
    /// Guardrail policy revision. Immediate safety.
    pub guardrail_policy: Option<AgentPolicyRef>,
    /// Durable loop-state schema version. Run-pinned.
    pub loop_state_schema_version: Option<StateSchemaVersion>,
    /// Memory schema version. Run-pinned.
    pub memory_schema_version: Option<StateSchemaVersion>,
}

impl AgentSettings {
    fn apply(&mut self, change: AgentSettingsChange) {
        match change {
            AgentSettingsChange::Instructions(instructions) => {
                self.instructions = instructions.map(|instructions| *instructions);
            }
            AgentSettingsChange::ModelProfile(profile) => self.model_profile = Some(profile),
            AgentSettingsChange::Sampling(sampling) => self.sampling = sampling,
            AgentSettingsChange::RetrievalLimit(limit) => self.retrieval_limit = Some(limit),
            AgentSettingsChange::RevokeTool(tool) => {
                self.revoked_tools.insert(tool);
            }
            AgentSettingsChange::RevokeCredentialBinding(binding) => {
                self.revoked_credential_bindings.insert(binding);
            }
            AgentSettingsChange::GuardrailPolicy(policy) => self.guardrail_policy = Some(policy),
            AgentSettingsChange::LoopStateSchemaVersion(version) => {
                self.loop_state_schema_version = Some(version);
            }
            AgentSettingsChange::MemorySchemaVersion(version) => {
                self.memory_schema_version = Some(version);
            }
        }
    }

    /// Accepts these settings under an authority envelope, or fails closed.
    ///
    /// The only settings field that can select outside the envelope is the model
    /// profile; everything else either narrows or is envelope-independent.
    pub fn validate_against(&self, envelope: &AgentAuthorityEnvelope) -> AgentDefinitionResult<()> {
        if let Some(profile) = &self.model_profile {
            if !envelope.model_profiles.contains(profile) {
                return Err(AgentDefinitionError::EnvelopeWidened {
                    violations: vec![AgentEnvelopeViolation::new(
                        AgentEnvelopeDimension::ModelProfile,
                        format!("model profile {profile} is not approved by the definition"),
                    )],
                });
            }
        }
        Ok(())
    }
}

/// One accepted settings update
/// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The revision holds both the materialized settings and the changes that
/// produced them, so an in-flight effect can be reasoned about against the exact
/// revision recorded in its intent.
///
/// The change list is bounded by [`AGENT_SETTINGS_MAX_CHANGES`] where a
/// revision is produced *and* on deserialization, so an oversized list can
/// neither cross the wire nor load from a durable record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettingsRevision {
    schema_version: StateSchemaVersion,
    revision: AgentRevisionNumber,
    settings: AgentSettings,
    changes: Vec<AgentSettingsChange>,
    provenance: AgentRevisionProvenance,
}

impl SettingsRevision {
    /// Creates the first settings revision of an agent.
    ///
    /// The settings are validated against the definition's envelope, so an agent
    /// cannot be instantiated already pointing at an unapproved model.
    pub fn initial(
        definition: &AgentDefinitionRevision,
        settings: AgentSettings,
        provenance: AgentRevisionProvenance,
    ) -> AgentDefinitionResult<Self> {
        settings.validate_against(definition.envelope())?;
        Ok(Self {
            schema_version: CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
            revision: AgentRevisionNumber::INITIAL,
            settings,
            changes: Vec::new(),
            provenance,
        })
    }

    /// Applies a bounded set of changes, producing the next revision.
    pub fn apply(
        &self,
        definition: &AgentDefinitionRevision,
        changes: Vec<AgentSettingsChange>,
        provenance: AgentRevisionProvenance,
    ) -> AgentDefinitionResult<Self> {
        if changes.is_empty() {
            return Err(AgentDefinitionError::EmptySettingsUpdate);
        }
        if changes.len() > AGENT_SETTINGS_MAX_CHANGES {
            return Err(AgentDefinitionError::TooManySettingsChanges {
                count: changes.len(),
                maximum: AGENT_SETTINGS_MAX_CHANGES,
            });
        }

        let mut settings = self.settings.clone();
        for change in changes.clone() {
            settings.apply(change);
        }
        settings.validate_against(definition.envelope())?;

        Ok(Self {
            schema_version: CURRENT_AGENT_SETTINGS_SCHEMA_VERSION,
            revision: self.revision.next(),
            settings,
            changes,
            provenance,
        })
    }

    /// Monotonic revision number.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// Materialized settings at this revision.
    #[must_use]
    pub const fn settings(&self) -> &AgentSettings {
        &self.settings
    }

    /// Changes that produced this revision.
    #[must_use]
    pub fn changes(&self) -> &[AgentSettingsChange] {
        &self.changes
    }

    /// Who accepted this revision, when, and under which audit reference.
    #[must_use]
    pub const fn provenance(&self) -> &AgentRevisionProvenance {
        &self.provenance
    }

    /// Accepts these settings under a definition revision, or fails closed.
    ///
    /// A published definition may narrow its envelope. When it does, the
    /// settings already in force must still be legal under the new envelope, or
    /// the agent would keep running against — for instance — a model profile the
    /// definition no longer approves.
    pub fn validate_against_definition(
        &self,
        definition: &AgentDefinitionRevision,
    ) -> AgentDefinitionResult<()> {
        self.settings.validate_against(definition.envelope())
    }

    /// The soonest point at which this revision must be honored.
    ///
    /// The initial revision has no changes and applies from the agent's first
    /// turn.
    #[must_use]
    pub fn application_point(&self) -> SettingsTimingClass {
        self.changes
            .iter()
            .map(AgentSettingsChange::timing_class)
            .max_by_key(|class| class.urgency())
            .unwrap_or(SettingsTimingClass::TurnBound)
    }

    /// Whether this revision carries a change that must be honored before any
    /// further dispatch.
    #[must_use]
    pub fn has_immediate_safety_change(&self) -> bool {
        self.changes
            .iter()
            .any(|change| change.timing_class() == SettingsTimingClass::ImmediateSafety)
    }
}

impl VersionedAgentRecord for SettingsRevision {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::SettingsRevision;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

impl<'de> Deserialize<'de> for SettingsRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            schema_version: StateSchemaVersion,
            revision: AgentRevisionNumber,
            settings: AgentSettings,
            changes: Vec<AgentSettingsChange>,
            provenance: AgentRevisionProvenance,
        }

        let record = Record::deserialize(deserializer)?;
        if record.changes.len() > AGENT_SETTINGS_MAX_CHANGES {
            return Err(DeserializeError::custom(
                AgentDefinitionError::TooManySettingsChanges {
                    count: record.changes.len(),
                    maximum: AGENT_SETTINGS_MAX_CHANGES,
                },
            ));
        }
        Ok(Self {
            schema_version: record.schema_version,
            revision: record.revision,
            settings: record.settings,
            changes: record.changes,
            provenance: record.provenance,
        })
    }
}

/// Resolves the settings one model turn of a run must use
/// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// `pinned` is the revision the run started under; `current` is the agent's
/// latest accepted revision. Turn-bound and immediate-safety fields come from
/// `current`, so a prompt change reaches the next turn and a suspension or
/// revocation reaches the next dispatch. Run-pinned fields come from `pinned`, so
/// a loop-state or memory schema change cannot mutate a run already executing
/// under the old schema; it waits for a new run or an explicit migration.
#[must_use]
pub fn effective_settings_for_turn(
    pinned: &SettingsRevision,
    current: &SettingsRevision,
) -> AgentSettings {
    AgentSettings {
        instructions: current.settings.instructions.clone(),
        model_profile: current.settings.model_profile.clone(),
        sampling: current.settings.sampling,
        retrieval_limit: current.settings.retrieval_limit,
        revoked_tools: current.settings.revoked_tools.clone(),
        revoked_credential_bindings: current.settings.revoked_credential_bindings.clone(),
        guardrail_policy: current.settings.guardrail_policy.clone(),
        loop_state_schema_version: pinned.settings.loop_state_schema_version,
        memory_schema_version: pinned.settings.memory_schema_version,
    }
}

/// The per-run narrowing of a definition
/// ([specification 7.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// A setup selects instructions, task capabilities, collaborators, knowledge and
/// environment scopes, and budgets for one run. It can only ever narrow: the
/// constructor validates the envelope against the definition it names and fails
/// closed, and slice 1.8 revalidates the same envelope at dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSetupRevision {
    schema_version: StateSchemaVersion,
    revision: AgentRevisionNumber,
    definition_revision: AgentRevisionNumber,
    instructions: Option<ArtifactRef>,
    envelope: AgentAuthorityEnvelope,
    provenance: AgentRevisionProvenance,
}

impl AgentSetupRevision {
    /// Creates a setup revision, rejecting an envelope that widens the
    /// definition's authority.
    pub fn new(
        revision: AgentRevisionNumber,
        definition: &AgentDefinitionRevision,
        envelope: AgentAuthorityEnvelope,
        provenance: AgentRevisionProvenance,
    ) -> AgentDefinitionResult<Self> {
        definition.envelope().validate_narrowing(&envelope)?;
        Ok(Self {
            schema_version: CURRENT_AGENT_SETUP_SCHEMA_VERSION,
            revision,
            definition_revision: definition.revision(),
            instructions: None,
            envelope,
            provenance,
        })
    }

    /// Selects the run's system instructions.
    #[must_use]
    pub fn with_instructions(mut self, instructions: ArtifactRef) -> Self {
        self.instructions = Some(instructions);
        self
    }

    /// Monotonic revision number.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// Definition revision this setup narrows.
    #[must_use]
    pub const fn definition_revision(&self) -> AgentRevisionNumber {
        self.definition_revision
    }

    /// Instructions selected for the run.
    #[must_use]
    pub const fn instructions(&self) -> Option<&ArtifactRef> {
        self.instructions.as_ref()
    }

    /// The narrowed authority this setup grants the run.
    #[must_use]
    pub const fn envelope(&self) -> &AgentAuthorityEnvelope {
        &self.envelope
    }

    /// Who authorized this setup, when, and under which audit reference.
    #[must_use]
    pub const fn provenance(&self) -> &AgentRevisionProvenance {
        &self.provenance
    }
}

impl VersionedAgentRecord for AgentSetupRevision {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::SetupRevision;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Rejection of a definition, settings, or setup revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentDefinitionError {
    /// An identifier in the revision was malformed.
    Identity(AgentIdentityError),
    /// The mandatory description was empty.
    EmptyDescription,
    /// The description exceeded the bounded length.
    DescriptionTooLong {
        /// Length of the rejected description, in bytes.
        length: usize,
        /// Maximum accepted length, in bytes.
        maximum: usize,
    },
    /// A settings update carried no changes.
    EmptySettingsUpdate,
    /// A settings update carried more changes than one revision may hold.
    TooManySettingsChanges {
        /// Number of changes in the rejected update.
        count: usize,
        /// Maximum accepted number of changes.
        maximum: usize,
    },
    /// The candidate envelope widened the authority it was meant to narrow.
    EnvelopeWidened {
        /// Every way the candidate widened authority.
        violations: Vec<AgentEnvelopeViolation>,
    },
}

impl AgentDefinitionError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::EmptyDescription => "empty-agent-description",
            Self::DescriptionTooLong { .. } => "agent-description-too-long",
            Self::EmptySettingsUpdate => "empty-settings-update",
            Self::TooManySettingsChanges { .. } => "too-many-settings-changes",
            Self::EnvelopeWidened { .. } => "envelope-widened",
        }
    }

    /// Stable reason codes for every widening violation, when this error
    /// rejected an envelope.
    #[must_use]
    pub fn violation_codes(&self) -> Vec<&'static str> {
        match self {
            Self::EnvelopeWidened { violations } => violations
                .iter()
                .map(AgentEnvelopeViolation::reason_code)
                .collect(),
            _ => Vec::new(),
        }
    }
}

impl Display for AgentDefinitionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::EmptyDescription => {
                f.write_str("an agent definition requires a bounded outcome-oriented description")
            }
            Self::DescriptionTooLong { length, maximum } => write!(
                f,
                "the agent description is {length} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::EmptySettingsUpdate => f.write_str("a settings update requires at least one change"),
            Self::TooManySettingsChanges { count, maximum } => write!(
                f,
                "a settings update carried {count} changes, which exceeds the {maximum} change limit"
            ),
            Self::EnvelopeWidened { violations } => {
                f.write_str("the envelope widened the definition's authority: ")?;
                for (index, violation) in violations.iter().enumerate() {
                    if index > 0 {
                        f.write_str("; ")?;
                    }
                    Display::fmt(violation, f)?;
                }
                Ok(())
            }
        }
    }
}

impl Error for AgentDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentDefinitionError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}
