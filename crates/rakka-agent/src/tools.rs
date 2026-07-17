//! The tool registry and the tool authority layers.
//!
//! Owns the tool registry — kinds, descriptor schema and version, safety class,
//! capabilities, and credential class
//! ([specification 11.7](../../../docs/plans/rakka-agent/spec.md)) — and the
//! four authority layers that stand between a model suggestion and an external
//! call ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)):
//!
//! | Layer | Type | Purpose |
//! | --- | --- | --- |
//! | descriptor | [`AgentToolDescriptor`] | bounded schema/description visible to the model |
//! | binding | [`AgentToolBinding`] | deployment-authorized target, safety, capability, credential class |
//! | effect intent | [`crate::effect::AgentRunEffect`] | exact target, canonical argument digest, revisions |
//! | dispatch grant | [`AgentDispatchGrant`] | current authorization to execute that exact intent |
//!
//! Each layer may only narrow the one above it, a grant binds to an exact
//! intent, and every grant is revalidated before the attempt. Model output can
//! request a call; it can never widen authority, target, capability, or
//! credential class — structurally, because the model's
//! [`crate::model::AgentToolCallRequest`] carries nothing but a call id, a tool
//! name, and arguments, and everything else is resolved from trusted
//! definition, setup, and deployment data by [`AgentToolAuthority`].
//!
//! Also owns the `ExecutionPolicyRef` routing hook
//! ([`AgentExecutionPolicyRouter`]) that lets an application place a tool
//! executor in the isolation its trust class requires. Rakka persists and
//! routes the reference; the application owns the worker pool, RBAC, network
//! policy, credential issuer, and sandbox behind it. The
//! `AgentEnvironmentRef` contract and concurrency rules for tool adapters
//! sharing an environment arrive with slice 4.6.
//!
//! Specification: sections 11.7 and 11.8, with the enforcement clauses of 16
//! and the envelope rules of 7.3. Filled by slice 1.8; the shared-environment
//! rules by slice 4.6.
//!
//! # Where enforcement runs
//!
//! The authority is evaluated by the dispatch pipeline before every attempt's
//! durable `Started` ([`crate::dispatch`]), against the agent's *current*
//! durable state — which is what makes an immediate-safety settings change
//! (a revoked tool, a revoked credential, a suspension) effective before any
//! further dispatch, and what enforces the slice 1.2 setup/settings envelope
//! at dispatch. A registered tool the definition never declared, a declared
//! tool the setup excluded, and a call the model invented all converge on the
//! same outcome: the effect stays undispatchable, with a stable reason code.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;

use rakka_agent_workflow::{AgentEffectId, AgentTimestampMillis};
use serde::Serialize;
use serde_json::Value;

use crate::agent::{AgentEntityState, AgentLifecycleStatus};
use crate::definition::{
    AgentAuthorityEnvelope, AgentCapabilityId, AgentCredentialBindingRef, AgentDefinitionRevision,
    AgentEffectSafetyClass, AgentEnvelopeDimension, AgentExecutionPolicyRef, AgentGuardrailStageId,
    AgentModelProfileId, AgentRevisionNumber, AgentSamplingSettings, AgentSettings,
    AgentSetupRevision, AgentToolDeclaration, AgentToolId, SettingsRevision,
};
use crate::effect::{
    AgentEffectError, AgentEffectGeneration, AgentEffectResult, AgentEffectSpec,
    AgentReconciliationProtocolRef, AgentRunEffect, AgentRunEffectRequest,
};
use crate::guardrails::{AgentGuardrailChain, AgentGuardrailDisposition, AgentGuardrailReport};
use crate::identity::{AgentGoalId, AgentRunScope, AgentTaskId};
use crate::model::{AgentToolCallRequest, AGENT_TOOL_ARGUMENTS_MAX_BYTES};
use crate::task::{AgentContentDigest, AgentSchemaRef};

/// Largest model-visible tool description, in bytes.
pub const AGENT_TOOL_DESCRIPTION_MAX_LENGTH: usize = 1024;

/// Largest inline model-visible parameter schema, in bytes.
pub const AGENT_TOOL_PARAMETERS_MAX_BYTES: usize = 4 * 1024;

/// Most tools one registry may hold.
pub const AGENT_TOOL_REGISTRY_MAX_TOOLS: usize = 256;

/// How long an issued dispatch grant stays valid, unless configured otherwise.
pub const AGENT_DISPATCH_GRANT_DEFAULT_TTL_MS: u64 = 60_000;

/// Result type for tool registry operations.
pub type AgentToolResult<T> = Result<T, AgentToolError>;

/// The kind of component behind a tool
/// ([specification 11.7](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentToolKind {
    /// A bounded function adapter.
    Function,
    /// A compiled workflow invoked as a tool
    /// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)).
    Workflow,
    /// A supervised child-process adapter.
    Process,
    /// A remote MCP tool behind the optional adapter. Never an indirect
    /// peer-agent channel that bypasses `rakka-a2a`.
    RemoteMcp,
    /// A retrieval or memory query.
    Retrieval,
    /// A shared-environment operation
    /// ([specification 8.5](../../../docs/plans/rakka-agent/spec.md)).
    Environment,
    /// An agent-coordination operation surfaced as a tool.
    AgentCoordination,
}

impl AgentToolKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::Workflow => "workflow",
            Self::Process => "process",
            Self::RemoteMcp => "remote-mcp",
            Self::Retrieval => "retrieval",
            Self::Environment => "environment",
            Self::AgentCoordination => "agent-coordination",
        }
    }
}

impl Display for AgentToolKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// How a tool's result is bounded
/// ([specification 11.7](../../../docs/plans/rakka-agent/spec.md): every
/// descriptor declares bounded result/artifact behavior).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentToolResultBehavior {
    /// The result is inline content bounded by
    /// [`crate::effect::AGENT_TOOL_RESULT_MAX_BYTES`].
    InlineBounded,
    /// The result arrives as an immutable artifact reference.
    ArtifactReference,
}

impl AgentToolResultBehavior {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::InlineBounded => "inline-bounded",
            Self::ArtifactReference => "artifact-reference",
        }
    }
}

impl Display for AgentToolResultBehavior {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The bounded, model-visible face of one registered tool
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md):
/// `ToolDescriptor`).
///
/// A descriptor implies no dispatch authority
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)): showing it
/// to a model lets the model *ask*, and everything that decides whether the
/// ask executes lives in the binding, the intent, and the grant.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentToolDescriptor {
    /// Stable tool name.
    pub tool: AgentToolId,
    /// Monotonic descriptor version. Bumped whenever the model-visible shape
    /// changes, so a grant can prove which shape it validated.
    pub version: AgentRevisionNumber,
    /// The kind of component behind the tool.
    pub kind: AgentToolKind,
    /// Bounded model-facing description.
    pub description: String,
    /// Versioned reference to the input schema.
    pub input_schema: AgentSchemaRef,
    /// Versioned reference to the output schema.
    pub output_schema: AgentSchemaRef,
    /// Inline model-visible parameter schema, when the adapter surfaces one.
    pub parameters: Option<Value>,
    /// How the tool's result is bounded.
    pub result_behavior: AgentToolResultBehavior,
}

impl AgentToolDescriptor {
    /// Creates a descriptor, rejecting an unbounded description.
    pub fn new(
        tool: AgentToolId,
        kind: AgentToolKind,
        description: impl Into<String>,
        input_schema: AgentSchemaRef,
        output_schema: AgentSchemaRef,
    ) -> AgentToolResult<Self> {
        let descriptor = Self {
            tool,
            version: AgentRevisionNumber::INITIAL,
            kind,
            description: description.into(),
            input_schema,
            output_schema,
            parameters: None,
            result_behavior: AgentToolResultBehavior::InlineBounded,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Sets the descriptor version.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// Attaches an inline model-visible parameter schema.
    pub fn with_parameters(mut self, parameters: Value) -> AgentToolResult<Self> {
        self.parameters = Some(parameters);
        self.validate()?;
        Ok(self)
    }

    /// Declares the tool's result behavior.
    #[must_use]
    pub const fn with_result_behavior(mut self, behavior: AgentToolResultBehavior) -> Self {
        self.result_behavior = behavior;
        self
    }

    /// Rejects a descriptor that exceeds its bounds.
    pub fn validate(&self) -> AgentToolResult<()> {
        if self.description.is_empty() {
            return Err(AgentToolError::EmptyDescription {
                tool: self.tool.clone(),
            });
        }
        if self.description.len() > AGENT_TOOL_DESCRIPTION_MAX_LENGTH {
            return Err(AgentToolError::DescriptionTooLong {
                tool: self.tool.clone(),
                length: self.description.len(),
                maximum: AGENT_TOOL_DESCRIPTION_MAX_LENGTH,
            });
        }
        if let Some(parameters) = &self.parameters {
            let bytes = serde_json::to_vec(parameters)
                .map(|encoded| encoded.len())
                .unwrap_or(usize::MAX);
            if bytes > AGENT_TOOL_PARAMETERS_MAX_BYTES {
                return Err(AgentToolError::ParametersTooLarge {
                    tool: self.tool.clone(),
                    bytes,
                    maximum: AGENT_TOOL_PARAMETERS_MAX_BYTES,
                });
            }
        }
        Ok(())
    }

    /// Canonical fingerprint of the model-visible shape.
    ///
    /// A dispatch grant binds it
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)), so a
    /// descriptor that changed between validation and execution is detectable
    /// rather than silently honored — the seam the remote-MCP adapter's
    /// catalog-drift rule builds on.
    #[must_use]
    pub fn schema_digest(&self) -> AgentContentDigest {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        AgentContentDigest::of_json(&value)
    }
}

/// The deployment-authorized authority of one registered tool
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md):
/// `ToolBinding`).
///
/// The binding is trusted deployment data: the safety class, capabilities,
/// credential class, execution-policy routing, guardrail requirements, and
/// attempt policy a dispatch may use. Model output can never produce or widen
/// one. A tool registered without an explicit declaration is *unclassified*
/// and fails safe: one non-idempotent attempt, no capabilities, no credential,
/// so an ambiguous loss parks for reconciliation rather than guessing that a
/// retry is harmless.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentToolBinding {
    descriptor: AgentToolDescriptor,
    declaration: AgentToolDeclaration,
    max_attempts: u32,
    reconciliation_protocol: Option<AgentReconciliationProtocolRef>,
    timeout_ms: Option<u64>,
    guardrails: BTreeSet<AgentGuardrailStageId>,
    checkpoint_required: bool,
}

impl AgentToolBinding {
    /// Binds a descriptor under the fail-safe unclassified declaration: one
    /// non-idempotent attempt.
    #[must_use]
    pub fn unclassified(descriptor: AgentToolDescriptor) -> Self {
        Self {
            descriptor,
            declaration: AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent),
            max_attempts: 1,
            reconciliation_protocol: None,
            timeout_ms: None,
            guardrails: BTreeSet::new(),
            checkpoint_required: false,
        }
    }

    /// Binds a descriptor under an explicit declaration and attempt bound.
    ///
    /// Construction is builder-shaped and cannot see the whole binding — a
    /// `Reconcileable` declaration is legal only once its protocol is
    /// attached — so the closed validation runs at
    /// [`AgentToolRegistry::register`], the point where a binding becomes
    /// authoritative, and again wherever [`Self::effect_spec`] is read.
    #[must_use]
    pub fn new(
        descriptor: AgentToolDescriptor,
        declaration: AgentToolDeclaration,
        max_attempts: u32,
    ) -> Self {
        Self {
            descriptor,
            declaration,
            max_attempts,
            reconciliation_protocol: None,
            timeout_ms: None,
            guardrails: BTreeSet::new(),
            checkpoint_required: false,
        }
    }

    /// Names the protocol that reconciles an ambiguous attempt; required
    /// exactly when the declared class is `Reconcileable`.
    #[must_use]
    pub fn with_reconciliation_protocol(
        mut self,
        protocol: AgentReconciliationProtocolRef,
    ) -> Self {
        self.reconciliation_protocol = Some(protocol);
        self
    }

    /// Sets the per-attempt timeout.
    #[must_use]
    pub const fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Requires a guardrail stage to be present in the deployment's chain
    /// before this tool may dispatch.
    #[must_use]
    pub fn with_guardrail(mut self, stage: AgentGuardrailStageId) -> Self {
        self.guardrails.insert(stage);
        self
    }

    /// Requires an effect-bound checkpoint grant before this tool may
    /// dispatch ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Until slice 1.10 lands the checkpoint runtime, no grant can exist, so a
    /// tool bound this way is undispatchable by construction — fail closed,
    /// never fail open.
    #[must_use]
    pub const fn with_checkpoint_required(mut self) -> Self {
        self.checkpoint_required = true;
        self
    }

    /// The bounded, model-visible descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &AgentToolDescriptor {
        &self.descriptor
    }

    /// The authorized declaration: safety, capabilities, credential class,
    /// execution-policy routing.
    #[must_use]
    pub const fn declaration(&self) -> &AgentToolDeclaration {
        &self.declaration
    }

    /// Guardrail stages that must be present before this tool may dispatch.
    #[must_use]
    pub const fn guardrails(&self) -> &BTreeSet<AgentGuardrailStageId> {
        &self.guardrails
    }

    /// Whether this tool requires an effect-bound checkpoint grant.
    #[must_use]
    pub const fn checkpoint_required(&self) -> bool {
        self.checkpoint_required
    }

    /// The effect spec calls to this tool dispatch under
    /// ([specification 11.2](../../../docs/plans/rakka-agent/spec.md): the
    /// registered tool supplies the permitted safety declaration).
    pub fn effect_spec(&self) -> AgentEffectResult<AgentEffectSpec> {
        let mut spec = AgentEffectSpec {
            safety_class: self.declaration.safety,
            max_attempts: self.max_attempts,
            reconciliation_protocol: self.reconciliation_protocol.clone(),
            credential_binding: self.declaration.credential_binding.clone(),
            timeout_ms: self.timeout_ms,
            execution_policy: None,
        };
        spec.validate()?;
        if let Some(policy) = &self.declaration.execution_policy {
            spec = spec.with_execution_policy(policy.clone());
        }
        Ok(spec)
    }
}

/// The deployment's registry of dispatchable tools
/// ([specification 11.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// Registration is necessary but never sufficient: a registered tool still
/// needs the agent definition (and any run setup) to declare it, and every
/// dispatch still needs a grant. The registry supplies the *deployment* half
/// of that meet — what exists, what it is, and what failure policy it
/// permits.
#[derive(Debug, Clone, Default)]
pub struct AgentToolRegistry {
    tools: BTreeMap<AgentToolId, AgentToolBinding>,
}

impl AgentToolRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one tool binding, refusing a duplicate or a binding whose
    /// failure policy the crash-and-timeout rules could not honor.
    pub fn register(mut self, binding: AgentToolBinding) -> AgentToolResult<Self> {
        if self.tools.len() >= AGENT_TOOL_REGISTRY_MAX_TOOLS {
            return Err(AgentToolError::RegistryFull {
                maximum: AGENT_TOOL_REGISTRY_MAX_TOOLS,
            });
        }
        binding.descriptor.validate()?;
        binding.effect_spec()?;
        let tool = binding.descriptor.tool.clone();
        if self.tools.contains_key(&tool) {
            return Err(AgentToolError::DuplicateTool { tool });
        }
        self.tools.insert(tool, binding);
        Ok(self)
    }

    /// The binding of one registered tool.
    #[must_use]
    pub fn binding(&self, tool: &AgentToolId) -> Option<&AgentToolBinding> {
        self.tools.get(tool)
    }

    /// How many tools are registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether no tool is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// The effect specs the run's transitions stamp onto committed effects,
    /// derived from the registered bindings.
    ///
    /// This is what replaces hand-written [`crate::effect::AgentEffectPolicies`]
    /// maps: the registry is the single source, and the run-side policies are a
    /// projection of it. The defaults stay fail-safe — a tool absent from the
    /// registry projects as one non-idempotent attempt at commit time, and is
    /// then refused outright at dispatch.
    pub fn effect_policies(&self) -> AgentEffectResult<crate::effect::AgentEffectPolicies> {
        let mut policies = crate::effect::AgentEffectPolicies::new();
        for (tool, binding) in &self.tools {
            policies = policies.with_tool_spec(tool.clone(), binding.effect_spec()?)?;
        }
        Ok(policies)
    }

    /// The declarations the registered bindings authorize, keyed by tool.
    ///
    /// A definition that wants to declare every registered tool exactly as the
    /// deployment classified it starts from this projection; anything it
    /// declares beyond a binding is refused at dispatch.
    #[must_use]
    pub fn tool_declarations(&self) -> BTreeMap<AgentToolId, AgentToolDeclaration> {
        self.tools
            .iter()
            .map(|(tool, binding)| (tool.clone(), binding.declaration.clone()))
            .collect()
    }

    /// The descriptors one agent's model may be shown: registered, declared by
    /// the envelope, and not revoked by the current settings.
    ///
    /// Visibility is not authority — a visible descriptor still needs a grant
    /// to execute — but the converse discipline matters too: a tool the agent
    /// could never dispatch should not be dangled in front of the model.
    #[must_use]
    pub fn model_visible(
        &self,
        envelope: &AgentAuthorityEnvelope,
        settings: &AgentSettings,
    ) -> Vec<&AgentToolDescriptor> {
        self.tools
            .iter()
            .filter(|(tool, _)| {
                envelope.tools.contains_key(*tool) && !settings.revoked_tools.contains(*tool)
            })
            .map(|(_, binding)| &binding.descriptor)
            .collect()
    }
}

/// The agent's durable authority state, as one dispatch decision reads it.
///
/// It borrows the durable records the decision is made against — the
/// definition revision, the current settings revision, and the run's setup
/// where one exists — so a grant can record exactly which revisions it
/// validated.
#[derive(Debug, Clone, Copy)]
pub struct AgentAuthorityContext<'a> {
    /// The agent's administrative lifecycle status.
    pub status: AgentLifecycleStatus,
    /// The published definition revision.
    pub definition: &'a AgentDefinitionRevision,
    /// The current settings revision, whose immediate-safety fields govern
    /// every further dispatch.
    pub settings: &'a SettingsRevision,
    /// The run's setup revision, when the run was created under one.
    pub setup: Option<&'a AgentSetupRevision>,
}

impl<'a> AgentAuthorityContext<'a> {
    /// The context one agent entity's durable state provides.
    #[must_use]
    pub fn for_entity(state: &'a AgentEntityState) -> Self {
        Self {
            status: state.status(),
            definition: state.definition(),
            settings: state.settings(),
            setup: None,
        }
    }

    /// Attaches the run's setup revision.
    #[must_use]
    pub const fn with_setup(mut self, setup: &'a AgentSetupRevision) -> Self {
        self.setup = Some(setup);
        self
    }
}

/// The descriptor identity one grant binds
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md): descriptor
/// name, version, and schema digest).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentGrantDescriptor {
    /// The tool the descriptor names.
    pub tool: AgentToolId,
    /// The descriptor version the grant validated.
    pub version: AgentRevisionNumber,
    /// Canonical fingerprint of the model-visible shape the grant validated.
    pub schema_digest: AgentContentDigest,
}

/// The current authorization to execute one exact effect intent
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md):
/// `DispatchGrant`).
///
/// A grant is issued by [`AgentToolAuthority::authorize`] and revalidated with
/// [`AgentDispatchGrant::validate_for`] before the attempt it covers. It binds
/// the full identity of what it authorizes — tenant, goal, task, agent, run,
/// effect and generation, descriptor version and schema digest, target and
/// argument digest, safety class, the revisions it validated, capabilities,
/// and credential binding — plus an expiry and an allowed use count, so a
/// grant can never quietly outlive or outspend the decision it records. It
/// carries no resolved credential and no secret material.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentDispatchGrant {
    /// The run the grant covers.
    pub scope: AgentRunScope,
    /// The task the run serves.
    pub task: Option<AgentTaskId>,
    /// The goal the run contributes to.
    pub goal: Option<AgentGoalId>,
    /// The effect the grant covers.
    pub effect_id: AgentEffectId,
    /// The effect generation the grant covers.
    pub generation: AgentEffectGeneration,
    /// The descriptor identity, for a tool call.
    pub descriptor: Option<AgentGrantDescriptor>,
    /// The dispatch target, as the intent names it.
    pub target: String,
    /// Canonical fingerprint of the exact arguments the grant authorizes.
    pub argument_digest: AgentContentDigest,
    /// The safety class the grant authorizes.
    pub safety_class: AgentEffectSafetyClass,
    /// The definition revision the grant validated against.
    pub definition_revision: AgentRevisionNumber,
    /// The settings revision the grant validated against.
    pub settings_revision: AgentRevisionNumber,
    /// The setup revision the grant validated against, when the run has one.
    pub setup_revision: Option<AgentRevisionNumber>,
    /// The guardrail chain revision the grant evaluated under, when a chain is
    /// configured.
    pub guardrail_revision: Option<AgentRevisionNumber>,
    /// The scoped capabilities the grant authorizes.
    pub capabilities: BTreeSet<AgentCapabilityId>,
    /// The logical credential binding the dispatch may resolve.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// The execution policy the dispatch is routed through.
    pub execution_policy: Option<AgentExecutionPolicyRef>,
    /// When the grant was issued.
    pub issued_at: AgentTimestampMillis,
    /// When the grant expires.
    pub expires_at: AgentTimestampMillis,
    /// The most attempts the grant covers, aligned to the intent's bound.
    pub allowed_use_count: u32,
}

impl AgentDispatchGrant {
    /// Accepts that this grant covers the given attempt of the given intent,
    /// or fails closed
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md): the
    /// dispatcher rechecks grant validity before each attempt).
    pub fn validate_for(
        &self,
        scope: &AgentRunScope,
        intent: &AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentAuthorityRefusal> {
        if &self.scope != scope
            || self.effect_id != intent.effect_id
            || self.generation != intent.generation
            || self.safety_class != intent.safety.class()
        {
            return Err(AgentAuthorityRefusal::of(
                "grant-intent-mismatch",
                "the grant does not bind the intent being dispatched",
            ));
        }
        if self.argument_digest != intent.argument_digest {
            return Err(AgentAuthorityRefusal::of(
                "argument-digest-mismatch",
                "the grant binds different arguments than the intent carries",
            ));
        }
        if now.as_millis() >= self.expires_at.as_millis() {
            return Err(AgentAuthorityRefusal::of(
                "grant-expired",
                "the grant expired before the attempt",
            ));
        }
        if attempt > self.allowed_use_count {
            return Err(AgentAuthorityRefusal::of(
                "grant-uses-exhausted",
                "the grant's allowed use count is spent",
            ));
        }
        Ok(())
    }
}

/// One authorized dispatch: the grant, plus what the authority resolved for
/// the bounded attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentGrantedDispatch {
    /// The grant binding the exact intent.
    pub grant: AgentDispatchGrant,
    /// The tool call to execute, when a guardrail stage deterministically
    /// transformed the intent's arguments. `None` means the intent's own call
    /// executes unchanged. The transform re-derives identically on every
    /// attempt of the generation, because it is a pure function of the durable
    /// intent under the chain revision the grant binds.
    pub tool_call: Option<Box<AgentToolCallRequest>>,
    /// The model profile the current settings resolve for the turn.
    pub model_profile: Option<AgentModelProfileId>,
    /// The sampling parameters the current settings resolve for the turn.
    pub sampling: Option<AgentSamplingSettings>,
    /// Report-only guardrail findings, for observability.
    pub reports: Vec<AgentGuardrailReport>,
}

/// A dispatch the authority refused, with a stable reason code.
///
/// `retryable` distinguishes a condition that may clear (a suspension) from a
/// decision that will not (an undeclared tool, a revocation, a digest
/// mismatch): the dispatch layer burns an attempt for the former and settles
/// the effect as failed for the latter. Either way the call stays
/// undispatchable now, which is what
/// [specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 54
/// requires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentAuthorityRefusal {
    /// Stable machine-readable reason code.
    pub code: String,
    /// Human-readable detail.
    pub message: String,
    /// Whether the refusing condition may clear without a new definition,
    /// setup, or reconfiguration.
    pub retryable: bool,
}

impl AgentAuthorityRefusal {
    /// A definitive refusal.
    #[must_use]
    pub fn of(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }

    /// A refusal whose condition may clear.
    #[must_use]
    pub fn transient(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: true,
        }
    }
}

impl Display for AgentAuthorityRefusal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "dispatch refused ({}): {}", self.code, self.message)
    }
}

/// Routes an effect's execution-policy reference onto an executor the
/// application trusts for it
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// Rakka persists the reference on the intent, carries it on the dispatch
/// ticket, and enforces here that an effect carrying one is only executed by a
/// worker the application accepted for that trust class. The worker pool,
/// Kubernetes RBAC, network policy, credential issuer, and sandbox behind the
/// reference are application-owned.
pub trait AgentExecutionPolicyRouter: Send + Sync {
    /// Whether the current executor may run effects of the given policy class.
    fn accepts(&self, policy: &AgentExecutionPolicyRef) -> bool;
}

/// The authority that turns a durable effect intent into a dispatch grant, or
/// refuses it with a stable reason
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md),
/// [16](../../../docs/plans/rakka-agent/spec.md)).
///
/// Evaluation is pure: everything it reads arrives as arguments — the agent's
/// durable authority context, the run scope, and the intent — so the caller
/// (the dispatch pipeline) decides *when* the durable state is read, and this
/// type decides only what it means. The checks are ordered so a refusal's
/// reason code names the *first* authority layer that failed.
#[derive(Clone)]
pub struct AgentToolAuthority {
    registry: AgentToolRegistry,
    guardrails: Option<AgentGuardrailChain>,
    execution_router: Option<Arc<dyn AgentExecutionPolicyRouter>>,
    grant_ttl_ms: u64,
}

impl AgentToolAuthority {
    /// An authority over the given registry, with no guardrail chain and no
    /// execution-policy router.
    #[must_use]
    pub fn new(registry: AgentToolRegistry) -> Self {
        Self {
            registry,
            guardrails: None,
            execution_router: None,
            grant_ttl_ms: AGENT_DISPATCH_GRANT_DEFAULT_TTL_MS,
        }
    }

    /// Uses the deployment's guardrail chain.
    #[must_use]
    pub fn with_guardrails(mut self, chain: AgentGuardrailChain) -> Self {
        self.guardrails = Some(chain);
        self
    }

    /// Routes execution-policy references through the given router.
    #[must_use]
    pub fn with_execution_router(mut self, router: Arc<dyn AgentExecutionPolicyRouter>) -> Self {
        self.execution_router = Some(router);
        self
    }

    /// Sets how long an issued grant stays valid.
    #[must_use]
    pub const fn with_grant_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.grant_ttl_ms = ttl_ms;
        self
    }

    /// The registry this authority answers from.
    #[must_use]
    pub const fn registry(&self) -> &AgentToolRegistry {
        &self.registry
    }

    /// Authorizes one dispatch attempt of one effect intent, or refuses it.
    ///
    /// This runs before every attempt's durable `Started`, so an
    /// immediate-safety settings change — a revoked tool or credential, a
    /// suspension — is honored before any further dispatch
    /// ([specification 7.2](../../../docs/plans/rakka-agent/spec.md)), and the
    /// setup/settings envelope of slice 1.2 is enforced where it finally
    /// matters ([specification 7.3](../../../docs/plans/rakka-agent/spec.md)).
    pub fn authorize(
        &self,
        context: &AgentAuthorityContext<'_>,
        scope: &AgentRunScope,
        task: Option<&AgentTaskId>,
        goal: Option<&AgentGoalId>,
        intent: &AgentRunEffect,
        now: AgentTimestampMillis,
    ) -> Result<AgentGrantedDispatch, AgentAuthorityRefusal> {
        // Immediate safety first: a terminated agent never dispatches again,
        // and a suspended one dispatches nothing until resumed.
        if context.status.is_terminal() {
            return Err(AgentAuthorityRefusal::of(
                "agent-terminated",
                "the agent is terminated and dispatches nothing",
            ));
        }
        if !context.status.permits_dispatch() {
            return Err(AgentAuthorityRefusal::transient(
                "agent-dispatch-suspended",
                "the agent is suspended; no further effect may be dispatched until it resumes",
            ));
        }

        // The grant binds the exact intent, so the intent must be internally
        // consistent before anything else is decided about it.
        let recomputed = intent.request.argument_digest().map_err(|error| {
            AgentAuthorityRefusal::of("argument-digest-mismatch", error.to_string())
        })?;
        if recomputed != intent.argument_digest {
            return Err(AgentAuthorityRefusal::of(
                "argument-digest-mismatch",
                "the intent's recorded argument digest does not match its arguments",
            ));
        }

        match &intent.request {
            AgentRunEffectRequest::Tool { call } => {
                self.authorize_tool(context, scope, task, goal, intent, call, now)
            }
            AgentRunEffectRequest::Model { .. } => {
                self.authorize_model(context, scope, task, goal, intent, now)
            }
        }
    }

    /// The tool half of [`Self::authorize`]: binding, envelope, immediate
    /// safety, credential, execution policy, checkpoint, and guardrails, in
    /// that order.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn authorize_tool(
        &self,
        context: &AgentAuthorityContext<'_>,
        scope: &AgentRunScope,
        task: Option<&AgentTaskId>,
        goal: Option<&AgentGoalId>,
        intent: &AgentRunEffect,
        call: &AgentToolCallRequest,
        now: AgentTimestampMillis,
    ) -> Result<AgentGrantedDispatch, AgentAuthorityRefusal> {
        // Layer 2, the binding: the deployment must have registered the tool.
        let Some(binding) = self.registry.binding(&call.tool) else {
            return Err(AgentAuthorityRefusal::of(
                "tool-binding-missing",
                format!(
                    "the tool {} is not registered for this deployment",
                    call.tool
                ),
            ));
        };

        // The definition must declare the tool, and the run's setup — a
        // narrowing — must not have excluded it. Checking both fails closed
        // when a later definition revision narrowed after the setup was
        // validated.
        let Some(declared) = context.definition.envelope().tools.get(&call.tool) else {
            return Err(AgentAuthorityRefusal::of(
                AgentEnvelopeDimension::Tool.as_label(),
                format!("the tool {} is not declared by the definition", call.tool),
            ));
        };
        if let Some(setup) = context.setup {
            if !setup.envelope().tools.contains_key(&call.tool) {
                return Err(AgentAuthorityRefusal::of(
                    "setup-excludes-tool",
                    format!("the run's setup does not select the tool {}", call.tool),
                ));
            }
        }

        // Immediate safety: a settings revocation reaches the next dispatch,
        // not the next run.
        if context
            .settings
            .settings()
            .revoked_tools
            .contains(&call.tool)
        {
            return Err(AgentAuthorityRefusal::of(
                "tool-revoked",
                format!("the tool {} is revoked by the current settings", call.tool),
            ));
        }

        // The binding must fit inside the definition's declaration: the
        // deployment may be stricter than the definition promised, never
        // laxer, and the pinned routing must hold.
        if binding.declaration.safety.strictness() < declared.safety.strictness() {
            return Err(AgentAuthorityRefusal::of(
                "tool-declaration-conflict",
                format!(
                    "the deployment binds {} as {} but the definition declares {}",
                    call.tool, binding.declaration.safety, declared.safety
                ),
            ));
        }
        if !binding
            .declaration
            .capabilities
            .is_subset(&declared.capabilities)
        {
            return Err(AgentAuthorityRefusal::of(
                AgentEnvelopeDimension::ToolCapability.as_label(),
                format!(
                    "the deployment grants {} a capability the definition never declared",
                    call.tool
                ),
            ));
        }
        if let Some(binding_credential) = &binding.declaration.credential_binding {
            if declared.credential_binding.as_ref() != Some(binding_credential) {
                return Err(AgentAuthorityRefusal::of(
                    AgentEnvelopeDimension::ToolCredentialBinding.as_label(),
                    format!(
                        "the deployment binds {} to a credential the definition never authorized",
                        call.tool
                    ),
                ));
            }
        }
        if binding.declaration.execution_policy != declared.execution_policy {
            return Err(AgentAuthorityRefusal::of(
                AgentEnvelopeDimension::ToolExecutionPolicy.as_label(),
                format!(
                    "the deployment routes {} through a policy other than the one the \
                     definition declared",
                    call.tool
                ),
            ));
        }

        // Layer 3, the intent: model output — or anything else — cannot have
        // widened what the binding permits.
        if intent.safety.class().strictness() < binding.declaration.safety.strictness() {
            return Err(AgentAuthorityRefusal::of(
                AgentEnvelopeDimension::EffectSafety.as_label(),
                format!(
                    "the intent claims {} but the binding declares {}",
                    intent.safety.class(),
                    binding.declaration.safety
                ),
            ));
        }
        if intent.max_attempts > binding.max_attempts {
            return Err(AgentAuthorityRefusal::of(
                "tool-policy-conflict",
                format!(
                    "the intent permits {} attempts but the binding permits {}",
                    intent.max_attempts, binding.max_attempts
                ),
            ));
        }
        if intent.credential_binding != binding.declaration.credential_binding {
            return Err(AgentAuthorityRefusal::of(
                if intent.credential_binding.is_some() {
                    AgentEnvelopeDimension::ToolCredentialBinding.as_label()
                } else {
                    "tool-policy-conflict"
                },
                format!(
                    "the intent's credential binding does not match the binding of {}",
                    call.tool
                ),
            ));
        }
        if intent.execution_policy != binding.declaration.execution_policy {
            return Err(AgentAuthorityRefusal::of(
                AgentEnvelopeDimension::ToolExecutionPolicy.as_label(),
                format!(
                    "the intent's execution policy does not match the binding of {}",
                    call.tool
                ),
            ));
        }

        // The credential class: authorized by the envelope(s), and not revoked
        // by the current settings.
        if let Some(credential) = &intent.credential_binding {
            self.check_credential(context, credential)?;
        }

        // The execution-policy routing hook: an intent that names a trust
        // class is only executed where the application accepted that class.
        self.check_execution_policy(intent.execution_policy.as_ref())?;

        // The effect-bound checkpoint gate. Until slice 1.10 lands the
        // checkpoint runtime, no grant can exist, so the requirement fails
        // closed here.
        if binding.checkpoint_required {
            return Err(AgentAuthorityRefusal::of(
                "checkpoint-required",
                format!(
                    "the tool {} requires an effect-bound checkpoint grant, and none exists",
                    call.tool
                ),
            ));
        }

        // Guardrails: every stage the envelopes and the binding require must
        // be runnable, and the tool-request boundary must allow the call.
        let mut required = self.required_stages(context);
        required.extend(binding.guardrails.iter().cloned());
        self.check_guardrail_coverage(&required)?;

        let mut tool_call = None;
        let mut reports = Vec::new();
        if let Some(chain) = &self.guardrails {
            let decision = chain.evaluate(
                crate::guardrails::AgentGuardrailBoundary::ToolRequest,
                &call.arguments,
            );
            reports = decision.reports.clone();
            match &decision.disposition {
                AgentGuardrailDisposition::Allowed => {}
                AgentGuardrailDisposition::Blocked { stage, reason_code } => {
                    return Err(AgentAuthorityRefusal::of(
                        "guardrail-blocked",
                        format!("guardrail stage {stage} blocked the call: {reason_code}"),
                    ));
                }
                AgentGuardrailDisposition::CheckpointRequired { stage, reason_code } => {
                    return Err(AgentAuthorityRefusal::of(
                        "checkpoint-required",
                        format!(
                            "guardrail stage {stage} requires a checkpoint grant, and none \
                             exists: {reason_code}"
                        ),
                    ));
                }
            }
            if decision.transformed {
                let transformed = AgentToolCallRequest::new(
                    call.call_id.clone(),
                    call.tool.clone(),
                    decision.content,
                )
                .map_err(|_| {
                    AgentAuthorityRefusal::of(
                        "guardrail-transform-oversized",
                        format!(
                            "the transformed arguments exceed the \
                             {AGENT_TOOL_ARGUMENTS_MAX_BYTES} byte bound"
                        ),
                    )
                })?;
                tool_call = Some(Box::new(transformed));
            }
        }

        Ok(AgentGrantedDispatch {
            grant: self.grant(
                context,
                scope,
                task,
                goal,
                intent,
                Some(AgentGrantDescriptor {
                    tool: binding.descriptor.tool.clone(),
                    version: binding.descriptor.version,
                    schema_digest: binding.descriptor.schema_digest(),
                }),
                binding.declaration.capabilities.clone(),
                now,
            ),
            tool_call,
            model_profile: None,
            sampling: None,
            reports,
        })
    }

    /// The model half of [`Self::authorize`]: lifecycle, profile approval,
    /// credential, execution policy, and guardrails.
    ///
    /// This is also where the turn-bound settings of
    /// [specification 7.2](../../../docs/plans/rakka-agent/spec.md) are
    /// resolved at dispatch: the granted dispatch carries the model profile
    /// and sampling the *current* settings revision selects, validated against
    /// the definition (and setup) envelope so a definition narrowed after the
    /// settings were accepted still fails closed.
    #[allow(clippy::too_many_arguments)]
    fn authorize_model(
        &self,
        context: &AgentAuthorityContext<'_>,
        scope: &AgentRunScope,
        task: Option<&AgentTaskId>,
        goal: Option<&AgentGoalId>,
        intent: &AgentRunEffect,
        now: AgentTimestampMillis,
    ) -> Result<AgentGrantedDispatch, AgentAuthorityRefusal> {
        let settings = context.settings.settings();
        let profile = settings
            .model_profile
            .clone()
            .or_else(|| match &intent.request {
                AgentRunEffectRequest::Model { profile, .. } => profile.clone(),
                AgentRunEffectRequest::Tool { .. } => None,
            });

        if let Some(profile) = &profile {
            if !context
                .definition
                .envelope()
                .model_profiles
                .contains(profile)
            {
                return Err(AgentAuthorityRefusal::of(
                    AgentEnvelopeDimension::ModelProfile.as_label(),
                    format!("the model profile {profile} is not approved by the definition"),
                ));
            }
            if let Some(setup) = context.setup {
                if !setup.envelope().model_profiles.contains(profile) {
                    return Err(AgentAuthorityRefusal::of(
                        AgentEnvelopeDimension::ModelProfile.as_label(),
                        format!("the run's setup does not select the model profile {profile}"),
                    ));
                }
            }
        }

        if let Some(credential) = &intent.credential_binding {
            self.check_credential(context, credential)?;
        }
        self.check_execution_policy(intent.execution_policy.as_ref())?;

        let required = self.required_stages(context);
        self.check_guardrail_coverage(&required)?;
        let mut reports = Vec::new();
        if let Some(chain) = &self.guardrails {
            // Until slice 1.11 gives context snapshots content, the
            // model-request boundary evaluates a bounded request descriptor —
            // enough for a kill-switch or checkpoint stage to act on.
            let content = serde_json::json!({
                "kind": "model-call",
                "profile": profile.as_ref().map(ToString::to_string),
                "turn": intent.turn,
            });
            let decision = chain.evaluate(
                crate::guardrails::AgentGuardrailBoundary::ModelRequest,
                &content,
            );
            reports = decision.reports.clone();
            match &decision.disposition {
                AgentGuardrailDisposition::Allowed => {}
                AgentGuardrailDisposition::Blocked { stage, reason_code } => {
                    return Err(AgentAuthorityRefusal::of(
                        "guardrail-blocked",
                        format!("guardrail stage {stage} blocked the model call: {reason_code}"),
                    ));
                }
                AgentGuardrailDisposition::CheckpointRequired { stage, reason_code } => {
                    return Err(AgentAuthorityRefusal::of(
                        "checkpoint-required",
                        format!(
                            "guardrail stage {stage} requires a checkpoint grant, and none \
                             exists: {reason_code}"
                        ),
                    ));
                }
            }
        }

        Ok(AgentGrantedDispatch {
            grant: self.grant(
                context,
                scope,
                task,
                goal,
                intent,
                None,
                BTreeSet::new(),
                now,
            ),
            tool_call: None,
            model_profile: profile,
            sampling: Some(settings.sampling),
            reports,
        })
    }

    /// Checks that a credential binding is authorized and not revoked.
    fn check_credential(
        &self,
        context: &AgentAuthorityContext<'_>,
        credential: &AgentCredentialBindingRef,
    ) -> Result<(), AgentAuthorityRefusal> {
        if !context
            .definition
            .envelope()
            .credential_bindings
            .contains(credential)
        {
            return Err(AgentAuthorityRefusal::of(
                AgentEnvelopeDimension::CredentialBinding.as_label(),
                format!("the credential binding {credential} is not authorized by the definition"),
            ));
        }
        if let Some(setup) = context.setup {
            if !setup.envelope().credential_bindings.contains(credential) {
                return Err(AgentAuthorityRefusal::of(
                    AgentEnvelopeDimension::CredentialBinding.as_label(),
                    format!("the run's setup does not select the credential binding {credential}"),
                ));
            }
        }
        if context
            .settings
            .settings()
            .revoked_credential_bindings
            .contains(credential)
        {
            return Err(AgentAuthorityRefusal::of(
                "credential-revoked",
                format!("the credential binding {credential} is revoked by the current settings"),
            ));
        }
        Ok(())
    }

    /// Checks that an execution-policy reference is routable here.
    fn check_execution_policy(
        &self,
        policy: Option<&AgentExecutionPolicyRef>,
    ) -> Result<(), AgentAuthorityRefusal> {
        let Some(policy) = policy else {
            return Ok(());
        };
        let routable = self
            .execution_router
            .as_ref()
            .is_some_and(|router| router.accepts(policy));
        if !routable {
            return Err(AgentAuthorityRefusal::of(
                "execution-policy-unroutable",
                format!(
                    "no configured executor accepts the execution policy {policy}; the effect \
                     stays undispatchable rather than running with ambient authority"
                ),
            ));
        }
        Ok(())
    }

    /// The guardrail stages the definition and setup envelopes require.
    fn required_stages(
        &self,
        context: &AgentAuthorityContext<'_>,
    ) -> BTreeSet<AgentGuardrailStageId> {
        let mut required = context.definition.envelope().mandatory_guardrails.clone();
        if let Some(setup) = context.setup {
            // A setup may add mandatory stages; the narrowing validation
            // already refused one that dropped any.
            required.extend(setup.envelope().mandatory_guardrails.iter().cloned());
        }
        required
    }

    /// Checks that every required stage is runnable by the configured chain.
    fn check_guardrail_coverage(
        &self,
        required: &BTreeSet<AgentGuardrailStageId>,
    ) -> Result<(), AgentAuthorityRefusal> {
        if required.is_empty() {
            return Ok(());
        }
        let Some(chain) = &self.guardrails else {
            return Err(AgentAuthorityRefusal::of(
                "guardrail-stage-missing",
                "the envelope requires guardrail stages and no chain is configured",
            ));
        };
        chain
            .validate_covers(required)
            .map_err(|error| AgentAuthorityRefusal::of(error.code(), error.to_string()))
    }

    /// Assembles the grant record for one validated intent.
    #[allow(clippy::too_many_arguments)]
    fn grant(
        &self,
        context: &AgentAuthorityContext<'_>,
        scope: &AgentRunScope,
        task: Option<&AgentTaskId>,
        goal: Option<&AgentGoalId>,
        intent: &AgentRunEffect,
        descriptor: Option<AgentGrantDescriptor>,
        capabilities: BTreeSet<AgentCapabilityId>,
        now: AgentTimestampMillis,
    ) -> AgentDispatchGrant {
        let target = intent.request.target();
        AgentDispatchGrant {
            scope: scope.clone(),
            task: task.cloned(),
            goal: goal.cloned(),
            effect_id: intent.effect_id.clone(),
            generation: intent.generation,
            descriptor,
            target: format!("{}:{}", target.target_type, target.name),
            argument_digest: intent.argument_digest.clone(),
            safety_class: intent.safety.class(),
            definition_revision: context.definition.revision(),
            settings_revision: context.settings.revision(),
            setup_revision: context.setup.map(AgentSetupRevision::revision),
            guardrail_revision: self.guardrails.as_ref().map(AgentGuardrailChain::revision),
            capabilities,
            credential_binding: intent.credential_binding.clone(),
            execution_policy: intent.execution_policy.clone(),
            issued_at: now,
            expires_at: AgentTimestampMillis::new(
                now.as_millis().saturating_add(self.grant_ttl_ms),
            ),
            allowed_use_count: intent.max_attempts,
        }
    }
}

impl Debug for AgentToolAuthority {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentToolAuthority")
            .field("registry", &self.registry)
            .field("guardrails", &self.guardrails)
            .field("grant_ttl_ms", &self.grant_ttl_ms)
            .finish_non_exhaustive()
    }
}

/// Rejection of a tool descriptor, binding, or registry operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentToolError {
    /// The mandatory model-facing description was empty.
    EmptyDescription {
        /// The tool whose descriptor was refused.
        tool: AgentToolId,
    },
    /// The description exceeded its bounded length.
    DescriptionTooLong {
        /// The tool whose descriptor was refused.
        tool: AgentToolId,
        /// Length of the rejected description, in bytes.
        length: usize,
        /// Maximum accepted length, in bytes.
        maximum: usize,
    },
    /// The inline parameter schema exceeded its bound.
    ParametersTooLarge {
        /// The tool whose descriptor was refused.
        tool: AgentToolId,
        /// Size of the rejected schema, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// The registry already holds as many tools as it may.
    RegistryFull {
        /// The maximum number of tools.
        maximum: usize,
    },
    /// A tool with the same identity is already registered.
    DuplicateTool {
        /// The duplicated tool.
        tool: AgentToolId,
    },
    /// The binding's failure policy could not be honored by the
    /// crash-and-timeout rules.
    Policy {
        /// What made it unenforceable.
        message: String,
    },
}

impl AgentToolError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyDescription { .. } => "tool-description-empty",
            Self::DescriptionTooLong { .. } => "tool-description-too-long",
            Self::ParametersTooLarge { .. } => "tool-parameters-too-large",
            Self::RegistryFull { .. } => "tool-registry-full",
            Self::DuplicateTool { .. } => "tool-already-registered",
            Self::Policy { .. } => "tool-policy-invalid",
        }
    }
}

impl Display for AgentToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDescription { tool } => {
                write!(
                    f,
                    "the tool {tool} requires a bounded model-facing description"
                )
            }
            Self::DescriptionTooLong {
                tool,
                length,
                maximum,
            } => write!(
                f,
                "the description of tool {tool} is {length} bytes, which exceeds the {maximum} \
                 byte limit"
            ),
            Self::ParametersTooLarge {
                tool,
                bytes,
                maximum,
            } => write!(
                f,
                "the parameter schema of tool {tool} is {bytes} bytes, which exceeds the \
                 {maximum} byte limit"
            ),
            Self::RegistryFull { maximum } => {
                write!(f, "a tool registry may hold at most {maximum} tools")
            }
            Self::DuplicateTool { tool } => {
                write!(f, "the tool {tool} is already registered")
            }
            Self::Policy { message } => {
                write!(f, "the tool binding's policy cannot be honored: {message}")
            }
        }
    }
}

impl Error for AgentToolError {}

impl From<AgentEffectError> for AgentToolError {
    fn from(error: AgentEffectError) -> Self {
        Self::Policy {
            message: error.to_string(),
        }
    }
}

impl From<AgentToolError> for AgentEffectError {
    fn from(error: AgentToolError) -> Self {
        Self::InvalidPolicy {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::{
        AgentDefinition, AgentDefinitionId, AgentRevisionProvenance, AgentSettingsChange,
    };
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::schema::{VersionedAgentRecord, CURRENT_AGENT_SETUP_SCHEMA_VERSION};
    use crate::task::AgentSchemaId;
    use rakka_agent_workflow::{AgentAuditEventId, AgentCausationId, PrincipalRef};

    fn schema(id: &str) -> AgentSchemaRef {
        AgentSchemaRef::new(
            AgentSchemaId::new(id).expect("the schema id is valid"),
            AgentRevisionNumber::INITIAL,
        )
    }

    fn tool_id(id: &str) -> AgentToolId {
        AgentToolId::new(id).expect("the tool id is valid")
    }

    fn descriptor(tool: &str) -> AgentToolDescriptor {
        AgentToolDescriptor::new(
            tool_id(tool),
            AgentToolKind::Function,
            "Charges a card.",
            schema("charge-input"),
            schema("charge-output"),
        )
        .expect("the descriptor is valid")
    }

    fn provenance() -> AgentRevisionProvenance {
        AgentRevisionProvenance {
            principal: PrincipalRef {
                principal_type: "service".to_string(),
                principal_id: "test".to_string(),
                display_name: None,
            },
            accepted_at: AgentTimestampMillis::new(1),
            causation_id: AgentCausationId::new("cause"),
            audit_ref: AgentAuditEventId::new("audit"),
        }
    }

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("the agent id is valid"),
            AgentRunId::new("t-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid")
    }

    fn definition_with(
        tools: BTreeMap<AgentToolId, AgentToolDeclaration>,
    ) -> AgentDefinitionRevision {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.tools = tools;
        for declaration in envelope.tools.values() {
            if let Some(credential) = &declaration.credential_binding {
                envelope.credential_bindings.insert(credential.clone());
            }
        }
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
            "Resolves tickets.",
            envelope,
        )
        .expect("the definition is valid");
        AgentDefinitionRevision::initial(definition, provenance())
    }

    fn settings_for(definition: &AgentDefinitionRevision) -> SettingsRevision {
        SettingsRevision::initial(definition, AgentSettings::default(), provenance())
            .expect("the settings are valid")
    }

    fn tool_intent(tool: &str, spec: &AgentEffectSpec) -> AgentRunEffect {
        let call = AgentToolCallRequest::new(
            crate::model::AgentToolCallId::new("call-1").expect("the call id is valid"),
            tool_id(tool),
            serde_json::json!({ "amount": 42 }),
        )
        .expect("the call is bounded");
        AgentRunEffect::new(
            &scope(),
            1,
            0,
            AgentRunEffectRequest::Tool {
                call: Box::new(call),
            },
            spec,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(1),
        )
        .expect("the effect derives")
    }

    #[test]
    fn an_unclassified_binding_fails_safe_and_the_registry_projects_policies() {
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::unclassified(descriptor("charge-card")))
            .expect("the tool registers");

        let binding = registry
            .binding(&tool_id("charge-card"))
            .expect("the binding exists");
        let spec = binding.effect_spec().expect("the spec derives");
        assert_eq!(spec.safety_class, AgentEffectSafetyClass::NonIdempotent);
        assert_eq!(spec.max_attempts, 1);

        // The projected policies stamp the same spec at commit time.
        let policies = registry.effect_policies().expect("the policies derive");
        let intent = tool_intent("charge-card", &AgentEffectSpec::non_idempotent());
        assert_eq!(policies.spec_for(&intent.request), &spec);
    }

    #[test]
    fn a_duplicate_registration_is_refused() {
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::unclassified(descriptor("charge-card")))
            .expect("the tool registers");
        let error = registry
            .register(AgentToolBinding::unclassified(descriptor("charge-card")))
            .expect_err("a duplicate is refused");
        assert_eq!(error.code(), "tool-already-registered");
    }

    #[test]
    fn the_descriptor_digest_moves_with_the_model_visible_shape() {
        let first = descriptor("charge-card");
        let same = descriptor("charge-card");
        assert_eq!(first.schema_digest(), same.schema_digest());

        let revised = descriptor("charge-card").with_version(AgentRevisionNumber::new(2));
        assert_ne!(first.schema_digest(), revised.schema_digest());
    }

    #[test]
    fn model_visibility_is_declared_and_not_revoked() {
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::unclassified(descriptor("charge-card")))
            .expect("the tool registers")
            .register(AgentToolBinding::unclassified(descriptor("refund-card")))
            .expect("the tool registers");

        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.tools.insert(
            tool_id("charge-card"),
            AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent),
        );

        // Declared and unrevoked: visible.
        let visible = registry.model_visible(&envelope, &AgentSettings::default());
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].tool, tool_id("charge-card"));

        // Revoked: invisible, though still declared and registered.
        let mut settings = AgentSettings::default();
        settings.revoked_tools.insert(tool_id("charge-card"));
        assert!(registry.model_visible(&envelope, &settings).is_empty());
    }

    #[test]
    fn an_intent_cannot_downgrade_the_bindings_safety_class() {
        // The binding declares the tool non-idempotent; an intent claiming a
        // read-only class — however it was produced — is refused. This is the
        // "model output cannot widen anything" rule at the intent layer.
        let declaration = AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent);
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::new(
                descriptor("charge-card"),
                declaration.clone(),
                1,
            ))
            .expect("the tool registers");
        let definition = definition_with(BTreeMap::from([(tool_id("charge-card"), declaration)]));
        let settings = settings_for(&definition);
        let context = AgentAuthorityContext {
            status: AgentLifecycleStatus::Active,
            definition: &definition,
            settings: &settings,
            setup: None,
        };

        let authority = AgentToolAuthority::new(registry);
        let intent = tool_intent("charge-card", &AgentEffectSpec::read_only());
        let refusal = authority
            .authorize(
                &context,
                &scope(),
                None,
                None,
                &intent,
                AgentTimestampMillis::new(2),
            )
            .expect_err("a downgraded intent is refused");
        assert_eq!(refusal.code, "downgraded-effect-safety");
        assert!(!refusal.retryable);
    }

    #[test]
    fn a_grant_validates_its_exact_intent_expiry_and_use_count() {
        let declaration = AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent);
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::new(
                descriptor("charge-card"),
                declaration.clone(),
                1,
            ))
            .expect("the tool registers");
        let definition = definition_with(BTreeMap::from([(tool_id("charge-card"), declaration)]));
        let settings = settings_for(&definition);
        let context = AgentAuthorityContext {
            status: AgentLifecycleStatus::Active,
            definition: &definition,
            settings: &settings,
            setup: None,
        };

        let authority = AgentToolAuthority::new(registry).with_grant_ttl_ms(1_000);
        let intent = tool_intent("charge-card", &AgentEffectSpec::non_idempotent());
        let granted = authority
            .authorize(
                &context,
                &scope(),
                None,
                None,
                &intent,
                AgentTimestampMillis::new(10),
            )
            .expect("the dispatch is granted");
        let grant = &granted.grant;

        // The grant binds the descriptor identity and the exact intent.
        let descriptor = grant.descriptor.as_ref().expect("a tool grant has one");
        assert_eq!(descriptor.tool, tool_id("charge-card"));
        assert_eq!(grant.allowed_use_count, 1);

        // Valid for its first attempt, inside its window.
        grant
            .validate_for(&scope(), &intent, 1, AgentTimestampMillis::new(500))
            .expect("the grant covers the attempt");

        // Spent uses and elapsed windows fail closed.
        assert_eq!(
            grant
                .validate_for(&scope(), &intent, 2, AgentTimestampMillis::new(500))
                .expect_err("a spent grant is refused")
                .code,
            "grant-uses-exhausted"
        );
        assert_eq!(
            grant
                .validate_for(&scope(), &intent, 1, AgentTimestampMillis::new(1_010))
                .expect_err("an expired grant is refused")
                .code,
            "grant-expired"
        );

        // A different generation is a different intent: the grant refuses it.
        let mut later = intent.clone();
        later.generation = later.generation.next();
        assert_eq!(
            grant
                .validate_for(&scope(), &later, 1, AgentTimestampMillis::new(500))
                .expect_err("a superseded intent is refused")
                .code,
            "grant-intent-mismatch"
        );
    }

    #[test]
    fn a_setup_that_excludes_a_declared_tool_is_enforced_at_dispatch() {
        // Scenario 44's dispatch half: the definition declares the tool, the
        // run's setup narrows it away, and the dispatch refuses even though
        // construction-time validation accepted the setup as a legal
        // narrowing.
        let declaration = AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent);
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::new(
                descriptor("charge-card"),
                declaration.clone(),
                1,
            ))
            .expect("the tool registers");
        let definition = definition_with(BTreeMap::from([(tool_id("charge-card"), declaration)]));
        let settings = settings_for(&definition);
        let setup = AgentSetupRevision::new(
            AgentRevisionNumber::INITIAL,
            &definition,
            AgentAuthorityEnvelope::empty(),
            provenance(),
        )
        .expect("an empty envelope is a legal narrowing");
        assert_eq!(
            setup.schema_version(),
            CURRENT_AGENT_SETUP_SCHEMA_VERSION,
            "the setup carries its schema version"
        );
        let context = AgentAuthorityContext {
            status: AgentLifecycleStatus::Active,
            definition: &definition,
            settings: &settings,
            setup: Some(&setup),
        };

        let authority = AgentToolAuthority::new(registry);
        let intent = tool_intent("charge-card", &AgentEffectSpec::non_idempotent());
        let refusal = authority
            .authorize(
                &context,
                &scope(),
                None,
                None,
                &intent,
                AgentTimestampMillis::new(2),
            )
            .expect_err("the setup's narrowing is enforced at dispatch");
        assert_eq!(refusal.code, "setup-excludes-tool");
    }

    #[test]
    fn an_immediate_safety_revocation_reaches_the_next_dispatch() {
        let declaration = AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent);
        let registry = AgentToolRegistry::new()
            .register(AgentToolBinding::new(
                descriptor("charge-card"),
                declaration.clone(),
                1,
            ))
            .expect("the tool registers");
        let definition = definition_with(BTreeMap::from([(tool_id("charge-card"), declaration)]));
        let settings = settings_for(&definition);
        let revoked = settings
            .apply(
                &definition,
                vec![AgentSettingsChange::RevokeTool(tool_id("charge-card"))],
                provenance(),
            )
            .expect("the revocation applies");
        let context = AgentAuthorityContext {
            status: AgentLifecycleStatus::Active,
            definition: &definition,
            settings: &revoked,
            setup: None,
        };

        let authority = AgentToolAuthority::new(registry);
        let intent = tool_intent("charge-card", &AgentEffectSpec::non_idempotent());
        let refusal = authority
            .authorize(
                &context,
                &scope(),
                None,
                None,
                &intent,
                AgentTimestampMillis::new(2),
            )
            .expect_err("a revoked tool is refused");
        assert_eq!(refusal.code, "tool-revoked");
    }

    #[test]
    fn suspension_is_a_transient_refusal_and_termination_a_definitive_one() {
        let registry = AgentToolRegistry::new();
        let definition = definition_with(BTreeMap::new());
        let settings = settings_for(&definition);
        let authority = AgentToolAuthority::new(registry);
        let intent = tool_intent("charge-card", &AgentEffectSpec::non_idempotent());

        let suspended = AgentAuthorityContext {
            status: AgentLifecycleStatus::Suspended,
            definition: &definition,
            settings: &settings,
            setup: None,
        };
        let refusal = authority
            .authorize(
                &suspended,
                &scope(),
                None,
                None,
                &intent,
                AgentTimestampMillis::new(2),
            )
            .expect_err("a suspended agent dispatches nothing");
        assert_eq!(refusal.code, "agent-dispatch-suspended");
        assert!(refusal.retryable, "a suspension may be resumed");

        let terminated = AgentAuthorityContext {
            status: AgentLifecycleStatus::Terminated,
            definition: &definition,
            settings: &settings,
            setup: None,
        };
        let refusal = authority
            .authorize(
                &terminated,
                &scope(),
                None,
                None,
                &intent,
                AgentTimestampMillis::new(2),
            )
            .expect_err("a terminated agent dispatches nothing");
        assert_eq!(refusal.code, "agent-terminated");
        assert!(!refusal.retryable);
    }
}
