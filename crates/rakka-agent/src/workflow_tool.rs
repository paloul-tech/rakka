//! Workflows as tools.
//!
//! Owns the [`AgentWorkflowToolDescriptor`] and the durable shape of one
//! workflow-tool invocation: the loop intercepts a model call naming a
//! configured workflow tool and commits — in one compare-and-set, strictly
//! before any dispatch — the [`AgentWorkflowInvocationRecord`], its cell, its
//! fan-in membership, and the start effect. Create-or-adopt is an identity
//! property, not a protocol: the invocation id derived by
//! [`workflow_invocation_id_for`] *is* the child workflow run id and the
//! `StartRun` command's deduplication key, so a replayed invocation addresses
//! the one child run's own durable inbox and adopts it rather than starting a
//! second ([specification 8.6]).
//!
//! The child's internal effects keep their own durable boundaries. A workflow
//! is never collapsed into a single opaque retryable effect, because retrying
//! it would replay every external call it already made: the only agent-side
//! effect is the start-or-adopt command, idempotent purely by its derived
//! identity, and the parent's wait is fan-in membership — durable state, not a
//! resident task.
//!
//! The `StartRun` identities are deliberately generation-free: a reconciled
//! new effect generation re-derives the identical command id and deduplication
//! key, which is what keeps an operator-driven recovery from minting a second
//! child run.
//!
//! Specification: sections 8.6 and 11.7. Filled by slice 4.5.
//!
//! [specification 8.6]: ../../../docs/plans/rakka-agent/spec.md

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    trigger_cancel_run_command, trigger_start_run_command, AgentCommand, AgentCommandId,
    AgentCommandMetadata, AgentDurabilityMetadata, AgentEffectId, AgentRunId as WorkflowRunId,
    AgentTelemetryContext, AgentTenantId, AgentTimestampMillis, AgentTriggerSource,
    AgentWorkflowId, AgentWorkflowKey, ArtifactRef, WorkflowDefinitionVersion,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::definition::{
    AgentCapabilityId, AgentCredentialBindingRef, AgentEffectSafetyClass, AgentRevisionNumber,
    AgentWorkflowToolId,
};
use crate::identity::{
    validate_tenant, AgentGoalId, AgentIdentityError, AgentOperationId, AgentOperationKind,
    AgentRunScope, AgentTaskId, AgentWorkflowInvocationId, TenantId,
};
use crate::task::{AgentContentDigest, AgentSchemaRef, AgentTaskContent};

/// Result type for workflow-tool construction, validation, and interception.
pub type AgentWorkflowToolResult<T> = Result<T, AgentWorkflowToolError>;

/// Prefix of every derived [`AgentWorkflowInvocationId`].
///
/// The suffix is a fixed-length digest, so the id always satisfies the
/// identity bounds whatever the parent scope contains, and an id without this
/// prefix was not derived by [`workflow_invocation_id_for`]. Disjointness from
/// [`crate::delegation::AGENT_DELEGATION_ID_PREFIX`] is load-bearing: fan-in
/// membership distinguishes its member kinds by exactly this prefix.
pub const AGENT_WORKFLOW_INVOCATION_ID_PREFIX: &str = "workflow-invocation-";

/// Maximum workflow-invocation cells one run retains.
///
/// Defined as [`crate::delegation::AGENT_RUN_MAX_DELEGATIONS`] — not restated —
/// because the enforced door bound is the *combined* fan-in membership count
/// ([`crate::fan_in::AGENT_RUN_MAX_FAN_IN_MEMBERS`], the same value): a run's
/// workflow-invocation count can never exceed the combined bound, so the two
/// constants must move together rather than drift apart.
pub const AGENT_RUN_MAX_WORKFLOW_INVOCATIONS: usize = crate::delegation::AGENT_RUN_MAX_DELEGATIONS;

/// Maximum workflow-tool descriptors one run's wiring declares.
pub const AGENT_RUN_MAX_WORKFLOW_TOOLS: usize = 32;

/// Maximum serialized bytes of one [`AgentWorkflowInvocationRecord`].
///
/// The record rides the parent run's bounded durable state, so an input that
/// does not fit inline belongs behind an artifact reference — the delegation
/// record's discipline.
pub const AGENT_WORKFLOW_INVOCATION_RECORD_MAX_BYTES: usize = 8 * 1024;

/// Maximum serialized bytes of one [`AgentWorkflowToolDescriptor`].
pub const AGENT_WORKFLOW_TOOL_DESCRIPTOR_MAX_BYTES: usize = 8 * 1024;

/// Maximum bytes of a descriptor's model-facing description.
pub const AGENT_WORKFLOW_TOOL_DESCRIPTION_MAX_LENGTH: usize = 1024;

/// Maximum serialized bytes of a descriptor's inline parameter schema.
pub const AGENT_WORKFLOW_TOOL_PARAMETERS_MAX_BYTES: usize = 4 * 1024;

/// Maximum bytes of a descriptor's workflow type.
pub const AGENT_WORKFLOW_TOOL_TYPE_MAX_BYTES: usize = 256;

/// Maximum capabilities one descriptor requires.
pub const AGENT_WORKFLOW_TOOL_MAX_CAPABILITIES: usize = 32;

/// The canonical code under which a workflow start's conflict settles.
///
/// The dispatch layer normalizes every
/// [`crate::dispatch::AgentWorkflowStartFinding::Conflict`] onto this code —
/// folding the executor's own code into the failure message — so the run
/// entity's conflict classification is structural, never an executor
/// convention: whatever code the application reports, a conflict settles the
/// cell [`AgentWorkflowInvocationStatus::Conflicted`] under this one code.
pub const AGENT_WORKFLOW_INVOCATION_CONFLICT_CODE: &str = "workflow-invocation-conflict";

/// Default attempt ceiling of the workflow start effect.
///
/// The start is idempotent by construction — the derived, generation-free
/// `StartRun` identities make every retry converge on the same child run — so
/// retrying a transient delivery failure is safe and cheap.
pub const AGENT_WORKFLOW_START_DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Default attempt ceiling of the workflow cancel effect
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The cancel is idempotent by construction — the derived, generation-free
/// `CancelRun` identities make every retry converge on one logical request —
/// so retrying a transient delivery failure is safe and cheap.
pub const AGENT_WORKFLOW_CANCEL_DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Derives the identity of the workflow invocation one run's turn commits in
/// one slot.
///
/// The derivation is pure — the delegation id's digest construction: every
/// segment is length-prefixed so the encoding is injective — and shares the
/// `(turn, slot)` coordinate with the effect that carries the start, so
/// replaying the transition that decided the invocation resolves to the same
/// identity, the same child run id, and the same `StartRun` deduplication key.
pub fn workflow_invocation_id_for(
    scope: &AgentRunScope,
    turn: u64,
    slot: usize,
) -> AgentWorkflowToolResult<AgentWorkflowInvocationId> {
    validate_tenant(scope.tenant())?;
    let mut canonical = Vec::new();
    for segment in [
        scope.tenant().as_str(),
        scope.agent().as_str(),
        scope.run().as_str(),
        &turn.to_string(),
        &slot.to_string(),
    ] {
        canonical.extend_from_slice(segment.len().to_string().as_bytes());
        canonical.push(b':');
        canonical.extend_from_slice(segment.as_bytes());
    }
    let digest = AgentContentDigest::sha256_of_bytes(&canonical);
    Ok(AgentWorkflowInvocationId::new(format!(
        "{AGENT_WORKFLOW_INVOCATION_ID_PREFIX}{}",
        digest.value
    ))?)
}

/// The child workflow run id one invocation creates or adopts: the invocation
/// id verbatim.
///
/// Every durable identity of the child — its inbox, its persistence id, its
/// entity id — derives from this run id, so a replayed start addresses the
/// same durable inbox and deduplicates instead of creating a second run.
#[must_use]
pub fn child_workflow_run_id(invocation: &AgentWorkflowInvocationId) -> WorkflowRunId {
    WorkflowRunId::new(invocation.as_str())
}

/// The derived, generation-free command id of the one `StartRun` this
/// invocation ever sends.
///
/// Generation-freedom is load-bearing: an operator-created new effect
/// generation re-derives the identical command id, so recovery can never mint
/// a second child run — the child inbox answers the replay as a duplicate,
/// which the executor reports as adoption.
#[must_use]
pub fn workflow_start_command_id(invocation: &AgentWorkflowInvocationId) -> AgentCommandId {
    AgentCommandId::new(format!("{}#start-run", invocation.as_str()))
}

/// The derived, generation-free command id of the one `CancelRun` this
/// invocation ever sends ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// Generation-freedom is load-bearing for the same reason as the start's: a
/// reconciled new effect generation re-derives the identical command id, so
/// recovery can never deliver a second logical cancellation — the child inbox
/// answers the replay as a duplicate.
#[must_use]
pub fn workflow_cancel_command_id(invocation: &AgentWorkflowInvocationId) -> AgentCommandId {
    AgentCommandId::new(format!("{}#cancel-run", invocation.as_str()))
}

/// Derives the stable operation id of the one workflow result a child run
/// ever owes its parent.
///
/// Pure over `(tenant, invocation)`: the child's terminal status is absorbing,
/// so one logical result exists per invocation, ever, and every re-drive of
/// the application-owed relay owes the identical operation.
pub fn workflow_result_operation_id(
    tenant: &TenantId,
    invocation: &AgentWorkflowInvocationId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::WorkflowResult,
        [tenant.as_str(), invocation.as_str(), "result"],
    )
}

/// The versioned descriptor under which a compiled workflow appears in an
/// agent's toolset ([specification 8.6 and 11.7], open decision 16).
///
/// Trusted deployment data, never model output: the descriptor names the
/// workflow definition and version it invokes, the schemas its input and
/// output satisfy, and the authority its dispatch may exercise. The declared
/// safety class describes the workflow's *contained* effects — the tool's
/// apparent safety can never be stronger than the internal effects and policy
/// it admits.
///
/// No resolved credential ever appears here: the credential binding is a
/// logical reference resolved inside the dispatch attempt's own boundary.
///
/// [specification 8.6 and 11.7]: ../../../docs/plans/rakka-agent/spec.md
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentWorkflowToolDescriptor {
    /// The workflow tool's stable identity: the envelope and goal-scope key,
    /// and the name the model calls.
    pub workflow_tool: AgentWorkflowToolId,
    /// Monotonic descriptor version. Bumped whenever the invoked shape
    /// changes, so a recorded invocation can prove which shape it validated.
    pub version: AgentRevisionNumber,
    /// The workflow type the invocation starts, resolved by the application
    /// against its own workflow registry.
    pub workflow_type: String,
    /// The workflow definition version the invocation pins.
    pub definition_version: WorkflowDefinitionVersion,
    /// Bounded model-facing description.
    pub description: String,
    /// Versioned reference to the workflow's input schema.
    pub input_schema: AgentSchemaRef,
    /// Versioned reference to the workflow's output schema.
    pub output_schema: AgentSchemaRef,
    /// Inline model-visible parameter schema, when the deployment surfaces
    /// one.
    #[serde(default)]
    pub parameters: Option<Value>,
    /// Scoped capabilities the workflow's contained effects may exercise.
    /// Copied onto every invocation record at commit and carried on the
    /// dispatch grant per attempt. The definition envelope declares workflow
    /// tools by id only, so a per-workflow-tool capability *narrowing* check
    /// (the regular tool declaration's subset discipline) awaits an
    /// envelope-side declaration — recorded follow-up work.
    #[serde(default)]
    pub required_capabilities: BTreeSet<AgentCapabilityId>,
    /// Logical credential binding the start dispatch may resolve.
    #[serde(default)]
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// The declared safety class of the workflow's *contained* effects.
    pub safety_class: AgentEffectSafetyClass,
    /// Default child deadline, in milliseconds from the invocation, when the
    /// deployment declares one; the envelope's deadline still bounds it.
    #[serde(default)]
    pub default_deadline_ms: Option<u64>,
    /// Whether the workflow declares durable cancellation support
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)). A
    /// winding-down parent commits the `CancelRun`-delivering effect only
    /// under a `true` declaration; under `false` it records a durable
    /// `Unsupported` disposition and waits for the child's natural terminal
    /// result.
    #[serde(default)]
    pub supports_cancellation: bool,
    /// Whether the workflow declares compensation support.
    #[serde(default)]
    pub supports_compensation: bool,
    /// Whether every invocation requires an approval checkpoint bound to the
    /// exact effect ([specification 12](../../../docs/plans/rakka-agent/spec.md)).
    #[serde(default)]
    pub checkpoint_required: bool,
    /// Whether every invocation requires a security authorization checkpoint.
    #[serde(default)]
    pub authorization_required: bool,
}

impl AgentWorkflowToolDescriptor {
    /// Creates a descriptor with the fail-safe defaults: version one, a
    /// non-idempotent contained-safety class, no capabilities, no credential,
    /// and no checkpoint gates.
    pub fn new(
        workflow_tool: AgentWorkflowToolId,
        workflow_type: impl Into<String>,
        definition_version: WorkflowDefinitionVersion,
        description: impl Into<String>,
        input_schema: AgentSchemaRef,
        output_schema: AgentSchemaRef,
    ) -> AgentWorkflowToolResult<Self> {
        let descriptor = Self {
            workflow_tool,
            version: AgentRevisionNumber::INITIAL,
            workflow_type: workflow_type.into(),
            definition_version,
            description: description.into(),
            input_schema,
            output_schema,
            parameters: None,
            required_capabilities: BTreeSet::new(),
            credential_binding: None,
            safety_class: AgentEffectSafetyClass::NonIdempotent,
            default_deadline_ms: None,
            supports_cancellation: false,
            supports_compensation: false,
            checkpoint_required: false,
            authorization_required: false,
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
    pub fn with_parameters(mut self, parameters: Value) -> AgentWorkflowToolResult<Self> {
        self.parameters = Some(parameters);
        self.validate()?;
        Ok(self)
    }

    /// Adds a required capability.
    pub fn with_capability(
        mut self,
        capability: AgentCapabilityId,
    ) -> AgentWorkflowToolResult<Self> {
        self.required_capabilities.insert(capability);
        self.validate()?;
        Ok(self)
    }

    /// Sets the logical credential binding.
    #[must_use]
    pub fn with_credential_binding(mut self, binding: AgentCredentialBindingRef) -> Self {
        self.credential_binding = Some(binding);
        self
    }

    /// Declares the contained-safety class.
    #[must_use]
    pub const fn with_safety_class(mut self, safety: AgentEffectSafetyClass) -> Self {
        self.safety_class = safety;
        self
    }

    /// Sets the default child deadline, in milliseconds from the invocation.
    #[must_use]
    pub const fn with_default_deadline_ms(mut self, deadline_ms: u64) -> Self {
        self.default_deadline_ms = Some(deadline_ms);
        self
    }

    /// Declares durable cancellation support.
    #[must_use]
    pub const fn with_cancellation_support(mut self) -> Self {
        self.supports_cancellation = true;
        self
    }

    /// Declares compensation support.
    #[must_use]
    pub const fn with_compensation_support(mut self) -> Self {
        self.supports_compensation = true;
        self
    }

    /// Requires an approval checkpoint on every invocation.
    #[must_use]
    pub const fn require_checkpoint(mut self) -> Self {
        self.checkpoint_required = true;
        self
    }

    /// Requires a security authorization checkpoint on every invocation.
    #[must_use]
    pub const fn require_authorization(mut self) -> Self {
        self.authorization_required = true;
        self
    }

    /// The registry key naming the workflow this descriptor invokes.
    #[must_use]
    pub fn workflow_key(&self) -> AgentWorkflowKey {
        AgentWorkflowKey::new(self.workflow_type.clone(), self.definition_version.clone())
    }

    /// Rejects a descriptor that exceeds its structural bounds.
    ///
    /// The whole descriptor refuses rather than truncating, the delegation
    /// record's discipline: trusted wiring that does not fit its bounds is a
    /// deployment error, never something to silently shrink.
    pub fn validate(&self) -> AgentWorkflowToolResult<()> {
        if self.description.is_empty()
            || self.description.len() > AGENT_WORKFLOW_TOOL_DESCRIPTION_MAX_LENGTH
        {
            return Err(AgentWorkflowToolError::DescriptorInvalid {
                message: format!(
                    "the description is {} bytes; it must be non-empty and at most {} bytes",
                    self.description.len(),
                    AGENT_WORKFLOW_TOOL_DESCRIPTION_MAX_LENGTH
                ),
            });
        }
        if self.workflow_type.is_empty()
            || self.workflow_type.len() > AGENT_WORKFLOW_TOOL_TYPE_MAX_BYTES
        {
            return Err(AgentWorkflowToolError::DescriptorInvalid {
                message: format!(
                    "the workflow type is {} bytes; it must be non-empty and at most {} bytes",
                    self.workflow_type.len(),
                    AGENT_WORKFLOW_TOOL_TYPE_MAX_BYTES
                ),
            });
        }
        if self.required_capabilities.len() > AGENT_WORKFLOW_TOOL_MAX_CAPABILITIES {
            return Err(AgentWorkflowToolError::DescriptorInvalid {
                message: format!(
                    "{} required capabilities exceed the {} bound",
                    self.required_capabilities.len(),
                    AGENT_WORKFLOW_TOOL_MAX_CAPABILITIES
                ),
            });
        }
        if let Some(parameters) = &self.parameters {
            let bytes = serde_json::to_vec(parameters)
                .map(|encoded| encoded.len())
                .unwrap_or(usize::MAX);
            if bytes > AGENT_WORKFLOW_TOOL_PARAMETERS_MAX_BYTES {
                return Err(AgentWorkflowToolError::DescriptorInvalid {
                    message: format!(
                        "the inline parameter schema is {bytes} bytes, which exceeds the {} \
                         byte bound",
                        AGENT_WORKFLOW_TOOL_PARAMETERS_MAX_BYTES
                    ),
                });
            }
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|error| AgentWorkflowToolError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_WORKFLOW_TOOL_DESCRIPTOR_MAX_BYTES {
            return Err(AgentWorkflowToolError::DescriptorTooLarge {
                bytes,
                maximum: AGENT_WORKFLOW_TOOL_DESCRIPTOR_MAX_BYTES,
            });
        }
        Ok(())
    }

    /// Canonical fingerprint of the descriptor's shape, recorded on every
    /// invocation so replay and audit can prove which shape was invoked
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md):
    /// catalog-drift detection).
    #[must_use]
    pub fn schema_digest(&self) -> AgentContentDigest {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        AgentContentDigest::of_json(&value)
    }
}

/// The wiring one run entity needs to serve workflow tools
/// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Each configured descriptor appears to the model as its own named tool —
/// the workflow tool id is the call name, the workflow input is the
/// arguments — and the loop intercepts the call by this map, never by the
/// generic tool path.
#[derive(Debug, Clone, Default)]
pub struct AgentRunWorkflowConfig {
    descriptors: BTreeMap<AgentWorkflowToolId, AgentWorkflowToolDescriptor>,
}

impl AgentRunWorkflowConfig {
    /// Creates an empty configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a validated descriptor, refusing a duplicate tool or a
    /// configuration past its bound.
    pub fn with_descriptor(
        mut self,
        descriptor: AgentWorkflowToolDescriptor,
    ) -> AgentWorkflowToolResult<Self> {
        descriptor.validate()?;
        if self.descriptors.contains_key(&descriptor.workflow_tool) {
            return Err(AgentWorkflowToolError::DuplicateTool {
                tool: descriptor.workflow_tool.clone(),
            });
        }
        if self.descriptors.len() >= AGENT_RUN_MAX_WORKFLOW_TOOLS {
            return Err(AgentWorkflowToolError::ToolLimitExceeded {
                maximum: AGENT_RUN_MAX_WORKFLOW_TOOLS,
            });
        }
        self.descriptors
            .insert(descriptor.workflow_tool.clone(), descriptor);
        Ok(self)
    }

    /// One configured descriptor, when the tool is declared.
    #[must_use]
    pub fn descriptor(&self, tool: &AgentWorkflowToolId) -> Option<&AgentWorkflowToolDescriptor> {
        self.descriptors.get(tool)
    }

    /// The configured descriptors, keyed by tool.
    #[must_use]
    pub const fn descriptors(&self) -> &BTreeMap<AgentWorkflowToolId, AgentWorkflowToolDescriptor> {
        &self.descriptors
    }

    /// Whether any workflow tool is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }
}

/// One durable workflow-tool invocation: the assignment of work from a parent
/// run to an independently durable child workflow run
/// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Persisted in the same compare-and-set that commits the start effect,
/// strictly before any dispatch. Every identity below is a pure derivation of
/// the parent's `(turn, slot)` coordinate, and the descriptor's resolved
/// shape is copied at commit — a replay never re-resolves, so a descriptor
/// upgrade mid-flight is a visible conflict rather than a silently different
/// child.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentWorkflowInvocationRecord {
    /// The invocation identity, derived by [`workflow_invocation_id_for`].
    pub invocation: AgentWorkflowInvocationId,
    /// The collaborative goal the parent serves, when it serves one.
    #[serde(default)]
    pub goal: Option<AgentGoalId>,
    /// The parent task whose run invokes.
    pub parent_task: AgentTaskId,
    /// The invoking run.
    pub parent_run: AgentRunScope,
    /// The workflow tool the model called.
    pub workflow_tool: AgentWorkflowToolId,
    /// The descriptor version the invocation validated under.
    pub descriptor_version: AgentRevisionNumber,
    /// The descriptor's canonical fingerprint at commit.
    pub descriptor_digest: AgentContentDigest,
    /// The workflow type the invocation starts, copied from the descriptor at
    /// commit.
    pub workflow_type: String,
    /// The workflow definition version the invocation pins, copied from the
    /// descriptor at commit.
    pub definition_version: WorkflowDefinitionVersion,
    /// The scoped capabilities the descriptor declared for the workflow's
    /// contained effects, copied at commit so the dispatch grant carries the
    /// authorized surface every attempt. A record persisted before this field
    /// decodes to the empty set.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub required_capabilities: BTreeSet<AgentCapabilityId>,
    /// The child workflow run this invocation creates or adopts: the
    /// invocation id verbatim ([`child_workflow_run_id`]).
    pub child_run: WorkflowRunId,
    /// The `StartRun` deduplication key: the invocation id verbatim.
    pub deduplication_key: String,
    /// The parent turn that committed the invocation.
    pub turn: u64,
    /// The effect slot within the turn.
    pub slot: usize,
    /// The start effect's derived identity.
    pub effect: AgentEffectId,
    /// The model tool call this invocation answers — its causation. The
    /// outcome transition records the start's bounded receipt as this call's
    /// tool result, which is how the turn completes.
    pub call_id: crate::model::AgentToolCallId,
    /// The bounded input the child workflow run is started with.
    pub input: AgentTaskContent,
    /// The child's deadline, in epoch milliseconds: the descriptor's default
    /// and the envelope's deadline, min-narrowed at commit.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
    /// The agent definition revision the parent decided under.
    pub definition_revision: AgentRevisionNumber,
    /// The agent settings revision the parent decided under.
    pub settings_revision: AgentRevisionNumber,
    /// Trace propagation for the start.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
    /// When the record was committed.
    pub created_at: AgentTimestampMillis,
}

impl AgentWorkflowInvocationRecord {
    /// Rejects a record that exceeds its structural bounds, or whose derived
    /// identities disagree.
    ///
    /// The coherence checks keep the create-or-adopt convergence
    /// non-forgeable: the child run id and the deduplication key must both be
    /// the invocation id verbatim, so no hand-built record can point one
    /// logical invocation at a foreign child run.
    pub fn validate(&self) -> AgentWorkflowToolResult<()> {
        if self.child_run.as_str() != self.invocation.as_str() {
            return Err(AgentWorkflowToolError::RecordIncoherent {
                message: "the child run id must be the invocation id verbatim".to_string(),
            });
        }
        if self.deduplication_key != self.invocation.as_str() {
            return Err(AgentWorkflowToolError::RecordIncoherent {
                message: "the deduplication key must be the invocation id verbatim".to_string(),
            });
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|error| AgentWorkflowToolError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_WORKFLOW_INVOCATION_RECORD_MAX_BYTES {
            return Err(AgentWorkflowToolError::RecordTooLarge {
                bytes,
                maximum: AGENT_WORKFLOW_INVOCATION_RECORD_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// Where one workflow invocation stands.
///
/// `Pending` is the only unsettled state: the record is durable and the start
/// effect is committed, but no receipt has returned. Every other variant is
/// settled and absorbing for this invocation identity — recovery after
/// ambiguity re-drives the *same* start, whose derived identity adopts rather
/// than duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWorkflowInvocationStatus {
    /// The record is persisted and the start effect is committed or in
    /// flight.
    Pending,
    /// The start durably reached the child run's inbox.
    Started {
        /// Whether the child already existed: the inbox answered the derived
        /// `StartRun` as a duplicate, which is adoption, not an error.
        #[serde(default)]
        adopted: bool,
    },
    /// A child run exists that this invocation's identity does not own — a
    /// mismatched workflow type or version, or a foreign command id behind
    /// the same deduplication key.
    Conflicted {
        /// Stable machine-readable conflict code.
        code: String,
    },
    /// The start failed definitively without reaching the child.
    Failed {
        /// Stable machine-readable failure code.
        code: String,
    },
}

impl AgentWorkflowInvocationStatus {
    /// Whether the invocation reached a settled state.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self, Self::Pending)
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Started { .. } => "started",
            Self::Conflicted { .. } => "conflicted",
            Self::Failed { .. } => "failed",
        }
    }
}

/// The terminal status one child workflow run reported.
///
/// A closed terminal set by construction: the result command's door refuses
/// anything non-terminal, so a "result" for a still-running child is
/// unrepresentable rather than merely refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWorkflowTerminalStatus {
    /// The child run completed.
    Completed,
    /// The child run failed.
    Failed,
    /// The child run was cancelled.
    Cancelled,
}

impl AgentWorkflowTerminalStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl Display for AgentWorkflowTerminalStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The bounded terminal outcome one child workflow run returned through the
/// deduplicated result command ([specification 8.6]: workflow completion
/// returns a deduplicated result/evidence reference).
///
/// References only, never content: the artifact reference and digest
/// fingerprint the child's result, and the child run id — already on the
/// record — is the authorized-query handle for anything more
/// ([`crate::query::authorized_agent_goal_view`] assembles the goal-wide
/// view those handles key into).
///
/// [specification 8.6]: ../../../docs/plans/rakka-agent/spec.md
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentWorkflowChildResult {
    /// The child run's terminal status.
    pub status: AgentWorkflowTerminalStatus,
    /// The child's stable terminal-reason code, when it recorded one. The
    /// recording transition truncates it to
    /// [`crate::run::AGENT_RUN_DETAIL_MAX_LENGTH`] bytes — the run's uniform
    /// detail bound — rather than refusing the child's report over its
    /// wording.
    #[serde(default)]
    pub terminal_reason: Option<String>,
    /// The child's result artifact reference, when one exists.
    #[serde(default)]
    pub result_ref: Option<ArtifactRef>,
    /// Content digest of the child's result, when one exists.
    #[serde(default)]
    pub result_digest: Option<AgentContentDigest>,
    /// Descendant tasks the child's own subtree created. Zero in this slice —
    /// a workflow run cannot create agent tasks yet — and recorded so a later
    /// slice's credit fold has uniform data.
    #[serde(default)]
    pub descendants_created: u64,
    /// When the parent recorded the result.
    pub recorded_at: AgentTimestampMillis,
}

/// The wind-down disposition of one invocation's workflow-cancel request
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)): the
/// durable, observable outcome of the propagation decision, recorded in the
/// same compare-and-set that made it, and the once-guard that keeps a
/// re-entered wind-down from committing a second cancel effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWorkflowCancelDisposition {
    /// The cancel effect was committed; its delivery settles independently,
    /// and the child's terminal outcome still returns through the result
    /// relay.
    Committed {
        /// The effect carrying the request.
        effect: AgentEffectId,
    },
    /// The descriptor declares no cancellation support
    /// (`supports_cancellation` is `false`), or the invocation's tool is no
    /// longer wired: no effect exists, and the parent waits for the child's
    /// natural terminal result.
    Unsupported,
    /// The wind-down could not afford the request's attempts: no effect
    /// exists, and the parent waits for the child's natural terminal result
    /// rather than blocking its own quiescence on budget.
    Unaffordable,
}

/// One workflow invocation's durable home on the parent run's loop state.
///
/// The cell commits with the start effect and settles in the same
/// compare-and-set that applies the effect's outcome, so the record, the
/// effect, and the status can never disagree about what happened. The
/// child's result is a separate field from the start's status — a result may
/// arrive before the start receipt, and both record independently,
/// first-writer-wins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentWorkflowInvocationCell {
    /// The durable record, persisted before the start.
    pub record: Box<AgentWorkflowInvocationRecord>,
    /// Where the invocation stands.
    pub status: AgentWorkflowInvocationStatus,
    /// When the status settled, when it has.
    #[serde(default)]
    pub settled_at: Option<AgentTimestampMillis>,
    /// The child's terminal outcome, once its result returned. First writer
    /// wins: one logical result per invocation, ever.
    #[serde(default)]
    pub result: Option<AgentWorkflowChildResult>,
    /// The wind-down disposition of this invocation's cancel request, once a
    /// wind-down decided one
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    /// Records persisted before this field load without one.
    #[serde(default)]
    pub cancel: Option<AgentWorkflowCancelDisposition>,
}

impl AgentWorkflowInvocationCell {
    /// Creates the pending cell committed alongside the start effect.
    #[must_use]
    pub fn pending(record: Box<AgentWorkflowInvocationRecord>) -> Self {
        Self {
            record,
            status: AgentWorkflowInvocationStatus::Pending,
            settled_at: None,
            result: None,
            cancel: None,
        }
    }

    /// Records the wind-down's cancel disposition, first-writer-wins: one
    /// logical request per invocation, ever.
    pub fn record_cancel_disposition(&mut self, disposition: AgentWorkflowCancelDisposition) {
        if self.cancel.is_none() {
            self.cancel = Some(disposition);
        }
    }

    /// Whether the child this cell started has recorded its terminal outcome.
    ///
    /// A cell that never reached its child — settled `Conflicted` or
    /// `Failed` — answers `false`; its settlement, not a child result, is
    /// what released its debits.
    #[must_use]
    pub const fn child_settled(&self) -> bool {
        self.result.is_some()
    }

    /// Records the child's terminal outcome, first-writer-wins: a duplicate
    /// delivery of one logical result cannot rewrite history.
    pub fn record_child_result(&mut self, result: AgentWorkflowChildResult) {
        if self.result.is_none() {
            self.result = Some(result);
        }
    }

    /// Settles the cell with the durably started — or adopted — child.
    ///
    /// Settlement is first-writer-wins: a cell that already settled keeps its
    /// original outcome, so a duplicate outcome delivery cannot rewrite
    /// history.
    pub fn settle_started(&mut self, adopted: bool, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentWorkflowInvocationStatus::Started { adopted };
        self.settled_at = Some(now);
    }

    /// Settles the cell with an explicit conflict.
    pub fn settle_conflicted(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentWorkflowInvocationStatus::Conflicted { code: code.into() };
        self.settled_at = Some(now);
    }

    /// Settles the cell with a definitive failure.
    pub fn settle_failed(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentWorkflowInvocationStatus::Failed { code: code.into() };
        self.settled_at = Some(now);
    }
}

/// The bounded receipt one completed workflow start returns.
///
/// Identities and the adoption flag only — never workflow content, and never
/// evidence that the child progressed: reaching the child's durable inbox is
/// all the start ever claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowStartReceipt {
    /// The invocation the start carried.
    pub invocation: AgentWorkflowInvocationId,
    /// The child workflow run the start durably reached.
    pub child_run: WorkflowRunId,
    /// Whether the child already existed: the derived `StartRun` deduplicated
    /// against the child's inbox or run state.
    #[serde(default)]
    pub adopted: bool,
}

/// Builds the validated, trigger-normalized `StartRun` command one invocation
/// sends its child run.
///
/// Pure over the record: the command id, deduplication key, run id, causation,
/// and correlation all derive from the invocation, so every re-drive of any
/// generation builds the identical command and the child inbox answers a
/// replay as a duplicate. The application resolves `workflow_id` against its
/// own registry — the record pins the workflow type and definition version it
/// must resolve to — and owes delivery to the child run's sharded inbox
/// entity, mapping acceptance to a started finding and duplicate acceptance
/// to adoption.
pub fn workflow_start_command(
    record: &AgentWorkflowInvocationRecord,
    workflow_id: AgentWorkflowId,
    payload_ref: Option<ArtifactRef>,
    received_at: AgentTimestampMillis,
) -> AgentWorkflowToolResult<AgentCommand> {
    let metadata = AgentCommandMetadata::new(
        workflow_id,
        record.child_run.clone(),
        workflow_start_command_id(&record.invocation),
        AgentDurabilityMetadata {
            deduplication_key: rakka_agent_workflow::AgentDeduplicationKey::new(
                record.deduplication_key.clone(),
            ),
            causation_id: rakka_agent_workflow::AgentCausationId::new(record.invocation.as_str()),
            correlation_id: rakka_agent_workflow::AgentCorrelationId::new(record.parent_run.key()),
            telemetry_context: record.telemetry.clone(),
        },
        AgentTenantId::new(record.parent_run.tenant().as_str()),
        received_at,
    )
    .map_err(|error| AgentWorkflowToolError::Command {
        message: error.to_string(),
    })?;
    trigger_start_run_command(metadata, AgentTriggerSource::child_workflow(), payload_ref).map_err(
        |error| AgentWorkflowToolError::Command {
            message: error.to_string(),
        },
    )
}

/// Builds the durable `CancelRun` command one workflow-cancel effect delivers
/// to the child's inbox ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The command id is the derived, generation-free
/// [`workflow_cancel_command_id`], and the deduplication key mirrors it, so
/// every retry and every reconciled new effect generation converges on one
/// logical request in the child's own durable inbox. Delivery is the whole
/// claim: the child's scheduler quiesces under its own cancellation record,
/// and its terminal outcome returns through the result relay.
pub fn workflow_cancel_command(
    record: &AgentWorkflowInvocationRecord,
    workflow_id: AgentWorkflowId,
    received_at: AgentTimestampMillis,
) -> AgentWorkflowToolResult<AgentCommand> {
    let command_id = workflow_cancel_command_id(&record.invocation);
    let metadata = AgentCommandMetadata::new(
        workflow_id,
        record.child_run.clone(),
        command_id.clone(),
        AgentDurabilityMetadata {
            deduplication_key: rakka_agent_workflow::AgentDeduplicationKey::new(
                command_id.as_str(),
            ),
            causation_id: rakka_agent_workflow::AgentCausationId::new(record.invocation.as_str()),
            correlation_id: rakka_agent_workflow::AgentCorrelationId::new(record.parent_run.key()),
            telemetry_context: record.telemetry.clone(),
        },
        AgentTenantId::new(record.parent_run.tenant().as_str()),
        received_at,
    )
    .map_err(|error| AgentWorkflowToolError::Command {
        message: error.to_string(),
    })?;
    trigger_cancel_run_command(metadata, AgentTriggerSource::child_workflow(), None).map_err(
        |error| AgentWorkflowToolError::Command {
            message: error.to_string(),
        },
    )
}

/// Errors of workflow-tool construction, validation, and interception.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentWorkflowToolError {
    /// A descriptor field exceeds its structural bounds.
    DescriptorInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// The descriptor exceeds its serialized-size bound.
    DescriptorTooLarge {
        /// Actual serialized bytes.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The configuration already declares the tool.
    DuplicateTool {
        /// The duplicated workflow tool.
        tool: AgentWorkflowToolId,
    },
    /// The configuration already declares its maximum tools.
    ToolLimitExceeded {
        /// The bound.
        maximum: usize,
    },
    /// The called workflow tool is outside the goal's allowed set.
    NotAllowed {
        /// The workflow tool the model called.
        tool: AgentWorkflowToolId,
    },
    /// A workflow invocation was planned after the same turn's await closed
    /// the group — the member would revive a superseded wind-down.
    AfterAwait,
    /// The run already retains its maximum workflow-invocation cells, or the
    /// combined fan-in membership would exceed its bound.
    LimitExceeded {
        /// The bound.
        maximum: usize,
    },
    /// The record would exceed the run's materialized headroom.
    HeadroomExceeded {
        /// The bytes the commit needs.
        needed: usize,
        /// The headroom available.
        available: usize,
    },
    /// The record exceeds its serialized-size bound.
    RecordTooLarge {
        /// Actual serialized bytes.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The record's derived identities disagree.
    RecordIncoherent {
        /// The coherence failure detail.
        message: String,
    },
    /// A value could not be encoded.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
    /// The `StartRun` command could not be built.
    Command {
        /// The construction failure detail.
        message: String,
    },
    /// An identity derivation failed.
    Identity(AgentIdentityError),
}

impl AgentWorkflowToolError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DescriptorInvalid { .. } => "workflow-tool-descriptor-invalid",
            Self::DescriptorTooLarge { .. } => "workflow-tool-descriptor-too-large",
            Self::DuplicateTool { .. } => "workflow-tool-duplicate",
            Self::ToolLimitExceeded { .. } => "workflow-tool-limit-exceeded",
            Self::NotAllowed { .. } => "goal-workflow-not-allowed",
            Self::AfterAwait => "workflow-after-await",
            Self::LimitExceeded { .. } => "workflow-invocation-limit-exceeded",
            Self::HeadroomExceeded { .. } => "workflow-invocation-headroom-exceeded",
            Self::RecordTooLarge { .. } => "workflow-invocation-record-too-large",
            Self::RecordIncoherent { .. } => "workflow-invocation-incoherent",
            Self::Encoding { .. } => "workflow-tool-encoding",
            Self::Command { .. } => "workflow-start-command-invalid",
            Self::Identity(_) => "workflow-tool-identity",
        }
    }
}

impl Display for AgentWorkflowToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DescriptorInvalid { message } => {
                write!(f, "the workflow-tool descriptor is invalid: {message}")
            }
            Self::DescriptorTooLarge { bytes, maximum } => write!(
                f,
                "the workflow-tool descriptor is {bytes} serialized bytes, which exceeds the \
                 {maximum} byte bound"
            ),
            Self::DuplicateTool { tool } => {
                write!(f, "the workflow tool {tool} is already configured")
            }
            Self::ToolLimitExceeded { maximum } => {
                write!(
                    f,
                    "the configuration already declares {maximum} workflow tools"
                )
            }
            Self::NotAllowed { tool } => write!(
                f,
                "the workflow tool {tool} is outside the goal's allowed set"
            ),
            Self::AfterAwait => write!(
                f,
                "a workflow invocation planned after the same turn's await is refused: the \
                 member would revive a superseded wind-down"
            ),
            Self::LimitExceeded { maximum } => write!(
                f,
                "the run already retains {maximum} combined fan-out members"
            ),
            Self::HeadroomExceeded { needed, available } => write!(
                f,
                "the invocation needs {needed} bytes of materialized headroom and {available} \
                 are available"
            ),
            Self::RecordTooLarge { bytes, maximum } => write!(
                f,
                "the invocation record is {bytes} serialized bytes, which exceeds the {maximum} \
                 byte bound"
            ),
            Self::RecordIncoherent { message } => {
                write!(f, "the invocation record is incoherent: {message}")
            }
            Self::Encoding { message } => write!(f, "encoding failed: {message}"),
            Self::Command { message } => {
                write!(f, "the start command could not be built: {message}")
            }
            Self::Identity(error) => write!(f, "identity derivation failed: {error}"),
        }
    }
}

impl Error for AgentWorkflowToolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentWorkflowToolError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::task::AgentSchemaId;

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("coordinator").expect("agent id"),
            AgentRunId::new("run-1").expect("run id"),
        )
        .expect("run scope")
    }

    fn schema(id: &str) -> AgentSchemaRef {
        AgentSchemaRef::new(
            AgentSchemaId::new(id).expect("schema id"),
            AgentRevisionNumber::new(1),
        )
    }

    fn descriptor() -> AgentWorkflowToolDescriptor {
        AgentWorkflowToolDescriptor::new(
            AgentWorkflowToolId::new("refund-flow").expect("tool id"),
            "refund",
            WorkflowDefinitionVersion::new("v1"),
            "Runs the compiled refund workflow.",
            schema("refund-input"),
            schema("refund-output"),
        )
        .expect("descriptor")
    }

    fn record() -> AgentWorkflowInvocationRecord {
        let invocation = workflow_invocation_id_for(&scope(), 1, 0).expect("invocation id");
        let descriptor = descriptor();
        AgentWorkflowInvocationRecord {
            invocation: invocation.clone(),
            goal: None,
            parent_task: AgentTaskId::new("ticket-1").expect("task id"),
            parent_run: scope(),
            workflow_tool: descriptor.workflow_tool.clone(),
            descriptor_version: descriptor.version,
            descriptor_digest: descriptor.schema_digest(),
            workflow_type: descriptor.workflow_type.clone(),
            definition_version: descriptor.definition_version.clone(),
            required_capabilities: descriptor.required_capabilities.clone(),
            child_run: child_workflow_run_id(&invocation),
            deduplication_key: invocation.as_str().to_string(),
            turn: 1,
            slot: 0,
            effect: AgentEffectId::new("effect-0"),
            call_id: crate::model::AgentToolCallId::new("call-0").expect("call id"),
            input: AgentTaskContent::inline(serde_json::json!({"order": "o-1"}))
                .expect("bounded input"),
            deadline: None,
            definition_revision: AgentRevisionNumber::new(1),
            settings_revision: AgentRevisionNumber::new(1),
            telemetry: AgentTelemetryContext::default(),
            created_at: AgentTimestampMillis::new(1),
        }
    }

    #[test]
    fn the_invocation_id_is_pure_and_prefixed() {
        let first = workflow_invocation_id_for(&scope(), 3, 2).expect("id");
        let second = workflow_invocation_id_for(&scope(), 3, 2).expect("id");
        assert_eq!(first, second, "the derivation is pure");
        assert!(first
            .as_str()
            .starts_with(AGENT_WORKFLOW_INVOCATION_ID_PREFIX));
        let other_slot = workflow_invocation_id_for(&scope(), 3, 3).expect("id");
        assert_ne!(first, other_slot, "the slot is part of the identity");
    }

    #[test]
    fn the_start_identities_all_derive_from_the_invocation() {
        let record = record();
        assert_eq!(record.child_run.as_str(), record.invocation.as_str());
        assert_eq!(record.deduplication_key, record.invocation.as_str());
        assert_eq!(
            workflow_start_command_id(&record.invocation).as_str(),
            format!("{}#start-run", record.invocation.as_str())
        );
        let command = workflow_start_command(
            &record,
            AgentWorkflowId::new("wf-refund"),
            None,
            AgentTimestampMillis::new(2),
        )
        .expect("command");
        assert_eq!(command.metadata.run_id.as_str(), record.invocation.as_str());
        assert_eq!(
            command.metadata.deduplication_key.as_str(),
            record.invocation.as_str()
        );
        assert_eq!(
            command.metadata.command_id.as_str(),
            workflow_start_command_id(&record.invocation).as_str()
        );
        assert_eq!(
            command.attributes.get("trigger_kind").map(String::as_str),
            Some("child-workflow")
        );
    }

    #[test]
    fn the_record_refuses_incoherent_identities() {
        let mut record = record();
        record.child_run = WorkflowRunId::new("foreign-run");
        assert_eq!(
            record.validate().expect_err("incoherent child run").code(),
            "workflow-invocation-incoherent"
        );
        let mut record = self::record();
        record.deduplication_key = "foreign-key".to_string();
        assert_eq!(
            record.validate().expect_err("incoherent dedup key").code(),
            "workflow-invocation-incoherent"
        );
        self::record()
            .validate()
            .expect("a derived record is coherent");
    }

    #[test]
    fn the_descriptor_bounds_fail_closed() {
        let overlong = "x".repeat(AGENT_WORKFLOW_TOOL_DESCRIPTION_MAX_LENGTH + 1);
        let error = AgentWorkflowToolDescriptor::new(
            AgentWorkflowToolId::new("refund-flow").expect("tool id"),
            "refund",
            WorkflowDefinitionVersion::new("v1"),
            overlong,
            schema("in"),
            schema("out"),
        )
        .expect_err("overlong description");
        assert_eq!(error.code(), "workflow-tool-descriptor-invalid");

        let config = AgentRunWorkflowConfig::new()
            .with_descriptor(descriptor())
            .expect("first registration");
        assert_eq!(
            config
                .with_descriptor(descriptor())
                .expect_err("duplicate")
                .code(),
            "workflow-tool-duplicate"
        );
    }

    #[test]
    fn the_cell_settles_and_records_first_writer_wins() {
        let mut cell = AgentWorkflowInvocationCell::pending(Box::new(record()));
        cell.settle_started(false, AgentTimestampMillis::new(2));
        cell.settle_failed("late-failure", AgentTimestampMillis::new(3));
        assert_eq!(
            cell.status,
            AgentWorkflowInvocationStatus::Started { adopted: false },
            "settlement is first-writer-wins"
        );
        cell.record_child_result(AgentWorkflowChildResult {
            status: AgentWorkflowTerminalStatus::Completed,
            terminal_reason: None,
            result_ref: None,
            result_digest: Some(AgentContentDigest::sha256_of_bytes(b"result")),
            descendants_created: 0,
            recorded_at: AgentTimestampMillis::new(4),
        });
        cell.record_child_result(AgentWorkflowChildResult {
            status: AgentWorkflowTerminalStatus::Failed,
            terminal_reason: Some("conflicting".to_string()),
            result_ref: None,
            result_digest: None,
            descendants_created: 0,
            recorded_at: AgentTimestampMillis::new(5),
        });
        let result = cell.result.as_ref().expect("first result stands");
        assert_eq!(result.status, AgentWorkflowTerminalStatus::Completed);
    }

    #[test]
    fn the_result_operation_id_is_pure_over_tenant_and_invocation() {
        let invocation = workflow_invocation_id_for(&scope(), 1, 0).expect("id");
        let first = workflow_result_operation_id(&TenantId::new("acme"), &invocation)
            .expect("operation id");
        let second = workflow_result_operation_id(&TenantId::new("acme"), &invocation)
            .expect("operation id");
        assert_eq!(first, second);
        assert!(first.as_str().starts_with("workflow-result/"));
    }
}
