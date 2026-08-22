//! A2A wire-to-Rakka workflow conversion boundary.
//!
//! This module normalizes public A2A identity and metadata before the request
//! handler crosses the durable Rakka inbox boundary. The `io.rakka.*`
//! metadata keys, the derived deduplication-key shape, and the stable
//! [`A2AMappingError::code`] strings are compatibility commitments.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use a2a::{CancelTaskRequest, Message, Part, PartContent, SendMessageRequest};
use a2a_server::ServiceParams;
use rakka_agent_workflow::{
    extract_agent_trace_context, parse_agent_trace_context, validate_command, AgentAttributes,
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentFacadeError,
    AgentRunId, AgentTelemetryContext, AgentTenantId, AgentTimestampMillis,
    AgentTriggerCommandBuilder, AgentTriggerSource, AgentWorkflow, AgentWorkflowId, ArtifactKind,
    ArtifactRef, InlineState, PrincipalRef, RedactionStatus, TRACEPARENT_HEADER, TRACESTATE_HEADER,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::support::current_timestamp_millis;

/// Metadata key for the adapter schema version.
pub const META_ADAPTER_VERSION: &str = "io.rakka.adapter.version";
/// Metadata key for selecting a workflow id.
pub const META_WORKFLOW_ID: &str = "io.rakka.workflow.id";
/// Metadata key for selecting a workflow type.
pub const META_WORKFLOW_TYPE: &str = "io.rakka.workflow.type";
/// Metadata key for selecting a workflow definition version.
pub const META_DEFINITION_VERSION: &str = "io.rakka.workflow.definition_version";
/// Metadata key for overriding the command id.
pub const META_COMMAND_ID: &str = "io.rakka.command.id";
/// Metadata key for selecting a specific command kind.
pub const META_COMMAND_KIND: &str = "io.rakka.command.kind";
/// Metadata key for selecting a submit-signal type.
pub const META_SIGNAL_TYPE: &str = "io.rakka.command.signal_type";
/// Metadata key for overriding the durable command deduplication key.
pub const META_DEDUPLICATION_KEY: &str = "io.rakka.command.deduplication_key";
/// Metadata key for overriding causation id.
pub const META_CAUSATION_ID: &str = "io.rakka.causation_id";
/// Metadata key for overriding correlation id.
pub const META_CORRELATION_ID: &str = "io.rakka.correlation_id";
/// Metadata key for a public-auth principal reference.
pub const META_PRINCIPAL_REF: &str = "io.rakka.principal.ref";
/// Metadata traceparent fallback used when transport headers are unavailable.
pub const META_TRACEPARENT: &str = "io.rakka.trace.traceparent";
/// Metadata tracestate fallback used when transport headers are unavailable.
pub const META_TRACESTATE: &str = "io.rakka.trace.tracestate";

/// Service parameter (header) keys accepted as the canonical tenant source.
pub const TENANT_HEADERS: [&str; 2] = ["x-rakka-tenant", "x-tenant-id"];

/// Default tenant used in single-tenant/local mode when no authenticated or
/// request tenant exists.
pub const DEFAULT_TENANT: &str = "public";
/// Default signal type for A2A continuation messages.
pub const DEFAULT_SIGNAL_TYPE: &str = "a2a.message";
/// Command attribute carrying the normalized A2A context id so projection
/// recovery can rebuild tasks with the client's original context.
pub const ATTR_CONTEXT_ID: &str = "a2a_context_id";
const COMMAND_PAYLOAD_CONTENT_TYPE: &str = "application/vnd.rakka.a2a.message+json";
const MAX_BOUNDED_METADATA_VALUE_BYTES: usize = 256;

/// Shared result type for A2A mapping.
pub type A2AMappingResult<T> = Result<T, A2AMappingError>;

/// Stable A2A mapping failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum A2AMappingError {
    /// A required field was absent or blank.
    MissingField {
        /// Stable field name.
        field: &'static str,
    },
    /// A metadata field had the wrong shape.
    InvalidMetadata {
        /// Metadata key or field.
        field: String,
        /// Stable reason.
        reason: &'static str,
    },
    /// Two metadata or first-class fields disagreed.
    MetadataConflict {
        /// Conflicting field.
        field: String,
        /// Canonical value from first-class fields or earlier metadata.
        canonical: String,
        /// Conflicting metadata value.
        metadata: String,
    },
    /// The selected workflow does not match a hosted workflow.
    InvalidWorkflowSelection {
        /// Workflow selection field.
        field: &'static str,
        /// Expected value.
        expected: String,
        /// Actual value.
        actual: String,
    },
    /// A tenant is required but none was resolved.
    TenantRequired,
    /// A payload exceeded inline policy and no artifact strategy is enabled.
    PayloadTooLarge {
        /// Observed payload size.
        size_bytes: u64,
        /// Configured inline limit.
        limit_bytes: u64,
    },
    /// A Rakka command failed validation.
    CommandValidation {
        /// Validation message.
        message: String,
    },
}

impl A2AMappingError {
    /// Stable machine-readable validation code.
    ///
    /// Compatibility commitment: these codes surface in A2A error messages
    /// and bounded adapter metrics labels.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingField { .. } => "missing-field",
            Self::InvalidMetadata { .. } => "invalid-metadata",
            Self::MetadataConflict { .. } => "metadata-conflict",
            Self::InvalidWorkflowSelection { .. } => "invalid-workflow-selection",
            Self::TenantRequired => "tenant-required",
            Self::PayloadTooLarge { .. } => "payload-too-large",
            Self::CommandValidation { .. } => "command-validation",
        }
    }
}

impl Display for A2AMappingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(f, "{field} is required"),
            Self::InvalidMetadata { field, reason } => {
                write!(f, "invalid metadata {field}: {reason}")
            }
            Self::MetadataConflict {
                field,
                canonical,
                metadata,
            } => write!(
                f,
                "metadata conflict for {field}: canonical `{canonical}` differs from `{metadata}`"
            ),
            Self::InvalidWorkflowSelection {
                field,
                expected,
                actual,
            } => write!(
                f,
                "invalid workflow selection {field}: expected `{expected}`, got `{actual}`"
            ),
            Self::TenantRequired => f.write_str("tenant is required"),
            Self::PayloadTooLarge {
                size_bytes,
                limit_bytes,
            } => write!(
                f,
                "payload has {size_bytes} bytes, exceeding inline limit {limit_bytes}"
            ),
            Self::CommandValidation { message } => {
                write!(f, "command validation failed: {message}")
            }
        }
    }
}

impl Error for A2AMappingError {}

impl From<AgentFacadeError> for A2AMappingError {
    fn from(error: AgentFacadeError) -> Self {
        Self::CommandValidation {
            message: error.to_string(),
        }
    }
}

/// Source used for the canonical tenant value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ATenantSource {
    /// Tenant came from authenticated transport/service parameters.
    ServiceParams,
    /// Tenant came from the A2A request field.
    Request,
    /// Tenant came from an application-supplied resolver.
    Resolver,
    /// Single-tenant/local development fallback.
    Default,
}

/// Whether an A2A message starts or continues a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ATaskIntent {
    /// A new task/run id was generated.
    NewTask,
    /// The request targets an existing task/run.
    ContinueTask,
}

impl A2ATaskIntent {
    pub(crate) const fn is_new(self) -> bool {
        matches!(self, Self::NewTask)
    }
}

/// Resolves the canonical tenant for A2A commands and reads.
///
/// The default implementation ([`A2AHeaderTenantResolver`]) uses header-first
/// precedence over the request tenant with conflict rejection. Applications
/// with authenticated multi-tenant traffic must supply their own resolver
/// that derives the tenant from verified transport identity, and must build
/// the service in tenant-scoped mode so unscoped reads are refused.
pub trait A2ATenantResolver: Send + Sync + 'static {
    /// Resolves the tenant for a durable command.
    ///
    /// Returning `Ok(None)` means "no tenant input"; the handler then either
    /// applies the single-tenant default or rejects, depending on mode.
    fn resolve_command_tenant(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
    ) -> A2AMappingResult<Option<(String, A2ATenantSource)>>;

    /// Resolves the tenant scope for a read.
    ///
    /// Returning `Ok(None)` requests an unscoped read, which is only
    /// permitted in single-tenant/local mode.
    fn resolve_read_tenant(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
    ) -> A2AMappingResult<Option<String>> {
        Ok(self
            .resolve_command_tenant(params, request_tenant)?
            .map(|(tenant, _)| tenant))
    }
}

/// Header-first tenant resolution with conflict rejection.
///
/// Accepts `x-rakka-tenant` / `x-tenant-id` service parameters as canonical,
/// falls back to the request tenant, and rejects disagreements between the
/// two. This resolver trusts transport headers, so it is only appropriate
/// behind an ingress that authenticates and sets them.
#[derive(Debug, Clone, Copy, Default)]
pub struct A2AHeaderTenantResolver;

impl A2ATenantResolver for A2AHeaderTenantResolver {
    fn resolve_command_tenant(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
    ) -> A2AMappingResult<Option<(String, A2ATenantSource)>> {
        let service_tenant = TENANT_HEADERS
            .iter()
            .find_map(|header| first_service_param(params, header));
        if let (Some(service), Some(request)) = (service_tenant.as_deref(), request_tenant) {
            reject_conflict("tenant", service, request, "request.tenant")?;
        }

        if let Some(tenant) = service_tenant {
            return Ok(Some((tenant, A2ATenantSource::ServiceParams)));
        }
        let Some(tenant) = request_tenant else {
            return Ok(None);
        };
        require_non_blank(tenant, "tenant")?;
        Ok(Some((tenant.to_string(), A2ATenantSource::Request)))
    }
}

/// Normalized identity and metadata for one A2A command request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedA2ACommand {
    /// A2A task id, equal to Rakka run id and sharded entity id.
    pub task_id: String,
    /// A2A context id used for public grouping.
    pub context_id: String,
    /// New task versus continuation command.
    pub intent: A2ATaskIntent,
    /// Workflow id selected for the command.
    pub workflow_id: AgentWorkflowId,
    /// Bounded workflow type label.
    pub workflow_type: String,
    /// Workflow definition version.
    pub definition_version: String,
    /// Durable command id.
    pub command_id: AgentCommandId,
    /// Durable deduplication key.
    pub deduplication_key: AgentDeduplicationKey,
    /// Causation id.
    pub causation_id: AgentCausationId,
    /// Correlation id.
    pub correlation_id: AgentCorrelationId,
    /// Canonical tenant.
    pub tenant: AgentTenantId,
    /// Canonical tenant source.
    pub tenant_source: A2ATenantSource,
    /// Transport-preferred trace context.
    pub telemetry_context: AgentTelemetryContext,
    /// Authenticated principal reference, when supplied.
    pub principal: Option<PrincipalRef>,
    /// Optional command kind hint from metadata.
    pub command_kind_hint: Option<String>,
    /// Optional submit-signal type from metadata.
    pub signal_type: Option<String>,
    /// Bounded metadata retained for audit/projection.
    pub bounded_metadata: AgentAttributes,
}

impl NormalizedA2ACommand {
    /// Returns the durable run id.
    #[must_use]
    pub fn run_id(&self) -> AgentRunId {
        AgentRunId::new(self.task_id.clone())
    }
}

/// Payload policy used while building command drafts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2APayloadPolicy {
    /// Maximum inline payload bytes.
    pub inline_limit_bytes: u64,
    /// Whether oversized or non-text parts may become artifact references.
    pub allow_artifact_references: bool,
}

impl A2APayloadPolicy {
    /// Default bounded policy: 4 KiB inline with artifact references enabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inline_limit_bytes: 4 * 1024,
            allow_artifact_references: true,
        }
    }

    /// Returns a policy that rejects anything too large for inline payloads.
    #[must_use]
    pub const fn without_artifact_strategy(mut self) -> Self {
        self.allow_artifact_references = false;
        self
    }

    /// Returns a policy with a custom inline payload limit.
    #[must_use]
    pub const fn inline_limit_bytes(mut self, limit_bytes: u64) -> Self {
        self.inline_limit_bytes = limit_bytes;
        self
    }
}

impl Default for A2APayloadPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Payload location selected for an A2A message command draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2ACommandPayload {
    /// The complete bounded message can be persisted inline.
    Inline(InlineState),
    /// Message parts are represented by artifact drafts pairing each reference
    /// with the source content that must be persisted behind it.
    ArtifactDrafts(Vec<A2AArtifactDraft>),
    /// The message had no parts.
    Empty,
}

impl A2ACommandPayload {
    /// Returns artifact drafts carried by this payload.
    #[must_use]
    pub fn artifact_drafts(&self) -> &[A2AArtifactDraft] {
        match self {
            Self::ArtifactDrafts(drafts) => drafts,
            Self::Inline(_) | Self::Empty => &[],
        }
    }
}

/// Artifact reference plus the source content it stands for.
///
/// Only `reference` may reach durable state. The application's artifact
/// strategy must persist `content` behind `reference.uri` (computing a real
/// checksum at that point) before durable inbox acceptance; `content` is
/// `None` when the part already lives at an external URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2AArtifactDraft {
    /// Bounded, reference-only artifact metadata safe for durable state.
    pub reference: ArtifactRef,
    /// Source part content backing a synthetic `a2a-message://` uri.
    pub content: Option<InlineState>,
}

/// Validated Rakka command plus its payload placement draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2ACommandDraft {
    /// Normalized A2A metadata.
    pub normalized: NormalizedA2ACommand,
    /// Validated Rakka agent command.
    pub command: AgentCommand,
    /// Payload to persist before durable inbox acceptance.
    pub payload: A2ACommandPayload,
}

/// Workflow selection extracted from A2A request/message metadata.
///
/// Every field is optional; an empty selection resolves to the catalog's
/// default workflow.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct A2AWorkflowSelection {
    /// Selected workflow id, from [`META_WORKFLOW_ID`].
    pub workflow_id: Option<String>,
    /// Selected workflow type, from [`META_WORKFLOW_TYPE`].
    pub workflow_type: Option<String>,
    /// Selected definition version, from [`META_DEFINITION_VERSION`].
    pub definition_version: Option<String>,
}

impl A2AWorkflowSelection {
    /// Extracts the workflow selection from merged request metadata.
    pub fn from_metadata(metadata: &HashMap<String, Value>) -> A2AMappingResult<Self> {
        Ok(Self {
            workflow_id: metadata_string(metadata, META_WORKFLOW_ID)?,
            workflow_type: metadata_string(metadata, META_WORKFLOW_TYPE)?,
            definition_version: metadata_string(metadata, META_DEFINITION_VERSION)?,
        })
    }

    /// Extracts the workflow selection from a send-message request.
    pub fn from_send_message_request(request: &SendMessageRequest) -> A2AMappingResult<Self> {
        let merged = merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())?;
        Self::from_metadata(&merged)
    }

    /// True when no selection field is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workflow_id.is_none()
            && self.workflow_type.is_none()
            && self.definition_version.is_none()
    }

    /// True when `workflow` satisfies every present selection field.
    #[must_use]
    pub fn matches(&self, workflow: &AgentWorkflow) -> bool {
        self.workflow_id
            .as_deref()
            .is_none_or(|id| id == workflow.workflow_id.as_str())
            && self
                .workflow_type
                .as_deref()
                .is_none_or(|workflow_type| workflow_type == workflow.workflow_type)
            && self
                .definition_version
                .as_deref()
                .is_none_or(|version| version == workflow.definition_version.as_str())
    }
}

/// Resolved tenant input shared by the normalization functions.
pub(crate) fn canonical_tenant(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request_tenant: Option<&str>,
) -> A2AMappingResult<(AgentTenantId, A2ATenantSource)> {
    if let Some((tenant, source)) = resolver.resolve_command_tenant(params, request_tenant)? {
        return Ok((AgentTenantId::new(tenant), source));
    }
    match default_tenant {
        Some(tenant) => Ok((AgentTenantId::new(tenant), A2ATenantSource::Default)),
        None => Err(A2AMappingError::TenantRequired),
    }
}

/// Normalizes a send-message request without accepting it durably.
pub fn normalize_send_message_request(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request: &SendMessageRequest,
    workflow: &AgentWorkflow,
) -> A2AMappingResult<NormalizedA2ACommand> {
    let merged = merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())?;
    normalize_message(
        resolver,
        default_tenant,
        params,
        &request.message,
        request.tenant.as_deref(),
        &merged,
        workflow,
    )
}

/// Builds a complete command draft for `message:send`.
pub fn build_send_message_command_draft(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request: &SendMessageRequest,
    workflow: &AgentWorkflow,
    policy: A2APayloadPolicy,
    received_at: AgentTimestampMillis,
) -> A2AMappingResult<A2ACommandDraft> {
    let normalized =
        normalize_send_message_request(resolver, default_tenant, params, request, workflow)?;
    let payload = convert_message_payload(&request.message, &normalized, policy, received_at)?;
    let kind = command_kind_for_message(&normalized)?;
    build_command_draft(
        normalized,
        kind,
        request.message.role.role_label(),
        payload,
        received_at,
    )
}

/// Builds a cancellation command draft without durable acceptance.
pub fn build_cancel_task_command_draft(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request: &CancelTaskRequest,
    workflow: &AgentWorkflow,
    received_at: AgentTimestampMillis,
) -> A2AMappingResult<A2ACommandDraft> {
    require_non_blank(&request.id, "id")?;
    let (tenant, tenant_source) =
        canonical_tenant(resolver, default_tenant, params, request.tenant.as_deref())?;
    let context_id = request.id.clone();
    let metadata = request.metadata.clone().unwrap_or_default();
    validate_workflow_selection(&metadata, workflow)?;
    let command_id = metadata_string(&metadata, META_COMMAND_ID)?
        .unwrap_or_else(|| format!("cancel-{}", request.id));
    let command_id = AgentCommandId::new(command_id);
    let telemetry_context = telemetry_context(params, &metadata)?;
    let deduplication_key =
        metadata_string(&metadata, META_DEDUPLICATION_KEY)?.unwrap_or_else(|| {
            derived_deduplication_key(tenant.as_str(), &request.id, command_id.as_str())
        });
    let normalized = NormalizedA2ACommand {
        task_id: request.id.clone(),
        context_id,
        intent: A2ATaskIntent::ContinueTask,
        workflow_id: workflow.workflow_id.clone(),
        workflow_type: workflow.workflow_type.clone(),
        definition_version: workflow.definition_version.to_string(),
        command_id,
        deduplication_key: AgentDeduplicationKey::new(deduplication_key),
        causation_id: AgentCausationId::new(
            metadata_string(&metadata, META_CAUSATION_ID)?
                .unwrap_or_else(|| format!("cancel-{}", request.id)),
        ),
        correlation_id: AgentCorrelationId::new(
            metadata_string(&metadata, META_CORRELATION_ID)?.unwrap_or_else(|| request.id.clone()),
        ),
        tenant,
        tenant_source,
        telemetry_context,
        principal: metadata
            .get(META_PRINCIPAL_REF)
            .map(principal_ref_from_value)
            .transpose()?,
        command_kind_hint: Some("cancel-run".to_string()),
        signal_type: None,
        bounded_metadata: bounded_metadata_from_values(&metadata),
    };
    build_command_draft(
        normalized,
        AgentCommandKind::CancelRun,
        "cancel",
        A2ACommandPayload::Empty,
        received_at,
    )
}

#[allow(clippy::too_many_arguments)]
fn normalize_message(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    message: &Message,
    request_tenant: Option<&str>,
    metadata: &HashMap<String, Value>,
    workflow: &AgentWorkflow,
) -> A2AMappingResult<NormalizedA2ACommand> {
    require_non_blank(&message.message_id, "message.message_id")?;
    validate_workflow_selection(metadata, workflow)?;

    let (tenant, tenant_source) =
        canonical_tenant(resolver, default_tenant, params, request_tenant)?;
    let (task_id, intent) = match message.task_id.as_deref() {
        Some(task_id) if !task_id.trim().is_empty() => {
            (task_id.to_string(), A2ATaskIntent::ContinueTask)
        }
        Some(_) => {
            return Err(A2AMappingError::MissingField {
                field: "message.task_id",
            })
        }
        None => (
            generated_task_id(tenant.as_str(), &message.message_id),
            A2ATaskIntent::NewTask,
        ),
    };
    let context_id = message
        .context_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&task_id)
        .to_string();

    let metadata_command_id = metadata_string(metadata, META_COMMAND_ID)?;
    if let Some(metadata_command_id) = &metadata_command_id {
        reject_conflict(
            "message.message_id",
            &message.message_id,
            metadata_command_id,
            META_COMMAND_ID,
        )?;
    }
    let command_id = AgentCommandId::new(message.message_id.clone());
    let deduplication_key =
        metadata_string(metadata, META_DEDUPLICATION_KEY)?.unwrap_or_else(|| {
            derived_deduplication_key(tenant.as_str(), &task_id, command_id.as_str())
        });

    Ok(NormalizedA2ACommand {
        task_id,
        context_id: context_id.clone(),
        intent,
        workflow_id: workflow.workflow_id.clone(),
        workflow_type: workflow.workflow_type.clone(),
        definition_version: workflow.definition_version.to_string(),
        command_id: command_id.clone(),
        deduplication_key: AgentDeduplicationKey::new(deduplication_key),
        causation_id: AgentCausationId::new(
            metadata_string(metadata, META_CAUSATION_ID)?
                .unwrap_or_else(|| command_id.as_str().to_string()),
        ),
        correlation_id: AgentCorrelationId::new(
            metadata_string(metadata, META_CORRELATION_ID)?.unwrap_or(context_id),
        ),
        tenant,
        tenant_source,
        telemetry_context: telemetry_context(params, metadata)?,
        principal: metadata
            .get(META_PRINCIPAL_REF)
            .map(principal_ref_from_value)
            .transpose()?,
        command_kind_hint: metadata_string(metadata, META_COMMAND_KIND)?,
        signal_type: metadata_string(metadata, META_SIGNAL_TYPE)?,
        bounded_metadata: bounded_metadata_from_values(metadata),
    })
}

fn build_command_draft(
    normalized: NormalizedA2ACommand,
    kind: AgentCommandKind,
    role_label: &'static str,
    payload: A2ACommandPayload,
    received_at: AgentTimestampMillis,
) -> A2AMappingResult<A2ACommandDraft> {
    let mut metadata = AgentCommandMetadata::new(
        normalized.workflow_id.clone(),
        normalized.run_id(),
        normalized.command_id.clone(),
        AgentDurabilityMetadata::new(
            normalized.deduplication_key.clone(),
            normalized.causation_id.clone(),
            normalized.correlation_id.clone(),
        )
        .telemetry_context(normalized.telemetry_context.clone()),
        normalized.tenant.clone(),
        received_at,
    )?;

    if let Some(principal) = normalized.principal.clone() {
        metadata = metadata.principal(principal);
    }

    // A2A commands enter through the application's API boundary, so they carry
    // the crate's normalized trigger-source attributes like other trigger paths.
    let trigger_source = AgentTriggerSource::api();
    let builder = match kind {
        AgentCommandKind::StartRun => {
            AgentTriggerCommandBuilder::start_run(metadata, trigger_source)
        }
        AgentCommandKind::SubmitSignal { signal_type } => {
            AgentTriggerCommandBuilder::submit_signal(metadata, trigger_source, signal_type)
        }
        AgentCommandKind::CancelRun => {
            AgentTriggerCommandBuilder::cancel_run(metadata, trigger_source)
        }
        other => {
            return Err(A2AMappingError::CommandValidation {
                message: format!("A2A mapping cannot produce command kind {other:?}"),
            })
        }
    };
    let mut command = builder
        .build()
        .map_err(|error| A2AMappingError::CommandValidation {
            message: error.to_string(),
        })?
        .attribute("workflow_type", normalized.workflow_type.clone())?
        .attribute("definition_version", normalized.definition_version.clone())?
        .attribute("a2a_role", role_label)?
        .attribute(
            "a2a_task_intent",
            if normalized.intent.is_new() {
                "new"
            } else {
                "continue"
            },
        )?
        // Persisted with the command in the durable inbox so projection
        // recovery can restore the client's original context id.
        .attribute(ATTR_CONTEXT_ID, normalized.context_id.clone())?;
    if !payload.artifact_drafts().is_empty() {
        command = command.attribute("a2a_payload", "artifact-ref")?;
    }
    validate_command(&command)?;

    Ok(A2ACommandDraft {
        normalized,
        command,
        payload,
    })
}

fn command_kind_for_message(
    normalized: &NormalizedA2ACommand,
) -> A2AMappingResult<AgentCommandKind> {
    match normalized.command_kind_hint.as_deref() {
        Some("start-run") | Some("StartRun") => Ok(AgentCommandKind::StartRun),
        Some("submit-signal") | Some("SubmitSignal") => Ok(AgentCommandKind::SubmitSignal {
            signal_type: normalized
                .signal_type
                .clone()
                .unwrap_or_else(|| DEFAULT_SIGNAL_TYPE.to_string()),
        }),
        Some("cancel-run") | Some("CancelRun") => Ok(AgentCommandKind::CancelRun),
        Some(other) => Err(A2AMappingError::InvalidMetadata {
            field: META_COMMAND_KIND.to_string(),
            reason: if other.is_empty() {
                "command kind must not be empty"
            } else {
                "unsupported command kind for this A2A adapter"
            },
        }),
        None if normalized.intent.is_new() => Ok(AgentCommandKind::StartRun),
        None => Ok(AgentCommandKind::SubmitSignal {
            signal_type: normalized
                .signal_type
                .clone()
                .unwrap_or_else(|| DEFAULT_SIGNAL_TYPE.to_string()),
        }),
    }
}

fn convert_message_payload(
    message: &Message,
    normalized: &NormalizedA2ACommand,
    policy: A2APayloadPolicy,
    received_at: AgentTimestampMillis,
) -> A2AMappingResult<A2ACommandPayload> {
    if message.parts.is_empty() {
        return Ok(A2ACommandPayload::Empty);
    }

    let serialized = serde_json::to_vec(message).map_err(|_| A2AMappingError::InvalidMetadata {
        field: "message".to_string(),
        reason: "message must serialize to JSON",
    })?;
    let serialized_len = serialized.len() as u64;
    if serialized_len <= policy.inline_limit_bytes {
        return Ok(A2ACommandPayload::Inline(InlineState {
            content_type: COMMAND_PAYLOAD_CONTENT_TYPE.to_string(),
            size_bytes: serialized_len,
            bytes: serialized,
        }));
    }

    if !policy.allow_artifact_references {
        return Err(A2AMappingError::PayloadTooLarge {
            size_bytes: serialized_len,
            limit_bytes: policy.inline_limit_bytes,
        });
    }

    let drafts = message
        .parts
        .iter()
        .enumerate()
        .map(|(index, part)| artifact_draft_for_part(normalized, part, index, received_at))
        .collect::<A2AMappingResult<Vec<_>>>()?;
    Ok(A2ACommandPayload::ArtifactDrafts(drafts))
}

fn artifact_draft_for_part(
    normalized: &NormalizedA2ACommand,
    part: &Part,
    index: usize,
    created_at: AgentTimestampMillis,
) -> A2AMappingResult<A2AArtifactDraft> {
    let media_type = part
        .media_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let (uri, content_bytes, source_kind) = match &part.content {
        PartContent::Text(text) => (
            format!("a2a-message://{}/part/{index}", normalized.task_id),
            Some(text.clone().into_bytes()),
            "text",
        ),
        PartContent::Raw(bytes) => (
            format!("a2a-message://{}/part/{index}/raw", normalized.task_id),
            Some(bytes.clone()),
            "raw",
        ),
        PartContent::Url(url) => (url.clone(), None, "url"),
        PartContent::Data(value) => (
            format!("a2a-message://{}/part/{index}/data", normalized.task_id),
            Some(value.to_string().into_bytes()),
            "data",
        ),
    };
    if uri.trim().is_empty() {
        return Err(A2AMappingError::InvalidMetadata {
            field: "part.url".to_string(),
            reason: "part reference uri must not be empty",
        });
    }

    let mut metadata = BTreeMap::new();
    metadata.insert("source".to_string(), format!("a2a-{source_kind}"));
    metadata.insert("part_index".to_string(), index.to_string());
    if let Some(filename) = &part.filename {
        metadata.insert("filename".to_string(), bounded_value(filename));
    }

    let byte_len = content_bytes.as_ref().map(|bytes| bytes.len() as u64);
    let content = content_bytes.map(|bytes| InlineState {
        content_type: media_type.clone(),
        size_bytes: bytes.len() as u64,
        bytes,
    });

    Ok(A2AArtifactDraft {
        reference: ArtifactRef {
            artifact_id: format!("{}-part-{index}", normalized.command_id),
            kind: ArtifactKind::Input,
            uri,
            checksum: None,
            content_type: Some(media_type),
            byte_len,
            retention_class: Some("standard".to_string()),
            encryption: None,
            redaction: RedactionStatus::ReferenceOnly,
            created_at,
            metadata,
        },
        content,
    })
}

/// Merges request- and message-level metadata, rejecting `io.rakka.*`
/// conflicts between the two levels.
pub(crate) fn merged_metadata(
    request_metadata: Option<&HashMap<String, Value>>,
    message_metadata: Option<&HashMap<String, Value>>,
) -> A2AMappingResult<HashMap<String, Value>> {
    let mut merged = request_metadata.cloned().unwrap_or_default();
    if let Some(message_metadata) = message_metadata {
        for (key, value) in message_metadata {
            if key.starts_with("io.rakka.") {
                if let Some(existing) = merged.get(key) {
                    if existing != value {
                        return Err(A2AMappingError::MetadataConflict {
                            field: key.clone(),
                            canonical: value_to_conflict_string(existing),
                            metadata: value_to_conflict_string(value),
                        });
                    }
                }
            }
            merged.insert(key.clone(), value.clone());
        }
    }
    Ok(merged)
}

fn validate_workflow_selection(
    metadata: &HashMap<String, Value>,
    workflow: &AgentWorkflow,
) -> A2AMappingResult<()> {
    validate_selection(metadata, META_WORKFLOW_ID, workflow.workflow_id.as_str())?;
    validate_selection(metadata, META_WORKFLOW_TYPE, &workflow.workflow_type)?;
    validate_selection(
        metadata,
        META_DEFINITION_VERSION,
        workflow.definition_version.as_str(),
    )
}

fn validate_selection(
    metadata: &HashMap<String, Value>,
    field: &'static str,
    expected: &str,
) -> A2AMappingResult<()> {
    if let Some(actual) = metadata_string(metadata, field)? {
        if actual != expected {
            return Err(A2AMappingError::InvalidWorkflowSelection {
                field,
                expected: expected.to_string(),
                actual,
            });
        }
    }
    Ok(())
}

/// Resolves the tenant scope shared by A2A read paths.
///
/// Reads use the command paths' precedence and conflict rejection, but do not
/// apply the single-tenant default: with no tenant input the result is `None`
/// and the caller's tenant mode decides whether unscoped reads are permitted.
pub fn canonical_read_tenant(
    resolver: &dyn A2ATenantResolver,
    params: &ServiceParams,
    request_tenant: Option<&str>,
) -> A2AMappingResult<Option<String>> {
    resolver.resolve_read_tenant(params, request_tenant)
}

fn telemetry_context(
    params: &ServiceParams,
    metadata: &HashMap<String, Value>,
) -> A2AMappingResult<AgentTelemetryContext> {
    let mut carrier = BTreeMap::new();
    if let Some(traceparent) = first_service_param(params, TRACEPARENT_HEADER) {
        carrier.insert(TRACEPARENT_HEADER.to_string(), traceparent);
    }
    if let Some(tracestate) = first_service_param(params, TRACESTATE_HEADER) {
        carrier.insert(TRACESTATE_HEADER.to_string(), tracestate);
    }
    // Per W3C trace-context guidance, an unparseable transport traceparent is
    // treated as absent (the trace restarts) instead of failing the request.
    if let Ok(Some(context)) = extract_agent_trace_context(&carrier) {
        return Ok(context);
    }

    let Some(traceparent) = metadata_string(metadata, META_TRACEPARENT)? else {
        return Ok(AgentTelemetryContext::default());
    };
    let tracestate = metadata_string(metadata, META_TRACESTATE)?;
    parse_agent_trace_context(&traceparent, tracestate.as_deref())
        .map(|context| context.to_telemetry_context())
        .map_err(|_| A2AMappingError::InvalidMetadata {
            field: META_TRACEPARENT.to_string(),
            reason: "invalid W3C trace context",
        })
}

/// Encodes a principal ref for the `io.rakka.principal.ref` metadata key.
///
/// The compact `type:id[:display]` string is kept byte-identical for every
/// principal whose components are colon-free — the shape existing clients
/// parse — but it cannot spell a colon-bearing component: `splitn` decoding
/// makes `("user:a", "b")` and `("user", "a:b")` the same bytes, so a SPIFFE
/// URI, ARN, or OIDC subject would silently truncate at the first colon
/// before authorization ever saw it. Those principals ride the object form
/// instead, which [`principal_ref_from_value`] has always accepted and which
/// round-trips every component verbatim.
pub(crate) fn principal_ref_to_value(principal: &PrincipalRef) -> Value {
    let colon_free = !principal.principal_type.contains(':')
        && !principal.principal_id.contains(':')
        && !principal
            .display_name
            .as_deref()
            .is_some_and(|display| display.contains(':'));
    if colon_free {
        let mut encoded = format!("{}:{}", principal.principal_type, principal.principal_id);
        if let Some(display) = &principal.display_name {
            encoded.push(':');
            encoded.push_str(display);
        }
        return Value::String(encoded);
    }
    let mut map = serde_json::Map::new();
    map.insert(
        "type".to_string(),
        Value::String(principal.principal_type.clone()),
    );
    map.insert(
        "id".to_string(),
        Value::String(principal.principal_id.clone()),
    );
    if let Some(display) = &principal.display_name {
        map.insert("displayName".to_string(), Value::String(display.clone()));
    }
    Value::Object(map)
}

/// Encodes a principal for a durable provenance string field.
///
/// Colon-free components keep the compact `type:id` join every existing
/// record carries; a colon-bearing component switches to the canonical JSON
/// object (which no legacy join can collide with — a join never starts with
/// `{`), so `("user:a", "b")` and `("user", "a:b")` stay distinguishable in
/// the durable record instead of collapsing to the same bytes.
pub(crate) fn principal_provenance_label(principal: &PrincipalRef) -> String {
    if !principal.principal_type.contains(':') && !principal.principal_id.contains(':') {
        return format!("{}:{}", principal.principal_type, principal.principal_id);
    }
    serde_json::json!({
        "type": principal.principal_type,
        "id": principal.principal_id,
    })
    .to_string()
}

pub(crate) fn principal_ref_from_value(value: &Value) -> A2AMappingResult<PrincipalRef> {
    match value {
        Value::String(value) => {
            let parts = value.splitn(3, ':').collect::<Vec<_>>();
            if parts.len() < 2 {
                return Err(A2AMappingError::InvalidMetadata {
                    field: META_PRINCIPAL_REF.to_string(),
                    reason: "string principal refs must use type:id",
                });
            }
            require_non_blank(parts[0], "principal.type")?;
            require_non_blank(parts[1], "principal.id")?;
            Ok(PrincipalRef {
                principal_type: parts[0].to_string(),
                principal_id: parts[1].to_string(),
                display_name: parts.get(2).map(|value| (*value).to_string()),
            })
        }
        Value::Object(map) => {
            let principal_type = object_string(map, "type")
                .or_else(|| object_string(map, "principalType"))
                .ok_or_else(|| A2AMappingError::InvalidMetadata {
                    field: META_PRINCIPAL_REF.to_string(),
                    reason: "principal type is required",
                })?;
            let principal_id = object_string(map, "id")
                .or_else(|| object_string(map, "principalId"))
                .ok_or_else(|| A2AMappingError::InvalidMetadata {
                    field: META_PRINCIPAL_REF.to_string(),
                    reason: "principal id is required",
                })?;
            require_non_blank(&principal_type, "principal.type")?;
            require_non_blank(&principal_id, "principal.id")?;
            Ok(PrincipalRef {
                principal_type,
                principal_id,
                display_name: object_string(map, "displayName"),
            })
        }
        _ => Err(A2AMappingError::InvalidMetadata {
            field: META_PRINCIPAL_REF.to_string(),
            reason: "principal ref must be a string or object",
        }),
    }
}

fn object_string(map: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn metadata_string(
    metadata: &HashMap<String, Value>,
    key: &'static str,
) -> A2AMappingResult<Option<String>> {
    let Some(value) = metadata.get(key) else {
        return Ok(None);
    };
    match value {
        Value::String(value) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Value::String(_) => Err(A2AMappingError::InvalidMetadata {
            field: key.to_string(),
            reason: "metadata string must not be empty",
        }),
        _ => Err(A2AMappingError::InvalidMetadata {
            field: key.to_string(),
            reason: "metadata value must be a string",
        }),
    }
}

fn bounded_metadata_from_values(metadata: &HashMap<String, Value>) -> AgentAttributes {
    metadata
        .iter()
        .filter(|(key, _)| key.starts_with("io.rakka."))
        .filter_map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), bounded_value(value)))
        })
        .collect()
}

fn bounded_value(value: &str) -> String {
    const ELLIPSIS: &str = "...";
    if value.len() <= MAX_BOUNDED_METADATA_VALUE_BYTES {
        return value.to_string();
    }
    let mut end = MAX_BOUNDED_METADATA_VALUE_BYTES - ELLIPSIS.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{ELLIPSIS}", &value[..end])
}

fn first_service_param(params: &ServiceParams, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(|values| values.first())
        .filter(|value| !value.trim().is_empty())
        .cloned()
}

pub(crate) fn derived_deduplication_key(tenant: &str, task_id: &str, command_id: &str) -> String {
    format!("a2a:{tenant}:{task_id}:{command_id}")
}

pub(crate) fn generated_task_id(tenant: &str, message_id: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in tenant
        .as_bytes()
        .iter()
        .copied()
        .chain([0xff])
        .chain(message_id.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    format!("task-{hash:016x}")
}

fn reject_conflict(
    field: impl Into<String>,
    canonical: &str,
    metadata: &str,
    metadata_field: impl Into<String>,
) -> A2AMappingResult<()> {
    if canonical != metadata {
        return Err(A2AMappingError::MetadataConflict {
            field: field.into(),
            canonical: canonical.to_string(),
            metadata: format!("{}={metadata}", metadata_field.into()),
        });
    }
    Ok(())
}

pub(crate) fn require_non_blank(value: &str, field: &'static str) -> A2AMappingResult<()> {
    if value.trim().is_empty() {
        return Err(A2AMappingError::MissingField { field });
    }
    Ok(())
}

fn value_to_conflict_string(value: &Value) -> String {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| value.to_string())
}

trait A2ARoleLabel {
    fn role_label(&self) -> &'static str;
}

impl A2ARoleLabel for a2a::Role {
    fn role_label(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Unspecified => "unspecified",
        }
    }
}

/// Returns a timestamp suitable for tests and non-runtime conversion callers.
#[must_use]
pub fn now_agent_timestamp() -> AgentTimestampMillis {
    AgentTimestampMillis::new(current_timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use a2a::{Part, Role};
    use rakka_agent_workflow::{is_forbidden_agent_metric_attribute, AGENT_TRIGGER_KIND_ATTRIBUTE};
    use serde_json::json;

    use crate::testing::fixture_workflow;

    const RESOLVER: A2AHeaderTenantResolver = A2AHeaderTenantResolver;

    fn params() -> ServiceParams {
        ServiceParams::new()
    }

    fn traced_params() -> ServiceParams {
        ServiceParams::from([(
            TRACEPARENT_HEADER.to_string(),
            vec!["00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()],
        )])
    }

    fn request(message: Message) -> SendMessageRequest {
        SendMessageRequest {
            message,
            configuration: None,
            metadata: None,
            tenant: Some("tenant-a".to_string()),
        }
    }

    fn normalize(
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> A2AMappingResult<NormalizedA2ACommand> {
        normalize_send_message_request(
            &RESOLVER,
            Some(DEFAULT_TENANT),
            params,
            request,
            &fixture_workflow(),
        )
    }

    fn draft(
        params: &ServiceParams,
        request: &SendMessageRequest,
        policy: A2APayloadPolicy,
    ) -> A2AMappingResult<A2ACommandDraft> {
        build_send_message_command_draft(
            &RESOLVER,
            Some(DEFAULT_TENANT),
            params,
            request,
            &fixture_workflow(),
            policy,
            AgentTimestampMillis::new(10),
        )
    }

    #[test]
    fn principal_refs_round_trip_colon_bearing_ids_unbroken() {
        // Colon-free principals keep the compact string byte-identical to
        // what existing clients parse.
        let plain = PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "alice".to_string(),
            display_name: Some("Alice".to_string()),
        };
        let encoded = principal_ref_to_value(&plain);
        assert_eq!(encoded, Value::String("user:alice:Alice".to_string()));
        let decoded = principal_ref_from_value(&encoded).expect("the compact form decodes");
        assert_eq!(decoded, plain);

        // A colon-bearing id rides the object form and round-trips verbatim
        // instead of truncating at its first colon under `splitn` decoding.
        let workload = PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "spiffe://mesh/agent-host".to_string(),
            display_name: None,
        };
        let encoded = principal_ref_to_value(&workload);
        assert!(
            matches!(&encoded, Value::Object(_)),
            "a colon-bearing id must not use the ambiguous join, got {encoded}"
        );
        let decoded = principal_ref_from_value(&encoded).expect("the object form decodes");
        assert_eq!(decoded, workload);

        // The durable provenance label stays the compact join for colon-free
        // principals and switches to the canonical object otherwise, so
        // ("user:a", "b") and ("user", "a:b") never collapse to one string.
        assert_eq!(principal_provenance_label(&plain), "user:alice");
        let ambiguous_left = PrincipalRef {
            principal_type: "user:a".to_string(),
            principal_id: "b".to_string(),
            display_name: None,
        };
        let ambiguous_right = PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "a:b".to_string(),
            display_name: None,
        };
        assert_ne!(
            principal_provenance_label(&ambiguous_left),
            principal_provenance_label(&ambiguous_right),
        );
    }

    #[test]
    fn new_message_generates_canonical_run_id_and_default_command_id() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();
        let request = request(message.clone());
        let normalized = normalize(&params(), &request).expect("normalize");
        let retry = normalize(&params(), &request).expect("retry normalize");

        assert_eq!(normalized.intent, A2ATaskIntent::NewTask);
        assert_eq!(normalized.task_id, "task-fa4457a412484d2b");
        assert_eq!(retry.task_id, normalized.task_id);
        assert_eq!(normalized.run_id().as_str(), normalized.task_id);
        assert_eq!(normalized.command_id.as_str(), "msg-1");
        assert_eq!(normalized.tenant.as_str(), "tenant-a");
        assert_eq!(normalized.tenant_source, A2ATenantSource::Request);
    }

    #[test]
    fn continuation_targets_message_task_id() {
        let mut message = Message::new(Role::User, vec![Part::text("again")]);
        message.message_id = "msg-2".to_string();
        message.task_id = Some("task-123".to_string());

        let normalized = normalize(&params(), &request(message)).expect("normalize");

        assert_eq!(normalized.intent, A2ATaskIntent::ContinueTask);
        assert_eq!(normalized.task_id, "task-123");
    }

    #[test]
    fn tenant_header_is_canonical_and_conflicts_are_rejected() {
        let mut params = ServiceParams::new();
        params.insert("x-tenant-id".to_string(), vec!["tenant-header".to_string()]);
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-3".to_string();

        let mut req = request(message);
        req.tenant = Some("tenant-body".to_string());
        let error = normalize(&params, &req).expect_err("tenant conflict");
        assert_eq!(error.code(), "metadata-conflict");
    }

    #[test]
    fn missing_tenant_without_default_is_rejected() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-tenantless".to_string();
        let mut req = request(message);
        req.tenant = None;

        let error =
            normalize_send_message_request(&RESOLVER, None, &params(), &req, &fixture_workflow())
                .expect_err("tenant required in tenant-scoped mode");
        assert_eq!(error.code(), "tenant-required");
    }

    #[test]
    fn command_id_metadata_conflict_is_rejected() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-4".to_string();
        message.metadata = Some(HashMap::from([(
            META_COMMAND_ID.to_string(),
            Value::String("different".to_string()),
        )]));
        let error = normalize(&params(), &request(message)).expect_err("command id conflict");
        assert_eq!(error.code(), "metadata-conflict");
    }

    #[test]
    fn trace_headers_win_over_metadata_fallback() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-5".to_string();
        message.metadata = Some(HashMap::from([(
            META_TRACEPARENT.to_string(),
            Value::String("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_string()),
        )]));

        let normalized = normalize(&traced_params(), &request(message)).expect("normalize");

        assert_eq!(
            normalized.telemetry_context.trace_parent.as_deref(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }

    #[test]
    fn text_only_message_builds_valid_start_command_draft() {
        let fixture = include_str!("../tests/fixtures/send-message-new-task.json");
        let req: SendMessageRequest = serde_json::from_str(fixture).expect("fixture");
        let draft = draft(&params(), &req, A2APayloadPolicy::default()).expect("draft");

        assert!(matches!(draft.command.kind, AgentCommandKind::StartRun));
        assert!(matches!(draft.payload, A2ACommandPayload::Inline(_)));
        assert_eq!(draft.command.metadata.command_id.as_str(), "msg-fixture-1");
        assert_eq!(draft.command.metadata.tenant.as_str(), "tenant-fixture");
        assert_eq!(
            draft
                .command
                .attributes
                .get(AGENT_TRIGGER_KIND_ATTRIBUTE)
                .map(String::as_str),
            Some("api")
        );
    }

    #[test]
    fn continuation_builds_submit_signal_command_draft() {
        let mut message = Message::new(Role::User, vec![Part::text("next")]);
        message.message_id = "msg-6".to_string();
        message.task_id = Some("task-6".to_string());
        let draft =
            draft(&params(), &request(message), A2APayloadPolicy::default()).expect("draft");
        assert!(matches!(
            draft.command.kind,
            AgentCommandKind::SubmitSignal { .. }
        ));
        draft.command.validate().expect("valid command");
    }

    #[test]
    fn command_attributes_do_not_include_forbidden_hot_metric_labels() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-labels".to_string();
        message.task_id = Some("task-labels".to_string());
        let draft =
            draft(&params(), &request(message), A2APayloadPolicy::default()).expect("draft");

        for key in draft.command.attributes.keys() {
            assert!(
                !is_forbidden_agent_metric_attribute(key),
                "command attribute {key} must not be a forbidden hot label"
            );
        }
    }

    #[test]
    fn non_text_parts_become_artifact_references_when_not_inline() {
        let mut message = Message::new(
            Role::User,
            vec![Part::url("https://example.test/input.json").with_media_type("application/json")],
        );
        message.message_id = "msg-7".to_string();
        let draft = draft(
            &params(),
            &request(message),
            A2APayloadPolicy {
                inline_limit_bytes: 1,
                allow_artifact_references: true,
            },
        )
        .expect("draft");
        assert_eq!(draft.payload.artifact_drafts().len(), 1);
        assert_eq!(
            draft.payload.artifact_drafts()[0].reference.uri,
            "https://example.test/input.json"
        );
        assert!(draft.payload.artifact_drafts()[0].content.is_none());
    }

    #[test]
    fn multiple_oversized_parts_preserve_order_as_artifact_references() {
        let mut message = Message::new(
            Role::User,
            vec![
                Part::text("first part"),
                Part::url("https://example.test/second.json").with_media_type("application/json"),
            ],
        );
        message.message_id = "msg-multipart".to_string();
        message.task_id = Some("task-multipart".to_string());
        let draft = draft(
            &params(),
            &request(message),
            A2APayloadPolicy {
                inline_limit_bytes: 1,
                allow_artifact_references: true,
            },
        )
        .expect("draft");

        let drafts = draft.payload.artifact_drafts();
        assert_eq!(drafts.len(), 2);
        assert_eq!(drafts[0].reference.artifact_id, "msg-multipart-part-0");
        assert_eq!(
            drafts[0].reference.uri,
            "a2a-message://task-multipart/part/0"
        );
        assert_eq!(drafts[1].reference.artifact_id, "msg-multipart-part-1");
        assert_eq!(drafts[1].reference.uri, "https://example.test/second.json");
    }

    #[test]
    fn oversized_part_content_is_retained_for_later_persistence() {
        let mut message = Message::new(Role::User, vec![Part::text("keep these bytes")]);
        message.message_id = "msg-content".to_string();
        message.task_id = Some("task-content".to_string());
        let draft = draft(
            &params(),
            &request(message),
            A2APayloadPolicy {
                inline_limit_bytes: 1,
                allow_artifact_references: true,
            },
        )
        .expect("draft");

        let drafts = draft.payload.artifact_drafts();
        assert_eq!(drafts.len(), 1);
        let content = drafts[0].content.as_ref().expect("content");
        assert_eq!(content.bytes, b"keep these bytes");
        assert_eq!(
            content.size_bytes,
            drafts[0].reference.byte_len.expect("len")
        );
        assert_eq!(drafts[0].reference.checksum, None);
    }

    #[test]
    fn oversized_payload_without_artifact_strategy_is_rejected() {
        let mut message = Message::new(Role::User, vec![Part::text("too large")]);
        message.message_id = "msg-8".to_string();
        let error = draft(
            &params(),
            &request(message),
            A2APayloadPolicy {
                inline_limit_bytes: 1,
                allow_artifact_references: false,
            },
        )
        .expect_err("oversize");
        assert_eq!(error.code(), "payload-too-large");
    }

    #[test]
    fn principal_ref_object_is_parsed() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-9".to_string();
        message.metadata = Some(HashMap::from([(
            META_PRINCIPAL_REF.to_string(),
            json!({"type": "user", "id": "alice", "displayName": "Alice"}),
        )]));
        let normalized = normalize(&params(), &request(message)).expect("normalize");
        let principal = normalized.principal.expect("principal");
        assert_eq!(principal.principal_type, "user");
        assert_eq!(principal.principal_id, "alice");
    }

    #[test]
    fn cancellation_builds_cancel_run_command() {
        let request = CancelTaskRequest {
            id: "task-10".to_string(),
            metadata: None,
            tenant: Some("tenant-a".to_string()),
        };
        let draft = build_cancel_task_command_draft(
            &RESOLVER,
            Some(DEFAULT_TENANT),
            &params(),
            &request,
            &fixture_workflow(),
            AgentTimestampMillis::new(10),
        )
        .expect("draft");
        assert!(matches!(draft.command.kind, AgentCommandKind::CancelRun));
        assert_eq!(draft.command.metadata.run_id.as_str(), "task-10");
        assert_eq!(
            draft
                .command
                .attributes
                .get(AGENT_TRIGGER_KIND_ATTRIBUTE)
                .map(String::as_str),
            Some("api")
        );
    }

    #[test]
    fn invalid_workflow_selection_fails_validation() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-11".to_string();
        message.metadata = Some(HashMap::from([(
            META_WORKFLOW_TYPE.to_string(),
            Value::String("other-workflow".to_string()),
        )]));
        let error = normalize(&params(), &request(message)).expect_err("workflow mismatch");
        assert_eq!(error.code(), "invalid-workflow-selection");
    }

    #[test]
    fn workflow_selection_extraction_and_matching() {
        let workflow = fixture_workflow();
        let metadata = HashMap::from([
            (
                META_WORKFLOW_ID.to_string(),
                Value::String(workflow.workflow_id.as_str().to_string()),
            ),
            (
                META_DEFINITION_VERSION.to_string(),
                Value::String(workflow.definition_version.to_string()),
            ),
        ]);
        let selection = A2AWorkflowSelection::from_metadata(&metadata).expect("selection");
        assert!(!selection.is_empty());
        assert!(selection.matches(&workflow));

        let mismatched = A2AWorkflowSelection {
            workflow_id: Some("other".to_string()),
            ..Default::default()
        };
        assert!(!mismatched.matches(&workflow));
        assert!(A2AWorkflowSelection::default().is_empty());
    }

    #[test]
    fn default_tenant_source_is_explicit() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-12".to_string();
        let mut req = request(message);
        req.tenant = None;
        let normalized = normalize(&params(), &req).expect("normalize");
        assert_eq!(normalized.tenant.as_str(), DEFAULT_TENANT);
        assert_eq!(normalized.tenant_source, A2ATenantSource::Default);
    }

    #[test]
    fn fixture_metadata_conflict_is_stable() {
        let fixture = include_str!("../tests/fixtures/send-message-conflict.json");
        let req: SendMessageRequest = serde_json::from_str(fixture).expect("fixture");
        let error = draft(&params(), &req, A2APayloadPolicy::default()).expect_err("conflict");
        assert_eq!(error.code(), "metadata-conflict");
    }

    #[test]
    fn command_id_conflict_error_attributes_values_to_their_sources() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-canonical".to_string();
        message.metadata = Some(HashMap::from([(
            META_COMMAND_ID.to_string(),
            Value::String("different".to_string()),
        )]));
        let error = normalize(&params(), &request(message)).expect_err("command id conflict");
        let rendered = error.to_string();
        assert!(rendered.contains("canonical `msg-canonical`"));
        assert!(rendered.contains(&format!("{META_COMMAND_ID}=different")));
    }

    #[test]
    fn read_tenant_uses_header_first_and_rejects_conflicts() {
        let mut params = ServiceParams::new();
        params.insert("x-rakka-tenant".to_string(), vec!["tenant-hdr".to_string()]);

        assert_eq!(
            canonical_read_tenant(&RESOLVER, &params, None).expect("header tenant"),
            Some("tenant-hdr".to_string())
        );
        assert_eq!(
            canonical_read_tenant(&RESOLVER, &ServiceParams::new(), Some("tenant-body"))
                .expect("body"),
            Some("tenant-body".to_string())
        );
        assert_eq!(
            canonical_read_tenant(&RESOLVER, &ServiceParams::new(), None).expect("unscoped"),
            None
        );
        let error =
            canonical_read_tenant(&RESOLVER, &params, Some("tenant-body")).expect_err("conflict");
        assert_eq!(error.code(), "metadata-conflict");
    }

    #[test]
    fn malformed_transport_traceparent_is_ignored_not_rejected() {
        let params =
            ServiceParams::from([(TRACEPARENT_HEADER.to_string(), vec!["00-bad".to_string()])]);
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-bad-trace".to_string();
        message.metadata = Some(HashMap::from([(
            META_TRACEPARENT.to_string(),
            Value::String("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_string()),
        )]));

        let normalized = normalize(&params, &request(message))
            .expect("malformed transport trace context must not fail the request");

        assert_eq!(
            normalized.telemetry_context.trace_parent.as_deref(),
            Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
        );
    }

    #[test]
    fn bounded_metadata_values_are_byte_bounded() {
        let multibyte = "🦀".repeat(300);
        let bounded = bounded_value(&multibyte);
        assert!(bounded.len() <= MAX_BOUNDED_METADATA_VALUE_BYTES);
        assert!(bounded.ends_with("..."));

        let ascii = "a".repeat(300);
        let bounded = bounded_value(&ascii);
        assert!(bounded.len() <= MAX_BOUNDED_METADATA_VALUE_BYTES);
        assert!(bounded.ends_with("..."));

        let short = "unchanged";
        assert_eq!(bounded_value(short), short);
    }
}
