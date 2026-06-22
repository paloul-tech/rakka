//! Credential binding resolver contracts.
//!
//! Rakka persists logical credential binding references, never resolved secret
//! values. Application code implements [`AgentCredentialResolver`] to resolve a
//! binding for one dispatch attempt or short-lived adapter call.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use rakka_workflow::OutboxDispatchResult;
use serde::{Deserialize, Serialize};

use crate::{
    AgentAdapterFailureClass, AgentAttributes, AgentCausationId, AgentCompiledNodeId,
    AgentCompiledPlanFingerprint, AgentCorrelationId, AgentCredentialBindingRef, AgentEffect,
    AgentEffectKind, AgentEffectTarget, AgentRunId, AgentTelemetryContext, AgentTenantId,
    AgentTimestampMillis, AgentWorkflowId,
};

/// Target attribute key containing a logical credential binding reference.
pub const AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE: &str = "credential_binding_ref";

/// Shared result type for credential resolver operations.
pub type AgentCredentialResult<T> = Result<T, AgentCredentialError>;

/// Boxed future returned by application credential resolvers.
pub type AgentCredentialResolverFuture<'a> =
    Pin<Box<dyn Future<Output = AgentCredentialResult<AgentEphemeralCredential>> + Send + 'a>>;

/// Intended use for a resolved credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentCredentialUse {
    /// Model provider API call.
    ModelProvider,
    /// Tool adapter invocation.
    ToolAdapter,
    /// Process adapter invocation.
    ProcessAdapter,
    /// HTTP request.
    HttpRequest,
    /// gRPC request.
    GrpcRequest,
    /// Stream publication.
    StreamPublisher,
    /// Artifact store operation.
    ArtifactStore,
    /// Child workflow command.
    ChildWorkflow,
    /// Notification provider request.
    Notification,
    /// Audit event sink.
    AuditEvent,
}

impl AgentCredentialUse {
    /// Infers the usual credential use for an effect kind.
    #[must_use]
    pub const fn for_effect_kind(kind: AgentEffectKind) -> Option<Self> {
        match kind {
            AgentEffectKind::ModelCall => Some(Self::ModelProvider),
            AgentEffectKind::ToolCall => Some(Self::ToolAdapter),
            AgentEffectKind::ProcessCall => Some(Self::ProcessAdapter),
            AgentEffectKind::HttpCall => Some(Self::HttpRequest),
            AgentEffectKind::GrpcCall => Some(Self::GrpcRequest),
            AgentEffectKind::StreamPublish => Some(Self::StreamPublisher),
            AgentEffectKind::ArtifactWrite => Some(Self::ArtifactStore),
            AgentEffectKind::ChildWorkflowCommand => Some(Self::ChildWorkflow),
            AgentEffectKind::Notification => Some(Self::Notification),
            AgentEffectKind::AuditEvent => Some(Self::AuditEvent),
            AgentEffectKind::HumanApprovalRequest => None,
        }
    }

    /// Stable lowercase label for bounded diagnostics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::ModelProvider => "model-provider",
            Self::ToolAdapter => "tool-adapter",
            Self::ProcessAdapter => "process-adapter",
            Self::HttpRequest => "http-request",
            Self::GrpcRequest => "grpc-request",
            Self::StreamPublisher => "stream-publisher",
            Self::ArtifactStore => "artifact-store",
            Self::ChildWorkflow => "child-workflow",
            Self::Notification => "notification",
            Self::AuditEvent => "audit-event",
        }
    }
}

/// Inputs supplied to an application credential resolver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCredentialResolutionRequest {
    /// Optional tenant or namespace that owns the run.
    pub tenant: Option<AgentTenantId>,
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Durable run id.
    pub run_id: AgentRunId,
    /// Immutable compiled plan fingerprint selected for the run.
    pub plan_fingerprint: AgentCompiledPlanFingerprint,
    /// Compiled graph node id requesting a credential.
    pub node_id: AgentCompiledNodeId,
    /// Logical effect target.
    pub target: AgentEffectTarget,
    /// Logical credential binding reference.
    pub credential_binding_ref: AgentCredentialBindingRef,
    /// Intended credential use.
    pub credential_use: AgentCredentialUse,
    /// Command or event that caused the dispatch.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related runtime records.
    pub correlation_id: AgentCorrelationId,
    /// Trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
}

impl AgentCredentialResolutionRequest {
    /// Creates a credential resolution request.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        tenant: Option<AgentTenantId>,
        workflow_id: AgentWorkflowId,
        run_id: AgentRunId,
        plan_fingerprint: AgentCompiledPlanFingerprint,
        node_id: AgentCompiledNodeId,
        target: AgentEffectTarget,
        credential_binding_ref: AgentCredentialBindingRef,
        credential_use: AgentCredentialUse,
        causation_id: AgentCausationId,
        correlation_id: AgentCorrelationId,
        telemetry_context: AgentTelemetryContext,
    ) -> Self {
        Self {
            tenant,
            workflow_id,
            run_id,
            plan_fingerprint,
            node_id,
            target,
            credential_binding_ref,
            credential_use,
            causation_id,
            correlation_id,
            telemetry_context,
        }
    }

    /// Builds a credential resolution request from a durable effect.
    pub fn from_effect(
        tenant: Option<AgentTenantId>,
        workflow_id: AgentWorkflowId,
        run_id: AgentRunId,
        plan_fingerprint: AgentCompiledPlanFingerprint,
        node_id: AgentCompiledNodeId,
        credential_use: AgentCredentialUse,
        effect: &AgentEffect,
    ) -> AgentCredentialResult<Self> {
        let credential_binding_ref =
            credential_binding_ref_from_effect(effect).ok_or_else(|| {
                AgentCredentialError::InvalidRequest {
                    field: "credential_binding_ref",
                    reason: "effect target is missing logical credential binding ref",
                }
            })?;
        Ok(Self::new(
            tenant,
            workflow_id,
            run_id,
            plan_fingerprint,
            node_id,
            effect.target.clone(),
            credential_binding_ref,
            credential_use,
            effect.causation_id.clone(),
            effect.correlation_id.clone(),
            effect.telemetry_context.clone(),
        ))
    }
}

/// Application-implemented resolver for logical credential bindings.
pub trait AgentCredentialResolver: Send + Sync {
    /// Resolves a logical binding into an ephemeral in-memory credential.
    fn resolve<'a>(
        &'a self,
        request: AgentCredentialResolutionRequest,
    ) -> AgentCredentialResolverFuture<'a>;
}

/// Secret material held only in memory for one dispatch attempt.
#[derive(Clone, PartialEq, Eq)]
pub enum AgentEphemeralCredentialMaterial {
    /// Bearer token secret.
    BearerToken {
        /// Bearer token value.
        token: String,
    },
    /// API key passed by header or provider-specific parameter.
    ApiKey {
        /// Header or parameter name.
        name: String,
        /// Secret key value.
        value: String,
    },
    /// Basic authentication credential.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Provider-specific secret value.
    Custom {
        /// Credential scheme or kind.
        scheme: String,
        /// Secret value.
        value: String,
    },
}

impl AgentEphemeralCredentialMaterial {
    /// Stable material kind label.
    #[must_use]
    pub const fn kind_label(&self) -> &'static str {
        match self {
            Self::BearerToken { .. } => "bearer-token",
            Self::ApiKey { .. } => "api-key",
            Self::Basic { .. } => "basic",
            Self::Custom { .. } => "custom",
        }
    }
}

impl Debug for AgentEphemeralCredentialMaterial {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEphemeralCredentialMaterial")
            .field("kind", &self.kind_label())
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Resolved credential returned by an application resolver for one dispatch.
#[derive(Clone, PartialEq, Eq)]
pub struct AgentEphemeralCredential {
    material: AgentEphemeralCredentialMaterial,
    expires_at: Option<AgentTimestampMillis>,
    attributes: AgentAttributes,
}

impl AgentEphemeralCredential {
    /// Creates an ephemeral credential from secret material.
    #[must_use]
    pub fn new(material: AgentEphemeralCredentialMaterial) -> Self {
        Self {
            material,
            expires_at: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Creates a bearer-token credential.
    #[must_use]
    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self::new(AgentEphemeralCredentialMaterial::BearerToken {
            token: token.into(),
        })
    }

    /// Creates an API-key credential.
    #[must_use]
    pub fn api_key(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(AgentEphemeralCredentialMaterial::ApiKey {
            name: name.into(),
            value: value.into(),
        })
    }

    /// Creates a basic-auth credential.
    #[must_use]
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(AgentEphemeralCredentialMaterial::Basic {
            username: username.into(),
            password: password.into(),
        })
    }

    /// Creates a provider-specific credential.
    #[must_use]
    pub fn custom(scheme: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(AgentEphemeralCredentialMaterial::Custom {
            scheme: scheme.into(),
            value: value.into(),
        })
    }

    /// Sets the short-lived expiration timestamp, when known.
    #[must_use]
    pub const fn expires_at(mut self, expires_at: AgentTimestampMillis) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Adds bounded in-memory metadata.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Returns the secret material for adapter use.
    #[must_use]
    pub const fn material(&self) -> &AgentEphemeralCredentialMaterial {
        &self.material
    }

    /// Returns the expiration timestamp.
    #[must_use]
    pub const fn expires_at_millis(&self) -> Option<AgentTimestampMillis> {
        self.expires_at
    }

    /// Returns bounded in-memory metadata.
    #[must_use]
    pub const fn attributes(&self) -> &AgentAttributes {
        &self.attributes
    }
}

impl Debug for AgentEphemeralCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let attribute_keys = self.attributes.keys().collect::<Vec<_>>();
        f.debug_struct("AgentEphemeralCredential")
            .field("material", &self.material)
            .field("expires_at", &self.expires_at)
            .field("attribute_keys", &attribute_keys)
            .finish()
    }
}

/// Credential resolver failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentCredentialError {
    /// The logical binding did not exist.
    MissingBinding {
        /// Logical binding reference.
        binding_ref: AgentCredentialBindingRef,
    },
    /// The logical binding was revoked.
    RevokedBinding {
        /// Logical binding reference.
        binding_ref: AgentCredentialBindingRef,
    },
    /// The run is not authorized to use the binding.
    Unauthorized {
        /// Logical binding reference.
        binding_ref: AgentCredentialBindingRef,
    },
    /// The requested use is not allowed for the binding.
    InvalidUse {
        /// Logical binding reference.
        binding_ref: AgentCredentialBindingRef,
        /// Requested use.
        credential_use: AgentCredentialUse,
    },
    /// Resolver backend is unavailable or timed out.
    Unavailable {
        /// Logical binding reference.
        binding_ref: AgentCredentialBindingRef,
        /// Stable bounded reason.
        reason: String,
        /// Optional retry-after timestamp.
        retry_after: Option<AgentTimestampMillis>,
    },
    /// The resolution request was malformed before resolver lookup.
    InvalidRequest {
        /// Invalid field.
        field: &'static str,
        /// Stable bounded reason.
        reason: &'static str,
    },
}

impl AgentCredentialError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingBinding { .. } => "credential-binding-missing",
            Self::RevokedBinding { .. } => "credential-binding-revoked",
            Self::Unauthorized { .. } => "credential-binding-unauthorized",
            Self::InvalidUse { .. } => "invalid-credential-use",
            Self::Unavailable { .. } => "credential-resolver-unavailable",
            Self::InvalidRequest { .. } => "invalid-credential-request",
        }
    }

    /// Maps resolver failure to adapter retry/permanent classification.
    #[must_use]
    pub const fn failure_class(&self) -> AgentAdapterFailureClass {
        match self {
            Self::Unavailable { .. } => AgentAdapterFailureClass::Retryable,
            Self::MissingBinding { .. }
            | Self::RevokedBinding { .. }
            | Self::Unauthorized { .. }
            | Self::InvalidUse { .. }
            | Self::InvalidRequest { .. } => AgentAdapterFailureClass::Permanent,
        }
    }

    /// Maps resolver failure to the lower-level outbox dispatch result.
    #[must_use]
    pub fn to_outbox_dispatch_result(&self) -> OutboxDispatchResult {
        let prefix = match self.failure_class() {
            AgentAdapterFailureClass::Retryable => "retryable",
            AgentAdapterFailureClass::Permanent => "permanent",
        };
        OutboxDispatchResult::failure(format!("{prefix}:{}", self.code()))
    }
}

impl Display for AgentCredentialError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBinding { binding_ref } => {
                write!(f, "credential binding `{binding_ref}` does not exist")
            }
            Self::RevokedBinding { binding_ref } => {
                write!(f, "credential binding `{binding_ref}` is revoked")
            }
            Self::Unauthorized { binding_ref } => {
                write!(f, "credential binding `{binding_ref}` is not authorized")
            }
            Self::InvalidUse {
                binding_ref,
                credential_use,
            } => write!(
                f,
                "credential binding `{binding_ref}` does not allow `{}` use",
                credential_use.as_label()
            ),
            Self::Unavailable {
                binding_ref,
                reason,
                retry_after,
            } => write!(
                f,
                "credential resolver unavailable for binding `{binding_ref}`: {reason}; retry_after={retry_after:?}"
            ),
            Self::InvalidRequest { field, reason } => {
                write!(f, "invalid credential resolution request field {field}: {reason}")
            }
        }
    }
}

impl Error for AgentCredentialError {}

/// Returns the logical credential binding ref embedded in an effect target.
#[must_use]
pub fn credential_binding_ref_from_effect(
    effect: &AgentEffect,
) -> Option<AgentCredentialBindingRef> {
    effect
        .target
        .attributes
        .get(AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .map(AgentCredentialBindingRef::new)
}
