//! Artifact reference policy and application-owned storage boundary.
//!
//! Rakka does not own blob storage in v1. Agent workflow state keeps prompts,
//! completions, tool outputs, files, screenshots, logs, embeddings, and large
//! state behind [`ArtifactRef`] values while applications provide the backing
//! store implementation.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use crate::{
    AgentAttributes, AgentAuditEvent, AgentEffect, AgentRunState, AgentStatePayload,
    AgentTimestampMillis, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, InlineState,
    RedactionStatus,
};

/// Default maximum bytes allowed in hot inline run state.
pub const DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES: u64 = 4 * 1024;

/// Default retention class used by test and example artifact stores.
pub const DEFAULT_AGENT_ARTIFACT_RETENTION_CLASS: &str = "standard";

/// Shared result type for artifact policy and store operations.
pub type AgentArtifactResult<T> = Result<T, AgentArtifactError>;

/// Boxed future returned by artifact stores.
pub type AgentArtifactStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentArtifactResult<T>> + Send + 'a>>;

/// Artifact policy, validation, and storage errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentArtifactError {
    /// Artifact reference metadata is invalid.
    InvalidReference {
        /// Artifact id, when available.
        artifact_id: Option<String>,
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Inline state metadata is invalid.
    InvalidInlineState {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Inline state exceeded the configured hot-state limit.
    InlineStateTooLarge {
        /// Declared or observed inline state size.
        size_bytes: u64,
        /// Configured inline state limit.
        limit_bytes: u64,
    },
    /// Artifact bytes were not found in a store.
    ArtifactNotFound {
        /// Missing artifact id.
        artifact_id: String,
    },
    /// Application-owned store failed.
    Store {
        /// Stable bounded store failure message.
        message: String,
    },
}

impl AgentArtifactError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidReference { .. } => "invalid-artifact-reference",
            Self::InvalidInlineState { .. } => "invalid-inline-state",
            Self::InlineStateTooLarge { .. } => "inline-state-too-large",
            Self::ArtifactNotFound { .. } => "artifact-not-found",
            Self::Store { .. } => "artifact-store",
        }
    }
}

impl Display for AgentArtifactError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReference {
                artifact_id,
                field,
                reason,
            } => match artifact_id {
                Some(artifact_id) => {
                    write!(
                        f,
                        "invalid artifact reference {artifact_id} field {field}: {reason}"
                    )
                }
                None => write!(f, "invalid artifact reference field {field}: {reason}"),
            },
            Self::InvalidInlineState { field, reason } => {
                write!(f, "invalid inline state field {field}: {reason}")
            }
            Self::InlineStateTooLarge {
                size_bytes,
                limit_bytes,
            } => write!(
                f,
                "inline state has {size_bytes} bytes, exceeding configured limit {limit_bytes}"
            ),
            Self::ArtifactNotFound { artifact_id } => {
                write!(f, "artifact {artifact_id} was not found")
            }
            Self::Store { message } => write!(f, "artifact store failed: {message}"),
        }
    }
}

impl Error for AgentArtifactError {}

/// Validation policy for hot state and artifact references.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentArtifactPolicy {
    /// Maximum inline state bytes allowed in hot durable run state.
    pub inline_state_limit_bytes: u64,
    /// Whether artifact references must include a checksum.
    pub require_checksum: bool,
    /// Whether artifact references must include content type.
    pub require_content_type: bool,
    /// Whether artifact references must include byte length.
    pub require_byte_len: bool,
    /// Whether artifact references must carry a non-unknown redaction status.
    pub require_redaction_status: bool,
}

impl AgentArtifactPolicy {
    /// Creates the default policy used by agent workflow validation helpers.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inline_state_limit_bytes: DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES,
            require_checksum: true,
            require_content_type: true,
            require_byte_len: true,
            require_redaction_status: true,
        }
    }

    /// Sets the maximum inline state size.
    #[must_use]
    pub const fn inline_state_limit_bytes(mut self, limit_bytes: u64) -> Self {
        self.inline_state_limit_bytes = limit_bytes;
        self
    }

    /// Sets whether checksum metadata is required.
    #[must_use]
    pub const fn require_checksum(mut self, require_checksum: bool) -> Self {
        self.require_checksum = require_checksum;
        self
    }

    /// Sets whether content type metadata is required.
    #[must_use]
    pub const fn require_content_type(mut self, require_content_type: bool) -> Self {
        self.require_content_type = require_content_type;
        self
    }

    /// Sets whether byte length metadata is required.
    #[must_use]
    pub const fn require_byte_len(mut self, require_byte_len: bool) -> Self {
        self.require_byte_len = require_byte_len;
        self
    }

    /// Sets whether redaction status must be known.
    #[must_use]
    pub const fn require_redaction_status(mut self, require_redaction_status: bool) -> Self {
        self.require_redaction_status = require_redaction_status;
        self
    }

    /// Returns true when a payload of this size may be stored inline.
    #[must_use]
    pub const fn allows_inline_state_bytes(&self, size_bytes: u64) -> bool {
        size_bytes <= self.inline_state_limit_bytes
    }

    /// Validates one artifact reference.
    pub fn validate_reference(&self, reference: &ArtifactRef) -> AgentArtifactResult<()> {
        validate_non_empty(
            Some(reference.artifact_id.clone()),
            "artifact_id",
            &reference.artifact_id,
        )?;
        validate_non_empty(Some(reference.artifact_id.clone()), "uri", &reference.uri)?;
        if self.require_checksum {
            validate_optional_non_empty(
                Some(reference.artifact_id.clone()),
                "checksum",
                reference.checksum.as_deref(),
            )?;
        }
        if self.require_content_type {
            validate_optional_non_empty(
                Some(reference.artifact_id.clone()),
                "content_type",
                reference.content_type.as_deref(),
            )?;
        }
        if self.require_byte_len && reference.byte_len.is_none() {
            return invalid_reference(
                Some(reference.artifact_id.clone()),
                "byte_len",
                "byte length is required",
            );
        }
        validate_optional_non_empty(
            Some(reference.artifact_id.clone()),
            "retention_class",
            reference.retention_class.as_deref(),
        )?;
        if self.require_redaction_status && reference.redaction == RedactionStatus::Unknown {
            return invalid_reference(
                Some(reference.artifact_id.clone()),
                "redaction",
                "redaction status must be known",
            );
        }
        if let Some(encryption) = &reference.encryption {
            validate_encryption_ref(Some(reference.artifact_id.clone()), encryption)?;
        }
        Ok(())
    }

    /// Validates inline state against this policy.
    pub fn validate_inline_state(&self, state: &InlineState) -> AgentArtifactResult<()> {
        if state.content_type.trim().is_empty() {
            return invalid_inline_state("content_type", "content type must not be empty");
        }
        let observed_size = state.bytes.len() as u64;
        if state.size_bytes != observed_size {
            return invalid_inline_state("size_bytes", "declared size must match byte length");
        }
        if !self.allows_inline_state_bytes(observed_size) {
            return Err(AgentArtifactError::InlineStateTooLarge {
                size_bytes: observed_size,
                limit_bytes: self.inline_state_limit_bytes,
            });
        }
        Ok(())
    }

    /// Validates all hot-state artifact references in one run state.
    pub fn validate_run_state(&self, run_state: &AgentRunState) -> AgentArtifactResult<()> {
        if let Some(inputs_ref) = &run_state.inputs_ref {
            self.validate_reference(inputs_ref)?;
        }
        match &run_state.state_payload {
            AgentStatePayload::Empty => {}
            AgentStatePayload::Artifact(reference) => self.validate_reference(reference)?,
            AgentStatePayload::Inline(state) => self.validate_inline_state(state)?,
        }
        for effect in &run_state.pending_effects {
            self.validate_effect(effect)?;
        }
        for checkpoint in &run_state.checkpoints {
            for reference in &checkpoint.context_artifacts {
                self.validate_reference(reference)?;
            }
        }
        Ok(())
    }

    /// Validates artifact references attached to one effect.
    pub fn validate_effect(&self, effect: &AgentEffect) -> AgentArtifactResult<()> {
        if let Some(payload_ref) = &effect.payload_ref {
            self.validate_reference(payload_ref)?;
        }
        if let Some(result_ref) = &effect.result_ref {
            self.validate_reference(result_ref)?;
        }
        Ok(())
    }
}

impl Default for AgentArtifactPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Write request accepted by application-owned artifact stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentArtifactWriteRequest {
    /// Optional caller-selected artifact id.
    pub artifact_id: Option<String>,
    /// Artifact kind.
    pub kind: ArtifactKind,
    /// Artifact bytes.
    pub bytes: Vec<u8>,
    /// Content type.
    pub content_type: Option<String>,
    /// Optional checksum supplied by the application.
    pub checksum: Option<String>,
    /// Retention class selected by application policy.
    pub retention_class: Option<String>,
    /// Redaction status for references to this artifact.
    pub redaction: RedactionStatus,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Bounded artifact metadata.
    pub metadata: AgentAttributes,
    /// Optional encryption metadata.
    pub encryption: Option<ArtifactEncryptionRef>,
}

impl AgentArtifactWriteRequest {
    /// Creates a write request for artifact bytes.
    #[must_use]
    pub fn new(
        kind: ArtifactKind,
        content_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
        created_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            artifact_id: None,
            kind,
            bytes: bytes.into(),
            content_type: Some(content_type.into()),
            checksum: None,
            retention_class: Some(DEFAULT_AGENT_ARTIFACT_RETENTION_CLASS.to_string()),
            redaction: RedactionStatus::ReferenceOnly,
            created_at,
            metadata: AgentAttributes::new(),
            encryption: None,
        }
    }

    /// Sets a caller-selected artifact id.
    #[must_use]
    pub fn artifact_id(mut self, artifact_id: impl Into<String>) -> Self {
        self.artifact_id = Some(artifact_id.into());
        self
    }

    /// Sets a checksum.
    #[must_use]
    pub fn checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }

    /// Sets a retention class.
    #[must_use]
    pub fn retention_class(mut self, retention_class: impl Into<String>) -> Self {
        self.retention_class = Some(retention_class.into());
        self
    }

    /// Sets redaction status.
    #[must_use]
    pub const fn redaction(mut self, redaction: RedactionStatus) -> Self {
        self.redaction = redaction;
        self
    }

    /// Adds bounded artifact metadata.
    #[must_use]
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Sets optional encryption metadata.
    #[must_use]
    pub fn encryption(mut self, encryption: ArtifactEncryptionRef) -> Self {
        self.encryption = Some(encryption);
        self
    }

    /// Returns the byte length of this write request.
    #[must_use]
    pub fn byte_len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// Bytes read from an application-owned artifact store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentArtifactRead {
    /// Artifact reference used for the read.
    pub reference: ArtifactRef,
    /// Artifact bytes.
    pub bytes: Vec<u8>,
}

/// Application-owned artifact store boundary.
pub trait AgentArtifactStore: Send {
    /// Writes artifact bytes and returns the durable reference.
    fn put_artifact<'a>(
        &'a mut self,
        request: AgentArtifactWriteRequest,
    ) -> AgentArtifactStoreFuture<'a, ArtifactRef>;

    /// Reads artifact bytes by durable reference.
    fn get_artifact<'a>(
        &'a self,
        reference: &'a ArtifactRef,
    ) -> AgentArtifactStoreFuture<'a, AgentArtifactRead>;
}

/// Validates one artifact reference with the default artifact policy.
pub fn validate_artifact_ref(reference: &ArtifactRef) -> AgentArtifactResult<()> {
    AgentArtifactPolicy::default().validate_reference(reference)
}

/// Validates one inline state payload with the default artifact policy.
pub fn validate_inline_state(state: &InlineState) -> AgentArtifactResult<()> {
    AgentArtifactPolicy::default().validate_inline_state(state)
}

/// Validates all hot-state artifact references in one run state.
pub fn validate_run_state_artifact_policy(
    run_state: &AgentRunState,
    policy: &AgentArtifactPolicy,
) -> AgentArtifactResult<()> {
    policy.validate_run_state(run_state)
}

/// Validates all artifact references in one effect.
pub fn validate_effect_artifact_policy(
    effect: &AgentEffect,
    policy: &AgentArtifactPolicy,
) -> AgentArtifactResult<()> {
    policy.validate_effect(effect)
}

/// Returns artifact references reachable from one run state.
#[must_use]
pub fn agent_run_artifact_refs(run_state: &AgentRunState) -> Vec<&ArtifactRef> {
    let mut references = Vec::new();
    if let Some(inputs_ref) = &run_state.inputs_ref {
        references.push(inputs_ref);
    }
    if let AgentStatePayload::Artifact(state_ref) = &run_state.state_payload {
        references.push(state_ref);
    }
    for effect in &run_state.pending_effects {
        references.extend(agent_effect_artifact_refs(effect));
    }
    for checkpoint in &run_state.checkpoints {
        references.extend(checkpoint.context_artifacts.iter());
    }
    references
}

/// Returns artifact references reachable from one effect.
#[must_use]
pub fn agent_effect_artifact_refs(effect: &AgentEffect) -> Vec<&ArtifactRef> {
    let mut references = Vec::new();
    if let Some(payload_ref) = &effect.payload_ref {
        references.push(payload_ref);
    }
    if let Some(result_ref) = &effect.result_ref {
        references.push(result_ref);
    }
    references
}

/// Returns artifact references attached to one audit event.
#[must_use]
pub fn agent_audit_artifact_refs(audit_event: &AgentAuditEvent) -> &[ArtifactRef] {
    &audit_event.artifact_refs
}

fn validate_encryption_ref(
    artifact_id: Option<String>,
    encryption: &ArtifactEncryptionRef,
) -> AgentArtifactResult<()> {
    validate_non_empty(
        artifact_id.clone(),
        "encryption.algorithm",
        &encryption.algorithm,
    )?;
    validate_non_empty(artifact_id, "encryption.key_ref", &encryption.key_ref)
}

fn validate_non_empty(
    artifact_id: Option<String>,
    field: &'static str,
    value: &str,
) -> AgentArtifactResult<()> {
    if value.trim().is_empty() {
        return invalid_reference(artifact_id, field, "field must not be empty");
    }
    Ok(())
}

fn validate_optional_non_empty(
    artifact_id: Option<String>,
    field: &'static str,
    value: Option<&str>,
) -> AgentArtifactResult<()> {
    match value {
        Some(value) => validate_non_empty(artifact_id, field, value),
        None => invalid_reference(artifact_id, field, "field is required"),
    }
}

fn invalid_reference<T>(
    artifact_id: Option<String>,
    field: &'static str,
    reason: &'static str,
) -> AgentArtifactResult<T> {
    Err(AgentArtifactError::InvalidReference {
        artifact_id,
        field,
        reason,
    })
}

fn invalid_inline_state<T>(field: &'static str, reason: &'static str) -> AgentArtifactResult<T> {
    Err(AgentArtifactError::InvalidInlineState { field, reason })
}
