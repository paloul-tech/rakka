//! Model and tool adapter traits for durable agent effects.
//!
//! Adapters translate durable [`AgentEffect`] records into
//! provider or tool calls. The traits keep large prompts, completions, arguments,
//! and tool outputs behind [`ArtifactRef`] values while preserving timeout,
//! idempotency, telemetry, receipt, retry, token, cost, and redaction metadata.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
#[cfg(feature = "process-tools")]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rakka_workflow::OutboxDispatchResult;
use serde::{Deserialize, Serialize};

use crate::{
    AgentAttributes, AgentCausationId, AgentCorrelationId, AgentEffect, AgentEffectId,
    AgentEffectKind, AgentEffectTarget, AgentIdempotencyKey, AgentTelemetryContext,
    AgentTimestampMillis, ArtifactRef, RedactionStatus,
};

/// Counter for model adapter invocations.
pub const METRIC_AGENT_MODEL_ADAPTER_CALLS: &str = "rakka.agent_workflow.model.calls";

/// Histogram for model adapter latency in milliseconds.
pub const METRIC_AGENT_MODEL_ADAPTER_LATENCY_MS: &str = "rakka.agent_workflow.model.latency_ms";

/// Histogram for model adapter token counts.
pub const METRIC_AGENT_MODEL_ADAPTER_TOKENS: &str = "rakka.agent_workflow.model.tokens";

/// Counter for tool adapter invocations.
pub const METRIC_AGENT_TOOL_ADAPTER_CALLS: &str = "rakka.agent_workflow.tool.calls";

/// Histogram for tool adapter latency in milliseconds.
pub const METRIC_AGENT_TOOL_ADAPTER_LATENCY_MS: &str = "rakka.agent_workflow.tool.latency_ms";

/// Shared result type for adapter operations.
pub type AgentAdapterResult<T> = Result<T, AgentAdapterError>;

/// Boxed future returned by model and tool adapters.
pub type AgentAdapterFuture<'a> =
    Pin<Box<dyn Future<Output = AgentAdapterResult<AgentAdapterOutcome>> + Send + 'a>>;

/// Boxed future returned by A2A peer adapters.
pub type AgentA2APeerAdapterFuture<'a> =
    Pin<Box<dyn Future<Output = AgentAdapterResult<AgentA2APeerOutcome>> + Send + 'a>>;

/// Adapter-level failures that occur before a provider/tool outcome exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentAdapterError {
    /// Effect kind was not compatible with the adapter request.
    InvalidEffectKind {
        /// Effect id.
        effect_id: AgentEffectId,
        /// Expected effect kind label.
        expected: &'static str,
        /// Actual effect kind.
        actual: AgentEffectKind,
    },
    /// Required request field was missing or invalid.
    InvalidRequest {
        /// Effect id.
        effect_id: AgentEffectId,
        /// Invalid field.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
    /// Adapter implementation was unavailable.
    Unavailable {
        /// Stable adapter/provider name.
        adapter: String,
        /// Stable reason.
        reason: String,
    },
    /// Adapter could not serialize a provider/tool request.
    Serialization {
        /// Serialization failure detail.
        message: String,
    },
    /// Adapter could not decode a provider/tool response.
    Deserialization {
        /// Deserialization failure detail.
        message: String,
    },
    /// Process-backed tool adapter failed before a tool outcome was available.
    #[cfg(feature = "process-tools")]
    Process {
        /// Process failure.
        error: rakka_process::ProcessError,
    },
}

impl AgentAdapterError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidEffectKind { .. } => "invalid-effect-kind",
            Self::InvalidRequest { .. } => "invalid-adapter-request",
            Self::Unavailable { .. } => "adapter-unavailable",
            Self::Serialization { .. } => "adapter-serialization",
            Self::Deserialization { .. } => "adapter-deserialization",
            #[cfg(feature = "process-tools")]
            Self::Process { error } => error.code(),
        }
    }
}

impl Display for AgentAdapterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEffectKind {
                effect_id,
                expected,
                actual,
            } => write!(
                f,
                "agent effect {effect_id} has invalid kind: expected {expected}, actual {}",
                actual.as_label()
            ),
            Self::InvalidRequest {
                effect_id,
                field,
                reason,
            } => write!(
                f,
                "agent adapter request for effect {effect_id} has invalid {field}: {reason}"
            ),
            Self::Unavailable { adapter, reason } => {
                write!(f, "agent adapter {adapter} is unavailable: {reason}")
            }
            Self::Serialization { message } => {
                write!(f, "agent adapter request serialization failed: {message}")
            }
            Self::Deserialization { message } => {
                write!(
                    f,
                    "agent adapter response deserialization failed: {message}"
                )
            }
            #[cfg(feature = "process-tools")]
            Self::Process { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(feature = "process-tools")]
            Self::Process { error } => Some(error),
            _ => None,
        }
    }
}

#[cfg(feature = "process-tools")]
impl From<rakka_process::ProcessError> for AgentAdapterError {
    fn from(error: rakka_process::ProcessError) -> Self {
        Self::Process { error }
    }
}

/// Retry classification for a failed model/tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAdapterFailureClass {
    /// The failure may succeed if retried later.
    Retryable,
    /// The failure should not be retried by adapter policy.
    Permanent,
}

impl AgentAdapterFailureClass {
    /// Stable lowercase label for metrics, logs, and snapshots.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
        }
    }
}

/// Token, cost, and bounded usage metadata returned by an adapter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAdapterUsage {
    /// Input or prompt tokens.
    pub input_tokens: Option<u64>,
    /// Output or completion tokens.
    pub output_tokens: Option<u64>,
    /// Total tokens, when supplied directly by the provider.
    pub total_tokens: Option<u64>,
    /// Cost in provider/application-defined micro-units.
    pub cost_microunits: Option<u64>,
    /// Bounded usage attributes.
    pub attributes: AgentAttributes,
}

impl AgentAdapterUsage {
    /// Creates empty usage metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets input token count.
    #[must_use]
    pub const fn input_tokens(mut self, input_tokens: u64) -> Self {
        self.input_tokens = Some(input_tokens);
        self
    }

    /// Sets output token count.
    #[must_use]
    pub const fn output_tokens(mut self, output_tokens: u64) -> Self {
        self.output_tokens = Some(output_tokens);
        self
    }

    /// Sets total token count.
    #[must_use]
    pub const fn total_tokens(mut self, total_tokens: u64) -> Self {
        self.total_tokens = Some(total_tokens);
        self
    }

    /// Sets cost in provider/application-defined micro-units.
    #[must_use]
    pub const fn cost_microunits(mut self, cost_microunits: u64) -> Self {
        self.cost_microunits = Some(cost_microunits);
        self
    }

    /// Adds a bounded usage attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Durable receipt returned by a model/tool adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAdapterReceipt {
    /// Stable receipt id supplied by the adapter or derived from the effect.
    pub receipt_id: String,
    /// Provider, process, or adapter name.
    pub provider: String,
    /// Target name selected by the effect.
    pub target_name: String,
    /// External request id, when the downstream system provides one.
    pub external_request_id: Option<String>,
    /// Idempotency key used for the downstream call.
    pub idempotency_key: AgentIdempotencyKey,
    /// Timestamp when the adapter produced this receipt.
    pub received_at: AgentTimestampMillis,
    /// Observed latency in milliseconds.
    pub latency_ms: Option<u64>,
    /// Redaction status for receipt metadata and associated result artifacts.
    pub redaction: RedactionStatus,
    /// Bounded receipt attributes.
    pub attributes: AgentAttributes,
}

impl AgentAdapterReceipt {
    /// Creates a receipt.
    #[must_use]
    pub fn new(
        receipt_id: impl Into<String>,
        provider: impl Into<String>,
        target_name: impl Into<String>,
        idempotency_key: AgentIdempotencyKey,
        received_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            receipt_id: receipt_id.into(),
            provider: provider.into(),
            target_name: target_name.into(),
            external_request_id: None,
            idempotency_key,
            received_at,
            latency_ms: None,
            redaction: RedactionStatus::Unknown,
            attributes: AgentAttributes::new(),
        }
    }

    /// Sets an external provider/tool request id.
    #[must_use]
    pub fn external_request_id(mut self, external_request_id: impl Into<String>) -> Self {
        self.external_request_id = Some(external_request_id.into());
        self
    }

    /// Sets observed latency in milliseconds.
    #[must_use]
    pub const fn latency_ms(mut self, latency_ms: u64) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }

    /// Sets receipt redaction status.
    #[must_use]
    pub const fn redaction(mut self, redaction: RedactionStatus) -> Self {
        self.redaction = redaction;
        self
    }

    /// Adds a bounded receipt attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Common metadata derived from a durable effect before adapter invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAdapterRequestMetadata {
    /// Effect id being invoked.
    pub effect_id: AgentEffectId,
    /// Effect kind being invoked.
    pub effect_kind: AgentEffectKind,
    /// Target selected by the effect.
    pub target: AgentEffectTarget,
    /// Downstream idempotency key.
    pub idempotency_key: AgentIdempotencyKey,
    /// Timeout budget in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Effect attempt number.
    pub attempt: u32,
    /// Command or step that caused this call.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related work.
    pub correlation_id: AgentCorrelationId,
    /// Trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
    /// Redaction status for request payload metadata.
    pub redaction: RedactionStatus,
    /// Bounded adapter attributes.
    pub attributes: AgentAttributes,
}

impl AgentAdapterRequestMetadata {
    /// Creates metadata from a durable effect and request artifact.
    #[must_use]
    pub fn from_effect(effect: &AgentEffect, payload_ref: Option<&ArtifactRef>) -> Self {
        Self {
            effect_id: effect.effect_id.clone(),
            effect_kind: effect.kind,
            target: effect.target.clone(),
            idempotency_key: effect.idempotency_key.clone(),
            timeout_ms: effect.timeout_ms,
            attempt: effect.attempt,
            causation_id: effect.causation_id.clone(),
            correlation_id: effect.correlation_id.clone(),
            telemetry_context: effect.telemetry_context.clone(),
            redaction: payload_ref.map_or(RedactionStatus::Unknown, |artifact| artifact.redaction),
            attributes: AgentAttributes::new(),
        }
    }

    /// Builds a default receipt for deterministic adapters and tests.
    #[must_use]
    pub fn receipt(
        &self,
        provider: impl Into<String>,
        received_at: AgentTimestampMillis,
    ) -> AgentAdapterReceipt {
        AgentAdapterReceipt::new(
            format!(
                "{}:{}",
                self.effect_kind.as_label(),
                self.effect_id.as_str()
            ),
            provider,
            self.target.name.clone(),
            self.idempotency_key.clone(),
            received_at,
        )
        .redaction(self.redaction)
    }
}

/// Request sent to a model adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentModelRequest {
    /// Source durable effect.
    pub effect: AgentEffect,
    /// Common adapter metadata.
    pub metadata: AgentAdapterRequestMetadata,
    /// Prompt or model input artifact reference.
    pub prompt_ref: Option<ArtifactRef>,
    /// Optional model name or deployment id.
    pub model_name: Option<String>,
    /// Bounded provider parameters.
    pub parameters: AgentAttributes,
}

impl AgentModelRequest {
    /// Builds a model request from a durable `ModelCall` effect.
    pub fn from_effect(effect: AgentEffect) -> AgentAdapterResult<Self> {
        if effect.kind != AgentEffectKind::ModelCall {
            return Err(AgentAdapterError::InvalidEffectKind {
                effect_id: effect.effect_id,
                expected: AgentEffectKind::ModelCall.as_label(),
                actual: effect.kind,
            });
        }
        let prompt_ref = effect.payload_ref.clone();
        Ok(Self {
            metadata: AgentAdapterRequestMetadata::from_effect(&effect, prompt_ref.as_ref()),
            model_name: model_name_from_target(&effect.target),
            parameters: effect.target.attributes.clone(),
            effect,
            prompt_ref,
        })
    }
}

/// Request sent to a tool adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentToolRequest {
    /// Source durable effect.
    pub effect: AgentEffect,
    /// Common adapter metadata.
    pub metadata: AgentAdapterRequestMetadata,
    /// Tool input artifact reference.
    pub input_ref: Option<ArtifactRef>,
    /// Stable tool name.
    pub tool_name: String,
    /// Bounded tool parameters.
    pub parameters: AgentAttributes,
}

impl AgentToolRequest {
    /// Builds a tool request from a durable `ToolCall` or `ProcessCall` effect.
    pub fn from_effect(effect: AgentEffect) -> AgentAdapterResult<Self> {
        if !matches!(
            effect.kind,
            AgentEffectKind::ToolCall | AgentEffectKind::ProcessCall
        ) {
            return Err(AgentAdapterError::InvalidEffectKind {
                effect_id: effect.effect_id,
                expected: "tool-call-or-process-call",
                actual: effect.kind,
            });
        }
        let input_ref = effect.payload_ref.clone();
        Ok(Self {
            metadata: AgentAdapterRequestMetadata::from_effect(&effect, input_ref.as_ref()),
            tool_name: effect.target.name.clone(),
            parameters: effect.target.attributes.clone(),
            effect,
            input_ref,
        })
    }
}

/// Request sent to an A2A peer-call adapter.
///
/// The core crate deliberately keeps this shape independent from the A2A SDK
/// wire types. An adapter in an A2A-facing crate can resolve the peer card,
/// choose REST or JSON-RPC with `a2a-client`, and translate this durable effect
/// into the SDK's `SendMessageRequest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentA2APeerRequest {
    /// Source durable effect.
    pub effect: AgentEffect,
    /// Common adapter metadata.
    pub metadata: AgentAdapterRequestMetadata,
    /// Out-of-line A2A message/request artifact.
    pub request_ref: Option<ArtifactRef>,
    /// Stable peer name selected by the effect target.
    pub peer_name: String,
    /// Optional peer agent-card artifact or logical card reference.
    pub peer_card_ref: Option<ArtifactRef>,
    /// Optional parent context id to pass to the peer.
    pub context_id: Option<String>,
    /// Optional peer task id for retries or continuations.
    pub peer_task_id: Option<String>,
    /// Optional preferred transport binding such as `jsonrpc` or `rest`.
    pub preferred_transport: Option<String>,
    /// Bounded target parameters.
    pub parameters: AgentAttributes,
}

impl AgentA2APeerRequest {
    /// Builds a peer request from a durable A2A-peer effect.
    pub fn from_effect(effect: AgentEffect) -> AgentAdapterResult<Self> {
        if !is_a2a_peer_effect(&effect) {
            return Err(AgentAdapterError::InvalidEffectKind {
                effect_id: effect.effect_id,
                expected: "a2a-peer",
                actual: effect.kind,
            });
        }
        let request_ref = effect.payload_ref.clone();
        let peer_card_ref = peer_card_ref_from_attributes(&effect.target.attributes);
        Ok(Self {
            metadata: AgentAdapterRequestMetadata::from_effect(&effect, request_ref.as_ref()),
            peer_name: effect.target.name.clone(),
            context_id: effect.target.attributes.get("context_id").cloned(),
            peer_task_id: effect.target.attributes.get("peer_task_id").cloned(),
            preferred_transport: effect.target.attributes.get("preferred_transport").cloned(),
            parameters: effect.target.attributes.clone(),
            effect,
            request_ref,
            peer_card_ref,
        })
    }
}

/// Result returned by an A2A peer-call adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AgentA2APeerOutcome {
    /// Peer call completed successfully.
    Completed {
        /// Durable receipt.
        receipt: AgentAdapterReceipt,
        /// Peer task id returned by the remote agent.
        peer_task_id: String,
        /// Peer context id returned by the remote agent.
        context_id: Option<String>,
        /// Result artifact reference.
        result_ref: Option<ArtifactRef>,
    },
    /// Peer call failed before completion.
    Failed {
        /// Durable receipt.
        receipt: AgentAdapterReceipt,
        /// Retry classification.
        classification: AgentAdapterFailureClass,
        /// Stable bounded error code.
        error_code: String,
        /// Optional retry-after timestamp.
        retry_after: Option<AgentTimestampMillis>,
        /// Optional artifact containing bounded error details.
        error_ref: Option<ArtifactRef>,
    },
    /// Peer call timed out.
    TimedOut {
        /// Durable receipt.
        receipt: AgentAdapterReceipt,
        /// Timeout budget that elapsed.
        timeout_ms: u64,
        /// Peer task id when the request may have been accepted remotely.
        peer_task_id: Option<String>,
    },
}

impl AgentA2APeerOutcome {
    /// Creates a successful peer outcome.
    #[must_use]
    pub fn completed(
        receipt: AgentAdapterReceipt,
        peer_task_id: impl Into<String>,
        context_id: Option<String>,
        result_ref: Option<ArtifactRef>,
    ) -> Self {
        Self::Completed {
            receipt,
            peer_task_id: peer_task_id.into(),
            context_id,
            result_ref,
        }
    }

    /// Creates a failed peer outcome.
    #[must_use]
    pub fn failed(
        receipt: AgentAdapterReceipt,
        classification: AgentAdapterFailureClass,
        error_code: impl Into<String>,
    ) -> Self {
        Self::Failed {
            receipt,
            classification,
            error_code: error_code.into(),
            retry_after: None,
            error_ref: None,
        }
    }

    /// Creates a timed-out peer outcome.
    #[must_use]
    pub fn timed_out(
        receipt: AgentAdapterReceipt,
        timeout_ms: u64,
        peer_task_id: Option<String>,
    ) -> Self {
        Self::TimedOut {
            receipt,
            timeout_ms,
            peer_task_id,
        }
    }

    /// Maps the peer outcome into the durable outbox dispatch result.
    #[must_use]
    pub fn to_outbox_dispatch_result(&self) -> OutboxDispatchResult {
        match self {
            Self::Completed { .. } => OutboxDispatchResult::Success,
            Self::Failed {
                classification,
                error_code,
                ..
            } => {
                let prefix = match classification {
                    AgentAdapterFailureClass::Retryable => "retryable",
                    AgentAdapterFailureClass::Permanent => "permanent",
                };
                OutboxDispatchResult::failure(format!("{prefix}:a2a-peer:{error_code}"))
            }
            Self::TimedOut { timeout_ms, .. } => {
                OutboxDispatchResult::timeout(format!("a2a-peer-timeout:{timeout_ms}"))
            }
        }
    }
}

/// Result returned by model and tool adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AgentAdapterOutcome {
    /// Adapter completed successfully.
    Completed {
        /// Durable receipt.
        receipt: AgentAdapterReceipt,
        /// Result artifact reference.
        result_ref: Option<ArtifactRef>,
        /// Token, cost, and bounded usage metadata.
        usage: AgentAdapterUsage,
    },
    /// Adapter failed before completion.
    Failed {
        /// Durable receipt.
        receipt: AgentAdapterReceipt,
        /// Retry classification.
        classification: AgentAdapterFailureClass,
        /// Stable bounded error code.
        error_code: String,
        /// Optional retry-after timestamp.
        retry_after: Option<AgentTimestampMillis>,
        /// Optional artifact containing bounded error details.
        error_ref: Option<ArtifactRef>,
    },
    /// Adapter timed out.
    TimedOut {
        /// Durable receipt.
        receipt: AgentAdapterReceipt,
        /// Timeout budget that elapsed.
        timeout_ms: u64,
        /// Partial result artifact reference, when available.
        partial_result_ref: Option<ArtifactRef>,
    },
}

impl AgentAdapterOutcome {
    /// Creates a successful outcome.
    #[must_use]
    pub fn completed(
        receipt: AgentAdapterReceipt,
        result_ref: Option<ArtifactRef>,
        usage: AgentAdapterUsage,
    ) -> Self {
        Self::Completed {
            receipt,
            result_ref,
            usage,
        }
    }

    /// Creates a failed outcome.
    #[must_use]
    pub fn failed(
        receipt: AgentAdapterReceipt,
        classification: AgentAdapterFailureClass,
        error_code: impl Into<String>,
    ) -> Self {
        Self::Failed {
            receipt,
            classification,
            error_code: error_code.into(),
            retry_after: None,
            error_ref: None,
        }
    }

    /// Creates a timed-out outcome.
    #[must_use]
    pub fn timed_out(
        receipt: AgentAdapterReceipt,
        timeout_ms: u64,
        partial_result_ref: Option<ArtifactRef>,
    ) -> Self {
        Self::TimedOut {
            receipt,
            timeout_ms,
            partial_result_ref,
        }
    }

    /// Returns true when the adapter completed successfully.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// Maps the adapter outcome into the lower-level durable outbox dispatch
    /// result used by the current dispatcher substrate.
    #[must_use]
    pub fn to_outbox_dispatch_result(&self) -> OutboxDispatchResult {
        match self {
            Self::Completed { .. } => OutboxDispatchResult::Success,
            Self::Failed {
                classification,
                error_code,
                ..
            } => {
                let prefix = match classification {
                    AgentAdapterFailureClass::Retryable => "retryable",
                    AgentAdapterFailureClass::Permanent => "permanent",
                };
                OutboxDispatchResult::failure(format!("{prefix}:{error_code}"))
            }
            Self::TimedOut { timeout_ms, .. } => {
                OutboxDispatchResult::timeout(format!("adapter-timeout:{timeout_ms}"))
            }
        }
    }
}

/// Trait implemented by model provider adapters.
pub trait AgentModelAdapter: Send {
    /// Invokes a model call.
    fn invoke_model<'a>(&'a mut self, request: AgentModelRequest) -> AgentAdapterFuture<'a>;
}

/// Trait implemented by tool adapters.
pub trait AgentToolAdapter: Send {
    /// Invokes a tool call.
    fn invoke_tool<'a>(&'a mut self, request: AgentToolRequest) -> AgentAdapterFuture<'a>;
}

/// Trait implemented by A2A peer-call adapters.
pub trait AgentA2APeerAdapter: Send {
    /// Invokes an A2A peer task call.
    fn invoke_peer<'a>(&'a mut self, request: AgentA2APeerRequest)
        -> AgentA2APeerAdapterFuture<'a>;
}

/// Feature-gated example of a process-backed tool adapter using
/// `rakka-process` file-watch mode.
#[cfg(feature = "process-tools")]
#[derive(Debug, Clone)]
pub struct ProcessFileWatchToolAdapter {
    provider: String,
    spec: rakka_process::ProcessSpec,
    allowlist: rakka_process::ExecutableAllowlist,
    config: rakka_process::FileWatchConfig,
    result_ref: Option<ArtifactRef>,
    redaction: RedactionStatus,
}

#[cfg(feature = "process-tools")]
impl ProcessFileWatchToolAdapter {
    /// Creates a process-backed tool adapter.
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        spec: rakka_process::ProcessSpec,
        allowlist: rakka_process::ExecutableAllowlist,
        config: rakka_process::FileWatchConfig,
    ) -> Self {
        Self {
            provider: provider.into(),
            spec,
            allowlist,
            config,
            result_ref: None,
            redaction: RedactionStatus::Unknown,
        }
    }

    /// Sets the result artifact reference returned on successful completion.
    #[must_use]
    pub fn result_ref(mut self, result_ref: ArtifactRef) -> Self {
        self.result_ref = Some(result_ref);
        self
    }

    /// Sets redaction status for generated receipts.
    #[must_use]
    pub const fn redaction(mut self, redaction: RedactionStatus) -> Self {
        self.redaction = redaction;
        self
    }
}

#[cfg(feature = "process-tools")]
impl AgentToolAdapter for ProcessFileWatchToolAdapter {
    fn invoke_tool<'a>(&'a mut self, request: AgentToolRequest) -> AgentAdapterFuture<'a> {
        Box::pin(async move {
            let started = Instant::now();
            let config = request.metadata.timeout_ms.map_or_else(
                || self.config.clone(),
                |timeout_ms| {
                    self.config
                        .clone()
                        .timeout(Duration::from_millis(timeout_ms))
                },
            );
            let outcome =
                rakka_process::run_file_watch(self.spec.clone(), &self.allowlist, config).await;
            let latency_ms = elapsed_millis(started);
            let mut receipt = request
                .metadata
                .receipt(self.provider.clone(), system_timestamp())
                .latency_ms(latency_ms)
                .redaction(self.redaction);
            match outcome {
                Ok(rakka_process::FileWatchOutcome::Completed(completed)) => {
                    receipt = receipt
                        .attribute("process_outcome", "completed")
                        .attribute("output_count", completed.outputs.len().to_string());
                    Ok(AgentAdapterOutcome::completed(
                        receipt,
                        self.result_ref.clone(),
                        AgentAdapterUsage::new(),
                    ))
                }
                Ok(rakka_process::FileWatchOutcome::TimedOut(timed_out)) => {
                    receipt = receipt
                        .attribute("process_outcome", "timed-out")
                        .attribute("output_count", timed_out.outputs.len().to_string());
                    Ok(AgentAdapterOutcome::timed_out(
                        receipt,
                        timed_out.timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                        self.result_ref.clone(),
                    ))
                }
                Ok(rakka_process::FileWatchOutcome::ProcessExited(exited)) => {
                    receipt = receipt
                        .attribute("process_outcome", "process-exited")
                        .attribute("output_count", exited.outputs.len().to_string());
                    Ok(AgentAdapterOutcome::failed(
                        receipt,
                        AgentAdapterFailureClass::Permanent,
                        "process-exited-before-completion",
                    ))
                }
                Err(error) => Err(AgentAdapterError::Process { error }),
            }
        })
    }
}

fn model_name_from_target(target: &AgentEffectTarget) -> Option<String> {
    Some(
        target
            .attributes
            .get("model")
            .or_else(|| target.attributes.get("deployment"))
            .cloned()
            .unwrap_or_else(|| target.name.clone()),
    )
}

fn is_a2a_peer_effect(effect: &AgentEffect) -> bool {
    matches!(
        effect.kind,
        AgentEffectKind::HttpCall | AgentEffectKind::GrpcCall
    ) && (effect.target.target_type == "a2a-peer"
        || effect
            .target
            .attributes
            .get("target_class")
            .is_some_and(|value| value == "a2a-peer"))
}

fn peer_card_ref_from_attributes(attributes: &AgentAttributes) -> Option<ArtifactRef> {
    attributes.get("peer_card_ref").map(|value| ArtifactRef {
        artifact_id: value.clone(),
        kind: crate::ArtifactKind::Other,
        uri: value.clone(),
        checksum: None,
        content_type: Some("application/vnd.a2a.agent-card+json".to_string()),
        byte_len: None,
        retention_class: None,
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(0),
        metadata: AgentAttributes::new(),
    })
}

#[cfg(feature = "process-tools")]
fn system_timestamp() -> AgentTimestampMillis {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    AgentTimestampMillis::new(millis)
}

#[cfg(feature = "process-tools")]
fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}
