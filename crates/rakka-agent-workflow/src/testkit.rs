//! Deterministic test harness helpers for agent workflow slices.

use std::collections::{BTreeMap, VecDeque};

use rakka_core::{InMemoryMetricsRecorder, MetricKind, MetricObservation, MetricsSnapshot};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    DurableInbox, InboxAcceptance, InboxCommand, ManualWorkflowClock, OutboxAcceptance,
    OutboxCommand, OutboxTarget, WorkflowClock, WorkflowId, WorkflowResult, WorkflowState,
    WorkflowTimestamp,
};

use crate::{
    agent_metric_instrument, require_agent_trace_context, validate_agent_audit_event,
    validate_agent_log_event, validate_agent_metric_attributes, validate_agent_span_link,
    AgentAdapterFailureClass, AgentAdapterFuture, AgentAdapterOutcome, AgentAdapterRequestMetadata,
    AgentAdapterUsage, AgentArtifactError, AgentArtifactRead, AgentArtifactStore,
    AgentArtifactStoreFuture, AgentArtifactWriteRequest, AgentAuditEvent, AgentAuditEventId,
    AgentAuditEventKind, AgentCausationId, AgentCommandId, AgentCorrelationId,
    AgentDeduplicationKey, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectStatus,
    AgentEffectTarget, AgentIdempotencyKey, AgentLogEvent, AgentLogSeverity, AgentMetricInstrument,
    AgentModelAdapter, AgentModelRequest, AgentOtelResource, AgentOtelSpanExport,
    AgentOtlpBridgeExport, AgentPayloadDescriptor, AgentRedactionPolicy, AgentRunId, AgentRunState,
    AgentRunStatus, AgentSpanLink, AgentStatePayload, AgentStep, AgentStepId, AgentStepKind,
    AgentTelemetryContext, AgentTenantId, AgentTimestampMillis, AgentToolAdapter, AgentToolRequest,
    AgentWorkflow, AgentWorkflowId, ArtifactKind, ArtifactRef, HumanCheckpoint, HumanCheckpointId,
    HumanCheckpointStatus, PrincipalRef, RedactionStatus, StateSchemaVersion,
    WorkflowDefinitionVersion,
};

/// Durable inbox type used by [`MinimalAgentFixture`].
pub type FixtureDurableInbox =
    DurableInbox<InMemoryDurableStateStore<WorkflowState>, ManualWorkflowClock>;

/// Deterministic id generator for agent workflow tests.
#[derive(Debug, Clone)]
pub struct DeterministicAgentIds {
    workflow: u64,
    run: u64,
    step: u64,
    effect: u64,
    checkpoint: u64,
    command: u64,
    causation: u64,
    correlation: u64,
    audit: u64,
    artifact: u64,
}

impl DeterministicAgentIds {
    /// Creates a fresh deterministic id generator.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            workflow: 0,
            run: 0,
            step: 0,
            effect: 0,
            checkpoint: 0,
            command: 0,
            causation: 0,
            correlation: 0,
            audit: 0,
            artifact: 0,
        }
    }

    /// Returns the next workflow id.
    #[must_use]
    pub fn next_workflow_id(&mut self) -> AgentWorkflowId {
        self.workflow += 1;
        AgentWorkflowId::new(format!("workflow-{}", self.workflow))
    }

    /// Returns the next run id.
    #[must_use]
    pub fn next_run_id(&mut self) -> AgentRunId {
        self.run += 1;
        AgentRunId::new(format!("run-{}", self.run))
    }

    /// Returns the next step id.
    #[must_use]
    pub fn next_step_id(&mut self) -> AgentStepId {
        self.step += 1;
        AgentStepId::new(format!("step-{}", self.step))
    }

    /// Returns the next effect id.
    #[must_use]
    pub fn next_effect_id(&mut self) -> AgentEffectId {
        self.effect += 1;
        AgentEffectId::new(format!("effect-{}", self.effect))
    }

    /// Returns the next checkpoint id.
    #[must_use]
    pub fn next_checkpoint_id(&mut self) -> HumanCheckpointId {
        self.checkpoint += 1;
        HumanCheckpointId::new(format!("checkpoint-{}", self.checkpoint))
    }

    /// Returns the next command id.
    #[must_use]
    pub fn next_command_id(&mut self) -> AgentCommandId {
        self.command += 1;
        AgentCommandId::new(format!("command-{}", self.command))
    }

    /// Returns the next causation id.
    #[must_use]
    pub fn next_causation_id(&mut self) -> AgentCausationId {
        self.causation += 1;
        AgentCausationId::new(format!("cause-{}", self.causation))
    }

    /// Returns the next correlation id.
    #[must_use]
    pub fn next_correlation_id(&mut self) -> AgentCorrelationId {
        self.correlation += 1;
        AgentCorrelationId::new(format!("corr-{}", self.correlation))
    }

    /// Returns the next audit event id.
    #[must_use]
    pub fn next_audit_event_id(&mut self) -> AgentAuditEventId {
        self.audit += 1;
        AgentAuditEventId::new(format!("audit-{}", self.audit))
    }

    /// Returns the next artifact id.
    #[must_use]
    pub fn next_artifact_id(&mut self) -> String {
        self.artifact += 1;
        format!("artifact-{}", self.artifact)
    }
}

impl Default for DeterministicAgentIds {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic clock for agent workflow tests.
#[derive(Debug, Clone)]
pub struct FakeAgentClock {
    clock: ManualWorkflowClock,
}

impl FakeAgentClock {
    /// Creates a fake clock at the supplied millisecond timestamp.
    #[must_use]
    pub fn new(now_millis: u64) -> Self {
        Self {
            clock: ManualWorkflowClock::new(WorkflowTimestamp::from_millis(now_millis)),
        }
    }

    /// Returns the current agent timestamp.
    #[must_use]
    pub fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.now().as_millis())
    }

    /// Returns the current workflow substrate timestamp.
    #[must_use]
    pub fn workflow_now(&self) -> WorkflowTimestamp {
        self.clock.now()
    }

    /// Returns a clone of the substrate clock.
    #[must_use]
    pub fn workflow_clock(&self) -> ManualWorkflowClock {
        self.clock.clone()
    }

    /// Sets the current timestamp.
    pub fn set_millis(&self, millis: u64) {
        self.clock.set(WorkflowTimestamp::from_millis(millis));
    }

    /// Advances the current timestamp.
    pub fn advance_millis(&self, millis: u64) {
        self.clock.advance_millis(millis);
    }
}

impl Default for FakeAgentClock {
    fn default() -> Self {
        Self::new(0)
    }
}

/// In-memory artifact record used by [`FakeArtifactStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeArtifactRecord {
    /// Artifact reference.
    pub reference: ArtifactRef,
    /// Stored bytes.
    pub bytes: Vec<u8>,
}

/// In-memory artifact store for deterministic tests.
#[derive(Debug, Clone, Default)]
pub struct FakeArtifactStore {
    artifacts: BTreeMap<String, FakeArtifactRecord>,
}

impl FakeArtifactStore {
    /// Stores an artifact reference and bytes.
    pub fn insert(&mut self, reference: ArtifactRef, bytes: impl Into<Vec<u8>>) -> ArtifactRef {
        let record = FakeArtifactRecord {
            reference: reference.clone(),
            bytes: bytes.into(),
        };
        self.artifacts.insert(reference.artifact_id.clone(), record);
        reference
    }

    /// Returns stored artifact bytes by artifact id.
    #[must_use]
    pub fn bytes(&self, artifact_id: &str) -> Option<&[u8]> {
        self.artifacts
            .get(artifact_id)
            .map(|record| record.bytes.as_slice())
    }

    /// Returns an artifact reference by artifact id.
    #[must_use]
    pub fn reference(&self, artifact_id: &str) -> Option<&ArtifactRef> {
        self.artifacts
            .get(artifact_id)
            .map(|record| &record.reference)
    }

    /// Returns true when the artifact id exists.
    #[must_use]
    pub fn contains(&self, artifact_id: &str) -> bool {
        self.artifacts.contains_key(artifact_id)
    }

    /// Returns the number of artifacts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.artifacts.len()
    }

    /// Returns true when no artifacts are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.artifacts.is_empty()
    }
}

impl AgentArtifactStore for FakeArtifactStore {
    fn put_artifact<'a>(
        &'a mut self,
        request: AgentArtifactWriteRequest,
    ) -> AgentArtifactStoreFuture<'a, ArtifactRef> {
        let artifact_id = request
            .artifact_id
            .unwrap_or_else(|| format!("artifact-{}", self.artifacts.len() + 1));
        let byte_len = request.bytes.len() as u64;
        let reference = ArtifactRef {
            artifact_id: artifact_id.clone(),
            kind: request.kind,
            uri: format!("memory://agent-fixture/{artifact_id}"),
            checksum: request.checksum.or_else(|| Some(format!("len:{byte_len}"))),
            content_type: request.content_type,
            byte_len: Some(byte_len),
            retention_class: request.retention_class,
            encryption: request.encryption,
            redaction: request.redaction,
            created_at: request.created_at,
            metadata: request.metadata,
        };
        self.insert(reference.clone(), request.bytes);
        Box::pin(async move { Ok(reference) })
    }

    fn get_artifact<'a>(
        &'a self,
        reference: &'a ArtifactRef,
    ) -> AgentArtifactStoreFuture<'a, AgentArtifactRead> {
        let artifact_id = reference.artifact_id.clone();
        let record = self.artifacts.get(&artifact_id).cloned();
        Box::pin(async move {
            record
                .map(|record| AgentArtifactRead {
                    reference: record.reference,
                    bytes: record.bytes,
                })
                .ok_or(AgentArtifactError::ArtifactNotFound { artifact_id })
        })
    }
}

/// Deterministic fake adapter request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeAdapterRequest {
    /// Effect being invoked.
    pub effect: AgentEffect,
    /// Request payload reference.
    pub payload_ref: Option<ArtifactRef>,
}

/// Deterministic fake adapter outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeAdapterOutcome {
    /// Adapter succeeded.
    Success {
        /// Optional result artifact.
        result_ref: Option<ArtifactRef>,
    },
    /// Adapter failed and may be retried by policy.
    RetryableFailure {
        /// Stable error code.
        error_code: String,
    },
    /// Adapter failed permanently.
    PermanentFailure {
        /// Stable error code.
        error_code: String,
    },
    /// Adapter timed out.
    Timeout {
        /// Timeout budget that elapsed.
        timeout_ms: u64,
        /// Optional partial result artifact.
        partial_result_ref: Option<ArtifactRef>,
    },
}

impl FakeAdapterOutcome {
    /// Creates a successful outcome.
    #[must_use]
    pub fn success(result_ref: Option<ArtifactRef>) -> Self {
        Self::Success { result_ref }
    }

    /// Creates a retryable failure outcome.
    #[must_use]
    pub fn retryable_failure(error_code: impl Into<String>) -> Self {
        Self::RetryableFailure {
            error_code: error_code.into(),
        }
    }

    /// Creates a permanent failure outcome.
    #[must_use]
    pub fn permanent_failure(error_code: impl Into<String>) -> Self {
        Self::PermanentFailure {
            error_code: error_code.into(),
        }
    }

    /// Creates a timeout outcome.
    #[must_use]
    pub const fn timeout(timeout_ms: u64, partial_result_ref: Option<ArtifactRef>) -> Self {
        Self::Timeout {
            timeout_ms,
            partial_result_ref,
        }
    }

    fn into_adapter_outcome(
        self,
        metadata: &AgentAdapterRequestMetadata,
        provider: &'static str,
    ) -> AgentAdapterOutcome {
        let receipt = metadata.receipt(provider, AgentTimestampMillis::new(0));
        match self {
            Self::Success { result_ref } => {
                AgentAdapterOutcome::completed(receipt, result_ref, AgentAdapterUsage::new())
            }
            Self::RetryableFailure { error_code } => AgentAdapterOutcome::failed(
                receipt,
                AgentAdapterFailureClass::Retryable,
                error_code,
            ),
            Self::PermanentFailure { error_code } => AgentAdapterOutcome::failed(
                receipt,
                AgentAdapterFailureClass::Permanent,
                error_code,
            ),
            Self::Timeout {
                timeout_ms,
                partial_result_ref,
            } => AgentAdapterOutcome::timed_out(receipt, timeout_ms, partial_result_ref),
        }
    }
}

/// Fake model adapter with queued deterministic outcomes.
#[derive(Debug, Clone, Default)]
pub struct FakeModelAdapter {
    requests: Vec<FakeAdapterRequest>,
    outcomes: VecDeque<FakeAdapterOutcome>,
}

impl FakeModelAdapter {
    /// Queues an outcome.
    pub fn push_outcome(&mut self, outcome: FakeAdapterOutcome) {
        self.outcomes.push_back(outcome);
    }

    /// Invokes the adapter.
    pub fn invoke(&mut self, request: FakeAdapterRequest) -> FakeAdapterOutcome {
        self.requests.push(request);
        self.outcomes
            .pop_front()
            .unwrap_or(FakeAdapterOutcome::Success { result_ref: None })
    }

    /// Returns recorded requests.
    #[must_use]
    pub fn requests(&self) -> &[FakeAdapterRequest] {
        &self.requests
    }
}

impl AgentModelAdapter for FakeModelAdapter {
    fn invoke_model<'a>(&'a mut self, request: AgentModelRequest) -> AgentAdapterFuture<'a> {
        let metadata = request.metadata.clone();
        let outcome = self.invoke(FakeAdapterRequest {
            effect: request.effect,
            payload_ref: request.prompt_ref,
        });
        Box::pin(async move { Ok(outcome.into_adapter_outcome(&metadata, "fake-model")) })
    }
}

/// Fake tool adapter with queued deterministic outcomes.
#[derive(Debug, Clone, Default)]
pub struct FakeToolAdapter {
    requests: Vec<FakeAdapterRequest>,
    outcomes: VecDeque<FakeAdapterOutcome>,
}

impl FakeToolAdapter {
    /// Queues an outcome.
    pub fn push_outcome(&mut self, outcome: FakeAdapterOutcome) {
        self.outcomes.push_back(outcome);
    }

    /// Invokes the adapter.
    pub fn invoke(&mut self, request: FakeAdapterRequest) -> FakeAdapterOutcome {
        self.requests.push(request);
        self.outcomes
            .pop_front()
            .unwrap_or(FakeAdapterOutcome::Success { result_ref: None })
    }

    /// Returns recorded requests.
    #[must_use]
    pub fn requests(&self) -> &[FakeAdapterRequest] {
        &self.requests
    }
}

impl AgentToolAdapter for FakeToolAdapter {
    fn invoke_tool<'a>(&'a mut self, request: AgentToolRequest) -> AgentAdapterFuture<'a> {
        let metadata = request.metadata.clone();
        let outcome = self.invoke(FakeAdapterRequest {
            effect: request.effect,
            payload_ref: request.input_ref,
        });
        Box::pin(async move { Ok(outcome.into_adapter_outcome(&metadata, "fake-tool")) })
    }
}

/// Asserts that an agent metric is registered with the expected instrument kind.
///
/// Returns the registered instrument so tests can continue with additional
/// assertions without repeating the lookup.
pub fn assert_agent_metric_registered(
    name: &str,
    expected_kind: MetricKind,
) -> &'static AgentMetricInstrument {
    let instrument = agent_metric_instrument(name)
        .unwrap_or_else(|| panic!("expected registered agent workflow metric {name:?}"));
    assert_eq!(
        instrument.kind, expected_kind,
        "metric {name:?} was registered with an unexpected kind"
    );
    instrument
}

/// Asserts that every metric attribute is approved for hot-path agent metrics.
pub fn assert_agent_metric_attributes_bounded(observation: &MetricObservation) {
    let attributes = observation
        .attributes()
        .iter()
        .map(|attribute| (attribute.key(), attribute.value()))
        .collect::<Vec<_>>();
    validate_agent_metric_attributes(&attributes).unwrap_or_else(|error| {
        panic!(
            "metric {:?} contains unbounded agent workflow attributes: {error}",
            observation.name()
        )
    });
}

/// Finds a metric observation by name, kind, and expected bounded attributes.
///
/// The matched observation is also checked against the agent metric label
/// policy, so accidental high-cardinality labels fail before an exporter or
/// dashboard sees them.
#[must_use]
pub fn expect_agent_metric_observation<'a>(
    snapshot: &'a MetricsSnapshot,
    name: &str,
    kind: MetricKind,
    expected_attributes: &[(&str, &str)],
) -> &'a MetricObservation {
    assert_agent_metric_registered(name, kind);
    let observation = snapshot
        .observations_named(name)
        .into_iter()
        .find(|observation| {
            observation.kind() == kind
                && expected_attributes
                    .iter()
                    .all(|(key, value)| observation.attribute(key) == Some(*value))
        })
        .unwrap_or_else(|| {
            panic!(
                "expected agent metric {name:?} with kind {kind:?} and attributes {:?}",
                expected_attributes
            )
        });
    assert_agent_metric_attributes_bounded(observation);
    observation
}

/// Asserts span name, validity, and expected span attributes.
pub fn assert_agent_span_attributes(
    span: &AgentOtelSpanExport,
    expected_name: &str,
    expected_attributes: &[(&str, &str)],
) {
    span.validate()
        .unwrap_or_else(|error| panic!("agent span export should be valid: {error}"));
    assert_eq!(span.name, expected_name);
    assert_agent_attributes(&span.attributes, expected_attributes, "span attributes");
}

/// Finds and validates a span link with expected bounded attributes.
pub fn assert_agent_span_has_link<'a>(
    span: &'a AgentOtelSpanExport,
    expected_trace_id: &str,
    expected_span_id: &str,
    expected_attributes: &[(&str, &str)],
) -> &'a AgentSpanLink {
    let link = span
        .links
        .iter()
        .find(|link| link.trace_id == expected_trace_id && link.span_id == expected_span_id)
        .unwrap_or_else(|| {
            panic!(
                "expected span {:?} to link trace {expected_trace_id:?} span {expected_span_id:?}",
                span.name
            )
        });
    validate_agent_span_link(link)
        .unwrap_or_else(|error| panic!("agent span link should be valid: {error}"));
    assert_agent_attributes(
        &link.attributes,
        expected_attributes,
        "span link attributes",
    );
    link
}

/// Asserts OpenTelemetry-compatible log fields and expected event attributes.
pub fn assert_agent_log_fields(
    event: &AgentLogEvent,
    expected_event_name: &str,
    expected_severity: AgentLogSeverity,
    expected_trace_id: Option<&str>,
    expected_span_id: Option<&str>,
    expected_attributes: &[(&str, &str)],
) {
    validate_agent_log_event(event, AgentRedactionPolicy::new())
        .unwrap_or_else(|error| panic!("agent log event should be valid: {error}"));
    assert_eq!(event.event_name, expected_event_name);
    assert_eq!(event.severity_text, expected_severity.severity_text());
    assert_eq!(event.severity_number, expected_severity.severity_number());
    assert_eq!(event.trace_id.as_deref(), expected_trace_id);
    assert_eq!(event.span_id.as_deref(), expected_span_id);
    assert_agent_attributes(&event.attributes, expected_attributes, "log attributes");
}

/// Asserts durable audit causation, correlation, and optional trace identity.
pub fn assert_agent_audit_correlation(
    event: &AgentAuditEvent,
    expected_causation_id: &str,
    expected_correlation_id: &str,
    expected_trace_id: Option<&str>,
) {
    validate_agent_audit_event(event, AgentRedactionPolicy::new())
        .unwrap_or_else(|error| panic!("agent audit event should be valid: {error}"));
    assert_eq!(event.causation_id.as_str(), expected_causation_id);
    assert_eq!(event.correlation_id.as_str(), expected_correlation_id);
    if let Some(expected_trace_id) = expected_trace_id {
        let trace = require_agent_trace_context(&event.telemetry_context).unwrap_or_else(|error| {
            panic!("agent audit event should carry trace context: {error}")
        });
        assert_eq!(trace.trace_id, expected_trace_id);
    }
}

/// Asserts OpenTelemetry resource attributes used by agent workflow exports.
pub fn assert_agent_resource_attributes(
    resource: &AgentOtelResource,
    expected_attributes: &[(&str, &str)],
) {
    resource
        .validate()
        .unwrap_or_else(|error| panic!("agent OpenTelemetry resource should be valid: {error}"));
    assert_agent_attributes(
        &resource.attributes,
        expected_attributes,
        "resource attributes",
    );
}

/// Asserts an OTLP bridge export contains valid resource, metrics, spans, and logs.
pub fn assert_agent_otlp_bridge_export(
    export: &AgentOtlpBridgeExport,
    expected_metric_names: &[&str],
    expected_resource_attributes: &[(&str, &str)],
) {
    export
        .exporter
        .validate()
        .unwrap_or_else(|error| panic!("agent OTLP exporter config should be valid: {error}"));
    assert_agent_resource_attributes(&export.resource, expected_resource_attributes);
    for metric_name in expected_metric_names {
        assert!(
            export
                .metrics
                .metrics()
                .iter()
                .any(|metric| metric.name() == *metric_name),
            "expected OTLP bridge export to include metric {metric_name:?}"
        );
    }
    for span in &export.spans {
        span.validate()
            .unwrap_or_else(|error| panic!("agent span export should be valid: {error}"));
    }
    for event in &export.logs {
        validate_agent_log_event(event, AgentRedactionPolicy::new())
            .unwrap_or_else(|error| panic!("agent log event should be valid: {error}"));
    }
}

fn assert_agent_attributes(
    attributes: &BTreeMap<String, String>,
    expected_attributes: &[(&str, &str)],
    surface: &str,
) {
    for (key, expected_value) in expected_attributes {
        assert_eq!(
            attributes.get(*key).map(String::as_str),
            Some(*expected_value),
            "expected {surface} to include {key:?}={expected_value:?}"
        );
    }
}

/// In-memory audit sink for deterministic tests.
#[derive(Debug, Clone, Default)]
pub struct FakeAuditSink {
    events: Vec<AgentAuditEvent>,
}

impl FakeAuditSink {
    /// Records an audit event.
    pub fn record(&mut self, event: AgentAuditEvent) {
        self.events.push(event);
    }

    /// Returns recorded audit events.
    #[must_use]
    pub fn events(&self) -> &[AgentAuditEvent] {
        &self.events
    }

    /// Returns the number of recorded events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true when no events are recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Minimal deterministic fixture for early agent workflow slices.
#[derive(Debug, Clone)]
pub struct MinimalAgentFixture {
    ids: DeterministicAgentIds,
    clock: FakeAgentClock,
    store: InMemoryDurableStateStore<WorkflowState>,
    metrics: InMemoryMetricsRecorder,
    artifact_store: FakeArtifactStore,
    audit_sink: FakeAuditSink,
    model_adapter: FakeModelAdapter,
    tool_adapter: FakeToolAdapter,
}

impl MinimalAgentFixture {
    /// Creates an empty deterministic fixture.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ids: DeterministicAgentIds::new(),
            clock: FakeAgentClock::default(),
            store: InMemoryDurableStateStore::new(),
            metrics: InMemoryMetricsRecorder::new(),
            artifact_store: FakeArtifactStore::default(),
            audit_sink: FakeAuditSink::default(),
            model_adapter: FakeModelAdapter::default(),
            tool_adapter: FakeToolAdapter::default(),
        }
    }

    /// Returns the deterministic id generator.
    #[must_use]
    pub const fn ids(&self) -> &DeterministicAgentIds {
        &self.ids
    }

    /// Returns the mutable deterministic id generator.
    #[must_use]
    pub fn ids_mut(&mut self) -> &mut DeterministicAgentIds {
        &mut self.ids
    }

    /// Returns the fake clock.
    #[must_use]
    pub const fn clock(&self) -> &FakeAgentClock {
        &self.clock
    }

    /// Returns the fake artifact store.
    #[must_use]
    pub const fn artifact_store(&self) -> &FakeArtifactStore {
        &self.artifact_store
    }

    /// Returns the mutable fake artifact store.
    #[must_use]
    pub fn artifact_store_mut(&mut self) -> &mut FakeArtifactStore {
        &mut self.artifact_store
    }

    /// Returns the fake audit sink.
    #[must_use]
    pub const fn audit_sink(&self) -> &FakeAuditSink {
        &self.audit_sink
    }

    /// Returns the mutable fake audit sink.
    #[must_use]
    pub fn audit_sink_mut(&mut self) -> &mut FakeAuditSink {
        &mut self.audit_sink
    }

    /// Returns the fake model adapter.
    #[must_use]
    pub const fn model_adapter(&self) -> &FakeModelAdapter {
        &self.model_adapter
    }

    /// Returns the mutable fake model adapter.
    #[must_use]
    pub fn model_adapter_mut(&mut self) -> &mut FakeModelAdapter {
        &mut self.model_adapter
    }

    /// Returns the fake tool adapter.
    #[must_use]
    pub const fn tool_adapter(&self) -> &FakeToolAdapter {
        &self.tool_adapter
    }

    /// Returns the mutable fake tool adapter.
    #[must_use]
    pub fn tool_adapter_mut(&mut self) -> &mut FakeToolAdapter {
        &mut self.tool_adapter
    }

    /// Returns the number of durable workflow records.
    #[must_use]
    pub fn durable_record_count(&self) -> usize {
        self.store.len()
    }

    /// Returns the in-memory metrics recorder.
    #[must_use]
    pub const fn metrics(&self) -> &InMemoryMetricsRecorder {
        &self.metrics
    }

    /// Creates a durable inbox for one run.
    #[must_use]
    pub fn durable_inbox(&self, run_id: &AgentRunId) -> FixtureDurableInbox {
        DurableInbox::with_clock(
            WorkflowId::new(format!("agent-run:{}", run_id.as_str())),
            self.store.clone(),
            self.clock.workflow_clock(),
        )
    }

    /// Stores an artifact with deterministic metadata and returns its reference.
    pub fn write_artifact(
        &mut self,
        kind: ArtifactKind,
        content_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> ArtifactRef {
        let bytes = bytes.into();
        let artifact_id = self.ids.next_artifact_id();
        let reference = ArtifactRef {
            artifact_id: artifact_id.clone(),
            kind,
            uri: format!("memory://agent-fixture/{artifact_id}"),
            checksum: Some(format!("len:{}", bytes.len())),
            content_type: Some(content_type.into()),
            byte_len: Some(bytes.len() as u64),
            retention_class: Some("test".to_string()),
            encryption: None,
            redaction: RedactionStatus::ReferenceOnly,
            created_at: self.clock.now(),
            metadata: BTreeMap::new(),
        };
        self.artifact_store.insert(reference, bytes)
    }

    /// Creates a sample workflow definition.
    pub fn sample_workflow(&mut self) -> AgentWorkflow {
        let workflow_id = self.ids.next_workflow_id();
        let step = self.sample_step(AgentStepKind::HumanCheckpoint);
        AgentWorkflow {
            workflow_id,
            workflow_type: "test-workflow".to_string(),
            definition_version: WorkflowDefinitionVersion::new("test-v1"),
            state_schema_version: StateSchemaVersion::new(1),
            display_name: Some("Test workflow".to_string()),
            status_labels: vec![
                AgentRunStatus::Accepted.as_label().to_string(),
                AgentRunStatus::Running.as_label().to_string(),
                AgentRunStatus::Completed.as_label().to_string(),
            ],
            command_types: vec![
                "StartRun".to_string(),
                "EffectCompleted".to_string(),
                "HumanDecisionSubmitted".to_string(),
            ],
            steps: vec![step],
            payload_types: vec![
                AgentPayloadDescriptor::new("fixture.input").content_type("application/json")
            ],
            retry_policy_ref: None,
            timeout_policy_ref: None,
            approval_policy_ref: None,
            observability_labels: BTreeMap::from([(
                "workflow_type".to_string(),
                "test-workflow".to_string(),
            )]),
        }
    }

    /// Creates a sample step.
    pub fn sample_step(&mut self, kind: AgentStepKind) -> AgentStep {
        AgentStep {
            step_id: self.ids.next_step_id(),
            kind,
            display_name: Some("Test step".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }
    }

    /// Creates a sample run state.
    pub fn sample_run_state(&mut self, workflow: &AgentWorkflow) -> AgentRunState {
        let run_id = self.ids.next_run_id();
        let input_ref =
            self.write_artifact(ArtifactKind::Input, "application/json", b"{}".to_vec());
        AgentRunState {
            run_id,
            workflow_id: workflow.workflow_id.clone(),
            tenant: Some(AgentTenantId::new("tenant-test")),
            definition_version: workflow.definition_version.clone(),
            state_schema_version: workflow.state_schema_version,
            graph_state: None,
            status: AgentRunStatus::Accepted,
            current_step_id: workflow.steps.first().map(|step| step.step_id.clone()),
            current_attempt: 0,
            inputs_ref: Some(input_ref),
            state_payload: AgentStatePayload::Empty,
            checkpoints: Vec::new(),
            pending_effects: Vec::new(),
            pending_human_checkpoint: None,
            cancellation: None,
            created_at: self.clock.now(),
            updated_at: self.clock.now(),
            completed_at: None,
        }
    }

    /// Creates a sample effect.
    pub fn sample_effect(&mut self, kind: AgentEffectKind) -> AgentEffect {
        let effect_id = self.ids.next_effect_id();
        AgentEffect {
            effect_id: effect_id.clone(),
            deduplication_key: AgentDeduplicationKey::new(format!("effect:{}", effect_id.as_str())),
            kind,
            target: AgentEffectTarget {
                target_type: "application".to_string(),
                name: "fake-target".to_string(),
                address: None,
                attributes: BTreeMap::new(),
            },
            status: AgentEffectStatus::Scheduled,
            payload_ref: None,
            result_ref: None,
            timeout_ms: Some(1_000),
            idempotency_key: AgentIdempotencyKey::new("effect-idempotency"),
            expected_result_type: Some("TestResult".to_string()),
            causation_id: self.ids.next_causation_id(),
            correlation_id: self.ids.next_correlation_id(),
            telemetry_context: AgentTelemetryContext::default(),
            attempt: 0,
            created_at: self.clock.now(),
            due_at: Some(self.clock.now()),
            last_error_code: None,
        }
    }

    /// Creates a sample human checkpoint.
    pub fn sample_checkpoint(&mut self) -> HumanCheckpoint {
        HumanCheckpoint {
            checkpoint_id: self.ids.next_checkpoint_id(),
            status: HumanCheckpointStatus::Open,
            summary: "Review fixture checkpoint".to_string(),
            available_decisions: Vec::new(),
            required_roles: vec!["reviewer".to_string()],
            due_at: None,
            escalation_target: None,
            context_artifacts: Vec::new(),
            created_by: Some(PrincipalRef {
                principal_type: "test".to_string(),
                principal_id: "fixture".to_string(),
                display_name: Some("Fixture".to_string()),
            }),
            resolved_by: None,
            created_at: self.clock.now(),
            resolved_at: None,
            audit_event_ids: Vec::new(),
        }
    }

    /// Records a sample audit event for a run.
    pub fn record_sample_audit_event(
        &mut self,
        workflow: &AgentWorkflow,
        run: &AgentRunState,
    ) -> AgentAuditEvent {
        let event = AgentAuditEvent {
            audit_event_id: self.ids.next_audit_event_id(),
            kind: AgentAuditEventKind::RunCreated,
            workflow_id: workflow.workflow_id.clone(),
            run_id: run.run_id.clone(),
            definition_version: workflow.definition_version.clone(),
            tenant: run.tenant.clone(),
            step_id: run.current_step_id.clone(),
            effect_id: None,
            checkpoint_id: None,
            command_id: Some(self.ids.next_command_id()),
            causation_id: self.ids.next_causation_id(),
            correlation_id: self.ids.next_correlation_id(),
            actor_principal: None,
            artifact_refs: run.inputs_ref.iter().cloned().collect(),
            content_hashes: BTreeMap::new(),
            redaction: RedactionStatus::ReferenceOnly,
            telemetry_context: AgentTelemetryContext::default(),
            occurred_at: self.clock.now(),
            attributes: BTreeMap::new(),
        };
        self.audit_sink.record(event.clone());
        event
    }

    /// Accepts a start command into a durable inbox.
    pub async fn accept_start(
        &mut self,
        inbox: &mut FixtureDurableInbox,
        run: &AgentRunState,
    ) -> WorkflowResult<InboxAcceptance> {
        let command_id = self.ids.next_command_id();
        let command = InboxCommand::new(
            command_id.as_str(),
            "agent.start-run",
            run.run_id.as_str().as_bytes().to_vec(),
        )
        .deduplication_key(format!("start:{}", run.run_id.as_str()));
        inbox.accept(command).await
    }

    /// Accepts the same start command id and deduplication key again.
    pub async fn accept_duplicate_start(
        &self,
        inbox: &mut FixtureDurableInbox,
        run: &AgentRunState,
        command_id: &AgentCommandId,
    ) -> WorkflowResult<InboxAcceptance> {
        let command = InboxCommand::new(
            command_id.as_str(),
            "agent.start-run",
            run.run_id.as_str().as_bytes().to_vec(),
        )
        .deduplication_key(format!("start:{}", run.run_id.as_str()));
        inbox.accept(command).await
    }

    /// Schedules an effect through the durable outbox.
    pub async fn schedule_effect(
        &self,
        inbox: &mut FixtureDurableInbox,
        effect: &AgentEffect,
    ) -> WorkflowResult<OutboxAcceptance> {
        let command = OutboxCommand::new(
            effect.effect_id.as_str(),
            OutboxTarget::application(effect.target.name.clone()),
            "agent.effect",
            effect.effect_id.as_str().as_bytes().to_vec(),
        )
        .deduplication_key(effect.deduplication_key.as_str());
        inbox.schedule_outbox(command).await
    }
}

impl Default for MinimalAgentFixture {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use rakka_workflow::{InboxAcceptance, OutboxAcceptance};

    use crate::{validate_artifact_ref, ArtifactEncryptionRef};

    use super::*;

    #[test]
    fn deterministic_ids_are_stable() {
        let mut ids = DeterministicAgentIds::new();

        assert_eq!(ids.next_run_id().as_str(), "run-1");
        assert_eq!(ids.next_run_id().as_str(), "run-2");
        assert_eq!(ids.next_effect_id().as_str(), "effect-1");
    }

    #[test]
    fn fake_artifact_store_records_bytes() {
        let mut fixture = MinimalAgentFixture::new();

        let reference =
            fixture.write_artifact(ArtifactKind::Prompt, "text/plain", b"hello".to_vec());

        assert!(fixture.artifact_store().contains(&reference.artifact_id));
        assert_eq!(
            fixture.artifact_store().bytes(&reference.artifact_id),
            Some(&b"hello"[..])
        );
    }

    #[tokio::test]
    async fn fake_artifact_store_implements_artifact_store_trait() {
        let mut store = FakeArtifactStore::default();
        let request = AgentArtifactWriteRequest::new(
            ArtifactKind::Prompt,
            "text/plain",
            b"hello".to_vec(),
            AgentTimestampMillis::new(10),
        )
        .artifact_id("prompt-artifact")
        .checksum("sha256:test")
        .retention_class("test")
        .redaction(RedactionStatus::ReferenceOnly)
        .encryption(
            ArtifactEncryptionRef::new("AES256-GCM", "kms://agent-workflow/test-key")
                .context("tenant", "tenant-test"),
        );

        let reference = store
            .put_artifact(request)
            .await
            .expect("fake store should write artifact");
        validate_artifact_ref(&reference).expect("fake store should create valid references");

        let read = store
            .get_artifact(&reference)
            .await
            .expect("fake store should read artifact");
        assert_eq!(read.reference, reference);
        assert_eq!(read.bytes, b"hello");
    }

    #[test]
    fn fixture_exposes_in_memory_metrics_recorder() {
        use rakka_core::MetricsRecorder;

        let fixture = MinimalAgentFixture::new();

        fixture.metrics().increment_counter(
            "rakka.agent_workflow.test.events",
            1,
            &[("surface", "testkit")],
        );

        let snapshot = fixture.metrics().snapshot();
        assert_eq!(snapshot.observations().len(), 1);
        assert_eq!(
            snapshot.observations()[0].name(),
            "rakka.agent_workflow.test.events"
        );
    }

    #[test]
    fn fake_adapters_record_requests_and_return_queued_outcomes() {
        let mut fixture = MinimalAgentFixture::new();
        let effect = fixture.sample_effect(AgentEffectKind::ModelCall);
        let result_ref =
            fixture.write_artifact(ArtifactKind::Completion, "text/plain", b"done".to_vec());
        fixture
            .model_adapter_mut()
            .push_outcome(FakeAdapterOutcome::success(Some(result_ref.clone())));

        let outcome = fixture.model_adapter_mut().invoke(FakeAdapterRequest {
            payload_ref: None,
            effect,
        });

        assert_eq!(
            outcome,
            FakeAdapterOutcome::Success {
                result_ref: Some(result_ref)
            }
        );
        assert_eq!(fixture.model_adapter().requests().len(), 1);
    }

    #[tokio::test]
    async fn fake_adapters_implement_model_and_tool_traits() {
        let mut fixture = MinimalAgentFixture::new();
        let prompt_ref =
            fixture.write_artifact(ArtifactKind::Prompt, "text/plain", b"prompt".to_vec());
        let result_ref =
            fixture.write_artifact(ArtifactKind::Completion, "text/plain", b"done".to_vec());
        let mut model_effect = fixture.sample_effect(AgentEffectKind::ModelCall);
        model_effect.payload_ref = Some(prompt_ref.clone());
        fixture
            .model_adapter_mut()
            .push_outcome(FakeAdapterOutcome::success(Some(result_ref.clone())));

        let model_request = AgentModelRequest::from_effect(model_effect).expect("model request");
        let model_outcome = fixture
            .model_adapter_mut()
            .invoke_model(model_request)
            .await
            .expect("fake model adapter should return outcome");

        match model_outcome {
            AgentAdapterOutcome::Completed {
                receipt,
                result_ref: Some(actual_ref),
                ..
            } => {
                assert_eq!(receipt.provider, "fake-model");
                assert_eq!(actual_ref, result_ref);
            }
            other => panic!("unexpected model outcome: {other:?}"),
        }
        assert_eq!(fixture.model_adapter().requests().len(), 1);
        assert_eq!(
            fixture.model_adapter().requests()[0].payload_ref,
            Some(prompt_ref)
        );

        let input_ref =
            fixture.write_artifact(ArtifactKind::Input, "application/json", b"{}".to_vec());
        let partial_ref = fixture.write_artifact(
            ArtifactKind::ToolOutput,
            "application/json",
            b"{\"partial\":true}".to_vec(),
        );
        let mut tool_effect = fixture.sample_effect(AgentEffectKind::ToolCall);
        tool_effect.payload_ref = Some(input_ref.clone());
        fixture
            .tool_adapter_mut()
            .push_outcome(FakeAdapterOutcome::timeout(250, Some(partial_ref.clone())));

        let tool_request = AgentToolRequest::from_effect(tool_effect).expect("tool request");
        let tool_outcome = fixture
            .tool_adapter_mut()
            .invoke_tool(tool_request)
            .await
            .expect("fake tool adapter should return outcome");

        match tool_outcome {
            AgentAdapterOutcome::TimedOut {
                receipt,
                timeout_ms,
                partial_result_ref: Some(actual_ref),
            } => {
                assert_eq!(receipt.provider, "fake-tool");
                assert_eq!(timeout_ms, 250);
                assert_eq!(actual_ref, partial_ref);
            }
            other => panic!("unexpected tool outcome: {other:?}"),
        }
        assert_eq!(fixture.tool_adapter().requests().len(), 1);
        assert_eq!(
            fixture.tool_adapter().requests()[0].payload_ref,
            Some(input_ref)
        );
    }

    #[tokio::test]
    async fn minimal_fixture_exercises_start_dedupe_effect_and_serialization() {
        let mut fixture = MinimalAgentFixture::new();
        let workflow = fixture.sample_workflow();
        let run = fixture.sample_run_state(&workflow);
        let run_json = serde_json::to_vec(&run).expect("run state should serialize");
        let decoded: AgentRunState =
            serde_json::from_slice(&run_json).expect("run state should deserialize");
        assert_eq!(run, decoded);

        let mut inbox = fixture.durable_inbox(&run.run_id);
        inbox.recover().await.expect("inbox should recover");

        let accepted = fixture
            .accept_start(&mut inbox, &run)
            .await
            .expect("start should be accepted");
        assert!(matches!(accepted, InboxAcceptance::Accepted { .. }));

        let duplicate = fixture
            .accept_duplicate_start(&mut inbox, &run, &AgentCommandId::new("command-duplicate"))
            .await
            .expect("duplicate start should be detected");
        assert!(matches!(duplicate, InboxAcceptance::Duplicate { .. }));

        let effect = fixture.sample_effect(AgentEffectKind::ToolCall);
        let scheduled = fixture
            .schedule_effect(&mut inbox, &effect)
            .await
            .expect("effect should be scheduled");
        assert!(matches!(scheduled, OutboxAcceptance::Scheduled { .. }));

        assert_eq!(inbox.due_outbox().expect("due outbox should load").len(), 1);
        assert_eq!(fixture.durable_record_count(), 1);
    }
}
