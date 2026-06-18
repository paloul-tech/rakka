//! Deterministic test harness helpers for agent workflow slices.

use std::collections::{BTreeMap, VecDeque};

use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    DurableInbox, InboxAcceptance, InboxCommand, ManualWorkflowClock, OutboxAcceptance,
    OutboxCommand, OutboxTarget, WorkflowClock, WorkflowId, WorkflowResult, WorkflowState,
    WorkflowTimestamp,
};

use crate::{
    AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCausationId, AgentCommandId,
    AgentCorrelationId, AgentDeduplicationKey, AgentEffect, AgentEffectId, AgentEffectKind,
    AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey, AgentPayloadDescriptor, AgentRunId,
    AgentRunState, AgentRunStatus, AgentStatePayload, AgentStep, AgentStepId, AgentStepKind,
    AgentTelemetryContext, AgentTenantId, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    ArtifactKind, ArtifactRef, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
    PrincipalRef, RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion,
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
