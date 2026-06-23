//! Runtime event contracts for graph-backed agent workflows.
//!
//! Runtime events are projection records emitted after durable graph state has
//! been persisted. They are useful for logs, UI projections, audit correlation,
//! and live streams, but durable run state remains the source of correctness.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::audit::{
    AGENT_LOG_ATTR_CAUSATION_ID, AGENT_LOG_ATTR_CHECKPOINT_ID, AGENT_LOG_ATTR_CORRELATION_ID,
    AGENT_LOG_ATTR_DEFINITION_VERSION, AGENT_LOG_ATTR_EFFECT_ID, AGENT_LOG_ATTR_RUN_ID,
    AGENT_LOG_ATTR_WORKFLOW_ID,
};
use crate::{
    is_bounded_agent_metric_attribute, is_forbidden_agent_metric_attribute, AgentAttributes,
    AgentCausationId, AgentCompiledNodeId, AgentCompiledPlanFingerprint, AgentCorrelationId,
    AgentEffectId, AgentGraphRunState, AgentRunId, AgentTelemetryContext, AgentTimerId,
    AgentTimestampMillis, AgentWorkflowId, HumanCheckpointId, WorkflowDefinitionVersion,
    AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES,
};

const AGENT_RUNTIME_EVENT_LOG_ATTR_KIND: &str = "runtime_event_kind";
const AGENT_RUNTIME_EVENT_LOG_ATTR_NODE_ID: &str = "node_id";
const AGENT_RUNTIME_EVENT_LOG_ATTR_TIMER_ID: &str = "timer_id";

/// Runtime event kinds emitted after durable graph state transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRuntimeEventKind {
    /// A run command was durably accepted.
    RunAccepted,
    /// A run started executing.
    RunStarted,
    /// A graph node became runnable.
    NodeRunnable,
    /// A graph node started executing.
    NodeStarted,
    /// A graph node completed.
    NodeCompleted,
    /// A graph node was skipped.
    NodeSkipped,
    /// A graph node failed.
    NodeFailed,
    /// A durable effect was scheduled.
    EffectScheduled,
    /// A durable effect completed.
    EffectCompleted,
    /// A durable effect failed.
    EffectFailed,
    /// A durable timer was scheduled.
    TimerScheduled,
    /// A durable timer fired.
    TimerFired,
    /// A human checkpoint opened.
    HumanCheckpointOpened,
    /// A human decision was accepted.
    HumanDecisionAccepted,
    /// A branch path was selected.
    BranchSelected,
    /// A bounded loop iteration started.
    LoopIterationStarted,
    /// A bounded loop iteration completed.
    LoopIterationCompleted,
    /// A run entered a waiting state.
    RunWaiting,
    /// A waiting run resumed.
    RunResumed,
    /// A run was cancelled.
    RunCancelled,
    /// A run completed successfully.
    RunCompleted,
    /// A run failed.
    RunFailed,
}

impl AgentRuntimeEventKind {
    /// Returns all runtime event kinds in stable order.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::RunAccepted,
            Self::RunStarted,
            Self::NodeRunnable,
            Self::NodeStarted,
            Self::NodeCompleted,
            Self::NodeSkipped,
            Self::NodeFailed,
            Self::EffectScheduled,
            Self::EffectCompleted,
            Self::EffectFailed,
            Self::TimerScheduled,
            Self::TimerFired,
            Self::HumanCheckpointOpened,
            Self::HumanDecisionAccepted,
            Self::BranchSelected,
            Self::LoopIterationStarted,
            Self::LoopIterationCompleted,
            Self::RunWaiting,
            Self::RunResumed,
            Self::RunCancelled,
            Self::RunCompleted,
            Self::RunFailed,
        ]
    }

    /// Stable lowercase label for storage, logs, and bounded diagnostics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::RunAccepted => "run-accepted",
            Self::RunStarted => "run-started",
            Self::NodeRunnable => "node-runnable",
            Self::NodeStarted => "node-started",
            Self::NodeCompleted => "node-completed",
            Self::NodeSkipped => "node-skipped",
            Self::NodeFailed => "node-failed",
            Self::EffectScheduled => "effect-scheduled",
            Self::EffectCompleted => "effect-completed",
            Self::EffectFailed => "effect-failed",
            Self::TimerScheduled => "timer-scheduled",
            Self::TimerFired => "timer-fired",
            Self::HumanCheckpointOpened => "human-checkpoint-opened",
            Self::HumanDecisionAccepted => "human-decision-accepted",
            Self::BranchSelected => "branch-selected",
            Self::LoopIterationStarted => "loop-iteration-started",
            Self::LoopIterationCompleted => "loop-iteration-completed",
            Self::RunWaiting => "run-waiting",
            Self::RunResumed => "run-resumed",
            Self::RunCancelled => "run-cancelled",
            Self::RunCompleted => "run-completed",
            Self::RunFailed => "run-failed",
        }
    }

    /// Parses a stable runtime event kind label.
    #[must_use]
    pub fn from_label(value: &str) -> Option<Self> {
        Self::all()
            .iter()
            .copied()
            .find(|kind| kind.as_label() == value)
    }

    const fn requires_node_id(self) -> bool {
        matches!(
            self,
            Self::NodeRunnable
                | Self::NodeStarted
                | Self::NodeCompleted
                | Self::NodeSkipped
                | Self::NodeFailed
                | Self::BranchSelected
                | Self::LoopIterationStarted
                | Self::LoopIterationCompleted
        )
    }

    const fn requires_effect_id(self) -> bool {
        matches!(
            self,
            Self::EffectScheduled | Self::EffectCompleted | Self::EffectFailed
        )
    }

    const fn requires_timer_id(self) -> bool {
        matches!(self, Self::TimerScheduled | Self::TimerFired)
    }

    const fn requires_checkpoint_id(self) -> bool {
        matches!(
            self,
            Self::HumanCheckpointOpened | Self::HumanDecisionAccepted
        )
    }

    const fn is_terminal_run_event(self) -> bool {
        matches!(
            self,
            Self::RunCancelled | Self::RunCompleted | Self::RunFailed
        )
    }
}

/// Runtime event emitted after a durable graph state transition succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRuntimeEvent {
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Durable run id.
    pub run_id: AgentRunId,
    /// Workflow definition version selected for the run.
    pub definition_version: WorkflowDefinitionVersion,
    /// Immutable compiled plan fingerprint selected for the run.
    pub plan_fingerprint: AgentCompiledPlanFingerprint,
    /// Scheduler revision persisted with the state transition.
    pub scheduler_revision: u64,
    /// Per-run monotonic event sequence.
    pub event_sequence: u64,
    /// Event timestamp.
    pub occurred_at: AgentTimestampMillis,
    /// Runtime event kind.
    pub kind: AgentRuntimeEventKind,
    /// Compiled graph node id, when the event is node-scoped.
    #[serde(default)]
    pub node_id: Option<AgentCompiledNodeId>,
    /// Durable effect id, when the event is effect-scoped.
    #[serde(default)]
    pub effect_id: Option<AgentEffectId>,
    /// Durable timer id, when the event is timer-scoped.
    #[serde(default)]
    pub timer_id: Option<AgentTimerId>,
    /// Human checkpoint id, when the event is human-scoped.
    #[serde(default)]
    pub checkpoint_id: Option<HumanCheckpointId>,
    /// Command or event that caused this runtime event.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related runtime records.
    pub correlation_id: AgentCorrelationId,
    /// Trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
    /// Bounded attributes safe for hot projection labels.
    #[serde(default)]
    pub attributes: AgentAttributes,
}

impl AgentRuntimeEvent {
    /// Creates a runtime event with explicit persisted ordering fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        workflow_id: AgentWorkflowId,
        run_id: AgentRunId,
        definition_version: WorkflowDefinitionVersion,
        plan_fingerprint: AgentCompiledPlanFingerprint,
        scheduler_revision: u64,
        event_sequence: u64,
        occurred_at: AgentTimestampMillis,
        kind: AgentRuntimeEventKind,
        causation_id: AgentCausationId,
        correlation_id: AgentCorrelationId,
        telemetry_context: AgentTelemetryContext,
    ) -> AgentRuntimeEventResult<Self> {
        let event = Self {
            workflow_id,
            run_id,
            definition_version,
            plan_fingerprint,
            scheduler_revision,
            event_sequence,
            occurred_at,
            kind,
            node_id: None,
            effect_id: None,
            timer_id: None,
            checkpoint_id: None,
            causation_id,
            correlation_id,
            telemetry_context,
            attributes: AgentAttributes::new(),
        };
        validate_runtime_event_base(&event)?;
        Ok(event)
    }

    /// Adds a compiled graph node id.
    pub fn node_id(mut self, node_id: AgentCompiledNodeId) -> AgentRuntimeEventResult<Self> {
        self.node_id = Some(node_id);
        validate_runtime_event(&self)?;
        Ok(self)
    }

    /// Adds a durable effect id.
    pub fn effect_id(mut self, effect_id: AgentEffectId) -> AgentRuntimeEventResult<Self> {
        self.effect_id = Some(effect_id);
        validate_runtime_event(&self)?;
        Ok(self)
    }

    /// Adds a durable timer id.
    pub fn timer_id(mut self, timer_id: AgentTimerId) -> AgentRuntimeEventResult<Self> {
        self.timer_id = Some(timer_id);
        validate_runtime_event(&self)?;
        Ok(self)
    }

    /// Adds a human checkpoint id.
    pub fn checkpoint_id(
        mut self,
        checkpoint_id: HumanCheckpointId,
    ) -> AgentRuntimeEventResult<Self> {
        self.checkpoint_id = Some(checkpoint_id);
        validate_runtime_event(&self)?;
        Ok(self)
    }

    /// Adds a bounded event attribute.
    pub fn attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> AgentRuntimeEventResult<Self> {
        self.attributes.insert(key.into(), value.into());
        validate_runtime_event(&self)?;
        Ok(self)
    }

    /// Returns the stable correlation fields shared by event streams, logs,
    /// audit records, and traces.
    #[must_use]
    pub fn correlation_fields(&self) -> AgentRuntimeEventCorrelationFields {
        AgentRuntimeEventCorrelationFields {
            workflow_id: self.workflow_id.clone(),
            run_id: self.run_id.clone(),
            definition_version: self.definition_version.clone(),
            event_kind: self.kind,
            node_id: self.node_id.clone(),
            effect_id: self.effect_id.clone(),
            timer_id: self.timer_id.clone(),
            checkpoint_id: self.checkpoint_id.clone(),
            causation_id: self.causation_id.clone(),
            correlation_id: self.correlation_id.clone(),
            telemetry_context: self.telemetry_context.clone(),
        }
    }
}

/// Stable fields shared by runtime events, logs, audit records, and traces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRuntimeEventCorrelationFields {
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Durable run id.
    pub run_id: AgentRunId,
    /// Workflow definition version selected for the run.
    pub definition_version: WorkflowDefinitionVersion,
    /// Runtime event kind.
    pub event_kind: AgentRuntimeEventKind,
    /// Compiled graph node id, when the event is node-scoped.
    #[serde(default)]
    pub node_id: Option<AgentCompiledNodeId>,
    /// Durable effect id, when the event is effect-scoped.
    #[serde(default)]
    pub effect_id: Option<AgentEffectId>,
    /// Durable timer id, when the event is timer-scoped.
    #[serde(default)]
    pub timer_id: Option<AgentTimerId>,
    /// Human checkpoint id, when the event is human-scoped.
    #[serde(default)]
    pub checkpoint_id: Option<HumanCheckpointId>,
    /// Command or event that caused this runtime event.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related runtime records.
    pub correlation_id: AgentCorrelationId,
    /// Trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
}

impl AgentRuntimeEventCorrelationFields {
    /// Converts correlation fields into structured log and audit attributes.
    ///
    /// These attributes may contain ids and are intended for logs/audit/traces,
    /// not for hot metric labels.
    #[must_use]
    pub fn log_attributes(&self) -> AgentAttributes {
        let mut attributes = AgentAttributes::new();
        attributes.insert(
            AGENT_LOG_ATTR_WORKFLOW_ID.to_string(),
            self.workflow_id.to_string(),
        );
        attributes.insert(AGENT_LOG_ATTR_RUN_ID.to_string(), self.run_id.to_string());
        attributes.insert(
            AGENT_LOG_ATTR_DEFINITION_VERSION.to_string(),
            self.definition_version.to_string(),
        );
        attributes.insert(
            AGENT_RUNTIME_EVENT_LOG_ATTR_KIND.to_string(),
            self.event_kind.as_label().to_string(),
        );
        attributes.insert(
            AGENT_LOG_ATTR_CAUSATION_ID.to_string(),
            self.causation_id.to_string(),
        );
        attributes.insert(
            AGENT_LOG_ATTR_CORRELATION_ID.to_string(),
            self.correlation_id.to_string(),
        );
        if let Some(node_id) = &self.node_id {
            attributes.insert(
                AGENT_RUNTIME_EVENT_LOG_ATTR_NODE_ID.to_string(),
                node_id.to_string(),
            );
        }
        if let Some(effect_id) = &self.effect_id {
            attributes.insert(AGENT_LOG_ATTR_EFFECT_ID.to_string(), effect_id.to_string());
        }
        if let Some(timer_id) = &self.timer_id {
            attributes.insert(
                AGENT_RUNTIME_EVENT_LOG_ATTR_TIMER_ID.to_string(),
                timer_id.to_string(),
            );
        }
        if let Some(checkpoint_id) = &self.checkpoint_id {
            attributes.insert(
                AGENT_LOG_ATTR_CHECKPOINT_ID.to_string(),
                checkpoint_id.to_string(),
            );
        }
        attributes
    }
}

/// Rebuildable run-level projection produced from an ordered runtime event stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRuntimeEventProjection {
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Durable run id.
    pub run_id: AgentRunId,
    /// Workflow definition version selected for the run.
    pub definition_version: WorkflowDefinitionVersion,
    /// Immutable compiled plan fingerprint selected for the run.
    pub plan_fingerprint: AgentCompiledPlanFingerprint,
    /// Last scheduler revision observed in the event stream.
    pub last_scheduler_revision: u64,
    /// Last per-run event sequence applied to this projection.
    pub last_event_sequence: u64,
    /// Timestamp of the last applied runtime event.
    #[serde(default)]
    pub last_event_at: Option<AgentTimestampMillis>,
    /// Kind of the last applied runtime event.
    #[serde(default)]
    pub last_event_kind: Option<AgentRuntimeEventKind>,
    /// Number of events applied to this projection.
    pub event_count: u64,
    /// Number of node-scoped events applied to this projection.
    #[serde(default)]
    pub node_event_count: u64,
    /// Number of effect-scoped events applied to this projection.
    #[serde(default)]
    pub effect_event_count: u64,
    /// Number of timer-scoped events applied to this projection.
    #[serde(default)]
    pub timer_event_count: u64,
    /// Number of human checkpoint-scoped events applied to this projection.
    #[serde(default)]
    pub human_event_count: u64,
    /// Terminal run event kind, when the stream has reached one.
    #[serde(default)]
    pub terminal_event_kind: Option<AgentRuntimeEventKind>,
}

impl AgentRuntimeEventProjection {
    /// Rebuilds a run-level projection from a full ordered runtime event stream.
    pub fn from_events(events: &[AgentRuntimeEvent]) -> AgentRuntimeEventResult<Self> {
        let Some(first) = events.first() else {
            return Err(AgentRuntimeEventError::InvalidEvent {
                field: "events",
                reason: "projection rebuild requires at least one event",
            });
        };
        let mut projection = Self::new_for_stream(first);
        for event in events {
            projection.apply_event(event)?;
        }
        Ok(projection)
    }

    /// Applies the next runtime event to this projection.
    pub fn apply_event(&mut self, event: &AgentRuntimeEvent) -> AgentRuntimeEventResult<()> {
        self.validate_same_stream(event)?;
        validate_runtime_event_follows(self.last_event_sequence, event)?;

        self.last_scheduler_revision = event.scheduler_revision;
        self.last_event_sequence = event.event_sequence;
        self.last_event_at = Some(event.occurred_at);
        self.last_event_kind = Some(event.kind);
        self.event_count =
            self.event_count
                .checked_add(1)
                .ok_or(AgentRuntimeEventError::InvalidEvent {
                    field: "event_count",
                    reason: "projection event count overflow",
                })?;

        if event.kind.requires_node_id() {
            self.node_event_count = self.node_event_count.checked_add(1).ok_or(
                AgentRuntimeEventError::InvalidEvent {
                    field: "node_event_count",
                    reason: "projection node event count overflow",
                },
            )?;
        }
        if event.kind.requires_effect_id() {
            self.effect_event_count = self.effect_event_count.checked_add(1).ok_or(
                AgentRuntimeEventError::InvalidEvent {
                    field: "effect_event_count",
                    reason: "projection effect event count overflow",
                },
            )?;
        }
        if event.kind.requires_timer_id() {
            self.timer_event_count = self.timer_event_count.checked_add(1).ok_or(
                AgentRuntimeEventError::InvalidEvent {
                    field: "timer_event_count",
                    reason: "projection timer event count overflow",
                },
            )?;
        }
        if event.kind.requires_checkpoint_id() {
            self.human_event_count = self.human_event_count.checked_add(1).ok_or(
                AgentRuntimeEventError::InvalidEvent {
                    field: "human_event_count",
                    reason: "projection human event count overflow",
                },
            )?;
        }
        if event.kind.is_terminal_run_event() {
            self.terminal_event_kind = Some(event.kind);
        }
        Ok(())
    }

    fn new_for_stream(first_event: &AgentRuntimeEvent) -> Self {
        Self {
            workflow_id: first_event.workflow_id.clone(),
            run_id: first_event.run_id.clone(),
            definition_version: first_event.definition_version.clone(),
            plan_fingerprint: first_event.plan_fingerprint.clone(),
            last_scheduler_revision: 0,
            last_event_sequence: 0,
            last_event_at: None,
            last_event_kind: None,
            event_count: 0,
            node_event_count: 0,
            effect_event_count: 0,
            timer_event_count: 0,
            human_event_count: 0,
            terminal_event_kind: None,
        }
    }

    fn validate_same_stream(&self, event: &AgentRuntimeEvent) -> AgentRuntimeEventResult<()> {
        if event.workflow_id != self.workflow_id {
            return Err(projection_stream_mismatch("workflow_id"));
        }
        if event.run_id != self.run_id {
            return Err(projection_stream_mismatch("run_id"));
        }
        if event.definition_version != self.definition_version {
            return Err(projection_stream_mismatch("definition_version"));
        }
        if event.plan_fingerprint != self.plan_fingerprint {
            return Err(projection_stream_mismatch("plan_fingerprint"));
        }
        Ok(())
    }
}

/// Runtime event data that can only become an event after persistence succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRuntimeEventDraft {
    workflow_id: AgentWorkflowId,
    run_id: AgentRunId,
    definition_version: WorkflowDefinitionVersion,
    occurred_at: AgentTimestampMillis,
    kind: AgentRuntimeEventKind,
    node_id: Option<AgentCompiledNodeId>,
    effect_id: Option<AgentEffectId>,
    timer_id: Option<AgentTimerId>,
    checkpoint_id: Option<HumanCheckpointId>,
    causation_id: AgentCausationId,
    correlation_id: AgentCorrelationId,
    telemetry_context: AgentTelemetryContext,
    attributes: AgentAttributes,
}

impl AgentRuntimeEventDraft {
    /// Creates a runtime event draft for a graph run.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        workflow_id: AgentWorkflowId,
        run_id: AgentRunId,
        definition_version: WorkflowDefinitionVersion,
        occurred_at: AgentTimestampMillis,
        kind: AgentRuntimeEventKind,
        causation_id: AgentCausationId,
        correlation_id: AgentCorrelationId,
        telemetry_context: AgentTelemetryContext,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            definition_version,
            occurred_at,
            kind,
            node_id: None,
            effect_id: None,
            timer_id: None,
            checkpoint_id: None,
            causation_id,
            correlation_id,
            telemetry_context,
            attributes: AgentAttributes::new(),
        }
    }

    /// Adds a compiled graph node id.
    #[must_use]
    pub fn node_id(mut self, node_id: AgentCompiledNodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Adds a durable effect id.
    #[must_use]
    pub fn effect_id(mut self, effect_id: AgentEffectId) -> Self {
        self.effect_id = Some(effect_id);
        self
    }

    /// Adds a durable timer id.
    #[must_use]
    pub fn timer_id(mut self, timer_id: AgentTimerId) -> Self {
        self.timer_id = Some(timer_id);
        self
    }

    /// Adds a human checkpoint id.
    #[must_use]
    pub fn checkpoint_id(mut self, checkpoint_id: HumanCheckpointId) -> Self {
        self.checkpoint_id = Some(checkpoint_id);
        self
    }

    /// Adds a bounded event attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Finalizes this draft only when durable persistence has succeeded.
    ///
    /// Pass `Some(persisted_graph)` after the state transition is durably
    /// persisted. Pass `None` when persistence failed; no success event is
    /// produced.
    pub fn after_persistence(
        self,
        persisted_graph: Option<&AgentGraphRunState>,
    ) -> AgentRuntimeEventResult<Option<AgentRuntimeEvent>> {
        let Some(graph) = persisted_graph else {
            return Ok(None);
        };
        let mut event = AgentRuntimeEvent::new(
            self.workflow_id,
            self.run_id,
            self.definition_version,
            graph.plan_fingerprint.clone(),
            graph.scheduler_revision,
            next_runtime_event_sequence(graph.last_event_sequence)?,
            self.occurred_at,
            self.kind,
            self.causation_id,
            self.correlation_id,
            self.telemetry_context,
        )?;
        event.node_id = self.node_id;
        event.effect_id = self.effect_id;
        event.timer_id = self.timer_id;
        event.checkpoint_id = self.checkpoint_id;
        event.attributes = self.attributes;
        validate_runtime_event(&event)?;
        Ok(Some(event))
    }
}

/// Boxed future returned by runtime event sinks.
pub type AgentRuntimeEventSinkFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentRuntimeEventResult<T>> + Send + 'a>>;

/// Runtime event sink write status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRuntimeEventWriteStatus {
    /// Event was recorded by the sink.
    Recorded,
    /// Event had already been recorded by the sink.
    Duplicate,
}

impl AgentRuntimeEventWriteStatus {
    /// Stable lowercase label for logs and bounded diagnostics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::Duplicate => "duplicate",
        }
    }
}

/// Result of recording one runtime event in a sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRuntimeEventAcceptance {
    /// Durable run id.
    pub run_id: AgentRunId,
    /// Per-run event sequence.
    pub event_sequence: u64,
    /// Sink write status.
    pub status: AgentRuntimeEventWriteStatus,
}

/// Post-persistence sink for runtime events.
///
/// Sink writes happen after durable graph state transitions. Sink failures are
/// observable through the returned error, but they must not be used to create
/// or roll back graph state transitions.
pub trait AgentRuntimeEventSink {
    /// Records one runtime event after the corresponding state transition persisted.
    fn record_runtime_event<'a>(
        &'a mut self,
        event: AgentRuntimeEvent,
    ) -> AgentRuntimeEventSinkFuture<'a, AgentRuntimeEventAcceptance>;

    /// Returns runtime events for one run ordered by per-run event sequence.
    fn runtime_events_for_run<'a>(
        &'a self,
        run_id: AgentRunId,
    ) -> AgentRuntimeEventSinkFuture<'a, Vec<AgentRuntimeEvent>>;

    /// Returns all runtime events in deterministic run/sequence order.
    fn all_runtime_events<'a>(&'a self) -> AgentRuntimeEventSinkFuture<'a, Vec<AgentRuntimeEvent>>;
}

/// In-memory runtime event sink for deterministic tests and examples.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentRuntimeEventSink {
    events: Vec<AgentRuntimeEvent>,
    fail_next_write: Option<String>,
}

impl InMemoryAgentRuntimeEventSink {
    /// Creates an empty in-memory runtime event sink.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            events: Vec::new(),
            fail_next_write: None,
        }
    }

    /// Configures the next write to fail with a bounded sink error.
    #[must_use]
    pub fn fail_next_write(mut self, message: impl Into<String>) -> Self {
        self.fail_next_write = Some(message.into());
        self
    }

    /// Returns all stored events in insertion order.
    #[must_use]
    pub fn events(&self) -> &[AgentRuntimeEvent] {
        &self.events
    }
}

impl AgentRuntimeEventSink for InMemoryAgentRuntimeEventSink {
    fn record_runtime_event<'a>(
        &'a mut self,
        event: AgentRuntimeEvent,
    ) -> AgentRuntimeEventSinkFuture<'a, AgentRuntimeEventAcceptance> {
        Box::pin(async move {
            if let Some(message) = self.fail_next_write.take() {
                return Err(AgentRuntimeEventError::Sink {
                    message: bounded_sink_message(message),
                });
            }

            validate_runtime_event(&event)?;
            if self.events.iter().any(|stored| {
                stored.run_id == event.run_id && stored.event_sequence == event.event_sequence
            }) {
                if self.events.iter().any(|stored| stored == &event) {
                    return Ok(AgentRuntimeEventAcceptance {
                        run_id: event.run_id,
                        event_sequence: event.event_sequence,
                        status: AgentRuntimeEventWriteStatus::Duplicate,
                    });
                }
                return Err(AgentRuntimeEventError::InvalidSequence {
                    previous: previous_event_sequence_for_run(&self.events, &event.run_id),
                    next: event.event_sequence,
                });
            }

            let previous = previous_event_sequence_for_run(&self.events, &event.run_id);
            validate_runtime_event_follows(previous, &event)?;
            let run_id = event.run_id.clone();
            let event_sequence = event.event_sequence;
            self.events.push(event);
            Ok(AgentRuntimeEventAcceptance {
                run_id,
                event_sequence,
                status: AgentRuntimeEventWriteStatus::Recorded,
            })
        })
    }

    fn runtime_events_for_run<'a>(
        &'a self,
        run_id: AgentRunId,
    ) -> AgentRuntimeEventSinkFuture<'a, Vec<AgentRuntimeEvent>> {
        Box::pin(async move {
            let mut events = self
                .events
                .iter()
                .filter(|event| event.run_id == run_id)
                .cloned()
                .collect::<Vec<_>>();
            events.sort_by_key(|event| event.event_sequence);
            Ok(events)
        })
    }

    fn all_runtime_events<'a>(&'a self) -> AgentRuntimeEventSinkFuture<'a, Vec<AgentRuntimeEvent>> {
        Box::pin(async move {
            let mut events = self.events.clone();
            events.sort_by(|left, right| {
                left.run_id
                    .cmp(&right.run_id)
                    .then(left.event_sequence.cmp(&right.event_sequence))
            });
            Ok(events)
        })
    }
}

/// Shared result type for runtime event validation.
pub type AgentRuntimeEventResult<T> = Result<T, AgentRuntimeEventError>;

/// Runtime event validation failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRuntimeEventError {
    /// A required runtime event field is missing or invalid.
    InvalidEvent {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Runtime event sequence did not advance monotonically.
    InvalidSequence {
        /// Previous persisted event sequence.
        previous: u64,
        /// Candidate event sequence.
        next: u64,
    },
    /// A runtime event attribute is unsafe for hot projections.
    UnsafeAttribute {
        /// Attribute key that failed validation.
        key: String,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Runtime event sink failed after persistence.
    Sink {
        /// Bounded sink failure message.
        message: String,
    },
}

impl AgentRuntimeEventError {
    /// Stable error code for programmatic assertions and logs.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidEvent { .. } => "invalid-runtime-event",
            Self::InvalidSequence { .. } => "invalid-runtime-event-sequence",
            Self::UnsafeAttribute { .. } => "unsafe-runtime-event-attribute",
            Self::Sink { .. } => "runtime-event-sink",
        }
    }
}

impl Display for AgentRuntimeEventError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvent { field, reason } => {
                write!(f, "invalid runtime event field `{field}`: {reason}")
            }
            Self::InvalidSequence { previous, next } => write!(
                f,
                "runtime event sequence `{next}` does not follow previous sequence `{previous}`"
            ),
            Self::UnsafeAttribute { key, reason } => {
                write!(f, "unsafe runtime event attribute `{key}`: {reason}")
            }
            Self::Sink { message } => write!(f, "runtime event sink failed: {message}"),
        }
    }
}

impl Error for AgentRuntimeEventError {}

/// Returns the next per-run runtime event sequence.
pub fn next_runtime_event_sequence(previous: u64) -> AgentRuntimeEventResult<u64> {
    previous
        .checked_add(1)
        .ok_or(AgentRuntimeEventError::InvalidEvent {
            field: "event_sequence",
            reason: "event sequence overflow",
        })
}

/// Validates that an event follows the previous per-run event sequence.
pub fn validate_runtime_event_follows(
    previous: u64,
    event: &AgentRuntimeEvent,
) -> AgentRuntimeEventResult<()> {
    let expected = next_runtime_event_sequence(previous)?;
    if event.event_sequence != expected {
        return Err(AgentRuntimeEventError::InvalidSequence {
            previous,
            next: event.event_sequence,
        });
    }
    validate_runtime_event(event)
}

/// Validates a runtime event contract.
pub fn validate_runtime_event(event: &AgentRuntimeEvent) -> AgentRuntimeEventResult<()> {
    validate_runtime_event_base(event)?;
    if event.kind.requires_node_id() && event.node_id.is_none() {
        return Err(AgentRuntimeEventError::InvalidEvent {
            field: "node_id",
            reason: "event kind requires a node id",
        });
    }
    if event.kind.requires_effect_id() && event.effect_id.is_none() {
        return Err(AgentRuntimeEventError::InvalidEvent {
            field: "effect_id",
            reason: "event kind requires an effect id",
        });
    }
    if event.kind.requires_timer_id() && event.timer_id.is_none() {
        return Err(AgentRuntimeEventError::InvalidEvent {
            field: "timer_id",
            reason: "event kind requires a timer id",
        });
    }
    if event.kind.requires_checkpoint_id() && event.checkpoint_id.is_none() {
        return Err(AgentRuntimeEventError::InvalidEvent {
            field: "checkpoint_id",
            reason: "event kind requires a checkpoint id",
        });
    }
    Ok(())
}

fn validate_runtime_event_base(event: &AgentRuntimeEvent) -> AgentRuntimeEventResult<()> {
    require(event.workflow_id.as_str(), "workflow_id")?;
    require(event.run_id.as_str(), "run_id")?;
    require(event.definition_version.as_str(), "definition_version")?;
    require(event.plan_fingerprint.as_str(), "plan_fingerprint")?;
    require(event.causation_id.as_str(), "causation_id")?;
    require(event.correlation_id.as_str(), "correlation_id")?;
    if event.event_sequence == 0 {
        return Err(AgentRuntimeEventError::InvalidEvent {
            field: "event_sequence",
            reason: "event sequence must start at one",
        });
    }
    validate_runtime_event_attributes(&event.attributes)?;
    Ok(())
}

fn validate_runtime_event_attributes(attributes: &AgentAttributes) -> AgentRuntimeEventResult<()> {
    for (key, value) in attributes {
        if key.trim().is_empty() {
            return Err(unsafe_attribute(key, "attribute keys must not be empty"));
        }
        if key.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
            return Err(unsafe_attribute(key, "attribute keys must be bounded"));
        }
        if value.trim().is_empty() {
            return Err(unsafe_attribute(key, "attribute values must not be empty"));
        }
        if value.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
            return Err(unsafe_attribute(key, "attribute values must be bounded"));
        }
        if value.contains('\n') || value.contains('\r') {
            return Err(unsafe_attribute(
                key,
                "attribute values must be single-line bounded labels",
            ));
        }
        if is_forbidden_agent_metric_attribute(key) || !is_bounded_agent_metric_attribute(key) {
            return Err(unsafe_attribute(
                key,
                "attribute keys must be bounded hot-metric attributes",
            ));
        }
    }
    Ok(())
}

fn require(value: &str, field: &'static str) -> AgentRuntimeEventResult<()> {
    if value.trim().is_empty() {
        return Err(AgentRuntimeEventError::InvalidEvent {
            field,
            reason: "required field must be non-empty",
        });
    }
    Ok(())
}

fn unsafe_attribute(key: &str, reason: &'static str) -> AgentRuntimeEventError {
    AgentRuntimeEventError::UnsafeAttribute {
        key: key.to_string(),
        reason,
    }
}

fn projection_stream_mismatch(field: &'static str) -> AgentRuntimeEventError {
    AgentRuntimeEventError::InvalidEvent {
        field,
        reason: "event does not belong to this projection stream",
    }
}

fn previous_event_sequence_for_run(events: &[AgentRuntimeEvent], run_id: &AgentRunId) -> u64 {
    events
        .iter()
        .filter(|event| &event.run_id == run_id)
        .map(|event| event.event_sequence)
        .max()
        .unwrap_or(0)
}

fn bounded_sink_message(message: impl Into<String>) -> String {
    let mut message = message.into();
    message.retain(|character| character != '\n' && character != '\r');
    if message.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
        message.truncate(AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES);
    }
    if message.trim().is_empty() {
        "runtime event sink failed".to_string()
    } else {
        message
    }
}
