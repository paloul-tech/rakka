//! Compiled graph effect bridge.
//!
//! This module maps effect-producing compiled graph nodes to first-class
//! [`AgentEffect`] values and schedules them through the existing durable
//! outbox boundary. It does not dispatch external work directly.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_persistence::DurableStateStore;
use rakka_workflow::{WorkflowClock, WorkflowState};
use serde::{Deserialize, Serialize};

use crate::{
    agent_run_workflow_id, validate_compiled_execution_plan, AgentCausationId,
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledNodeTarget, AgentCompiledPlanNode, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectTarget, AgentFacadeError, AgentGraphNodeStatus,
    AgentGraphRunState, AgentGraphScheduler, AgentGraphSchedulerTransition,
    AgentGraphTerminalStatus, AgentGraphWaitReason, AgentIdempotencyKey, AgentOutboxAcceptance,
    AgentOutboxError, AgentRunId, AgentRunInbox, AgentTelemetryContext, AgentTimestampMillis,
    ArtifactRef, AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE,
};

const ATTR_COMPILED_NODE_ID: &str = "compiled_node_id";
const ATTR_COMPILED_PLAN_FINGERPRINT: &str = "compiled_plan_fingerprint";
const ATTR_LOOP_INSTANCE_ID: &str = "loop_instance_id";
const ATTR_NODE_KIND: &str = "node_kind";
const ATTR_TARGET_CLASS: &str = "target_class";
const ROOT_LOOP_INSTANCE_ID: &str = "root";

/// Result type for compiled graph effect bridge operations.
pub type AgentGraphEffectBridgeResult<T> = Result<T, AgentGraphEffectBridgeError>;

/// Stable failures returned while mapping graph nodes to durable effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentGraphEffectBridgeError {
    /// Compiled plan validation failed before effect mapping.
    InvalidCompiledPlan {
        /// Stable compiled-plan validation code.
        validation_code: &'static str,
        /// Bounded diagnostic reason.
        reason: String,
    },
    /// Durable graph state does not match the compiled plan.
    PlanStateMismatch {
        /// Mismatched field.
        field: &'static str,
        /// Bounded diagnostic reason.
        reason: String,
    },
    /// Effect scheduling was requested against the wrong run inbox.
    RunInboxMismatch {
        /// Expected durable workflow id.
        expected_workflow_id: String,
        /// Actual durable workflow id.
        actual_workflow_id: String,
    },
    /// Effect scheduling was requested for a terminal graph.
    TerminalGraph {
        /// Terminal graph status.
        status: AgentGraphTerminalStatus,
    },
    /// A caller referenced a node not present in the compiled plan.
    UnknownNode {
        /// Unknown node id.
        node_id: AgentCompiledNodeId,
    },
    /// Durable graph state is missing a node from the compiled plan.
    MissingNodeState {
        /// Missing node id.
        node_id: AgentCompiledNodeId,
    },
    /// The compiled node kind does not map to a durable outbox effect.
    UnsupportedNodeKind {
        /// Node id.
        node_id: AgentCompiledNodeId,
        /// Unsupported node kind.
        kind: AgentCompiledNodeKind,
    },
    /// An effect-producing node is missing its logical target.
    MissingTarget {
        /// Node id.
        node_id: AgentCompiledNodeId,
        /// Node kind.
        kind: AgentCompiledNodeKind,
    },
    /// The node state is not in a status that can schedule an effect.
    InvalidNodeStatus {
        /// Node id.
        node_id: AgentCompiledNodeId,
        /// Current node status.
        status: AgentGraphNodeStatus,
    },
    /// Agent effect facade validation rejected the mapped effect.
    InvalidEffect {
        /// Validation failure.
        error: AgentFacadeError,
    },
    /// Durable outbox scheduling failed.
    Outbox {
        /// Durable outbox failure.
        error: AgentOutboxError,
    },
}

impl AgentGraphEffectBridgeError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidCompiledPlan { .. } => "invalid-compiled-plan",
            Self::PlanStateMismatch { .. } => "graph-plan-state-mismatch",
            Self::RunInboxMismatch { .. } => "graph-effect-run-inbox-mismatch",
            Self::TerminalGraph { .. } => "terminal-graph",
            Self::UnknownNode { .. } => "unknown-graph-node",
            Self::MissingNodeState { .. } => "missing-graph-node-state",
            Self::UnsupportedNodeKind { .. } => "unsupported-graph-effect-node-kind",
            Self::MissingTarget { .. } => "missing-graph-effect-target",
            Self::InvalidNodeStatus { .. } => "invalid-graph-effect-node-status",
            Self::InvalidEffect { .. } => "invalid-graph-effect",
            Self::Outbox { error } => error.code(),
        }
    }
}

impl Display for AgentGraphEffectBridgeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCompiledPlan {
                validation_code,
                reason,
            } => write!(
                f,
                "compiled graph plan failed validation ({validation_code}): {reason}"
            ),
            Self::PlanStateMismatch { field, reason } => {
                write!(
                    f,
                    "graph state field `{field}` does not match plan: {reason}"
                )
            }
            Self::RunInboxMismatch {
                expected_workflow_id,
                actual_workflow_id,
            } => write!(
                f,
                "effect request targets workflow `{expected_workflow_id}` but inbox is `{actual_workflow_id}`"
            ),
            Self::TerminalGraph { status } => {
                write!(
                    f,
                    "graph is already terminal with status `{}`",
                    status.as_label()
                )
            }
            Self::UnknownNode { node_id } => {
                write!(f, "compiled graph does not contain node `{node_id}`")
            }
            Self::MissingNodeState { node_id } => {
                write!(f, "graph state is missing node `{node_id}`")
            }
            Self::UnsupportedNodeKind { node_id, kind } => write!(
                f,
                "node `{node_id}` with kind `{}` cannot be mapped to a durable outbox effect",
                kind.as_label()
            ),
            Self::MissingTarget { node_id, kind } => write!(
                f,
                "node `{node_id}` with kind `{}` is missing a logical target",
                kind.as_label()
            ),
            Self::InvalidNodeStatus { node_id, status } => write!(
                f,
                "node `{node_id}` cannot schedule an effect while `{}`",
                status.as_label()
            ),
            Self::InvalidEffect { error } => write!(f, "mapped graph effect is invalid: {error}"),
            Self::Outbox { error } => write!(f, "graph effect outbox scheduling failed: {error}"),
        }
    }
}

impl Error for AgentGraphEffectBridgeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidEffect { error } => Some(error),
            Self::Outbox { error } => Some(error),
            Self::InvalidCompiledPlan { .. }
            | Self::PlanStateMismatch { .. }
            | Self::RunInboxMismatch { .. }
            | Self::TerminalGraph { .. }
            | Self::UnknownNode { .. }
            | Self::MissingNodeState { .. }
            | Self::UnsupportedNodeKind { .. }
            | Self::MissingTarget { .. }
            | Self::InvalidNodeStatus { .. } => None,
        }
    }
}

impl From<AgentFacadeError> for AgentGraphEffectBridgeError {
    fn from(error: AgentFacadeError) -> Self {
        Self::InvalidEffect { error }
    }
}

impl From<AgentOutboxError> for AgentGraphEffectBridgeError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox { error }
    }
}

/// Request metadata used to map one running graph node to a durable effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGraphEffectScheduleRequest {
    /// Durable run id used in the effect identity and inbox check.
    pub run_id: AgentRunId,
    /// Compiled node id being scheduled.
    pub node_id: AgentCompiledNodeId,
    /// Optional deterministic loop instance id.
    pub loop_instance_id: Option<String>,
    /// Optional out-of-line request payload.
    pub payload_ref: Option<ArtifactRef>,
    /// First due timestamp for dispatch.
    pub due_at: Option<AgentTimestampMillis>,
    /// Timeout in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Expected result type name.
    pub expected_result_type: Option<String>,
    /// Effect creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Command or event that caused this effect.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related commands, effects, logs, and audit events.
    pub correlation_id: AgentCorrelationId,
    /// Trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
}

impl AgentGraphEffectScheduleRequest {
    /// Creates a graph effect schedule request.
    #[must_use]
    pub fn new(
        run_id: AgentRunId,
        node_id: impl Into<AgentCompiledNodeId>,
        created_at: AgentTimestampMillis,
        causation_id: AgentCausationId,
        correlation_id: AgentCorrelationId,
    ) -> Self {
        Self {
            run_id,
            node_id: node_id.into(),
            loop_instance_id: None,
            payload_ref: None,
            due_at: None,
            timeout_ms: None,
            expected_result_type: None,
            created_at,
            causation_id,
            correlation_id,
            telemetry_context: AgentTelemetryContext::default(),
        }
    }

    /// Sets a deterministic loop instance id.
    #[must_use]
    pub fn loop_instance_id(mut self, loop_instance_id: impl Into<String>) -> Self {
        self.loop_instance_id = Some(loop_instance_id.into());
        self
    }

    /// Sets an out-of-line request payload reference.
    #[must_use]
    pub fn payload_ref(mut self, payload_ref: ArtifactRef) -> Self {
        self.payload_ref = Some(payload_ref);
        self
    }

    /// Sets the first due timestamp for dispatch.
    #[must_use]
    pub const fn due_at(mut self, due_at: AgentTimestampMillis) -> Self {
        self.due_at = Some(due_at);
        self
    }

    /// Sets the effect timeout in milliseconds.
    #[must_use]
    pub const fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Sets the expected result type name.
    #[must_use]
    pub fn expected_result_type(mut self, expected_result_type: impl Into<String>) -> Self {
        self.expected_result_type = Some(expected_result_type.into());
        self
    }

    /// Sets trace, baggage, and span-link context.
    #[must_use]
    pub fn telemetry_context(mut self, telemetry_context: AgentTelemetryContext) -> Self {
        self.telemetry_context = telemetry_context;
        self
    }
}

/// Result of scheduling one graph node effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentGraphEffectScheduleOutcome {
    /// Mapped first-class effect.
    pub effect: AgentEffect,
    /// Durable outbox acceptance result.
    pub acceptance: AgentOutboxAcceptance,
    /// Graph state transition after the durable outbox boundary succeeded.
    pub transition: AgentGraphSchedulerTransition,
}

/// Bridge from compiled graph effect nodes to durable agent outbox effects.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentGraphEffectBridge;

impl AgentGraphEffectBridge {
    /// Creates a graph effect bridge.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Maps a compiled graph node to a first-class scheduled effect.
    pub fn effect_from_node(
        &self,
        plan: &AgentCompiledExecutionPlan,
        request: &AgentGraphEffectScheduleRequest,
    ) -> AgentGraphEffectBridgeResult<AgentEffect> {
        validate_plan(plan)?;
        let node = ensure_known_node(plan, &request.node_id)?;
        effect_from_node(plan, node, request)
    }

    /// Schedules one running graph node as a durable outbox effect.
    ///
    /// The graph node is marked waiting only after
    /// [`AgentRunInbox::schedule_effect`] returns a durable scheduled or
    /// duplicate acceptance.
    pub async fn schedule_node_effect<Store, Clock>(
        &self,
        plan: &AgentCompiledExecutionPlan,
        state: AgentGraphRunState,
        request: AgentGraphEffectScheduleRequest,
        inbox: &mut AgentRunInbox<Store, Clock>,
    ) -> AgentGraphEffectBridgeResult<AgentGraphEffectScheduleOutcome>
    where
        Store: DurableStateStore<WorkflowState>,
        Clock: WorkflowClock,
    {
        validate_plan_state(plan, &state)?;
        validate_inbox_matches_run(&request, inbox)?;
        let effect = self.effect_from_node(plan, &request)?;
        ensure_node_can_schedule_effect(&state, &request.node_id, &effect.effect_id)?;

        let acceptance = inbox.schedule_effect(effect.clone()).await?;
        let transition = record_scheduled_effect(state, &request, &effect.effect_id)?;

        Ok(AgentGraphEffectScheduleOutcome {
            effect,
            acceptance,
            transition,
        })
    }
}

fn validate_plan(plan: &AgentCompiledExecutionPlan) -> AgentGraphEffectBridgeResult<()> {
    validate_compiled_execution_plan(plan).map_err(|error| {
        AgentGraphEffectBridgeError::InvalidCompiledPlan {
            validation_code: error.code(),
            reason: error.to_string(),
        }
    })
}

fn validate_plan_state(
    plan: &AgentCompiledExecutionPlan,
    state: &AgentGraphRunState,
) -> AgentGraphEffectBridgeResult<()> {
    validate_plan(plan)?;
    if state.plan_id != plan.plan_id {
        return Err(AgentGraphEffectBridgeError::PlanStateMismatch {
            field: "plan_id",
            reason: format!("state has {}, plan has {}", state.plan_id, plan.plan_id),
        });
    }
    if state.plan_fingerprint != plan.plan_fingerprint {
        return Err(AgentGraphEffectBridgeError::PlanStateMismatch {
            field: "plan_fingerprint",
            reason: format!(
                "state has {}, plan has {}",
                state.plan_fingerprint, plan.plan_fingerprint
            ),
        });
    }
    if let Some(status) = state.terminal_status {
        return Err(AgentGraphEffectBridgeError::TerminalGraph { status });
    }

    let plan_node_ids = plan
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    for node_id in &plan_node_ids {
        if !state.node_states.contains_key(node_id) {
            return Err(AgentGraphEffectBridgeError::MissingNodeState {
                node_id: node_id.clone(),
            });
        }
    }
    for node_id in state.node_states.keys() {
        if !plan_node_ids.contains(node_id) {
            return Err(AgentGraphEffectBridgeError::UnknownNode {
                node_id: node_id.clone(),
            });
        }
    }

    Ok(())
}

fn validate_inbox_matches_run<Store, Clock>(
    request: &AgentGraphEffectScheduleRequest,
    inbox: &AgentRunInbox<Store, Clock>,
) -> AgentGraphEffectBridgeResult<()>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    let expected_workflow_id = agent_run_workflow_id(&request.run_id);
    if inbox.workflow_id() != &expected_workflow_id {
        return Err(AgentGraphEffectBridgeError::RunInboxMismatch {
            expected_workflow_id: expected_workflow_id.as_str().to_string(),
            actual_workflow_id: inbox.workflow_id().as_str().to_string(),
        });
    }
    Ok(())
}

fn ensure_known_node<'a>(
    plan: &'a AgentCompiledExecutionPlan,
    node_id: &AgentCompiledNodeId,
) -> AgentGraphEffectBridgeResult<&'a AgentCompiledPlanNode> {
    plan.nodes
        .iter()
        .find(|node| node.node_id == *node_id)
        .ok_or_else(|| AgentGraphEffectBridgeError::UnknownNode {
            node_id: node_id.clone(),
        })
}

fn ensure_node_can_schedule_effect(
    state: &AgentGraphRunState,
    node_id: &AgentCompiledNodeId,
    effect_id: &AgentEffectId,
) -> AgentGraphEffectBridgeResult<()> {
    let node_state = state.node_states.get(node_id).ok_or_else(|| {
        AgentGraphEffectBridgeError::MissingNodeState {
            node_id: node_id.clone(),
        }
    })?;
    if node_state.status == AgentGraphNodeStatus::Running {
        return Ok(());
    }
    if node_state.status == AgentGraphNodeStatus::Waiting
        && node_state.wait_reason == Some(AgentGraphWaitReason::Effect)
        && node_state
            .scheduled_effect_ids
            .iter()
            .any(|scheduled| scheduled == effect_id)
    {
        return Ok(());
    }

    Err(AgentGraphEffectBridgeError::InvalidNodeStatus {
        node_id: node_id.clone(),
        status: node_state.status,
    })
}

fn effect_from_node(
    plan: &AgentCompiledExecutionPlan,
    node: &AgentCompiledPlanNode,
    request: &AgentGraphEffectScheduleRequest,
) -> AgentGraphEffectBridgeResult<AgentEffect> {
    let kind = effect_kind_for_node(node)?;
    let target = effect_target_for_node(plan, node, request)?;
    let target_class = target_class_from_attributes(&target.attributes, node.kind);
    let identity = effect_identity(plan, node, request, kind, &target_class);
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("graph-effect-dedupe:{identity}")),
        request.causation_id.clone(),
        request.correlation_id.clone(),
    )
    .telemetry_context(request.telemetry_context.clone());
    let mut metadata = AgentEffectMetadata::new(
        AgentEffectId::new(format!("graph-effect:{identity}")),
        durability,
        AgentIdempotencyKey::new(format!("graph-effect-idempotency:{identity}")),
        request.created_at,
    )?;
    if let Some(due_at) = request.due_at {
        metadata = metadata.due_at(due_at);
    }
    if let Some(timeout_ms) = request.timeout_ms {
        metadata = metadata.timeout_ms(timeout_ms);
    }

    let mut schedule = AgentEffectSchedule::new(kind, target, metadata)?;
    if let Some(payload_ref) = &request.payload_ref {
        schedule = schedule.payload_ref(payload_ref.clone());
    }
    if let Some(expected_result_type) = &request.expected_result_type {
        schedule = schedule.expected_result_type(expected_result_type.clone())?;
    }

    Ok(schedule.into_effect()?)
}

fn effect_kind_for_node(
    node: &AgentCompiledPlanNode,
) -> AgentGraphEffectBridgeResult<AgentEffectKind> {
    match node.kind {
        AgentCompiledNodeKind::ModelCall => Ok(AgentEffectKind::ModelCall),
        AgentCompiledNodeKind::ToolCall => Ok(AgentEffectKind::ToolCall),
        AgentCompiledNodeKind::ProcessCall => Ok(AgentEffectKind::ProcessCall),
        AgentCompiledNodeKind::HttpCall => Ok(AgentEffectKind::HttpCall),
        AgentCompiledNodeKind::GrpcCall => Ok(AgentEffectKind::GrpcCall),
        AgentCompiledNodeKind::StreamPublish => Ok(AgentEffectKind::StreamPublish),
        AgentCompiledNodeKind::ArtifactWrite => Ok(AgentEffectKind::ArtifactWrite),
        AgentCompiledNodeKind::ChildWorkflowCommand => Ok(AgentEffectKind::ChildWorkflowCommand),
        AgentCompiledNodeKind::Notification => Ok(AgentEffectKind::Notification),
        AgentCompiledNodeKind::AuditEvent => Ok(AgentEffectKind::AuditEvent),
        AgentCompiledNodeKind::Input
        | AgentCompiledNodeKind::Transform
        | AgentCompiledNodeKind::Branch
        | AgentCompiledNodeKind::Join
        | AgentCompiledNodeKind::Iterator
        | AgentCompiledNodeKind::HumanCheckpoint
        | AgentCompiledNodeKind::TimerWait
        | AgentCompiledNodeKind::Terminal => {
            Err(AgentGraphEffectBridgeError::UnsupportedNodeKind {
                node_id: node.node_id.clone(),
                kind: node.kind,
            })
        }
    }
}

fn effect_target_for_node(
    plan: &AgentCompiledExecutionPlan,
    node: &AgentCompiledPlanNode,
    request: &AgentGraphEffectScheduleRequest,
) -> AgentGraphEffectBridgeResult<AgentEffectTarget> {
    let target =
        node.target
            .as_ref()
            .ok_or_else(|| AgentGraphEffectBridgeError::MissingTarget {
                node_id: node.node_id.clone(),
                kind: node.kind,
            })?;
    let target_class = target_class(target, node.kind);
    let mut attributes = target.attributes.clone();
    attributes
        .entry(ATTR_TARGET_CLASS.to_string())
        .or_insert(target_class);
    attributes.insert(ATTR_NODE_KIND.to_string(), node.kind.as_label().to_string());
    attributes.insert(
        ATTR_COMPILED_NODE_ID.to_string(),
        node.node_id.as_str().to_string(),
    );
    attributes.insert(
        ATTR_COMPILED_PLAN_FINGERPRINT.to_string(),
        plan.plan_fingerprint.as_str().to_string(),
    );
    attributes.insert(
        ATTR_LOOP_INSTANCE_ID.to_string(),
        loop_instance_key(request).to_string(),
    );
    if let Some(binding_ref) = &node.credential_binding_ref {
        attributes.insert(
            AGENT_CREDENTIAL_BINDING_REF_ATTRIBUTE.to_string(),
            binding_ref.as_str().to_string(),
        );
    }

    Ok(AgentEffectTarget {
        target_type: target.target_type.clone(),
        name: target.name.clone(),
        address: target.address.clone(),
        attributes,
    })
}

fn target_class(target: &AgentCompiledNodeTarget, kind: AgentCompiledNodeKind) -> String {
    target
        .attributes
        .get(ATTR_TARGET_CLASS)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| format!("{}:{}", target.target_type, kind.as_label()))
}

fn target_class_from_attributes(
    attributes: &BTreeMap<String, String>,
    kind: AgentCompiledNodeKind,
) -> String {
    attributes
        .get(ATTR_TARGET_CLASS)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| kind.as_label().to_string())
}

fn effect_identity(
    plan: &AgentCompiledExecutionPlan,
    node: &AgentCompiledPlanNode,
    request: &AgentGraphEffectScheduleRequest,
    kind: AgentEffectKind,
    target_class: &str,
) -> String {
    format!(
        "run={};plan={};node={};loop={};kind={};target_class={}",
        request.run_id.as_str(),
        plan.plan_fingerprint.as_str(),
        node.node_id.as_str(),
        loop_instance_key(request),
        kind.as_label(),
        target_class
    )
}

fn loop_instance_key(request: &AgentGraphEffectScheduleRequest) -> &str {
    request
        .loop_instance_id
        .as_deref()
        .unwrap_or(ROOT_LOOP_INSTANCE_ID)
}

fn record_scheduled_effect(
    mut state: AgentGraphRunState,
    request: &AgentGraphEffectScheduleRequest,
    effect_id: &AgentEffectId,
) -> AgentGraphEffectBridgeResult<AgentGraphSchedulerTransition> {
    let node_state = state.node_states.get_mut(&request.node_id).ok_or_else(|| {
        AgentGraphEffectBridgeError::MissingNodeState {
            node_id: request.node_id.clone(),
        }
    })?;
    let mut changed = false;
    if !node_state
        .scheduled_effect_ids
        .iter()
        .any(|scheduled| scheduled == effect_id)
    {
        node_state.scheduled_effect_ids.push(effect_id.clone());
        changed = true;
    }
    if node_state.status != AgentGraphNodeStatus::Waiting
        || node_state.wait_reason != Some(AgentGraphWaitReason::Effect)
    {
        node_state.status = AgentGraphNodeStatus::Waiting;
        node_state.wait_reason = Some(AgentGraphWaitReason::Effect);
        node_state.updated_at = request.created_at;
        changed = true;
    }

    let changed_node_ids = if changed {
        state.scheduler_revision += 1;
        state.blocked_reason = None;
        vec![request.node_id.clone()]
    } else {
        Vec::new()
    };
    let runnable_node_ids = AgentGraphScheduler::new().runnable_nodes(&state);

    Ok(AgentGraphSchedulerTransition {
        state,
        changed_node_ids,
        runnable_node_ids,
    })
}
