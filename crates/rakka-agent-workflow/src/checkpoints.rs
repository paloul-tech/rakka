//! Durable human checkpoint facade for agent workflow runs.
//!
//! Human checkpoints are persisted in [`AgentRunState`].
//! This facade composes the run state machine, durable outbox approval request,
//! and durable inbox decision command so a run can wait without keeping a live
//! task, actor, or pod-local worker active.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::DurableStateStore;
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState};
use serde::{Deserialize, Serialize};

use crate::{
    validate_command, AgentAttributes, AgentCommand, AgentCommandKind, AgentCommandMetadata,
    AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectTarget, AgentFacadeError, AgentIdempotencyKey,
    AgentInboxAcceptance, AgentInboxError, AgentOutboxAcceptance, AgentOutboxError,
    AgentRunEngineError, AgentRunId, AgentRunInbox, AgentRunState, AgentRunTransition,
    AgentStepRunner, AgentTimestampMillis, AgentWorkflow, ArtifactRef, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, PrincipalRef,
};

/// Counter for human checkpoint open, decision, escalation, and timeout events.
pub const METRIC_AGENT_HUMAN_CHECKPOINTS: &str = "rakka.agent_workflow.human.checkpoints";

/// Histogram for human checkpoint wait latency in milliseconds.
pub const METRIC_AGENT_HUMAN_WAIT_LATENCY_MS: &str = "rakka.agent_workflow.human.wait.latency_ms";

/// Default HTTP path for human decision submissions.
#[cfg(feature = "http")]
pub const DEFAULT_HUMAN_DECISION_HTTP_PATH: &str = "/agent-workflows/human-checkpoints/decisions";

/// Shared result type for human checkpoint operations.
pub type AgentHumanCheckpointResult<T> = Result<T, AgentHumanCheckpointError>;

/// Human checkpoint facade failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentHumanCheckpointError {
    /// Checkpoint shape failed validation.
    InvalidCheckpoint {
        /// Checkpoint id.
        checkpoint_id: HumanCheckpointId,
        /// Invalid field.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
    /// Decision submission failed validation.
    InvalidDecision {
        /// Checkpoint id.
        checkpoint_id: HumanCheckpointId,
        /// Invalid field.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
    /// A run has no durable state.
    MissingRunState {
        /// Run id.
        run_id: AgentRunId,
    },
    /// Requested checkpoint was not found in the run state.
    CheckpointNotFound {
        /// Checkpoint id.
        checkpoint_id: HumanCheckpointId,
    },
    /// Command or effect construction failed.
    Facade {
        /// Facade validation error.
        error: AgentFacadeError,
    },
    /// Durable inbox operation failed.
    Inbox {
        /// Inbox failure.
        error: AgentInboxError,
    },
    /// Durable outbox operation failed.
    Outbox {
        /// Outbox failure.
        error: AgentOutboxError,
    },
    /// Durable run state-machine operation failed.
    RunEngine {
        /// Run-engine failure.
        error: AgentRunEngineError,
    },
}

impl AgentHumanCheckpointError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidCheckpoint { .. } => "invalid-human-checkpoint",
            Self::InvalidDecision { .. } => "invalid-human-decision",
            Self::MissingRunState { .. } => "missing-run-state",
            Self::CheckpointNotFound { .. } => "checkpoint-not-found",
            Self::Facade { error } => facade_error_code(error),
            Self::Inbox { error } => error.code(),
            Self::Outbox { error } => error.code(),
            Self::RunEngine { error } => error.code(),
        }
    }
}

impl Display for AgentHumanCheckpointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCheckpoint {
                checkpoint_id,
                field,
                reason,
            } => write!(
                f,
                "human checkpoint {checkpoint_id} has invalid {field}: {reason}"
            ),
            Self::InvalidDecision {
                checkpoint_id,
                field,
                reason,
            } => write!(
                f,
                "human decision for checkpoint {checkpoint_id} has invalid {field}: {reason}"
            ),
            Self::MissingRunState { run_id } => {
                write!(f, "agent run {run_id} has no durable state")
            }
            Self::CheckpointNotFound { checkpoint_id } => {
                write!(f, "human checkpoint {checkpoint_id} was not found")
            }
            Self::Facade { error } => Display::fmt(error, f),
            Self::Inbox { error } => Display::fmt(error, f),
            Self::Outbox { error } => Display::fmt(error, f),
            Self::RunEngine { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentHumanCheckpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Facade { error } => Some(error),
            Self::Inbox { error } => Some(error),
            Self::Outbox { error } => Some(error),
            Self::RunEngine { error } => Some(error),
            Self::InvalidCheckpoint { .. }
            | Self::InvalidDecision { .. }
            | Self::MissingRunState { .. }
            | Self::CheckpointNotFound { .. } => None,
        }
    }
}

impl From<AgentFacadeError> for AgentHumanCheckpointError {
    fn from(error: AgentFacadeError) -> Self {
        Self::Facade { error }
    }
}

impl From<AgentInboxError> for AgentHumanCheckpointError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox { error }
    }
}

impl From<AgentOutboxError> for AgentHumanCheckpointError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox { error }
    }
}

impl From<AgentRunEngineError> for AgentHumanCheckpointError {
    fn from(error: AgentRunEngineError) -> Self {
        Self::RunEngine { error }
    }
}

/// Request to open a human checkpoint and notify a human-facing target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHumanApprovalRequest {
    /// Checkpoint to persist into the run state.
    pub checkpoint: HumanCheckpoint,
    /// Effect id for the approval request outbox entry.
    pub effect_id: AgentEffectId,
    /// Durable outbox and telemetry metadata.
    pub durability: AgentDurabilityMetadata,
    /// Idempotency key supplied to the human-facing target.
    pub idempotency_key: AgentIdempotencyKey,
    /// Target that should receive the approval request.
    pub target: AgentEffectTarget,
    /// Optional approval payload artifact.
    pub payload_ref: Option<ArtifactRef>,
    /// Expected decision result type.
    pub expected_result_type: Option<String>,
    /// Optional first dispatch timestamp for the approval request.
    pub dispatch_at: Option<AgentTimestampMillis>,
    /// Optional target timeout in milliseconds.
    pub timeout_ms: Option<u64>,
}

impl AgentHumanApprovalRequest {
    /// Creates an approval request and validates checkpoint metadata.
    pub fn new(
        checkpoint: HumanCheckpoint,
        effect_id: AgentEffectId,
        durability: AgentDurabilityMetadata,
        idempotency_key: AgentIdempotencyKey,
        target: AgentEffectTarget,
    ) -> AgentHumanCheckpointResult<Self> {
        validate_checkpoint(&checkpoint)?;
        let request = Self {
            checkpoint,
            effect_id,
            durability,
            idempotency_key,
            target,
            payload_ref: None,
            expected_result_type: Some("HumanDecision".to_string()),
            dispatch_at: None,
            timeout_ms: None,
        };
        validate_human_target(&request.target, &request.checkpoint.checkpoint_id)?;
        Ok(request)
    }

    /// Sets an approval payload artifact reference.
    #[must_use]
    pub fn payload_ref(mut self, payload_ref: ArtifactRef) -> Self {
        self.payload_ref = Some(payload_ref);
        self
    }

    /// Sets the expected decision result type.
    pub fn expected_result_type(
        mut self,
        expected_result_type: impl Into<String>,
    ) -> AgentHumanCheckpointResult<Self> {
        let expected_result_type = expected_result_type.into();
        require_checkpoint(
            &self.checkpoint.checkpoint_id,
            &expected_result_type,
            "expected_result_type",
        )?;
        self.expected_result_type = Some(expected_result_type);
        Ok(self)
    }

    /// Sets first dispatch timestamp for the approval request.
    #[must_use]
    pub const fn dispatch_at(mut self, dispatch_at: AgentTimestampMillis) -> Self {
        self.dispatch_at = Some(dispatch_at);
        self
    }

    /// Sets target timeout in milliseconds.
    #[must_use]
    pub const fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Builds the `HumanApprovalRequest` outbox effect.
    pub fn approval_effect(&self) -> AgentHumanCheckpointResult<AgentEffect> {
        validate_checkpoint(&self.checkpoint)?;
        validate_human_target(&self.target, &self.checkpoint.checkpoint_id)?;

        let mut metadata = AgentEffectMetadata::new(
            self.effect_id.clone(),
            self.durability.clone(),
            self.idempotency_key.clone(),
            self.checkpoint.created_at,
        )?;
        if let Some(dispatch_at) = self.dispatch_at {
            metadata = metadata.due_at(dispatch_at);
        }
        if let Some(timeout_ms) = self.timeout_ms {
            metadata = metadata.timeout_ms(timeout_ms);
        }

        let mut schedule = AgentEffectSchedule::new(
            AgentEffectKind::HumanApprovalRequest,
            self.target.clone(),
            metadata,
        )?;
        if let Some(payload_ref) = &self.payload_ref {
            schedule = schedule.payload_ref(payload_ref.clone());
        }
        if let Some(expected_result_type) = &self.expected_result_type {
            schedule = schedule.expected_result_type(expected_result_type.clone())?;
        }
        Ok(schedule.into_effect()?)
    }
}

/// Public decision submission accepted through HTTP, gRPC, or another ingress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHumanDecisionSubmission {
    /// Required command metadata.
    pub metadata: AgentCommandMetadata,
    /// Checkpoint being resolved.
    pub checkpoint_id: HumanCheckpointId,
    /// Stable decision value selected by the human.
    pub decision: String,
    /// Checkpoint status to persist when the decision is accepted.
    pub resolved_status: HumanCheckpointStatus,
    /// Optional decision payload or comment artifact.
    pub payload_ref: Option<ArtifactRef>,
    /// Bounded command attributes.
    pub attributes: AgentAttributes,
}

impl AgentHumanDecisionSubmission {
    /// Creates a decision submission.
    #[must_use]
    pub fn new(
        metadata: AgentCommandMetadata,
        checkpoint_id: HumanCheckpointId,
        decision: impl Into<String>,
        resolved_status: HumanCheckpointStatus,
    ) -> Self {
        Self {
            metadata,
            checkpoint_id,
            decision: decision.into(),
            resolved_status,
            payload_ref: None,
            attributes: AgentAttributes::new(),
        }
    }

    /// Sets an out-of-line decision payload or comment artifact.
    #[must_use]
    pub fn payload_ref(mut self, payload_ref: ArtifactRef) -> Self {
        self.payload_ref = Some(payload_ref);
        self
    }

    /// Adds a bounded command attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Builds a `HumanDecisionSubmitted` command from a public submission.
pub fn human_decision_command(
    submission: &AgentHumanDecisionSubmission,
) -> AgentHumanCheckpointResult<AgentCommand> {
    validate_decision_submission_shape(submission)?;
    let mut command = AgentCommand::new(
        AgentCommandKind::HumanDecisionSubmitted {
            checkpoint_id: submission.checkpoint_id.clone(),
            decision: submission.decision.clone(),
        },
        submission.metadata.clone(),
    )?
    .attribute("decision_status", submission.resolved_status.as_label())?;
    for (key, value) in &submission.attributes {
        command = command.attribute(key.clone(), value.clone())?;
    }
    if let Some(payload_ref) = &submission.payload_ref {
        command = command.payload_ref(payload_ref.clone());
    }
    validate_command(&command)?;
    Ok(command)
}

/// Result of opening a human checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHumanCheckpointOpening {
    /// Checkpoint persisted into the run state.
    pub checkpoint: HumanCheckpoint,
    /// Approval outbox effect that was scheduled.
    pub approval_effect: AgentEffect,
    /// Durable run transition into `waiting-for-human`.
    pub transition: AgentRunTransition,
    /// Durable outbox scheduling result.
    pub outbox_acceptance: AgentOutboxAcceptance,
}

/// Result of submitting a human decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHumanDecisionResult {
    /// Durable inbox acceptance result.
    pub inbox_acceptance: AgentInboxAcceptance,
    /// Resume transition when this was a newly accepted decision.
    pub transition: Option<AgentRunTransition>,
    /// Resolved checkpoint when a transition was applied.
    pub checkpoint: Option<HumanCheckpoint>,
}

/// Runtime facade for human checkpoint operations on one run.
pub struct AgentHumanCheckpointRuntime<RunStore, WorkflowStore, Clock = SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    workflow: AgentWorkflow,
    run_id: AgentRunId,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    clock: Clock,
    metrics: Arc<dyn MetricsRecorder>,
}

impl<RunStore, WorkflowStore> AgentHumanCheckpointRuntime<RunStore, WorkflowStore>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    /// Creates a checkpoint runtime with the system clock and no-op metrics.
    #[must_use]
    pub fn new(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
    ) -> Self {
        Self::with_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            Arc::new(NoopMetricsRecorder),
        )
    }

    /// Creates a checkpoint runtime with the system clock and explicit metrics.
    #[must_use]
    pub fn with_metrics(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::with_clock_and_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            SystemWorkflowClock,
            metrics,
        )
    }
}

impl<RunStore, WorkflowStore, Clock> Clone
    for AgentHumanCheckpointRuntime<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    fn clone(&self) -> Self {
        Self {
            workflow: self.workflow.clone(),
            run_id: self.run_id.clone(),
            run_store: self.run_store.clone(),
            workflow_store: self.workflow_store.clone(),
            clock: self.clock.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<RunStore, WorkflowStore, Clock> AgentHumanCheckpointRuntime<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates a checkpoint runtime with explicit dependencies.
    #[must_use]
    pub fn with_clock_and_metrics(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        clock: Clock,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            workflow,
            run_id,
            run_store,
            workflow_store,
            clock,
            metrics,
        }
    }

    /// Opens a checkpoint, persists the run wait, and schedules the approval
    /// request through the durable outbox.
    pub async fn open_checkpoint(
        &mut self,
        request: AgentHumanApprovalRequest,
    ) -> AgentHumanCheckpointResult<AgentHumanCheckpointOpening> {
        validate_checkpoint(&request.checkpoint)?;
        let approval_effect = request.approval_effect()?;
        let now = current_agent_timestamp(&self.clock);

        let mut runner = self.runner();
        runner.recover().await?;
        let transition = runner
            .open_human_checkpoint(request.checkpoint.clone(), now)
            .await?;

        let mut inbox = self.inbox();
        inbox.recover().await?;
        let outbox_acceptance = inbox.schedule_effect(approval_effect.clone()).await?;

        self.record_metric("open", "scheduled", "none");
        Ok(AgentHumanCheckpointOpening {
            checkpoint: request.checkpoint,
            approval_effect,
            transition,
            outbox_acceptance,
        })
    }

    /// Accepts a human decision through the durable inbox and resumes the run
    /// when the decision is newly accepted.
    pub async fn submit_decision(
        &mut self,
        submission: AgentHumanDecisionSubmission,
    ) -> AgentHumanCheckpointResult<AgentHumanDecisionResult> {
        let command = human_decision_command(&submission)?;
        let mut inbox = self.inbox();
        inbox.recover().await?;
        let inbox_acceptance = inbox.accept_command(command).await?;
        if inbox_acceptance.is_duplicate() {
            self.record_metric("decision", "duplicate", "inbox");
            return Ok(AgentHumanDecisionResult {
                inbox_acceptance,
                transition: None,
                checkpoint: None,
            });
        }

        let now = current_agent_timestamp(&self.clock);
        let mut runner = self.runner();
        let state = runner.recover().await?.cloned().ok_or_else(|| {
            AgentHumanCheckpointError::MissingRunState {
                run_id: self.run_id.clone(),
            }
        })?;
        let checkpoint = state
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.checkpoint_id == submission.checkpoint_id)
            .cloned()
            .ok_or_else(|| AgentHumanCheckpointError::CheckpointNotFound {
                checkpoint_id: submission.checkpoint_id.clone(),
            })?;
        validate_decision_for_checkpoint(&submission, &checkpoint)?;

        let transition = runner
            .resolve_human_checkpoint(
                &submission.checkpoint_id,
                submission.resolved_status,
                submission.metadata.principal.clone(),
                now,
            )
            .await?;
        let resolved_checkpoint = transition
            .state
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.checkpoint_id == submission.checkpoint_id)
            .cloned();
        if let Some(checkpoint) = &resolved_checkpoint {
            self.record_wait_latency(checkpoint);
        }
        self.record_metric(
            "decision",
            submission.resolved_status.as_label(),
            "accepted",
        );
        Ok(AgentHumanDecisionResult {
            inbox_acceptance,
            transition: Some(transition),
            checkpoint: resolved_checkpoint,
        })
    }

    /// Returns open checkpoints whose due timestamp is at or before `now`.
    pub async fn overdue_checkpoints(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentHumanCheckpointResult<Vec<HumanCheckpoint>> {
        let mut runner = self.runner();
        let state = runner.recover().await?.cloned().ok_or_else(|| {
            AgentHumanCheckpointError::MissingRunState {
                run_id: self.run_id.clone(),
            }
        })?;
        let overdue = state
            .checkpoints
            .into_iter()
            .filter(|checkpoint| {
                checkpoint.status == HumanCheckpointStatus::Open
                    && checkpoint.due_at.is_some_and(|due_at| due_at <= now)
            })
            .collect();
        Ok(overdue)
    }

    /// Marks the pending checkpoint escalated while the run remains waiting.
    pub async fn escalate_checkpoint(
        &mut self,
        checkpoint_id: &HumanCheckpointId,
    ) -> AgentHumanCheckpointResult<AgentRunTransition> {
        let now = current_agent_timestamp(&self.clock);
        let mut runner = self.runner();
        runner.recover().await?;
        let transition = runner.escalate_human_checkpoint(checkpoint_id, now).await?;
        self.record_metric("escalate", "escalated", "none");
        Ok(transition)
    }

    fn runner(&self) -> AgentStepRunner<RunStore> {
        AgentStepRunner::new(
            self.workflow.clone(),
            self.run_id.clone(),
            self.run_store.clone(),
        )
    }

    fn inbox(&self) -> AgentRunInbox<WorkflowStore, Clock> {
        AgentRunInbox::with_clock_and_metrics(
            self.run_id.clone(),
            self.workflow_store.clone(),
            self.clock.clone(),
            self.metrics.clone(),
        )
    }

    fn record_metric(&self, operation: &'static str, outcome: &'static str, detail: &'static str) {
        self.metrics.increment_counter(
            METRIC_AGENT_HUMAN_CHECKPOINTS,
            1,
            &[
                ("operation", operation),
                ("outcome", outcome),
                ("detail", detail),
            ],
        );
    }

    fn record_wait_latency(&self, checkpoint: &HumanCheckpoint) {
        if let Some(resolved_at) = checkpoint.resolved_at {
            let latency_ms = resolved_at
                .as_millis()
                .saturating_sub(checkpoint.created_at.as_millis());
            self.metrics.record_histogram(
                METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
                latency_ms as f64,
                &[("status", checkpoint.status.as_label())],
            );
        }
    }
}

/// HTTP response for decision submission routes.
#[cfg(feature = "http")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentHumanDecisionHttpResponse {
    /// True when the durable inbox accepted a new command.
    pub accepted: bool,
    /// True when the durable inbox detected a duplicate submission.
    pub duplicate: bool,
    /// True when the run was resumed by this request.
    pub resumed: bool,
    /// Resolved checkpoint status, when available.
    pub checkpoint_status: Option<String>,
}

#[cfg(feature = "http")]
impl From<AgentHumanDecisionResult> for AgentHumanDecisionHttpResponse {
    fn from(result: AgentHumanDecisionResult) -> Self {
        Self {
            accepted: result.inbox_acceptance.is_accepted(),
            duplicate: result.inbox_acceptance.is_duplicate(),
            resumed: result.transition.is_some(),
            checkpoint_status: result
                .checkpoint
                .map(|checkpoint| checkpoint.status.as_label().to_string()),
        }
    }
}

/// Creates a JSON HTTP route for public human decision submission.
#[cfg(feature = "http")]
pub fn human_decision_http_route<RunStore, WorkflowStore, Clock>(
    path: &'static str,
    config: rakka_http::HttpRouteConfig,
    runtime: AgentHumanCheckpointRuntime<RunStore, WorkflowStore, Clock>,
) -> rakka_http::HttpRouter
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    rakka_http::json_service_route::<
        AgentHumanDecisionSubmission,
        AgentHumanDecisionHttpResponse,
        _,
        _,
    >(path, config, move |submission| {
        let mut runtime = runtime.clone();
        async move {
            runtime
                .submit_decision(submission)
                .await
                .map(AgentHumanDecisionHttpResponse::from)
                .map_err(|error| rakka_http::HttpError::service(error.to_string()))
        }
    })
}

fn current_agent_timestamp(clock: &impl WorkflowClock) -> AgentTimestampMillis {
    AgentTimestampMillis::new(clock.now().as_millis())
}

fn validate_checkpoint(checkpoint: &HumanCheckpoint) -> AgentHumanCheckpointResult<()> {
    require_checkpoint(
        &checkpoint.checkpoint_id,
        checkpoint.checkpoint_id.as_str(),
        "checkpoint_id",
    )?;
    require_checkpoint(&checkpoint.checkpoint_id, &checkpoint.summary, "summary")?;
    if checkpoint.status != HumanCheckpointStatus::Open {
        return Err(AgentHumanCheckpointError::InvalidCheckpoint {
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            field: "status",
            reason: "must be open",
        });
    }
    if checkpoint.available_decisions.is_empty() {
        return Err(AgentHumanCheckpointError::InvalidCheckpoint {
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            field: "available_decisions",
            reason: "must include at least one decision",
        });
    }
    for decision in &checkpoint.available_decisions {
        validate_decision_option(&checkpoint.checkpoint_id, decision)?;
    }
    for role in &checkpoint.required_roles {
        require_checkpoint(&checkpoint.checkpoint_id, role, "required_roles")?;
    }
    if let Some(escalation_target) = &checkpoint.escalation_target {
        require_checkpoint(
            &checkpoint.checkpoint_id,
            escalation_target,
            "escalation_target",
        )?;
    }
    if let Some(created_by) = &checkpoint.created_by {
        validate_principal(&checkpoint.checkpoint_id, created_by, "created_by")?;
    }
    Ok(())
}

fn validate_decision_option(
    checkpoint_id: &HumanCheckpointId,
    decision: &HumanDecisionOption,
) -> AgentHumanCheckpointResult<()> {
    require_checkpoint(checkpoint_id, &decision.value, "available_decisions.value")?;
    require_checkpoint(checkpoint_id, &decision.label, "available_decisions.label")
}

fn validate_human_target(
    target: &AgentEffectTarget,
    checkpoint_id: &HumanCheckpointId,
) -> AgentHumanCheckpointResult<()> {
    require_checkpoint(checkpoint_id, &target.target_type, "target.target_type")?;
    require_checkpoint(checkpoint_id, &target.name, "target.name")
}

fn validate_principal(
    checkpoint_id: &HumanCheckpointId,
    principal: &PrincipalRef,
    field: &'static str,
) -> AgentHumanCheckpointResult<()> {
    require_checkpoint(checkpoint_id, &principal.principal_type, field)?;
    require_checkpoint(checkpoint_id, &principal.principal_id, field)
}

fn validate_decision_submission_shape(
    submission: &AgentHumanDecisionSubmission,
) -> AgentHumanCheckpointResult<()> {
    require_decision(
        &submission.checkpoint_id,
        submission.checkpoint_id.as_str(),
        "checkpoint_id",
    )?;
    require_decision(&submission.checkpoint_id, &submission.decision, "decision")?;
    if matches!(
        submission.resolved_status,
        HumanCheckpointStatus::Open
            | HumanCheckpointStatus::Escalated
            | HumanCheckpointStatus::TimedOut
    ) {
        return Err(AgentHumanCheckpointError::InvalidDecision {
            checkpoint_id: submission.checkpoint_id.clone(),
            field: "resolved_status",
            reason: "must be approved, rejected, edited, or cancelled",
        });
    }
    for key in submission.attributes.keys() {
        require_decision(&submission.checkpoint_id, key, "attributes.key")?;
    }
    Ok(())
}

fn validate_decision_for_checkpoint(
    submission: &AgentHumanDecisionSubmission,
    checkpoint: &HumanCheckpoint,
) -> AgentHumanCheckpointResult<()> {
    if checkpoint.status != HumanCheckpointStatus::Open
        && checkpoint.status != HumanCheckpointStatus::Escalated
    {
        return Err(AgentHumanCheckpointError::InvalidDecision {
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            field: "checkpoint.status",
            reason: "checkpoint is not open",
        });
    }
    let Some(option) = checkpoint
        .available_decisions
        .iter()
        .find(|option| option.value == submission.decision)
    else {
        return Err(AgentHumanCheckpointError::InvalidDecision {
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            field: "decision",
            reason: "decision is not allowed for checkpoint",
        });
    };
    if option.requires_comment && submission.payload_ref.is_none() {
        return Err(AgentHumanCheckpointError::InvalidDecision {
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            field: "payload_ref",
            reason: "decision requires a comment artifact",
        });
    }
    Ok(())
}

fn require_checkpoint(
    checkpoint_id: &HumanCheckpointId,
    value: &str,
    field: &'static str,
) -> AgentHumanCheckpointResult<()> {
    if value.trim().is_empty() {
        return Err(AgentHumanCheckpointError::InvalidCheckpoint {
            checkpoint_id: checkpoint_id.clone(),
            field,
            reason: "must not be empty",
        });
    }
    Ok(())
}

fn require_decision(
    checkpoint_id: &HumanCheckpointId,
    value: &str,
    field: &'static str,
) -> AgentHumanCheckpointResult<()> {
    if value.trim().is_empty() {
        return Err(AgentHumanCheckpointError::InvalidDecision {
            checkpoint_id: checkpoint_id.clone(),
            field,
            reason: "must not be empty",
        });
    }
    Ok(())
}

fn facade_error_code(error: &AgentFacadeError) -> &'static str {
    match error {
        AgentFacadeError::InvalidCommandMetadata { .. } => "invalid-command-metadata",
        AgentFacadeError::InvalidCommand { .. } => "invalid-command",
        AgentFacadeError::InvalidEffectMetadata { .. } => "invalid-effect-metadata",
        AgentFacadeError::InvalidEffect { .. } => "invalid-effect",
    }
}
