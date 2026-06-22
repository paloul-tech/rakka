//! Durable agent run step state machine.
//!
//! The runner in this slice owns durable `AgentRunState` transitions. It does
//! not dispatch effects or host an actor yet; later Phase 2 slices can compose
//! those capabilities on top of the persistence boundary established here.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision, StateRecord};

use crate::{
    AgentCancellation, AgentGraphRunState, AgentGraphTerminalStatus, AgentRunId, AgentRunState,
    AgentRunStatus, AgentStatePayload, AgentStep, AgentStepId, AgentTimestampMillis, AgentWorkflow,
    HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus, PrincipalRef,
};

/// Prefix used for durable agent-run state persistence ids.
pub const AGENT_RUN_PERSISTENCE_PREFIX: &str = "agent-run";

/// Creates the persistence id used to store one durable agent run state.
#[must_use]
pub fn agent_run_persistence_id(run_id: &AgentRunId) -> PersistenceId {
    PersistenceId::new(format!(
        "{AGENT_RUN_PERSISTENCE_PREFIX}:{}",
        run_id.as_str()
    ))
}

/// Shared result type for durable run engine operations.
pub type AgentRunEngineResult<T> = Result<T, AgentRunEngineError>;

/// State transition categories emitted by [`AgentStepRunner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRunTransitionKind {
    /// Initial accepted run state was persisted.
    Start,
    /// Current step execution began.
    BeginStep,
    /// Current step succeeded and moved to another step.
    StepSucceeded,
    /// Current step failed.
    StepFailed,
    /// Run is waiting for a durable timer.
    WaitForTimer,
    /// Run is waiting for a human decision.
    WaitForHuman,
    /// Run is waiting for an external effect result.
    WaitForEffect,
    /// A human checkpoint was escalated but remains waiting.
    CheckpointEscalated,
    /// Run resumed from a waiting status.
    Resume,
    /// Run completed successfully.
    Complete,
    /// Run failed outside a specific step failure.
    Fail,
    /// Run cancellation was requested.
    RequestCancellation,
    /// Run cancellation completed.
    Cancel,
    /// Run entered compensation.
    BeginCompensation,
    /// Compiled graph execution state changed.
    GraphUpdated,
}

impl AgentRunTransitionKind {
    /// Stable lowercase label for telemetry and error details.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::BeginStep => "begin-step",
            Self::StepSucceeded => "step-succeeded",
            Self::StepFailed => "step-failed",
            Self::WaitForTimer => "wait-for-timer",
            Self::WaitForHuman => "wait-for-human",
            Self::WaitForEffect => "wait-for-effect",
            Self::CheckpointEscalated => "checkpoint-escalated",
            Self::Resume => "resume",
            Self::Complete => "complete",
            Self::Fail => "fail",
            Self::RequestCancellation => "request-cancellation",
            Self::Cancel => "cancel",
            Self::BeginCompensation => "begin-compensation",
            Self::GraphUpdated => "graph-updated",
        }
    }
}

/// Reason a running agent run is pausing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRunWaitReason {
    /// Pause until a durable timer fires.
    Timer,
    /// Pause until a human resolves the checkpoint.
    Human {
        /// Checkpoint that must be resolved.
        checkpoint_id: HumanCheckpointId,
    },
    /// Pause until an external effect result is accepted.
    Effect,
}

impl AgentRunWaitReason {
    fn status(&self) -> AgentRunStatus {
        match self {
            Self::Timer => AgentRunStatus::WaitingForTimer,
            Self::Human { .. } => AgentRunStatus::WaitingForHuman,
            Self::Effect => AgentRunStatus::WaitingForEffect,
        }
    }

    fn transition_kind(&self) -> AgentRunTransitionKind {
        match self {
            Self::Timer => AgentRunTransitionKind::WaitForTimer,
            Self::Human { .. } => AgentRunTransitionKind::WaitForHuman,
            Self::Effect => AgentRunTransitionKind::WaitForEffect,
        }
    }
}

/// Successful step outcome selected by application code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentStepSuccess {
    /// Next step id. `None` means the run should complete.
    pub next_step_id: Option<AgentStepId>,
    /// Durable application state after the step.
    pub state_payload: AgentStatePayload,
}

impl AgentStepSuccess {
    /// Creates a step success that advances to another step.
    #[must_use]
    pub fn advance(next_step_id: AgentStepId, state_payload: AgentStatePayload) -> Self {
        Self {
            next_step_id: Some(next_step_id),
            state_payload,
        }
    }

    /// Creates a step success that completes the run.
    #[must_use]
    pub fn complete(state_payload: AgentStatePayload) -> Self {
        Self {
            next_step_id: None,
            state_payload,
        }
    }
}

/// Result of one durable agent run transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunTransition {
    /// Run that transitioned.
    pub run_id: AgentRunId,
    /// Transition kind.
    pub kind: AgentRunTransitionKind,
    /// Previous status. `None` is used for the initial start transition.
    pub previous_status: Option<AgentRunStatus>,
    /// Status after persistence succeeded.
    pub next_status: AgentRunStatus,
    /// Durable store revision after persistence.
    pub revision: Revision,
    /// Persisted state after the transition.
    pub state: AgentRunState,
}

/// Durable run engine failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRunEngineError {
    /// Runner was used before `recover`.
    NotRecovered {
        /// Run id.
        run_id: AgentRunId,
    },
    /// No durable run state exists for this runner.
    MissingRunState {
        /// Run id.
        run_id: AgentRunId,
    },
    /// Start was requested but a durable state record already exists.
    AlreadyStarted {
        /// Run id.
        run_id: AgentRunId,
    },
    /// Run state failed validation.
    InvalidRunState {
        /// Run id.
        run_id: AgentRunId,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Run state does not match the runner workflow.
    WorkflowMismatch {
        /// Run id.
        run_id: AgentRunId,
        /// Field that mismatched.
        field: &'static str,
        /// Expected value.
        expected: String,
        /// Actual value.
        actual: String,
    },
    /// Run state has no current step.
    MissingCurrentStep {
        /// Run id.
        run_id: AgentRunId,
    },
    /// Step id is not registered on the workflow definition.
    UnknownStep {
        /// Run id.
        run_id: AgentRunId,
        /// Unknown step id.
        step_id: AgentStepId,
    },
    /// Transition is not valid for the current status.
    InvalidTransition {
        /// Run id.
        run_id: AgentRunId,
        /// Requested transition.
        transition: AgentRunTransitionKind,
        /// Current status.
        from: AgentRunStatus,
        /// Stable reason.
        reason: &'static str,
    },
    /// Durable state persistence failed.
    Persistence {
        /// Run id.
        run_id: AgentRunId,
        /// Durable persistence error.
        error: DurableError,
    },
}

impl AgentRunEngineError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotRecovered { .. } => "not-recovered",
            Self::MissingRunState { .. } => "missing-run-state",
            Self::AlreadyStarted { .. } => "run-already-started",
            Self::InvalidRunState { .. } => "invalid-run-state",
            Self::WorkflowMismatch { .. } => "workflow-mismatch",
            Self::MissingCurrentStep { .. } => "missing-current-step",
            Self::UnknownStep { .. } => "unknown-step",
            Self::InvalidTransition { .. } => "invalid-transition",
            Self::Persistence { error, .. } => error.code(),
        }
    }
}

impl Display for AgentRunEngineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRecovered { run_id } => write!(f, "agent run {run_id} is not recovered"),
            Self::MissingRunState { run_id } => {
                write!(f, "agent run {run_id} has no durable state")
            }
            Self::AlreadyStarted { run_id } => {
                write!(f, "agent run {run_id} has already started")
            }
            Self::InvalidRunState { run_id, reason } => {
                write!(f, "agent run {run_id} has invalid state: {reason}")
            }
            Self::WorkflowMismatch {
                run_id,
                field,
                expected,
                actual,
            } => write!(
                f,
                "agent run {run_id} workflow field {field} mismatch: expected {expected}, actual {actual}"
            ),
            Self::MissingCurrentStep { run_id } => {
                write!(f, "agent run {run_id} has no current step")
            }
            Self::UnknownStep { run_id, step_id } => {
                write!(f, "agent run {run_id} references unknown step {step_id}")
            }
            Self::InvalidTransition {
                run_id,
                transition,
                from,
                reason,
            } => write!(
                f,
                "agent run {run_id} cannot apply {} from {}: {reason}",
                transition.as_label(),
                from.as_label()
            ),
            Self::Persistence { error, .. } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentRunEngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Persistence { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// Durable step state machine for one agent run.
pub struct AgentStepRunner<Store>
where
    Store: DurableStateStore<AgentRunState>,
{
    workflow: AgentWorkflow,
    run_id: AgentRunId,
    persistence_id: PersistenceId,
    store: Store,
    recovered: bool,
    record: Option<StateRecord<AgentRunState>>,
}

impl<Store> AgentStepRunner<Store>
where
    Store: DurableStateStore<AgentRunState>,
{
    /// Creates a step runner for one workflow definition and run id.
    #[must_use]
    pub fn new(workflow: AgentWorkflow, run_id: AgentRunId, store: Store) -> Self {
        let persistence_id = agent_run_persistence_id(&run_id);
        Self {
            workflow,
            run_id,
            persistence_id,
            store,
            recovered: false,
            record: None,
        }
    }

    /// Workflow definition used by this runner.
    #[must_use]
    pub const fn workflow(&self) -> &AgentWorkflow {
        &self.workflow
    }

    /// Run id used by this runner.
    #[must_use]
    pub const fn run_id(&self) -> &AgentRunId {
        &self.run_id
    }

    /// Persistence id used for this run state.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Recovers the latest durable run state, if present.
    pub async fn recover(&mut self) -> AgentRunEngineResult<Option<&AgentRunState>> {
        self.record = self
            .store
            .load(&self.persistence_id)
            .await
            .map_err(|error| self.persistence_error(error))?;
        self.recovered = true;
        Ok(self.record.as_ref().map(|record| &record.state))
    }

    /// Current recovered state, when present.
    pub fn state(&self) -> AgentRunEngineResult<Option<&AgentRunState>> {
        self.ensure_recovered()?;
        Ok(self.record.as_ref().map(|record| &record.state))
    }

    /// Persists the initial accepted run state.
    pub async fn start(
        &mut self,
        initial_state: AgentRunState,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        self.ensure_recovered()?;
        if self.record.is_some() {
            return Err(AgentRunEngineError::AlreadyStarted {
                run_id: self.run_id.clone(),
            });
        }

        self.validate_start_state(&initial_state)?;
        let persisted = self
            .store
            .compare_and_set(&self.persistence_id, Revision::INITIAL, initial_state)
            .await
            .map_err(|error| self.persistence_error(error))?;
        self.record = Some(persisted.clone());
        Ok(self.transition(AgentRunTransitionKind::Start, None, persisted))
    }

    /// Starts executing the current step.
    pub async fn begin_step(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Accepted, AgentRunStatus::Running],
            AgentRunTransitionKind::BeginStep,
            "step execution can only begin from accepted or running",
        )?;
        self.current_step(&record.state)?;

        let mut next = record.state;
        next.status = AgentRunStatus::Running;
        next.current_attempt = next.current_attempt.saturating_add(1);
        next.updated_at = now;

        self.persist_transition(
            AgentRunTransitionKind::BeginStep,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Records successful completion of the current step.
    pub async fn succeed_step(
        &mut self,
        success: AgentStepSuccess,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Running],
            AgentRunTransitionKind::StepSucceeded,
            "step success can only be recorded while running",
        )?;
        self.current_step(&record.state)?;

        let mut next = record.state;
        next.state_payload = success.state_payload;
        next.updated_at = now;

        let kind = if let Some(next_step_id) = success.next_step_id {
            self.workflow_step(&next_step_id)?;
            next.current_step_id = Some(next_step_id);
            next.current_attempt = 0;
            next.status = AgentRunStatus::Running;
            AgentRunTransitionKind::StepSucceeded
        } else {
            next.current_step_id = None;
            next.status = AgentRunStatus::Completed;
            next.completed_at = Some(now);
            AgentRunTransitionKind::Complete
        };

        self.persist_transition(kind, Some(previous_status), record.revision, next)
            .await
    }

    /// Records failure of the current step and fails the run.
    pub async fn fail_step(
        &mut self,
        _error_code: impl Into<String>,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Running],
            AgentRunTransitionKind::StepFailed,
            "step failure can only be recorded while running",
        )?;
        self.current_step(&record.state)?;

        let mut next = record.state;
        next.status = AgentRunStatus::Failed;
        next.updated_at = now;
        next.completed_at = Some(now);

        self.persist_transition(
            AgentRunTransitionKind::StepFailed,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Pauses the run while it waits for an external condition.
    pub async fn wait(
        &mut self,
        reason: AgentRunWaitReason,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Running],
            reason.transition_kind(),
            "wait can only be entered while running",
        )?;

        let transition_kind = reason.transition_kind();
        let mut next = record.state;
        next.status = reason.status();
        next.updated_at = now;
        if let AgentRunWaitReason::Human { checkpoint_id } = reason {
            next.pending_human_checkpoint = Some(checkpoint_id);
        }

        self.persist_transition(
            transition_kind,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Persists an open human checkpoint and pauses the run for a decision.
    pub async fn open_human_checkpoint(
        &mut self,
        checkpoint: HumanCheckpoint,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Running],
            AgentRunTransitionKind::WaitForHuman,
            "human checkpoints can only be opened while running",
        )?;

        if checkpoint.status != HumanCheckpointStatus::Open {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "human checkpoint must be open",
            });
        }

        if record
            .state
            .checkpoints
            .iter()
            .any(|existing| existing.checkpoint_id == checkpoint.checkpoint_id)
        {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "human checkpoint id already exists",
            });
        }

        let mut next = record.state;
        next.status = AgentRunStatus::WaitingForHuman;
        next.pending_human_checkpoint = Some(checkpoint.checkpoint_id.clone());
        next.checkpoints.push(checkpoint);
        next.updated_at = now;

        self.persist_transition(
            AgentRunTransitionKind::WaitForHuman,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Resolves the currently pending human checkpoint and resumes the run.
    pub async fn resolve_human_checkpoint(
        &mut self,
        checkpoint_id: &HumanCheckpointId,
        resolved_status: HumanCheckpointStatus,
        resolved_by: Option<PrincipalRef>,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        if matches!(
            resolved_status,
            HumanCheckpointStatus::Open
                | HumanCheckpointStatus::Escalated
                | HumanCheckpointStatus::TimedOut
        ) {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "human decision must resolve to a terminal decision status",
            });
        }

        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::WaitingForHuman],
            AgentRunTransitionKind::Resume,
            "human checkpoint decisions can only resume a human wait",
        )?;
        if record.state.pending_human_checkpoint.as_ref() != Some(checkpoint_id) {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "human decision does not match the pending checkpoint",
            });
        }

        let mut next = record.state;
        let checkpoint = next
            .checkpoints
            .iter_mut()
            .find(|checkpoint| &checkpoint.checkpoint_id == checkpoint_id)
            .ok_or_else(|| AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "pending human checkpoint is missing from run state",
            })?;
        if checkpoint.status != HumanCheckpointStatus::Open
            && checkpoint.status != HumanCheckpointStatus::Escalated
        {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "human checkpoint is not open",
            });
        }

        checkpoint.status = resolved_status;
        checkpoint.resolved_by = resolved_by;
        checkpoint.resolved_at = Some(now);
        next.status = AgentRunStatus::Running;
        next.pending_human_checkpoint = None;
        next.updated_at = now;

        self.persist_transition(
            AgentRunTransitionKind::Resume,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Marks the pending checkpoint escalated while the run remains waiting.
    pub async fn escalate_human_checkpoint(
        &mut self,
        checkpoint_id: &HumanCheckpointId,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::WaitingForHuman],
            AgentRunTransitionKind::CheckpointEscalated,
            "human checkpoint escalation can only happen while waiting for human input",
        )?;
        if record.state.pending_human_checkpoint.as_ref() != Some(checkpoint_id) {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "escalation does not match the pending checkpoint",
            });
        }

        let mut next = record.state;
        let checkpoint = next
            .checkpoints
            .iter_mut()
            .find(|checkpoint| &checkpoint.checkpoint_id == checkpoint_id)
            .ok_or_else(|| AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "pending human checkpoint is missing from run state",
            })?;
        if checkpoint.status != HumanCheckpointStatus::Open {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "only open human checkpoints can be escalated",
            });
        }

        checkpoint.status = HumanCheckpointStatus::Escalated;
        next.updated_at = now;

        self.persist_transition(
            AgentRunTransitionKind::CheckpointEscalated,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Resumes a run from a waiting status.
    pub async fn resume(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[
                AgentRunStatus::WaitingForTimer,
                AgentRunStatus::WaitingForHuman,
                AgentRunStatus::WaitingForEffect,
            ],
            AgentRunTransitionKind::Resume,
            "resume can only be applied from a waiting status",
        )?;

        let mut next = record.state;
        next.status = AgentRunStatus::Running;
        next.updated_at = now;
        next.pending_human_checkpoint = None;

        self.persist_transition(
            AgentRunTransitionKind::Resume,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Completes a running run.
    pub async fn complete(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Running],
            AgentRunTransitionKind::Complete,
            "completion can only be applied while running",
        )?;

        let mut next = record.state;
        next.status = AgentRunStatus::Completed;
        next.current_step_id = None;
        next.updated_at = now;
        next.completed_at = Some(now);

        self.persist_transition(
            AgentRunTransitionKind::Complete,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Fails a non-terminal run outside a specific step failure.
    pub async fn fail_run(
        &mut self,
        _error_code: impl Into<String>,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.reject_terminal(
            previous_status,
            AgentRunTransitionKind::Fail,
            "terminal runs cannot be failed again",
        )?;

        let mut next = record.state;
        next.status = AgentRunStatus::Failed;
        next.updated_at = now;
        next.completed_at = Some(now);

        self.persist_transition(
            AgentRunTransitionKind::Fail,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Requests cancellation for a non-terminal run.
    pub async fn request_cancellation(
        &mut self,
        reason_code: impl Into<String>,
        reason_summary: Option<String>,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let reason_code = reason_code.into();
        if reason_code.trim().is_empty() {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "cancellation reason code is required",
            });
        }

        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.reject_terminal(
            previous_status,
            AgentRunTransitionKind::RequestCancellation,
            "terminal runs cannot be cancelled",
        )?;

        let mut next = record.state;
        next.status = AgentRunStatus::Cancelling;
        next.updated_at = now;
        next.cancellation = Some(AgentCancellation {
            requested_by: None,
            requested_at: now,
            reason_code,
            reason_summary,
        });

        self.persist_transition(
            AgentRunTransitionKind::RequestCancellation,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Marks a cancelling run cancelled.
    pub async fn cancel(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Cancelling],
            AgentRunTransitionKind::Cancel,
            "cancel can only complete from cancelling",
        )?;

        let mut next = record.state;
        next.status = AgentRunStatus::Cancelled;
        next.updated_at = now;
        next.completed_at = Some(now);

        self.persist_transition(
            AgentRunTransitionKind::Cancel,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Moves a failed run into compensation.
    pub async fn begin_compensation(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        self.require_status(
            previous_status,
            &[AgentRunStatus::Failed],
            AgentRunTransitionKind::BeginCompensation,
            "compensation can only begin from failed",
        )?;

        let mut next = record.state;
        next.status = AgentRunStatus::Compensating;
        next.updated_at = now;
        next.completed_at = None;

        self.persist_transition(
            AgentRunTransitionKind::BeginCompensation,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    /// Persists an updated compiled graph state into the durable run state.
    pub async fn update_graph_state(
        &mut self,
        graph_state: AgentGraphRunState,
        now: AgentTimestampMillis,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        let record = self.current_record()?;
        let previous_status = record.state.status;
        if graph_state.terminal_status.is_none() {
            self.reject_terminal(
                previous_status,
                AgentRunTransitionKind::GraphUpdated,
                "terminal runs cannot apply non-terminal graph updates",
            )?;
        }

        let mut next = record.state;
        next.graph_state = Some(graph_state.clone());
        next.updated_at = now;
        match graph_state.terminal_status {
            Some(AgentGraphTerminalStatus::Completed) => {
                next.status = AgentRunStatus::Completed;
                next.current_step_id = None;
                next.completed_at = Some(now);
            }
            Some(AgentGraphTerminalStatus::Failed) => {
                next.status = AgentRunStatus::Failed;
                next.current_step_id = None;
                next.completed_at = Some(now);
            }
            Some(AgentGraphTerminalStatus::Cancelled) => {
                next.status = AgentRunStatus::Cancelled;
                next.current_step_id = None;
                next.completed_at = Some(now);
            }
            None => {
                next.status = AgentRunStatus::Running;
                next.completed_at = None;
            }
        }

        self.persist_transition(
            AgentRunTransitionKind::GraphUpdated,
            Some(previous_status),
            record.revision,
            next,
        )
        .await
    }

    fn ensure_recovered(&self) -> AgentRunEngineResult<()> {
        if self.recovered {
            Ok(())
        } else {
            Err(AgentRunEngineError::NotRecovered {
                run_id: self.run_id.clone(),
            })
        }
    }

    fn current_record(&self) -> AgentRunEngineResult<StateRecord<AgentRunState>> {
        self.ensure_recovered()?;
        self.record
            .clone()
            .ok_or_else(|| AgentRunEngineError::MissingRunState {
                run_id: self.run_id.clone(),
            })
    }

    fn validate_start_state(&self, state: &AgentRunState) -> AgentRunEngineResult<()> {
        self.validate_workflow_fields(state)?;
        if state.status != AgentRunStatus::Accepted {
            return Err(AgentRunEngineError::InvalidRunState {
                run_id: self.run_id.clone(),
                reason: "initial run state must be accepted",
            });
        }
        if state.graph_state.is_none() {
            self.current_step(state)?;
        }
        Ok(())
    }

    fn validate_workflow_fields(&self, state: &AgentRunState) -> AgentRunEngineResult<()> {
        if state.run_id != self.run_id {
            return Err(AgentRunEngineError::WorkflowMismatch {
                run_id: self.run_id.clone(),
                field: "run_id",
                expected: self.run_id.to_string(),
                actual: state.run_id.to_string(),
            });
        }
        if state.workflow_id != self.workflow.workflow_id {
            return Err(AgentRunEngineError::WorkflowMismatch {
                run_id: self.run_id.clone(),
                field: "workflow_id",
                expected: self.workflow.workflow_id.to_string(),
                actual: state.workflow_id.to_string(),
            });
        }
        if state.definition_version != self.workflow.definition_version {
            return Err(AgentRunEngineError::WorkflowMismatch {
                run_id: self.run_id.clone(),
                field: "definition_version",
                expected: self.workflow.definition_version.to_string(),
                actual: state.definition_version.to_string(),
            });
        }
        if state.state_schema_version != self.workflow.state_schema_version {
            return Err(AgentRunEngineError::WorkflowMismatch {
                run_id: self.run_id.clone(),
                field: "state_schema_version",
                expected: self.workflow.state_schema_version.get().to_string(),
                actual: state.state_schema_version.get().to_string(),
            });
        }
        if let Some(step_id) = &state.current_step_id {
            self.workflow_step(step_id)?;
        }
        Ok(())
    }

    fn current_step(&self, state: &AgentRunState) -> AgentRunEngineResult<&AgentStep> {
        let step_id = state.current_step_id.as_ref().ok_or_else(|| {
            AgentRunEngineError::MissingCurrentStep {
                run_id: self.run_id.clone(),
            }
        })?;
        self.workflow_step(step_id)
    }

    fn workflow_step(&self, step_id: &AgentStepId) -> AgentRunEngineResult<&AgentStep> {
        self.workflow
            .steps
            .iter()
            .find(|step| &step.step_id == step_id)
            .ok_or_else(|| AgentRunEngineError::UnknownStep {
                run_id: self.run_id.clone(),
                step_id: step_id.clone(),
            })
    }

    fn require_status(
        &self,
        from: AgentRunStatus,
        allowed: &[AgentRunStatus],
        transition: AgentRunTransitionKind,
        reason: &'static str,
    ) -> AgentRunEngineResult<()> {
        if allowed.contains(&from) {
            Ok(())
        } else {
            Err(AgentRunEngineError::InvalidTransition {
                run_id: self.run_id.clone(),
                transition,
                from,
                reason,
            })
        }
    }

    fn reject_terminal(
        &self,
        from: AgentRunStatus,
        transition: AgentRunTransitionKind,
        reason: &'static str,
    ) -> AgentRunEngineResult<()> {
        if is_terminal(from) {
            Err(AgentRunEngineError::InvalidTransition {
                run_id: self.run_id.clone(),
                transition,
                from,
                reason,
            })
        } else {
            Ok(())
        }
    }

    async fn persist_transition(
        &mut self,
        kind: AgentRunTransitionKind,
        previous_status: Option<AgentRunStatus>,
        expected_revision: Revision,
        next_state: AgentRunState,
    ) -> AgentRunEngineResult<AgentRunTransition> {
        self.validate_workflow_fields(&next_state)?;
        let persisted = self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, next_state)
            .await
            .map_err(|error| self.persistence_error(error))?;
        self.record = Some(persisted.clone());
        Ok(self.transition(kind, previous_status, persisted))
    }

    fn transition(
        &self,
        kind: AgentRunTransitionKind,
        previous_status: Option<AgentRunStatus>,
        record: StateRecord<AgentRunState>,
    ) -> AgentRunTransition {
        AgentRunTransition {
            run_id: self.run_id.clone(),
            kind,
            previous_status,
            next_status: record.state.status,
            revision: record.revision,
            state: record.state,
        }
    }

    fn persistence_error(&self, error: DurableError) -> AgentRunEngineError {
        AgentRunEngineError::Persistence {
            run_id: self.run_id.clone(),
            error,
        }
    }
}

fn is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
    )
}
