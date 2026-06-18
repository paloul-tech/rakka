//! Actor-backed runtime for one durable agent run.
//!
//! `AgentRunActor` is a process-local host for the durable run engine. It does
//! not replace durable state, inbox, or outbox storage; it serializes access to
//! those facades and recovers them when the actor starts. Runtime messages carry
//! small command envelopes and domain records. Large prompts, model responses,
//! files, and tool outputs should continue to move through `ArtifactRef` values
//! stored in those records.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFailure, ActorFuture, MetricsRecorder,
    NoopMetricsRecorder, RakkaError, ReplyTo, Subsystem,
};
use rakka_persistence::DurableStateStore;
use rakka_workflow::{SystemWorkflowClock, WorkflowClock, WorkflowState};

use crate::{
    AgentCommand, AgentDueEffect, AgentEffect, AgentInboxAcceptance, AgentInboxError,
    AgentOutboxAcceptance, AgentOutboxError, AgentOutboxResult, AgentRunEngineError, AgentRunId,
    AgentRunInbox, AgentRunState, AgentRunTransition, AgentRunWaitReason, AgentStepRunner,
    AgentStepSuccess, AgentTimestampMillis, AgentWorkflow,
};

/// Shared result type for actor-backed agent run runtime operations.
pub type AgentRunRuntimeResult<T> = Result<T, AgentRunRuntimeError>;

/// Errors surfaced by the actor-backed agent run runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRunRuntimeError {
    /// Durable run state-machine operation failed.
    RunEngine {
        /// Run engine failure.
        error: AgentRunEngineError,
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
}

impl AgentRunRuntimeError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::RunEngine { error } => error.code(),
            Self::Inbox { error } => error.code(),
            Self::Outbox { error } => error.code(),
        }
    }
}

impl Display for AgentRunRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunEngine { error } => Display::fmt(error, f),
            Self::Inbox { error } => Display::fmt(error, f),
            Self::Outbox { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentRunRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RunEngine { error } => Some(error),
            Self::Inbox { error } => Some(error),
            Self::Outbox { error } => Some(error),
        }
    }
}

impl From<AgentRunEngineError> for AgentRunRuntimeError {
    fn from(error: AgentRunEngineError) -> Self {
        Self::RunEngine { error }
    }
}

impl From<AgentInboxError> for AgentRunRuntimeError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox { error }
    }
}

impl From<AgentOutboxError> for AgentRunRuntimeError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox { error }
    }
}

/// Diagnostic snapshot for one actor-hosted run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRunActorSnapshot {
    /// Run id hosted by the actor.
    pub run_id: AgentRunId,
    /// Latest recovered or persisted run state.
    pub run_state: Option<AgentRunState>,
    /// Recoverable durable inbox command count.
    pub recoverable_command_count: usize,
    /// Due durable outbox effect count.
    pub due_effect_count: usize,
}

/// Typed message protocol accepted by [`AgentRunActor`].
///
/// Messages are intentionally small runtime commands. Application payloads,
/// prompts, completions, and tool outputs should be stored out of line and
/// referenced from `AgentRunState` or `AgentEffect` artifacts.
pub enum AgentRunActorCommand {
    /// Recover durable run state, inbox, and outbox state.
    Recover {
        /// Reply channel for the recovered snapshot.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunActorSnapshot>>,
    },
    /// Return the current recovered snapshot without forcing another load.
    Snapshot {
        /// Reply channel for the current snapshot.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunActorSnapshot>>,
    },
    /// Accept an agent command through the durable inbox facade.
    AcceptCommand {
        /// Command envelope to durably accept.
        command: AgentCommand,
        /// Reply channel for durable inbox acceptance.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentInboxAcceptance>>,
    },
    /// Persist the initial accepted run state.
    Start {
        /// Initial accepted run state.
        initial_state: AgentRunState,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Begin executing the current step.
    BeginStep {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Record current step success.
    SucceedStep {
        /// Successful step outcome.
        success: AgentStepSuccess,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Record current step failure and fail the run.
    FailStep {
        /// Stable bounded error code.
        error_code: String,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Pause the run for a durable wait condition.
    Wait {
        /// Wait reason.
        reason: AgentRunWaitReason,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Resume a waiting run.
    Resume {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Complete a running run.
    Complete {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Fail a non-terminal run outside a specific step failure.
    FailRun {
        /// Stable bounded error code.
        error_code: String,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Request run cancellation.
    RequestCancellation {
        /// Stable bounded cancellation reason code.
        reason_code: String,
        /// Optional human-readable cancellation summary.
        reason_summary: Option<String>,
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Mark a cancelling run cancelled.
    Cancel {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Move a failed run into compensation.
    BeginCompensation {
        /// Transition timestamp.
        now: AgentTimestampMillis,
        /// Reply channel for the persisted transition.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunTransition>>,
    },
    /// Schedule a first-class effect through the durable outbox facade.
    ScheduleEffect {
        /// Effect envelope to durably schedule.
        effect: AgentEffect,
        /// Reply channel for durable outbox scheduling.
        reply_to: ReplyTo<AgentRunRuntimeResult<AgentOutboxAcceptance>>,
    },
    /// Return due effects from the durable outbox snapshot.
    DueEffects {
        /// Reply channel for due effects.
        reply_to: ReplyTo<AgentRunRuntimeResult<Vec<AgentDueEffect>>>,
    },
}

/// Actor-backed host for one durable agent run.
pub struct AgentRunActor<RunStore, WorkflowStore, Clock = SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    runner: AgentStepRunner<RunStore>,
    inbox: AgentRunInbox<WorkflowStore, Clock>,
}

impl<RunStore, WorkflowStore> AgentRunActor<RunStore, WorkflowStore, SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    /// Creates an actor-backed runtime with the system clock and no-op metrics.
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

    /// Creates an actor-backed runtime with the system clock and explicit
    /// metrics recorder.
    #[must_use]
    pub fn with_metrics(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::from_parts(
            AgentStepRunner::new(workflow, run_id.clone(), run_store),
            AgentRunInbox::with_metrics(run_id, workflow_store, metrics),
        )
    }
}

impl<RunStore, WorkflowStore, Clock> AgentRunActor<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates an actor-backed runtime with an explicit clock and no-op metrics.
    #[must_use]
    pub fn with_clock(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        clock: Clock,
    ) -> Self {
        Self::with_clock_and_metrics(
            workflow,
            run_id,
            run_store,
            workflow_store,
            clock,
            Arc::new(NoopMetricsRecorder),
        )
    }

    /// Creates an actor-backed runtime with an explicit clock and metrics
    /// recorder.
    #[must_use]
    pub fn with_clock_and_metrics(
        workflow: AgentWorkflow,
        run_id: AgentRunId,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        clock: Clock,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::from_parts(
            AgentStepRunner::new(workflow, run_id.clone(), run_store),
            AgentRunInbox::with_clock_and_metrics(run_id, workflow_store, clock, metrics),
        )
    }

    /// Creates an actor-backed runtime from already constructed durable facades.
    #[must_use]
    pub const fn from_parts(
        runner: AgentStepRunner<RunStore>,
        inbox: AgentRunInbox<WorkflowStore, Clock>,
    ) -> Self {
        Self { runner, inbox }
    }

    /// Durable run state-machine facade.
    #[must_use]
    pub const fn runner(&self) -> &AgentStepRunner<RunStore> {
        &self.runner
    }

    /// Durable inbox and outbox facade.
    #[must_use]
    pub const fn inbox(&self) -> &AgentRunInbox<WorkflowStore, Clock> {
        &self.inbox
    }

    async fn recover_components(&mut self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
        self.runner.recover().await?;
        self.inbox.recover().await?;
        self.snapshot()
    }

    fn snapshot(&self) -> AgentRunRuntimeResult<AgentRunActorSnapshot> {
        let run_state = self.runner.state()?.cloned();
        let recoverable_command_count = self
            .inbox
            .inner()
            .recoverable_inbox()
            .map_err(AgentInboxError::from)?
            .len();
        let due_effect_count = self.inbox.due_effects()?.len();
        Ok(AgentRunActorSnapshot {
            run_id: self.runner.run_id().clone(),
            run_state,
            recoverable_command_count,
            due_effect_count,
        })
    }
}

impl<RunStore, WorkflowStore, Clock> Actor for AgentRunActor<RunStore, WorkflowStore, Clock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    type Msg = AgentRunActorCommand;

    fn started<'a>(&'a mut self, _ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async move {
            self.recover_components()
                .await
                .map_err(runtime_error_to_rakka)?;
            Ok(ActorAction::Continue)
        })
    }

    fn restarted<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _failure: &'a ActorFailure,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.recover_components()
                .await
                .map_err(runtime_error_to_rakka)?;
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                AgentRunActorCommand::Recover { reply_to } => {
                    let _reply_dropped = reply_to.reply(self.recover_components().await);
                }
                AgentRunActorCommand::Snapshot { reply_to } => {
                    let _reply_dropped = reply_to.reply(self.snapshot());
                }
                AgentRunActorCommand::AcceptCommand { command, reply_to } => {
                    let result = self
                        .inbox
                        .accept_command(command)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Start {
                    initial_state,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .start(initial_state)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::BeginStep { now, reply_to } => {
                    let result = self
                        .runner
                        .begin_step(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::SucceedStep {
                    success,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .succeed_step(success, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::FailStep {
                    error_code,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .fail_step(error_code, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Wait {
                    reason,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .wait(reason, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Resume { now, reply_to } => {
                    let result = self
                        .runner
                        .resume(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Complete { now, reply_to } => {
                    let result = self
                        .runner
                        .complete(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::FailRun {
                    error_code,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .fail_run(error_code, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::RequestCancellation {
                    reason_code,
                    reason_summary,
                    now,
                    reply_to,
                } => {
                    let result = self
                        .runner
                        .request_cancellation(reason_code, reason_summary, now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::Cancel { now, reply_to } => {
                    let result = self
                        .runner
                        .cancel(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::BeginCompensation { now, reply_to } => {
                    let result = self
                        .runner
                        .begin_compensation(now)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::ScheduleEffect { effect, reply_to } => {
                    let result = self
                        .inbox
                        .schedule_effect(effect)
                        .await
                        .map_err(AgentRunRuntimeError::from);
                    let _reply_dropped = reply_to.reply(result);
                }
                AgentRunActorCommand::DueEffects { reply_to } => {
                    let result: AgentOutboxResult<Vec<AgentDueEffect>> = self.inbox.due_effects();
                    let _reply_dropped = reply_to.reply(result.map_err(AgentRunRuntimeError::from));
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

fn runtime_error_to_rakka(error: AgentRunRuntimeError) -> RakkaError {
    RakkaError::new(Subsystem::Workflow, error.code(), error.to_string())
}
