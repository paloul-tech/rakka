//! Durable A2A request handler for the local Phase 2 example.
//!
//! Public command paths acknowledge work only after `AgentRunInbox` accepts the
//! command durably. Public read paths are served from the task projection store.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};

use a2a::{
    A2AError, AgentCard, CancelTaskRequest, DeleteTaskPushNotificationConfigRequest,
    GetExtendedAgentCardRequest, GetTaskPushNotificationConfigRequest, GetTaskRequest,
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    ListTasksRequest, ListTasksResponse, Message, SendMessageRequest, SendMessageResponse,
    StreamResponse, SubscribeToTaskRequest, Task, TaskPushNotificationConfig, TaskState,
};
use a2a_server::{RequestHandler, ServiceParams};
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream};
use rakka::agent_workflow::{
    AgentInboxAcceptance, AgentInboxError, AgentRunEngineError, AgentRunId, AgentRunInbox,
    AgentRunState, AgentRunStatus, AgentStatePayload, AgentStepRunner, AgentTimestampMillis,
    AgentWorkflow, ArtifactRef,
};
use rakka::persistence::{DurableError, DurableStateStore};
use rakka::prelude::{MetricsRecorder, NoopMetricsRecorder};

use crate::a2a_mapping::{
    build_cancel_task_command_draft, build_send_message_command_draft, canonical_read_tenant,
    now_agent_timestamp, A2ACommandDraft, A2ACommandPayload, A2AMappingError, A2APayloadPolicy,
    A2ATaskIntent, DEFAULT_TENANT,
};
use crate::durable_stores::{RunStore, WorkflowStore};
use crate::task_projection::{
    A2ATaskEventPayload, A2ATaskProjection, InMemoryA2ATaskProjectionStore, TaskProjectionError,
};

const STREAMING_UNIMPLEMENTED: &str =
    "A2A streaming is intentionally deferred until the streaming phase";
const PUSH_UNIMPLEMENTED: &str =
    "A2A push notifications are intentionally deferred until the push phase";

/// Shared observer used by tests to prove headers reach `ServiceParams`.
#[derive(Debug, Clone, Default)]
pub struct HeaderObserver {
    last_params: Arc<Mutex<Option<ServiceParams>>>,
}

impl HeaderObserver {
    /// Records the latest service parameters seen by the handler.
    pub fn record(&self, params: &ServiceParams) {
        *self.last_params.lock().expect("header observer mutex") = Some(params.clone());
    }

    /// Returns the last captured service parameters.
    #[must_use]
    pub fn last(&self) -> Option<ServiceParams> {
        self.last_params
            .lock()
            .expect("header observer mutex")
            .clone()
    }
}

/// Stable adapter-local failures mapped to A2A protocol errors.
#[derive(Debug, Clone)]
pub enum RakkaA2AHandlerError {
    /// A2A input could not be mapped to a Rakka command.
    Mapping(A2AMappingError),
    /// Task projection read or write failed.
    Projection(TaskProjectionError),
    /// Durable inbox acceptance failed.
    Inbox(AgentInboxError),
    /// Durable run-state transition failed.
    RunEngine(AgentRunEngineError),
    /// Durable-state store query failed.
    Persistence(DurableError),
    /// The requested run was not found before accepting a continuation command.
    MissingRun {
        /// Missing public task id.
        task_id: String,
    },
    /// The command kind is not valid for the normalized task lifecycle intent.
    InvalidLifecycle {
        /// Public task id.
        task_id: String,
        /// Stable reason.
        reason: &'static str,
    },
}

impl RakkaA2AHandlerError {
    /// Stable machine-readable adapter error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Mapping(error) => error.code(),
            Self::Projection(error) => error.code(),
            Self::Inbox(error) => error.code(),
            Self::RunEngine(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::MissingRun { .. } => "task-not-found",
            Self::InvalidLifecycle { .. } => "invalid-command-lifecycle",
        }
    }

    fn into_a2a_error(self) -> A2AError {
        let code = self.code();
        match self {
            Self::Projection(TaskProjectionError::TaskNotFound { task_id })
            | Self::MissingRun { task_id } => A2AError::task_not_found(&task_id),
            Self::Mapping(error) => A2AError::invalid_params(format!("{code}: {error}")),
            Self::Projection(error) => A2AError::invalid_params(format!("{code}: {error}")),
            Self::InvalidLifecycle { reason, .. } => {
                A2AError::invalid_params(format!("{code}: {reason}"))
            }
            Self::RunEngine(AgentRunEngineError::MissingRunState { run_id }) => {
                A2AError::task_not_found(run_id.as_str())
            }
            Self::Inbox(error) => A2AError::internal(format!("{code}: {error}")),
            Self::RunEngine(error) => A2AError::internal(format!("{code}: {error}")),
            Self::Persistence(error) => A2AError::internal(format!("{code}: {error}")),
        }
    }
}

impl Display for RakkaA2AHandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mapping(error) => Display::fmt(error, f),
            Self::Projection(error) => Display::fmt(error, f),
            Self::Inbox(error) => Display::fmt(error, f),
            Self::RunEngine(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::MissingRun { task_id } => write!(f, "task not found: {task_id}"),
            Self::InvalidLifecycle { task_id, reason } => {
                write!(f, "task {task_id} has invalid command lifecycle: {reason}")
            }
        }
    }
}

impl Error for RakkaA2AHandlerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Mapping(error) => Some(error),
            Self::Projection(error) => Some(error),
            Self::Inbox(error) => Some(error),
            Self::RunEngine(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::MissingRun { .. } | Self::InvalidLifecycle { .. } => None,
        }
    }
}

impl From<A2AMappingError> for RakkaA2AHandlerError {
    fn from(error: A2AMappingError) -> Self {
        Self::Mapping(error)
    }
}

impl From<TaskProjectionError> for RakkaA2AHandlerError {
    fn from(error: TaskProjectionError) -> Self {
        Self::Projection(error)
    }
}

impl From<AgentInboxError> for RakkaA2AHandlerError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox(error)
    }
}

impl From<AgentRunEngineError> for RakkaA2AHandlerError {
    fn from(error: AgentRunEngineError) -> Self {
        Self::RunEngine(error)
    }
}

impl From<DurableError> for RakkaA2AHandlerError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

/// Phase 2 A2A handler implementation backed by durable Rakka stores.
pub struct RakkaA2ARequestHandler {
    agent_card: AgentCard,
    workflow: AgentWorkflow,
    task_store: InMemoryA2ATaskProjectionStore,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    metrics: Arc<dyn MetricsRecorder>,
    header_observer: HeaderObserver,
}

impl RakkaA2ARequestHandler {
    /// Creates a local durable handler with no-op metrics.
    #[must_use]
    pub fn new(
        agent_card: AgentCard,
        workflow: AgentWorkflow,
        task_store: InMemoryA2ATaskProjectionStore,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        header_observer: HeaderObserver,
    ) -> Self {
        Self::with_metrics(
            agent_card,
            workflow,
            task_store,
            run_store,
            workflow_store,
            Arc::new(NoopMetricsRecorder),
            header_observer,
        )
    }

    /// Creates a local durable handler with an explicit metrics recorder.
    #[must_use]
    pub fn with_metrics(
        agent_card: AgentCard,
        workflow: AgentWorkflow,
        task_store: InMemoryA2ATaskProjectionStore,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        metrics: Arc<dyn MetricsRecorder>,
        header_observer: HeaderObserver,
    ) -> Self {
        Self {
            agent_card,
            workflow,
            task_store,
            run_store,
            workflow_store,
            metrics,
            header_observer,
        }
    }

    /// Rebuilds any missing local task projections from durable run state.
    pub async fn recover_task_projections(&self) -> Result<usize, RakkaA2AHandlerError> {
        self.recover_task_projections_impl().await
    }

    fn record(&self, params: &ServiceParams) {
        self.header_observer.record(params);
    }

    async fn send_message_impl(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, RakkaA2AHandlerError> {
        let received_at = now_agent_timestamp();
        let draft = build_send_message_command_draft(
            params,
            &req,
            &self.workflow,
            A2APayloadPolicy::default().without_artifact_strategy(),
            received_at,
        )?;
        self.validate_lifecycle(&draft).await?;
        let acceptance = self.accept_draft(&draft).await?;
        let accepted = acceptance.is_accepted();
        let mut run_state = if matches!(draft.normalized.intent, A2ATaskIntent::NewTask) {
            self.ensure_started(&draft, received_at).await?
        } else {
            self.require_run_state(&draft.normalized.run_id()).await?
        };

        let projected_message = projected_message(&req.message, &draft);
        let artifacts = artifact_refs(&draft.payload);
        if !return_immediately(&req) && accepted && run_state.status == AgentRunStatus::Accepted {
            run_state = self
                .begin_first_transition(&draft.normalized.run_id(), received_at)
                .await?;
        }
        self.project_send_result(&draft, &projected_message, artifacts, &run_state, accepted)?;
        let task = self.task_store.get(
            Some(draft.normalized.tenant.as_str()),
            &draft.normalized.task_id,
            req.configuration
                .as_ref()
                .and_then(|config| config.history_length),
        )?;
        Ok(SendMessageResponse::Task(task))
    }

    async fn cancel_task_impl(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, RakkaA2AHandlerError> {
        let received_at = now_agent_timestamp();
        let draft = build_cancel_task_command_draft(params, &req, &self.workflow, received_at)?;
        let mut run_state = self.require_run_state(&draft.normalized.run_id()).await?;
        if run_is_terminal(run_state.status) {
            self.ensure_projection_from_state(
                &run_state,
                &draft.normalized.context_id,
                received_at,
            )?;
            return self
                .task_store
                .get(
                    Some(draft.normalized.tenant.as_str()),
                    &draft.normalized.task_id,
                    None,
                )
                .map_err(Into::into);
        }

        let acceptance = self.accept_draft(&draft).await?;
        let accepted = acceptance.is_accepted();
        if accepted && run_state.status != AgentRunStatus::Cancelling {
            run_state = self
                .request_cancellation(&draft.normalized.run_id(), received_at)
                .await?;
        }
        if accepted {
            self.record_status_projection(&run_state, &draft.normalized.context_id, received_at)?;
        } else {
            self.ensure_projection_from_state(
                &run_state,
                &draft.normalized.context_id,
                received_at,
            )?;
        }
        self.task_store
            .get(
                Some(draft.normalized.tenant.as_str()),
                &draft.normalized.task_id,
                None,
            )
            .map_err(Into::into)
    }

    async fn recover_task_projections_impl(&self) -> Result<usize, RakkaA2AHandlerError> {
        let ids = self.run_store.persistence_ids().await?;
        let mut recovered = 0;
        for persistence_id in ids {
            let Some(run_id) = persistence_id.as_str().strip_prefix("agent-run:") else {
                continue;
            };
            let run_id = AgentRunId::new(run_id.to_string());
            let Some(state) = self.recover_run_state(&run_id).await? else {
                continue;
            };
            let tenant = run_tenant(&state);
            if self
                .task_store
                .projection(Some(&tenant), state.run_id.as_str())
                .is_ok()
            {
                continue;
            }
            self.snapshot_projection(
                &state,
                state.run_id.as_str(),
                Vec::new(),
                Vec::new(),
                state.updated_at,
            )?;
            recovered += 1;
        }
        Ok(recovered)
    }

    async fn validate_lifecycle(
        &self,
        draft: &A2ACommandDraft,
    ) -> Result<(), RakkaA2AHandlerError> {
        if matches!(draft.normalized.intent, A2ATaskIntent::NewTask)
            && !matches!(
                &draft.command.kind,
                rakka::agent_workflow::AgentCommandKind::StartRun
            )
        {
            return Err(RakkaA2AHandlerError::InvalidLifecycle {
                task_id: draft.normalized.task_id.clone(),
                reason: "new A2A tasks must map to StartRun",
            });
        }
        if matches!(draft.normalized.intent, A2ATaskIntent::ContinueTask)
            && self
                .recover_run_state(&draft.normalized.run_id())
                .await?
                .is_none()
        {
            return Err(RakkaA2AHandlerError::MissingRun {
                task_id: draft.normalized.task_id.clone(),
            });
        }
        Ok(())
    }

    async fn accept_draft(
        &self,
        draft: &A2ACommandDraft,
    ) -> Result<AgentInboxAcceptance, RakkaA2AHandlerError> {
        let mut inbox = AgentRunInbox::with_metrics(
            draft.normalized.run_id(),
            self.workflow_store.clone(),
            self.metrics.clone(),
        );
        inbox.recover().await?;
        inbox
            .accept_command(draft.command.clone())
            .await
            .map_err(Into::into)
    }

    async fn ensure_started(
        &self,
        draft: &A2ACommandDraft,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let run_id = draft.normalized.run_id();
        let mut runner = self.recovered_runner(run_id.clone()).await?;
        if let Some(state) = runner.state()?.cloned() {
            return Ok(state);
        }

        let initial = self.initial_run_state(draft, now)?;
        match runner.start(initial).await {
            Ok(transition) => Ok(transition.state),
            Err(AgentRunEngineError::AlreadyStarted { .. }) => {
                self.require_run_state(&run_id).await
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn begin_first_transition(
        &self,
        run_id: &AgentRunId,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let mut runner = self.recovered_runner(run_id.clone()).await?;
        let transition = runner
            .begin_step(AgentTimestampMillis::new(now.as_millis().saturating_add(1)))
            .await?;
        Ok(transition.state)
    }

    async fn request_cancellation(
        &self,
        run_id: &AgentRunId,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let mut runner = self.recovered_runner(run_id.clone()).await?;
        let transition = runner
            .request_cancellation(
                "a2a-cancel",
                Some("A2A client requested cancellation".to_string()),
                now,
            )
            .await?;
        Ok(transition.state)
    }

    async fn require_run_state(
        &self,
        run_id: &AgentRunId,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        self.recover_run_state(run_id)
            .await?
            .ok_or_else(|| RakkaA2AHandlerError::MissingRun {
                task_id: run_id.as_str().to_string(),
            })
    }

    async fn recover_run_state(
        &self,
        run_id: &AgentRunId,
    ) -> Result<Option<AgentRunState>, RakkaA2AHandlerError> {
        let runner = self.recovered_runner(run_id.clone()).await?;
        Ok(runner.state()?.cloned())
    }

    async fn recovered_runner(
        &self,
        run_id: AgentRunId,
    ) -> Result<AgentStepRunner<RunStore>, RakkaA2AHandlerError> {
        let mut runner =
            AgentStepRunner::new(self.workflow.clone(), run_id, self.run_store.clone());
        runner.recover().await?;
        Ok(runner)
    }

    fn initial_run_state(
        &self,
        draft: &A2ACommandDraft,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let current_step_id = self
            .workflow
            .steps
            .first()
            .map(|step| step.step_id.clone())
            .ok_or_else(|| RakkaA2AHandlerError::InvalidLifecycle {
                task_id: draft.normalized.task_id.clone(),
                reason: "workflow has no executable steps",
            })?;
        let artifacts = artifact_refs(&draft.payload);
        Ok(AgentRunState {
            run_id: draft.normalized.run_id(),
            workflow_id: self.workflow.workflow_id.clone(),
            tenant: Some(draft.normalized.tenant.clone()),
            definition_version: self.workflow.definition_version.clone(),
            state_schema_version: self.workflow.state_schema_version,
            graph_state: None,
            status: AgentRunStatus::Accepted,
            current_step_id: Some(current_step_id),
            current_attempt: 0,
            inputs_ref: artifacts.first().cloned(),
            state_payload: state_payload(&draft.payload),
            checkpoints: Vec::new(),
            pending_effects: Vec::new(),
            pending_human_checkpoint: None,
            cancellation: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        })
    }

    fn project_send_result(
        &self,
        draft: &A2ACommandDraft,
        message: &Message,
        artifacts: Vec<ArtifactRef>,
        run_state: &AgentRunState,
        accepted: bool,
    ) -> Result<Task, RakkaA2AHandlerError> {
        let tenant = draft.normalized.tenant.as_str();
        let existing = self
            .task_store
            .projection(Some(tenant), &draft.normalized.task_id);

        match (existing, accepted) {
            (Ok(_), false) => self
                .task_store
                .get(Some(tenant), &draft.normalized.task_id, None)
                .map_err(Into::into),
            (Ok(_), true) if matches!(draft.normalized.intent, A2ATaskIntent::ContinueTask) => {
                self.task_store.append_event_payload(
                    tenant,
                    &draft.normalized.task_id,
                    &draft.normalized.context_id,
                    run_state.updated_at,
                    A2ATaskEventPayload::MessageUpdate {
                        message: message.clone(),
                    },
                )?;
                self.task_store
                    .get(Some(tenant), &draft.normalized.task_id, None)
                    .map_err(Into::into)
            }
            (Ok(_), true) => {
                self.record_status_projection(
                    run_state,
                    &draft.normalized.context_id,
                    run_state.updated_at,
                )?;
                self.task_store
                    .get(Some(tenant), &draft.normalized.task_id, None)
                    .map_err(Into::into)
            }
            (Err(TaskProjectionError::TaskNotFound { .. }), _) => self.snapshot_projection(
                run_state,
                &draft.normalized.context_id,
                vec![message.clone()],
                artifacts,
                run_state.updated_at,
            ),
            (Err(error), _) => Err(error.into()),
        }
    }

    fn ensure_projection_from_state(
        &self,
        run_state: &AgentRunState,
        context_id: &str,
        now: AgentTimestampMillis,
    ) -> Result<(), RakkaA2AHandlerError> {
        let tenant = run_tenant(run_state);
        if self
            .task_store
            .projection(Some(&tenant), run_state.run_id.as_str())
            .is_err()
        {
            self.snapshot_projection(run_state, context_id, Vec::new(), Vec::new(), now)?;
        }
        Ok(())
    }

    fn record_status_projection(
        &self,
        run_state: &AgentRunState,
        context_id: &str,
        now: AgentTimestampMillis,
    ) -> Result<(), RakkaA2AHandlerError> {
        let tenant = run_tenant(run_state);
        let state = task_state(run_state.status);
        let payload = if state.is_terminal() {
            A2ATaskEventPayload::Terminal { state }
        } else {
            A2ATaskEventPayload::StatusUpdate { state }
        };
        match self.task_store.append_event_payload(
            tenant.as_str(),
            run_state.run_id.as_str(),
            context_id,
            now,
            payload,
        ) {
            Ok(_) => Ok(()),
            Err(TaskProjectionError::TaskNotFound { .. }) => {
                self.snapshot_projection(run_state, context_id, Vec::new(), Vec::new(), now)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn snapshot_projection(
        &self,
        run_state: &AgentRunState,
        context_id: &str,
        history: Vec<Message>,
        artifacts: Vec<ArtifactRef>,
        now: AgentTimestampMillis,
    ) -> Result<Task, RakkaA2AHandlerError> {
        let projection =
            A2ATaskProjection::from_run_state(run_state, context_id, history, artifacts, 0);
        let tenant = projection.tenant.clone();
        let task_id = projection.task_id.clone();
        let context_id = projection.context_id.clone();
        self.task_store.append_event_payload(
            tenant.clone(),
            task_id.clone(),
            context_id,
            now,
            A2ATaskEventPayload::Snapshot(projection),
        )?;
        self.task_store
            .get(Some(&tenant), &task_id, None)
            .map_err(Into::into)
    }
}

#[async_trait]
impl RequestHandler for RakkaA2ARequestHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        self.record(params);
        self.send_message_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.record(params);
        build_send_message_command_draft(
            params,
            &req,
            &self.workflow,
            A2APayloadPolicy::default().without_artifact_strategy(),
            now_agent_timestamp(),
        )
        .map_err(RakkaA2AHandlerError::Mapping)
        .map_err(RakkaA2AHandlerError::into_a2a_error)?;
        Err(A2AError::unsupported_operation(STREAMING_UNIMPLEMENTED))
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
        let tenant = canonical_read_tenant(params, req.tenant.as_deref())
            .map_err(RakkaA2AHandlerError::Mapping)
            .map_err(RakkaA2AHandlerError::into_a2a_error)?;
        self.task_store
            .get(tenant.as_deref(), &req.id, req.history_length)
            .map_err(RakkaA2AHandlerError::Projection)
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.record(params);
        let tenant = canonical_read_tenant(params, req.tenant.as_deref())
            .map_err(RakkaA2AHandlerError::Mapping)
            .map_err(RakkaA2AHandlerError::into_a2a_error)?;
        let req = ListTasksRequest { tenant, ..req };
        self.task_store
            .list(&req)
            .map_err(RakkaA2AHandlerError::Projection)
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
        self.cancel_task_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        _req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.record(params);
        Ok(Box::pin(stream::once(async {
            Err(A2AError::unsupported_operation(STREAMING_UNIMPLEMENTED))
        })))
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        _req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.record(params);
        Err(A2AError::unsupported_operation(PUSH_UNIMPLEMENTED))
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        _req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.record(params);
        Err(A2AError::unsupported_operation(PUSH_UNIMPLEMENTED))
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        _req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.record(params);
        Err(A2AError::unsupported_operation(PUSH_UNIMPLEMENTED))
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        _req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.record(params);
        Err(A2AError::unsupported_operation(PUSH_UNIMPLEMENTED))
    }

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        _req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        self.record(params);
        Err(A2AError::unsupported_operation(format!(
            "extended agent card is not configured; use the public card for {}",
            self.agent_card.name
        )))
    }
}

fn projected_message(message: &Message, draft: &A2ACommandDraft) -> Message {
    let mut message = message.clone();
    message.task_id = Some(draft.normalized.task_id.clone());
    message.context_id = Some(draft.normalized.context_id.clone());
    message
}

fn artifact_refs(payload: &A2ACommandPayload) -> Vec<ArtifactRef> {
    payload
        .artifact_drafts()
        .iter()
        .map(|draft| draft.reference.clone())
        .collect()
}

fn state_payload(payload: &A2ACommandPayload) -> AgentStatePayload {
    match payload {
        A2ACommandPayload::Inline(inline) => AgentStatePayload::Inline(inline.clone()),
        A2ACommandPayload::ArtifactDrafts(drafts) => drafts
            .first()
            .map(|draft| AgentStatePayload::Artifact(draft.reference.clone()))
            .unwrap_or(AgentStatePayload::Empty),
        A2ACommandPayload::Empty => AgentStatePayload::Empty,
    }
}

fn return_immediately(req: &SendMessageRequest) -> bool {
    req.configuration
        .as_ref()
        .and_then(|config| config.return_immediately)
        .unwrap_or(false)
}

fn run_tenant(run_state: &AgentRunState) -> String {
    run_state
        .tenant
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| DEFAULT_TENANT.to_string())
}

fn run_is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
    )
}

fn task_state(status: AgentRunStatus) -> TaskState {
    crate::task_projection::task_state_from_run_status(status)
}
