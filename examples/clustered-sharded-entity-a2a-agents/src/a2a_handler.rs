//! Durable A2A request handler for the clustered Phase 3 example.
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
use rakka::agent_workflow::substrate::{
    DeduplicationKey, WorkflowError, WorkflowMessageId, WorkflowState,
};
use rakka::agent_workflow::{
    AgentCommand, AgentCommandKind, AgentInboxAcceptance, AgentInboxError, AgentRunEngineError,
    AgentRunId, AgentRunInbox, AgentRunRuntimeError, AgentRunState, AgentRunStatus,
    AgentRunTransition, AgentStatePayload, AgentStepRunner, AgentTimestampMillis, AgentWorkflow,
    ArtifactRef,
};
use rakka::persistence::{DurableError, DurableStateStore};
use rakka::prelude::{ClusterSharding, EntityTypeKey};
use rakka::remote::{RemoteRequestError, TcpRemoteTransport};
use rakka::sharding::{EntityAskError, RemoteEntityAskClient, RemoteEntityAskError};

use crate::a2a_mapping::{
    build_cancel_task_command_draft, build_send_message_command_draft, canonical_read_tenant,
    now_agent_timestamp, A2ACommandDraft, A2ACommandPayload, A2AMappingError, A2APayloadPolicy,
    A2ATaskIntent, ATTR_CONTEXT_ID, DEFAULT_TENANT,
};
use crate::durable_stores::{RunStore, WorkflowStore};
use crate::protocol::{
    A2AProjectionHints, A2ARunCommandMetadata, A2ARunFailureKind, A2ARunRequest, A2ARunRequestKind,
    A2ARunResponse, A2ARunResponseKind, A2ATimeoutPolicy, A2A_RUN_PROTOCOL_VERSION,
};
use crate::reachability::PeerReachability;
use crate::sharded_run_entity::A2ARunEntityCommand;
use crate::support::RUN_ASK_TIMEOUT;
use crate::task_projection::{
    status_transition_allowed, A2ATaskEventPayload, A2ATaskProjection,
    InMemoryA2ATaskProjectionStore, TaskProjectionError,
};

const STREAMING_UNIMPLEMENTED: &str =
    "A2A streaming is intentionally deferred until the streaming phase";
const PUSH_UNIMPLEMENTED: &str =
    "A2A push notifications are intentionally deferred until the push phase";
/// Bound on optimistic-concurrency re-drives for inbox accepts and run
/// transitions; each attempt requires a distinct concurrent writer, so the
/// bound is a livelock guard rather than a functional limit.
const MAX_CONFLICT_ATTEMPTS: usize = 3;

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

/// Cluster routing helper for owner-only A2A run requests.
#[derive(Clone)]
pub struct A2ARunRouter {
    sharding: ClusterSharding,
    key: EntityTypeKey<A2ARunEntityCommand>,
    ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
    reachability: PeerReachability,
}

impl A2ARunRouter {
    /// Creates a router over the shared cluster sharding facade.
    #[must_use]
    pub fn new(
        sharding: ClusterSharding,
        key: EntityTypeKey<A2ARunEntityCommand>,
        ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
        reachability: PeerReachability,
    ) -> Self {
        Self {
            sharding,
            key,
            ask_client,
            reachability,
        }
    }

    async fn route(&self, request: A2ARunRequest) -> Result<A2ARunResponse, RakkaA2AHandlerError> {
        let entity = self
            .sharding
            .entity_ref_for(&self.key, request.task_id.clone())
            .map_err(|error| RakkaA2AHandlerError::Unavailable {
                message: error.to_string(),
            })?;
        let (owner, _shard) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| RakkaA2AHandlerError::Unavailable {
                message: error.to_string(),
            })?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local| local == &owner);

        if is_local {
            entity
                .ask(
                    |reply_to| A2ARunEntityCommand::Handle { request, reply_to },
                    RUN_ASK_TIMEOUT,
                )
                .await
                .map_err(entity_ask_error)
        } else {
            let outcome = entity
                .remote_ask(&self.ask_client, request, RUN_ASK_TIMEOUT)
                .await;
            record_remote_outcome(&self.reachability, &outcome);
            outcome.map_err(remote_ask_error)
        }
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
    /// Actor-backed owner runtime failed.
    RunActor(AgentRunRuntimeError),
    /// Durable-state store query failed.
    Persistence(DurableError),
    /// The owning entity or peer was temporarily unavailable.
    Unavailable {
        /// Stable retryable summary.
        message: String,
    },
    /// Local owner actor ask failed before the command reached durable state.
    OwnerAsk {
        /// Stable failure summary.
        message: String,
    },
    /// The requested run was not found before accepting a continuation command.
    MissingRun {
        /// Missing public task id.
        task_id: String,
    },
    /// The task is already in a terminal state and cannot be cancelled.
    TaskNotCancelable {
        /// Public task id.
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
            Self::RunActor(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::Unavailable { .. } => "a2a-run-owner-unavailable",
            Self::OwnerAsk { .. } => "a2a-run-owner-ask",
            Self::MissingRun { .. } => "task-not-found",
            Self::TaskNotCancelable { .. } => "task-not-cancelable",
            Self::InvalidLifecycle { .. } => "invalid-command-lifecycle",
        }
    }

    pub(crate) fn into_a2a_error(self) -> A2AError {
        let code = self.code();
        match self {
            Self::Projection(TaskProjectionError::TaskNotFound { task_id })
            | Self::MissingRun { task_id } => A2AError::task_not_found(&task_id),
            Self::TaskNotCancelable { task_id } => A2AError::task_not_cancelable(&task_id),
            Self::Mapping(error) => A2AError::invalid_params(format!("{code}: {error}")),
            Self::Projection(error) => A2AError::invalid_params(format!("{code}: {error}")),
            Self::InvalidLifecycle { reason, .. } => {
                A2AError::invalid_params(format!("{code}: {reason}"))
            }
            Self::RunEngine(AgentRunEngineError::MissingRunState { run_id }) => {
                A2AError::task_not_found(run_id.as_str())
            }
            Self::RunActor(AgentRunRuntimeError::RunEngine {
                error: AgentRunEngineError::MissingRunState { run_id },
            }) => A2AError::task_not_found(run_id.as_str()),
            Self::Inbox(error) => A2AError::internal(format!("{code}: {error}")),
            Self::RunEngine(error) => A2AError::internal(format!("{code}: {error}")),
            Self::RunActor(error) => A2AError::internal(format!("{code}: {error}")),
            Self::Persistence(error) => A2AError::internal(format!("{code}: {error}")),
            Self::Unavailable { message } => A2AError::internal(format!("{code}: {message}")),
            Self::OwnerAsk { message } => A2AError::internal(format!("{code}: {message}")),
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
            Self::RunActor(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::Unavailable { message } | Self::OwnerAsk { message } => f.write_str(message),
            Self::MissingRun { task_id } => write!(f, "task not found: {task_id}"),
            Self::TaskNotCancelable { task_id } => {
                write!(f, "task {task_id} is terminal and cannot be cancelled")
            }
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
            Self::RunActor(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::Unavailable { .. } | Self::OwnerAsk { .. } => None,
            Self::MissingRun { .. }
            | Self::TaskNotCancelable { .. }
            | Self::InvalidLifecycle { .. } => None,
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

impl From<AgentRunRuntimeError> for RakkaA2AHandlerError {
    fn from(error: AgentRunRuntimeError) -> Self {
        Self::RunActor(error)
    }
}

impl From<DurableError> for RakkaA2AHandlerError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

/// A2A handler implementation backed by durable Rakka stores and optional sharded routing.
pub struct RakkaA2ARequestHandler {
    agent_card: AgentCard,
    workflow: AgentWorkflow,
    task_store: InMemoryA2ATaskProjectionStore,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    header_observer: HeaderObserver,
    router: Option<A2ARunRouter>,
}

impl RakkaA2ARequestHandler {
    /// Creates a local durable handler.
    #[must_use]
    pub fn new(
        agent_card: AgentCard,
        workflow: AgentWorkflow,
        task_store: InMemoryA2ATaskProjectionStore,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        header_observer: HeaderObserver,
    ) -> Self {
        Self {
            agent_card,
            workflow,
            task_store,
            run_store,
            workflow_store,
            header_observer,
            router: None,
        }
    }

    /// Creates a durable handler that routes owner-only work through sharding.
    #[must_use]
    pub fn new_clustered(
        agent_card: AgentCard,
        workflow: AgentWorkflow,
        task_store: InMemoryA2ATaskProjectionStore,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        header_observer: HeaderObserver,
        router: A2ARunRouter,
    ) -> Self {
        let mut handler = Self::new(
            agent_card,
            workflow,
            task_store,
            run_store,
            workflow_store,
            header_observer,
        );
        handler.router = Some(router);
        handler
    }

    /// Rebuilds any missing local task projections from durable run state.
    pub async fn recover_task_projections(&self) -> Result<usize, RakkaA2AHandlerError> {
        self.recover_task_projections_impl().await
    }

    fn record(&self, params: &ServiceParams) {
        self.header_observer.record(params);
    }

    async fn route_for_projection(
        &self,
        router: &A2ARunRouter,
        request: A2ARunRequest,
    ) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
        let response = router.route(request).await?;
        projection_from_response(response).inspect(|projection| {
            self.task_store.upsert(projection.clone());
        })
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
        if let Some(router) = &self.router {
            let tenant = draft.normalized.tenant.as_str().to_string();
            let request = A2ARunRequest::new(
                draft.normalized.task_id.clone(),
                Some(tenant),
                A2ARunCommandMetadata::from_draft(&draft),
                projection_hints(
                    req.configuration
                        .as_ref()
                        .and_then(|config| config.history_length),
                ),
                A2ATimeoutPolicy::from_duration(RUN_ASK_TIMEOUT),
                A2ARunRequestKind::AcceptMessage {
                    projected_message: Box::new(projected_message(&req.message, &draft)),
                    artifacts: artifact_refs(&draft.payload),
                    return_immediately: return_immediately(&req),
                    received_at,
                    draft: Box::new(draft),
                },
            );
            let projection = self.route_for_projection(router, request).await?;
            let task = projection.to_task(
                req.configuration
                    .as_ref()
                    .and_then(|config| config.history_length),
                true,
            );
            return Ok(SendMessageResponse::Task(task));
        }
        // One durable recovery serves the whole request: lifecycle validation
        // and every run transition below reuse this runner instead of
        // re-loading run state per step.
        let mut runner = self.recovered_runner(draft.normalized.run_id()).await?;
        let existing_state = runner.state()?.cloned();
        validate_send_lifecycle(&draft, existing_state.as_ref())?;
        self.accept_draft(&draft, existing_state.is_some()).await?;
        let mut run_state = match existing_state {
            Some(state) => state,
            None => {
                let (state, adopted) = self.start_run(&mut runner, &draft, received_at).await?;
                if adopted {
                    validate_adopted_run(&state, &draft)?;
                }
                state
            }
        };

        let projected_message = projected_message(&req.message, &draft);
        let artifacts = artifact_refs(&draft.payload);
        // The first transition is driven by recovered run state rather than
        // fresh acceptance so a retried command can complete a partially
        // applied start instead of leaving the run stuck in `Accepted`.
        if !return_immediately(&req) && run_state.status == AgentRunStatus::Accepted {
            run_state = self
                .begin_first_transition(&mut runner, received_at)
                .await?;
        }
        self.project_send_result(
            &draft,
            &projected_message,
            artifacts,
            &run_state,
            received_at,
        )?;
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
        if let Some(router) = &self.router {
            let tenant = draft.normalized.tenant.as_str().to_string();
            let request = A2ARunRequest::new(
                draft.normalized.task_id.clone(),
                Some(tenant),
                A2ARunCommandMetadata::from_draft(&draft),
                A2AProjectionHints::default(),
                A2ATimeoutPolicy::from_duration(RUN_ASK_TIMEOUT),
                A2ARunRequestKind::CancelTask {
                    draft: Box::new(draft),
                    received_at,
                },
            );
            let projection = self.route_for_projection(router, request).await?;
            return Ok(projection.to_task(None, true));
        }
        let mut runner = self.recovered_runner(draft.normalized.run_id()).await?;
        let mut run_state = runner
            .state()?
            .cloned()
            .ok_or_else(|| missing_run(&draft.normalized.task_id))?;
        // A run owned by another tenant must be indistinguishable from a
        // missing task, and must never be cancelled by this request.
        if run_tenant(&run_state) != draft.normalized.tenant.as_str() {
            return Err(missing_run(&draft.normalized.task_id));
        }
        if run_is_terminal(run_state.status) {
            // Converge the projection to the terminal truth before rejecting
            // so a follow-up read observes the final state, then answer with
            // the protocol's canonical error for terminal cancels.
            self.sync_status_projection(
                &run_state,
                &draft.normalized.context_id,
                received_at,
                None,
            )?;
            return Err(RakkaA2AHandlerError::TaskNotCancelable {
                task_id: draft.normalized.task_id.clone(),
            });
        }
        self.accept_draft(&draft, true).await?;
        // State-driven like the send path: whether this acceptance was fresh
        // or a duplicate retry, drive the run through Cancelling to the
        // durable terminal Cancelled state so a retry can complete a
        // partially applied cancellation.
        run_state = self
            .apply_cancellation(&mut runner, run_state, received_at)
            .await?;
        self.sync_status_projection(&run_state, &draft.normalized.context_id, received_at, None)?;
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
            // This pass only reads state, so load the record directly instead
            // of building a workflow-bearing runner per run.
            let Some(record) = self.run_store.load(&persistence_id).await? else {
                continue;
            };
            let state = record.state;
            let tenant = run_tenant(&state);
            if self
                .task_store
                .projection(Some(&tenant), state.run_id.as_str())
                .is_ok()
            {
                continue;
            }
            let context_id = self
                .recover_context_id(&run_id)
                .await?
                .unwrap_or_else(|| state.run_id.as_str().to_string());
            self.snapshot_projection(
                &state,
                &context_id,
                Vec::new(),
                Vec::new(),
                state.updated_at,
            )?;
            recovered += 1;
        }
        Ok(recovered)
    }

    /// Recovers the original A2A context id from the run's durable inbox.
    async fn recover_context_id(
        &self,
        run_id: &AgentRunId,
    ) -> Result<Option<String>, RakkaA2AHandlerError> {
        let mut inbox = AgentRunInbox::new(run_id.clone(), self.workflow_store.clone());
        let state = inbox.recover().await?;
        let mut fallback = None;
        for entry in state.inbox().values() {
            let command = match serde_json::from_slice::<AgentCommand>(entry.payload()) {
                Ok(command) => command,
                Err(error) => {
                    // Surface undecodable durable payloads instead of silently
                    // degrading recovery, matching the crate's typed
                    // deserialization errors on the outbox/dispatcher paths.
                    eprintln!(
                        "warning: skipping undecodable inbox entry {} for run {}: {error}",
                        entry.message_id().as_str(),
                        run_id.as_str(),
                    );
                    continue;
                }
            };
            let context_id = command.attributes.get(ATTR_CONTEXT_ID).cloned();
            if context_id.is_none() {
                continue;
            }
            if matches!(command.kind, AgentCommandKind::StartRun) {
                return Ok(context_id);
            }
            fallback = fallback.or(context_id);
        }
        Ok(fallback)
    }

    async fn accept_draft(
        &self,
        draft: &A2ACommandDraft,
        run_exists: bool,
    ) -> Result<AgentInboxAcceptance, RakkaA2AHandlerError> {
        let mut inbox = AgentRunInbox::new(draft.normalized.run_id(), self.workflow_store.clone());
        let state = inbox.recover().await?;
        // A brand-new message whose generated task id resolves to an existing
        // run that has never seen this command means the hashed id collided
        // with an unrelated task. Reject before accepting anything durably
        // instead of silently merging the two tasks. This probe requires
        // inbox entries to outlive run state: wiring inbox compaction or
        // divergent run/inbox retention would turn legitimate late retries
        // into false collisions.
        if run_exists
            && matches!(draft.normalized.intent, A2ATaskIntent::NewTask)
            && !known_command(state, draft)
        {
            return Err(RakkaA2AHandlerError::InvalidLifecycle {
                task_id: draft.normalized.task_id.clone(),
                reason: "generated task id collides with an existing task",
            });
        }
        // A concurrent request can win the inbox write; re-recover and retry
        // a bounded number of times — each retry dedupes against the winner's
        // entry or accepts cleanly at the new revision, mirroring the
        // run-store transitions.
        let mut attempts = 0;
        loop {
            match inbox.accept_command(draft.command.clone()).await {
                Ok(acceptance) => return Ok(acceptance),
                Err(AgentInboxError::Workflow {
                    error: WorkflowError::RevisionConflict { .. },
                }) if attempts + 1 < MAX_CONFLICT_ATTEMPTS => {
                    attempts += 1;
                    inbox.recover().await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Starts the run, returning the state and whether a concurrent winner's
    /// run was adopted instead of created (so the caller can re-validate).
    async fn start_run(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        draft: &A2ACommandDraft,
        now: AgentTimestampMillis,
    ) -> Result<(AgentRunState, bool), RakkaA2AHandlerError> {
        let initial = self.initial_run_state(draft, now)?;
        match runner.start(initial).await {
            Ok(transition) => Ok((transition.state, false)),
            // A concurrent request created the run between our recovery and
            // this write; adopt the winner's state instead of failing.
            Err(
                AgentRunEngineError::AlreadyStarted { .. }
                | AgentRunEngineError::Persistence {
                    error: DurableError::RevisionConflict { .. },
                    ..
                },
            ) => Ok((refreshed_state(runner).await?, true)),
            Err(error) => Err(error.into()),
        }
    }

    async fn begin_first_transition(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let begin_at = AgentTimestampMillis::new(now.as_millis().saturating_add(1));
        let result = runner.begin_step(begin_at).await;
        adopt_on_conflict(runner, result).await
    }

    /// Drives the run to the durable terminal `Cancelled` state.
    ///
    /// Nothing is in flight in this phase, so a durably accepted cancellation
    /// completes immediately: `Cancelling` is transient and the public task
    /// state becomes the terminal `Canceled` instead of reading as `Working`
    /// forever. This is phase-local lifecycle policy — once step execution
    /// and in-flight effects exist, completion must move to the executor,
    /// which has to drain outstanding work before calling `cancel`.
    ///
    /// The loop re-drives the cancellation when a concurrent transition (for
    /// example a racing send's first `begin_step`) wins the revision race
    /// mid-way; only `Accepted`, `Running`, `Cancelling`, and `Cancelled` are
    /// reachable in this phase, so it converges within the bound.
    async fn apply_cancellation(
        &self,
        runner: &mut AgentStepRunner<RunStore>,
        run_state: AgentRunState,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let mut state = run_state;
        for _ in 0..MAX_CONFLICT_ATTEMPTS {
            if run_is_terminal(state.status) {
                return Ok(state);
            }
            let result = if state.status == AgentRunStatus::Cancelling {
                let completed_at = AgentTimestampMillis::new(now.as_millis().saturating_add(1));
                runner.cancel(completed_at).await
            } else {
                runner
                    .request_cancellation(
                        "a2a-cancel",
                        Some("A2A client requested cancellation".to_string()),
                        now,
                    )
                    .await
            };
            state = adopt_on_conflict(runner, result).await?;
        }
        Ok(state)
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
        now: AgentTimestampMillis,
    ) -> Result<(), RakkaA2AHandlerError> {
        let tenant = draft.normalized.tenant.as_str();
        match self
            .task_store
            .projection(Some(tenant), &draft.normalized.task_id)
        {
            Ok(projection) => {
                // The message is appended by history presence, not by fresh
                // acceptance: a durably accepted command whose projection
                // write was lost must be healed by its retry, while ordinary
                // duplicates find their message already recorded. (A message
                // evicted from the bounded history would be re-appended by a
                // very late duplicate; acceptable for this local example.)
                let already_projected = projection
                    .history
                    .iter()
                    .any(|recorded| recorded.message_id == message.message_id);
                if !already_projected {
                    self.task_store.append_event_payload(
                        tenant,
                        &draft.normalized.task_id,
                        &draft.normalized.context_id,
                        now,
                        A2ATaskEventPayload::MessageUpdate {
                            message: message.clone(),
                        },
                    )?;
                }
                self.sync_status_projection(
                    run_state,
                    &draft.normalized.context_id,
                    now,
                    Some(projection.status),
                )
            }
            Err(TaskProjectionError::TaskNotFound { .. }) => self.snapshot_projection(
                run_state,
                &draft.normalized.context_id,
                vec![message.clone()],
                artifacts,
                now,
            ),
            Err(error) => Err(error.into()),
        }
    }

    /// Brings the task projection in line with durable run state, creating it
    /// when missing and appending a status event only when the public task
    /// state actually changed. `current_status` skips the projection read
    /// when the caller already holds it.
    fn sync_status_projection(
        &self,
        run_state: &AgentRunState,
        context_id: &str,
        now: AgentTimestampMillis,
        current_status: Option<TaskState>,
    ) -> Result<(), RakkaA2AHandlerError> {
        let tenant = run_tenant(run_state);
        let state = task_state(run_state.status);
        let current = match current_status {
            Some(status) => status,
            None => match self
                .task_store
                .projection(Some(&tenant), run_state.run_id.as_str())
            {
                Ok(projection) => projection.status,
                Err(TaskProjectionError::TaskNotFound { .. }) => {
                    return self.snapshot_projection(
                        run_state,
                        context_id,
                        Vec::new(),
                        Vec::new(),
                        now,
                    );
                }
                Err(error) => return Err(error.into()),
            },
        };
        if current == state {
            return Ok(());
        }
        // The shared no-regression rule (also enforced inside apply_event)
        // is checked here first so a disallowed transition — e.g. from a
        // stale run-state snapshot — appends no event at all.
        if !status_transition_allowed(&current, &state) {
            return Ok(());
        }
        let payload = if state.is_terminal() {
            A2ATaskEventPayload::Terminal { state }
        } else {
            A2ATaskEventPayload::StatusUpdate { state }
        };
        self.task_store.append_event_payload(
            tenant.as_str(),
            run_state.run_id.as_str(),
            context_id,
            now,
            payload,
        )?;
        Ok(())
    }

    fn snapshot_projection(
        &self,
        run_state: &AgentRunState,
        context_id: &str,
        history: Vec<Message>,
        artifacts: Vec<ArtifactRef>,
        now: AgentTimestampMillis,
    ) -> Result<(), RakkaA2AHandlerError> {
        let projection =
            A2ATaskProjection::from_run_state(run_state, context_id, history, artifacts, 0);
        let tenant = projection.tenant.clone();
        let task_id = projection.task_id.clone();
        let context_id = projection.context_id.clone();
        self.task_store.append_event_payload(
            tenant,
            task_id,
            context_id,
            now,
            A2ATaskEventPayload::Snapshot(projection),
        )?;
        Ok(())
    }
}

/// Validates a send request's lifecycle intent against recovered run state.
pub(crate) fn validate_send_lifecycle(
    draft: &A2ACommandDraft,
    run_state: Option<&AgentRunState>,
) -> Result<(), RakkaA2AHandlerError> {
    if matches!(draft.normalized.intent, A2ATaskIntent::NewTask)
        && !matches!(&draft.command.kind, AgentCommandKind::StartRun)
    {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "new A2A tasks must map to StartRun",
        });
    }
    match run_state {
        // A run owned by another tenant is indistinguishable from a missing
        // task to this caller.
        Some(state) if run_tenant(state) != draft.normalized.tenant.as_str() => {
            Err(RakkaA2AHandlerError::MissingRun {
                task_id: draft.normalized.task_id.clone(),
            })
        }
        // Terminal tasks reject new messages before anything is accepted
        // durably, mirroring the terminal handling on the cancel path.
        Some(state)
            if matches!(draft.normalized.intent, A2ATaskIntent::ContinueTask)
                && run_is_terminal(state.status) =>
        {
            Err(RakkaA2AHandlerError::InvalidLifecycle {
                task_id: draft.normalized.task_id.clone(),
                reason: "messages cannot be sent to a task in a terminal state",
            })
        }
        None if matches!(draft.normalized.intent, A2ATaskIntent::ContinueTask) => {
            Err(RakkaA2AHandlerError::MissingRun {
                task_id: draft.normalized.task_id.clone(),
            })
        }
        _ => Ok(()),
    }
}

/// Re-validates a run adopted from a concurrent winner on the start path.
///
/// A retry of the same message adopts a run identical to the one this request
/// would have created; anything else means the hashed id collided with an
/// unrelated task. On rejection the command accepted just before the start
/// race remains in the adopted run's inbox — unreachable garbage that only a
/// hash collision plus a concurrent create can produce.
pub(crate) fn validate_adopted_run(
    state: &AgentRunState,
    draft: &A2ACommandDraft,
) -> Result<(), RakkaA2AHandlerError> {
    if run_tenant(state) != draft.normalized.tenant.as_str() {
        return Err(missing_run(&draft.normalized.task_id));
    }
    if !same_state_payload(&state.state_payload, &state_payload(&draft.payload)) {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "generated task id collides with an existing task",
        });
    }
    Ok(())
}

/// Compares run payloads semantically rather than byte-for-byte.
///
/// Inline payloads hold a serialized A2A message whose map fields (`Message`
/// and `Part` metadata) have no deterministic wire ordering, so identical
/// messages can serialize to different bytes; deserialize and compare the
/// messages instead, falling back to byte equality for non-message payloads.
pub(crate) fn same_state_payload(
    existing: &AgentStatePayload,
    candidate: &AgentStatePayload,
) -> bool {
    match (existing, candidate) {
        (AgentStatePayload::Inline(existing), AgentStatePayload::Inline(candidate)) => {
            if existing.content_type != candidate.content_type {
                return false;
            }
            match (
                serde_json::from_slice::<Message>(&existing.bytes),
                serde_json::from_slice::<Message>(&candidate.bytes),
            ) {
                (Ok(existing), Ok(candidate)) => existing == candidate,
                _ => existing.bytes == candidate.bytes,
            }
        }
        (existing, candidate) => existing == candidate,
    }
}

/// Returns true when the run's durable inbox already holds this command,
/// using the inbox's own keyed lookups so the match cannot drift from the
/// acceptance-time duplicate detection.
pub(crate) fn known_command(state: &WorkflowState, draft: &A2ACommandDraft) -> bool {
    let command_id = WorkflowMessageId::new(draft.command.metadata.command_id.as_str());
    let deduplication_key =
        DeduplicationKey::new(draft.command.metadata.deduplication_key.as_str());
    state.inbox_entry(&command_id).is_some()
        || state
            .inbox_entry_by_deduplication_key(&deduplication_key)
            .is_some()
}

/// Resolves a run transition, adopting the concurrent winner's state when the
/// optimistic write lost a revision race.
async fn adopt_on_conflict(
    runner: &mut AgentStepRunner<RunStore>,
    result: Result<AgentRunTransition, AgentRunEngineError>,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    match result {
        Ok(transition) => Ok(transition.state),
        Err(AgentRunEngineError::Persistence {
            error: DurableError::RevisionConflict { .. },
            ..
        }) => refreshed_state(runner).await,
        Err(error) => Err(error.into()),
    }
}

/// Re-recovers the runner and returns the current durable run state.
async fn refreshed_state(
    runner: &mut AgentStepRunner<RunStore>,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    runner.recover().await?;
    let run_id = runner.run_id().as_str().to_string();
    runner.state()?.cloned().ok_or_else(|| missing_run(&run_id))
}

/// Constructs the task-not-found error used for missing and foreign-tenant runs.
pub(crate) fn missing_run(task_id: &str) -> RakkaA2AHandlerError {
    RakkaA2AHandlerError::MissingRun {
        task_id: task_id.to_string(),
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
        if let Some(router) = &self.router {
            // A missing tenant stays `None` end-to-end: the owner resolves
            // the run's stored tenant, matching the local unscoped read path
            // instead of forcing the local-development default tenant.
            let request = A2ARunRequest::new(
                req.id.clone(),
                tenant,
                A2ARunCommandMetadata::query(),
                projection_hints(req.history_length),
                A2ATimeoutPolicy::from_duration(RUN_ASK_TIMEOUT),
                A2ARunRequestKind::QueryTaskSnapshot,
            );
            let projection = self
                .route_for_projection(router, request)
                .await
                .map_err(RakkaA2AHandlerError::into_a2a_error)?;
            return Ok(projection.to_task(req.history_length, true));
        }
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

pub(crate) fn projected_message(message: &Message, draft: &A2ACommandDraft) -> Message {
    let mut message = message.clone();
    message.task_id = Some(draft.normalized.task_id.clone());
    message.context_id = Some(draft.normalized.context_id.clone());
    message
}

fn projection_hints(history_length: Option<i32>) -> A2AProjectionHints {
    A2AProjectionHints::new(history_length, true)
}

fn projection_from_response(
    response: A2ARunResponse,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    if response.version != A2A_RUN_PROTOCOL_VERSION {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: response.task_id,
            reason: "owner response protocol version mismatch",
        });
    }
    match response.outcome {
        A2ARunResponseKind::TaskSnapshot { projection } => Ok(projection),
        A2ARunResponseKind::Failure { failure } => Err(owner_failure_error(
            &response.task_id,
            failure.kind,
            failure.message,
        )),
        A2ARunResponseKind::StreamCursor { .. }
        | A2ARunResponseKind::PushConfigRecorded { .. }
        | A2ARunResponseKind::PushConfigDeleted => Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: response.task_id,
            reason: "owner returned an unexpected response kind",
        }),
    }
}

fn owner_failure_error(
    task_id: &str,
    kind: A2ARunFailureKind,
    message: String,
) -> RakkaA2AHandlerError {
    match kind {
        A2ARunFailureKind::TaskNotFound => missing_run(task_id),
        A2ARunFailureKind::TaskNotCancelable => RakkaA2AHandlerError::TaskNotCancelable {
            task_id: task_id.to_string(),
        },
        A2ARunFailureKind::InvalidRequest
        | A2ARunFailureKind::VersionMismatch
        | A2ARunFailureKind::Unsupported => RakkaA2AHandlerError::InvalidLifecycle {
            task_id: task_id.to_string(),
            reason: "owner rejected the request",
        },
        A2ARunFailureKind::Unavailable => RakkaA2AHandlerError::Unavailable { message },
        A2ARunFailureKind::Internal => RakkaA2AHandlerError::OwnerAsk { message },
    }
}

fn artifact_refs(payload: &A2ACommandPayload) -> Vec<ArtifactRef> {
    payload
        .artifact_drafts()
        .iter()
        .map(|draft| draft.reference.clone())
        .collect()
}

pub(crate) fn state_payload(payload: &A2ACommandPayload) -> AgentStatePayload {
    match payload {
        A2ACommandPayload::Inline(inline) => AgentStatePayload::Inline(inline.clone()),
        A2ACommandPayload::ArtifactDrafts(drafts) => drafts
            .first()
            .map(|draft| AgentStatePayload::Artifact(draft.reference.clone()))
            .unwrap_or(AgentStatePayload::Empty),
        A2ACommandPayload::Empty => AgentStatePayload::Empty,
    }
}

pub(crate) fn return_immediately(req: &SendMessageRequest) -> bool {
    req.configuration
        .as_ref()
        .and_then(|config| config.return_immediately)
        .unwrap_or(false)
}

pub(crate) fn run_tenant(run_state: &AgentRunState) -> String {
    run_state
        .tenant
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| DEFAULT_TENANT.to_string())
}

pub(crate) fn run_is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
    )
}

pub(crate) fn task_state(status: AgentRunStatus) -> TaskState {
    crate::task_projection::task_state_from_run_status(status)
}

fn record_remote_outcome(
    reachability: &PeerReachability,
    outcome: &Result<A2ARunResponse, RemoteEntityAskError>,
) {
    match outcome {
        Ok(_) => reachability.record(true),
        Err(error) if is_peer_unreachable(error) => reachability.record(false),
        Err(_) => {}
    }
}

/// Classifies which remote ask failures count as peer-unreachability
/// evidence for self-fencing.
///
/// Only transport send failures qualify. Reply timeouts are deliberately
/// neutral: the ingress ask budget can elapse while a healthy owner is still
/// doing durable work, so counting timeouts would let a slow peer fence a
/// healthy ingress node out of the cluster. Validation and codec errors are
/// ignored per the routing contract.
fn is_peer_unreachable(error: &RemoteEntityAskError) -> bool {
    matches!(error, RemoteEntityAskError::Send { .. })
}

fn entity_ask_error(error: EntityAskError) -> RakkaA2AHandlerError {
    match error {
        EntityAskError::NoRoute(error) => RakkaA2AHandlerError::Unavailable {
            message: error.to_string(),
        },
        EntityAskError::NotLocal { owner } => RakkaA2AHandlerError::Unavailable {
            message: format!("entity owned by {owner}"),
        },
        EntityAskError::MailboxFull => RakkaA2AHandlerError::Unavailable {
            message: "entity mailbox full".to_string(),
        },
        EntityAskError::MailboxClosed => RakkaA2AHandlerError::Unavailable {
            message: "entity mailbox closed".to_string(),
        },
        EntityAskError::ShardHandoff { shard_id, state } => RakkaA2AHandlerError::Unavailable {
            message: format!("shard {shard_id} is {state}"),
        },
        EntityAskError::ShardBufferFull { shard_id, .. } => RakkaA2AHandlerError::Unavailable {
            message: format!("shard {shard_id} buffer full"),
        },
        EntityAskError::Timeout => RakkaA2AHandlerError::Unavailable {
            message: "entity ask timed out".to_string(),
        },
        EntityAskError::ReplyDropped => RakkaA2AHandlerError::OwnerAsk {
            message: "entity reply dropped".to_string(),
        },
        EntityAskError::SpawnFailed(message)
        | EntityAskError::RemoteEncode(message)
        | EntityAskError::RemoteSend(message)
        | EntityAskError::Rejected(message) => RakkaA2AHandlerError::OwnerAsk { message },
    }
}

fn remote_ask_error(error: RemoteEntityAskError) -> RakkaA2AHandlerError {
    match error {
        RemoteEntityAskError::NoRoute { error } => RakkaA2AHandlerError::Unavailable {
            message: error.to_string(),
        },
        RemoteEntityAskError::Send { message } => RakkaA2AHandlerError::Unavailable { message },
        RemoteEntityAskError::Encode { error } => RakkaA2AHandlerError::OwnerAsk {
            message: error.to_string(),
        },
        RemoteEntityAskError::Register { error } => RakkaA2AHandlerError::OwnerAsk {
            message: error.to_string(),
        },
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => RakkaA2AHandlerError::Unavailable {
                message: "remote ask timed out".to_string(),
            },
            RemoteRequestError::ReplyDropped => RakkaA2AHandlerError::OwnerAsk {
                message: "remote reply dropped".to_string(),
            },
            other => RakkaA2AHandlerError::OwnerAsk {
                message: other.to_string(),
            },
        },
    }
}
