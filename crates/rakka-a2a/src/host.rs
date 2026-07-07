//! Clustered sharded A2A run host (the `sharding` feature).
//!
//! [`A2ARunEntity`] is the cluster-addressable owner shell for one A2A
//! task/run. Non-owning public ingress nodes route serializable
//! [`A2ARunRequest`] values to this entity, which maps each request to local
//! [`AgentRunActorCommand`] messages and durable projection operations. Those
//! local actor commands are never serialized over the wire; only the
//! remote-safe protocol is.
//!
//! Idle entities passivate; durable run, inbox, and projection state recover
//! lazily on the next access, so owner restart and shard movement are
//! transparent to clients.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rakka_agent_workflow::substrate::WorkflowError;
use rakka_agent_workflow::{
    AgentCommand, AgentInboxAcceptance, AgentInboxError, AgentRunActor, AgentRunActorCommand,
    AgentRunActorSnapshot, AgentRunEngineError, AgentRunId, AgentRunInbox, AgentRunRuntimeError,
    AgentRunRuntimeResult, AgentRunState, AgentRunStatus, AgentRunTransition, AgentTimestampMillis,
    AgentWorkflow, ArtifactRef,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorRef, ActorSystem, AskError,
    ReplyTo, TerminationReason,
};
use rakka_persistence::DurableError;
use rakka_sharding::{
    ClusterNodeRuntime, ClusterSharding, ClusterShardingResult, Entity, EntityContext,
    EntityTypeKey, EntityTypeRegistration,
};

use crate::error::RakkaA2AHandlerError;
use crate::mapping::{A2ACommandDraft, A2ATaskIntent, DEFAULT_TENANT};
use crate::projection::A2ATaskProjectionStore;
use crate::protocol::{
    A2ARunFailure, A2ARunFailureKind, A2ARunRequest, A2ARunRequestKind, A2ARunResponse,
    A2A_RUN_PROTOCOL_VERSION,
};
use crate::push::{schedule_push_effects_for_events, A2APushConfigStore};
use crate::runsync::{
    initial_run_state, known_command, missing_run, project_send_result, recover_context_id,
    run_is_terminal, run_tenant, snapshot_projection, sync_status_projection, validate_adopted_run,
    validate_send_lifecycle,
};
use crate::stores::{A2ARunStateStore, A2AWorkflowStateStore};
use crate::task::{A2ATaskEvent, A2ATaskProjection, TaskProjectionError};

/// Default entity type name for sharded A2A runs.
pub const DEFAULT_ENTITY_TYPE: &str = "A2AAgentRun";
/// Default shard count for the A2A run entity type.
pub const DEFAULT_NUMBER_OF_SHARDS: u32 = 32;
/// Default idle passivation for A2A run entities.
///
/// Passivation keeps read-only probes for arbitrary task ids from pinning an
/// entity and child run actor forever; durable state recovers lazily on the
/// next reference.
pub const DEFAULT_IDLE_PASSIVATION: Duration = Duration::from_secs(120);
/// Default ask timeout for owner and child run-actor requests.
pub const DEFAULT_RUN_ASK_TIMEOUT: Duration = Duration::from_secs(3);

const MAX_CONFLICT_ATTEMPTS: usize = 3;

/// Monotonic suffix keeping child actor names unique across entity
/// re-activations: a passivated entity's child may still be terminating when
/// the same entity id is activated again, so reusing the previous child name
/// would collide with the still-registered actor path.
static CHILD_INSTANCE: AtomicU64 = AtomicU64::new(0);

/// Local-only command protocol for the sharded owner shell.
pub enum A2ARunEntityCommand {
    /// Handle a remote-safe owner request.
    Handle {
        /// Remote-safe request.
        request: A2ARunRequest,
        /// Reply channel for the remote-safe response.
        reply_to: ReplyTo<A2ARunResponse>,
    },
}

/// Per-node configuration for hosting A2A run entities.
///
/// The run store, workflow store, task projection store, and push config
/// store handles supplied here must be the **same shared handles** used by
/// the public request handler, so ingress reads and owner writes observe one
/// durable truth.
pub struct A2ARunHost {
    /// Workflow definition every run entity hosts.
    pub workflow: AgentWorkflow,
    /// Shared durable store for run state.
    pub run_store: A2ARunStateStore,
    /// Shared durable store for inbox/outbox workflow state.
    pub workflow_store: A2AWorkflowStateStore,
    /// Shared task projection store.
    pub task_store: std::sync::Arc<dyn A2ATaskProjectionStore>,
    /// Shared durable push config store.
    pub push_configs: A2APushConfigStore,
    /// How long an idle entity stays resident before passivating.
    pub idle_passivation: Duration,
    /// Ask timeout for the entity's child run-actor requests.
    pub run_ask_timeout: Duration,
}

impl A2ARunHost {
    /// Creates a host with default passivation and ask-timeout settings.
    #[must_use]
    pub fn new(
        workflow: AgentWorkflow,
        run_store: A2ARunStateStore,
        workflow_store: A2AWorkflowStateStore,
        task_store: std::sync::Arc<dyn A2ATaskProjectionStore>,
        push_configs: A2APushConfigStore,
    ) -> Self {
        Self {
            workflow,
            run_store,
            workflow_store,
            task_store,
            push_configs,
            idle_passivation: DEFAULT_IDLE_PASSIVATION,
            run_ask_timeout: DEFAULT_RUN_ASK_TIMEOUT,
        }
    }

    /// Overrides the idle passivation duration.
    #[must_use]
    pub fn idle_passivation(mut self, idle_passivation: Duration) -> Self {
        self.idle_passivation = idle_passivation;
        self
    }

    /// Overrides the child run-actor ask timeout.
    #[must_use]
    pub fn run_ask_timeout(mut self, run_ask_timeout: Duration) -> Self {
        self.run_ask_timeout = run_ask_timeout;
        self
    }
}

/// Builds an A2A run entity type key.
pub fn a2a_run_entity_key(
    entity_type: &str,
    number_of_shards: u32,
) -> ClusterShardingResult<EntityTypeKey<A2ARunEntityCommand>> {
    Ok(EntityTypeKey::new(entity_type).with_number_of_shards(number_of_shards)?)
}

/// Builds the default A2A run entity type key.
pub fn default_a2a_run_entity_key() -> ClusterShardingResult<EntityTypeKey<A2ARunEntityCommand>> {
    a2a_run_entity_key(DEFAULT_ENTITY_TYPE, DEFAULT_NUMBER_OF_SHARDS)
}

/// Initializes the remote-aware sharded A2A run owner entity.
pub fn init_a2a_run_sharding(
    system: &ActorSystem,
    runtime: &mut ClusterNodeRuntime,
    sharding: &ClusterSharding,
    key: EntityTypeKey<A2ARunEntityCommand>,
    host: A2ARunHost,
) -> rakka_sharding::ClusterNodeRuntimeResult<EntityTypeRegistration<A2ARunEntityCommand>> {
    let idle_passivation = host.idle_passivation;
    let host = std::sync::Arc::new(host);
    let registration = sharding.init_remote_with_ask(
        runtime,
        Entity::of(key, {
            let system = system.clone();
            let host = std::sync::Arc::clone(&host);
            move |context: EntityContext<A2ARunEntityCommand>| {
                A2ARunEntity::new(system.clone(), context, std::sync::Arc::clone(&host))
            }
        })
        .with_idle_passivation(idle_passivation),
        |request: A2ARunRequest, reply_to: ReplyTo<A2ARunResponse>| A2ARunEntityCommand::Handle {
            request,
            reply_to,
        },
    )?;
    Ok(registration)
}

/// Sharded entity that owns and drives one durable A2A run locally.
pub struct A2ARunEntity {
    run_id: AgentRunId,
    host: std::sync::Arc<A2ARunHost>,
    /// `None` when the child run actor failed to spawn; requests are then
    /// answered with a retryable failure instead of panicking the factory.
    child: Option<ActorRef<AgentRunActorCommand>>,
}

impl A2ARunEntity {
    fn new(
        system: ActorSystem,
        context: EntityContext<A2ARunEntityCommand>,
        host: std::sync::Arc<A2ARunHost>,
    ) -> Self {
        let run_id = AgentRunId::new(context.entity_id().as_str());
        let child_name = format!(
            "{}-agent-run-{}",
            context.actor_name(),
            CHILD_INSTANCE.fetch_add(1, Ordering::Relaxed)
        );
        let child = match system.spawn(
            child_name,
            AgentRunActor::new(
                host.workflow.clone(),
                run_id.clone(),
                host.run_store.clone(),
                host.workflow_store.clone(),
            ),
        ) {
            Ok(child) => Some(child),
            Err(error) => {
                eprintln!(
                    "warning: run entity {} could not spawn its run actor: {error}",
                    run_id.as_str()
                );
                None
            }
        };
        Self {
            run_id,
            host,
            child,
        }
    }
}

impl Actor for A2ARunEntity {
    type Msg = A2ARunEntityCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        let run_id = self.run_id.clone();
        let host = std::sync::Arc::clone(&self.host);
        actor_future(async move {
            match msg {
                A2ARunEntityCommand::Handle { request, reply_to } => {
                    let response = match child {
                        Some(child) => handle_owner_request(&child, &run_id, &host, request).await,
                        None => A2ARunResponse::failure(
                            request.task_id,
                            echo_tenant(&request.tenant),
                            failure_from_error(RakkaA2AHandlerError::Unavailable {
                                message: "owner run actor failed to spawn; retry".to_string(),
                            }),
                        ),
                    };
                    let _reply_dropped = reply_to.reply(response);
                }
            }
            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a TerminationReason,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        let run_id = self.run_id.clone();
        actor_future(async move {
            if let Some(child) = child {
                if let Err(error) = child.stop() {
                    eprintln!(
                        "warning: run entity {} could not stop its run actor: {error}",
                        run_id.as_str()
                    );
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

async fn handle_owner_request(
    child: &ActorRef<AgentRunActorCommand>,
    run_id: &AgentRunId,
    host: &A2ARunHost,
    request: A2ARunRequest,
) -> A2ARunResponse {
    if request.version != A2A_RUN_PROTOCOL_VERSION {
        return A2ARunResponse::failure(
            request.task_id,
            echo_tenant(&request.tenant),
            A2ARunFailure::version_mismatch(request.version),
        );
    }

    let task_id = request.task_id.clone();
    let tenant = request.tenant.clone();
    let result = match request.kind {
        A2ARunRequestKind::AcceptMessage {
            draft,
            projected_message,
            artifacts,
            request_push_config,
            return_immediately,
            received_at,
        } => {
            accept_message(
                child,
                host,
                AcceptMessageInput {
                    draft: *draft,
                    projected_message: *projected_message,
                    artifacts,
                    request_push_config,
                    return_immediately,
                    received_at,
                },
            )
            .await
        }
        A2ARunRequestKind::QueryTaskSnapshot => {
            query_task_projection(child, host, run_id, tenant.as_deref()).await
        }
        A2ARunRequestKind::CancelTask { draft, received_at } => {
            cancel_task(child, host, *draft, received_at).await
        }
        A2ARunRequestKind::OpenStreamCursor { after_cursor } => {
            match open_stream_cursor(
                child,
                host,
                run_id,
                tenant.as_deref(),
                after_cursor.as_deref(),
            )
            .await
            {
                Ok((projection, events, resync)) => {
                    let tenant = projection.tenant.clone();
                    return A2ARunResponse::stream_cursor(
                        task_id, tenant, projection, events, resync,
                    );
                }
                Err(error) => Err(error),
            }
        }
        A2ARunRequestKind::RecordPushConfig { config } => {
            match record_push_config(child, host, run_id, tenant.as_deref(), config).await {
                Ok(config) => {
                    return A2ARunResponse::push_config_recorded(
                        task_id,
                        config
                            .tenant
                            .clone()
                            .unwrap_or_else(|| echo_tenant(&tenant)),
                        config,
                    );
                }
                Err(error) => Err(error),
            }
        }
        A2ARunRequestKind::DeletePushConfig { config_id } => {
            match delete_push_config(child, host, run_id, tenant.as_deref(), &config_id).await {
                Ok(tenant) => {
                    return A2ARunResponse::push_config_deleted(task_id, tenant);
                }
                Err(error) => Err(error),
            }
        }
    };

    match result {
        // Echo the projection's stored tenant so unscoped reads report the
        // run's true tenant rather than a caller-side default.
        Ok(projection) => {
            let tenant = projection.tenant.clone();
            A2ARunResponse::task(task_id, tenant, projection)
        }
        Err(error) => {
            A2ARunResponse::failure(task_id, echo_tenant(&tenant), failure_from_error(error))
        }
    }
}

/// Tenant echoed on failure responses when no canonical tenant is known.
fn echo_tenant(tenant: &Option<String>) -> String {
    tenant.clone().unwrap_or_else(|| DEFAULT_TENANT.to_string())
}

struct AcceptMessageInput {
    draft: A2ACommandDraft,
    projected_message: a2a::Message,
    artifacts: Vec<ArtifactRef>,
    request_push_config: Option<a2a::TaskPushNotificationConfig>,
    return_immediately: bool,
    received_at: AgentTimestampMillis,
}

async fn accept_message(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    input: AcceptMessageInput,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    let AcceptMessageInput {
        draft,
        projected_message,
        artifacts,
        request_push_config,
        return_immediately,
        received_at,
    } = input;
    let snapshot = recover_child(child, host.run_ask_timeout).await?;
    let existing_state = snapshot.run_state;
    validate_send_lifecycle(&draft, existing_state.as_ref())?;
    validate_inbox_collision(host, &draft, existing_state.is_some()).await?;
    accept_child_command(child, host.run_ask_timeout, draft.command.clone()).await?;

    let mut run_state = match existing_state {
        Some(state) => state,
        None => {
            let (state, adopted) = start_child_run(child, host, &draft, received_at).await?;
            if adopted {
                validate_adopted_run(&state, &draft)?;
            }
            state
        }
    };
    if !return_immediately && run_state.status == AgentRunStatus::Accepted {
        run_state = begin_child_step(child, host.run_ask_timeout, received_at).await?;
    }
    if let Some(config) = request_push_config {
        host.push_configs
            .save(draft.normalized.tenant.as_str(), config)
            .await?;
    }
    let events = project_send_result(
        host.task_store.as_ref(),
        &draft,
        &projected_message,
        artifacts,
        &run_state,
        received_at,
    )
    .await?;
    // Runs even when this retry emitted nothing new: the scheduler works from
    // its watermark over the retained log, so a retry heals a push schedule
    // that failed after the original acceptance.
    schedule_push_effects_for_events(
        &host.workflow_store,
        &host.push_configs,
        host.task_store.as_ref(),
        draft.normalized.tenant.as_str(),
        &draft.normalized.task_id,
        &events,
    )
    .await?;
    host.task_store
        .projection(
            Some(draft.normalized.tenant.as_str()),
            &draft.normalized.task_id,
        )
        .await
        .map_err(Into::into)
}

async fn cancel_task(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    draft: A2ACommandDraft,
    received_at: AgentTimestampMillis,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    let snapshot = recover_child(child, host.run_ask_timeout).await?;
    let mut run_state = snapshot
        .run_state
        .ok_or_else(|| missing_run(&draft.normalized.task_id))?;
    if run_tenant(&run_state) != draft.normalized.tenant.as_str() {
        return Err(missing_run(&draft.normalized.task_id));
    }
    if run_is_terminal(run_state.status) {
        let events: Vec<A2ATaskEvent> = sync_status_projection(
            host.task_store.as_ref(),
            &run_state,
            &draft.normalized.context_id,
            received_at,
            None,
        )
        .await?
        .into_iter()
        .collect();
        schedule_push_effects_for_events(
            &host.workflow_store,
            &host.push_configs,
            host.task_store.as_ref(),
            draft.normalized.tenant.as_str(),
            &draft.normalized.task_id,
            &events,
        )
        .await?;
        return Err(RakkaA2AHandlerError::TaskNotCancelable {
            task_id: draft.normalized.task_id.clone(),
        });
    }
    validate_inbox_collision(host, &draft, true).await?;
    accept_child_command(child, host.run_ask_timeout, draft.command.clone()).await?;
    run_state = apply_cancellation(child, host.run_ask_timeout, run_state, received_at).await?;
    let events: Vec<A2ATaskEvent> = sync_status_projection(
        host.task_store.as_ref(),
        &run_state,
        &draft.normalized.context_id,
        received_at,
        None,
    )
    .await?
    .into_iter()
    .collect();
    schedule_push_effects_for_events(
        &host.workflow_store,
        &host.push_configs,
        host.task_store.as_ref(),
        draft.normalized.tenant.as_str(),
        &draft.normalized.task_id,
        &events,
    )
    .await?;
    host.task_store
        .projection(
            Some(draft.normalized.tenant.as_str()),
            &draft.normalized.task_id,
        )
        .await
        .map_err(Into::into)
}

async fn query_task_projection(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    run_id: &AgentRunId,
    tenant: Option<&str>,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    let snapshot = recover_child(child, host.run_ask_timeout).await?;
    let run_state = snapshot
        .run_state
        .ok_or_else(|| missing_run(run_id.as_str()))?;
    // A caller-scoped read must not see another tenant's run; an unscoped
    // read (`None`) resolves the run's stored tenant, matching the
    // single-tenant projection store's unscoped read semantics.
    let resolved = run_tenant(&run_state);
    if tenant.is_some_and(|scoped| scoped != resolved) {
        return Err(missing_run(run_id.as_str()));
    }
    let tenant = resolved.as_str();

    // Convergence on the read path emits real public events (a transition can
    // be observed here first, e.g. a step that completed with no follow-up
    // command), so those events must schedule push effects like any other.
    let emitted: Vec<A2ATaskEvent> = match host
        .task_store
        .projection(Some(tenant), run_id.as_str())
        .await
    {
        Ok(projection) => sync_status_projection(
            host.task_store.as_ref(),
            &run_state,
            &projection.context_id,
            run_state.updated_at,
            Some(projection.status),
        )
        .await?
        .into_iter()
        .collect(),
        Err(TaskProjectionError::TaskNotFound { .. }) => {
            let context_id = recover_context_id(&host.workflow_store, run_id)
                .await?
                .unwrap_or_else(|| run_id.as_str().to_string());
            vec![
                snapshot_projection(
                    host.task_store.as_ref(),
                    &run_state,
                    &context_id,
                    Vec::new(),
                    Vec::new(),
                    run_state.updated_at,
                )
                .await?,
            ]
        }
        Err(error) => return Err(error.into()),
    };
    // Unconditional: a read also heals push schedules that failed on an
    // earlier request via the scheduler's watermark over the retained log. A
    // scheduling failure does not fail the read.
    if let Err(error) = schedule_push_effects_for_events(
        &host.workflow_store,
        &host.push_configs,
        host.task_store.as_ref(),
        tenant,
        run_id.as_str(),
        &emitted,
    )
    .await
    {
        eprintln!(
            "warning: push scheduling deferred for task {}: {error}",
            run_id.as_str()
        );
    }

    host.task_store
        .projection(Some(tenant), run_id.as_str())
        .await
        .map_err(Into::into)
}

/// Converges the owner projection, then replays public events after the
/// caller's cursor for a stream subscriber on another public node.
async fn open_stream_cursor(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    run_id: &AgentRunId,
    tenant: Option<&str>,
    after_cursor: Option<&str>,
) -> Result<(A2ATaskProjection, Vec<A2ATaskEvent>, bool), RakkaA2AHandlerError> {
    let projection = query_task_projection(child, host, run_id, tenant).await?;
    match host
        .task_store
        .replay_events(&projection.tenant, run_id.as_str(), after_cursor)
        .await
    {
        Ok(events) => Ok((projection, events, false)),
        Err(
            TaskProjectionError::ReplayWindowExpired { .. }
            | TaskProjectionError::InvalidReplayCursor { .. },
        ) => Ok((projection, Vec::new(), true)),
        Err(error) => Err(error.into()),
    }
}

async fn record_push_config(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    run_id: &AgentRunId,
    tenant: Option<&str>,
    config: a2a::TaskPushNotificationConfig,
) -> Result<a2a::TaskPushNotificationConfig, RakkaA2AHandlerError> {
    let projection = authorize_owner_task(child, host, run_id, tenant).await?;
    host.push_configs
        .save(&projection.tenant, config)
        .await
        .map_err(Into::into)
}

async fn delete_push_config(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    run_id: &AgentRunId,
    tenant: Option<&str>,
    config_id: &str,
) -> Result<String, RakkaA2AHandlerError> {
    let projection = authorize_owner_task(child, host, run_id, tenant).await?;
    host.push_configs
        .delete(&projection.tenant, run_id.as_str(), config_id)
        .await?;
    Ok(projection.tenant)
}

async fn authorize_owner_task(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    run_id: &AgentRunId,
    tenant: Option<&str>,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    let snapshot = recover_child(child, host.run_ask_timeout).await?;
    let run_state = snapshot
        .run_state
        .ok_or_else(|| missing_run(run_id.as_str()))?;
    let resolved = run_tenant(&run_state);
    if tenant.is_some_and(|scoped| scoped != resolved) {
        return Err(missing_run(run_id.as_str()));
    }
    match host
        .task_store
        .projection(Some(&resolved), run_id.as_str())
        .await
    {
        Ok(projection) => Ok(projection),
        Err(TaskProjectionError::TaskNotFound { .. }) => Ok(A2ATaskProjection::from_run_state(
            &run_state,
            run_id.as_str(),
            Vec::new(),
            Vec::new(),
            0,
        )),
        Err(error) => Err(error.into()),
    }
}

async fn validate_inbox_collision(
    host: &A2ARunHost,
    draft: &A2ACommandDraft,
    run_exists: bool,
) -> Result<(), RakkaA2AHandlerError> {
    if !run_exists || !matches!(draft.normalized.intent, A2ATaskIntent::NewTask) {
        return Ok(());
    }
    let mut inbox = AgentRunInbox::new(draft.normalized.run_id(), host.workflow_store.clone());
    let state = inbox.recover().await?;
    if !known_command(state, draft) {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "generated task id collides with an existing task",
        });
    }
    Ok(())
}

async fn accept_child_command(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
    command: AgentCommand,
) -> Result<AgentInboxAcceptance, RakkaA2AHandlerError> {
    let mut attempts = 0;
    loop {
        let result = ask_child(child, ask_timeout, |reply_to| {
            AgentRunActorCommand::AcceptCommand {
                command: command.clone(),
                reply_to,
            }
        })
        .await;
        match result {
            Ok(acceptance) => return Ok(acceptance),
            Err(error)
                if inbox_revision_conflict(&error) && attempts + 1 < MAX_CONFLICT_ATTEMPTS =>
            {
                attempts += 1;
                let _ = recover_child(child, ask_timeout).await?;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn start_child_run(
    child: &ActorRef<AgentRunActorCommand>,
    host: &A2ARunHost,
    draft: &A2ACommandDraft,
    now: AgentTimestampMillis,
) -> Result<(AgentRunState, bool), RakkaA2AHandlerError> {
    let initial = initial_run_state(&host.workflow, draft, now)?;
    match ask_child(child, host.run_ask_timeout, |reply_to| {
        AgentRunActorCommand::Start {
            initial_state: initial,
            reply_to,
        }
    })
    .await
    {
        Ok(transition) => Ok((transition.state, false)),
        Err(error) if run_start_conflict(&error) => {
            Ok((refreshed_state(child, host.run_ask_timeout).await?, true))
        }
        Err(error) => Err(error),
    }
}

async fn begin_child_step(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
    now: AgentTimestampMillis,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    let begin_at = AgentTimestampMillis::new(now.as_millis().saturating_add(1));
    let result = ask_child(child, ask_timeout, |reply_to| {
        AgentRunActorCommand::BeginStep {
            now: begin_at,
            reply_to,
        }
    })
    .await;
    adopt_on_conflict(child, ask_timeout, result).await
}

async fn apply_cancellation(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
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
            ask_child(child, ask_timeout, |reply_to| {
                AgentRunActorCommand::Cancel {
                    now: completed_at,
                    reply_to,
                }
            })
            .await
        } else {
            ask_child(child, ask_timeout, |reply_to| {
                AgentRunActorCommand::RequestCancellation {
                    reason_code: "a2a-cancel".to_string(),
                    reason_summary: Some("A2A client requested cancellation".to_string()),
                    now,
                    reply_to,
                }
            })
            .await
        };
        state = adopt_on_conflict(child, ask_timeout, result).await?;
    }
    Ok(state)
}

async fn adopt_on_conflict(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
    result: Result<AgentRunTransition, RakkaA2AHandlerError>,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    match result {
        Ok(transition) => Ok(transition.state),
        Err(error) if run_transition_conflict(&error) => refreshed_state(child, ask_timeout).await,
        Err(error) => Err(error),
    }
}

async fn refreshed_state(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    let snapshot = recover_child(child, ask_timeout).await?;
    let run_id = snapshot.run_id.as_str().to_string();
    snapshot.run_state.ok_or_else(|| missing_run(&run_id))
}

async fn recover_child(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
) -> Result<AgentRunActorSnapshot, RakkaA2AHandlerError> {
    ask_child(child, ask_timeout, |reply_to| {
        AgentRunActorCommand::Recover { reply_to }
    })
    .await
}

async fn ask_child<R>(
    child: &ActorRef<AgentRunActorCommand>,
    ask_timeout: Duration,
    build: impl FnOnce(ReplyTo<AgentRunRuntimeResult<R>>) -> AgentRunActorCommand,
) -> Result<R, RakkaA2AHandlerError>
where
    R: Send + 'static,
{
    child
        .ask(build, ask_timeout)
        .await
        .map_err(|error| RakkaA2AHandlerError::OwnerAsk {
            message: owner_ask_message(error),
        })?
        .map_err(Into::into)
}

fn owner_ask_message(error: AskError) -> String {
    match error {
        AskError::MailboxFull => "owner run actor mailbox full".to_string(),
        AskError::MailboxClosed => "owner run actor mailbox closed".to_string(),
        AskError::Timeout => "owner run actor ask timed out".to_string(),
        AskError::ReplyDropped => "owner run actor reply dropped".to_string(),
    }
}

fn inbox_revision_conflict(error: &RakkaA2AHandlerError) -> bool {
    matches!(
        error,
        RakkaA2AHandlerError::RunActor(AgentRunRuntimeError::Inbox {
            error: AgentInboxError::Workflow {
                error: WorkflowError::RevisionConflict { .. },
            },
        })
    )
}

fn run_start_conflict(error: &RakkaA2AHandlerError) -> bool {
    matches!(
        error,
        RakkaA2AHandlerError::RunActor(AgentRunRuntimeError::RunEngine {
            error: AgentRunEngineError::AlreadyStarted { .. },
        }) | RakkaA2AHandlerError::RunActor(AgentRunRuntimeError::RunEngine {
            error: AgentRunEngineError::Persistence {
                error: DurableError::RevisionConflict { .. },
                ..
            },
        })
    )
}

fn run_transition_conflict(error: &RakkaA2AHandlerError) -> bool {
    matches!(
        error,
        RakkaA2AHandlerError::RunActor(AgentRunRuntimeError::RunEngine {
            error: AgentRunEngineError::Persistence {
                error: DurableError::RevisionConflict { .. },
                ..
            },
        })
    )
}

fn failure_from_error(error: RakkaA2AHandlerError) -> A2ARunFailure {
    let code = error.code().to_string();
    let message = error.to_string();
    let kind = match &error {
        RakkaA2AHandlerError::Projection(TaskProjectionError::TaskNotFound { .. })
        | RakkaA2AHandlerError::MissingRun { .. }
        | RakkaA2AHandlerError::RunEngine(AgentRunEngineError::MissingRunState { .. })
        | RakkaA2AHandlerError::RunActor(AgentRunRuntimeError::RunEngine {
            error: AgentRunEngineError::MissingRunState { .. },
        }) => A2ARunFailureKind::TaskNotFound,
        RakkaA2AHandlerError::TaskNotCancelable { .. } => A2ARunFailureKind::TaskNotCancelable,
        RakkaA2AHandlerError::Mapping(_)
        | RakkaA2AHandlerError::Projection(_)
        | RakkaA2AHandlerError::NotAuthorized { .. }
        | RakkaA2AHandlerError::InvalidLifecycle { .. } => A2ARunFailureKind::InvalidRequest,
        RakkaA2AHandlerError::Unavailable { .. }
        | RakkaA2AHandlerError::OwnerAsk { .. }
        | RakkaA2AHandlerError::StreamLimit { .. }
        | RakkaA2AHandlerError::Draining => A2ARunFailureKind::Unavailable,
        RakkaA2AHandlerError::Inbox(_)
        | RakkaA2AHandlerError::RunEngine(_)
        | RakkaA2AHandlerError::RunActor(_)
        | RakkaA2AHandlerError::Persistence(_)
        | RakkaA2AHandlerError::Push(_) => A2ARunFailureKind::Internal,
    };
    let retryable = matches!(kind, A2ARunFailureKind::Unavailable);
    A2ARunFailure::new(code, message, kind, retryable)
}
