//! Clustered sharded A2A run host.
//!
//! `A2ARunEntity` is the cluster-addressable owner shell for one A2A task/run.
//! Non-owning public ingress nodes route serializable [`A2ARunRequest`] values
//! to this entity. The entity maps each request to local
//! [`AgentRunActorCommand`] messages and projection operations; those local
//! actor commands are never serialized over the wire.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rakka::agent_workflow::substrate::{WorkflowError, WorkflowState};
use rakka::agent_workflow::{
    AgentCommand, AgentCommandKind, AgentInboxAcceptance, AgentInboxError, AgentRunActor,
    AgentRunActorCommand, AgentRunActorSnapshot, AgentRunEngineError, AgentRunId,
    AgentRunRuntimeError, AgentRunRuntimeResult, AgentRunState, AgentRunStatus, AgentRunTransition,
    AgentTimestampMillis, AgentWorkflow, ArtifactRef,
};
use rakka::prelude::*;
use rakka::sharding::ClusterNodeRuntime;

use crate::a2a_handler::{
    known_command, missing_run, run_is_terminal, run_tenant, state_payload, task_state,
    validate_adopted_run, validate_send_lifecycle, RakkaA2AHandlerError,
};
use crate::a2a_mapping::{A2ACommandDraft, A2ATaskIntent, ATTR_CONTEXT_ID, DEFAULT_TENANT};
use crate::durable_stores::{RunStore, WorkflowStore};
use crate::protocol::{
    A2ARunFailure, A2ARunFailureKind, A2ARunRequest, A2ARunRequestKind, A2ARunResponse,
    A2A_RUN_PROTOCOL_VERSION,
};
use crate::push_config::{schedule_push_effects_for_event, A2APushConfigStore};
use crate::support::{ENTITY_TYPE, NUMBER_OF_SHARDS, RUN_ASK_TIMEOUT};
use crate::task_projection::{
    status_transition_allowed, A2ATaskEvent, A2ATaskEventPayload, A2ATaskProjection,
    InMemoryA2ATaskProjectionStore, TaskProjectionError,
};

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

/// Sharded entity that owns and drives one durable A2A run locally.
pub struct A2ARunEntity<WorkflowStoreT>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    run_id: AgentRunId,
    workflow: AgentWorkflow,
    workflow_store: WorkflowStoreT,
    task_store: InMemoryA2ATaskProjectionStore,
    push_configs: A2APushConfigStore,
    /// `None` when the child run actor failed to spawn; requests are then
    /// answered with a retryable failure instead of panicking the factory.
    child: Option<ActorRef<AgentRunActorCommand>>,
}

impl<WorkflowStoreT> A2ARunEntity<WorkflowStoreT>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    fn new<RunStoreT>(
        system: ActorSystem,
        context: EntityContext<A2ARunEntityCommand>,
        workflow: AgentWorkflow,
        run_store: RunStoreT,
        workflow_store: WorkflowStoreT,
        task_store: InMemoryA2ATaskProjectionStore,
        push_configs: A2APushConfigStore,
    ) -> Self
    where
        RunStoreT: DurableStateStore<AgentRunState>,
    {
        let run_id = AgentRunId::new(context.entity_id().as_str());
        let child_name = format!(
            "{}-agent-run-{}",
            context.actor_name(),
            CHILD_INSTANCE.fetch_add(1, Ordering::Relaxed)
        );
        let child = match system.spawn(
            child_name,
            AgentRunActor::new(
                workflow.clone(),
                run_id.clone(),
                run_store,
                workflow_store.clone(),
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
            workflow,
            workflow_store,
            task_store,
            push_configs,
            child,
        }
    }
}

impl<WorkflowStoreT> Actor for A2ARunEntity<WorkflowStoreT>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    type Msg = A2ARunEntityCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        let run_id = self.run_id.clone();
        let workflow = self.workflow.clone();
        let workflow_store = self.workflow_store.clone();
        let task_store = self.task_store.clone();
        let push_configs = self.push_configs.clone();
        actor_future(async move {
            match msg {
                A2ARunEntityCommand::Handle { request, reply_to } => {
                    let response = match child {
                        Some(child) => {
                            handle_owner_request(
                                child,
                                run_id,
                                workflow,
                                workflow_store,
                                task_store,
                                push_configs,
                                request,
                            )
                            .await
                        }
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

/// Per-node configuration for hosting A2A run entities.
pub struct A2ARunHost {
    /// Workflow definition every run entity hosts.
    pub workflow: AgentWorkflow,
    /// Durable store for run state.
    pub run_store: RunStore,
    /// Durable store for inbox/outbox workflow state.
    pub workflow_store: WorkflowStore,
    /// Local task projection store.
    pub task_store: InMemoryA2ATaskProjectionStore,
    /// Durable push config store.
    pub push_configs: A2APushConfigStore,
    /// How long an idle entity stays resident before passivating.
    ///
    /// Without passivation, a read probe for an arbitrary task id would pin
    /// an entity and child run actor forever.
    pub idle_passivation: Duration,
}

/// Initializes the remote-aware sharded A2A run owner entity.
pub fn init_a2a_run_sharding(
    system: &ActorSystem,
    runtime: &mut ClusterNodeRuntime,
    sharding: &ClusterSharding,
    key: EntityTypeKey<A2ARunEntityCommand>,
    host: A2ARunHost,
) -> crate::support::ExampleResult<EntityTypeRegistration<A2ARunEntityCommand>> {
    let A2ARunHost {
        workflow,
        run_store,
        workflow_store,
        task_store,
        push_configs,
        idle_passivation,
    } = host;
    let registration = sharding.init_remote_with_ask(
        runtime,
        Entity::of(key, {
            let system = system.clone();
            move |context: EntityContext<A2ARunEntityCommand>| {
                A2ARunEntity::new(
                    system.clone(),
                    context,
                    workflow.clone(),
                    run_store.clone(),
                    workflow_store.clone(),
                    task_store.clone(),
                    push_configs.clone(),
                )
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

/// Creates the example's A2A run entity type key.
pub fn a2a_run_entity_key(
) -> rakka::sharding::ClusterShardingResult<EntityTypeKey<A2ARunEntityCommand>> {
    Ok(EntityTypeKey::new(ENTITY_TYPE).with_number_of_shards(NUMBER_OF_SHARDS)?)
}

async fn handle_owner_request<WorkflowStoreT>(
    child: ActorRef<AgentRunActorCommand>,
    run_id: AgentRunId,
    workflow: AgentWorkflow,
    workflow_store: WorkflowStoreT,
    task_store: InMemoryA2ATaskProjectionStore,
    push_configs: A2APushConfigStore,
    request: A2ARunRequest,
) -> A2ARunResponse
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
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
                &child,
                &workflow,
                &workflow_store,
                &task_store,
                &push_configs,
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
            query_task_projection(
                &child,
                &workflow_store,
                &task_store,
                &run_id,
                tenant.as_deref(),
            )
            .await
        }
        A2ARunRequestKind::CancelTask { draft, received_at } => {
            cancel_task(
                &child,
                &workflow_store,
                &task_store,
                &push_configs,
                *draft,
                received_at,
            )
            .await
        }
        A2ARunRequestKind::OpenStreamCursor { .. } => Err(RakkaA2AHandlerError::Unavailable {
            message: "A2A streaming is deferred until the streaming phase".to_string(),
        }),
        A2ARunRequestKind::RecordPushConfig { config } => {
            match record_push_config(
                &child,
                &task_store,
                &push_configs,
                &run_id,
                tenant.as_deref(),
                config,
            )
            .await
            {
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
            match delete_push_config(
                &child,
                &task_store,
                &push_configs,
                &run_id,
                tenant.as_deref(),
                &config_id,
            )
            .await
            {
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

async fn accept_message<WorkflowStoreT>(
    child: &ActorRef<AgentRunActorCommand>,
    workflow: &AgentWorkflow,
    workflow_store: &WorkflowStoreT,
    task_store: &InMemoryA2ATaskProjectionStore,
    push_configs: &A2APushConfigStore,
    input: AcceptMessageInput,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let AcceptMessageInput {
        draft,
        projected_message,
        artifacts,
        request_push_config,
        return_immediately,
        received_at,
    } = input;
    let snapshot = recover_child(child).await?;
    let existing_state = snapshot.run_state;
    validate_send_lifecycle(&draft, existing_state.as_ref())?;
    validate_inbox_collision(workflow_store, &draft, existing_state.is_some()).await?;
    accept_child_command(child, draft.command.clone()).await?;

    let mut run_state = match existing_state {
        Some(state) => state,
        None => {
            let (state, adopted) = start_child_run(child, workflow, &draft, received_at).await?;
            if adopted {
                validate_adopted_run(&state, &draft)?;
            }
            state
        }
    };
    if !return_immediately && run_state.status == AgentRunStatus::Accepted {
        run_state = begin_child_step(child, received_at).await?;
    }
    if let Some(config) = request_push_config {
        push_configs
            .save(draft.normalized.tenant.as_str(), config)
            .await?;
    }
    let events = project_send_result(
        task_store,
        &draft,
        &projected_message,
        artifacts,
        &run_state,
        received_at,
    )?;
    schedule_owner_push_effects(workflow_store, push_configs, &events).await?;
    task_store
        .projection(
            Some(draft.normalized.tenant.as_str()),
            &draft.normalized.task_id,
        )
        .map_err(Into::into)
}

async fn cancel_task<WorkflowStoreT>(
    child: &ActorRef<AgentRunActorCommand>,
    workflow_store: &WorkflowStoreT,
    task_store: &InMemoryA2ATaskProjectionStore,
    push_configs: &A2APushConfigStore,
    draft: A2ACommandDraft,
    received_at: AgentTimestampMillis,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let snapshot = recover_child(child).await?;
    let mut run_state = snapshot
        .run_state
        .ok_or_else(|| missing_run(&draft.normalized.task_id))?;
    if run_tenant(&run_state) != draft.normalized.tenant.as_str() {
        return Err(missing_run(&draft.normalized.task_id));
    }
    if run_is_terminal(run_state.status) {
        if let Some(event) = sync_status_projection(
            task_store,
            &run_state,
            &draft.normalized.context_id,
            received_at,
            None,
        )? {
            schedule_owner_push_effects(workflow_store, push_configs, &[event]).await?;
        }
        return Err(RakkaA2AHandlerError::TaskNotCancelable {
            task_id: draft.normalized.task_id.clone(),
        });
    }
    validate_inbox_collision(workflow_store, &draft, true).await?;
    accept_child_command(child, draft.command.clone()).await?;
    run_state = apply_cancellation(child, run_state, received_at).await?;
    if let Some(event) = sync_status_projection(
        task_store,
        &run_state,
        &draft.normalized.context_id,
        received_at,
        None,
    )? {
        schedule_owner_push_effects(workflow_store, push_configs, &[event]).await?;
    }
    task_store
        .projection(
            Some(draft.normalized.tenant.as_str()),
            &draft.normalized.task_id,
        )
        .map_err(Into::into)
}

async fn query_task_projection<WorkflowStoreT>(
    child: &ActorRef<AgentRunActorCommand>,
    workflow_store: &WorkflowStoreT,
    task_store: &InMemoryA2ATaskProjectionStore,
    run_id: &AgentRunId,
    tenant: Option<&str>,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let snapshot = recover_child(child).await?;
    let run_state = snapshot
        .run_state
        .ok_or_else(|| missing_run(run_id.as_str()))?;
    // A caller-scoped read must not see another tenant's run; an unscoped
    // read (`None`) resolves the run's stored tenant, matching the local
    // projection store's unscoped read semantics.
    let resolved = run_tenant(&run_state);
    if tenant.is_some_and(|scoped| scoped != resolved) {
        return Err(missing_run(run_id.as_str()));
    }
    let tenant = resolved.as_str();

    match task_store.projection(Some(tenant), run_id.as_str()) {
        Ok(projection) => {
            let _ = sync_status_projection(
                task_store,
                &run_state,
                &projection.context_id,
                run_state.updated_at,
                Some(projection.status),
            )?;
        }
        Err(TaskProjectionError::TaskNotFound { .. }) => {
            let context_id = recover_context_id(workflow_store, run_id)
                .await?
                .unwrap_or_else(|| run_id.as_str().to_string());
            let _ = snapshot_projection(
                task_store,
                &run_state,
                &context_id,
                Vec::new(),
                Vec::new(),
                run_state.updated_at,
            )?;
        }
        Err(error) => return Err(error.into()),
    }

    task_store
        .projection(Some(tenant), run_id.as_str())
        .map_err(Into::into)
}

async fn record_push_config(
    child: &ActorRef<AgentRunActorCommand>,
    task_store: &InMemoryA2ATaskProjectionStore,
    push_configs: &A2APushConfigStore,
    run_id: &AgentRunId,
    tenant: Option<&str>,
    config: a2a::TaskPushNotificationConfig,
) -> Result<a2a::TaskPushNotificationConfig, RakkaA2AHandlerError> {
    let projection = authorize_owner_task(child, task_store, run_id, tenant).await?;
    push_configs
        .save(&projection.tenant, config)
        .await
        .map_err(Into::into)
}

async fn delete_push_config(
    child: &ActorRef<AgentRunActorCommand>,
    task_store: &InMemoryA2ATaskProjectionStore,
    push_configs: &A2APushConfigStore,
    run_id: &AgentRunId,
    tenant: Option<&str>,
    config_id: &str,
) -> Result<String, RakkaA2AHandlerError> {
    let projection = authorize_owner_task(child, task_store, run_id, tenant).await?;
    push_configs
        .delete(&projection.tenant, run_id.as_str(), config_id)
        .await?;
    Ok(projection.tenant)
}

async fn authorize_owner_task(
    child: &ActorRef<AgentRunActorCommand>,
    task_store: &InMemoryA2ATaskProjectionStore,
    run_id: &AgentRunId,
    tenant: Option<&str>,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    let snapshot = recover_child(child).await?;
    let run_state = snapshot
        .run_state
        .ok_or_else(|| missing_run(run_id.as_str()))?;
    let resolved = run_tenant(&run_state);
    if tenant.is_some_and(|scoped| scoped != resolved) {
        return Err(missing_run(run_id.as_str()));
    }
    task_store
        .projection(Some(&resolved), run_id.as_str())
        .or_else(|error| {
            if matches!(error, TaskProjectionError::TaskNotFound { .. }) {
                Ok(A2ATaskProjection::from_run_state(
                    &run_state,
                    run_id.as_str(),
                    Vec::new(),
                    Vec::new(),
                    0,
                ))
            } else {
                Err(error)
            }
        })
        .map_err(Into::into)
}

async fn schedule_owner_push_effects<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    push_configs: &A2APushConfigStore,
    events: &[A2ATaskEvent],
) -> Result<(), RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    for event in events {
        schedule_push_effects_for_event(workflow_store, push_configs, event).await?;
    }
    Ok(())
}

async fn validate_inbox_collision<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    draft: &A2ACommandDraft,
    run_exists: bool,
) -> Result<(), RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    if !run_exists || !matches!(draft.normalized.intent, A2ATaskIntent::NewTask) {
        return Ok(());
    }
    let mut inbox = rakka::agent_workflow::AgentRunInbox::new(
        draft.normalized.run_id(),
        workflow_store.clone(),
    );
    let state = inbox.recover().await?;
    if !known_command(state, draft) {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "generated task id collides with an existing task",
        });
    }
    Ok(())
}

async fn recover_context_id<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    run_id: &AgentRunId,
) -> Result<Option<String>, RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let mut inbox =
        rakka::agent_workflow::AgentRunInbox::new(run_id.clone(), workflow_store.clone());
    let state = inbox.recover().await?;
    let mut fallback = None;
    for entry in state.inbox().values() {
        let command = match serde_json::from_slice::<AgentCommand>(entry.payload()) {
            Ok(command) => command,
            Err(error) => {
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

async fn accept_child_command(
    child: &ActorRef<AgentRunActorCommand>,
    command: AgentCommand,
) -> Result<AgentInboxAcceptance, RakkaA2AHandlerError> {
    let mut attempts = 0;
    loop {
        let result = ask_child(child, |reply_to| AgentRunActorCommand::AcceptCommand {
            command: command.clone(),
            reply_to,
        })
        .await;
        match result {
            Ok(acceptance) => return Ok(acceptance),
            Err(error)
                if inbox_revision_conflict(&error) && attempts + 1 < MAX_CONFLICT_ATTEMPTS =>
            {
                attempts += 1;
                let _ = recover_child(child).await?;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn start_child_run(
    child: &ActorRef<AgentRunActorCommand>,
    workflow: &AgentWorkflow,
    draft: &A2ACommandDraft,
    now: AgentTimestampMillis,
) -> Result<(AgentRunState, bool), RakkaA2AHandlerError> {
    let initial = initial_run_state(workflow, draft, now)?;
    match ask_child(child, |reply_to| AgentRunActorCommand::Start {
        initial_state: initial,
        reply_to,
    })
    .await
    {
        Ok(transition) => Ok((transition.state, false)),
        Err(error) if run_start_conflict(&error) => Ok((refreshed_state(child).await?, true)),
        Err(error) => Err(error),
    }
}

async fn begin_child_step(
    child: &ActorRef<AgentRunActorCommand>,
    now: AgentTimestampMillis,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    let begin_at = AgentTimestampMillis::new(now.as_millis().saturating_add(1));
    let result = ask_child(child, |reply_to| AgentRunActorCommand::BeginStep {
        now: begin_at,
        reply_to,
    })
    .await;
    adopt_on_conflict(child, result).await
}

async fn apply_cancellation(
    child: &ActorRef<AgentRunActorCommand>,
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
            ask_child(child, |reply_to| AgentRunActorCommand::Cancel {
                now: completed_at,
                reply_to,
            })
            .await
        } else {
            ask_child(child, |reply_to| {
                AgentRunActorCommand::RequestCancellation {
                    reason_code: "a2a-cancel".to_string(),
                    reason_summary: Some("A2A client requested cancellation".to_string()),
                    now,
                    reply_to,
                }
            })
            .await
        };
        state = adopt_on_conflict(child, result).await?;
    }
    Ok(state)
}

async fn adopt_on_conflict(
    child: &ActorRef<AgentRunActorCommand>,
    result: Result<AgentRunTransition, RakkaA2AHandlerError>,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    match result {
        Ok(transition) => Ok(transition.state),
        Err(error) if run_transition_conflict(&error) => refreshed_state(child).await,
        Err(error) => Err(error),
    }
}

async fn refreshed_state(
    child: &ActorRef<AgentRunActorCommand>,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    let snapshot = recover_child(child).await?;
    let run_id = snapshot.run_id.as_str().to_string();
    snapshot.run_state.ok_or_else(|| missing_run(&run_id))
}

async fn recover_child(
    child: &ActorRef<AgentRunActorCommand>,
) -> Result<AgentRunActorSnapshot, RakkaA2AHandlerError> {
    ask_child(child, |reply_to| AgentRunActorCommand::Recover { reply_to }).await
}

async fn ask_child<R>(
    child: &ActorRef<AgentRunActorCommand>,
    build: impl FnOnce(ReplyTo<AgentRunRuntimeResult<R>>) -> AgentRunActorCommand,
) -> Result<R, RakkaA2AHandlerError>
where
    R: Send + 'static,
{
    child
        .ask(build, RUN_ASK_TIMEOUT)
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

fn initial_run_state(
    workflow: &AgentWorkflow,
    draft: &A2ACommandDraft,
    now: AgentTimestampMillis,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    let current_step_id = workflow
        .steps
        .first()
        .map(|step| step.step_id.clone())
        .ok_or_else(|| RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "workflow has no executable steps",
        })?;
    let artifacts = draft.payload.artifact_drafts();
    Ok(AgentRunState {
        run_id: draft.normalized.run_id(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(draft.normalized.tenant.clone()),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(current_step_id),
        current_attempt: 0,
        inputs_ref: artifacts.first().map(|draft| draft.reference.clone()),
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
    task_store: &InMemoryA2ATaskProjectionStore,
    draft: &A2ACommandDraft,
    message: &a2a::Message,
    artifacts: Vec<ArtifactRef>,
    run_state: &AgentRunState,
    now: AgentTimestampMillis,
) -> Result<Vec<A2ATaskEvent>, RakkaA2AHandlerError> {
    let tenant = draft.normalized.tenant.as_str();
    let mut events = Vec::new();
    match task_store.projection(Some(tenant), &draft.normalized.task_id) {
        Ok(projection) => {
            let already_projected = projection
                .history
                .iter()
                .any(|recorded| recorded.message_id == message.message_id);
            if !already_projected {
                events.push(task_store.append_event_payload(
                    tenant,
                    &draft.normalized.task_id,
                    &draft.normalized.context_id,
                    now,
                    A2ATaskEventPayload::MessageUpdate {
                        message: message.clone(),
                    },
                )?);
            }
            if let Some(event) = sync_status_projection(
                task_store,
                run_state,
                &draft.normalized.context_id,
                now,
                Some(projection.status),
            )? {
                events.push(event);
            }
            Ok(events)
        }
        Err(TaskProjectionError::TaskNotFound { .. }) => {
            events.push(snapshot_projection(
                task_store,
                run_state,
                &draft.normalized.context_id,
                vec![message.clone()],
                artifacts,
                now,
            )?);
            Ok(events)
        }
        Err(error) => Err(error.into()),
    }
}

fn sync_status_projection(
    task_store: &InMemoryA2ATaskProjectionStore,
    run_state: &AgentRunState,
    context_id: &str,
    now: AgentTimestampMillis,
    current_status: Option<a2a::TaskState>,
) -> Result<Option<A2ATaskEvent>, RakkaA2AHandlerError> {
    let tenant = run_tenant(run_state);
    let state = task_state(run_state.status);
    let current = match current_status {
        Some(status) => status,
        None => match task_store.projection(Some(&tenant), run_state.run_id.as_str()) {
            Ok(projection) => projection.status,
            Err(TaskProjectionError::TaskNotFound { .. }) => {
                return snapshot_projection(
                    task_store,
                    run_state,
                    context_id,
                    Vec::new(),
                    Vec::new(),
                    now,
                )
                .map(Some);
            }
            Err(error) => return Err(error.into()),
        },
    };
    if current == state || !status_transition_allowed(&current, &state) {
        return Ok(None);
    }
    let payload = if state.is_terminal() {
        A2ATaskEventPayload::Terminal { state }
    } else {
        A2ATaskEventPayload::StatusUpdate { state }
    };
    task_store
        .append_event_payload(
            tenant.as_str(),
            run_state.run_id.as_str(),
            context_id,
            now,
            payload,
        )
        .map(Some)
        .map_err(Into::into)
}

fn snapshot_projection(
    task_store: &InMemoryA2ATaskProjectionStore,
    run_state: &AgentRunState,
    context_id: &str,
    history: Vec<a2a::Message>,
    artifacts: Vec<ArtifactRef>,
    now: AgentTimestampMillis,
) -> Result<A2ATaskEvent, RakkaA2AHandlerError> {
    let projection =
        A2ATaskProjection::from_run_state(run_state, context_id, history, artifacts, 0);
    let tenant = projection.tenant.clone();
    let task_id = projection.task_id.clone();
    let context_id = projection.context_id.clone();
    task_store
        .append_event_payload(
            tenant,
            task_id,
            context_id,
            now,
            A2ATaskEventPayload::Snapshot(projection),
        )
        .map_err(Into::into)
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
                error: rakka::persistence::DurableError::RevisionConflict { .. },
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
                error: rakka::persistence::DurableError::RevisionConflict { .. },
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
        | RakkaA2AHandlerError::InvalidLifecycle { .. } => A2ARunFailureKind::InvalidRequest,
        RakkaA2AHandlerError::Unavailable { .. } | RakkaA2AHandlerError::OwnerAsk { .. } => {
            A2ARunFailureKind::Unavailable
        }
        RakkaA2AHandlerError::StreamLimit { .. } => A2ARunFailureKind::Unavailable,
        RakkaA2AHandlerError::Inbox(_)
        | RakkaA2AHandlerError::RunEngine(_)
        | RakkaA2AHandlerError::RunActor(_)
        | RakkaA2AHandlerError::Persistence(_)
        | RakkaA2AHandlerError::Push(_) => A2ARunFailureKind::Internal,
    };
    let retryable = matches!(kind, A2ARunFailureKind::Unavailable);
    A2ARunFailure::new(code, message, kind, retryable)
}
