//! The durable command core of the agents surface.
//!
//! [`RakkaAgentA2AService`] is the agents-surface analog of the substrate's
//! request handler: every state-changing A2A operation is normalized, then
//! authorized, then durably accepted by the owning entity's deduplicated
//! operation-id inbox *before* it is acknowledged (specification 14.1). The
//! reply a duplicate receives is the original outcome, which is what makes a
//! retried send converge on one task, one run, one turn.
//!
//! The service drives the entity store facades directly over durable state,
//! exactly like the sharded entity actors do; the exchange router it holds
//! is the same seam the entities use to reach each other, so a deployment
//! wires in-process, sharded, or test routing without changing this type.

use std::collections::HashMap;
use std::sync::Arc;

use a2a::{CancelTaskRequest, Message, SendMessageRequest, Task};
use a2a_server::ServiceParams;
use rakka_agent::AgentExchangeRouter;
use rakka_agent::{
    load_agent_run_state, AgentEntityCommand, AgentEntityReply, AgentEntityState, AgentEntityStore,
    AgentId, AgentOperationId, AgentOperationKind, AgentRunScope, AgentRunState, AgentRunStatus,
    AgentSchemaPolicy, AgentScope, AgentTaskEntityCommand, AgentTaskEntityReply,
    AgentTaskEntityStore, AgentTaskHistoryStore, AgentTaskId, AgentTaskScope, AgentTaskSnapshot,
    AgentTaskState, AgentTeamEntityReply, AgentTeamEntityStore, AgentTeamHistoryStore,
    AgentTeamState, TenantId,
};
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_persistence::DurableStateStore;

use crate::auth::{A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, A2AOperation};
use crate::mapping::{
    canonical_tenant, merged_metadata, metadata_string, now_agent_timestamp,
    principal_ref_from_value, A2ATenantResolver,
};
use crate::projection::A2ATaskProjectionStore;

use super::catalog::A2AAgentCatalog;
use super::error::{RakkaAgentA2AError, RakkaAgentA2AResult};
use super::ingress::{
    agent_task_cancel_command, agent_task_create_command, agent_task_handoff_command,
    agent_task_input, agent_team_command, normalize_agent_cancel, normalize_agent_send,
    resolve_agent_target, resolve_handoff_target, NormalizedAgentCommand,
};
use super::management::{
    is_management_message, management_provenance, management_response_message,
    parse_management_request, refusal_response, AgentManagementCommand, AgentManagementResponse,
    META_AUDIT_REF,
};
use super::sync::{project_agent_send, sync_agent_status};

/// Time source for durable acceptance timestamps.
///
/// Injected so deterministic tests can drive entity time explicitly; the
/// default is the system clock the substrate surface uses.
pub trait A2AAgentClock: Send + Sync + 'static {
    /// The current instant, as epoch milliseconds.
    fn now(&self) -> AgentTimestampMillis;
}

/// System-clock implementation of [`A2AAgentClock`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemA2AAgentClock;

impl A2AAgentClock for SystemA2AAgentClock {
    fn now(&self) -> AgentTimestampMillis {
        now_agent_timestamp()
    }
}

/// The typed agent A2A service core.
///
/// Generic over the durable stores exactly like the entity facades it
/// drives; every store is cheap-clone by the [`DurableStateStore`] contract,
/// so each request materializes its own entity facade over shared state.
pub struct RakkaAgentA2AService<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<AgentTeamState>,
    TeamHistory: AgentTeamHistoryStore + Clone,
{
    tasks: Tasks,
    agents: Agents,
    history: History,
    runs: Runs,
    teams: Teams,
    team_history: TeamHistory,
    router: AgentExchangeRouter,
    catalog: Arc<dyn A2AAgentCatalog>,
    projections: Arc<dyn A2ATaskProjectionStore>,
    tenant_resolver: Arc<dyn A2ATenantResolver>,
    authorizer: Arc<dyn A2AAuthorizer>,
    clock: Arc<dyn A2AAgentClock>,
    default_tenant: Option<String>,
}

impl<Tasks, Agents, History, Runs, Teams, TeamHistory>
    RakkaAgentA2AService<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<AgentTeamState>,
    TeamHistory: AgentTeamHistoryStore + Clone,
{
    /// Creates a service over the given durable stores and policy seams.
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor mirrors the substrate builder's store/policy seams"
    )]
    pub fn new(
        tasks: Tasks,
        agents: Agents,
        history: History,
        runs: Runs,
        teams: Teams,
        team_history: TeamHistory,
        router: AgentExchangeRouter,
        catalog: Arc<dyn A2AAgentCatalog>,
        projections: Arc<dyn A2ATaskProjectionStore>,
        tenant_resolver: Arc<dyn A2ATenantResolver>,
        authorizer: Arc<dyn A2AAuthorizer>,
    ) -> Self {
        Self {
            tasks,
            agents,
            history,
            runs,
            teams,
            team_history,
            router,
            catalog,
            projections,
            tenant_resolver,
            authorizer,
            clock: Arc::new(SystemA2AAgentClock),
            default_tenant: None,
        }
    }

    /// Uses an explicit time source.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn A2AAgentClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Uses a fallback tenant for requests that resolve none.
    #[must_use]
    pub fn with_default_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.default_tenant = Some(tenant.into());
        self
    }

    /// Serves one `message/send`, dispatching on the message's declared
    /// extensions: a message tagged with the agent-management extension
    /// answers with an immediate management message (resolved open decision
    /// 10 — never a task); every other message is a typed task send.
    ///
    /// # Errors
    ///
    /// Fails closed exactly as [`Self::send_message`] and
    /// [`Self::manage_agent`] do.
    pub async fn send(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> RakkaAgentA2AResult<a2a::SendMessageResponse> {
        if is_management_message(&request.message) {
            return self
                .manage_agent(params, request)
                .await
                .map(a2a::SendMessageResponse::Message);
        }
        // A team command answers with an immediate message, exactly as a
        // management command does — it drives a board decision, never a
        // task creation (specification 8.10). The collaboration engagement
        // either parses whole or fails the send closed.
        let metadata =
            merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())
                .map_err(RakkaAgentA2AError::Mapping)?;
        if matches!(
            super::collaboration::parse_collaboration_envelope(&request.message, &metadata)?,
            Some(super::collaboration::AgentCollaborationEnvelope::Team(_))
        ) {
            return self
                .team_command(params, request)
                .await
                .map(a2a::SendMessageResponse::Message);
        }
        self.send_message(params, request)
            .await
            .map(a2a::SendMessageResponse::Task)
    }

    /// Serves one team board or membership command carried by the
    /// collaboration extension's team cluster
    /// ([specification 8.10](../../../../docs/plans/rakka-agent/spec.md)):
    /// authorizes it under its own operation class with the claimed team
    /// command bound in, durably applies it through the team entity's
    /// deduplicated inbox, drives the settle passes that deliver what the
    /// decision owed, and answers with an immediate response message.
    ///
    /// # Errors
    ///
    /// Fails closed on normalization, a missing required cluster field, an
    /// unauthenticated membership change, or authorization denial. A domain
    /// refusal — a stale epoch, a non-member, a busy entry — is not an
    /// error: it answers as a structured refusal message so the caller can
    /// rebase on the current board.
    pub async fn team_command(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> RakkaAgentA2AResult<Message> {
        let now = self.clock.now();
        let metadata =
            merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())
                .map_err(RakkaAgentA2AError::Mapping)?;
        let normalized = normalize_agent_send(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request.tenant.as_deref(),
            &request.message,
            &metadata,
        )?;
        let Some(super::collaboration::AgentCollaborationEnvelope::Team(cluster)) =
            normalized.collaboration.as_ref()
        else {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the send carries no team envelope",
            });
        };
        // The command is its own operation class at the authorization
        // boundary: the deployment authorizer sees `TeamCommand` with the
        // cluster's claimed verb, member, task, and target bound into the
        // request — never an undifferentiated send.
        let authorization = A2AAuthorizationRequest {
            operation: A2AOperation::TeamCommand,
            tenant: Some(normalized.tenant.as_str()),
            task_id: cluster.task.as_deref(),
            principal: normalized.principal.as_ref(),
            handoff: None,
            team: Some(crate::auth::A2ATeamClaim {
                team: &cluster.team,
                operation: cluster.operation.as_label(),
                member: cluster.member.as_deref(),
                task: cluster.task.as_deref(),
                target_member: cluster.target_member.as_deref(),
            }),
        };
        match self.authorizer.authorize(&authorization).await {
            A2AAuthorizationDecision::Allow => {}
            A2AAuthorizationDecision::Deny => return Err(RakkaAgentA2AError::Unauthorized),
        }

        let board_task = cluster.task.clone();
        let (scope, command) = agent_team_command(&normalized, now)?;
        let mut store =
            AgentTeamEntityStore::new(scope.clone(), self.teams.clone(), self.team_history.clone());
        let reply = match store.apply(command, &self.router, now).await {
            Ok(reply) => reply,
            // A domain refusal is a decision the caller rebases on, not a
            // transport failure; infrastructure faults stay errors.
            Err(error) if error.is_domain_refusal() => AgentTeamEntityReply::Rejected {
                code: error.code().to_string(),
                message: error.to_string(),
            },
            Err(error) => return Err(error.into()),
        };
        // The courier duty the entity actors otherwise perform: the team's
        // settle pass delivers the board decision to the task, whose accept
        // decides the assignment; the task's settle pass then delivers the
        // assignment onward and, once the claim resolves, the claim result
        // home; the final team settle absorbs an already-arrived result.
        // Outstanding exchanges beyond these bounded passes re-drive on
        // later operations and settle sweeps — convergence never depends on
        // this call completing them all.
        let _ = store.settle_side_effects(&self.router, now).await;
        if let Some(task) = board_task {
            if let Ok(task) = AgentTaskId::new(task) {
                if let Ok(task_scope) = AgentTaskScope::new(normalized.tenant.clone(), task) {
                    let mut tasks = AgentTaskEntityStore::new(
                        task_scope,
                        self.tasks.clone(),
                        self.agents.clone(),
                        self.history.clone(),
                    );
                    let _ = tasks.settle_side_effects(&self.router, now).await;
                }
            }
        }
        let _ = store.settle_side_effects(&self.router, now).await;
        Ok(team_response_message(&request.message.message_id, &reply))
    }

    /// Serves one agent-management command: parses the versioned envelope
    /// (failing closed on an unsupported version), authorizes it, applies it
    /// through the agent entity's durable deduplicated inbox, and answers
    /// with the immediate response message.
    ///
    /// # Errors
    ///
    /// Fails closed on an unsupported extension version, a missing
    /// authenticated principal on a write, an unresolved tenant, or
    /// authorization denial. A domain refusal — including the
    /// stale-revision conflict — is not an error: it answers as
    /// [`AgentManagementResponse::Refused`] so the caller can rebase.
    pub async fn manage_agent(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> RakkaAgentA2AResult<Message> {
        let now = self.clock.now();
        let management = parse_management_request(&request.message)?;
        let metadata =
            merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())
                .map_err(RakkaAgentA2AError::Mapping)?;
        let (tenant, _source) = canonical_tenant(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request.tenant.as_deref(),
        )
        .map_err(RakkaAgentA2AError::Mapping)?;
        let principal = metadata
            .get(crate::mapping::META_PRINCIPAL_REF)
            .map(principal_ref_from_value)
            .transpose()
            .map_err(RakkaAgentA2AError::Mapping)?;

        let operation = if management.command.is_write() {
            A2AOperation::AgentManagementWrite
        } else {
            A2AOperation::AgentManagementRead
        };
        let authorization = A2AAuthorizationRequest {
            operation,
            tenant: Some(tenant.as_str()),
            task_id: None,
            principal: principal.as_ref(),
            handoff: None,
            team: None,
        };
        match self.authorizer.authorize(&authorization).await {
            A2AAuthorizationDecision::Allow => {}
            A2AAuthorizationDecision::Deny => return Err(RakkaAgentA2AError::Unauthorized),
        }

        let agent = AgentId::new(management.command.agent())?;
        let scope = AgentScope::new(tenant.clone(), agent.clone())?;
        let discriminator = metadata_string(&metadata, crate::mapping::META_DEDUPLICATION_KEY)
            .map_err(RakkaAgentA2AError::Mapping)?
            .unwrap_or_else(|| request.message.message_id.clone());
        let audit_ref =
            metadata_string(&metadata, META_AUDIT_REF).map_err(RakkaAgentA2AError::Mapping)?;

        let command = match &management.command {
            AgentManagementCommand::Describe { .. } => AgentEntityCommand::Describe,
            write => {
                // Every write records who accepted it (specification 7.2);
                // an unauthenticated management write fails closed.
                let principal = principal.ok_or(RakkaAgentA2AError::Mapping(
                    crate::mapping::A2AMappingError::MissingField {
                        field: "io.rakka.principal.ref",
                    },
                ))?;
                let provenance = Box::new(management_provenance(
                    principal,
                    &request.message.message_id,
                    audit_ref.as_deref(),
                    now,
                ));
                // Each verb carries its own operation-id kind, so a reused
                // deduplication discriminator can never make one lifecycle
                // command alias another's durable operation — e.g. a `Resume`
                // colliding onto a prior `Suspend`'s cached outcome.
                let (kind, segments) = (
                    match write {
                        AgentManagementCommand::UpdateSettings { .. } => {
                            AgentOperationKind::SettingsUpdate
                        }
                        AgentManagementCommand::Suspend { .. } => {
                            AgentOperationKind::LifecycleSuspend
                        }
                        AgentManagementCommand::Resume { .. } => {
                            AgentOperationKind::LifecycleResume
                        }
                        AgentManagementCommand::Terminate { .. } => {
                            AgentOperationKind::LifecycleTerminate
                        }
                        AgentManagementCommand::Describe { .. } => {
                            unreachable!("describe is handled by the outer match")
                        }
                    },
                    [tenant.as_str(), agent.as_str(), discriminator.as_str()],
                );
                let operation_id = AgentOperationId::new(kind, segments)?;
                match write {
                    AgentManagementCommand::UpdateSettings {
                        expected_revision,
                        changes,
                        ..
                    } => AgentEntityCommand::UpdateSettings {
                        operation_id,
                        expected_revision: *expected_revision,
                        changes: changes.clone(),
                        provenance,
                    },
                    AgentManagementCommand::Suspend {
                        expected_lifecycle_revision,
                        ..
                    } => AgentEntityCommand::Suspend {
                        operation_id,
                        expected_lifecycle_revision: *expected_lifecycle_revision,
                        provenance,
                    },
                    AgentManagementCommand::Resume {
                        expected_lifecycle_revision,
                        ..
                    } => AgentEntityCommand::Resume {
                        operation_id,
                        expected_lifecycle_revision: *expected_lifecycle_revision,
                        provenance,
                    },
                    AgentManagementCommand::Terminate {
                        expected_lifecycle_revision,
                        ..
                    } => AgentEntityCommand::Terminate {
                        operation_id,
                        expected_lifecycle_revision: *expected_lifecycle_revision,
                        provenance,
                    },
                    AgentManagementCommand::Describe { .. } => unreachable!("handled above"),
                }
            }
        };

        let mut store = AgentEntityStore::new(scope, self.agents.clone());
        store.recover().await?;
        let response = match store.apply(command).await {
            Ok(AgentEntityReply::Applied { outcome }) => AgentManagementResponse::Applied {
                outcome: outcome.into(),
            },
            Ok(AgentEntityReply::Duplicate { outcome }) => AgentManagementResponse::Duplicate {
                outcome: outcome.into(),
            },
            Ok(AgentEntityReply::Snapshot(snapshot)) => match snapshot {
                Some(snapshot) => AgentManagementResponse::Described {
                    description: snapshot.as_ref().into(),
                },
                None => AgentManagementResponse::Refused {
                    code: "agent-not-instantiated".to_string(),
                    message: "the agent has no durable state to describe".to_string(),
                },
            },
            Ok(AgentEntityReply::Rejected { code, message }) => {
                AgentManagementResponse::Refused { code, message }
            }
            Ok(other) => AgentManagementResponse::Refused {
                code: "unexpected-reply".to_string(),
                message: format!("unexpected entity reply {other:?}"),
            },
            Err(error) => match refusal_response(&error) {
                Some(response) => response,
                None => return Err(error.into()),
            },
        };
        Ok(management_response_message(
            &request.message.message_id,
            &response,
        ))
    }

    /// Serves one typed task `message/send`: durably accepts the
    /// deduplicated task creation, settles what it made possible, projects
    /// the public view, and returns the public task.
    ///
    /// # Errors
    ///
    /// Fails closed on normalization, authorization, catalog resolution,
    /// entity refusal, or projection failure. A duplicate send is not an
    /// error: it returns the original task.
    pub async fn send_message(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> RakkaAgentA2AResult<Task> {
        let now = self.clock.now();
        let metadata =
            merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())
                .map_err(RakkaAgentA2AError::Mapping)?;
        let normalized = normalize_agent_send(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request.tenant.as_deref(),
            &request.message,
            &metadata,
        )?;
        if matches!(
            normalized.intent,
            crate::mapping::A2ATaskIntent::ContinueTask
        ) {
            // A continuation carrying the handoff cluster is the same-task
            // transfer of specification 8.9: the deduplicated handoff command
            // records the transfer, and the inline assignment decision offers
            // the target its generation in the same compare-and-set. Plain
            // input delivery stays parked for its own slice, cleanly
            // distinguished by the collaboration metadata.
            if let Some(super::collaboration::AgentCollaborationEnvelope::Handoff(cluster)) =
                normalized.collaboration.as_ref()
            {
                // The transfer is its own operation class at the
                // authorization boundary: the deployment authorizer sees
                // `RecordHandoff` with the cluster's claimed source and
                // target bound into the request — never an undifferentiated
                // send — so it can bind the authenticated caller to the
                // source run the cluster claims to speak for.
                self.authorize_claimed(
                    A2AOperation::RecordHandoff,
                    &normalized,
                    Some(crate::auth::A2AHandoffClaim {
                        handoff: &cluster.handoff,
                        source_agent: &cluster.source_agent,
                        source_run: &cluster.source_run,
                        source_generation: cluster.source_generation,
                        target_agent: &cluster.target_agent,
                    }),
                )
                .await?;
                // The same catalog gate the creation path passes: the wire's
                // target must be an agent this surface serves, checked before
                // the state-mutating command can commit — or spend one of the
                // task's bounded handoffs on an unserved target.
                resolve_handoff_target(self.catalog.as_ref(), cluster)?;
                let command = agent_task_handoff_command(&normalized)?;
                let snapshot = self.apply_task_command(&normalized, command, now).await?;
                let run = self.current_run_status(&normalized, &snapshot).await?;
                project_agent_send(
                    self.projections.as_ref(),
                    &snapshot,
                    run,
                    normalized.tenant.as_str(),
                    &normalized.context_id,
                    &request.message,
                    now,
                )
                .await?;
                return self.public_task(&normalized, None).await;
            }
            self.authorize(A2AOperation::SendMessage, &normalized)
                .await?;
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "send-message",
                reason: "input delivery to an existing agent task is not served yet",
            });
        }
        self.authorize(A2AOperation::SendMessage, &normalized)
            .await?;
        let input = agent_task_input(&request.message)?;
        let target = resolve_agent_target(self.catalog.as_ref(), &normalized)?;
        let command = agent_task_create_command(&normalized, &target, input)?;

        let snapshot = self.apply_task_command(&normalized, command, now).await?;
        let run = self.current_run_status(&normalized, &snapshot).await?;
        project_agent_send(
            self.projections.as_ref(),
            &snapshot,
            run,
            normalized.tenant.as_str(),
            &normalized.context_id,
            &request.message,
            now,
        )
        .await?;
        self.public_task(&normalized, None).await
    }

    /// Serves one `tasks/get` from the authoritative durable snapshot,
    /// healing the public projection when it lags.
    ///
    /// # Errors
    ///
    /// Fails closed on an unresolved tenant, authorization denial, or an
    /// unknown task.
    pub async fn get_task(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
        task_id: &str,
        history_length: Option<i32>,
    ) -> RakkaAgentA2AResult<Task> {
        let now = self.clock.now();
        let normalized = normalize_agent_cancel(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request_tenant,
            task_id,
            &HashMap::new(),
        )?;
        self.authorize(A2AOperation::GetTask, &normalized).await?;
        let snapshot = self.task_snapshot(&normalized, now).await?.ok_or_else(|| {
            RakkaAgentA2AError::TaskNotFound {
                task_id: task_id.to_string(),
            }
        })?;
        let run = self.current_run_status(&normalized, &snapshot).await?;
        sync_agent_status(
            self.projections.as_ref(),
            &snapshot,
            run,
            normalized.tenant.as_str(),
            &normalized.context_id,
            now,
            None,
        )
        .await?;
        self.public_task(&normalized, history_length).await
    }

    /// Serves one `tasks/cancel`: durably accepts the deduplicated
    /// cancellation and returns the resulting public task.
    ///
    /// A cancellation request alone never makes the public task terminal;
    /// the projection follows the authoritative condition while cancellation
    /// propagates (specification 14.3).
    ///
    /// # Errors
    ///
    /// Fails closed on an unresolved tenant, authorization denial, entity
    /// refusal, or an unknown task.
    pub async fn cancel_task(
        &self,
        params: &ServiceParams,
        request: &CancelTaskRequest,
    ) -> RakkaAgentA2AResult<Task> {
        let now = self.clock.now();
        let metadata = request.metadata.clone().unwrap_or_default();
        let normalized = normalize_agent_cancel(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request.tenant.as_deref(),
            &request.id,
            &metadata,
        )?;
        self.authorize(A2AOperation::CancelTask, &normalized)
            .await?;
        let command = agent_task_cancel_command(&normalized, "a2a-cancellation-requested");
        let snapshot = self.apply_task_command(&normalized, command, now).await?;
        let run = self.current_run_status(&normalized, &snapshot).await?;
        sync_agent_status(
            self.projections.as_ref(),
            &snapshot,
            run,
            normalized.tenant.as_str(),
            &normalized.context_id,
            now,
            None,
        )
        .await?;
        self.public_task(&normalized, None).await
    }

    /// Replays public task events after an optional cursor, reusing the
    /// shared A2A event replay: task-scoped cursors, bounded retention, and
    /// an explicit expired-window signal instead of a silent gap
    /// (specification 14.5).
    ///
    /// # Errors
    ///
    /// Fails closed on an unresolved tenant or authorization denial;
    /// surfaces [`crate::task::TaskProjectionError::ReplayWindowExpired`]
    /// through [`RakkaAgentA2AError::Projection`] so a caller resyncs.
    pub async fn replay_task_events(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
        task_id: &str,
        after_cursor: Option<&str>,
    ) -> RakkaAgentA2AResult<Vec<crate::task::A2ATaskEvent>> {
        let normalized = normalize_agent_cancel(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request_tenant,
            task_id,
            &HashMap::new(),
        )?;
        self.authorize(A2AOperation::SubscribeToTask, &normalized)
            .await?;
        self.projections
            .replay_events(normalized.tenant.as_str(), task_id, after_cursor)
            .await
            .map_err(Into::into)
    }

    /// Applies one deduplicated command through the task entity facade and
    /// returns the resulting authoritative snapshot.
    async fn apply_task_command(
        &self,
        normalized: &NormalizedAgentCommand,
        command: AgentTaskEntityCommand,
        now: AgentTimestampMillis,
    ) -> RakkaAgentA2AResult<AgentTaskSnapshot> {
        let scope = AgentTaskScope::new(normalized.tenant.clone(), normalized.task.clone())?;
        let mut store = AgentTaskEntityStore::new(
            scope,
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let reply = store.apply(command, &self.router, now).await?;
        match reply {
            AgentTaskEntityReply::Applied { .. } | AgentTaskEntityReply::Duplicate { .. } => {}
            AgentTaskEntityReply::Rejected { code, message } => {
                return Err(RakkaAgentA2AError::Refused { code, message });
            }
            other => {
                return Err(RakkaAgentA2AError::Refused {
                    code: "unexpected-reply".to_string(),
                    message: format!("unexpected entity reply {other:?}"),
                });
            }
        }
        store
            .snapshot()?
            .ok_or_else(|| RakkaAgentA2AError::TaskNotFound {
                task_id: normalized.task.as_str().to_string(),
            })
    }

    /// The authoritative durable view the in-process handoff executor probes
    /// after an ambiguous send: the task snapshot plus the current run's
    /// status, read from durable state — never the public projection, which
    /// is an observability read model that may lag the very commit the probe
    /// must find.
    ///
    /// In-process wiring only: the executor holding this service *is* the
    /// deployment, and its send already passed the deployment authorizer.
    /// External callers read through `tasks/get` and its authorization.
    pub(crate) async fn authoritative_task_view(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> RakkaAgentA2AResult<Option<(AgentTaskSnapshot, Option<AgentRunStatus>)>> {
        let now = self.clock.now();
        let tenant = TenantId::new(tenant);
        let task = AgentTaskId::new(task_id)?;
        let scope = AgentTaskScope::new(tenant.clone(), task)?;
        let mut store = AgentTaskEntityStore::new(
            scope,
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        store.recover(now).await?;
        let Some(snapshot) = store.snapshot()? else {
            return Ok(None);
        };
        let run = match snapshot.assignment.as_ref() {
            None => None,
            Some(assignment) => {
                let scope =
                    AgentRunScope::new(tenant, assignment.agent.clone(), assignment.run.clone())?;
                load_agent_run_state(&self.runs, &scope, &AgentSchemaPolicy::default())
                    .await?
                    .as_ref()
                    .and_then(rakka_agent::AgentRunState::status)
            }
        };
        Ok(Some((snapshot, run)))
    }

    /// Reads the authoritative task snapshot without mutating anything.
    async fn task_snapshot(
        &self,
        normalized: &NormalizedAgentCommand,
        now: AgentTimestampMillis,
    ) -> RakkaAgentA2AResult<Option<AgentTaskSnapshot>> {
        let scope = AgentTaskScope::new(normalized.tenant.clone(), normalized.task.clone())?;
        let mut store = AgentTaskEntityStore::new(
            scope,
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        store.recover(now).await?;
        Ok(store.snapshot()?)
    }

    /// The current run's status, when the task has an assigned run.
    async fn current_run_status(
        &self,
        normalized: &NormalizedAgentCommand,
        snapshot: &AgentTaskSnapshot,
    ) -> RakkaAgentA2AResult<Option<AgentRunStatus>> {
        let Some(assignment) = &snapshot.assignment else {
            return Ok(None);
        };
        let scope = AgentRunScope::new(
            normalized.tenant.clone(),
            assignment.agent.clone(),
            assignment.run.clone(),
        )?;
        let state = load_agent_run_state(&self.runs, &scope, &AgentSchemaPolicy::default()).await?;
        Ok(state.as_ref().and_then(rakka_agent::AgentRunState::status))
    }

    /// Renders the public task from the projection read model.
    async fn public_task(
        &self,
        normalized: &NormalizedAgentCommand,
        history_length: Option<i32>,
    ) -> RakkaAgentA2AResult<Task> {
        self.projections
            .get(
                Some(normalized.tenant.as_str()),
                normalized.task.as_str(),
                history_length,
            )
            .await
            .map_err(Into::into)
    }

    /// Runs the deployment authorizer for one operation.
    async fn authorize(
        &self,
        operation: A2AOperation,
        normalized: &NormalizedAgentCommand,
    ) -> RakkaAgentA2AResult<()> {
        self.authorize_claimed(operation, normalized, None).await
    }

    /// Runs the deployment authorizer for one operation, binding the claimed
    /// transfer into the request on a record-handoff check.
    async fn authorize_claimed(
        &self,
        operation: A2AOperation,
        normalized: &NormalizedAgentCommand,
        handoff: Option<crate::auth::A2AHandoffClaim<'_>>,
    ) -> RakkaAgentA2AResult<()> {
        let request = A2AAuthorizationRequest {
            operation,
            tenant: Some(normalized.tenant.as_str()),
            task_id: Some(normalized.task.as_str()),
            principal: normalized.principal.as_ref(),
            handoff,
            team: None,
        };
        match self.authorizer.authorize(&request).await {
            A2AAuthorizationDecision::Allow => Ok(()),
            A2AAuthorizationDecision::Deny => Err(RakkaAgentA2AError::Unauthorized),
        }
    }
}

/// A message helper for tests and clients: the accepted request message,
/// echoed with the task id the send resolved to.
#[must_use]
pub fn accepted_message(message: &Message, task_id: &str) -> Message {
    let mut message = message.clone();
    message.task_id = Some(task_id.to_string());
    message
}

/// Builds the immediate response message one team command answers with —
/// the management-response precedent: a team command never creates a task,
/// so its outcome rides a message, not a task projection.
#[must_use]
pub fn team_response_message(
    request_message_id: &str,
    reply: &rakka_agent::AgentTeamEntityReply,
) -> Message {
    let payload = serde_json::to_value(reply).unwrap_or(serde_json::Value::Null);
    let mut message = Message::new(
        a2a::Role::Agent,
        vec![a2a::Part {
            content: a2a::PartContent::Data(payload),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.extensions = Some(vec![
        super::collaboration::AGENT_COLLABORATION_EXTENSION_URI.to_string(),
    ]);
    message.message_id = format!("{request_message_id}::team-response");
    message
}
